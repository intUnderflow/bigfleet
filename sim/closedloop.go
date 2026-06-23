// Closed-loop simulation mode. The scripted runner (runner.go) drives
// the real decision engine against fixed rollup timelines — demand
// never reacts to BigFleet's actions, so feedback-loop bugs (the
// ADR-0038/0039/0040 class: Reclaim → drain → evict Pods → controllers
// recreate them → CR population churns → next rollup changes → Phase 1
// reacts) are invisible by construction. RunClosedLoop adds the other
// half of the loop: a per-cluster workload model that owns Pods and
// their CapacityRequests, derives each cycle's rollup from them
// (mirroring pkg/operator buildRollup semantics), binds Pods onto the
// cluster's Configured machines, and evicts them when BigFleet drains
// a machine. The pathologies that cost 90-minute cloud runs to find
// become seconds-long `go test` failures.
package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// sameRackKey is the topology key sameRack workloads co-locate on —
// the same label the scaletest seed stamps on machines and the
// operator's Same(rack) requirement matches (ADR-0024).
const sameRackKey = "topology.bigfleet/rack"

// sameZoneKey is the topology key sameZone workloads co-locate on.
// The engine resolves it from machine.Profile.Zone (lookupAttribute
// special-cases it), mirroring the realistic catalog's Same(zone)
// gang archetypes (gpu-training-medium/-large).
const sameZoneKey = "topology.kubernetes.io/zone"

// defaultZones mirrors the scaletest harness's zone pool.
var defaultZones = []string{"zone-a", "zone-b", "zone-c"}

// WorkloadShape is one entry in a closed-loop scenario's compact,
// scenario-local shape catalog. It carries exactly the fields the
// UPC → operator chain projects into a Need's Profile: nodeAffinity
// (instance types / zones → In requirements), priority, penalties,
// and the co-location bit (sameRack → Same(rack) requirement appended
// at rollup, mirroring withSameRequirement).
type WorkloadShape struct {
	Name string

	// PodResources is the per-Pod resource request. One CR = one Pod =
	// this vector for both AggregateResources and MinUnit (ADR-0027).
	PodResources map[string]string

	// InstanceTypes / Zones become `In` requirements when non-empty
	// (mirrors the load-driver's buildPodTemplate nodeAffinity terms).
	InstanceTypes []string
	Zones         []string

	Priority                   int32
	InterruptionPenaltyDollars float64
	ReclamationPenaltyDollars  float64

	// SameRack marks the shape co-located: each workload object is one
	// gang, its CRs share a co-location group, and the derived Need
	// carries a Same requirement on sameRackKey. SameZone is the
	// zone-scoped twin (Same on sameZoneKey). At most one may be set —
	// a Profile carries at most one Same requirement (ADR-0024).
	SameRack bool
	SameZone bool
}

// WorkloadSpec declares controller-managed workload objects of one
// shape in a cluster. For SameRack shapes each object is one gang
// (ADR-0038: one workload object IS one co-location group), so
// Replicas is the gang size.
type WorkloadSpec struct {
	Shape    string
	Objects  int // number of controller objects; 0 → 1
	Replicas int // Pods per object
}

// ClusterSpec is one cluster's workload population.
type ClusterSpec struct {
	ID        machine.ClusterID
	Workloads []WorkloadSpec
}

// SeedPool seeds machines shaped for one catalog shape, mirroring
// cmd/bigfleet seedFakeInventory's three tiers (ADR-0026): Configured
// (workloads already running), Idle (owned headroom, bare metal,
// price 0), and Speculative (elastic quota, on-demand, priced).
type SeedPool struct {
	// Shape names the catalog shape this pool hosts. Machine profile
	// (instance type, zone rotation, resources) derives from it.
	Shape string

	// MachineInstanceType overrides the machine's instance type.
	// Defaults to the shape's first instance type; shapes with no
	// instance affinity need an explicit value (any string — their
	// Needs don't constrain it).
	MachineInstanceType string

	// Density is Pods per machine: Allocatable = Density × the shape's
	// PodResources (ADR-0022's densityMultiplier). 0 / 1 → one Pod per
	// machine.
	Density int

	// IdleCapacityType sets the capacity type of the pool's Idle tier.
	// Zero value → BareMetal at price 0 (the historical owned-headroom
	// seed). OnDemand / Spot idle carries the elastic price so M73
	// release scenarios can seed surplus idle in a releasable tier.
	IdleCapacityType machine.CapacityType

	// RacksPerZone sizes the synthetic rack pool (rack name embeds the
	// zone, as in seedZoneRack). 0 → 4.
	RacksPerZone int

	// ContiguousRackBlock > 0 places machines in contiguous rack
	// blocks of that size — one block per rack — per ADR-0040 Addendum
	// §4 (real fleets procure co-located capacity in rack units).
	// 0 = the hostile round-robin spread that left ~1-3 co-located
	// machines per rack against gangs (the #50/#51 topology).
	ContiguousRackBlock int

	ConfiguredPerCluster int // Configured machines seeded per cluster
	Idle                 int // shard-wide Idle headroom
	Speculative          int // shard-wide Speculative slots (on-demand)

	// InterruptibleSpeculative seeds a SECOND Speculative tier that is
	// cheaper but has a higher interruption probability, with profiles
	// IDENTICAL to the primary Speculative tier (same instance type, zone,
	// resources, and CapacityType), so both compete for the same Need and
	// Phase 1's effective-cost sort alone decides between them (#354
	// cost-routing). The tiers differ only in (price, interruption
	// probability) — BigFleet routes on effective_cost, not a "spot" label.
	// Zero → no second tier (byte-identical to before).
	InterruptibleSpeculative int
}

// TargetScale rescales every workload object of Shape (across all
// clusters) to Replicas at the start of cycle Cycle, before that
// cycle's rollups derive — the closed-loop analogue of
// `kubectl scale`. Scale-down deletes pending Pods first, then bound
// Pods newest-first (the ReplicaSet victim preference); the CRs die
// with their Pods, so the cycle's rollup carries the shrunken demand
// (ADR-0039 full replacement) and Phase 3's shrinkage diff sees
// bound > demand. Scale-up appends pending Pods.
type TargetScale struct {
	Cycle    int
	Shape    string
	Replicas int
}

// FaultEvent is a single incumbent loss: fail Count Configured machines
// of cluster Cluster at the start of cycle Cycle (before that cycle's
// rollups derive), via the provider's FailMachine (the M38 spot-reclaim /
// hardware-fault path). The victims are the cluster's Configured machines
// in deterministic ID order so the same scenario fails the same machines
// every run.
type FaultEvent struct {
	Cycle   int
	Cluster machine.ClusterID
	Count   int
}

// ClosedLoopScenario declares one closed-loop run.
type ClosedLoopScenario struct {
	Name     string
	Shapes   []WorkloadShape
	Clusters []ClusterSpec
	Seeds    []SeedPool

	// ControllerManaged gates ADR-0038 semantics: true → evicted Pods
	// are recreated by their controller (demand is conserved); false →
	// bare Pods, eviction destroys demand permanently (the #45 cascade
	// pathology).
	ControllerManaged bool

	// CRPerPod gates ADR-0039 semantics: true → one CR per live Pod
	// (papers §6.1, total demand); false → CRs only for pending Pods
	// (the pre-fix unmet-only signal that gave Phase 3 a phantom
	// surplus).
	CRPerPod bool

	// Scales are mid-run demand changes — the trigger ADR-0045's
	// shrinkage-only Phase 3 acts on.
	Scales []TargetScale

	// Faults are mid-run incumbent losses: at cycle Cycle, fail Count
	// Configured machines of cluster Cluster in the provider (the M38
	// FailMachine path — spot reclaim / hardware fault discovered via
	// List). The shard's reconcile ingests StateFailed, the cluster model
	// evicts the lost machine's Pods, and the next Phase 1 re-derives the
	// gang's now-uncovered deficit. STATIC demand otherwise: this is the
	// single perturbation the pre-Configuring-runway over-acquire
	// hypothesis turns on (the dev-50 #66 churn injector at sim speed).
	Faults []FaultEvent

	// RollupArrivalStamps models the wire path's arrival semantics
	// (conv.NeedsFromRollup): every Need row in a rollup carries the
	// MESSAGE's build timestamp as ArrivalUnixNanos, so each rollup
	// re-stamps all of the cluster's rows to "now". Operators roll up
	// independently, so the cross-cluster freshness order races; the
	// sim models that race deterministically by rotating which
	// cluster's rollup is "older" each cycle. The shard's NeedsTable
	// sorts equal-priority rows arrival-asc, so this rotation is
	// exactly the cross-cluster walk-order flip a real shard sees.
	// False keeps the historical arrival=0 (stable-order) behaviour.
	RollupArrivalStamps bool

	// BootstrapDwellCycles models the engine's own bootstrap LATENCY
	// (ADR-0051 / M77g, field-confirmed in bigfleet-uber #64): a machine
	// the engine bootstraps stays Configuring for this many sim cycles
	// before completing to Configured/acquirable. 0 (default) keeps the
	// historical instant Configuring→Configured, so every pre-existing
	// scenario is byte-identical. With dwell > 0 the acquirable pool
	// (Idle + Speculative — Configuring is excluded) shrinks while a
	// bootstrap is in flight and refills when it completes, so the
	// snapshot the Same-domain tiebreak ranks on genuinely moves every
	// cycle — the perturbation the offline diagnosis (#63) could not
	// reproduce because instant transitions froze the pool at
	// equilibrium. Modelled minimally: a per-machine countdown decremented
	// each cycle; the fake provider holds Configure at Configuring
	// (ConfigureStaged) and this loop drives the completion.
	BootstrapDwellCycles int

	// ProvisionDwellCycles models the engine's PRE-Configuring (host
	// provisioning) latency — the Speculative → Creating → Idle runway a
	// real provider's Create traverses (boot, image pull, kubelet join).
	// A machine the engine Provisions (Speculative source) stays in
	// Creating for this many sim cycles before reaching Idle, where the
	// next cycle's Phase 1 can Bootstrap it (Idle → Configuring). 0
	// (default) keeps the historical instant Create → Idle, so every
	// pre-existing scenario is byte-identical.
	//
	// Why this is distinct from BootstrapDwellCycles and why it matters:
	// Configuring IS a state the Phase 1 Same-domain coverage walk counts
	// (pkg/decision/occ/seed.go: Configured + Configuring), and
	// executeBootstrap stamps AssignedGroup at Idle→Configuring — so a
	// machine in the BOOTSTRAP dwell is visible to its own gang's
	// coverage and the dwell self-damps. Creating is counted by NEITHER
	// the coverage walk (not in {Configured,Configuring}) NOR the
	// acquirable pool (foldAcquirable folds only Idle+Speculative), and
	// executeProvision leaves it with NO AssignedGroup. A machine in the
	// PROVISION dwell is therefore INVISIBLE: the gang that triggered its
	// acquisition still sees the full deficit (ADR-0045 re-derives from
	// scratch, no memory of the in-flight acquisition) and acquires
	// again. Modelled the same shape as BootstrapDwellCycles: a
	// per-machine countdown the loop drives, with the fake provider
	// holding Create at Creating (CreateStaged).
	ProvisionDwellCycles int

	// StampSeededGangAttribution stamps AssignedGroup (+
	// AssignedNeedFingerprint) on the seeded Configured machines a gang
	// binds to in the initial fill (ADR-0051 / M77g). A real fleet's
	// Configured machine that serves gang G was Configured *for* G, so it
	// carries G's group; the harness's cmd/bigfleet seed cannot know the
	// load-driver's runtime gang IDs, but the sim's initial fill decides
	// the gang→machine mapping deterministically and so can stamp it. With
	// this off (default) seeded gang machines carry no attribution — which
	// is the production seed's actual limitation, the case the unit pin and
	// the engine-acquired repro arm exercise separately. False keeps every
	// pre-existing scenario byte-identical.
	StampSeededGangAttribution bool

	// ReleasePolicy overrides the shard's M73 idle-hold policy. nil →
	// the paper-§8 production constants (10m / 1m), which sim cycles
	// (milliseconds of wall-clock) never cross — so every pre-M73
	// scenario is byte-identical. Release scenarios pass short holds;
	// sim cycles ≈ time, and there is deliberately no production
	// config surface behind this (ADR-0049).
	ReleasePolicy *decision.ReleasePolicy

	Cycles int
}

// CycleStats is one cycle's counters.
type CycleStats struct {
	Cycle int

	// Action counts from this cycle's shard.Step.
	Bootstraps int
	Provisions int
	Reclaims   int
	Preempts   int
	Deletes    int // M73 idle releases (paper §8 Idle→Speculative)

	Configured  int // shard-wide Configured machines after the cycle
	BoundPods   int
	PendingPods int
	LivePods    int // bound + pending across all clusters
	Evicted     int // Pods evicted by this cycle's machine transitions

	Shortfalls int // shard.ShortfallCount after the cycle

	// ReclaimMatchesShortfall is the ADR-0040 §4 probe: Reclaim actions
	// this cycle whose machine MatchProfiles a Need the same cycle left
	// unsatisfied (read from the shard's shortfall tracker). Under
	// correct Same-domain attribution this reads zero in steady state.
	ReclaimMatchesShortfall int
}

// Churn is the cycle's total decision-action volume. The convergence
// assertions sum it over a trailing window: a converged closed loop
// emits no actions at all.
func (c CycleStats) Churn() int {
	return c.Bootstraps + c.Provisions + c.Reclaims + c.Preempts + c.Deletes
}

// WorkloadStanding is one workload object's final population.
type WorkloadStanding struct {
	Cluster  machine.ClusterID
	Workload string
	Shape    string
	Target   int // declared replicas (the controller's spec)
	Live     int // Pods that exist (bound + pending)
	Bound    int
}

// ClosedLoopResult is RunClosedLoop's output.
type ClosedLoopResult struct {
	Cycles     []CycleStats
	Workloads  []WorkloadStanding
	TargetPods int
	// FinalSnapshot is the shard inventory after the last cycle, so tests
	// can assert on the converged per-machine state (e.g. which capacity
	// tier each cluster's machines were routed to). Not serialized.
	FinalSnapshot *inventory.Snapshot
}

// Last returns the trailing k cycles (or all, if fewer ran).
func (r *ClosedLoopResult) Last(k int) []CycleStats {
	if k >= len(r.Cycles) {
		return r.Cycles
	}
	return r.Cycles[len(r.Cycles)-k:]
}

// SumLast folds f over the trailing k cycles.
func (r *ClosedLoopResult) SumLast(k int, f func(CycleStats) int) int {
	total := 0
	for _, c := range r.Last(k) {
		total += f(c)
	}
	return total
}

// resVec is a resource vector in interned milli-units (one slot per
// scenario dimension). Integer math keeps the per-cycle bind scan free
// of resource.Quantity parsing — the same trick occ.SameSupplyIndex
// uses on the engine side.
type resVec []int64

// vecSpace interns the scenario's resource dimensions.
type vecSpace struct {
	dims []string
	idx  map[string]int
}

func newVecSpace(shapes []WorkloadShape) *vecSpace {
	seen := map[string]struct{}{}
	for i := range shapes {
		for name := range shapes[i].PodResources {
			seen[name] = struct{}{}
		}
	}
	dims := make([]string, 0, len(seen))
	for name := range seen {
		dims = append(dims, name)
	}
	sort.Strings(dims)
	idx := make(map[string]int, len(dims))
	for i, d := range dims {
		idx[d] = i
	}
	return &vecSpace{dims: dims, idx: idx}
}

func (s *vecSpace) fromMap(m map[string]string) (resVec, error) {
	v := make(resVec, len(s.dims))
	for name, raw := range m {
		i, ok := s.idx[name]
		if !ok {
			continue // dimension no scenario shape requests; irrelevant to binding
		}
		q, err := resource.ParseQuantity(raw)
		if err != nil {
			return nil, fmt.Errorf("quantity %q for %s: %w", raw, name, err)
		}
		v[i] = q.MilliValue()
	}
	return v, nil
}

func vecCoversVec(have, want resVec) bool {
	for i := range want {
		if want[i] > 0 && have[i] < want[i] {
			return false
		}
	}
	return true
}

func vecSubInPlace(have, want resVec) {
	for i := range want {
		have[i] -= want[i]
	}
}

func vecAddInPlace(have, add resVec) {
	for i := range add {
		have[i] += add[i]
	}
}

// vecSlots is how many more want-sized units fit in have — the bind
// scan's "least allocated" score (kube-scheduler's default spreading),
// which keeps unconstrained Pods off other shapes' slack so they don't
// fragment capacity the engine's vector math can't see.
func vecSlots(have, want resVec) int {
	slots := int(^uint(0) >> 1)
	for i := range want {
		if want[i] <= 0 {
			continue
		}
		s := int(have[i] / want[i])
		if s < slots {
			slots = s
		}
	}
	return slots
}

// shapeRT is a shape with its derived runtime artifacts: the Need
// Profile (computed once — NewProfile canonicalises and fingerprints),
// the per-Pod resource vector in both Need ([]ResourceQty) and bind
// (resVec) forms, and the Same topology key ("" for non-gang shapes).
type shapeRT struct {
	spec    *WorkloadShape
	profile needs.Profile
	podQty  []needs.ResourceQty
	podVec  resVec
	sameKey string
}

// sameValue resolves the machine's value for the shape's Same key the
// way the engine's lookupAttribute does: the zone key reads
// Profile.Zone, anything else reads the label map.
func (s *shapeRT) sameValue(m *machine.Machine) (string, bool) {
	switch s.sameKey {
	case sameZoneKey:
		return m.Profile.Zone, m.Profile.Zone != ""
	default:
		v, ok := m.Profile.Labels[s.sameKey]
		return v, ok
	}
}

// machineMatches is the bind model's nodeAffinity check — the same
// constraints the shape's requirements express, evaluated against the
// machine the way a scheduler would evaluate them against a Node.
func (s *shapeRT) machineMatches(m *machine.Machine) bool {
	if len(s.spec.InstanceTypes) > 0 && !containsString(s.spec.InstanceTypes, m.Profile.InstanceType) {
		return false
	}
	if len(s.spec.Zones) > 0 && !containsString(s.spec.Zones, m.Profile.Zone) {
		return false
	}
	return true
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// buildShapes resolves the catalog into runtime form, mirroring the
// UPC → buildRollup Profile derivation: In requirements from the
// nodeAffinity sets, a Same(rack) requirement appended for sameRack
// shapes (withSameRequirement), penalties bucketed via BucketForDollars
// (the operator is the canonical bucketing site).
func buildShapes(specs []WorkloadShape, space *vecSpace) (map[string]*shapeRT, error) {
	out := make(map[string]*shapeRT, len(specs))
	for i := range specs {
		sp := &specs[i]
		if sp.Name == "" {
			return nil, fmt.Errorf("shape %d: empty name", i)
		}
		if _, dup := out[sp.Name]; dup {
			return nil, fmt.Errorf("duplicate shape %q", sp.Name)
		}
		if len(sp.PodResources) == 0 {
			return nil, fmt.Errorf("shape %q: empty PodResources", sp.Name)
		}
		reqs := make([]needs.Requirement, 0, 3)
		if len(sp.InstanceTypes) > 0 {
			reqs = append(reqs, needs.Requirement{
				Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn,
				Values: append([]string(nil), sp.InstanceTypes...),
			})
		}
		if len(sp.Zones) > 0 {
			reqs = append(reqs, needs.Requirement{
				Key: "topology.kubernetes.io/zone", Operator: needs.OperatorIn,
				Values: append([]string(nil), sp.Zones...),
			})
		}
		if sp.SameRack && sp.SameZone {
			return nil, fmt.Errorf("shape %q: SameRack and SameZone are mutually exclusive (ADR-0024: one Same requirement per Profile)", sp.Name)
		}
		sameKey := ""
		switch {
		case sp.SameRack:
			sameKey = sameRackKey
		case sp.SameZone:
			sameKey = sameZoneKey
		}
		if sameKey != "" {
			reqs = append(reqs, needs.Requirement{Key: sameKey, Operator: needs.OperatorSame})
		}
		podVec, err := space.fromMap(sp.PodResources)
		if err != nil {
			return nil, fmt.Errorf("shape %q: %w", sp.Name, err)
		}
		out[sp.Name] = &shapeRT{
			spec: sp,
			profile: needs.NewProfile(reqs, nil, sp.Priority,
				needs.BucketForDollars(sp.InterruptionPenaltyDollars),
				needs.BucketForDollars(sp.ReclamationPenaltyDollars)),
			podQty:  needs.ResourceQtysFromMap(sp.PodResources),
			podVec:  podVec,
			sameKey: sameKey,
		}
	}
	return out, nil
}

// clPod is one Pod in the cluster model: bound to a machine or pending.
type clPod struct {
	name  string
	bound machine.ID // "" = pending
	gone  bool       // evicted this cycle; swept by the controller pass
}

// clWorkload is one controller-managed workload object.
type clWorkload struct {
	name  string
	shape *shapeRT

	// group is the co-location aggregation key for sameRack shapes —
	// the analogue of coLocationGroup's TopologyKey + NUL + selector
	// canonical form, unique per gang so each gang stays its own Need
	// (ADR-0024). Empty for ordinary shapes.
	group string

	target int // declared replicas
	next   int // ordinal for deterministically-named recreated Pods

	// anchor is the gang's pinned Same-domain value (rack for SameRack,
	// zone for SameZone): set when the first member binds, cleared when
	// no member remains bound (the gang re-anchors on rebind). Mirrors
	// the harness's anchor + rack-faithful pre-bind (ADR-0040 §3) at
	// the granularity the spec asks for.
	anchor string

	pods []*clPod
}

// clusterModel owns one cluster's workloads, Pods, and the per-machine
// capacity bookkeeping for the bind model. All state is the model's
// own — it observes BigFleet only through the shard's inventory
// snapshot, exactly as a real cluster observes Nodes.
type clusterModel struct {
	id        machine.ClusterID
	workloads []*clWorkload
	bindOrder []*clWorkload // gangs, then instance-typed, then unconstrained

	podsByMachine map[machine.ID][]*clPod
	remaining     map[machine.ID]resVec // EffectiveAllocatable − Σ bound Pod requests
	space         *vecSpace
}

func buildClusterModel(spec ClusterSpec, shapes map[string]*shapeRT, space *vecSpace) (*clusterModel, error) {
	c := &clusterModel{
		id:            spec.ID,
		podsByMachine: make(map[machine.ID][]*clPod),
		remaining:     make(map[machine.ID]resVec),
		space:         space,
	}
	for wi, w := range spec.Workloads {
		sh, ok := shapes[w.Shape]
		if !ok {
			return nil, fmt.Errorf("cluster %s workload %d: unknown shape %q", spec.ID, wi, w.Shape)
		}
		objects := w.Objects
		if objects <= 0 {
			objects = 1
		}
		if w.Replicas <= 0 {
			return nil, fmt.Errorf("cluster %s workload %d: Replicas must be > 0", spec.ID, wi)
		}
		for o := 0; o < objects; o++ {
			wl := &clWorkload{
				name:   fmt.Sprintf("%s-%s-%d", spec.ID, w.Shape, wi*1000+o),
				shape:  sh,
				target: w.Replicas,
			}
			if sh.sameKey != "" {
				wl.group = sh.sameKey + "\x00" + wl.name
			}
			for i := 0; i < w.Replicas; i++ {
				wl.pods = append(wl.pods, &clPod{name: fmt.Sprintf("%s-%d", wl.name, i)})
			}
			wl.next = w.Replicas
			c.workloads = append(c.workloads, wl)
		}
	}
	// Bind order: hardest-to-place first (gangs, then instance-typed,
	// then unconstrained), stable within a class. Keeps unconstrained
	// Pods from squatting on capacity a constrained Pod needs in the
	// same pass.
	c.bindOrder = append([]*clWorkload(nil), c.workloads...)
	sort.SliceStable(c.bindOrder, func(i, j int) bool {
		return bindClass(c.bindOrder[i]) < bindClass(c.bindOrder[j])
	})
	return c, nil
}

func bindClass(wl *clWorkload) int {
	switch {
	case wl.shape.sameKey != "":
		return 0
	case len(wl.shape.spec.InstanceTypes) > 0 || len(wl.shape.spec.Zones) > 0:
		return 1
	default:
		return 2
	}
}

// rollup derives the cluster's ClusterCapacityNeeds-equivalent from
// the live Pod population, mirroring pkg/operator buildRollup: one CR
// per Pod (or per pending Pod when crPerPod is false — the pre-ADR-0039
// unmet-only signal), each contributing a Need with AggregateResources
// = MinUnit = the Pod's request, grouped by (Profile fingerprint,
// co-location group) through the real needs.Aggregate.
//
// arrivalNanos is the rollup message's build timestamp, stamped on
// every row exactly as conv.NeedsFromRollup does on the wire path
// (0 = unstamped, the historical sim behaviour).
func (c *clusterModel) rollup(crPerPod bool, arrivalNanos int64) []needs.Need {
	raw := make([]needs.Need, 0, 64)
	for _, wl := range c.workloads {
		for _, p := range wl.pods {
			if !crPerPod && p.bound != "" {
				continue
			}
			raw = append(raw, needs.Need{
				ClusterID:          c.id,
				Profile:            wl.shape.profile,
				AggregateResources: wl.shape.podQty,
				MinUnit:            wl.shape.podQty,
				Group:              wl.group,
				ArrivalUnixNanos:   arrivalNanos,
			})
		}
	}
	return needs.Aggregate(raw)
}

// evictAndReconcile is the cluster's reaction to BigFleet's machine
// transitions: any machine that was hosting this cluster's Pods and is
// no longer Configured for it (drained by Reclaim/Preempt, failed,
// deleted) has its Pods evicted. Eviction deletes the Pod (and its CR
// dies with it — ownerRef GC); when controllerManaged, the controller
// recreates the deleted Pods as pending (ADR-0038: demand conserved);
// otherwise they are gone for good (the bare-Pod pathology). Gangs
// whose every member was evicted lose their rack pin and re-anchor on
// rebind. Returns the evicted Pod count.
func (c *clusterModel) evictAndReconcile(snap *inventory.Snapshot, controllerManaged bool) int {
	ids := make([]machine.ID, 0, len(c.podsByMachine))
	for id := range c.podsByMachine {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	evicted := 0
	for _, id := range ids {
		m, ok := snap.Get(id)
		if ok && m.State == machine.StateConfigured && m.Cluster == c.id {
			continue
		}
		for _, p := range c.podsByMachine[id] {
			p.bound = ""
			p.gone = true
			evicted++
		}
		delete(c.podsByMachine, id)
		delete(c.remaining, id)
	}
	if evicted == 0 {
		return 0
	}
	for _, wl := range c.workloads {
		kept := wl.pods[:0]
		removed := 0
		for _, p := range wl.pods {
			if p.gone {
				removed++
				continue
			}
			kept = append(kept, p)
		}
		wl.pods = kept
		if controllerManaged {
			for i := 0; i < removed; i++ {
				wl.pods = append(wl.pods, &clPod{name: fmt.Sprintf("%s-%d", wl.name, wl.next)})
				wl.next++
			}
		}
		if wl.anchor != "" {
			bound := false
			for _, p := range wl.pods {
				if p.bound != "" {
					bound = true
					break
				}
			}
			if !bound {
				wl.anchor = ""
			}
		}
	}
	return evicted
}

// bind places pending Pods onto the cluster's Configured machines.
// Candidate filter: the shape's nodeAffinity sets, plus Same-domain
// coherence for gangs (first member anchors the gang's rack/zone;
// later members bind only there — a member that doesn't fit in the
// anchor domain stays pending, never scattered, mirroring
// planGroupOntoRack's "one rack or nowhere"). Among candidates the
// emptiest machine wins (most free Pod-slots; ID tiebreak) —
// kube-scheduler-style least-allocated spreading.
func (c *clusterModel) bind(snap *inventory.Snapshot) {
	machines := snap.ListByClusterState(c.id, machine.StateConfigured)
	sort.Slice(machines, func(i, j int) bool { return machines[i].ID < machines[j].ID })
	for i := range machines {
		if _, ok := c.remaining[machines[i].ID]; !ok {
			vec, err := c.space.fromMap(machines[i].EffectiveAllocatable())
			if err != nil {
				// Unparseable allocatable: the machine effectively can't
				// host anything — the conservative answer, mirroring the
				// harness's scaleResourceMap fallback.
				vec = make(resVec, len(c.space.dims))
			}
			c.remaining[machines[i].ID] = vec
		}
	}
	for _, wl := range c.bindOrder {
		for _, p := range wl.pods {
			if p.bound != "" {
				continue
			}
			best := -1
			bestSlots := -1
			for i := range machines {
				m := &machines[i]
				if !wl.shape.machineMatches(m) {
					continue
				}
				if wl.shape.sameKey != "" {
					v, has := wl.shape.sameValue(m)
					if !has {
						continue
					}
					if wl.anchor != "" && v != wl.anchor {
						continue
					}
				}
				rem := c.remaining[m.ID]
				if !vecCoversVec(rem, wl.shape.podVec) {
					continue
				}
				if s := vecSlots(rem, wl.shape.podVec); s > bestSlots {
					bestSlots = s
					best = i
				}
			}
			if best < 0 {
				continue // stays pending; its CR keeps the demand visible
			}
			m := &machines[best]
			vecSubInPlace(c.remaining[m.ID], wl.shape.podVec)
			p.bound = m.ID
			c.podsByMachine[m.ID] = append(c.podsByMachine[m.ID], p)
			if wl.shape.sameKey != "" && wl.anchor == "" {
				wl.anchor, _ = wl.shape.sameValue(m)
			}
		}
	}
}

// scaleTo applies a TargetScale to this cluster: every workload object
// of the named shape gets its target set to replicas, deleting surplus
// Pods (pending first, then newest bound — the ReplicaSet preference)
// or appending pending ones. Deleting a bound Pod frees its machine
// capacity immediately; the next rollup simply carries fewer CRs.
func (c *clusterModel) scaleTo(shapeName string, replicas int) {
	for _, wl := range c.workloads {
		if wl.shape.spec.Name != shapeName {
			continue
		}
		wl.target = replicas
		for len(wl.pods) > replicas {
			victim := -1
			for i := len(wl.pods) - 1; i >= 0; i-- {
				if wl.pods[i].bound == "" {
					victim = i
					break
				}
			}
			if victim < 0 {
				victim = len(wl.pods) - 1
			}
			p := wl.pods[victim]
			if p.bound != "" {
				c.unbind(wl, p)
			}
			wl.pods = append(wl.pods[:victim], wl.pods[victim+1:]...)
		}
		for len(wl.pods) < replicas {
			wl.pods = append(wl.pods, &clPod{name: fmt.Sprintf("%s-%d", wl.name, wl.next)})
			wl.next++
		}
		// Gang anchor pin: with no bound member left, re-anchor on
		// rebind (same rule as evictAndReconcile).
		if wl.anchor != "" {
			bound := false
			for _, p := range wl.pods {
				if p.bound != "" {
					bound = true
					break
				}
			}
			if !bound {
				wl.anchor = ""
			}
		}
	}
}

// unbind releases p's machine capacity and bookkeeping without marking
// it evicted — the Pod is being deleted on purpose (scale-down), not
// killed by a machine transition.
func (c *clusterModel) unbind(wl *clWorkload, p *clPod) {
	mid := p.bound
	p.bound = ""
	pods := c.podsByMachine[mid]
	for i, q := range pods {
		if q == p {
			c.podsByMachine[mid] = append(pods[:i], pods[i+1:]...)
			break
		}
	}
	if rem, ok := c.remaining[mid]; ok {
		vecAddInPlace(rem, wl.shape.podVec)
	}
}

// gangBinding pairs a machine with the gang (co-location group +
// Profile fingerprint) currently bound to it.
type gangBinding struct {
	id          machine.ID
	group       string
	fingerprint string
}

// gangBindings returns, for every machine hosting at least one Pod of a
// gang (sameKey != "") workload, the gang's group + fingerprint
// (ADR-0051 / M77g). Used to stamp AssignedGroup on seeded Configured
// machines so a pre-bound gang carries the attribution the Same-domain
// tiebreak reads — mirroring a real fleet, where the machine serving a
// gang was Configured for it.
func (c *clusterModel) gangBindings() []gangBinding {
	out := make([]gangBinding, 0)
	seen := map[machine.ID]struct{}{}
	for _, wl := range c.workloads {
		if wl.shape.sameKey == "" {
			continue
		}
		for _, p := range wl.pods {
			if p.bound == "" {
				continue
			}
			if _, dup := seen[p.bound]; dup {
				continue
			}
			seen[p.bound] = struct{}{}
			out = append(out, gangBinding{
				id:          p.bound,
				group:       wl.group,
				fingerprint: wl.shape.profile.Fingerprint(),
			})
		}
	}
	return out
}

func (c *clusterModel) podCounts() (bound, pending int) {
	for _, wl := range c.workloads {
		for _, p := range wl.pods {
			if p.bound != "" {
				bound++
			} else {
				pending++
			}
		}
	}
	return bound, pending
}

// scaleQuantityMap multiplies each quantity by factor with
// resource.Quantity arithmetic, preserving canonical string form
// (mirrors cmd/bigfleet scaleResourceMap). factor ≤ 1 copies unchanged.
func scaleQuantityMap(in map[string]string, factor int) (map[string]string, error) {
	out := make(map[string]string, len(in))
	for k, v := range in {
		q, err := resource.ParseQuantity(v)
		if err != nil {
			return nil, fmt.Errorf("quantity %q for %s: %w", v, k, err)
		}
		if factor > 1 {
			scaled := q.DeepCopy()
			scaled.Mul(int64(factor))
			q = scaled
		}
		out[k] = q.String()
	}
	return out, nil
}

// seedClosedLoop mints the scenario's machine pools into the fake
// provider and the shard inventory, mirroring cmd/bigfleet
// seedFakeInventory's tiers, labels, and rack layout. One rack counter
// per pool spans all three tiers so contiguous blocks never collide
// across tiers (each block is its own physical rack).
func seedClosedLoop(prov *fake.Provider, sh *shard.Shard, sc *ClosedLoopScenario, shapes map[string]*shapeRT) error {
	const (
		specPricePerHour  = 1.0
		specInterruptProb = 0.05
		// A second Speculative tier (#354 cost-routing): cheaper per-hour but a
		// much higher interruption probability. BigFleet has no "spot" concept —
		// the engine routes purely on effective_cost = price + prob×penalty
		// (sortSpeculativeCandidates / EffectiveCost never read CapacityType).
		// With these two tiers the effective_cost ordering flips at penalty
		// ≈ (specPrice − interruptiblePrice)/(interruptibleProb − specInterruptProb)
		// ≈ 2.8: interruption-tolerant demand (low/zero penalty) routes to the
		// cheap/high-interruption tier; sensitive demand (high penalty) routes to
		// the stable tier. Both tiers carry the SAME CapacityType, so the proof
		// isolates cost from any type-based grouping.
		interruptibleSpecPricePerHour  = 0.3
		interruptibleSpecInterruptProb = 0.30
	)
	for pi := range sc.Seeds {
		pool := &sc.Seeds[pi]
		shape, ok := shapes[pool.Shape]
		if !ok {
			return fmt.Errorf("seed pool %d: unknown shape %q", pi, pool.Shape)
		}
		instanceType := pool.MachineInstanceType
		if instanceType == "" {
			if len(shape.spec.InstanceTypes) > 0 {
				instanceType = shape.spec.InstanceTypes[0]
			} else {
				instanceType = "sim-generic"
			}
		}
		zones := shape.spec.Zones
		if len(zones) == 0 {
			zones = defaultZones
		}
		racksPerZone := pool.RacksPerZone
		if racksPerZone <= 0 {
			racksPerZone = 4
		}
		density := pool.Density
		if density <= 0 {
			density = 1
		}
		allocatable, err := scaleQuantityMap(shape.spec.PodResources, density)
		if err != nil {
			return fmt.Errorf("seed pool %q: %w", pool.Shape, err)
		}

		counter := 0
		zoneRack := func() (string, string) {
			slot := counter
			if pool.ContiguousRackBlock > 0 {
				slot = counter / pool.ContiguousRackBlock
			}
			counter++
			zone := zones[slot%len(zones)]
			return zone, fmt.Sprintf("%s-rack-%d", zone, slot%racksPerZone)
		}
		mkProfile := func(capType machine.CapacityType) machine.Profile {
			zone, rack := zoneRack()
			return machine.Profile{
				InstanceType: instanceType,
				Zone:         zone,
				CapacityType: capType,
				Resources:    shape.spec.PodResources,
				Labels:       map[string]string{sameRackKey: rack},
			}
		}

		if pool.ConfiguredPerCluster > 0 {
			for _, cl := range sc.Clusters {
				for i := 0; i < pool.ConfiguredPerCluster; i++ {
					id := machine.ID(fmt.Sprintf("clp-%s-cfg-%s-%d", pool.Shape, cl.ID, i))
					profile := mkProfile(machine.CapacityTypeBareMetal)
					prov.AddConfigured(id, profile, machine.CapacityTypeBareMetal, 0, 0,
						cl.ID, shape.spec.Priority,
						shape.spec.InterruptionPenaltyDollars, shape.spec.ReclamationPenaltyDollars)
					prov.SetAllocatable(id, allocatable)
					// Host mirrors the fake provider's own record: the
					// inventory's Insert validates machine invariants
					// (Configured/Idle require a host), and the model's
					// step-0 pre-bind needs the inventory live before the
					// first reconcile.
					if err := sh.SeedInventory(machine.Machine{
						ID:                                 id,
						State:                              machine.StateConfigured,
						Host:                               machine.HostRef{Provider: "fake", Ref: string(id)},
						Cluster:                            cl.ID,
						Profile:                            profile,
						Allocatable:                        allocatable,
						AssignedPriority:                   shape.spec.Priority,
						AssignedInterruptionPenaltyDollars: shape.spec.InterruptionPenaltyDollars,
						AssignedReclamationPenaltyDollars:  shape.spec.ReclamationPenaltyDollars,
					}); err != nil {
						return fmt.Errorf("seed configured %s: %w", id, err)
					}
				}
			}
		}
		idleCapType := pool.IdleCapacityType
		if idleCapType == machine.CapacityTypeUnspecified {
			idleCapType = machine.CapacityTypeBareMetal
		}
		// Elastic idle carries the elastic price; fixed idle is owned
		// hardware at price 0 (the historical seed shape).
		idlePrice, idleProb := 0.0, 0.0
		if idleCapType == machine.CapacityTypeOnDemand || idleCapType == machine.CapacityTypeSpot {
			idlePrice, idleProb = specPricePerHour, specInterruptProb
		}
		for i := 0; i < pool.Idle; i++ {
			id := machine.ID(fmt.Sprintf("clp-%s-idle-%d", pool.Shape, i))
			profile := mkProfile(idleCapType)
			prov.AddIdle(id, profile, idleCapType, idlePrice, idleProb)
			prov.SetAllocatable(id, allocatable)
			if err := sh.SeedInventory(machine.Machine{
				ID:                      id,
				State:                   machine.StateIdle,
				Host:                    machine.HostRef{Provider: "fake", Ref: string(id)},
				Profile:                 profile,
				Allocatable:             allocatable,
				PricePerHour:            idlePrice,
				InterruptionProbability: idleProb,
			}); err != nil {
				return fmt.Errorf("seed idle %s: %w", id, err)
			}
		}
		for i := 0; i < pool.Speculative; i++ {
			id := machine.ID(fmt.Sprintf("clp-%s-spec-%d", pool.Shape, i))
			profile := mkProfile(machine.CapacityTypeOnDemand)
			prov.AddSpeculative(id, profile, machine.CapacityTypeOnDemand, specPricePerHour, specInterruptProb)
			prov.SetAllocatable(id, allocatable)
			if err := sh.SeedInventory(machine.Machine{
				ID:                      id,
				State:                   machine.StateSpeculative,
				Profile:                 profile,
				Allocatable:             allocatable,
				PricePerHour:            specPricePerHour,
				InterruptionProbability: specInterruptProb,
			}); err != nil {
				return fmt.Errorf("seed speculative %s: %w", id, err)
			}
		}
		// #354 cost-routing: a second Speculative tier with a profile
		// IDENTICAL to the primary tier above (same mkProfile(OnDemand) →
		// same instance-type/zone/resources/CapacityType), so both are
		// candidates for the same Need and the effective-cost sort
		// (sortSpeculativeCandidates) — which reads only price and
		// interruption probability — is the sole decider. The two tiers
		// differ only in (price, interruption probability); there is no
		// "spot" type involved. A distinct id prefix avoids colliding with
		// the primary spec ids.
		for i := 0; i < pool.InterruptibleSpeculative; i++ {
			id := machine.ID(fmt.Sprintf("clp-%s-intr-%d", pool.Shape, i))
			profile := mkProfile(machine.CapacityTypeOnDemand)
			prov.AddSpeculative(id, profile, machine.CapacityTypeOnDemand, interruptibleSpecPricePerHour, interruptibleSpecInterruptProb)
			prov.SetAllocatable(id, allocatable)
			if err := sh.SeedInventory(machine.Machine{
				ID:                      id,
				State:                   machine.StateSpeculative,
				Profile:                 profile,
				Allocatable:             allocatable,
				PricePerHour:            interruptibleSpecPricePerHour,
				InterruptionProbability: interruptibleSpecInterruptProb,
			}); err != nil {
				return fmt.Errorf("seed interruptible speculative %s: %w", id, err)
			}
		}
	}
	return nil
}

// RunClosedLoop executes a closed-loop scenario: the real shard engine
// (Phase 1/2/3 + OCC, fake provider with instant transitions) against
// the reactive cluster model. Per cycle, in order: (1) each cluster's
// rollup is derived from its live Pods and applied; (2) one real
// decision cycle runs; (3) machine-state changes are observed and the
// affected Pods evicted; (4) controllers recreate evicted Pods and
// pending Pods bind; (5) counters are recorded.
//
// Deterministic: fixed provider seed, no goroutines beyond shard.Step's
// own, no wall-clock dependence.
func RunClosedLoop(ctx context.Context, sc ClosedLoopScenario) (*ClosedLoopResult, error) {
	if sc.Cycles <= 0 {
		return nil, fmt.Errorf("closed loop %q: Cycles must be > 0", sc.Name)
	}
	space := newVecSpace(sc.Shapes)
	shapes, err := buildShapes(sc.Shapes, space)
	if err != nil {
		return nil, fmt.Errorf("closed loop %q: %w", sc.Name, err)
	}

	// BootstrapDwellCycles > 0: hold Configure at Configuring so the
	// engine observes in-flight bootstraps for N cycles (ADR-0051 / M77g).
	prov := fake.New(fake.Options{
		InstantTransitions: true,
		ConfigureStaged:    sc.BootstrapDwellCycles > 0,
		CreateStaged:       sc.ProvisionDwellCycles > 0,
		Seed:               0xC0FFEE,
	})

	tmpDir, err := os.MkdirTemp("", "bigfleet-closedloop-"+sc.Name+"-")
	if err != nil {
		return nil, fmt.Errorf("tmp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	epoch, err := fencing.LoadEpoch(filepath.Join(tmpDir, "epoch"))
	if err != nil {
		return nil, fmt.Errorf("load epoch: %w", err)
	}

	// The ADR-0046 safety rails stay at their zero values (off): the
	// canaries here pin ENGINE pathologies — mass-reclaim cascades,
	// empty-roll-up oscillations — that the rails exist to blunt;
	// running them rails-on would dampen the very signal they detect.
	// The rails have their own test surface (pkg/shard/safety_test.go).
	sh, err := shard.New(shard.Config{
		ID:               "closedloop-" + sc.Name,
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    1 * time.Second, // unused; cycles are driven via Step
		BootstrapTimeout: 1 * time.Second,
		LocalBootstrap: func(_ context.Context, cluster machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# closed-loop bootstrap for " + string(cluster) + "\n"), nil
		},
		// M73: nil keeps the paper-constant holds, which sim wall-clock
		// never crosses; release scenarios pass short holds.
		ReleasePolicy: sc.ReleasePolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("shard new: %w", err)
	}
	if err := seedClosedLoop(prov, sh, &sc, shapes); err != nil {
		return nil, fmt.Errorf("closed loop %q: %w", sc.Name, err)
	}

	models := make([]*clusterModel, 0, len(sc.Clusters))
	targetPods := 0
	for _, cl := range sc.Clusters {
		m, err := buildClusterModel(cl, shapes, space)
		if err != nil {
			return nil, fmt.Errorf("closed loop %q: %w", sc.Name, err)
		}
		models = append(models, m)
		for _, wl := range m.workloads {
			targetPods += wl.target
		}
	}

	// Initial fill: bind onto the seeded Configured machines before the
	// first decision cycle, mirroring the harness's pre-bind fill. The
	// first rollup then describes a *running* cluster, so cycle 1's
	// Phase 3 reclaims real surplus carrying real Pods — the feedback
	// edge the scripted runner can't exhibit.
	snap := sh.Inventory().Snapshot()
	for _, m := range models {
		m.bind(snap)
	}
	// ADR-0051 / M77g: stamp the gang attribution onto the seeded
	// Configured machines the initial fill bound gangs to, so a pre-bound
	// gang carries AssignedGroup the way a real fleet's serving machine
	// would. Off by default (production seeds can't know runtime gang IDs).
	if sc.StampSeededGangAttribution {
		if err := stampSeededGangAttribution(sh, models); err != nil {
			return nil, fmt.Errorf("closed loop %q: %w", sc.Name, err)
		}
	}

	res := &ClosedLoopResult{
		Cycles:     make([]CycleStats, 0, sc.Cycles),
		TargetPods: targetPods,
	}
	// dwell tracks machines the engine has bootstrapped that are still
	// Configuring: id → cycles remaining before completion (ADR-0051 /
	// M77g). Empty when BootstrapDwellCycles == 0.
	dwell := map[machine.ID]int{}
	// createDwell is the pre-Configuring twin: machines the engine has
	// Provisioned that are still Creating (Speculative→Idle in flight),
	// id → cycles remaining before Creating→Idle. Empty when
	// ProvisionDwellCycles == 0. A machine in here is INVISIBLE to its
	// gang's coverage walk and carries no AssignedGroup — the runway the
	// over-acquire turns on.
	createDwell := map[machine.ID]int{}
	for cycle := 1; cycle <= sc.Cycles; cycle++ {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		default:
		}

		// Provision-dwell: age the in-flight Creating machines and complete
		// the matured ones (Creating → Idle) before this cycle's Step, so a
		// finished host provisioning becomes acquirable Idle this cycle.
		// Runs before the bootstrap-dwell completion so a machine cannot
		// traverse both dwells in a single cycle (Creating→Idle here,
		// Idle→Configuring would be this cycle's Step, the bootstrap dwell
		// then starts next cycle) — the honest one-stage-per-cycle runway.
		completeMaturedCreateDwell(prov, createDwell)
		// Bootstrap-dwell: age the in-flight machines and complete the
		// ones whose budget elapsed (Configuring → Configured) before this
		// cycle's Step, so the engine now sees them as acquirable/serving
		// supply. Honest model of the engine's own bootstrap latency.
		completeMaturedDwell(sh, dwell)

		for _, ts := range sc.Scales {
			if ts.Cycle != cycle {
				continue
			}
			for _, m := range models {
				m.scaleTo(ts.Shape, ts.Replicas)
			}
		}

		for _, fe := range sc.Faults {
			if fe.Cycle == cycle {
				injectFaults(prov, sh, fe)
			}
		}

		for i, m := range models {
			arrival := int64(0)
			if sc.RollupArrivalStamps {
				// Each cycle every operator rolls up once; their build
				// timestamps race. Rotating the within-cycle order makes
				// the race deterministic while still exercising every
				// cross-cluster ordering — the NeedsTable's
				// equal-priority arrival-asc sort then flips the seed
				// walk's cluster interleave cycle to cycle, as it does
				// on a real shard with independent operators.
				arrival = int64(cycle)*int64(len(models)) + int64((cycle+i)%len(models)) + 1
			}
			sh.ApplyRollup(m.id, m.rollup(sc.CRPerPod, arrival))
		}

		actions := sh.Step(ctx)

		stats := CycleStats{Cycle: cycle}
		for _, a := range actions {
			switch a.Kind {
			case decision.ActionKindBootstrap:
				stats.Bootstraps++
			case decision.ActionKindProvision:
				stats.Provisions++
			case decision.ActionKindReclaim:
				stats.Reclaims++
			case decision.ActionKindPreempt:
				stats.Preempts++
			case decision.ActionKindDelete:
				stats.Deletes++
			}
		}
		// Provision-dwell: the machines this cycle's Step just Provisioned
		// (Create held them at Creating via CreateStaged) begin their
		// Creating countdown. A Provision NEVER reaches Configuring this
		// cycle (executeProvision hands off to bootstrap only when Create
		// returned Idle), so when the provision dwell is on it is recorded
		// here, not in the bootstrap dwell below.
		if sc.ProvisionDwellCycles > 0 {
			recordCreateDwellEntries(actions, createDwell, sc.ProvisionDwellCycles)
		}
		// Bootstrap-dwell: the machines this cycle's Step just bootstrapped
		// (Configure held them at Configuring via ConfigureStaged) begin
		// their dwell countdown (ADR-0051 / M77g). A Reclaim that drains a
		// still-dwelling machine cancels its countdown — the binding the
		// dwell would complete is gone. Only Bootstrap (Idle source) reaches
		// Configuring; with the provision dwell on, a Provision left the
		// machine at Creating and is excluded here.
		if sc.BootstrapDwellCycles > 0 {
			recordBootstrapDwellEntries(actions, dwell, sc.BootstrapDwellCycles, sc.ProvisionDwellCycles > 0)
		}

		snap = sh.Inventory().Snapshot()
		stats.ReclaimMatchesShortfall = reclaimMatchesShortfall(actions, snap, sh.Shortfalls())

		for _, m := range models {
			stats.Evicted += m.evictAndReconcile(snap, sc.ControllerManaged)
		}
		for _, m := range models {
			m.bind(snap)
		}

		stats.Configured = snap.CountByState(machine.StateConfigured)
		stats.Shortfalls = sh.ShortfallCount()
		for _, m := range models {
			b, p := m.podCounts()
			stats.BoundPods += b
			stats.PendingPods += p
		}
		stats.LivePods = stats.BoundPods + stats.PendingPods
		res.Cycles = append(res.Cycles, stats)
	}

	for _, m := range models {
		for _, wl := range m.workloads {
			st := WorkloadStanding{
				Cluster:  m.id,
				Workload: wl.name,
				Shape:    wl.shape.spec.Name,
				Target:   wl.target,
				Live:     len(wl.pods),
			}
			for _, p := range wl.pods {
				if p.bound != "" {
					st.Bound++
				}
			}
			res.Workloads = append(res.Workloads, st)
		}
	}
	res.FinalSnapshot = sh.Inventory().Snapshot()
	return res, nil
}

// stampSeededGangAttribution writes AssignedGroup + AssignedNeedFingerprint
// onto each seeded Configured machine a gang bound to in the initial fill
// (ADR-0051 / M77g). A same-state (Configured → Configured) inventory
// Apply, so it carries no transition. Mirrors a real fleet where the
// machine serving gang G holds G's attribution from its Configure.
func stampSeededGangAttribution(sh *shard.Shard, models []*clusterModel) error {
	inv := sh.Inventory()
	for _, m := range models {
		for _, b := range m.gangBindings() {
			cur, err := inv.Get(b.id)
			if err != nil {
				return fmt.Errorf("stamp gang attribution: get %s: %w", b.id, err)
			}
			cur.AssignedGroup = b.group
			cur.AssignedNeedFingerprint = b.fingerprint
			if err := inv.Apply(cur); err != nil {
				return fmt.Errorf("stamp gang attribution: apply %s: %w", b.id, err)
			}
		}
	}
	return nil
}

// recordBootstrapDwellEntries starts a bootstrap-dwell countdown for each
// machine this cycle's Step bootstrapped (ADR-0051 / M77g). With
// ConfigureStaged the provider left these machines at Configuring; the
// countdown is how many further cycles they stay there before
// completeMaturedDwell drives them to Configured.
//
// provisionDwellOn excludes Provision actions: when the provision dwell is
// active a Provision leaves the machine at Creating (CreateStaged), not
// Configuring, so it belongs in the create dwell, not here. With it off, a
// Provision completes Create→Idle instantly and then hands off to
// bootstrap in the same Step (executeProvision), landing at Configuring —
// so it dwells here exactly as a Bootstrap does (the historical behaviour,
// preserved byte-for-byte).
func recordBootstrapDwellEntries(actions []decision.Action, dwell map[machine.ID]int, n int, provisionDwellOn bool) {
	for _, a := range actions {
		switch a.Kind {
		case decision.ActionKindBootstrap:
			dwell[a.MachineID] = n
		case decision.ActionKindProvision:
			if !provisionDwellOn {
				dwell[a.MachineID] = n
			}
		}
	}
}

// recordCreateDwellEntries starts a create (pre-Configuring) dwell
// countdown for each machine this cycle's Step Provisioned. With
// CreateStaged the provider left these machines at Creating; the countdown
// is how many further cycles they stay there before
// completeMaturedCreateDwell drives them to Idle.
func recordCreateDwellEntries(actions []decision.Action, createDwell map[machine.ID]int, n int) {
	for _, a := range actions {
		if a.Kind == decision.ActionKindProvision {
			createDwell[a.MachineID] = n
		}
	}
}

// completeMaturedCreateDwell ages every in-flight (Creating) machine by one
// cycle and completes the ones whose budget reached zero by advancing the
// PROVIDER's staged Create to Idle (CompleteStaged). Driving the provider —
// the host-lifecycle source of truth — rather than poking inventory
// directly is what keeps the subsequent Bootstrap's provider.Configure RPC
// valid: the next cycle's reconcileFull pulls the now-Idle provider record
// into inventory through the normal forward Creating→Idle transition. (The
// bootstrap dwell pokes inventory because Configured is terminal until a
// Drain, and a backward Configured→Configuring reconcile is rejected by the
// FSM; Creating→Idle is NOT terminal — a Configure follows — so the
// provider must actually advance.) The completed machine is acquirable Idle
// with NO cluster, NO AssignedGroup — exactly the state executeProvision
// leaves it in before its later Idle→Configuring. A machine the provider no
// longer holds in Creating (a Failed injection) is dropped.
func completeMaturedCreateDwell(prov *fake.Provider, createDwell map[machine.ID]int) {
	if len(createDwell) == 0 {
		return
	}
	for id, left := range createDwell {
		m, err := prov.Get(context.Background(), id)
		if err != nil || m.State != machine.StateCreating {
			delete(createDwell, id) // provisioning abandoned or machine moved on
			continue
		}
		if left > 1 {
			createDwell[id] = left - 1
			continue
		}
		prov.CompleteStaged(id) // Creating → Idle in the provider; reconcile syncs inventory
		delete(createDwell, id)
	}
}

// completeMaturedDwell ages every in-flight (Configuring) machine by one
// cycle and completes the ones whose budget reached zero: a real forward
// Configuring → Configured transition through the inventory (ADR-0051 /
// M77g). A machine no longer Configuring (the engine rolled its bootstrap
// back to Idle, or a later Reclaim drained it) is dropped from the map —
// there is nothing to complete.
func completeMaturedDwell(sh *shard.Shard, dwell map[machine.ID]int) {
	if len(dwell) == 0 {
		return
	}
	inv := sh.Inventory()
	for id, left := range dwell {
		m, err := inv.Get(id)
		if err != nil || m.State != machine.StateConfiguring {
			delete(dwell, id) // bootstrap abandoned or machine moved on
			continue
		}
		if left > 1 {
			dwell[id] = left - 1
			continue
		}
		m.State = machine.StateConfigured
		if err := inv.Apply(m); err != nil {
			// Lost a race (e.g. drained between Get and Apply); the next
			// cycle's reconcile/scan settles it. Drop so we don't retry a
			// stale record.
			delete(dwell, id)
			continue
		}
		delete(dwell, id)
	}
}

// injectFaults removes fe.Count of the cluster's Configured machines from
// the provider (a hard host loss — terminated / spot-reclaimed node). The
// victims are chosen in deterministic ID order from the current inventory
// snapshot, so the perturbation is reproducible. The shard's next full
// reconcile detects the absent machines and removes them from inventory;
// the cluster model then evicts their Pods and the gang re-derives its
// now-uncovered deficit. (RemoveMachine, not FailMachine: the inventory FSM
// rejects an in-place Configured→Failed, so a clean removal is the
// reconcile-ingestible incumbent-loss model — see RemoveMachine.)
func injectFaults(prov *fake.Provider, sh *shard.Shard, fe FaultEvent) {
	snap := sh.Inventory().Snapshot()
	victims := snap.SortedClusterStateBucket(fe.Cluster, machine.StateConfigured)
	n := fe.Count
	for i := range victims {
		if n <= 0 {
			break
		}
		if prov.RemoveMachine(victims[i].ID) {
			n--
		}
	}
}

// reclaimMatchesShortfall is the ADR-0040 §4 probe at sim level: count
// Reclaim actions whose machine MatchProfiles a Need the shard left
// unsatisfied this cycle (the shard's shortfall tracker — recorded
// before Step returns). A non-zero steady-state value is the
// "Phase 3 reclaimed what Phase 1 wants" attribution-split signature
// that ADR-0040 closed.
func reclaimMatchesShortfall(actions []decision.Action, snap *inventory.Snapshot, shortfalls []shard.Shortfall) int {
	if len(shortfalls) == 0 {
		return 0
	}
	n := 0
	for _, a := range actions {
		if a.Kind != decision.ActionKindReclaim {
			continue
		}
		m, ok := snap.Get(a.MachineID)
		if !ok {
			continue
		}
		for i := range shortfalls {
			if decision.MatchProfile(shortfalls[i].Profile, m) {
				n++
				break
			}
		}
	}
	return n
}
