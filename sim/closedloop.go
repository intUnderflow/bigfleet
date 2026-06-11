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
	// carries a Same requirement on sameRackKey.
	SameRack bool
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
	Speculative          int // shard-wide Speculative slots
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
	return c.Bootstraps + c.Provisions + c.Reclaims + c.Preempts
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
// (resVec) forms.
type shapeRT struct {
	spec    *WorkloadShape
	profile needs.Profile
	podQty  []needs.ResourceQty
	podVec  resVec
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
		if sp.SameRack {
			reqs = append(reqs, needs.Requirement{Key: sameRackKey, Operator: needs.OperatorSame})
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
			podQty: needs.ResourceQtysFromMap(sp.PodResources),
			podVec: podVec,
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

	// rack is the gang's pinned rack: set when the first member binds,
	// cleared when no member remains bound (the gang re-anchors on
	// rebind). Mirrors the harness's anchor + rack-faithful pre-bind
	// (ADR-0040 §3) at the granularity the spec asks for.
	rack string

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
			if sh.spec.SameRack {
				wl.group = sameRackKey + "\x00" + wl.name
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
	case wl.shape.spec.SameRack:
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
func (c *clusterModel) rollup(crPerPod bool) []needs.Need {
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
		if wl.rack != "" {
			bound := false
			for _, p := range wl.pods {
				if p.bound != "" {
					bound = true
					break
				}
			}
			if !bound {
				wl.rack = ""
			}
		}
	}
	return evicted
}

// bind places pending Pods onto the cluster's Configured machines.
// Candidate filter: the shape's nodeAffinity sets, plus rack coherence
// for gangs (first member anchors the gang's rack; later members bind
// only there — a member that doesn't fit on the anchor rack stays
// pending, never scattered, mirroring planGroupOntoRack's
// "one rack or nowhere"). Among candidates the emptiest machine wins
// (most free Pod-slots; ID tiebreak) — kube-scheduler-style least-
// allocated spreading.
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
				if wl.shape.spec.SameRack {
					rack, has := m.Profile.Labels[sameRackKey]
					if !has {
						continue
					}
					if wl.rack != "" && rack != wl.rack {
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
			if wl.shape.spec.SameRack && wl.rack == "" {
				wl.rack = m.Profile.Labels[sameRackKey]
			}
		}
	}
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
		for i := 0; i < pool.Idle; i++ {
			id := machine.ID(fmt.Sprintf("clp-%s-idle-%d", pool.Shape, i))
			profile := mkProfile(machine.CapacityTypeBareMetal)
			prov.AddIdle(id, profile, machine.CapacityTypeBareMetal, 0, 0)
			prov.SetAllocatable(id, allocatable)
			if err := sh.SeedInventory(machine.Machine{
				ID:          id,
				State:       machine.StateIdle,
				Host:        machine.HostRef{Provider: "fake", Ref: string(id)},
				Profile:     profile,
				Allocatable: allocatable,
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

	prov := fake.New(fake.Options{InstantTransitions: true, Seed: 0xC0FFEE})

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

	res := &ClosedLoopResult{
		Cycles:     make([]CycleStats, 0, sc.Cycles),
		TargetPods: targetPods,
	}
	for cycle := 1; cycle <= sc.Cycles; cycle++ {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		default:
		}

		for _, m := range models {
			sh.ApplyRollup(m.id, m.rollup(sc.CRPerPod))
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
			}
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
	return res, nil
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
