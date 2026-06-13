package decision_test

// Diagnostic harness for the M77a gang claimed-set oscillation
// (commit f306319): a minimal engine-only closed loop — constant gang
// demand, inventory mutated synchronously by the engine's own actions
// — with full per-cycle visibility of every gang's chosen Same domain,
// credited supply, acquisitions, and Phase 3 strays. Pure diagnosis;
// the durable pin lives in sim/.

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

const (
	diagRackKey = "topology.bigfleet/rack"
	diagZoneKey = "topology.kubernetes.io/zone"
)

var diagZones = []string{"zone-a", "zone-b", "zone-c"}

var diagGPUResources = map[string]string{"cpu": "64", "memory": "256Gi", "nvidia.com/gpu": "8"}

func diagProfile(sameKey string, intPen, recPen float64) needs.Profile {
	reqs := []needs.Requirement{
		{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"a3-highgpu-8g"}},
		{Key: diagZoneKey, Operator: needs.OperatorIn, Values: append([]string(nil), diagZones...)},
		{Key: sameKey, Operator: needs.OperatorSame},
	}
	return needs.NewProfile(reqs, nil, 1000,
		needs.BucketForDollars(intPen), needs.BucketForDollars(recPen))
}

// diagSeedSpec seeds machines for one archetype tier.
type diagSeedSpec struct {
	prefix    string
	count     int
	state     machine.State
	cluster   machine.ClusterID // Configured only
	block     int               // contiguous rack block; 0 = round-robin
	racksPer  int
	price     float64
	intPen    float64 // Configured only
	recPen    float64 // Configured only
	counterAt *int    // shared per-pool counter across tiers/clusters
}

func diagSeed(t *testing.T, inv *inventory.Inventory, s diagSeedSpec) {
	t.Helper()
	for i := 0; i < s.count; i++ {
		slot := *s.counterAt
		if s.block > 0 {
			slot = *s.counterAt / s.block
		}
		*s.counterAt++
		zone := diagZones[slot%len(diagZones)]
		rack := fmt.Sprintf("%s-rack-%d", zone, slot%s.racksPer)
		m := machine.Machine{
			ID:    machine.ID(fmt.Sprintf("%s-%d", s.prefix, i)),
			State: s.state,
			Host:  machine.HostRef{Provider: "diag", Ref: s.prefix},
			Profile: machine.Profile{
				InstanceType: "a3-highgpu-8g",
				Zone:         zone,
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    diagGPUResources,
				Labels:       map[string]string{diagRackKey: rack},
			},
			Allocatable:  diagGPUResources,
			PricePerHour: s.price,
		}
		if s.state == machine.StateConfigured {
			m.Cluster = s.cluster
			m.AssignedPriority = 1000
			m.AssignedInterruptionPenaltyDollars = s.intPen
			m.AssignedReclamationPenaltyDollars = s.recPen
		}
		if s.state == machine.StateSpeculative {
			m.Host = machine.HostRef{}
			m.Profile.CapacityType = machine.CapacityTypeOnDemand
			m.InterruptionProbability = 0.05
		}
		if err := inv.Insert(m); err != nil {
			t.Fatalf("seed %s-%d: %v", s.prefix, i, err)
		}
	}
}

// diagApply mirrors the shard's execute semantics synchronously:
// Bootstrap/Provision walk to Configured with the Need's identity
// stamped (executeBootstrap); Reclaim walks to Idle with it cleared
// (executeDrain).
func diagApply(t *testing.T, inv *inventory.Inventory, actions []decision.Action) {
	t.Helper()
	for _, a := range actions {
		m, err := inv.Get(a.MachineID)
		if err != nil {
			t.Fatalf("apply %s on %s: %v", a.Kind, a.MachineID, err)
		}
		switch a.Kind {
		case decision.ActionKindBootstrap, decision.ActionKindProvision:
			if m.State == machine.StateSpeculative {
				// Provision walks Speculative → Creating → Idle first
				// (executeProvision), then hands off to bootstrap.
				m.State = machine.StateCreating
				if err := inv.Apply(m); err != nil {
					t.Fatalf("apply creating %s: %v", a.MachineID, err)
				}
				m.State = machine.StateIdle
				m.Host = machine.HostRef{Provider: "diag", Ref: string(a.MachineID)}
				if err := inv.Apply(m); err != nil {
					t.Fatalf("apply created-idle %s: %v", a.MachineID, err)
				}
			}
			m.State = machine.StateConfiguring
			m.Cluster = a.Cluster
			m.Host = machine.HostRef{Provider: "diag", Ref: string(a.MachineID)}
			m.AssignedNeedFingerprint = a.SourceProfile.Fingerprint()
			if err := inv.Apply(m); err != nil {
				t.Fatalf("apply configuring %s: %v", a.MachineID, err)
			}
			m.State = machine.StateConfigured
			m.Host = machine.HostRef{Provider: "diag", Ref: string(a.MachineID)}
			m.AssignedPriority = a.SourceProfile.Priority()
			m.AssignedInterruptionPenaltyDollars = decision.BucketUpperBoundDollars(a.SourceProfile.InterruptionPenaltyBucket())
			m.AssignedReclamationPenaltyDollars = decision.BucketUpperBoundDollars(a.SourceProfile.ReclamationPenaltyBucket())
			if err := inv.Apply(m); err != nil {
				t.Fatalf("apply configured %s: %v", a.MachineID, err)
			}
		case decision.ActionKindReclaim:
			m.State = machine.StateDraining
			if err := inv.Apply(m); err != nil {
				t.Fatalf("apply draining %s: %v", a.MachineID, err)
			}
			m.State = machine.StateIdle
			m.Cluster = ""
			m.AssignedPriority = 0
			m.AssignedInterruptionPenaltyDollars = 0
			m.AssignedReclamationPenaltyDollars = 0
			m.AssignedNeedFingerprint = ""
			if err := inv.Apply(m); err != nil {
				t.Fatalf("apply idle %s: %v", a.MachineID, err)
			}
		}
	}
}

// diagSeedReplica replays SeedConfiguredSupply's Same arm read-only on
// a snapshot to expose each gang's bucket ranking — the forensics the
// engine doesn't (and shouldn't) export. Must mirror seedSameProfile
// exactly; drift is flagged by comparing chosen domains against the
// real cycle's results.
type diagSeedReplica struct {
	snap       *inventory.Snapshot
	acquirable *occ.SameSupplyIndex
	claimed    map[machine.ID]struct{}
	consumed   map[machine.ID]struct{}
}

func newDiagSeedReplica(snap *inventory.Snapshot) *diagSeedReplica {
	return &diagSeedReplica{
		snap:       snap,
		acquirable: occ.NewSameSupplyIndex(snap),
		claimed:    map[machine.ID]struct{}{},
		consumed:   map[machine.ID]struct{}{},
	}
}

// diagBucketLine is one bucket's view at one gang's walk turn.
type diagBucketLine struct {
	value      string
	creditable int
	acqCount   int
	total      []int64
	sat        bool
	chosen     bool
}

func diagMachineSameValue(m *machine.Machine, sameKey string) (string, bool) {
	if sameKey == diagZoneKey {
		return m.Profile.Zone, m.Profile.Zone != ""
	}
	v, ok := m.Profile.Labels[sameKey]
	return v, ok
}

// walkNeed advances the replica through one Same Need: builds the
// joint buckets exactly as seedSameProfile does, chooses via the real
// ChooseSameBucket, claims the creditable members, consumes acquirable
// on residual. Returns the chosen domain and the full ranking view.
func (r *diagSeedReplica) walkNeed(n *needs.Need) (string, []diagBucketLine) {
	sameKey, _ := occ.SameRequirementKey(n.Profile)
	minUnitVec := r.acquirable.ParseVec(n.MinUnit)
	index := map[string]int{}
	var buckets []occ.SameBucket
	var members [][]machine.Machine
	var memberAllocs [][][]needs.ResourceQty
	for _, st := range []machine.State{machine.StateConfigured, machine.StateConfiguring} {
		for _, m := range r.snap.SortedClusterStateBucket(n.ClusterID, st) {
			if _, ok := r.claimed[m.ID]; ok {
				continue
			}
			if !decision.MatchProfile(n.Profile, m) {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			vec := r.acquirable.ParseVec(alloc)
			if !occ.VecCovers(vec, minUnitVec) {
				continue
			}
			v, ok := diagMachineSameValue(&m, sameKey)
			if !ok {
				continue
			}
			i, exists := index[v]
			if !exists {
				i = len(buckets)
				index[v] = i
				buckets = append(buckets, occ.SameBucket{Value: v})
				members = append(members, nil)
				memberAllocs = append(memberAllocs, nil)
			}
			buckets[i].Count++
			buckets[i].CreditableCount++
			buckets[i].Total = occ.VecAdd(buckets[i].Total, vec)
			buckets[i].CreditableTotal = occ.VecAdd(buckets[i].CreditableTotal, vec)
			members[i] = append(members[i], m)
			memberAllocs[i] = append(memberAllocs[i], alloc)
		}
	}
	unavailable := func(id machine.ID) bool {
		if _, ok := r.consumed[id]; ok {
			return true
		}
		_, ok := r.claimed[id]
		return ok
	}
	acq := r.acquirable.AcquirableTotals(n.Profile, sameKey, n.MinUnit, unavailable)
	for v, ab := range acq {
		i, exists := index[v]
		if !exists {
			i = len(buckets)
			index[v] = i
			buckets = append(buckets, occ.SameBucket{Value: v})
			members = append(members, nil)
			memberAllocs = append(memberAllocs, nil)
		}
		buckets[i].Count += ab.Count
		buckets[i].Total = occ.VecAdd(buckets[i].Total, ab.Total)
	}
	deficitVec := r.acquirable.ParseVec(n.AggregateResources)
	best := occ.ChooseSameBucket(buckets, deficitVec)

	lines := make([]diagBucketLine, 0, len(buckets))
	for i := range buckets {
		lines = append(lines, diagBucketLine{
			value:      buckets[i].Value,
			creditable: buckets[i].CreditableCount,
			acqCount:   buckets[i].Count - buckets[i].CreditableCount,
			total:      buckets[i].Total,
			sat:        occ.VecCovers(buckets[i].Total, deficitVec),
			chosen:     i == best,
		})
	}
	sort.Slice(lines, func(a, b int) bool { return lines[a].value < lines[b].value })
	if best < 0 {
		return "", lines
	}
	remaining := n.AggregateResources
	for mi, m := range members[best] {
		if needs.IsZero(remaining) {
			break
		}
		r.claimed[m.ID] = struct{}{}
		remaining = needs.SubResources(remaining, memberAllocs[best][mi])
	}
	if !needs.IsZero(remaining) {
		r.acquirable.ConsumeAcquirable(n.Profile, sameKey, buckets[best].Value, n.MinUnit, remaining, r.consumed)
	}
	return buckets[best].Value, lines
}

// gangLoopCfg is one engine-loop scenario: per-cluster gang draws and
// the seeded machine pools they run against.
type gangLoopCfg struct {
	name string

	smallSizes  []int // Same(rack) gang sizes per cluster
	mediumSizes []int // Same(zone) gang sizes per cluster

	smallCfg, mediumCfg   int // Configured seeded per cluster
	smallBlock            int // contiguous rack block (rack gangs)
	mediumBlock           int // 0 = zone round-robin (the harness default for sameZone)
	smallIdle, mediumIdle int
	smallSpec, mediumSpec int
	racksPer              int

	settleBudget int // cycles allowed to absorb the seed's misalignment
}

// TestGangSteadyDemandFixedPoint_EngineLoop pins the M77a gang
// claimed-set fixed point at engine granularity: constant gang demand,
// inventory mutated only by the engine's own actions, cross-cluster
// walk order alternating every cycle (the rollup arrival race a real
// shard sees — conv.NeedsFromRollup stamps every row with the message
// timestamp). After a bounded settling window the engine must be
// inert: zero Bootstrap/Provision/Reclaim, every cycle, under
// continuing order alternation.
//
// The walkNeed replica above doubles as the forensics instrument: on
// any domain flip the gang's full bucket ranking is dumped, so a
// regression arrives with the rule that caused it attached.
func TestGangSteadyDemandFixedPoint_EngineLoop(t *testing.T) {
	cfgs := []gangLoopCfg{
		{
			// The dev-50 base shape: aligned seeds (blocks tile gangs),
			// shared racks between the small and medium pools. Pre-fix
			// this sustained a two-cycle cross-class steal loop under
			// order alternation (see ChooseSameBucket's rule-2 history).
			name:       "aligned",
			smallSizes: []int{3, 3}, mediumSizes: []int{6, 6},
			smallCfg: 6, mediumCfg: 12, smallBlock: 3, mediumBlock: 6,
			smallIdle: 3, mediumIdle: 3, smallSpec: 6, mediumSpec: 6,
			racksPer: 4, settleBudget: 4,
		},
		{
			// The dev-50 field shape at ADR-0044 arithmetic: excess
			// Configured per archetype, zone-round-robin medium seeds,
			// per-zone floors, blocks that don't tile the drawn gang
			// sizes. Pre-fix: the lockstep Bootstrap≈Reclaim ping-pong.
			name:       "field",
			smallSizes: []int{2, 4, 3, 2, 4, 3}, mediumSizes: []int{4, 8, 6, 5},
			smallCfg: 24, mediumCfg: 27, smallBlock: 4,
			smallIdle: 12, mediumIdle: 24, smallSpec: 24, mediumSpec: 40,
			racksPer: 10, settleBudget: 10,
		},
	}
	for _, cfg := range cfgs {
		cfg := cfg
		t.Run(cfg.name, func(t *testing.T) { runGangEngineLoop(t, cfg) })
	}
}

func runGangEngineLoop(t *testing.T, cfg gangLoopCfg) {
	inv := inventory.New()

	smallProfile := diagProfile(diagRackKey, 16384, 32768)
	mediumProfile := diagProfile(diagZoneKey, 32768, 65536)

	// Seed: per-pool counters span tiers and clusters like the sim's
	// seedClosedLoop / the harness's per-archetype rack counters.
	// Mediums (Same(zone)) seed round-robin: seedZoneRack only blocks
	// sameRack archetypes.
	smallCtr, mediumCtr := 0, 0
	clusters := []machine.ClusterID{"c1", "c2"}
	for _, cl := range clusters {
		diagSeed(t, inv, diagSeedSpec{prefix: "cfg-small-" + string(cl), count: cfg.smallCfg,
			state: machine.StateConfigured, cluster: cl, block: cfg.smallBlock, racksPer: cfg.racksPer,
			intPen: 16384, recPen: 32768, counterAt: &smallCtr})
		diagSeed(t, inv, diagSeedSpec{prefix: "cfg-medium-" + string(cl), count: cfg.mediumCfg,
			state: machine.StateConfigured, cluster: cl, block: 0, racksPer: cfg.racksPer,
			intPen: 32768, recPen: 65536, counterAt: &mediumCtr})
	}
	diagSeed(t, inv, diagSeedSpec{prefix: "idle-small", count: cfg.smallIdle,
		state: machine.StateIdle, block: cfg.smallBlock, racksPer: cfg.racksPer, counterAt: &smallCtr})
	diagSeed(t, inv, diagSeedSpec{prefix: "idle-medium", count: cfg.mediumIdle,
		state: machine.StateIdle, block: 0, racksPer: cfg.racksPer, counterAt: &mediumCtr})
	diagSeed(t, inv, diagSeedSpec{prefix: "spec-small", count: cfg.smallSpec,
		state: machine.StateSpeculative, block: cfg.smallBlock, racksPer: cfg.racksPer, price: 1, counterAt: &smallCtr})
	diagSeed(t, inv, diagSeedSpec{prefix: "spec-medium", count: cfg.mediumSpec,
		state: machine.StateSpeculative, block: 0, racksPer: cfg.racksPer, price: 1, counterAt: &mediumCtr})

	// Demand: constant gang rows.
	smallSizes := cfg.smallSizes
	mediumSizes := cfg.mediumSizes
	gpuQty := needs.ResourceQtysFromMap(diagGPUResources)
	baseDemand := make([]needs.Need, 0, 2*(len(smallSizes)+len(mediumSizes)))
	for _, cl := range clusters {
		for gi, n := range smallSizes {
			baseDemand = append(baseDemand, needs.Need{
				ClusterID:          cl,
				Profile:            smallProfile,
				AggregateResources: needs.ScaleResources(gpuQty, n),
				MinUnit:            gpuQty,
				Group:              fmt.Sprintf("%s\x00%s-small-%d", diagRackKey, cl, gi),
			})
		}
		for gi, n := range mediumSizes {
			baseDemand = append(baseDemand, needs.Need{
				ClusterID:          cl,
				Profile:            mediumProfile,
				AggregateResources: needs.ScaleResources(gpuQty, n),
				MinUnit:            gpuQty,
				Group:              fmt.Sprintf("%s\x00%s-medium-%d", diagZoneKey, cl, gi),
			})
		}
	}

	const cycles = 40
	prevDomain := map[string]string{}
	for cycle := 1; cycle <= cycles; cycle++ {
		snap := inv.Snapshot()

		// Cross-cluster arrival race: rotate which cluster's rollup is
		// older, mirroring conv.NeedsFromRollup's per-message stamps.
		demand := make([]needs.Need, len(baseDemand))
		copy(demand, baseDemand)
		for i := range demand {
			ci := 0
			if demand[i].ClusterID == "c2" {
				ci = 1
			}
			demand[i].ArrivalUnixNanos = int64(cycle)*2 + int64((cycle+ci)%2) + 1
		}
		sort.SliceStable(demand, func(a, b int) bool {
			if demand[a].ArrivalUnixNanos != demand[b].ArrivalUnixNanos {
				return demand[a].ArrivalUnixNanos < demand[b].ArrivalUnixNanos
			}
			return demand[a].ClusterID < demand[b].ClusterID
		})

		norm := decision.NormalizeDemand(snap, demand)

		// Replica walk for forensics: per-gang bucket rankings at the
		// gang's walk turn, mirroring seedSameProfile.
		replica := newDiagSeedReplica(snap)
		replicaDomain := map[string]string{}
		replicaLines := map[string][]diagBucketLine{}
		for i := range norm {
			n := &norm[i]
			if _, ok := occ.SameRequirementKey(n.Profile); !ok {
				continue
			}
			dom, lines := replica.walkNeed(n)
			key := string(n.ClusterID) + "/" + n.Group
			replicaDomain[key] = dom
			replicaLines[key] = lines
		}

		cyc := occ.RunCycle(snap, norm)

		var actions []decision.Action
		boots, provs := 0, 0
		flips := 0
		for i := range cyc.Results {
			r := &cyc.Results[i]
			if r.Need == nil {
				continue
			}
			profile := r.Need.Profile
			for _, mid := range r.BootstrapMachines {
				actions = append(actions, decision.Action{
					Kind: decision.ActionKindBootstrap, MachineID: mid,
					Cluster: r.Need.ClusterID, SourceProfile: &profile})
				boots++
			}
			for _, mid := range r.ProvisionMachines {
				actions = append(actions, decision.Action{
					Kind: decision.ActionKindProvision, MachineID: mid,
					Cluster: r.Need.ClusterID, SourceProfile: &profile})
				provs++
			}
			key := string(r.Need.ClusterID) + "/" + r.Need.Group
			if r.SameDomain != "" {
				if rd, ok := replicaDomain[key]; ok && rd != r.SameDomain {
					t.Logf("cycle %2d: REPLICA DRIFT %s replica=%s real=%s", cycle, shortGroup(key), rd, r.SameDomain)
				}
				if prev, ok := prevDomain[key]; ok && prev != r.SameDomain {
					flips++
					t.Logf("cycle %2d: FLIP %-22s %-10s -> %-10s acq=%d unsat=%v",
						cycle, shortGroup(key), prev, r.SameDomain,
						len(r.BootstrapMachines)+len(r.ProvisionMachines), r.Unsatisfied)
					for _, l := range replicaLines[key] {
						if !l.chosen && l.value != prev && l.creditable == 0 {
							continue // only the interesting buckets
						}
						mark := " "
						if l.chosen {
							mark = "*"
						}
						old := " "
						if l.value == prev {
							old = "<"
						}
						t.Logf("        %s%s %-14s cred=%-2d acq=%-3d total=%v sat=%v",
							mark, old, l.value, l.creditable, l.acqCount, l.total, l.sat)
					}
				}
				prevDomain[key] = r.SameDomain
			}
		}

		p3 := decision.Phase3(snap, cyc.Claimed, decision.AlwaysReady, decision.DefaultReleasePolicy(), time.Now())
		recls := 0
		for _, a := range p3.Actions {
			if a.Kind == decision.ActionKindReclaim {
				recls++
				actions = append(actions, a)
			}
		}

		t.Logf("cycle %2d: boot=%d prov=%d recl=%d flips=%d", cycle, boots, provs, recls, flips)
		diagApply(t, inv, actions)
	}
}

// shortGroup trims the NUL-separated group key for log lines.
func shortGroup(key string) string {
	out := make([]rune, 0, len(key))
	for _, r := range key {
		if r == 0 {
			r = '|'
		}
		out = append(out, r)
	}
	s := string(out)
	if len(s) > 40 {
		s = s[len(s)-40:]
	}
	return s
}
