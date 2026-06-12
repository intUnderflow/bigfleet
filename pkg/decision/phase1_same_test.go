package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Tests for Phase 1's Same-operator co-location enforcement (paper §8).
// A Need whose Profile carries a Same(topologyKey) requirement must
// have all its picked machines share the topology key value.

func gpuProfileWithSame(priority int32, sameKey string) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{
			{
				Key:      "node.kubernetes.io/instance-type",
				Operator: needs.OperatorIn,
				Values:   []string{"a3-highgpu-8g"},
			},
			{
				Key:      sameKey,
				Operator: needs.OperatorSame,
			},
		},
		nil, priority,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
}

func gpuMachineInZone(id machine.ID, zone string, price float64) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: machine.StateIdle,
		Host:  machine.HostRef{Provider: "fake", Ref: string(id)},
		Profile: machine.Profile{
			InstanceType: "a3-highgpu-8g",
			Zone:         zone,
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    map[string]string{"nvidia.com/gpu": "8"},
		},
		PricePerHour: price,
	}
}

// 6 machines spread across two zones (3 per zone). Need wants 3 with
// Same(zone) — should pick 3 from a single zone.
func TestPhase1_Same_PicksOneZone(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 1.0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		3,
	)})

	if got := len(r.Actions); got != 3 {
		t.Fatalf("actions = %d, want 3", got)
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, ok := snap.Get(a.MachineID)
		if !ok {
			t.Fatalf("snap missing %s", a.MachineID)
		}
		zones[m.Profile.Zone]++
	}
	if len(zones) != 1 {
		t.Errorf("picks span %d zones, want 1 (Same enforcement broken): %+v", len(zones), zones)
	}
}

// 4 machines in zone-a, 2 in zone-b. Need wants 4 with Same(zone) —
// must pick zone-a (only atomic-satisfiable choice).
func TestPhase1_Same_PicksAtomicSatisfiableZone(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
	}
	for i := 0; i < 2; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 1.0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		4,
	)})

	if got := len(r.Actions); got != 4 {
		t.Fatalf("actions = %d, want 4", got)
	}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		if m.Profile.Zone != "zone-a" {
			t.Errorf("picked machine in %s, want zone-a", m.Profile.Zone)
		}
	}
}

// No single zone can satisfy a Need of 5 (3 + 3 across two zones). Pick
// the largest bucket (3) and surface a Deficit of 2 as Unsatisfied.
func TestPhase1_Same_FallsBackToLargestBucketWithShortfall(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 1.0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		5,
	)})

	if got := len(r.Actions); got != 3 {
		t.Errorf("actions = %d, want 3 (largest single zone)", got)
	}
	if len(r.Unsatisfied) != 1 {
		t.Fatalf("unsatisfied = %d, want 1", len(r.Unsatisfied))
	}
	if gpuQty(r.Unsatisfied[0].Deficit) != "16" {
		t.Errorf("deficit nvidia.com/gpu = %q, want 16 (2 units)", gpuQty(r.Unsatisfied[0].Deficit))
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zones[m.Profile.Zone]++
	}
	if len(zones) != 1 {
		t.Errorf("picks span %d zones, want 1: %+v", len(zones), zones)
	}
}

// gpuMachineDense returns an Idle GPU machine with density-`density`
// Allocatable (densityMultiplier × the per-replica Resources). Profile
// resources stay at the per-replica shape so MatchProfile's exact-
// equality check passes against a Need asking for the same shape.
func gpuMachineDense(id machine.ID, zone string, density int) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: machine.StateIdle,
		Host:  machine.HostRef{Provider: "fake", Ref: string(id)},
		Profile: machine.Profile{
			InstanceType: "a3-highgpu-8g",
			Zone:         zone,
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    map[string]string{"nvidia.com/gpu": "8"},
		},
		Allocatable:  map[string]string{"nvidia.com/gpu": strconv.Itoa(8 * density)},
		PricePerHour: 1.0,
	}
}

func gpuProfileSameWithRes(priority int32, sameKey string) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"a3-highgpu-8g"}},
			{Key: sameKey, Operator: needs.OperatorSame},
		},
		nil, priority,
		needs.PenaltyBucket8192, needs.PenaltyBucketPinned,
	)
}

// Regression for the M45.1 machinesNeeded density bug. A co-located
// Need of Count=N against a density-D idle pool must take only
// ceil(N/D) machines — not N. Pre-fix, phase1_assign computed
// MachinesForAggregate(profResources, profResources, …), always
// density 1, so takeCoLocated was asked for N machines and drained the
// scarce co-located pool dry — the ADR-0024 sameRack-shortfall root
// cause. Here: 8 density-100 machines, a group of 8 Pods → exactly one
// machine covers it.
func TestPhase1_Same_DensityAwareMachineCount(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 8; i++ {
		_ = inv.Insert(gpuMachineDense(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 100))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileSameWithRes(1_000_000, "topology.kubernetes.io/zone"),
		8,
	)})

	if got := len(r.Actions); got != 1 {
		t.Fatalf("actions = %d, want 1 (density-100: one machine covers a group of 8). Pre-fix this took 8 — the over-consumption bug.", got)
	}
	if len(r.Unsatisfied) != 0 {
		t.Errorf("unsatisfied = %d, want 0", len(r.Unsatisfied))
	}
}

// Two Needs with Same in the same cluster, both acquirable-only.
// ADR-0040 Addendum: each Need's domain is chosen once in the
// pre-pass over joint potential. The ADR-0041 consumption ledger —
// hoisted into the shared seed walk by ADR-0045 — makes the choice
// sequence-aware: hi (seeded first, priority order) anchors zone-a
// (tie-broken smallest value) and virtually consumes its acquirable
// members, so lo's joint view no longer contains zone-a and it
// anchors zone-b in the SAME cycle. Pre-ledger both gangs anchored
// zone-a, hi won it, and lo parked a cycle before re-anchoring —
// one wasted cycle per contended domain. Either way, lo must never
// scatter across zones.
func TestPhase1_Same_TwoNeedsLandInDifferentZones(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 1.0))
	}

	// Two distinct co-location groups; high-priority seeds first.
	hi := gpuProfileWithSame(2_000_000, "topology.kubernetes.io/zone")
	lo := gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone")
	hiNeed := needs.Need{ClusterID: "cluster-x", Profile: hi, AggregateResources: needs.ScaleResources(gpuUnit, 3), MinUnit: gpuUnit, Group: "owner-A"}
	loNeed := needs.Need{ClusterID: "cluster-x", Profile: lo, AggregateResources: needs.ScaleResources(gpuUnit, 3), MinUnit: gpuUnit, Group: "owner-B"}

	snap := inv.Snapshot()
	r := decision.Phase1(snap, []needs.Need{hiNeed, loNeed})

	// Cycle 1: both gangs land, each whole within its own zone — hi in
	// zone-a (smallest-value tie-break), lo in zone-b (zone-a consumed
	// from its joint view).
	if got := len(r.Actions); got != 6 {
		t.Fatalf("cycle 1 actions = %d, want 6 (both gangs, one zone each)", got)
	}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		wantZone := "zone-a"
		if a.SourceProfile.Priority() == lo.Priority() {
			wantZone = "zone-b"
		}
		if m.Profile.Zone != wantZone {
			t.Errorf("cycle 1 picked %s in %s for priority %d, want %s (gangs must not scatter or contend)",
				a.MachineID, m.Profile.Zone, a.SourceProfile.Priority(), wantZone)
		}
	}
	if len(r.Unsatisfied) != 0 {
		t.Fatalf("cycle 1 unsatisfied = %+v, want none (both gangs placed)", r.Unsatisfied)
	}

	// Apply the bootstraps; cycle 2 credits both serving zones and
	// emits nothing — steady state.
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		stepInventory(t, inv, a.MachineID, machine.StateConfiguring, m.Host, a.Cluster)
		stepInventory(t, inv, a.MachineID, machine.StateConfigured, m.Host, a.Cluster)
	}
	snap2 := inv.Snapshot()
	r2 := decision.Phase1(snap2, []needs.Need{hiNeed, loNeed})
	if got := len(r2.Actions); got != 0 {
		t.Fatalf("cycle 2 actions = %d, want 0 (both gangs credited where they landed)", got)
	}
	if len(r2.Unsatisfied) != 0 {
		t.Errorf("cycle 2 unsatisfied = %+v, want none (both gangs placed)", r2.Unsatisfied)
	}
}

// ADR-0041 rider 3, Phase 1 side: a Same Need already served by a
// satisfiable Configured domain stays there even when a fresh
// acquirable-only domain has a smaller satisfiable total. Pre-rider
// the smallest-satisfiable rule chose the 2-Idle zone-b, credited
// nothing, and bootstrapped a relocation Phase 3 would then unwind —
// the cycle-stability hole the rider closes. The serving domain's
// excess third machine is Phase 3's business, not Phase 1's: the
// pre-pass simply credits two and emits nothing.
func TestPhase1_Same_PrefersServingCreditableDomain(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		m := gpuMachineInZone("a-"+idN(i), "zone-a", 1.0)
		m.State = machine.StateConfigured
		m.Cluster = "cluster-x"
		_ = inv.Insert(m)
	}
	for i := 0; i < 2; i++ {
		_ = inv.Insert(gpuMachineInZone("b-"+idN(i), "zone-b", 1.0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		2,
	)})

	if got := len(r.Actions); got != 0 {
		t.Fatalf("actions = %d, want 0 (the serving zone-a credits the Need; no relocation into zone-b): %+v", got, r.Actions)
	}
	if got := len(r.Unsatisfied); got != 0 {
		t.Errorf("unsatisfied = %+v, want none", r.Unsatisfied)
	}
}
