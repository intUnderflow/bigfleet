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
		nil, nil, priority,
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

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		Count:     3,
	}})

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

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		Count:     4,
	}})

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

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		Count:     5,
	}})

	if got := len(r.Actions); got != 3 {
		t.Errorf("actions = %d, want 3 (largest single zone)", got)
	}
	if len(r.Unsatisfied) != 1 {
		t.Fatalf("unsatisfied = %d, want 1", len(r.Unsatisfied))
	}
	if r.Unsatisfied[0].Deficit != 2 {
		t.Errorf("deficit = %d, want 2", r.Unsatisfied[0].Deficit)
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
		[]needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}},
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

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileSameWithRes(1_000_000, "topology.kubernetes.io/zone"),
		Count:     8,
	}})

	if got := len(r.Actions); got != 1 {
		t.Fatalf("actions = %d, want 1 (density-100: one machine covers a group of 8). Pre-fix this took 8 — the over-consumption bug.", got)
	}
	if len(r.Unsatisfied) != 0 {
		t.Errorf("unsatisfied = %d, want 0", len(r.Unsatisfied))
	}
}

// Two Needs with Same in the same cluster; first claims one zone fully,
// the second must pick another zone (cross-Need claim coordination).
func TestPhase1_Same_TwoNeedsLandInDifferentZones(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 1.0))
	}
	snap := inv.Snapshot()

	// Two distinct co-location groups; high-priority wins first.
	hi := gpuProfileWithSame(2_000_000, "topology.kubernetes.io/zone")
	lo := gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone")
	r := decision.Phase1(snap, []needs.Need{
		{ClusterID: "cluster-x", Profile: hi, Count: 3, Group: "owner-A"},
		{ClusterID: "cluster-x", Profile: lo, Count: 3, Group: "owner-B"},
	})

	if got := len(r.Actions); got != 6 {
		t.Fatalf("actions = %d, want 6", got)
	}
	zonesByCount := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zonesByCount[m.Profile.Zone]++
	}
	if zonesByCount["zone-a"] != 3 || zonesByCount["zone-b"] != 3 {
		t.Errorf("expected 3 in each zone, got %+v", zonesByCount)
	}
}
