package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Tests for Phase 1's TopologySpread (DoNotSchedule + MaxSkew)
// enforcement. ScheduleAnyway is intentionally unenforced — it
// represents a soft preference and the standard cheapest-first
// behaviour approximates it well enough for v1.

func gpuProfileWithSpread(priority int32, key string, maxSkew int32, action needs.WhenUnsatisfiable) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"a3-highgpu-8g"},
		}},
		nil,
		[]needs.TopologySpread{{
			TopologyKey:       key,
			MaxSkew:           maxSkew,
			WhenUnsatisfiable: action,
		}},
		priority,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
}

// MaxSkew=1 + DoNotSchedule across 3 zones, want 6 machines: must be
// distributed exactly 2/2/2.
func TestPhase1_Spread_MaxSkew1_DistributesEvenly(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for _, z := range []string{"zone-a", "zone-b", "zone-c"} {
		for i := 0; i < 4; i++ {
			_ = inv.Insert(gpuMachineInZone(machine.ID(z+"-"+strconv.Itoa(i)), z, 1.0))
		}
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSpread(1_000_000, "topology.kubernetes.io/zone", 1, needs.WhenUnsatisfiableDoNotSchedule),
		Count:     6,
	}})

	if got := len(r.Actions); got != 6 {
		t.Fatalf("actions = %d, want 6", got)
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zones[m.Profile.Zone]++
	}
	if zones["zone-a"] != 2 || zones["zone-b"] != 2 || zones["zone-c"] != 2 {
		t.Errorf("expected 2 per zone, got %+v", zones)
	}
}

// MaxSkew=1 + DoNotSchedule with insufficient supply in one zone:
// the deficient zone limits the rest. 3 zones; zone-a has 1, others
// have plenty. Want 6 machines. Distribution must satisfy max-min ≤ 1
// — i.e., zone-a=1, others can each grow to 2 before the constraint
// would be violated. So 1+2+2 = 5 picks possible; sixth would push
// some bucket to 3 with min=1, max-min=2 > 1.
func TestPhase1_Spread_MaxSkew1_RespectsConstraintWhenSupplyUneven(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(gpuMachineInZone("a-0", "zone-a", 1.0))
	for i := 0; i < 5; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 1.0))
		_ = inv.Insert(gpuMachineInZone(machine.ID("c-"+strconv.Itoa(i)), "zone-c", 1.0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSpread(1_000_000, "topology.kubernetes.io/zone", 1, needs.WhenUnsatisfiableDoNotSchedule),
		Count:     6,
	}})

	if got := len(r.Actions); got != 5 {
		t.Errorf("actions = %d, want 5 (constrained by MaxSkew=1)", got)
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zones[m.Profile.Zone]++
	}
	max, min := -1, -1
	for _, c := range zones {
		if max == -1 || c > max {
			max = c
		}
		if min == -1 || c < min {
			min = c
		}
	}
	if max-min > 1 {
		t.Errorf("MaxSkew=1 violated: distribution = %+v (max-min=%d)", zones, max-min)
	}
	if r.Unsatisfied[0].Deficit != 1 {
		t.Errorf("deficit = %d, want 1", r.Unsatisfied[0].Deficit)
	}
}

// MaxSkew=2 + DoNotSchedule: more relaxed. 2 zones × 5 each, want 8 —
// distribution can be 4/4 or 5/3 or 3/5 (max-min=2). Greedy picks
// would give 4/4 (perfectly balanced, cheapest-first within ties).
func TestPhase1_Spread_MaxSkew2_AllowsLargerSkew(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 5; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 1.0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSpread(1_000_000, "topology.kubernetes.io/zone", 2, needs.WhenUnsatisfiableDoNotSchedule),
		Count:     8,
	}})

	if got := len(r.Actions); got != 8 {
		t.Fatalf("actions = %d, want 8", got)
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zones[m.Profile.Zone]++
	}
	max, min := -1, -1
	for _, c := range zones {
		if max == -1 || c > max {
			max = c
		}
		if min == -1 || c < min {
			min = c
		}
	}
	if max-min > 2 {
		t.Errorf("MaxSkew=2 violated: distribution = %+v (max-min=%d)", zones, max-min)
	}
}

// ScheduleAnyway: spread is best-effort, no enforcement. With 6 in
// zone-a (cheap) and 2 in zone-b (expensive), demand of 4 should pick
// the 4 cheapest — all from zone-a — since the soft constraint
// doesn't reject cost-optimal picks.
func TestPhase1_Spread_ScheduleAnyway_DoesNotEnforce(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 6; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
	}
	for i := 0; i < 2; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 5.0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSpread(1_000_000, "topology.kubernetes.io/zone", 1, needs.WhenUnsatisfiableScheduleAnyway),
		Count:     4,
	}})

	if got := len(r.Actions); got != 4 {
		t.Fatalf("actions = %d, want 4", got)
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zones[m.Profile.Zone]++
	}
	if zones["zone-a"] != 4 {
		t.Errorf("expected all 4 from zone-a (ScheduleAnyway is soft, cheapest-first wins): %+v", zones)
	}
}

// Cheapest-first within the constraint: with all zones balanced and
// available, the allocator should still prefer cheaper buckets when
// the eligibility envelope ties.
func TestPhase1_Spread_PrefersCheaperBucketWithinSkew(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// zone-a is cheap (1.0), zone-b is expensive (5.0). 3 each.
	for i := 0; i < 3; i++ {
		_ = inv.Insert(gpuMachineInZone(machine.ID("a-"+strconv.Itoa(i)), "zone-a", 1.0))
		_ = inv.Insert(gpuMachineInZone(machine.ID("b-"+strconv.Itoa(i)), "zone-b", 5.0))
	}
	snap := inv.Snapshot()

	// MaxSkew=1: alternates a, b, a, b — 2/2.
	r := decision.Phase1(snap, []needs.Need{{
		ClusterID: "cluster-x",
		Profile:   gpuProfileWithSpread(1_000_000, "topology.kubernetes.io/zone", 1, needs.WhenUnsatisfiableDoNotSchedule),
		Count:     4,
	}})
	if got := len(r.Actions); got != 4 {
		t.Fatalf("actions = %d, want 4", got)
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zones[m.Profile.Zone]++
	}
	if zones["zone-a"] != 2 || zones["zone-b"] != 2 {
		t.Errorf("expected 2 per zone (MaxSkew=1, even count), got %+v", zones)
	}
}
