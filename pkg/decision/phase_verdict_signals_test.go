package decision_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// findVerdict returns the Phase-1 verdict for the given cluster, or nil.
func findVerdict(vs []decision.NeedVerdict, cluster machine.ClusterID) *decision.NeedVerdict {
	for i := range vs {
		if vs[i].Need != nil && vs[i].Need.ClusterID == cluster {
			return &vs[i]
		}
	}
	return nil
}

// ADR-0061: Phase1Result.Verdicts must carry an entry for EVERY Need —
// the satisfied majority included, which otherwise leaves no per-Need
// record (only Unsatisfied / SatisfiedGangs survive).
func TestPhase1_Verdicts_CoverSatisfiedAndUnsatisfied(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(gpuMachine("idle-0", machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))

	// cluster-a outranks cluster-z, so it deterministically wins the one
	// idle machine; cluster-z is left unsatisfied.
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-a", gpuProfile(2_000_000), 1),
		gpuNeed("cluster-z", gpuProfile(1_000_000), 1),
	})

	if len(r.Verdicts) != 2 {
		t.Fatalf("verdicts = %d, want 2 (one per Need)", len(r.Verdicts))
	}
	va := findVerdict(r.Verdicts, "cluster-a")
	vz := findVerdict(r.Verdicts, "cluster-z")
	if va == nil || vz == nil {
		t.Fatalf("missing a verdict: a=%v z=%v", va, vz)
	}
	if !va.Satisfied {
		t.Errorf("cluster-a verdict: satisfied=false, want true")
	}
	if va.BootstrapCount != 1 || va.ClaimedCount != 1 {
		t.Errorf("cluster-a counts: bootstrap=%d claimed=%d, want 1/1", va.BootstrapCount, va.ClaimedCount)
	}
	if vz.Satisfied {
		t.Errorf("cluster-z verdict: satisfied=true, want false")
	}
	// cluster-z is unsatisfied, but a machine of its shape exists (the idle
	// one cluster-a took) — matching supply exists, it is contention, not
	// absence of supply.
	if !vz.MatchingSupplyExists {
		t.Errorf("cluster-z MatchingSupplyExists=false, want true (the idle machine matches its shape)")
	}
}

// MatchingSupplyExists must be false when no machine of the Need's shape
// exists anywhere — the NO_MATCHING_SUPPLY signal.
func TestPhase1_MatchingSupplyExists_FalseWhenNoSupply(t *testing.T) {
	t.Parallel()
	r := decision.Phase1(inventory.New().Snapshot(), []needs.Need{
		gpuNeed("cluster-a", gpuProfile(1_000_000), 1),
	})
	v := findVerdict(r.Verdicts, "cluster-a")
	if v == nil || v.Satisfied {
		t.Fatalf("want an unsatisfied verdict, got %v", v)
	}
	if v.MatchingSupplyExists {
		t.Errorf("MatchingSupplyExists=true on empty inventory, want false")
	}
}

// MatchingSupplyExists must be true when machines of the shape exist but
// are all held by a higher-priority incumbent — the PRIORITY_STARVED
// signal (vs NO_MATCHING_SUPPLY).
func TestPhase1_MatchingSupplyExists_TrueWhenHeldByHigherPriority(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// One configured a3-highgpu-8g, bound to another cluster at a HIGHER
	// priority: not creditable to cluster-a, not displaceable by it.
	_ = inv.Insert(configuredVictim("held-0", "cluster-incumbent", 2_000_000, 5.0, 1.0))

	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-a", gpuProfile(1_000_000), 1),
	})
	v := findVerdict(r.Verdicts, "cluster-a")
	if v == nil || v.Satisfied {
		t.Fatalf("want an unsatisfied verdict, got %v", v)
	}
	if v.ClaimedCount != 0 {
		t.Errorf("claimed=%d, want 0 (held by a higher-priority incumbent)", v.ClaimedCount)
	}
	if !v.MatchingSupplyExists {
		t.Errorf("MatchingSupplyExists=false, want true (a matching machine exists, just held above the cut-line)")
	}
}

// ADR-0061: Phase 2 Preempted=true when it freed some victims but the
// deficit still exceeded what was freed (PREEMPTION_EXHAUSTED).
func TestPhase2_Preempted_TrueWhenFellShort(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// One displaceable victim (lower priority) frees one unit; the need
	// wants two.
	_ = inv.Insert(configuredVictim("victim-0", "cluster-batch", 400_000, 5.0, 1.0))

	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{{
			Need:    needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), AggregateResources: needs.ScaleResources(gpuUnit, 2), MinUnit: gpuUnit},
			Deficit: needs.ScaleResources(gpuUnit, 2),
		}},
		decision.DefaultPhase2Options(),
	)
	if len(r.Actions) != 1 {
		t.Fatalf("preempt actions = %d, want 1", len(r.Actions))
	}
	if len(r.Unresolved) != 1 {
		t.Fatalf("unresolved = %d, want 1", len(r.Unresolved))
	}
	if !r.Unresolved[0].Preempted {
		t.Errorf("Preempted=false, want true (one victim freed, deficit still positive)")
	}
}

// Phase 2 Preempted=false when there was no displaceable victim at all
// (PRIORITY_STARVED, not PREEMPTION_EXHAUSTED).
func TestPhase2_Preempted_FalseWhenNoVictim(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// Only an equal-priority machine exists: nothing displaceable.
	_ = inv.Insert(configuredVictim("peer-0", "cluster-other", 1_000_000, 5.0, 1.0))

	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{{
			Need:    needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), AggregateResources: gpuUnit, MinUnit: gpuUnit},
			Deficit: gpuUnit,
		}},
		decision.DefaultPhase2Options(),
	)
	if len(r.Actions) != 0 {
		t.Fatalf("preempt actions = %d, want 0", len(r.Actions))
	}
	if len(r.Unresolved) != 1 {
		t.Fatalf("unresolved = %d, want 1", len(r.Unresolved))
	}
	if r.Unresolved[0].Preempted {
		t.Errorf("Preempted=true, want false (no displaceable victim existed)")
	}
}
