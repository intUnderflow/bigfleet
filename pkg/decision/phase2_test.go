package decision_test

import (
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

func configuredVictim(id machine.ID, cluster machine.ClusterID, priority int32, intPen, recPen float64) machine.Machine {
	m := gpuMachine(id, machine.StateConfigured, cluster, machine.CapacityTypeOnDemand, 6.0)
	m.AssignedPriority = priority
	m.AssignedInterruptionPenaltyDollars = intPen
	m.AssignedReclamationPenaltyDollars = recPen
	return m
}

// Phase 2 with no unresolved needs is a no-op.
func TestPhase2_NoOpWhenAllResolved(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	r := decision.Phase2(inv.Snapshot(), nil, decision.DefaultPhase2Options())
	if len(r.Actions) != 0 || len(r.Unresolved) != 0 {
		t.Errorf("expected empty result, got actions=%d unresolved=%d", len(r.Actions), len(r.Unresolved))
	}
}

// Classic priority inversion: the only configured machines are at
// priority 500K; an unsatisfied priority 1M need preempts them.
func TestPhase2_PriorityInversion_Preempts(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		// Priority gap = 1M - 400K = 600K → falls in the >500K bucket (30s grace).
		_ = inv.Insert(configuredVictim(idN(i), "cluster-batch", 400_000, 5.0, 1.0))
	}
	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), Count: 4}, Deficit: 4},
		},
		decision.DefaultPhase2Options(),
	)
	if got := len(r.Actions); got != 4 {
		t.Fatalf("preempt actions = %d, want 4", got)
	}
	for _, a := range r.Actions {
		if a.Kind != decision.ActionKindPreempt {
			t.Errorf("expected Preempt action, got %s", a.Kind)
		}
		if a.PreemptorPriority != 1_000_000 {
			t.Errorf("expected preemptor=1M, got %d", a.PreemptorPriority)
		}
		if a.GracePeriod != 30*time.Second {
			t.Errorf("expected 30s grace for gap=600K, got %v", a.GracePeriod)
		}
	}
	if len(r.Unresolved) != 0 {
		t.Errorf("expected zero unresolved, got %d", len(r.Unresolved))
	}
}

// No equal-or-higher-priority victims may be preempted.
func TestPhase2_RefusesToPreemptEqualOrHigher(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-other", 1_000_000, 5.0, 1.0))
	}
	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), Count: 2}, Deficit: 2},
		},
		decision.DefaultPhase2Options(),
	)
	if len(r.Actions) != 0 {
		t.Errorf("expected zero preemption (equal priority), got %d", len(r.Actions))
	}
	if len(r.Unresolved) != 1 || r.Unresolved[0].Deficit != 2 {
		t.Errorf("expected unresolved deficit=2, got %+v", r.Unresolved)
	}
}

// Tie-breaking: among equally-low-priority victims, prefer the one with
// lower interruption penalty (cheaper to interrupt).
func TestPhase2_PrefersLowerInterruptionPenalty(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	cheap := configuredVictim("v-cheap", "cluster-batch", 500_000, 1.0, 1.0)
	expensive := configuredVictim("v-expensive", "cluster-batch", 500_000, 100_000, 1.0)
	_ = inv.Insert(cheap)
	_ = inv.Insert(expensive)

	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), Count: 1}, Deficit: 1},
		},
		decision.DefaultPhase2Options(),
	)
	if got := len(r.Actions); got != 1 {
		t.Fatalf("actions = %d, want 1", got)
	}
	if r.Actions[0].MachineID != "v-cheap" {
		t.Errorf("preempted %s, want v-cheap (lower interruption penalty)", r.Actions[0].MachineID)
	}
}

// Drain grace scales with priority gap.
func TestPhase2_GracePeriodMatchesGap(t *testing.T) {
	t.Parallel()
	cases := []struct {
		victimPriority int32
		wantGrace      time.Duration
	}{
		{0, 10 * time.Second},       // gap = 1M > 900K
		{499_999, 30 * time.Second}, // gap > 500K
		{899_999, 2 * time.Minute},  // gap > 100K
		{999_999, 10 * time.Minute}, // gap = 1
	}
	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			inv := inventory.New()
			_ = inv.Insert(configuredVictim("v-1", "cluster-batch", tc.victimPriority, 5.0, 1.0))
			r := decision.Phase2(inv.Snapshot(),
				[]decision.UnsatisfiedNeed{
					{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), Count: 1}, Deficit: 1},
				},
				decision.DefaultPhase2Options(),
			)
			if len(r.Actions) != 1 {
				t.Fatalf("actions = %d, want 1", len(r.Actions))
			}
			if r.Actions[0].GracePeriod != tc.wantGrace {
				t.Errorf("grace = %v, want %v", r.Actions[0].GracePeriod, tc.wantGrace)
			}
		})
	}
}

// Two preemptors competing for the same victim: the higher-priority one wins.
func TestPhase2_MultiplePreemptorsResolvedByPriority(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(configuredVictim("v-1", "cluster-batch", 100_000, 5.0, 1.0))

	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: needs.Need{ClusterID: "cluster-mid", Profile: gpuProfile(500_000), Count: 1}, Deficit: 1},
			{Need: needs.Need{ClusterID: "cluster-high", Profile: gpuProfile(1_000_000), Count: 1}, Deficit: 1},
		},
		decision.DefaultPhase2Options(),
	)
	if len(r.Actions) != 1 {
		t.Fatalf("expected 1 preempt action, got %d", len(r.Actions))
	}
	if r.Actions[0].PreemptorPriority != 1_000_000 {
		t.Errorf("victim went to wrong preemptor; got priority %d", r.Actions[0].PreemptorPriority)
	}
	// The mid-priority preemptor remains unresolved.
	if len(r.Unresolved) != 1 || r.Unresolved[0].Need.ClusterID != "cluster-mid" {
		t.Errorf("expected cluster-mid to remain unresolved, got %+v", r.Unresolved)
	}
}
