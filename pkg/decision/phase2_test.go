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
	// Default to gpuProfile fingerprint at the matching priority so
	// Phase 3's keep-on-fingerprint-equality has something to match.
	m.AssignedNeedFingerprint = gpuProfile(priority).Fingerprint()
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
			{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), AggregateResources: needs.ScaleResources(gpuUnit, 4), MinUnit: gpuUnit}, Deficit: needs.ScaleResources(gpuUnit, 4)},
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
			{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), AggregateResources: needs.ScaleResources(gpuUnit, 2), MinUnit: gpuUnit}, Deficit: needs.ScaleResources(gpuUnit, 2)},
		},
		decision.DefaultPhase2Options(),
	)
	if len(r.Actions) != 0 {
		t.Errorf("expected zero preemption (equal priority), got %d", len(r.Actions))
	}
	if len(r.Unresolved) != 1 || gpuQty(r.Unresolved[0].Deficit) != "16" {
		t.Errorf("expected unresolved deficit nvidia.com/gpu=16 (2 units), got %+v", r.Unresolved)
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
			{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), AggregateResources: needs.ScaleResources(gpuUnit, 1), MinUnit: gpuUnit}, Deficit: needs.ScaleResources(gpuUnit, 1)},
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
					{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), AggregateResources: needs.ScaleResources(gpuUnit, 1), MinUnit: gpuUnit}, Deficit: needs.ScaleResources(gpuUnit, 1)},
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

// M68 (a): a victim too small to host one MinUnit chunk of the
// demanded shape frees nothing the Need can use — it must never be
// selected, even when it is the only lower-priority machine around.
func TestPhase2_VictimBelowMinUnit_NeverSelected(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		small := configuredVictim(idN(i), "cluster-batch", 400_000, 5.0, 1.0)
		// Same instance type / profile, but only 4 of the 8 GPUs one
		// MinUnit chunk needs are allocatable.
		small.Allocatable = map[string]string{"nvidia.com/gpu": "4"}
		_ = inv.Insert(small)
	}
	big := configuredVictim("v-big", "cluster-batch", 400_000, 5.0, 1.0)
	_ = inv.Insert(big)

	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: needs.Need{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), AggregateResources: needs.ScaleResources(gpuUnit, 2), MinUnit: gpuUnit}, Deficit: needs.ScaleResources(gpuUnit, 2)},
		},
		decision.DefaultPhase2Options(),
	)
	if got := len(r.Actions); got != 1 {
		t.Fatalf("preempt actions = %d, want 1 (only the MinUnit-covering victim)", got)
	}
	if r.Actions[0].MachineID != "v-big" {
		t.Errorf("preempted %s, want v-big (sub-MinUnit victims are pure disruption)", r.Actions[0].MachineID)
	}
	// The residual one unit stays unresolved — the small victims must
	// not be burned to paper over it.
	if len(r.Unresolved) != 1 || gpuQty(r.Unresolved[0].Deficit) != "8" {
		t.Errorf("expected unresolved deficit nvidia.com/gpu=8, got %+v", r.Unresolved)
	}
}

// sameVictimInZone is configuredVictim placed in an explicit zone, for
// the M68 Same-domain scoping tests.
func sameVictimInZone(id machine.ID, zone string, priority int32) machine.Machine {
	m := gpuMachineInZone(id, zone, 6.0)
	m.State = machine.StateConfigured
	m.Cluster = "cluster-batch"
	m.AssignedPriority = priority
	m.AssignedInterruptionPenaltyDollars = 5.0
	m.AssignedReclamationPenaltyDollars = 1.0
	return m
}

// M68 (b): a Same-carrying Need preempts only inside the domain
// Phase 1 chose for it. zone-a victims sort first (equal scores, ID
// tiebreak) — pre-fix they were picked even for a zone-b gang.
func TestPhase2_Same_ConfinedToChosenDomain(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 2; i++ {
		_ = inv.Insert(sameVictimInZone(machine.ID("a-")+idN(i), "zone-a", 400_000))
		_ = inv.Insert(sameVictimInZone(machine.ID("b-")+idN(i), "zone-b", 400_000))
	}

	gang := needs.Need{
		ClusterID:          "cluster-train",
		Profile:            gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		AggregateResources: needs.ScaleResources(gpuUnit, 2),
		MinUnit:            gpuUnit,
	}
	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: gang, Deficit: needs.ScaleResources(gpuUnit, 2), SameDomain: "zone-b"},
		},
		decision.DefaultPhase2Options(),
	)
	if got := len(r.Actions); got != 2 {
		t.Fatalf("preempt actions = %d, want 2", got)
	}
	snap := inv.Snapshot()
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		if m.Profile.Zone != "zone-b" {
			t.Errorf("preempted %s in %s, want zone-b only (capacity freed off-domain cannot assemble the gang)", a.MachineID, m.Profile.Zone)
		}
	}
	if len(r.Unresolved) != 0 {
		t.Errorf("expected zero unresolved, got %+v", r.Unresolved)
	}
}

// M68 (b), no-domain arm: a Same Need whose pre-pass recorded no
// domain (no creditable or acquirable bucket existed) has nothing to
// extend — Phase 2 must skip rather than preempt domain-blind, and the
// deficit passes through to the shortfall path.
func TestPhase2_Same_NoDomainChoice_SkipsPreemption(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(sameVictimInZone(machine.ID("a-")+idN(i), "zone-a", 400_000))
	}

	gang := needs.Need{
		ClusterID:          "cluster-train",
		Profile:            gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		AggregateResources: needs.ScaleResources(gpuUnit, 4),
		MinUnit:            gpuUnit,
	}
	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: gang, Deficit: needs.ScaleResources(gpuUnit, 4), SameDomain: ""},
		},
		decision.DefaultPhase2Options(),
	)
	if len(r.Actions) != 0 {
		t.Errorf("expected zero preempt actions for a domain-less Same Need, got %d", len(r.Actions))
	}
	if len(r.Unresolved) != 1 || gpuQty(r.Unresolved[0].Deficit) != "32" {
		t.Errorf("expected full deficit unresolved, got %+v", r.Unresolved)
	}
}

// M68 / ADR-0042 Addendum: a parked Need produces zero Preempt actions
// — Phase 1 decided not to acquire for it this cycle, and preempting
// victims for demand that won't assemble contradicts that decision.
// The residual still passes through so it keeps aging in the shortfall
// buffer.
func TestPhase2_ParkedNeed_NoPreempt(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(sameVictimInZone(machine.ID("a-")+idN(i), "zone-a", 400_000))
	}

	gang := needs.Need{
		ClusterID:          "cluster-train",
		Profile:            gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		AggregateResources: needs.ScaleResources(gpuUnit, 4),
		MinUnit:            gpuUnit,
		AcquisitionParked:  true,
	}
	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			// Even with a chosen domain full of cheap victims: parked
			// means no preemption.
			{Need: gang, Deficit: needs.ScaleResources(gpuUnit, 4), SameDomain: "zone-a"},
		},
		decision.DefaultPhase2Options(),
	)
	if len(r.Actions) != 0 {
		t.Errorf("expected zero preempt actions for a parked Need, got %d", len(r.Actions))
	}
	if len(r.Unresolved) != 1 || gpuQty(r.Unresolved[0].Deficit) != "32" {
		t.Errorf("expected full deficit unresolved, got %+v", r.Unresolved)
	}
}

// The #309 pin, end-to-end through Phase 1's domain plumbing: a
// priority-2000 Same gang with a partial assembly in zone-a, starved
// behind priority-1000 holders bound to another cluster, DOES preempt
// post-fix — and only inside its chosen domain.
//
// (#309's other shape — holders bound to the gang's OWN cluster — is
// sanctioned non-firing: the ADR-0045 credit walk counts cluster-bound
// capacity regardless of AssignedPriority, so an intra-cluster
// inversion produces no deficit and nothing reaches Phase 2;
// kube-scheduler preemption resolves it cluster-side, ADR-0045 §4.)
func TestPhase2_Same_GangBehindLowerPriorityHolders_PreemptsInDomain(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// The gang's partial assembly: 2 machines bound to cluster-train in
	// zone-a. The pre-pass credits these and records zone-a as the
	// gang's domain.
	for i := 0; i < 2; i++ {
		m := gpuMachineInZone(machine.ID("train-")+idN(i), "zone-a", 1.0)
		m.State = machine.StateConfigured
		m.Cluster = "cluster-train"
		m.AssignedPriority = 2000
		_ = inv.Insert(m)
	}
	// Priority-1000 holders bound to cluster-batch: 2 in the gang's
	// domain, 4 outside it.
	for i := 0; i < 2; i++ {
		_ = inv.Insert(sameVictimInZone(machine.ID("hold-a-")+idN(i), "zone-a", 1000))
	}
	for i := 0; i < 4; i++ {
		_ = inv.Insert(sameVictimInZone(machine.ID("hold-b-")+idN(i), "zone-b", 1000))
	}
	snap := inv.Snapshot()

	gang := needs.Need{
		ClusterID:          "cluster-train",
		Profile:            gpuProfileWithSame(2000, "topology.kubernetes.io/zone"),
		AggregateResources: needs.ScaleResources(gpuUnit, 4),
		MinUnit:            gpuUnit,
		Group:              "gang-1",
	}
	p1 := decision.Phase1(snap, []needs.Need{gang})
	if len(p1.Unsatisfied) != 1 {
		t.Fatalf("phase 1 unsatisfied = %d, want 1", len(p1.Unsatisfied))
	}
	if got := p1.Unsatisfied[0].SameDomain; got != "zone-a" {
		t.Fatalf("phase 1 chose domain %q, want zone-a (the serving partial assembly)", got)
	}

	p2 := decision.Phase2(snap, p1.Unsatisfied, decision.DefaultPhase2Options())
	if got := len(p2.Actions); got != 2 {
		t.Fatalf("preempt actions = %d, want 2 (#309: the gang must preempt its domain's lower-priority holders)", got)
	}
	for _, a := range p2.Actions {
		m, _ := snap.Get(a.MachineID)
		if m.Profile.Zone != "zone-a" {
			t.Errorf("preempted %s in %s, want zone-a only", a.MachineID, m.Profile.Zone)
		}
		if a.Kind != decision.ActionKindPreempt {
			t.Errorf("expected Preempt action, got %s", a.Kind)
		}
	}
}

// Two preemptors competing for the same victim: the higher-priority one wins.
func TestPhase2_MultiplePreemptorsResolvedByPriority(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(configuredVictim("v-1", "cluster-batch", 100_000, 5.0, 1.0))

	r := decision.Phase2(inv.Snapshot(),
		[]decision.UnsatisfiedNeed{
			{Need: needs.Need{ClusterID: "cluster-mid", Profile: gpuProfile(500_000), AggregateResources: needs.ScaleResources(gpuUnit, 1), MinUnit: gpuUnit}, Deficit: needs.ScaleResources(gpuUnit, 1)},
			{Need: needs.Need{ClusterID: "cluster-high", Profile: gpuProfile(1_000_000), AggregateResources: needs.ScaleResources(gpuUnit, 1), MinUnit: gpuUnit}, Deficit: needs.ScaleResources(gpuUnit, 1)},
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
