package decision_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// All needs deleted: every configured machine reclaimed.
func TestPhase3_AllExcessWhenNoNeeds(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	r := decision.Phase3(inv.Snapshot(), nil)
	if got := len(r.Actions); got != 4 {
		t.Fatalf("reclaim actions = %d, want 4", got)
	}
	for _, a := range r.Actions {
		if a.Kind != decision.ActionKindReclaim {
			t.Errorf("expected Reclaim, got %s", a.Kind)
		}
	}
}

// Need is fully satisfied by the configured machines: nothing reclaimed.
func TestPhase3_NoOpWhenExactlyMet(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{{ClusterID: "cluster-a", Profile: gpuProfile(100), Count: 3}},
	)
	if got := len(r.Actions); got != 0 {
		t.Errorf("expected zero reclaim actions, got %d", got)
	}
}

// Cluster has 5 configured but only 3 are needed: 2 reclaimed, cheapest
// per-hour first.
func TestPhase3_ReclaimsCheapestFirst(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// Mix of three on-demand ($6) and two bare-metal ($0). Cluster
	// needs 3. Phase 3 should release the three on-demand machines.
	for i := 0; i < 3; i++ {
		m := configuredVictim(idN(i), "cluster-a", 100, 0, 0)
		m.PricePerHour = 6.0
		m.Profile.CapacityType = machine.CapacityTypeOnDemand
		_ = inv.Insert(m)
	}
	for i := 3; i < 5; i++ {
		m := configuredVictim(idN(i), "cluster-a", 100, 0, 0)
		m.PricePerHour = 0
		m.Profile.CapacityType = machine.CapacityTypeBareMetal
		_ = inv.Insert(m)
	}
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{{ClusterID: "cluster-a", Profile: gpuProfile(100), Count: 3}},
	)
	if got := len(r.Actions); got != 2 {
		t.Fatalf("reclaim actions = %d, want 2", got)
	}
	for _, a := range r.Actions {
		// The two on-demand machines (priced higher) are reclaimed first.
		// Check by looking up the machine's price.
		m, _ := inv.Get(a.MachineID)
		if m.PricePerHour != 6.0 {
			t.Errorf("reclaimed cheap machine %s (price=%f); should reclaim expensive first", a.MachineID, m.PricePerHour)
		}
	}
}

// Tied prices: prefer reclaiming the lower reclamation_penalty machine.
func TestPhase3_TiebreakByReclamationPenalty(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	low := configuredVictim("v-low", "cluster-a", 100, 0, 0)
	low.PricePerHour = 6.0
	low.AssignedReclamationPenaltyDollars = 0
	high := configuredVictim("v-high", "cluster-a", 100, 0, 0)
	high.PricePerHour = 6.0
	high.AssignedReclamationPenaltyDollars = 5_000
	_ = inv.Insert(low)
	_ = inv.Insert(high)

	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{{ClusterID: "cluster-a", Profile: gpuProfile(100), Count: 1}},
	)
	if got := len(r.Actions); got != 1 {
		t.Fatalf("reclaim actions = %d, want 1", got)
	}
	if r.Actions[0].MachineID != "v-low" {
		t.Errorf("reclaimed %s, want v-low (lower reclamation penalty)", r.Actions[0].MachineID)
	}
}

// M44.4 Drop F regression: a machine bound to fingerprint A whose
// Need's demand has dropped to 0 must reclaim, even when the cluster
// still has Needs for other fingerprints (B, C, …) whose requirements
// are subsets of A's. Pre-Drop-F, MatchProfile-based keep semantics
// kept the stale machine against B's budget — the chain ended up
// holding inventory for long-dead fingerprints, forcing all new binds
// through Phase 2 preemption thrash.
func TestPhase3_StaleAssignmentReclaims(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	pfA := gpuProfile(100)
	pfB := needs.NewProfile(
		// Subset of pfA's requirements (no instance-type pin).
		nil, nil, nil, 100,
		needs.PenaltyBucket8192, needs.PenaltyBucketPinned,
	)

	// Machine bound to fp_A.
	m := configuredVictim("victim-stale", "cluster-a", 100, 0, 0)
	m.AssignedNeedFingerprint = pfA.Fingerprint()
	_ = inv.Insert(m)

	// Cluster's only current Need is for fp_B (pfB ⊂ pfA via
	// requirements subset). Pre-Drop-F: keep because pfB matches m;
	// post: reclaim because m.AssignedNeedFingerprint != pfB.fingerprint.
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{{ClusterID: "cluster-a", Profile: pfB, Count: 5}},
	)
	if got := len(r.Actions); got != 1 {
		t.Fatalf("reclaim actions = %d, want 1 (stale machine should reclaim)", got)
	}
	if r.Actions[0].MachineID != "victim-stale" {
		t.Errorf("reclaimed %s, want victim-stale", r.Actions[0].MachineID)
	}
}

// Different profiles: a configured machine that doesn't match any
// remaining need is reclaimed even when other configured machines still
// satisfy their respective needs.
func TestPhase3_PerProfileMatching(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// 4 GPU machines configured for cluster-a.
	for i := 0; i < 4; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	// Roll-up says: only 1 GPU need remains (training mostly done).
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{{ClusterID: "cluster-a", Profile: gpuProfile(100), Count: 1}},
	)
	if got := len(r.Actions); got != 3 {
		t.Errorf("reclaim actions = %d, want 3", got)
	}
}

// Conservation: total Configured count before Phase 3 = (machines kept)
// + (machines whose Reclaim action is emitted). Machines aren't
// double-counted.
func TestPhase3_Conservation(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 10; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	totalConfigured := 10
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{{ClusterID: "cluster-a", Profile: gpuProfile(100), Count: 4}},
	)
	reclaim := len(r.Actions)
	keep := totalConfigured - reclaim
	if keep != 4 {
		t.Errorf("keep = %d, want 4 (need.count); reclaim = %d", keep, reclaim)
	}
}

// TestPhase3_DenseMachine_OneCoversManyPodsOfDemand: ADR-0022 / M45.2.
// One Configured machine with density=8 (c6a.4xlarge shape, 4Gi per-
// replica Pods) covers up to 8 Pods of demand without being reclaimed.
// If demand is 8 the machine is fully utilised and kept; if demand is 0
// the machine is reclaimed. The pre-ADR-0022 budget math would have
// only "consumed" 1 Pod of budget per kept machine, so a Need.Count of
// 8 with one dense machine would have reclaimed 7 phantom machines.
func TestPhase3_DenseMachine_OneCoversManyPodsOfDemand(t *testing.T) {
	t.Parallel()

	profile := needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"c6a.4xlarge"},
		}},
		[]needs.ResourceQty{
			{Name: "cpu", Quantity: "1"},
			{Name: "memory", Quantity: "4Gi"},
		},
		nil,
		1000,
		needs.PenaltyBucket64,
		needs.PenaltyBucketPinned,
	)

	denseMachine := func(id machine.ID) machine.Machine {
		return machine.Machine{
			ID:    id,
			State: machine.StateConfigured,
			Host:  machine.HostRef{Provider: "fake", Ref: string(id)},
			Profile: machine.Profile{
				InstanceType: "c6a.4xlarge",
				Resources:    map[string]string{"cpu": "1", "memory": "4Gi"},
			},
			Cluster:                 "cluster-A",
			AssignedNeedFingerprint: profile.Fingerprint(),
			Allocatable:             map[string]string{"cpu": "16", "memory": "32Gi"},
		}
	}

	// Two dense machines (16 Pods total capacity) vs Need.Count=8.
	// Pre-ADR-0022: budget=8, kept-2-decremented-to-6, no reclaim because
	// both kept. Same result either way, but the test confirms the new
	// budget tracks density.
	inv := inventory.New()
	if err := inv.Insert(denseMachine("dense-1")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := inv.Insert(denseMachine("dense-2")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	snap := inv.Snapshot()

	// Demand = 8 Pods. First dense machine absorbs 8 (density). Second
	// has no remaining budget → Reclaim.
	need := needs.Need{
		ClusterID: "cluster-A",
		Profile:   profile,
		Count:     8,
	}
	res := decision.Phase3(snap, []needs.Need{need})

	if len(res.Actions) != 1 {
		t.Fatalf("expected 1 Reclaim (second dense machine has no Pod budget left after first absorbs 8), got %d actions: %#v", len(res.Actions), res.Actions)
	}
	if res.Actions[0].Kind != decision.ActionKindReclaim {
		t.Errorf("action kind = %v, want Reclaim", res.Actions[0].Kind)
	}
}
