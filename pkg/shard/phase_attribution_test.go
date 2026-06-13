package shard

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

func paProfile(pri int32, same bool) needs.Profile {
	reqs := []needs.Requirement{{
		Key:      "node.kubernetes.io/instance-type",
		Operator: needs.OperatorIn,
		Values:   []string{"a3-highgpu-8g"},
	}}
	if same {
		reqs = append(reqs, needs.Requirement{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame})
	}
	return needs.NewProfile(reqs, nil, pri, needs.PenaltyBucket8192, needs.PenaltyBucket8192)
}

func paMachine(id machine.ID, instanceType string, gpus string) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: machine.StateConfigured,
		Host:  machine.HostRef{Provider: "fake", Ref: string(id)},
		Profile: machine.Profile{
			InstanceType: instanceType,
			Resources:    map[string]string{"nvidia.com/gpu": gpus},
			Labels:       map[string]string{"topology.bigfleet/rack": "rack-1"},
		},
		Cluster: "c1",
	}
}

// TestCollectPhaseAttribution pins the ADR-0040 §4 probe's counting:
// the co-location splits and the reclaim-matches-unsatisfied
// discriminator (match = MatchProfile against some unsatisfied Need;
// fits = additionally Covers that Need's MinUnit).
func TestCollectPhaseAttribution(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// Matches the unsatisfied Need and fits its MinUnit.
	if err := inv.Insert(paMachine("m-fit", "a3-highgpu-8g", "8")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Matches but can't host one MinUnit (4 < 8 GPUs).
	if err := inv.Insert(paMachine("m-small", "a3-highgpu-8g", "4")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Doesn't MatchProfile at all.
	if err := inv.Insert(paMachine("m-other", "m5.large", "8")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	snap := inv.Snapshot()

	gpuUnit := []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}}
	sameNeed := needs.Need{ClusterID: "c1", Profile: paProfile(1000, true), AggregateResources: gpuUnit, MinUnit: gpuUnit}
	demand := []needs.Need{
		sameNeed,
		{ClusterID: "c1", Profile: paProfile(2000, true), AggregateResources: gpuUnit, MinUnit: gpuUnit},
		{ClusterID: "c1", Profile: paProfile(3000, false), AggregateResources: gpuUnit, MinUnit: gpuUnit},
	}
	p1 := decision.Phase1Result{Unsatisfied: []decision.UnsatisfiedNeed{
		{Need: sameNeed, Deficit: gpuUnit},
	}}
	p3 := decision.Phase3Result{Actions: []decision.Action{
		{Kind: decision.ActionKindReclaim, MachineID: "m-fit", Cluster: "c1"},
		{Kind: decision.ActionKindReclaim, MachineID: "m-small", Cluster: "c1"},
		{Kind: decision.ActionKindReclaim, MachineID: "m-other", Cluster: "c1"},
	}}

	pa := collectPhaseAttribution(snap, demand, p1, p3)
	// The gang probe (ADR-0042) samples the unsatisfied Same-Need; the
	// reclaim probe (v3, the #58 follow-up) samples every reclaim with
	// its match/fit verdicts. Asserted separately since the struct is
	// no longer comparable.
	if len(pa.gangProbe) != 1 {
		t.Errorf("gangProbe entries = %d, want 1", len(pa.gangProbe))
	}
	wantReclaims := []reclaimProbeEntry{
		{machineID: "m-fit", cluster: "c1", instanceType: "a3-highgpu-8g", matches: true, fits: true},
		{machineID: "m-small", cluster: "c1", instanceType: "a3-highgpu-8g", matches: true, fits: false},
		{machineID: "m-other", cluster: "c1", instanceType: "m5.large", matches: false, fits: false},
	}
	if !reflect.DeepEqual(pa.reclaimProbe, wantReclaims) {
		t.Errorf("reclaimProbe = %+v, want %+v", pa.reclaimProbe, wantReclaims)
	}
	pa.gangProbe = nil
	pa.reclaimProbe = nil
	want := phaseAttribution{
		needsTotal:                  3,
		needsSame:                   2,
		p1Unsatisfied:               1,
		p1UnsatisfiedSame:           1,
		p3Reclaim:                   3,
		p3ReclaimMatchesUnsatisfied: 2,
		p3ReclaimMatchesAndFits:     1,
	}
	if !reflect.DeepEqual(pa, want) {
		t.Errorf("collectPhaseAttribution = %+v, want %+v", pa, want)
	}
}

// TestCollectPhaseAttribution_SatisfiedGang pins the #325 satisfied-gang
// probe: a SATISFIED Same-Need (one with a Same key, absent from
// Phase1Result.Unsatisfied) emits one probe line carrying its
// chosen_domain and its full claimed machine-set — the observability the
// M77g diagnosis needs, since the existing gangProbe fires only for
// UNSATISFIED Same-Needs (p1_unsatisfied_same is 0 during the
// bigfleet-uber #61/#63 oscillation). The claimed IDs come back sorted so
// a reader can compare lines cycle-to-cycle for churn.
func TestCollectPhaseAttribution_SatisfiedGang(t *testing.T) {
	t.Parallel()
	snap := inventory.New().Snapshot()

	gpuUnit := []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}}
	gang := needs.Need{
		ClusterID:          "c1",
		Profile:            paProfile(1000, true),
		AggregateResources: gpuUnit,
		MinUnit:            gpuUnit,
		Group:              "trainer-a",
	}
	// IDs out of order on purpose — the probe must sort them.
	p1 := decision.Phase1Result{SatisfiedGangs: []decision.SatisfiedGang{{
		Need:     gang,
		Domain:   "rack-7",
		Machines: []machine.ID{"m-c", "m-a", "m-b"},
	}}}

	pa := collectPhaseAttribution(snap, []needs.Need{gang}, p1, decision.Phase3Result{})

	want := []satGangProbeEntry{{
		group:        "trainer-a",
		cluster:      "c1",
		domain:       "rack-7",
		machineCount: 3,
		machines:     []string{"m-a", "m-b", "m-c"},
	}}
	if !reflect.DeepEqual(pa.satGangProbe, want) {
		t.Errorf("satGangProbe = %+v, want %+v", pa.satGangProbe, want)
	}
}

// TestCollectPhaseAttribution_SatisfiedGangCap pins the claimed-machine
// volume bound (#325): the printed list is capped to satGangMachineCap
// IDs while machine_count still reports the true total, so a churn the
// truncated list hides is still flagged by the count.
func TestCollectPhaseAttribution_SatisfiedGangCap(t *testing.T) {
	t.Parallel()
	snap := inventory.New().Snapshot()

	gpuUnit := []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}}
	gang := needs.Need{ClusterID: "c1", Profile: paProfile(1000, true), AggregateResources: gpuUnit, MinUnit: gpuUnit, Group: "big"}
	// Two more machines than the cap; zero-padded so sort order is
	// numeric-stable and the expected prefix is unambiguous.
	total := satGangMachineCap + 2
	ids := make([]machine.ID, 0, total)
	for i := 0; i < total; i++ {
		ids = append(ids, machine.ID(fmt.Sprintf("m-%03d", i)))
	}
	p1 := decision.Phase1Result{SatisfiedGangs: []decision.SatisfiedGang{{
		Need:     gang,
		Domain:   "rack-1",
		Machines: ids,
	}}}

	pa := collectPhaseAttribution(snap, []needs.Need{gang}, p1, decision.Phase3Result{})

	if len(pa.satGangProbe) != 1 {
		t.Fatalf("satGangProbe entries = %d, want 1", len(pa.satGangProbe))
	}
	got := pa.satGangProbe[0]
	if got.machineCount != total {
		t.Errorf("machineCount = %d, want %d (true total, uncapped)", got.machineCount, total)
	}
	if len(got.machines) != satGangMachineCap {
		t.Errorf("printed machines = %d, want %d (capped)", len(got.machines), satGangMachineCap)
	}
	if got.machines[0] != "m-000" {
		t.Errorf("first printed machine = %q, want sorted prefix m-000", got.machines[0])
	}
}
