package shard

import (
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
	// The gang probe (ADR-0042) samples the unsatisfied Same-Need; its
	// presence is asserted separately since the struct is no longer
	// comparable.
	if len(pa.gangProbe) != 1 {
		t.Errorf("gangProbe entries = %d, want 1", len(pa.gangProbe))
	}
	pa.gangProbe = nil
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
