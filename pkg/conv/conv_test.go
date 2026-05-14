package conv_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

func TestNeedsFromRollup_FullProfile(t *testing.T) {
	t.Parallel()
	in := &pb.ClusterCapacityNeeds{
		ClusterId:          "cluster-amsterdam",
		TimestampUnixNanos: 1000,
		Needs: []*pb.CapacityNeed{
			{
				Requirements: []*pb.NodeSelectorRequirement{
					{
						Key:      "node.kubernetes.io/instance-type",
						Operator: pb.NodeSelectorRequirement_OPERATOR_IN,
						Values:   []string{"a3-highgpu-8g"},
					},
					{
						Key:      "topology.kubernetes.io/rack",
						Operator: pb.NodeSelectorRequirement_OPERATOR_SAME,
					},
				},
				// ADR-0027: aggregate demand + the indivisibility floor,
				// not a per-pod shape and count.
				AggregateResources: map[string]string{"cpu": "6144", "nvidia.com/gpu": "512"},
				MinUnit:            map[string]string{"cpu": "96", "nvidia.com/gpu": "8"},
				Priority:           1_000_000,
				Spread: []*pb.TopologySpread{{
					TopologyKey:       "topology.kubernetes.io/zone",
					MaxSkew:           1,
					WhenUnsatisfiable: pb.TopologySpread_WHEN_UNSATISFIABLE_DO_NOT_SCHEDULE,
				}},
				InterruptionPenaltyBucket: pb.PenaltyBucket_PENALTY_BUCKET_8192,
				ReclamationPenaltyBucket:  pb.PenaltyBucket_PENALTY_BUCKET_PINNED,
			},
		},
	}
	got, err := conv.NeedsFromRollup(in)
	if err != nil {
		t.Fatalf("NeedsFromRollup: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	n := got[0]
	if n.ClusterID != "cluster-amsterdam" {
		t.Errorf("ClusterID = %s", n.ClusterID)
	}
	if got := qtyOf(n.AggregateResources, "cpu"); got != "6144" {
		t.Errorf("AggregateResources[cpu] = %q, want 6144", got)
	}
	if got := qtyOf(n.AggregateResources, "nvidia.com/gpu"); got != "512" {
		t.Errorf("AggregateResources[nvidia.com/gpu] = %q, want 512", got)
	}
	if got := qtyOf(n.MinUnit, "nvidia.com/gpu"); got != "8" {
		t.Errorf("MinUnit[nvidia.com/gpu] = %q, want 8", got)
	}
	if n.Profile.Priority() != 1_000_000 {
		t.Errorf("Priority = %d", n.Profile.Priority())
	}
	if n.Profile.InterruptionPenaltyBucket() != needs.PenaltyBucket8192 {
		t.Errorf("InterruptionPenaltyBucket = %v", n.Profile.InterruptionPenaltyBucket())
	}
	if n.Profile.ReclamationPenaltyBucket() != needs.PenaltyBucketPinned {
		t.Errorf("ReclamationPenaltyBucket = %v", n.Profile.ReclamationPenaltyBucket())
	}
	if got, want := len(n.Profile.Requirements()), 2; got != want {
		t.Errorf("requirements len = %d, want %d", got, want)
	}
}

func qtyOf(rs []needs.ResourceQty, name string) string {
	for _, r := range rs {
		if r.Name == name {
			return r.Quantity
		}
	}
	return ""
}

func TestNeedsFromRollup_NilSafe(t *testing.T) {
	t.Parallel()
	got, err := conv.NeedsFromRollup(nil)
	if err != nil {
		t.Fatalf("NeedsFromRollup(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty slice, got %d", len(got))
	}
}

func TestMachineRoundTrip_AllStates(t *testing.T) {
	t.Parallel()
	for state := machine.StateSpeculative; state <= machine.StateFailed; state++ {
		state := state
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			m := machine.Machine{
				ID:                      "m-1",
				State:                   state,
				PricePerHour:            6.0,
				InterruptionProbability: 0.05,
				Profile: machine.Profile{
					InstanceType: "p5.48xlarge",
					Zone:         "us-east-1a",
					CapacityType: machine.CapacityTypeOnDemand,
					Resources:    map[string]string{"cpu": "96"},
					Labels:       map[string]string{"accelerator-type": "nvidia-h100-80gb"},
				},
			}
			if state != machine.StateSpeculative && state != machine.StateCreating {
				m.Host = machine.HostRef{Provider: "aws", Ref: "i-1"}
			}
			if state == machine.StateFailed {
				m.LastError = "configure timed out"
			}
			pbMachine := conv.MachineToProto(m)
			back, err := conv.MachineFromProto(pbMachine)
			if err != nil {
				t.Fatalf("round-trip: %v", err)
			}
			// Spot-check critical fields. Labels / resources are maps so
			// we only check sizes — order doesn't matter.
			if back.ID != m.ID || back.State != m.State || back.PricePerHour != m.PricePerHour {
				t.Errorf("scalar mismatch: in=%+v out=%+v", m, back)
			}
			if len(back.Profile.Labels) != len(m.Profile.Labels) {
				t.Errorf("labels: in=%d out=%d", len(m.Profile.Labels), len(back.Profile.Labels))
			}
			if len(back.Profile.Resources) != len(m.Profile.Resources) {
				t.Errorf("resources: in=%d out=%d", len(m.Profile.Resources), len(back.Profile.Resources))
			}
		})
	}
}

func TestMachineRoundTrip_AllocatableEmpty_DoesNotSerialize(t *testing.T) {
	// ADR-0022 / M45.0: an empty Allocatable means "consumer should fall
	// back to Profile.Resources." MachineToProto must NOT emit a redundant
	// Allocatable on the wire in that case.
	t.Parallel()
	m := machine.Machine{
		ID:    "m-1",
		State: machine.StateConfigured,
		Host:  machine.HostRef{Provider: "aws", Ref: "i-1"},
		Profile: machine.Profile{
			InstanceType: "c6a.4xlarge",
			Resources:    map[string]string{"cpu": "16"},
		},
	}
	pbMachine := conv.MachineToProto(m)
	if pbMachine.GetAllocatable() != nil {
		t.Fatalf("MachineToProto should not emit Allocatable when empty, got %v", pbMachine.GetAllocatable())
	}
	back, err := conv.MachineFromProto(pbMachine)
	if err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if len(back.Allocatable) != 0 {
		t.Errorf("round-tripped Allocatable should be empty, got %v", back.Allocatable)
	}
	// EffectiveAllocatable() must fall back to Profile.Resources.
	if back.EffectiveAllocatable()["cpu"] != "16" {
		t.Errorf("EffectiveAllocatable fallback failed: %v", back.EffectiveAllocatable())
	}
}

func TestMachineRoundTrip_AllocatableSet_PreservesField(t *testing.T) {
	t.Parallel()
	m := machine.Machine{
		ID:    "m-1",
		State: machine.StateConfigured,
		Host:  machine.HostRef{Provider: "aws", Ref: "i-1"},
		Profile: machine.Profile{
			InstanceType: "c6a.4xlarge",
			Resources:    map[string]string{"cpu": "1", "memory": "4Gi"},
		},
		Allocatable: map[string]string{"cpu": "16", "memory": "32Gi"},
	}
	pbMachine := conv.MachineToProto(m)
	if pbMachine.GetAllocatable() == nil {
		t.Fatalf("MachineToProto should emit Allocatable when set")
	}
	back, err := conv.MachineFromProto(pbMachine)
	if err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if back.Allocatable["cpu"] != "16" || back.Allocatable["memory"] != "32Gi" {
		t.Errorf("round-tripped Allocatable mismatch: %v", back.Allocatable)
	}
}

func TestRequirementsToProto_PreservesValues(t *testing.T) {
	t.Parallel()
	in := []needs.Requirement{
		{Key: "x", Operator: needs.OperatorIn, Values: []string{"a", "b"}},
		{Key: "y", Operator: needs.OperatorSame},
	}
	out := conv.RequirementsToProto(in)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	if out[0].GetOperator() != pb.NodeSelectorRequirement_OPERATOR_IN {
		t.Errorf("op[0] = %v", out[0].GetOperator())
	}
	if out[1].GetOperator() != pb.NodeSelectorRequirement_OPERATOR_SAME {
		t.Errorf("op[1] = %v", out[1].GetOperator())
	}
}
