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
				Resources: map[string]string{"cpu": "96", "nvidia.com/gpu": "8"},
				Priority:  1_000_000,
				Count:     64,
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
	if n.Count != 64 {
		t.Errorf("Count = %d", n.Count)
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
