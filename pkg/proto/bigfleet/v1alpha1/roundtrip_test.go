package bigfleetv1alpha1_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	bigfleetv1alpha1 "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// Round-trip tests for the wire formats. Each test marshals a
// representative value, unmarshals it, and checks that the result is
// equal under proto.Equal. The point isn't proto serialisation correctness
// (the upstream library covers that) — it's making sure the proto
// definitions cover every field we expect to set, with no surprises from
// missing oneof tags or wrong field numbers.

func TestRoundTrip_ClusterCapacityNeeds(t *testing.T) {
	t.Parallel()
	in := &bigfleetv1alpha1.ClusterCapacityNeeds{
		ClusterId:          "cluster-amsterdam-1",
		TimestampUnixNanos: 1714478400_000_000_000,
		Needs: []*bigfleetv1alpha1.CapacityNeed{
			{
				Requirements: []*bigfleetv1alpha1.NodeSelectorRequirement{
					{
						Key:      "node.kubernetes.io/instance-type",
						Operator: bigfleetv1alpha1.NodeSelectorRequirement_OPERATOR_IN,
						Values:   []string{"a3-highgpu-8g", "p5.48xlarge"},
					},
					{
						Key:      "topology.kubernetes.io/rack",
						Operator: bigfleetv1alpha1.NodeSelectorRequirement_OPERATOR_SAME,
					},
				},
				Resources: map[string]string{
					"cpu":            "96",
					"memory":         "768Gi",
					"nvidia.com/gpu": "8",
				},
				Priority: 1_000_000,
				Count:    64,
				Spread: []*bigfleetv1alpha1.TopologySpread{
					{
						TopologyKey:       "topology.kubernetes.io/zone",
						MaxSkew:           1,
						WhenUnsatisfiable: bigfleetv1alpha1.TopologySpread_WHEN_UNSATISFIABLE_DO_NOT_SCHEDULE,
					},
				},
				InterruptionPenaltyBucket: bigfleetv1alpha1.PenaltyBucket_PENALTY_BUCKET_8192,
				ReclamationPenaltyBucket:  bigfleetv1alpha1.PenaltyBucket_PENALTY_BUCKET_PINNED,
			},
		},
	}
	assertRoundTrip(t, in, &bigfleetv1alpha1.ClusterCapacityNeeds{})
}

func TestRoundTrip_OperatorMessage_OneOfPayloads(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  *bigfleetv1alpha1.OperatorMessage
	}{
		{
			name: "hello",
			msg: &bigfleetv1alpha1.OperatorMessage{
				Payload: &bigfleetv1alpha1.OperatorMessage_Hello{
					Hello: &bigfleetv1alpha1.Hello{
						ClusterId:       "cluster-amsterdam-1",
						Capabilities:    []string{"node-state-update", "available-capacity"},
						ProtocolVersion: "v1alpha1",
					},
				},
			},
		},
		{
			name: "rollup",
			msg: &bigfleetv1alpha1.OperatorMessage{
				Payload: &bigfleetv1alpha1.OperatorMessage_Rollup{
					Rollup: &bigfleetv1alpha1.ClusterCapacityNeeds{
						ClusterId: "cluster-amsterdam-1",
					},
				},
			},
		},
		{
			name: "bootstrap_response",
			msg: &bigfleetv1alpha1.OperatorMessage{
				Payload: &bigfleetv1alpha1.OperatorMessage_BootstrapResponse{
					BootstrapResponse: &bigfleetv1alpha1.BootstrapBlobResponse{
						RequestId:  "req-001",
						UserData:   []byte("#cloud-config\n…"),
						TtlSeconds: 600,
					},
				},
			},
		},
		{
			name: "bootstrap_response_error",
			msg: &bigfleetv1alpha1.OperatorMessage{
				Payload: &bigfleetv1alpha1.OperatorMessage_BootstrapResponse{
					BootstrapResponse: &bigfleetv1alpha1.BootstrapBlobResponse{
						RequestId: "req-002",
						Error:     "kubelet 1.40 outside cluster's skew window",
					},
				},
			},
		},
		{
			name: "reclaim_ack",
			msg: &bigfleetv1alpha1.OperatorMessage{
				Payload: &bigfleetv1alpha1.OperatorMessage_ReclaimAck{
					ReclaimAck: &bigfleetv1alpha1.ReclaimAck{
						InstructionId: "instr-7",
						NodesStarted:  64,
					},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertRoundTrip(t, tc.msg, &bigfleetv1alpha1.OperatorMessage{})
		})
	}
}

func TestRoundTrip_ShardMessage_OneOfPayloads(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  *bigfleetv1alpha1.ShardMessage
	}{
		{
			name: "ack",
			msg: &bigfleetv1alpha1.ShardMessage{
				Payload: &bigfleetv1alpha1.ShardMessage_Ack{
					Ack: &bigfleetv1alpha1.Acknowledgement{
						Echo:            "rollup",
						CoordinatorTerm: 3,
						ShardEpoch:      12,
					},
				},
			},
		},
		{
			name: "bootstrap_request",
			msg: &bigfleetv1alpha1.ShardMessage{
				Payload: &bigfleetv1alpha1.ShardMessage_BootstrapRequest{
					BootstrapRequest: &bigfleetv1alpha1.BootstrapRequest{
						RequestId: "req-001",
						Requirements: []*bigfleetv1alpha1.NodeSelectorRequirement{
							{Key: "feature.node.kubernetes.io/dra", Operator: bigfleetv1alpha1.NodeSelectorRequirement_OPERATOR_EXISTS},
						},
						ClusterId: "cluster-amsterdam-1",
					},
				},
			},
		},
		{
			name: "reclaim_instruction",
			msg: &bigfleetv1alpha1.ShardMessage{
				Payload: &bigfleetv1alpha1.ShardMessage_ReclaimInstruction{
					ReclaimInstruction: &bigfleetv1alpha1.ReclaimInstruction{
						InstructionId:      "instr-7",
						Nodes:              []string{"node-gpu-0142", "node-gpu-0143"},
						GracePeriodSeconds: 30,
						PreemptorPriority:  1_000_000,
					},
				},
			},
		},
		{
			name: "node_state_update",
			msg: &bigfleetv1alpha1.ShardMessage{
				Payload: &bigfleetv1alpha1.ShardMessage_NodeStateUpdate{
					NodeStateUpdate: &bigfleetv1alpha1.NodeStateUpdate{
						SupersedesKey:           "node:m-0142",
						MachineId:               "m-0142",
						ClusterId:               "cluster-amsterdam-1",
						State:                   bigfleetv1alpha1.MachineState_MACHINE_STATE_CONFIGURING,
						NodeName:                "node-gpu-0142",
						ProviderId:              "aws:///us-east-1a/i-0abc123def456",
						EstimatedReadyUnixNanos: 1714478500_000_000_000,
					},
				},
			},
		},
		{
			name: "available_capacity",
			msg: &bigfleetv1alpha1.ShardMessage{
				Payload: &bigfleetv1alpha1.ShardMessage_AvailableCapacity{
					AvailableCapacity: &bigfleetv1alpha1.AvailableCapacityUpdate{
						SupersedesKey:                "available:gpu-h100-east",
						AvailableCount:               200,
						Confidence:                   bigfleetv1alpha1.AvailableCapacityUpdate_CONFIDENCE_HIGH,
						CostPerHour:                  31.22,
						SupportsAtomicProvisioning:   true,
						EstimatedProvisioningSeconds: 180,
					},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertRoundTrip(t, tc.msg, &bigfleetv1alpha1.ShardMessage{})
		})
	}
}

func TestRoundTrip_Machine_AllStates(t *testing.T) {
	t.Parallel()
	for state := bigfleetv1alpha1.MachineState_MACHINE_STATE_SPECULATIVE; state <= bigfleetv1alpha1.MachineState_MACHINE_STATE_FAILED; state++ {
		state := state
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			m := &bigfleetv1alpha1.Machine{
				Id:                      "m-0001",
				State:                   state,
				InstanceType:            "p5.48xlarge",
				Zone:                    "us-east-1a",
				CapacityType:            bigfleetv1alpha1.CapacityType_CAPACITY_TYPE_ON_DEMAND,
				PricePerHour:            31.22,
				InterruptionProbability: 0.05,
				Resources:               &bigfleetv1alpha1.Resources{Resources: map[string]string{"cpu": "96"}},
				Labels:                  map[string]string{"accelerator-type": "nvidia-h100-80gb"},
			}
			if state != bigfleetv1alpha1.MachineState_MACHINE_STATE_SPECULATIVE && state != bigfleetv1alpha1.MachineState_MACHINE_STATE_CREATING {
				m.Host = &bigfleetv1alpha1.HostRef{Provider: "aws-eu-west-1", Ref: "i-0abc"}
			}
			if state == bigfleetv1alpha1.MachineState_MACHINE_STATE_FAILED {
				m.LastError = "configure timed out"
			}
			assertRoundTrip(t, m, &bigfleetv1alpha1.Machine{})
		})
	}
}

func TestRoundTrip_CoordinatorInstruction_OneOfPayloads(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		msg  *bigfleetv1alpha1.CoordinatorInstruction
	}{
		{
			name: "assign_domain",
			msg: &bigfleetv1alpha1.CoordinatorInstruction{
				CoordinatorTerm: 4,
				SequenceNumber:  100,
				InstructionId:   "instr-1",
				Payload: &bigfleetv1alpha1.CoordinatorInstruction_AssignDomain{
					AssignDomain: &bigfleetv1alpha1.AssignDomain{
						TopologyKey:   "topology.kubernetes.io/rack",
						TopologyValue: "rack-07-14",
					},
				},
			},
		},
		{
			name: "reassign_speculative",
			msg: &bigfleetv1alpha1.CoordinatorInstruction{
				CoordinatorTerm: 4,
				SequenceNumber:  101,
				InstructionId:   "instr-2",
				Payload: &bigfleetv1alpha1.CoordinatorInstruction_ReassignSpeculative{
					ReassignSpeculative: &bigfleetv1alpha1.ReassignSpeculative{
						MachineIds: []string{"m-spec-1", "m-spec-2"},
					},
				},
			},
		},
		{
			name: "cross_shard_drain",
			msg: &bigfleetv1alpha1.CoordinatorInstruction{
				CoordinatorTerm: 4,
				SequenceNumber:  102,
				InstructionId:   "instr-3",
				Payload: &bigfleetv1alpha1.CoordinatorInstruction_CrossShardDrain{
					CrossShardDrain: &bigfleetv1alpha1.CrossShardDrain{
						MachineIds:        []string{"m-1", "m-2"},
						PreemptorPriority: 1_000_000,
					},
				},
			},
		},
		{
			name: "transfer_ownership",
			msg: &bigfleetv1alpha1.CoordinatorInstruction{
				CoordinatorTerm: 4,
				SequenceNumber:  103,
				InstructionId:   "instr-4",
				Payload: &bigfleetv1alpha1.CoordinatorInstruction_TransferOwnership{
					TransferOwnership: &bigfleetv1alpha1.TransferOwnership{
						MachineIds:  []string{"m-1"},
						FromShardId: "shard-7",
						ToShardId:   "shard-12",
					},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertRoundTrip(t, tc.msg, &bigfleetv1alpha1.CoordinatorInstruction{})
		})
	}
}

func TestRoundTrip_ListFilter_SinceRevision(t *testing.T) {
	t.Parallel()
	in := &bigfleetv1alpha1.ListFilter{
		States: []bigfleetv1alpha1.MachineState{
			bigfleetv1alpha1.MachineState_MACHINE_STATE_IDLE,
			bigfleetv1alpha1.MachineState_MACHINE_STATE_CONFIGURED,
		},
		Zone:          "us-east-1a",
		InstanceType:  "p5.48xlarge",
		MaxResults:    1000,
		SinceRevision: []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}
	assertRoundTrip(t, in, &bigfleetv1alpha1.ListFilter{})
}

func TestRoundTrip_ShardReport_WithShortfalls(t *testing.T) {
	t.Parallel()
	in := &bigfleetv1alpha1.ShardReport{
		ShardId:            "shard-12",
		Cycle:              42,
		TimestampUnixNanos: 1714478400_000_000_000,
		ShardEpoch:         3,
		Summary: &bigfleetv1alpha1.ShardSummary{
			TotalMachines: 500_000,
			FreeMachines:  4_200,
			PerInstanceTypeCounts: map[string]int32{
				"p5.48xlarge": 100,
				"m6i.8xlarge": 4_100,
			},
			PerZoneCounts: map[string]int32{
				"us-east-1a": 200_000,
				"us-east-1b": 300_000,
			},
			UtilisationCpuFraction:    0.18,
			UtilisationMemoryFraction: 0.31,
		},
		Shortfalls: []*bigfleetv1alpha1.Shortfall{
			{
				Resources:                 &bigfleetv1alpha1.Resources{Resources: map[string]string{"nvidia.com/gpu": "8"}},
				Priority:                  1_000_000,
				Count:                     32,
				AgeCycles:                 6,
				InterruptionPenaltyBucket: bigfleetv1alpha1.PenaltyBucket_PENALTY_BUCKET_PINNED,
			},
		},
	}
	assertRoundTrip(t, in, &bigfleetv1alpha1.ShardReport{})
}

func assertRoundTrip(t *testing.T, in proto.Message, into proto.Message) {
	t.Helper()
	wire, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := proto.Unmarshal(wire, into); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(in, into) {
		t.Fatalf("round-trip mismatch:\n  in:  %v\n  out: %v", in, into)
	}
}
