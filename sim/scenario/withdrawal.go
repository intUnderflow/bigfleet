package scenario

import (
	"fmt"
	"strconv"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/sim"
)

// withdrawal — paper §10. Steady state with N machines configured;
// cluster's needs go to zero; Phase 3 reclaims everything to Idle.
func withdrawal() sim.Scenario {
	idle := make([]sim.SeedMachine, 0, 32)
	for i := 0; i < 32; i++ {
		idle = append(idle, sim.SeedMachine{
			ID:           machine.ID("gpu-" + strconv.Itoa(i)),
			InstanceType: "a3-highgpu-8g",
			Zone:         "us-east-1a",
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    map[string]string{"nvidia.com/gpu": "8"},
		})
	}
	pf := needs.NewProfile(
		[]needs.Requirement{{
			Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn,
			Values: []string{"a3-highgpu-8g"},
		}},
		[]needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}},
		nil, 1_000_000,
		needs.PenaltyBucket8192, needs.PenaltyBucketPinned,
	)
	return sim.Scenario{
		Name:        "withdrawal",
		Description: "32 machines configured; cluster's needs drop to zero; Phase 3 reclaims to Idle.",
		InitialIdle: idle,
		Events: []sim.Event{
			{
				Cluster:     "cluster-train",
				Needs:       []needs.Need{{ClusterID: "cluster-train", Profile: pf, Count: 32}},
				CyclesAfter: 1,
			},
			{
				// Withdrawal: empty needs list.
				Cluster:     "cluster-train",
				Needs:       nil,
				CyclesAfter: 1,
			},
		},
		Assertions: []sim.Assertion{
			{
				Name: "all 32 machines back to Idle",
				Check: func(s *shard.Shard) error {
					if got := s.Inventory().CountByState(machine.StateIdle); got != 32 {
						return fmt.Errorf("idle = %d, want 32", got)
					}
					return nil
				},
			},
			{
				Name: "no machines configured for cluster-train",
				Check: func(s *shard.Shard) error {
					if got := s.Inventory().CountByState(machine.StateConfigured); got != 0 {
						return fmt.Errorf("configured = %d, want 0", got)
					}
					return nil
				},
			},
		},
	}
}

func init() {
	Register("withdrawal", withdrawal)
}
