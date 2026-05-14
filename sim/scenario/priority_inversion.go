package scenario

import (
	"fmt"
	"strconv"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/sim"
)

// priorityInversion — paper §8 + §10. Cluster A holds 4 GPU machines
// at priority 100K; cluster B arrives at priority 1M wanting 4. Phase
// 2 picks A's machines as victims and emits Preempt actions. Two
// cycles after the second event: A's machines are draining; once they
// reach Idle, cycle 3 should redirect to B.
//
// In the simulator the fake provider's instant-transitions mode
// collapses Drain → Idle within the same cycle, so 2 cycles after
// the second rollup is enough.
func priorityInversion() sim.Scenario {
	idle := make([]sim.SeedMachine, 0, 4)
	for i := 0; i < 4; i++ {
		idle = append(idle, sim.SeedMachine{
			ID:           machine.ID("gpu-" + strconv.Itoa(i)),
			InstanceType: "a3-highgpu-8g",
			Zone:         "us-east-1a",
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    map[string]string{"nvidia.com/gpu": "8"},
		})
	}
	gpuUnit := []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}}
	mkProfile := func(prio int32) needs.Profile {
		return needs.NewProfile(
			[]needs.Requirement{{
				Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn,
				Values: []string{"a3-highgpu-8g"},
			}},
			nil, prio,
			needs.PenaltyBucket8192, needs.PenaltyBucketPinned,
		)
	}
	low := mkProfile(100_000)
	high := mkProfile(1_000_000)
	return sim.Scenario{
		Name:        "priority-inversion",
		Description: "4 GPUs claimed by low-priority cluster A; high-priority B arrives wanting same. Preempt + reassign.",
		InitialIdle: idle,
		Events: []sim.Event{
			{
				Cluster: "cluster-batch",
				Needs: []needs.Need{{
					ClusterID:          "cluster-batch",
					Profile:            low,
					AggregateResources: needs.ScaleResources(gpuUnit, 4),
					MinUnit:            gpuUnit,
				}},
				CyclesAfter: 1,
			},
			{
				// cluster-train arrives. Keep cluster-batch's roll-up
				// in place by re-asserting it (full-replacement
				// semantics would otherwise withdraw it).
				Cluster: "cluster-train",
				Needs: []needs.Need{{
					ClusterID:          "cluster-train",
					Profile:            high,
					AggregateResources: needs.ScaleResources(gpuUnit, 4),
					MinUnit:            gpuUnit,
				}},
				// Two cycles: cycle A emits Preempt actions
				// (Configured→Idle in instant mode); cycle B's Phase 1
				// re-bootstraps the now-Idle machines for cluster-train.
				CyclesAfter: 2,
			},
		},
		Assertions: []sim.Assertion{
			{
				Name: "all 4 machines configured for cluster-train",
				Check: func(s *shard.Shard) error {
					inv := s.Inventory().Snapshot()
					configured := 0
					for _, m := range inv.All() {
						if m.State == machine.StateConfigured && m.Cluster == "cluster-train" {
							configured++
						}
					}
					if configured != 4 {
						return fmt.Errorf("configured for cluster-train = %d, want 4", configured)
					}
					return nil
				},
			},
			{
				Name: "no machines remain configured for cluster-batch",
				Check: func(s *shard.Shard) error {
					inv := s.Inventory().Snapshot()
					for _, m := range inv.All() {
						if m.State == machine.StateConfigured && m.Cluster == "cluster-batch" {
							return fmt.Errorf("machine %s still configured for cluster-batch", m.ID)
						}
					}
					return nil
				},
			},
		},
	}
}

func init() {
	Register("priority-inversion", priorityInversion)
}
