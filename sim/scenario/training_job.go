package scenario

import (
	"fmt"
	"strconv"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/sim"
)

// trainingJobTopology — paper §10. 64 GPU nodes available; cluster
// asks for 64. Phase 1 satisfies in one cycle. End state: 64
// Configured for cluster-train, 0 unsatisfied.
func trainingJobTopology() sim.Scenario {
	idle := make([]sim.SeedMachine, 0, 64)
	for i := 0; i < 64; i++ {
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
		Name:        "training-job-topology",
		Description: "64 GPU nodes available; cluster asks for 64. Phase 1 satisfies in one cycle.",
		InitialIdle: idle,
		Events: []sim.Event{{
			Cluster:     "cluster-train",
			Needs:       []needs.Need{{ClusterID: "cluster-train", Profile: pf, Count: 64}},
			CyclesAfter: 1,
		}},
		Assertions: []sim.Assertion{
			{
				Name: "all 64 machines configured for cluster-train",
				Check: func(s *shard.Shard) error {
					inv := s.Inventory().Snapshot()
					configured := 0
					for _, m := range inv.All() {
						if m.State == machine.StateConfigured && m.Cluster == "cluster-train" {
							configured++
						}
					}
					if configured != 64 {
						return fmt.Errorf("configured = %d, want 64", configured)
					}
					return nil
				},
			},
			{
				Name: "no unresolved shortfalls",
				Check: func(s *shard.Shard) error {
					if got := len(s.Shortfalls()); got != 0 {
						return fmt.Errorf("shortfalls = %d, want 0", got)
					}
					return nil
				},
			},
		},
	}
}

func init() {
	Register("training-job-topology", trainingJobTopology)
}
