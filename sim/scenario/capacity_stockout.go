package scenario

import (
	"fmt"
	"strconv"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/sim"
)

// capacityStockout — paper §10. 64 needed, 32 idle. Phase 1 fills 32;
// the remaining 32 can't be satisfied (no preemption candidates) and
// surface as a shortfall.
func capacityStockout() sim.Scenario {
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
	gpuUnit := []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}}
	pf := needs.NewProfile(
		[]needs.Requirement{{
			Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn,
			Values: []string{"a3-highgpu-8g"},
		}},
		nil, 1_000_000,
		needs.PenaltyBucket8192, needs.PenaltyBucketPinned,
	)
	return sim.Scenario{
		Name:        "capacity-stockout",
		Description: "64 needed, 32 idle, no preemption candidates. 32 satisfied + 32 shortfall.",
		InitialIdle: idle,
		Events: []sim.Event{{
			Cluster: "cluster-train",
			Needs: []needs.Need{{
				ClusterID:          "cluster-train",
				Profile:            pf,
				AggregateResources: needs.ScaleResources(gpuUnit, 64),
				MinUnit:            gpuUnit,
			}},
			CyclesAfter: 2, // one cycle for assignment, one for shortfall to surface in tracking.
		}},
		Assertions: []sim.Assertion{
			{
				Name: "32 machines configured for cluster-train",
				Check: func(s *shard.Shard) error {
					inv := s.Inventory().Snapshot()
					configured := 0
					for _, m := range inv.All() {
						if m.State == machine.StateConfigured && m.Cluster == "cluster-train" {
							configured++
						}
					}
					if configured != 32 {
						return fmt.Errorf("configured = %d, want 32", configured)
					}
					return nil
				},
			},
			{
				Name: "shortfall recorded with gpu deficit 256",
				Check: func(s *shard.Shard) error {
					sfs := s.Shortfalls()
					if len(sfs) != 1 {
						return fmt.Errorf("shortfalls = %d, want 1", len(sfs))
					}
					gpu := ""
					for _, r := range sfs[0].Deficit {
						if r.Name == "nvidia.com/gpu" {
							gpu = r.Quantity
						}
					}
					if gpu != "256" {
						return fmt.Errorf("shortfall deficit nvidia.com/gpu = %q, want 256", gpu)
					}
					return nil
				},
			},
		},
	}
}

func init() {
	Register("capacity-stockout", capacityStockout)
}
