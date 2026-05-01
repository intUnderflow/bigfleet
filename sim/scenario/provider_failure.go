package scenario

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/sim"
)

// providerConfigureFailure exercises the engine's response to a
// transient provider failure. Before any cycle runs we queue a
// one-shot Configure failure for "gpu-0"; the engine catches the
// error, marks the machine Failed, and Phase 1 in the next cycle
// satisfies the remaining demand from the other idle machines.
//
// Asserts: 3 machines Configured for cluster-train at the end and
// no shortfalls remain. (The 4th machine — the failed one — sits
// in StateFailed; demand was 3, so we don't need it.)
func providerConfigureFailure() sim.Scenario {
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
		Name:        "provider-configure-failure",
		Description: "Provider's Configure on one machine fails; engine marks it Failed and satisfies demand from the rest.",
		InitialIdle: idle,
		BeforeRun: func(prov *fake.Provider) {
			prov.FailNext("gpu-0", machine.StateConfigured,
				errors.New("simulated configure failure"))
		},
		Events: []sim.Event{{
			Cluster: "cluster-train",
			Needs: []needs.Need{{
				ClusterID: "cluster-train", Profile: pf, Count: 3,
			}},
			CyclesAfter: 2, // first cycle attempts; second satisfies the remainder.
		}},
		Assertions: []sim.Assertion{
			{
				Name: "3 machines configured for cluster-train",
				Check: func(s *shard.Shard) error {
					inv := s.Inventory().Snapshot()
					configured := 0
					for _, m := range inv.All() {
						if m.State == machine.StateConfigured && m.Cluster == "cluster-train" {
							configured++
						}
					}
					if configured != 3 {
						return fmt.Errorf("configured for cluster-train = %d, want 3", configured)
					}
					return nil
				},
			},
			{
				Name: "exactly one machine in Failed with last_error",
				Check: func(s *shard.Shard) error {
					failedCount := 0
					for _, m := range s.Inventory().Snapshot().All() {
						if m.State == machine.StateFailed {
							failedCount++
							if m.LastError == "" {
								return fmt.Errorf("Failed machine %s: last_error empty", m.ID)
							}
						}
					}
					if failedCount != 1 {
						return fmt.Errorf("Failed machines = %d, want 1", failedCount)
					}
					return nil
				},
			},
		},
	}
}

// drainFailureDuringWithdrawal exercises the reclaim path under
// failure: 8 machines configured for a cluster, the cluster
// withdraws, and the Drain on gpu-0 fails. The other 7 reclaim
// cleanly to Idle; gpu-0 lands in Failed with last_error set.
func drainFailureDuringWithdrawal() sim.Scenario {
	idle := make([]sim.SeedMachine, 0, 8)
	for i := 0; i < 8; i++ {
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
		Name:        "drain-failure-withdrawal",
		Description: "Drain fails on one machine during cluster withdrawal; the other 7 reclaim cleanly.",
		InitialIdle: idle,
		BeforeRun: func(prov *fake.Provider) {
			prov.FailNext("gpu-0", machine.StateIdle,
				errors.New("simulated drain failure"))
		},
		Events: []sim.Event{
			{
				Cluster:     "cluster-train",
				Needs:       []needs.Need{{ClusterID: "cluster-train", Profile: pf, Count: 8}},
				CyclesAfter: 1,
			},
			{
				Cluster:     "cluster-train",
				Needs:       nil,
				CyclesAfter: 1,
			},
		},
		Assertions: []sim.Assertion{
			{
				Name: "7 machines back to Idle",
				Check: func(s *shard.Shard) error {
					if got := s.Inventory().CountByState(machine.StateIdle); got != 7 {
						return fmt.Errorf("idle = %d, want 7", got)
					}
					return nil
				},
			},
			{
				Name: "1 machine in Failed with last_error",
				Check: func(s *shard.Shard) error {
					failed := 0
					for _, m := range s.Inventory().Snapshot().All() {
						if m.State == machine.StateFailed {
							failed++
							if m.LastError == "" {
								return fmt.Errorf("Failed machine %s: last_error empty", m.ID)
							}
						}
					}
					if failed != 1 {
						return fmt.Errorf("Failed machines = %d, want 1", failed)
					}
					return nil
				},
			},
		},
	}
}

func init() {
	Register("provider-configure-failure", providerConfigureFailure)
	Register("drain-failure-withdrawal", drainFailureDuringWithdrawal)
}
