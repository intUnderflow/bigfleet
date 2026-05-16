package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkPhase1_Uber50K_BucketSizedBuckets reproduces the per-pool
// inventory shape inner agent measured on uber-50k (bigfleet-uber #17):
//
//   - ~20K machines in one (state, profile fingerprint) pool that
//     routes through takeCoLocated (sameRack archetype)
//   - 8 distinct rack values → ~2.5K machines per bucket
//   - Many Needs hitting the SAME pool (one fingerprint per
//     archetype × cluster, but archetype dominance means most Needs
//     land on a handful of pools)
//
// The existing BenchmarkPhase1_Uber5K_LateRun has 21K machines TOTAL
// spread across all states + fingerprints, so the per-pool size is
// far smaller and the per-call cost looks artificially constant. At
// uber-50k inner measured 9.41 ms/takeCoLocated call vs uber-5k's
// ~130 µs/call (72× scaling for 7.6× inventory growth — superlinear
// in inventory). Reproducing that on the local bench is the first
// step before any optimization.
//
//	go test -bench=Phase1_Uber50K_BucketSizedBuckets -benchtime=10x ./pkg/decision/
func BenchmarkPhase1_Uber50K_BucketSizedBuckets(b *testing.B) {
	snap := buildUber50KSinglePool(b)
	allNeeds := buildUber50KSinglePoolNeeds()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase1(snap, allNeeds)
	}
}

// buildUber50KSinglePool seeds one Idle pool with 20 000 machines
// sharing the same (instanceType, zone, resources) — a single
// sameRack archetype scaled up. 8 distinct rack values give us 2.5K
// machines per coLocated bucket, matching what inner agent measured
// per-pool at uber-50k 11-host scale.
func buildUber50KSinglePool(b *testing.B) *inventory.Snapshot {
	b.Helper()
	const (
		idleN  = 100_000
		racksN = 8
	)
	inv := inventory.New()
	for i := 0; i < idleN; i++ {
		rack := "rack-mem-" + strconv.Itoa(i%racksN)
		inv.Apply(machine.Machine{
			ID:    machine.ID("idle-" + strconv.Itoa(i)),
			State: machine.StateIdle,
			Profile: machine.Profile{
				InstanceType: "r6i.2xlarge",
				Zone:         "zone-a",
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    map[string]string{"cpu": "4", "memory": "32Gi"},
				Labels:       map[string]string{"topology.bigfleet/rack": rack},
			},
		})
	}
	return inv.Snapshot()
}

// buildUber50KSinglePoolNeeds produces Needs that all match the
// same single pool (same instance type + sameRack rack-key). One
// Need per cluster keeps the deficit small so each call claims ~1
// machine — the realistic "small co-location group" shape that
// drives 99.9% of Phase 1 work through takeCoLocated at uber-50k.
//
// Need count is sized so the score loop runs many times against
// a stable bucket layout, exposing the per-call cost without being
// drowned by cache-build amortization.
func buildUber50KSinglePoolNeeds() []needs.Need {
	const clustersN = 20_000 // ~half the inventory; each Need claims one
	unit := []needs.ResourceQty{{Name: "cpu", Quantity: "4"}, {Name: "memory", Quantity: "32Gi"}}
	profile := needs.NewProfile(
		[]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"r6i.2xlarge"}},
			{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame},
		},
		nil, 1000, needs.PenaltyBucket8192, needs.PenaltyBucket8192,
	)
	out := make([]needs.Need, clustersN)
	for i := 0; i < clustersN; i++ {
		out[i] = needs.Need{
			ClusterID:          machine.ClusterID("c-" + strconv.Itoa(i)),
			Profile:            profile,
			AggregateResources: unit,
			MinUnit:            unit,
		}
	}
	return out
}
