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
// shape inner agent measured at uber-50k (bigfleet-uber #17):
// a single sameRack pool with ~12K machines per bucket. The previous
// shape (score loop walking O(unclaimed-in-bucket) per call) cost
// ~9.41 ms/call at production scale. After the incremental bucket
// aggregate cache, the per-call work becomes O(buckets) — bench
// measures both the bucket-build cost and the post-cache per-call
// fast path.
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

func buildUber50KSinglePoolNeeds() []needs.Need {
	const clustersN = 20_000
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
