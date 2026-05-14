package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkTakeCoLocated_Repeated_50K — directly exercises the cache-
// reuse path: 50 000 takeCoLocated-bound Needs share ONE fingerprint
// against a 10 000-machine Idle pool with 8 distinct rack values.
//
// Pre-cache (M44.4 / ADR-0019): each call walks pool.src (10K
// MatchProfile + lookupAttribute) → ~1 ms/call → ~50 s for 50K calls.
//
// Post-cache: first call builds the bucketing (~1 ms), subsequent
// calls advance per-bucket head cursors (O(buckets) = ~8 ops) → mean
// per-call ~µs.
//
// 50K-shaped Phase 1 mean dropping from O(50s) to O(50ms) is what
// the cloud regime needs to bring cycle p99 under the 5 s envelope.
//
//	go test -bench=TakeCoLocated_Repeated -benchtime=1x ./pkg/decision/
func BenchmarkTakeCoLocated_Repeated_50K(b *testing.B) {
	const (
		idleN  = 10000
		racksN = 8
		needsN = 50000
	)

	// Build a single-archetype Idle pool: same instanceType + zone +
	// resources, but rack value rotates over racksN values.
	inv := inventory.New()
	for i := 0; i < idleN; i++ {
		rack := "rack-" + strconv.Itoa(i%racksN)
		inv.Apply(machine.Machine{
			ID:    machine.ID("idle-" + strconv.Itoa(i)),
			State: machine.StateIdle,
			Profile: machine.Profile{
				InstanceType: "c6i.4xlarge",
				Zone:         "zone-a",
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    map[string]string{"cpu": "4"},
				Labels:       map[string]string{"topology.bigfleet/rack": rack},
			},
		})
	}
	snap := inv.Snapshot()

	// All Needs share the same Profile fingerprint (single archetype +
	// Same(rack)). Different ClusterID per Need so they don't merge in
	// the deficit bookkeeping.
	unit := []needs.ResourceQty{{Name: "cpu", Quantity: "4"}}
	profile := needs.NewProfile(
		[]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"c6i.4xlarge"}},
			{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame},
		},
		nil, 1000, needs.PenaltyBucket8192, needs.PenaltyBucket8192,
	)
	allNeeds := make([]needs.Need, needsN)
	for i := 0; i < needsN; i++ {
		allNeeds[i] = needs.Need{
			ClusterID:          machine.ClusterID("c-" + strconv.Itoa(i)),
			Profile:            profile,
			AggregateResources: unit,
			MinUnit:            unit,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase1(snap, allNeeds)
	}
}
