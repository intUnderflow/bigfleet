package needs_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkTableSnapshot_50K mirrors the cloud regime that ADR-0019
// surfaced as the actual Phase 1 cloud bottleneck: 50 K Needs spread
// across 50 clusters, each with a unique Profile fingerprint (the
// Same(rack) translation makes per-Pod-UID groups distinct). The
// pre-M44.4 implementation called sort.SliceStable on the full
// []Need; that triggered reflect-based typedmemmove of the full struct
// per swap and dominated cycle CPU at scale (~25 % of all CPU on
// scaleway-50k cloud). Post-M44.4 sorts an []uint32 permutation.
func BenchmarkTableSnapshot_50K(b *testing.B) {
	t := needs.NewTable()
	for c := 0; c < 50; c++ {
		clusterID := machine.ClusterID("cluster-" + strconv.Itoa(c))
		ns := make([]needs.Need, 1000)
		for i := 0; i < 1000; i++ {
			profile := needs.NewProfile(
				[]needs.Requirement{{
					Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn,
					Values: []string{"c6i.4xlarge"},
				}, {
					Key: "topology.bigfleet/rack", Operator: needs.OperatorSame,
				}},
				nil, 1000, needs.PenaltyBucket8192, needs.PenaltyBucket8192,
			)
			ns[i] = needs.Need{
				ClusterID:          clusterID,
				Profile:            profile,
				AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
				MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
				Group:              "g-" + strconv.Itoa(c) + "-" + strconv.Itoa(i),
				ArrivalUnixNanos:   int64(c*1000 + i),
			}
		}
		t.Replace(clusterID, ns)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Warm cache then dirty + re-snapshot to measure the sort path.
		_ = t.Snapshot()
		t.Replace(machine.ClusterID("cluster-0"), nil)
		_ = t.Snapshot()
	}
}
