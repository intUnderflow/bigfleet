package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkPhase3_HighDemand mirrors the per-shard shape M13.gate
// drove on Scaleway nl-ams: 500K Configured machines, 50 clusters,
// each cluster with one (or a handful of) demand profiles whose
// counts roughly equal that cluster's share of inventory.
//
// At this load the M13.gate run measured Phase 3 p99 at 416 ms — the
// algorithmic wall M27 is built to push past. Run via
//
//	go test -bench=Phase3_HighDemand -benchtime=3x -benchmem ./pkg/decision/...
//
// Target after M27 lands: per-cycle Phase 3 ≤ 100 ms on M5 Max
// (PRO2-M is ~2-3× slower per core, so the cloud target is the same
// 100 ms cycle SLO).
func BenchmarkPhase3_HighDemand(b *testing.B) {
	const (
		clusterCount    = 50
		configuredCount = 500_000
		needsPerCluster = 3 // distinct profile fingerprints per cluster
		instanceTypeCt  = 5
	)
	types := []string{"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge"}

	inv := inventory.New()
	for i := 0; i < configuredCount; i++ {
		t := types[i%instanceTypeCt]
		_ = inv.Insert(machine.Machine{
			ID:    machine.ID("m-" + strconv.Itoa(i)),
			State: machine.StateConfigured,
			Host:  machine.HostRef{Provider: "fake", Ref: strconv.Itoa(i)},
			// Round-robin across clusters: every clusterCount-th machine
			// belongs to the same cluster.
			Cluster: machine.ClusterID("c-" + strconv.Itoa(i%clusterCount)),
			Profile: machine.Profile{
				InstanceType: t,
				Zone:         "zone-a",
				CapacityType: machine.CapacityTypeOnDemand,
			},
			AssignedPriority:                   100,
			AssignedInterruptionPenaltyDollars: 1.0,
			AssignedReclamationPenaltyDollars:  1.0,
		})
	}
	snap := inv.Snapshot()

	// Each cluster's needs ask for needsPerCluster distinct profile
	// fingerprints, with counts that roughly add up to the cluster's
	// share of inventory (~10K per cluster). Phase 3 should keep most
	// machines and reclaim few.
	allNeeds := make([]needs.Need, 0, clusterCount*needsPerCluster)
	machinesPerCluster := configuredCount / clusterCount
	for c := 0; c < clusterCount; c++ {
		clusterID := machine.ClusterID("c-" + strconv.Itoa(c))
		for k := 0; k < needsPerCluster; k++ {
			t := types[(c+k)%instanceTypeCt]
			profile := needs.NewProfile(
				[]needs.Requirement{{
					Key:      "node.kubernetes.io/instance-type",
					Operator: needs.OperatorIn,
					Values:   []string{t},
				}},
				nil, nil,
				1_000_000,
				needs.PenaltyBucket8192,
				needs.PenaltyBucketPinned,
			)
			allNeeds = append(allNeeds, needs.Need{
				ClusterID: clusterID,
				Profile:   profile,
				Count:     machinesPerCluster / needsPerCluster,
			})
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase3(snap, allNeeds)
	}
}
