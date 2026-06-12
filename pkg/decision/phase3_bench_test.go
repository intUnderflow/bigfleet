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
				Resources:    map[string]string{"cpu": "1"},
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
				nil,
				1_000_000,
				needs.PenaltyBucket8192,
				needs.PenaltyBucketPinned,
			)
			unit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}
			allNeeds = append(allNeeds, needs.Need{
				ClusterID:          clusterID,
				Profile:            profile,
				AggregateResources: needs.ScaleResources(unit, machinesPerCluster/needsPerCluster),
				MinUnit:            unit,
			})
		}
	}

	// ADR-0045: the attribution walk Phase 3 used to re-run lives in
	// Phase 1 (covered by the Phase 1 benches); the timed loop is what
	// remains of Phase 3 — the Configured-vs-claimed diff.
	claimed := decision.Phase1(snap, allNeeds).Claimed

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase3(snap, claimed, decision.AlwaysReady)
	}
}

// BenchmarkPhase3_M29Shape mirrors the per-shard load of the failed
// scaleway-1m M29 cloud run: 450K Configured machines, 50 clusters,
// every Configured machine pins to the same single instance type
// (a3-highgpu-8g — what the load-driver creates), one collapsed group
// per cluster with remaining > Configured count (so every machine is
// kept, no reclaim actions emitted).
//
// Live measurement was Phase 3 p99 = 255 ms on PRO2-L. Target after
// M30.2 lands: ≤ 30 ms on M5 Max (so PRO2-L tracks ≤ 100 ms).
func BenchmarkPhase3_M29Shape(b *testing.B) {
	const (
		clusterCount         = 50
		configuredPerCluster = 9_000
		needsPerCluster      = 10_000
	)
	const instType = "a3-highgpu-8g"

	inv := inventory.New()
	for c := 0; c < clusterCount; c++ {
		clusterID := machine.ClusterID("kwok-cluster-" + strconv.Itoa(c))
		for i := 0; i < configuredPerCluster; i++ {
			_ = inv.Insert(machine.Machine{
				ID:      machine.ID("conf-" + strconv.Itoa(c) + "-" + strconv.Itoa(i)),
				State:   machine.StateConfigured,
				Host:    machine.HostRef{Provider: "fake", Ref: strconv.Itoa(c*configuredPerCluster + i)},
				Cluster: clusterID,
				Profile: machine.Profile{
					InstanceType: instType,
					Zone:         "zone-a",
					CapacityType: machine.CapacityTypeBareMetal,
					Resources:    map[string]string{"cpu": "1"},
				},
				AssignedPriority:                   1_000_000,
				AssignedInterruptionPenaltyDollars: 8192,
				AssignedReclamationPenaltyDollars:  65536,
			})
		}
	}
	snap := inv.Snapshot()

	// One Need per cluster with Count = needsPerCluster, all pinned
	// to a3-highgpu-8g — the same shape the operator rolls up to the
	// shard in scaleway-1m.
	allNeeds := make([]needs.Need, 0, clusterCount)
	for c := 0; c < clusterCount; c++ {
		clusterID := machine.ClusterID("kwok-cluster-" + strconv.Itoa(c))
		profile := needs.NewProfile(
			[]needs.Requirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: needs.OperatorIn,
				Values:   []string{instType},
			}},
			nil,
			1000,
			needs.PenaltyBucket8192,
			needs.PenaltyBucket65536,
		)
		unit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}
		allNeeds = append(allNeeds, needs.Need{
			ClusterID:          clusterID,
			Profile:            profile,
			AggregateResources: needs.ScaleResources(unit, needsPerCluster),
			MinUnit:            unit,
		})
	}

	// ADR-0045: see BenchmarkPhase3_HighDemand — the walk is Phase 1's,
	// the timed loop is the residual diff.
	claimed := decision.Phase1(snap, allNeeds).Claimed

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase3(snap, claimed, decision.AlwaysReady)
	}
}
