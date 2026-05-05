package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkPhase2_ScaleInversions sets up a worst-case Phase 2 input:
// many configured machines (low priority, varied instance types) plus
// a batch of unresolved high-priority needs, each pinning to a single
// instance type. Phase 2 must score victims per need and pick the
// best matches.
//
// This is the §10.4 scenario the plan flagged: naive scoring is
// O(unresolved × configured). Run with:
//
//	go test -bench=Phase2_ScaleInversions -benchmem ./pkg/decision/...
func BenchmarkPhase2_ScaleInversions(b *testing.B) {
	const (
		configuredCount = 500_000 // M27 baseline: M13.gate's per-shard configured
		unresolvedCount = 100     // distinct higher-priority needs
		instanceTypeCt  = 5
	)

	types := []string{"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge"}

	inv := inventory.New()
	for i := 0; i < configuredCount; i++ {
		t := types[i%instanceTypeCt]
		_ = inv.Insert(machine.Machine{
			ID:                                 machine.ID("m-" + strconv.Itoa(i)),
			State:                              machine.StateConfigured,
			Host:                               machine.HostRef{Provider: "fake", Ref: strconv.Itoa(i)},
			Cluster:                            machine.ClusterID("c-" + strconv.Itoa(i%10)),
			Profile:                            machine.Profile{InstanceType: t, Zone: "zone-a", CapacityType: machine.CapacityTypeBareMetal},
			AssignedPriority:                   100, // low
			AssignedInterruptionPenaltyDollars: 1.0,
			AssignedReclamationPenaltyDollars:  1.0,
		})
	}
	snap := inv.Snapshot()

	unresolved := make([]decision.UnsatisfiedNeed, unresolvedCount)
	for i := 0; i < unresolvedCount; i++ {
		t := types[i%instanceTypeCt]
		p := needs.NewProfile(
			[]needs.Requirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: needs.OperatorIn,
				Values:   []string{t},
			}},
			nil, nil,
			1_000_000, // higher than the configured priority
			needs.PenaltyBucket8192,
			needs.PenaltyBucketPinned,
		)
		unresolved[i] = decision.UnsatisfiedNeed{
			Need:    needs.Need{ClusterID: machine.ClusterID("preempting"), Profile: p, Count: 5},
			Deficit: 5,
		}
	}

	opts := decision.DefaultPhase2Options()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase2(snap, unresolved, opts)
	}
}

// BenchmarkPhase2_NoPreemptableVictims models the M29 burst regime
// where Configured machines are seeded at top priority (no demand can
// preempt them) but Phase 2 still runs every cycle because some
// Phase 1 unresolveds may remain. The realistic shape is:
//
//	450K Configured at AssignedPriority = 1000000
//	100 unresolved needs at priority 1000 (load-driver default in
//	scaleway-1m) pinning to a3-highgpu-8g
//
// Without M30.1's short-circuit, Phase 2 walks 90K configured a3
// machines per Need × 100 Needs = 9M MatchProfile calls, all
// short-circuited on the priority filter. With M30.1, the snapshot's
// pre-computed minPriorityByStateInstanceType lets Phase 2 skip the
// walk entirely. Run with:
//
//	go test -bench=Phase2_NoPreemptableVictims -benchmem ./pkg/decision/...
func BenchmarkPhase2_NoPreemptableVictims(b *testing.B) {
	const (
		configuredCount = 450_000
		unresolvedCount = 100
		instanceTypeCt  = 5
	)

	types := []string{"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge"}

	inv := inventory.New()
	for i := 0; i < configuredCount; i++ {
		t := types[i%instanceTypeCt]
		_ = inv.Insert(machine.Machine{
			ID:                                 machine.ID("m-" + strconv.Itoa(i)),
			State:                              machine.StateConfigured,
			Host:                               machine.HostRef{Provider: "fake", Ref: strconv.Itoa(i)},
			Cluster:                            machine.ClusterID("c-" + strconv.Itoa(i%50)),
			Profile:                            machine.Profile{InstanceType: t, Zone: "zone-a", CapacityType: machine.CapacityTypeBareMetal},
			AssignedPriority:                   1_000_000,
			AssignedInterruptionPenaltyDollars: 8192,
			AssignedReclamationPenaltyDollars:  65536,
		})
	}
	snap := inv.Snapshot()

	unresolved := make([]decision.UnsatisfiedNeed, unresolvedCount)
	for i := 0; i < unresolvedCount; i++ {
		p := needs.NewProfile(
			[]needs.Requirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: needs.OperatorIn,
				Values:   []string{"a3-highgpu-8g"},
			}},
			nil, nil,
			1000,
			needs.PenaltyBucket8192,
			needs.PenaltyBucket65536,
		)
		unresolved[i] = decision.UnsatisfiedNeed{
			Need:    needs.Need{ClusterID: machine.ClusterID("c-" + strconv.Itoa(i%50)), Profile: p, Count: 5},
			Deficit: 5,
		}
	}

	opts := decision.DefaultPhase2Options()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase2(snap, unresolved, opts)
	}
}
