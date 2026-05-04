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
		configuredCount = 100_000 // configured low-priority workload
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
