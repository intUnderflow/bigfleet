package occ

import (
	"fmt"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkAcquirableTotals_Uber5KShape is the regression guard for the
// ADR-0040 Addendum's joint-index hot path. The bigfleet-uber #52 run
// found the original implementation re-parsing resource.Quantity
// strings per member per Need (58 % of shard CPU, ~100 s cycles at
// ~2,500 co-located Needs); this benchmark reproduces that shape —
// one fingerprint class, a spec-pool-sized member set across 30
// domains, called once per Need — so any parse-on-the-hot-path
// regression shows up as a per-op explosion here long before a cloud
// run.
//
// uber-5k shape: ~12K acquirable machines fleet-wide, ~2,500 Same
// Needs per cycle. One b.N iteration = one Need's AcquirableTotals.
func BenchmarkAcquirableTotals_Uber5KShape(b *testing.B) {
	const members = 12000
	const racks = 30

	machines := make([]machine.Machine, 0, members)
	for i := 0; i < members; i++ {
		machines = append(machines, machine.Machine{
			ID:    machine.ID(fmt.Sprintf("spec-%d", i)),
			State: machine.StateSpeculative,
			Profile: machine.Profile{
				InstanceType: "r6i.2xlarge",
				Zone:         "zone-a",
				Labels: map[string]string{
					"topology.bigfleet/rack": fmt.Sprintf("zone-a-rack-%d", i%racks),
				},
				Resources: map[string]string{"cpu": "8", "memory": "64Gi"},
			},
		})
	}
	inv := inventory.New()
	for _, m := range machines {
		_ = inv.Insert(m)
	}
	snap := inv.Snapshot()

	profile := needs.NewProfile([]needs.Requirement{
		{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"r6i.2xlarge"}},
		{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame},
	}, nil, 1000, needs.PenaltyBucket(0), needs.PenaltyBucket(0))
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "8"}, {Name: "memory", Quantity: "64Gi"}}

	ix := NewSameSupplyIndex(snap)
	// Prime the lazy per-fingerprint build outside the timed loop —
	// the build is per-cycle-per-fingerprint; the per-Need cost is
	// what the #52 regression was made of.
	_ = ix.AcquirableTotals(profile, "topology.bigfleet/rack", minUnit, nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ix.AcquirableTotals(profile, "topology.bigfleet/rack", minUnit, nil)
	}
}
