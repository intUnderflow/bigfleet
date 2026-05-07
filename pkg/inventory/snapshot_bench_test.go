package inventory_test

import (
	"fmt"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// BenchmarkSnapshot mirrors the scaleway-50k cloud shape (M44.4 Drop A
// snapread regression): 60 K total inventory split between Idle (heavy)
// and Configured (lighter), six instance types, 50 clusters. Confirms
// the post-Drop-A snapshotLocked p99 (~477 ms on Kapsule) and validates
// any optimisation against that baseline.
func BenchmarkSnapshot(b *testing.B) {
	inv := inventory.New()
	const (
		clusters      = 50
		idlePer       = 800 // 50 × 800 = 40 000 Idle
		configuredPer = 400 // 50 × 400 = 20 000 Configured (close to the 17 K observed mid-run)
	)
	instanceTypes := []string{"a3-highgpu-8g", "n2-standard-32", "c3-highcpu-176", "m3-megamem-128", "g2-standard-16", "h3-standard-88"}
	for c := 0; c < clusters; c++ {
		cluster := machine.ClusterID(fmt.Sprintf("cluster-%d", c))
		for i := 0; i < idlePer; i++ {
			it := instanceTypes[i%len(instanceTypes)]
			id := machine.ID(fmt.Sprintf("m-idle-%d-%d", c, i))
			m := machine.Machine{
				ID:           id,
				State:        machine.StateIdle,
				Host:         machine.HostRef{Provider: "fake", Ref: string(id)},
				Profile:      machine.Profile{InstanceType: it, Zone: "fr-par-1"},
				PricePerHour: 1.0 + float64(i%17)*0.05,
			}
			if err := inv.Insert(m); err != nil {
				b.Fatalf("insert idle: %v", err)
			}
		}
		for i := 0; i < configuredPer; i++ {
			it := instanceTypes[i%len(instanceTypes)]
			id := machine.ID(fmt.Sprintf("m-cfg-%d-%d", c, i))
			m := machine.Machine{
				ID:                                id,
				State:                             machine.StateConfigured,
				Host:                              machine.HostRef{Provider: "fake", Ref: string(id)},
				Cluster:                           cluster,
				Profile:                           machine.Profile{InstanceType: it, Zone: "fr-par-1"},
				PricePerHour:                      1.0 + float64(i%17)*0.05,
				AssignedPriority:                  1000 + int32(i%500),
				AssignedReclamationPenaltyDollars: float64(i % 8192),
			}
			if err := inv.Insert(m); err != nil {
				b.Fatalf("insert configured: %v", err)
			}
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = inv.Snapshot()
	}
}
