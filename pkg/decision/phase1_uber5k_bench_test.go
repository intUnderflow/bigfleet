package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkPhase1_Uber5K_LateRun reproduces uber-5k's late-ramp state:
// 20 clusters × 250 Configured machines each (= 5K machines = profile
// target), small remaining Idle/Speculative pool, and per-cluster
// Needs reflecting aggregated demand at that point. Mirrors what
// Phase 1 sees during the last 3-5% of the ramp where bigfleet-uber #16
// measured shard cycle p99 = 1.019s (Phase 1 = 1.012s of that).
//
// Profile this bench to find the actual hotspot rather than
// extrapolate from the smaller Realistic_50K bench (which uses 0
// Configured + 60K Idle, i.e. a *pre-ramp* state where creditExistingSupply
// has no work to do).
//
//	go test -bench=Phase1_Uber5K -benchtime=10x -cpuprofile=/tmp/p1.prof ./pkg/decision/
//	go tool pprof -top -cum /tmp/p1.prof
func BenchmarkPhase1_Uber5K_LateRun(b *testing.B) {
	snap := buildUber5KLateRun(b)
	allNeeds := buildUber5KNeeds()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase1(snap, allNeeds)
	}
}

// buildUber5KLateRun seeds the inventory in the late-ramp state:
// each of 20 clusters has 250 Configured machines (mostly satisfying
// the cluster's demand), plus a partially-claimed Idle + Speculative
// pool that Phase 1's tail-end still walks.
func buildUber5KLateRun(b *testing.B) *inventory.Snapshot {
	b.Helper()
	inv := inventory.New()

	cat := realisticCatalog()
	totalWeight := 0
	for _, a := range cat {
		totalWeight += a.weight
	}

	// 5K Configured machines distributed across 20 clusters (250 each),
	// weighted by archetype. Each carries the matching (instance-type,
	// zone, rack, resources) so creditExistingSupply matches them.
	for c := 0; c < 20; c++ {
		cluster := machine.ClusterID("cluster-" + strconv.Itoa(c))
		for j := 0; j < 250; j++ {
			r := (c*250 + j) % totalWeight
			var a archetypeSpec
			for _, candidate := range cat {
				if r < candidate.weight {
					a = candidate
					break
				}
				r -= candidate.weight
			}
			it := a.instanceTypes[j%len(a.instanceTypes)]
			zone := a.zones[j%len(a.zones)]
			rack := "rack-" + a.name + "-" + strconv.Itoa(j%8)
			size := a.sizes[j%len(a.sizes)]
			inv.Apply(machine.Machine{
				ID:      machine.ID("cfg-" + strconv.Itoa(c) + "-" + strconv.Itoa(j)),
				State:   machine.StateConfigured,
				Cluster: cluster,
				Profile: machine.Profile{
					InstanceType: it,
					Zone:         zone,
					CapacityType: machine.CapacityTypeBareMetal,
					Resources:    copyMap(size),
					Labels: map[string]string{
						"topology.bigfleet/rack": rack,
					},
				},
				Allocatable: copyMap(size),
			})
		}
	}

	// 1K residual Idle (most of the 6K seed was claimed during ramp).
	for i := 0; i < 1000; i++ {
		r := i % totalWeight
		var a archetypeSpec
		for _, candidate := range cat {
			if r < candidate.weight {
				a = candidate
				break
			}
			r -= candidate.weight
		}
		it := a.instanceTypes[i%len(a.instanceTypes)]
		zone := a.zones[i%len(a.zones)]
		rack := "rack-" + a.name + "-" + strconv.Itoa(i%8)
		size := a.sizes[i%len(a.sizes)]
		inv.Apply(machine.Machine{
			ID:    machine.ID("idle-" + strconv.Itoa(i)),
			State: machine.StateIdle,
			Profile: machine.Profile{
				InstanceType: it, Zone: zone,
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    copyMap(size),
				Labels:       map[string]string{"topology.bigfleet/rack": rack},
			},
		})
	}

	// 12K residual Speculative.
	for i := 0; i < 12000; i++ {
		r := i % totalWeight
		var a archetypeSpec
		for _, candidate := range cat {
			if r < candidate.weight {
				a = candidate
				break
			}
			r -= candidate.weight
		}
		it := a.instanceTypes[i%len(a.instanceTypes)]
		zone := a.zones[i%len(a.zones)]
		rack := "rack-" + a.name + "-" + strconv.Itoa(i%8)
		size := a.sizes[i%len(a.sizes)]
		inv.Apply(machine.Machine{
			ID:    machine.ID("spec-" + strconv.Itoa(i)),
			State: machine.StateSpeculative,
			Profile: machine.Profile{
				InstanceType: it, Zone: zone,
				CapacityType: machine.CapacityTypeOnDemand,
				Resources:    copyMap(size),
				Labels:       map[string]string{"topology.bigfleet/rack": rack},
			},
			PricePerHour: 1.0,
		})
	}

	return inv.Snapshot()
}

// buildUber5KNeeds reproduces the operator's aggregated rollup for
// 20 clusters × ~12.5K aggregated demand each. With M35 label-axes
// producing ~8 distinct Profile fingerprints per cluster, this is
// ~160 Needs total at the shard side — and each Need carries the
// full aggregated AggregateResources for its (cluster, profile).
//
// If we instead simulate the un-aggregated path (one Need per CR),
// the count balloons to 250K, which is what BenchmarkPhase1_Uber5K_Unaggregated
// covers below.
func buildUber5KNeeds() []needs.Need {
	cat := realisticCatalog()
	totalWeight := 0
	for _, a := range cat {
		totalWeight += a.weight
	}

	// ~8 Profile fingerprints per cluster, 20 clusters = 160 Needs.
	out := make([]needs.Need, 0, 20*8)
	for c := 0; c < 20; c++ {
		cluster := machine.ClusterID("cluster-" + strconv.Itoa(c))
		for fp := 0; fp < 8; fp++ {
			r := (c*8 + fp) % totalWeight
			var a archetypeSpec
			for _, candidate := range cat {
				if r < candidate.weight {
					a = candidate
					break
				}
				r -= candidate.weight
			}
			it := a.instanceTypes[fp%len(a.instanceTypes)]
			size := a.sizes[fp%len(a.sizes)]
			pri := a.priorities[fp%len(a.priorities)]

			reqs := []needs.Requirement{{
				Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{it},
			}}
			if a.sameRack {
				reqs = append(reqs, needs.Requirement{
					Key: "topology.bigfleet/rack", Operator: needs.OperatorSame,
				})
			}

			// AggregateResources scaled by per-fingerprint share of cluster
			// demand: 25K Pods / 8 fingerprints ≈ 3125 Pods worth.
			res := []needs.ResourceQty{}
			for k, v := range size {
				res = append(res, needs.ResourceQty{Name: k, Quantity: v})
			}
			profile := needs.NewProfile(reqs, nil, pri, needs.PenaltyBucket8192, needs.PenaltyBucket8192)
			out = append(out, needs.Need{
				ClusterID:          cluster,
				Profile:            profile,
				AggregateResources: needs.ScaleResources(res, 3125),
				MinUnit:            res,
			})
		}
	}
	return out
}

// BenchmarkPhase1_Uber5K_Unaggregated tests the worst-case Phase 1
// input where the operator's rollup hasn't aggregated CRs into Needs —
// each CR is its own Need at the shard. Late-run uber-5k late state:
// 5K Configured + 250K un-aggregated Needs. This is the "if operator
// rollup aggregation isn't working" stress test.
func BenchmarkPhase1_Uber5K_Unaggregated(b *testing.B) {
	snap := buildUber5KLateRun(b)
	// 20 clusters × 12500 individual Needs = 250K total, matching the
	// inner agent's "250K active CRs" framing if CRs are not aggregated.
	allNeeds := buildRealisticDemand(20, 12500)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase1(snap, allNeeds)
	}
}
