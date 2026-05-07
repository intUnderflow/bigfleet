package decision_test

import (
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BenchmarkPhase1_Realistic_50K mirrors the chart's M44 default
// archetype catalog at the cloud run shape: 50 clusters × 1K Pods
// = 50K demand against a 60K Idle pool, 6 archetypes weighted by
// their realistic.yaml weights.
//
// The cloud run measured Phase 1 p99 = 8.2 s on PRO2-L (16 vCPU /
// 64 GiB). This bench reproduces that on the M5 Max so we can
// cpu-profile and find the bottleneck:
//
//	go test -bench=Phase1_Realistic -benchtime=10x -cpuprofile=/tmp/p1.prof ./pkg/decision/
//	go tool pprof -web /tmp/p1.prof
func BenchmarkPhase1_Realistic_50K(b *testing.B) {
	snap := buildRealisticIdle(b, 60_000)
	allNeeds := buildRealisticDemand(50, 1000)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase1(snap, allNeeds)
	}
}

// BenchmarkPhase1_Realistic_50K_AllSame mirrors the cloud-only
// regime where the operator's coLocationKey + ownerReference
// translation puts a Same() requirement on EVERY Need (each Pod
// has its own ownerRef UID, so each CR gets a unique group, but
// all are forced through takeCoLocated). ADR-0019 (M44.4) found
// this is the actual cloud bottleneck — the basic Realistic_50K
// bench above only exercises takeCoLocated for 15% of Needs
// (gpu-training + memory-db archetypes).
func BenchmarkPhase1_Realistic_50K_AllSame(b *testing.B) {
	snap := buildRealisticIdle(b, 60_000)
	allNeeds := buildRealisticDemandAllSame(50, 1000)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase1(snap, allNeeds)
	}
}

// archetypeSpec mirrors the realistic catalog: instance types, zones,
// size-bucket resources, priorities, sameRack hint. weight controls
// how many clusters / machines this archetype claims.
type archetypeSpec struct {
	name          string
	weight        int
	instanceTypes []string
	zones         []string
	sizes         []map[string]string // per-bucket resources
	priorities    []int32
	sameRack      bool
}

func realisticCatalog() []archetypeSpec {
	return []archetypeSpec{
		{
			name: "gpu-training", weight: 5,
			instanceTypes: []string{"a3-highgpu-8g"},
			zones:         []string{"zone-a", "zone-b", "zone-c"},
			sizes: []map[string]string{
				{"nvidia.com/gpu": "8"},
				{"nvidia.com/gpu": "16"},
			},
			priorities: []int32{1000},
			sameRack:   true,
		},
		{
			name: "gpu-inference", weight: 15,
			instanceTypes: []string{"a3-highgpu-1g", "a2-highgpu-1g"},
			zones:         []string{"zone-a", "zone-b", "zone-c"},
			sizes: []map[string]string{
				{"nvidia.com/gpu": "1"},
				{"nvidia.com/gpu": "2"},
			},
			priorities: []int32{100, 1000},
		},
		{
			name: "cpu-batch", weight: 35,
			instanceTypes: []string{"c6i.4xlarge", "c6i.8xlarge", "n2-standard-32"},
			zones:         []string{"zone-a", "zone-b", "zone-c"},
			sizes: []map[string]string{
				{"cpu": "4", "memory": "8Gi"},
				{"cpu": "8", "memory": "16Gi"},
				{"cpu": "16", "memory": "32Gi"},
				{"cpu": "32", "memory": "64Gi"},
			},
			priorities: []int32{100},
		},
		{
			name: "cpu-service", weight: 30,
			instanceTypes: []string{"m6i.large", "m6i.xlarge", "m6i.2xlarge"},
			zones:         []string{"zone-a", "zone-b", "zone-c"},
			sizes: []map[string]string{
				{"cpu": "2", "memory": "8Gi"},
				{"cpu": "4", "memory": "16Gi"},
				{"cpu": "8", "memory": "32Gi"},
				{"cpu": "16", "memory": "64Gi"},
			},
			priorities: []int32{1000},
		},
		{
			name: "memory-db", weight: 10,
			instanceTypes: []string{"r6i.xlarge", "r6i.2xlarge"},
			zones:         []string{"zone-a", "zone-b"},
			sizes: []map[string]string{
				{"cpu": "4", "memory": "32Gi"},
				{"cpu": "4", "memory": "64Gi"},
				{"cpu": "8", "memory": "128Gi"},
			},
			priorities: []int32{1000},
			sameRack:   true,
		},
		{
			name: "critical-realtime", weight: 5,
			instanceTypes: []string{"c6i.4xlarge", "m6i.2xlarge"},
			zones:         []string{"zone-a", "zone-b", "zone-c"},
			sizes: []map[string]string{
				{"cpu": "8", "memory": "16Gi"},
				{"cpu": "16", "memory": "32Gi"},
			},
			priorities: []int32{1000000},
		},
	}
}

// buildRealisticIdle pre-seeds n machines distributed across the
// realistic catalog, weighted. Each machine gets a (instanceType,
// zone, rack) profile so Same-rack archetypes have something to
// bucket on. Resources match a sampled size bucket for that archetype.
func buildRealisticIdle(b *testing.B, n int) *inventory.Snapshot {
	b.Helper()
	cat := realisticCatalog()
	totalWeight := 0
	for _, a := range cat {
		totalWeight += a.weight
	}

	inv := inventory.New()
	idx := 0
	for _, a := range cat {
		count := n * a.weight / totalWeight
		for i := 0; i < count; i++ {
			it := a.instanceTypes[i%len(a.instanceTypes)]
			zone := a.zones[i%len(a.zones)]
			rack := "rack-" + a.name + "-" + strconv.Itoa(i%8)
			size := a.sizes[i%len(a.sizes)]
			m := machine.Machine{
				ID:    machine.ID("idle-" + strconv.Itoa(idx)),
				State: machine.StateIdle,
				Profile: machine.Profile{
					InstanceType: it,
					Zone:         zone,
					CapacityType: machine.CapacityTypeBareMetal,
					Resources:    copyMap(size),
					Labels: map[string]string{
						"topology.bigfleet/rack": rack,
					},
				},
			}
			inv.Apply(m)
			idx++
		}
	}
	return inv.Snapshot()
}

// buildRealisticDemand creates clusters × per-cluster Needs, drawn
// from the realistic catalog by weight. Each cluster's Needs span
// archetypes; per-cluster all archetypes show up roughly in proportion.
func buildRealisticDemand(clusters, perCluster int) []needs.Need {
	cat := realisticCatalog()
	totalWeight := 0
	for _, a := range cat {
		totalWeight += a.weight
	}

	out := make([]needs.Need, 0, clusters*perCluster)
	for c := 0; c < clusters; c++ {
		clusterID := machine.ClusterID("cluster-" + strconv.Itoa(c))
		for i := 0; i < perCluster; i++ {
			// Weighted-pick an archetype.
			r := (c*perCluster + i) % totalWeight
			var a archetypeSpec
			for _, candidate := range cat {
				if r < candidate.weight {
					a = candidate
					break
				}
				r -= candidate.weight
			}

			it := a.instanceTypes[i%len(a.instanceTypes)]
			size := a.sizes[i%len(a.sizes)]
			pri := a.priorities[i%len(a.priorities)]

			reqs := []needs.Requirement{{
				Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{it},
			}}
			if a.sameRack {
				reqs = append(reqs, needs.Requirement{
					Key: "topology.bigfleet/rack", Operator: needs.OperatorSame,
				})
			}

			res := []needs.ResourceQty{}
			for k, v := range size {
				res = append(res, needs.ResourceQty{Name: k, Quantity: v})
			}

			profile := needs.NewProfile(reqs, res, nil, pri, needs.PenaltyBucket8192, needs.PenaltyBucket8192)
			out = append(out, needs.Need{
				ClusterID: clusterID,
				Profile:   profile,
				Count:     1,
			})
		}
	}
	return out
}

// buildRealisticDemandAllSame mirrors the cloud regime: every Need
// carries a Same(topology.bigfleet/rack) requirement (per-Pod
// ownerRef → operator translates to Same regardless of archetype's
// own sameRack flag). Each Need's archetype, instance type, size,
// priority is otherwise drawn from the realistic catalog.
func buildRealisticDemandAllSame(clusters, perCluster int) []needs.Need {
	cat := realisticCatalog()
	totalWeight := 0
	for _, a := range cat {
		totalWeight += a.weight
	}

	out := make([]needs.Need, 0, clusters*perCluster)
	for c := 0; c < clusters; c++ {
		clusterID := machine.ClusterID("cluster-" + strconv.Itoa(c))
		for i := 0; i < perCluster; i++ {
			r := (c*perCluster + i) % totalWeight
			var a archetypeSpec
			for _, candidate := range cat {
				if r < candidate.weight {
					a = candidate
					break
				}
				r -= candidate.weight
			}

			it := a.instanceTypes[i%len(a.instanceTypes)]
			size := a.sizes[i%len(a.sizes)]
			pri := a.priorities[i%len(a.priorities)]

			reqs := []needs.Requirement{
				{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{it}},
				{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame},
			}
			res := []needs.ResourceQty{}
			for k, v := range size {
				res = append(res, needs.ResourceQty{Name: k, Quantity: v})
			}
			profile := needs.NewProfile(reqs, res, nil, pri, needs.PenaltyBucket8192, needs.PenaltyBucket8192)
			out = append(out, needs.Need{ClusterID: clusterID, Profile: profile, Count: 1})
		}
	}
	return out
}

func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
