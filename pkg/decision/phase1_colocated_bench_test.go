package decision_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// The post-ADR-0039/0040 uber-5k Need shape. ADR-0039 made every Pod
// carry a CR, so every co-location group — not just the ones with
// pending Pods — produces its own Same-carrying Need: the measured
// shard state is ~2,600 Needs, ~93 % of them gangs (phasedump:
// needs_total≈2,680, needs_same≈2,500), against 5K Configured plus a
// shard-wide Idle + Speculative pool that the ADR-0040 Addendum's
// joint-domain index walks per gang Need.
//
// BenchmarkPhase1_Uber5K_LateRun predates this shape (160 Needs, no
// gang cardinality) — which is why the #52 quantity-parsing regression
// (~100 s cycles in the cloud) was invisible to it. These benches pin
// the co-located shape for both phases; a parse-on-the-hot-path or
// per-Need pool-walk regression shows up here as a per-op explosion
// long before a cloud run.
//
//	go test -bench=CoLocated -benchtime=5x ./pkg/decision/
const (
	colocClusters         = 20
	colocMemcacheGroups   = 95 // groups of 4 — the stateful/memcache gang class
	colocGPUGroups        = 32 // groups of 5 — the whole-machine GPU gang class
	colocConfiguredPerCl  = 250
	colocIdleShardWide    = 1000
	colocSpecShardWide    = 12000
	colocRackBlock        = 8 // rack-contiguous block size for gang archetypes (ADR-0040 §4)
	colocRacksPerArchetyp = 12
)

func colocRes(cpu, mem string, extra ...needs.ResourceQty) []needs.ResourceQty {
	out := []needs.ResourceQty{{Name: "cpu", Quantity: cpu}}
	if mem != "" {
		out = append(out, needs.ResourceQty{Name: "memory", Quantity: mem})
	}
	return append(out, extra...)
}

func colocProfile(instanceTypes []string, same bool, pri int32) needs.Profile {
	reqs := []needs.Requirement{{
		Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: instanceTypes,
	}}
	if same {
		reqs = append(reqs, needs.Requirement{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame})
	}
	return needs.NewProfile(reqs, nil, pri, needs.PenaltyBucket8192, needs.PenaltyBucket8192)
}

// buildCoLocatedNeeds returns the measured post-ADR-0039 demand: per
// cluster, two large aggregated plain Needs (tiny + service) and one
// Need per co-location group.
func buildCoLocatedNeeds() []needs.Need {
	tiny := colocRes("400m", "500Mi")
	service := colocRes("2200m", "8500Mi")
	memcache := colocRes("4", "32Gi")
	gpu := colocRes("64", "", needs.ResourceQty{Name: "nvidia.com/gpu", Quantity: "8"})

	tinyProfile := colocProfile([]string{"m6i.large", "m6i.xlarge"}, false, 100)
	serviceProfile := colocProfile([]string{"m6i.xlarge"}, false, 1000)
	memcacheProfile := colocProfile([]string{"r6i.2xlarge"}, true, 1000)
	gpuProfile := colocProfile([]string{"a3-highgpu-8g"}, true, 1000)

	out := make([]needs.Need, 0, colocClusters*(2+colocMemcacheGroups+colocGPUGroups))
	for c := 0; c < colocClusters; c++ {
		cluster := machine.ClusterID("cluster-" + strconv.Itoa(c))
		out = append(out,
			needs.Need{
				ClusterID: cluster, Profile: tinyProfile,
				AggregateResources: needs.ScaleResources(tiny, 17500), MinUnit: tiny,
			},
			needs.Need{
				ClusterID: cluster, Profile: serviceProfile,
				AggregateResources: needs.ScaleResources(service, 3000), MinUnit: service,
			},
		)
		// Every gang is its own Need: same Profile content (one
		// fingerprint class per archetype — the joint index caches per
		// fingerprint), distinct aggregation identity.
		for g := 0; g < colocMemcacheGroups; g++ {
			out = append(out, needs.Need{
				ClusterID: cluster, Profile: memcacheProfile,
				AggregateResources: needs.ScaleResources(memcache, 4), MinUnit: memcache,
			})
		}
		for g := 0; g < colocGPUGroups; g++ {
			out = append(out, needs.Need{
				ClusterID: cluster, Profile: gpuProfile,
				AggregateResources: needs.ScaleResources(gpu, 5), MinUnit: gpu,
			})
		}
	}
	return out
}

// buildCoLocatedInventory seeds the uber-5k steady-state pool: 250
// Configured per cluster (gang archetypes rack-blocked per ADR-0040
// §4), plus the shard-wide Idle + Speculative tiers whose per-gang
// member lists are exactly what the joint-domain index walks per Need.
func buildCoLocatedInventory(b *testing.B) *inventory.Snapshot {
	b.Helper()
	inv := inventory.New()

	add := func(id string, st machine.State, cluster machine.ClusterID, it, zone, rack string, res map[string]string, density int) {
		labels := map[string]string{}
		if rack != "" {
			labels["topology.bigfleet/rack"] = rack
		}
		alloc := map[string]string{}
		for k, v := range res {
			q := needs.ScaleResources([]needs.ResourceQty{{Name: k, Quantity: v}}, density)
			alloc[k] = q[0].Quantity
		}
		_ = inv.Insert(machine.Machine{
			ID:      machine.ID(id),
			State:   st,
			Cluster: cluster,
			Host:    machine.HostRef{Provider: "fake", Ref: id},
			Profile: machine.Profile{
				InstanceType: it,
				Zone:         zone,
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    res,
				Labels:       labels,
			},
			Allocatable:                        alloc,
			AssignedPriority:                   1000,
			AssignedInterruptionPenaltyDollars: 8192,
			AssignedReclamationPenaltyDollars:  8192,
		})
	}

	rackFor := func(arch string, i int) string {
		return "zone-a-" + arch + "-rack-" + strconv.Itoa((i/colocRackBlock)%colocRacksPerArchetyp)
	}

	for c := 0; c < colocClusters; c++ {
		cluster := machine.ClusterID("cluster-" + strconv.Itoa(c))
		cl := strconv.Itoa(c)
		for i := 0; i < 175; i++ {
			add("cfg-tiny-"+cl+"-"+strconv.Itoa(i), machine.StateConfigured, cluster,
				"m6i.large", "zone-a", "", map[string]string{"cpu": "400m", "memory": "500Mi"}, 100)
		}
		for i := 0; i < 30; i++ {
			add("cfg-svc-"+cl+"-"+strconv.Itoa(i), machine.StateConfigured, cluster,
				"m6i.xlarge", "zone-a", "", map[string]string{"cpu": "2200m", "memory": "8500Mi"}, 10)
		}
		for i := 0; i < 30; i++ {
			add("cfg-mc-"+cl+"-"+strconv.Itoa(i), machine.StateConfigured, cluster,
				"r6i.2xlarge", "zone-a", rackFor("r6i", i), map[string]string{"cpu": "4", "memory": "32Gi"}, 10)
		}
		for i := 0; i < 15; i++ {
			add("cfg-gpu-"+cl+"-"+strconv.Itoa(i), machine.StateConfigured, cluster,
				"a3-highgpu-8g", "zone-a", rackFor("a3", i), map[string]string{"cpu": "64", "nvidia.com/gpu": "8"}, 1)
		}
	}

	idleTypes := []struct {
		it, cpu, mem, gpuQ, arch string
		density                  int
	}{
		{"m6i.large", "400m", "500Mi", "", "m6i", 100},
		{"r6i.2xlarge", "4", "32Gi", "", "r6i", 10},
		{"a3-highgpu-8g", "64", "", "8", "a3", 1},
	}
	for i := 0; i < colocIdleShardWide; i++ {
		t := idleTypes[i%len(idleTypes)]
		res := map[string]string{"cpu": t.cpu}
		if t.mem != "" {
			res["memory"] = t.mem
		}
		if t.gpuQ != "" {
			res["nvidia.com/gpu"] = t.gpuQ
		}
		add("idle-"+strconv.Itoa(i), machine.StateIdle, "",
			t.it, "zone-a", rackFor(t.arch, i), res, t.density)
	}
	for i := 0; i < colocSpecShardWide; i++ {
		t := idleTypes[i%len(idleTypes)]
		res := map[string]string{"cpu": t.cpu}
		if t.mem != "" {
			res["memory"] = t.mem
		}
		if t.gpuQ != "" {
			res["nvidia.com/gpu"] = t.gpuQ
		}
		add("spec-"+strconv.Itoa(i), machine.StateSpeculative, "",
			t.it, "zone-a", rackFor(t.arch, i), res, t.density)
	}
	return inv.Snapshot()
}

func BenchmarkPhase1_Uber5K_CoLocated(b *testing.B) {
	snap := buildCoLocatedInventory(b)
	allNeeds := buildCoLocatedNeeds()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase1(snap, allNeeds)
	}
}

func BenchmarkPhase3_Uber5K_CoLocated(b *testing.B) {
	snap := buildCoLocatedInventory(b)
	allNeeds := buildCoLocatedNeeds()

	// ADR-0045: Phase 3 no longer walks demand — the attribution cost
	// moved into Phase 1's pre-pass (BenchmarkPhase1_Uber5K_CoLocated
	// guards it). The timed loop is the surviving per-cycle Phase 3
	// work: the Configured-vs-claimed diff over the whole fleet.
	claimed := decision.Phase1(snap, allNeeds).Claimed

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = decision.Phase3(snap, claimed, decision.AlwaysReady, decision.ReleasePolicy{}, time.Time{})
	}
}
