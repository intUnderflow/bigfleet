package sim_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/sim"
)

// Cost-routing engine-correctness proof (#354). BigFleet has no "spot"
// concept: the decision engine routes capacity purely on
//
//	effective_cost = price + interruption_probability × interruption_penalty
//
// sortSpeculativeCandidates and Machine.EffectiveCost never read
// CapacityType. This test seeds two Speculative tiers that are identical
// in every routing-relevant dimension — same instance type, zones,
// resources, and CapacityType — and differ ONLY in (price, interruption
// probability):
//
//	stable:        price 1.0, interruption probability 0.05
//	interruptible: price 0.3, interruption probability 0.30
//
// effective_cost ties at interruption penalty ≈ (1.0−0.3)/(0.30−0.05) =
// 2.8. Two clusters run byte-identical workloads that differ only in
// interruption penalty:
//
//	cl-tol  (tolerant):  penalty $0  → below the flip → routes to interruptible
//	cl-sen  (sensitive): penalty $8  → above the flip → routes to stable
//
// If routing went by a "spot" / CapacityType label, the shared
// CapacityType would make the two tiers indistinguishable and a label-based
// router could not partition them at all. The proof is that interruption
// penalty alone splits the fleet across two tiers that are cost-distinct
// but type-identical — exactly the model the papers describe.
func TestClosedLoop_CostRouting_PenaltyPartitionsTiers(t *testing.T) {
	t.Parallel()

	const (
		stableProb        = 0.05 // mirrors seedClosedLoop specInterruptProb
		interruptibleProb = 0.30 // mirrors seedClosedLoop interruptibleSpecInterruptProb
		replicas          = 4
	)

	// Both shapes are identical except name + interruption penalty, so they
	// are candidates for the very same seeded machines and only the cost
	// sort can separate them.
	mkShape := func(name string, penalty float64) sim.WorkloadShape {
		return sim.WorkloadShape{
			Name:                       name,
			PodResources:               map[string]string{"cpu": "2", "memory": "8Gi"},
			InstanceTypes:              []string{"m6i.xlarge"},
			Zones:                      []string{"zone-a", "zone-b", "zone-c"},
			Priority:                   1000,
			InterruptionPenaltyDollars: penalty,
			ReclamationPenaltyDollars:  8192,
		}
	}

	sc := sim.ClosedLoopScenario{
		Name: "cost-routing-penalty-partitions-tiers",
		Shapes: []sim.WorkloadShape{
			mkShape("tolerant", 0),  // bucket $0 → below the ~2.8 flip
			mkShape("sensitive", 8), // bucket $8 → above the ~2.8 flip
		},
		Clusters: []sim.ClusterSpec{
			{ID: "cl-tol", Workloads: []sim.WorkloadSpec{{Shape: "tolerant", Replicas: replicas}}},
			{ID: "cl-sen", Workloads: []sim.WorkloadSpec{{Shape: "sensitive", Replicas: replicas}}},
		},
		// One pool. Its machine profile derives from "tolerant", but
		// "sensitive" has identical instance type / zones / resources, so the
		// seeded machines satisfy both clusters' Needs. Idle = 0 and
		// ConfiguredPerCluster = 0 force demand to walk to the Speculative
		// tiers, where the effective-cost sort (not the Idle raw-price sort)
		// decides. Each tier carries slack so neither cluster is pushed off
		// its preference by scarcity — any cross-tier landing is a routing
		// choice, not a shortage.
		Seeds: []sim.SeedPool{{
			Shape: "tolerant", Density: 1,
			Speculative: 6, InterruptibleSpeculative: 6,
		}},
		ControllerManaged: true,
		CRPerPod:          true,
		Cycles:            20,
	}

	res := runClosedLoop(t, sc)

	// Demand must be fully delivered, else the routing never happened and the
	// tier assertions below would pass vacuously on an empty fleet.
	end := res.Cycles[len(res.Cycles)-1]
	if end.Shortfalls != 0 || end.BoundPods != res.TargetPods {
		dumpTrace(t, res)
		t.Fatalf("not converged: shortfalls=%d (want 0), bound=%d (want %d)",
			end.Shortfalls, end.BoundPods, res.TargetPods)
	}

	// The tolerant cluster lands entirely on the cheap/high-interruption
	// tier; the sensitive cluster entirely on the expensive/stable tier.
	assertTier(t, res, "cl-tol", interruptibleProb, replicas)
	assertTier(t, res, "cl-sen", stableProb, replicas)
}

// assertTier asserts every Configured machine bound to cluster carries the
// expected interruption probability (i.e. landed on the intended tier) and
// that the cluster got the expected machine count.
func assertTier(t *testing.T, res *sim.ClosedLoopResult, cluster string, wantProb float64, wantCount int) {
	t.Helper()
	got := res.FinalSnapshot.ListByClusterState(machine.ClusterID(cluster), machine.StateConfigured)
	if len(got) != wantCount {
		t.Errorf("%s: %d Configured machines, want %d", cluster, len(got), wantCount)
	}
	for _, m := range got {
		if m.InterruptionProbability != wantProb {
			t.Errorf("%s: machine %s interruption probability = %g, want %g — routed to the wrong tier",
				cluster, m.ID, m.InterruptionProbability, wantProb)
		}
	}
}
