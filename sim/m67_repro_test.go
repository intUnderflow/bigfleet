package sim_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// TestClosedLoop_ConsumedCapacityInvisible is the M67 step-1
// reproduction (plan §12; engine task #323) of the consumed-capacity
// attribution defect observed live on kind and cloud 2026-06-11.
//
// The model gap: the roll-up is the cluster's TOTAL desired state —
// one CR per Pod for the Pod's whole lifetime (ADR-0039, papers
// §6.1/§7), bound and pending alike — and needs.Need has no
// vocabulary for the bound/open split, nor machine.Machine a
// consumed/free field. Phase 1's existing-supply credit
// (occ/seed.go:84,144) and Phase 3's keep-set (phase3_reclaim.go
// claimMatching:178) both subtract machines' GROSS
// EffectiveAllocatable from that total, as if every machine were
// empty. The sim's cluster model does track per-machine consumption
// (clusterModel.remaining) for binding, but the rollup it derives
// cannot carry it — which is exactly the engine's wire-level view.
//
// Shape: per cluster, 20 Configured machines of (10cpu, 10Gi); 160
// bound filler Pods (1cpu, 1Gi) spread 8 per machine, leaving a
// (2cpu, 2Gi) residual on each; 5 open "big" Pods (4cpu, 2Gi) that
// fit the gross aggregate but no per-machine residual. Gross supply
// (200cpu, 200Gi) covers total demand (180cpu, 170Gi), so the gross
// arithmetic concludes everything is fine:
//
//   - Phase 1 reports zero unsatisfied — no shortfall, ever — and
//     acquires nothing, although idle big-fitting machines sit in the
//     pool and the scheduler holds 5 unplaceable Pods per cluster.
//   - Phase 3 independently derives 2 machines/cluster of gross
//     "excess" and reclaims them out from under bound Pods while that
//     open demand is pending.
//
// CHARACTERIZATION TEST: every assertion below pins the DEFECT. The
// M67 fix (consumption in the model, whichever side the ADR puts it
// on) must invert them — when this test starts failing, rewrite the
// assertions to the fixed contract: the open demand becomes visible
// as a deficit, the idle supply is acquired (pending → 0), and no
// machine is reclaimed out from under the demand it is hosting.
func TestClosedLoop_ConsumedCapacityInvisible(t *testing.T) {
	const (
		cycles, k          = 60, 30
		nClusters          = 2
		machinesPerCluster = 20
		fillerPerCluster   = 160 // 8/machine at density 10 → (2cpu,2Gi) residual each
		bigPerCluster      = 5   // (4cpu,2Gi) each: fits the gross aggregate, fits no residual
	)
	shapes := []sim.WorkloadShape{
		{
			Name:                       "filler",
			PodResources:               map[string]string{"cpu": "1", "memory": "1Gi"},
			InstanceTypes:              []string{"shared-fill.large"},
			Priority:                   100,
			InterruptionPenaltyDollars: 32,
			ReclamationPenaltyDollars:  64,
		},
		{
			// Same instance type as filler: the open demand competes for
			// exactly the machines whose gross the bound population consumed.
			Name:                       "big",
			PodResources:               map[string]string{"cpu": "4", "memory": "2Gi"},
			InstanceTypes:              []string{"shared-fill.large"},
			Priority:                   1000,
			InterruptionPenaltyDollars: 1024,
			ReclamationPenaltyDollars:  8192,
		},
	}
	// Filler listed first: both shapes are instance-typed (same bind
	// class), and the bind walk is stable in spec order, so the filler
	// population consumes the machines before the big Pods try — the
	// tail-of-fill state the live incident showed.
	workloads := []sim.WorkloadSpec{
		{Shape: "filler", Replicas: fillerPerCluster},
		{Shape: "big", Replicas: bigPerCluster},
	}
	clusters := []sim.ClusterSpec{
		{ID: "c1", Workloads: workloads},
		{ID: "c2", Workloads: workloads},
	}
	seeds := []sim.SeedPool{
		{Shape: "filler", Density: 10, ConfiguredPerCluster: machinesPerCluster},
		// Idle big-fitting supply the CORRECT engine would acquire for
		// the open demand. The defective zero deficit never touches it.
		{Shape: "big", Density: 1, Idle: 12},
	}
	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:              "consumed-capacity-invisible",
		Shapes:            shapes,
		Clusters:          clusters,
		Seeds:             seeds,
		ControllerManaged: true,
		CRPerPod:          true,
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	failed := false

	// DEFECT pin 1 — p1_unsatisfied=0 with unplaceable Pods: the open
	// big demand never registers as a deficit, so no shortfall is ever
	// recorded. Fixed engine: a deficit (and shortfall, until filled).
	for _, c := range res.Cycles {
		if c.Shortfalls != 0 {
			t.Errorf("cycle %d: shortfalls = %d; the gross-credit defect reports the open demand satisfied (fix landed? invert this test)", c.Cycle, c.Shortfalls)
			failed = true
			break
		}
	}

	// DEFECT pin 2 — no acquisition: 12 idle machines that fit the big
	// MinUnit sit unused for the whole run because Phase 1's gross math
	// sees zero deficit. Fixed engine: bootstraps them, pending → 0.
	if got := res.SumLast(cycles, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions }); got != 0 {
		t.Errorf("acquisitions over the run = %d; the gross-credit defect never acquires for the open demand (fix landed? invert this test)", got)
		failed = true
	}

	// DEFECT pin 3 — the open demand stays pending for the entire run.
	end := res.Last(1)[0]
	if want := nClusters * bigPerCluster; end.PendingPods != want {
		t.Errorf("pending = %d, want %d (the unplaceable open demand the engine cannot see)", end.PendingPods, want)
		failed = true
	}

	// DEFECT pin 4 — Phase 3 reclaims under the pending demand: gross
	// supply exceeds total demand by 2 machines/cluster, so Phase 3
	// reclaims machines that are hosting bound Pods (evicting them)
	// while the open demand is pending. Fixed engine: zero reclaims —
	// every machine's capacity is consumed or needed.
	reclaimedUnderPending := 0
	for _, c := range res.Cycles {
		if c.Reclaims > 0 && c.PendingPods > 0 {
			reclaimedUnderPending += c.Reclaims
		}
	}
	if reclaimedUnderPending == 0 {
		t.Errorf("no reclaims fired while open demand was pending; the defect reclaims gross 'excess' under pending demand (fix landed? invert this test)")
		failed = true
	}
	if got := res.SumLast(cycles, func(c sim.CycleStats) int { return c.Evicted }); got == 0 {
		t.Errorf("no Pods evicted; the defect's reclaims land on machines hosting bound Pods")
		failed = true
	}

	// DEFECT pin 5 — after the reclaim wave the loop goes quiet: the
	// engine believes the cluster is fully satisfied while the
	// scheduler holds the pending Pods. Quiescence-with-pending is the
	// defect's steady state, not convergence.
	if got := res.SumLast(k, churn); got != 0 {
		t.Errorf("churn over last %d cycles = %d, want 0 (the defect settles into satisfied-while-pending)", k, got)
		failed = true
	}

	if failed {
		dumpTrace(t, res)
	}
}
