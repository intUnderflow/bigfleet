package sim_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// boundCountsScenario is the M67 shape, inverted into the ADR-0045
// contract pin. History: the original TestClosedLoop_ConsumedCapacityInvisible
// characterized a "defect" — p1_unsatisfied=0 with unplaceable Pods,
// Phase 3 reclaiming under unchanged demand — and demanded the engine
// learn per-machine consumption. The author's correction (ADR-0045)
// reframed it: clusters demand capacity, BigFleet fulfills by binding,
// and any arithmetic anticipating whether the cluster's scheduler can
// USE bound capacity is scheduler-shadowing, out of scope by design.
//
// Shape: per cluster, 20 Configured machines of (10cpu, 10Gi); 180
// bound filler Pods (1cpu, 1Gi) at 9 per machine, leaving a (1cpu,
// 1Gi) residual on each; 5 "big" Pods (4cpu, 2Gi) that fit no
// residual and stay pending forever. Bound capacity (200cpu, 200Gi)
// covers total demand (200cpu, 190Gi) — and consumes it whole at
// machine granularity, so there is no excess machine to shrink away.
// 12 idle big-fitting machines sit in the pool as bait: an engine
// that modeled consumption would "see" the unplaceable big demand and
// acquire them; the contract engine must not.
func boundCountsScenario(name string, cycles int, scales []sim.TargetScale) sim.ClosedLoopScenario {
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
			// Same instance type as filler: the pending demand competes
			// for exactly the machines the bound population fragments.
			Name:                       "big",
			PodResources:               map[string]string{"cpu": "4", "memory": "2Gi"},
			InstanceTypes:              []string{"shared-fill.large"},
			Priority:                   1000,
			InterruptionPenaltyDollars: 1024,
			ReclamationPenaltyDollars:  8192,
		},
	}
	workloads := []sim.WorkloadSpec{
		{Shape: "filler", Replicas: 180},
		{Shape: "big", Replicas: 5},
	}
	return sim.ClosedLoopScenario{
		Name:   name,
		Shapes: shapes,
		Clusters: []sim.ClusterSpec{
			{ID: "c1", Workloads: workloads},
			{ID: "c2", Workloads: workloads},
		},
		Seeds: []sim.SeedPool{
			{Shape: "filler", Density: 10, ConfiguredPerCluster: 20},
			// Idle big-fitting bait. A consumption-modeling engine would
			// acquire these for the pending big Pods; the ADR-0045
			// engine sees bound ≥ demand and leaves them alone.
			{Shape: "big", Density: 1, Idle: 12},
		},
		ControllerManaged: true,
		CRPerPod:          true,
		Scales:            scales,
		Cycles:            cycles,
	}
}

// TestClosedLoop_BoundCountsContract pins ADR-0045's accounting rule
// at steady demand: capacity counts for a cluster iff it is bound;
// binding is the atomic act of fulfillment. Bound capacity covers the
// roll-up's total demand here, so every quantity below is asserted as
// CORRECT behavior:
//
//   - shortfalls = 0: bound ≥ demand, the engine's job is done. That
//     the cluster's scheduler holds 5 big Pods per cluster on
//     fragmented residuals is satisfied-but-stuck — the cluster's
//     problem (preemption, descheduler, revised demands), carrying no
//     BigFleet signal by design.
//   - acquisitions = 0: no deficit, so the idle big-fitting machines
//     stay untouched.
//   - ZERO reclaims across the whole run (the headline): Phase 3 is
//     shrinkage-only; at steady demand it emits nothing, whatever
//     shape the cluster's packing takes. The pre-ADR-0045 keep-set
//     re-derivation reclaimed bound-pod-hosting machines out of this
//     exact state.
//   - the pending Pods are exactly the cluster's residue, untouched:
//     no evictions, bound count constant.
func TestClosedLoop_BoundCountsContract(t *testing.T) {
	const cycles = 60
	const nClusters, fillerPerCluster, bigPerCluster = 2, 180, 5
	res := runClosedLoop(t, boundCountsScenario("bound-counts-contract", cycles, nil))
	logConverged(t, res, cycles)

	failed := false

	// Headline: zero reclaims, ever, at steady demand.
	if got := res.SumLast(cycles, func(c sim.CycleStats) int { return c.Reclaims }); got != 0 {
		t.Errorf("reclaims over the run = %d, want 0 (Phase 3 is shrinkage-only; demand never shrank)", got)
		failed = true
	}
	// Correct: bound covers demand, so no shortfall and no acquisition.
	for _, c := range res.Cycles {
		if c.Shortfalls != 0 {
			t.Errorf("cycle %d: shortfalls = %d, want 0 (bound capacity covers demand)", c.Cycle, c.Shortfalls)
			failed = true
			break
		}
	}
	if got := res.SumLast(cycles, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions }); got != 0 {
		t.Errorf("acquisitions over the run = %d, want 0 (no deficit; the idle pool is not BigFleet's to spend)", got)
		failed = true
	}
	// The pending big Pods are the cluster's residue, untouched by
	// BigFleet: no evictions, the bound population constant, pending
	// exactly the unplaceable big Pods.
	if got := res.SumLast(cycles, func(c sim.CycleStats) int { return c.Evicted }); got != 0 {
		t.Errorf("evictions over the run = %d, want 0", got)
		failed = true
	}
	for _, c := range res.Cycles {
		if c.PendingPods != nClusters*bigPerCluster || c.BoundPods != nClusters*fillerPerCluster {
			t.Errorf("cycle %d: bound=%d pending=%d, want bound=%d pending=%d (the residue stays put)",
				c.Cycle, c.BoundPods, c.PendingPods, nClusters*fillerPerCluster, nClusters*bigPerCluster)
			failed = true
			break
		}
	}
	if got := res.SumLast(cycles, churn); got != 0 {
		t.Errorf("churn over the run = %d, want 0 (steady demand, fully bound: the engine is inert)", got)
		failed = true
	}

	if failed {
		dumpTrace(t, res)
	}
}

// TestClosedLoop_BoundCountsContract_ShrinkageReclaimsExcess is the
// other half of ADR-0045's Phase 3 contract: demand shrinkage IS the
// reclaim trigger. Same scenario, but at mid-run the filler deployment
// scales 180 → 90 per cluster. Demand drops to (110cpu, 100Gi) against
// 20 bound machines; the shared attribution walk claims 11 (2 for big,
// 9 for filler) and Phase 3 reclaims exactly the 9-machine excess per
// cluster — once, in the paper-§8 release order, with the M69 drain
// grace — then goes quiet again. Nothing is reclaimed while demand is
// steady on either side of the step.
func TestClosedLoop_BoundCountsContract_ShrinkageReclaimsExcess(t *testing.T) {
	const cycles, scaleCycle = 60, 31
	const nClusters, bigPerCluster = 2, 5
	const machinesPerCluster, keptPerCluster = 20, 11
	const excess = nClusters * (machinesPerCluster - keptPerCluster)
	res := runClosedLoop(t, boundCountsScenario("bound-counts-shrinkage", cycles,
		[]sim.TargetScale{{Cycle: scaleCycle, Shape: "filler", Replicas: 90}}))
	logConverged(t, res, cycles)

	failed := false

	// Steady before the step: no reclaims, no churn.
	preReclaims, preChurn := 0, 0
	for _, c := range res.Cycles[:scaleCycle-1] {
		preReclaims += c.Reclaims
		preChurn += c.Churn()
	}
	if preReclaims != 0 || preChurn != 0 {
		t.Errorf("pre-shrinkage reclaims=%d churn=%d, want 0/0 (steady demand)", preReclaims, preChurn)
		failed = true
	}
	// The step reclaims exactly the excess — bound minus what the
	// shrunken demand claims — and nothing more, ever.
	if got := res.SumLast(cycles, func(c sim.CycleStats) int { return c.Reclaims }); got != excess {
		t.Errorf("reclaims over the run = %d, want exactly the excess %d", got, excess)
		failed = true
	}
	if got := res.Cycles[scaleCycle-1].Reclaims; got != excess {
		t.Errorf("cycle %d reclaims = %d, want %d (the whole excess in the shrinkage cycle)", scaleCycle, got, excess)
		failed = true
	}
	// Shrinkage never manufactures a deficit: no shortfall, no
	// acquisition at any point.
	for _, c := range res.Cycles {
		if c.Shortfalls != 0 {
			t.Errorf("cycle %d: shortfalls = %d, want 0", c.Cycle, c.Shortfalls)
			failed = true
			break
		}
	}
	if got := res.SumLast(cycles, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions }); got != 0 {
		t.Errorf("acquisitions over the run = %d, want 0", got)
		failed = true
	}
	// Converged after the wave: the surviving filler rebinds onto the
	// kept machines (controller-managed eviction conserves demand), the
	// big residue stays pending, and the loop is quiet.
	end := res.Last(1)[0]
	if end.Configured != nClusters*keptPerCluster {
		t.Errorf("configured at end = %d, want %d", end.Configured, nClusters*keptPerCluster)
		failed = true
	}
	if end.BoundPods != nClusters*90 || end.PendingPods != nClusters*bigPerCluster {
		t.Errorf("end bound=%d pending=%d, want bound=%d pending=%d",
			end.BoundPods, end.PendingPods, nClusters*90, nClusters*bigPerCluster)
		failed = true
	}
	if got := res.SumLast(cycles-scaleCycle-5, churn); got != 0 {
		t.Errorf("churn after the shrinkage wave = %d, want 0", got)
		failed = true
	}

	if failed {
		dumpTrace(t, res)
	}
}
