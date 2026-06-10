package sim_test

import (
	"context"
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// Closed-loop regression scenarios. Each pins one historical
// feedback-loop bug from the bigfleet-uber #45→#52 cascade arc
// (ADRs 0038 / 0039 / 0040 + Addendum) at `go test` speed: demand
// reacts to BigFleet's actions through the cluster model, so
// Reclaim → evict → recreate → rebind → rollup-change dynamics that
// previously needed a 90-minute cloud run to surface fail here in
// seconds.
//
// Shared scale: 2 clusters, ~500 Pods total, ~90 machines — small
// enough that the suite runs in a few seconds, large enough that
// per-shape machine counts, gang racks, and surpluses are non-trivial.
// The canaries (ADR-0038/0039 classes) run without the gang shape so
// every action in their traces is attributable to the pathology under
// test; the gang tests (ADR-0040 class) carry the tiny/service
// background so co-location dynamics play out in a mixed fleet.

const (
	tinyPodsPerCluster = 200 // density 10 → 20 machines at exact fit
	svcPodsPerCluster  = 50  // density 10 → 5 machines at exact fit
	gangsPerCluster    = 3
	gangSize           = 4 // density 1 → 4 machines per gang, one rack
	podDensity         = 10
)

// closedLoopShapes is the compact scenario-local catalog: a
// high-volume no-affinity tiny shape, an instance-typed service shape,
// and a sameRack gang shape (GPU-only resources, so the unconstrained
// tiny demand can never be credited against gang machines — the
// MinUnit floor keeps the pools honest). withGang=false trims the
// catalog for the canaries.
func closedLoopShapes(withGang bool) []sim.WorkloadShape {
	shapes := []sim.WorkloadShape{
		{
			Name:                       "tiny",
			PodResources:               map[string]string{"cpu": "200m", "memory": "256Mi"},
			Priority:                   100,
			InterruptionPenaltyDollars: 32,
			ReclamationPenaltyDollars:  64,
		},
		{
			Name:                       "service",
			PodResources:               map[string]string{"cpu": "2", "memory": "8Gi"},
			InstanceTypes:              []string{"m6i.xlarge"},
			Zones:                      []string{"zone-a", "zone-b", "zone-c"},
			Priority:                   1000,
			InterruptionPenaltyDollars: 1024,
			ReclamationPenaltyDollars:  8192,
		},
	}
	if withGang {
		shapes = append(shapes, sim.WorkloadShape{
			Name:                       "gang",
			PodResources:               map[string]string{"nvidia.com/gpu": "8"},
			InstanceTypes:              []string{"a3-highgpu-8g"},
			Zones:                      []string{"zone-a", "zone-b", "zone-c"},
			Priority:                   5000,
			InterruptionPenaltyDollars: 8192,
			ReclamationPenaltyDollars:  65536,
			SameRack:                   true,
		})
	}
	return shapes
}

func closedLoopClusters(workloads []sim.WorkloadSpec) []sim.ClusterSpec {
	return []sim.ClusterSpec{
		{ID: "cl-0", Workloads: workloads},
		{ID: "cl-1", Workloads: workloads},
	}
}

func baseWorkloads(withGang bool) []sim.WorkloadSpec {
	w := []sim.WorkloadSpec{
		{Shape: "tiny", Replicas: tinyPodsPerCluster},
		{Shape: "service", Replicas: svcPodsPerCluster},
	}
	if withGang {
		w = append(w, sim.WorkloadSpec{Shape: "gang", Objects: gangsPerCluster, Replicas: gangSize})
	}
	return w
}

// closedLoopSeeds builds the machine pools. tinyCfg / svcCfg size the
// per-cluster Configured seed so tests can dial surplus (Phase 3
// fodder) or deficit (Phase 1 fodder) against the exact-demand
// baselines of 20 and 5. gangCfg ≤ 0 omits the gang pool (canaries);
// gangContiguous selects ADR-0040 Addendum §4 rack blocks vs the
// hostile round-robin scatter.
func closedLoopSeeds(tinyCfg, svcCfg, gangCfg int, gangContiguous bool) []sim.SeedPool {
	seeds := []sim.SeedPool{
		{
			Shape: "tiny", MachineInstanceType: "m6i.large", Density: podDensity,
			ConfiguredPerCluster: tinyCfg, Idle: 3, Speculative: 2,
		},
		{
			Shape: "service", Density: podDensity,
			ConfiguredPerCluster: svcCfg, Idle: 2, Speculative: 2,
		},
	}
	if gangCfg > 0 {
		gangBlock := 0
		if gangContiguous {
			gangBlock = gangSize
		}
		seeds = append(seeds, sim.SeedPool{
			Shape: "gang", Density: 1, ContiguousRackBlock: gangBlock,
			ConfiguredPerCluster: gangCfg, Idle: 4, Speculative: 4,
		})
	}
	return seeds
}

func runClosedLoop(t *testing.T, sc sim.ClosedLoopScenario) *sim.ClosedLoopResult {
	t.Helper()
	res, err := sim.RunClosedLoop(context.Background(), sc)
	if err != nil {
		t.Fatalf("RunClosedLoop(%s): %v", sc.Name, err)
	}
	return res
}

func churn(c sim.CycleStats) int { return c.Churn() }

// logConverged records the trailing-window numbers each test reports.
func logConverged(t *testing.T, res *sim.ClosedLoopResult, k int) {
	t.Helper()
	last := res.Last(k)
	end := last[len(last)-1]
	t.Logf("last %d cycles: bootstraps=%d reclaims=%d provisions=%d preempts=%d; end: configured=%d bound=%d pending=%d shortfalls=%d (target pods=%d)",
		k,
		res.SumLast(k, func(c sim.CycleStats) int { return c.Bootstraps }),
		res.SumLast(k, func(c sim.CycleStats) int { return c.Reclaims }),
		res.SumLast(k, func(c sim.CycleStats) int { return c.Provisions }),
		res.SumLast(k, func(c sim.CycleStats) int { return c.Preempts }),
		end.Configured, end.BoundPods, end.PendingPods, end.Shortfalls, res.TargetPods)
}

// dumpTrace prints the per-cycle counters on failure so a regression
// arrives with the evidence attached.
func dumpTrace(t *testing.T, res *sim.ClosedLoopResult) {
	t.Helper()
	for _, c := range res.Cycles {
		if c.Churn() == 0 && c.Evicted == 0 {
			continue // quiet cycles don't earn a line
		}
		t.Logf("cycle %3d: boot=%d prov=%d recl=%d pre=%d evicted=%d configured=%d bound=%d pending=%d shortfalls=%d probe=%d",
			c.Cycle, c.Bootstraps, c.Provisions, c.Reclaims, c.Preempts,
			c.Evicted, c.Configured, c.BoundPods, c.PendingPods, c.Shortfalls, c.ReclaimMatchesShortfall)
	}
}

// assertQuiescent asserts the trailing window has zero churn and a
// shortfall count that is constant at wantShortfalls — the closed
// loop's definition of converged.
func assertQuiescent(t *testing.T, res *sim.ClosedLoopResult, k, wantShortfalls int) bool {
	t.Helper()
	ok := true
	if got := res.SumLast(k, churn); got != 0 {
		t.Errorf("churn over last %d cycles = %d, want 0", k, got)
		ok = false
	}
	for _, c := range res.Last(k) {
		if c.Shortfalls != wantShortfalls {
			t.Errorf("cycle %d: shortfalls = %d, want constant %d", c.Cycle, c.Shortfalls, wantShortfalls)
			ok = false
			break
		}
	}
	return ok
}

// TestClosedLoop_SteadyStateConverges: controller-managed, CR-per-pod,
// rack-blocked gang seed, Configured seed ≈ demand (tiny +2 surplus,
// service −1 deficit per cluster) plus idle headroom and speculative
// quota. The loop must absorb the seed imperfection — reclaim the
// surplus (evicting and recreating its Pods), bootstrap the deficit —
// and then go quiet: this is ADR-0035's steady-state definition,
// reachable only because ADR-0038/0039 made eviction
// demand-conserving.
func TestClosedLoop_SteadyStateConverges(t *testing.T) {
	const cycles, k = 300, 100
	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:              "steady-state",
		Shapes:            closedLoopShapes(true),
		Clusters:          closedLoopClusters(baseWorkloads(true)),
		Seeds:             closedLoopSeeds(22, 4, gangsPerCluster*gangSize, true),
		ControllerManaged: true,
		CRPerPod:          true,
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	ok := assertQuiescent(t, res, k, 0)
	// Demand in machines: per cluster 20 tiny + 5 service + 12 gang.
	const wantConfigured = 2 * (20 + 5 + gangsPerCluster*gangSize)
	end := res.Cycles[len(res.Cycles)-1]
	if end.Configured != wantConfigured {
		t.Errorf("configured = %d, want %d", end.Configured, wantConfigured)
		ok = false
	}
	if frac := float64(end.BoundPods) / float64(res.TargetPods); frac < 0.99 {
		t.Errorf("bind fraction = %.4f (%d/%d), want >= 0.99", frac, end.BoundPods, res.TargetPods)
		ok = false
	}
	if !ok {
		dumpTrace(t, res)
	}
}

// TestClosedLoop_BarePodsDestroyDemand_Canary: the ADR-0038 pathology.
// With bare Pods (ControllerManaged=false) and a small genuine surplus
// (+3 tiny machines per cluster), Phase 3's correct surplus reclaim
// evicts Pods that no controller recreates; the dead Pods' CRs die
// with them, demand permanently shrinks, and the next cycle sees yet
// more surplus — the #45 self-sustaining cascade. The canary proves
// the closed loop *detects* the class; the controller-managed twin on
// the same seed proves the ADR-0038 fix converges with demand
// conserved.
func TestClosedLoop_BarePodsDestroyDemand_Canary(t *testing.T) {
	const cycles, k = 80, 40
	const surplusMachines = 2 * 3
	mk := func(name string, managed bool) sim.ClosedLoopScenario {
		return sim.ClosedLoopScenario{
			Name:              name,
			Shapes:            closedLoopShapes(false),
			Clusters:          closedLoopClusters(baseWorkloads(false)),
			Seeds:             closedLoopSeeds(23, 5, 0, false),
			ControllerManaged: managed,
			CRPerPod:          true,
			Cycles:            cycles,
		}
	}

	bare := runClosedLoop(t, mk("bare-pods", false))
	logConverged(t, bare, k)
	bareOK := true
	// Reconciling the genuine surplus destroys at most one surplus
	// machine's worth of Pods per reclaim; anything well beyond that is
	// demand the cascade ate. Require 2× to make the signal unambiguous.
	destroyed := bare.TargetPods - bare.Cycles[len(bare.Cycles)-1].LivePods
	if destroyed < 2*surplusMachines*podDensity {
		t.Errorf("bare pods: destroyed %d pods, want >= %d (cascade past the genuine surplus)",
			destroyed, 2*surplusMachines*podDensity)
		bareOK = false
	}
	if got := bare.SumLast(cycles, func(c sim.CycleStats) int { return c.Reclaims }); got <= 2*surplusMachines {
		t.Errorf("bare pods: total reclaims = %d, want > %d", got, 2*surplusMachines)
		bareOK = false
	}
	// Self-sustaining, not a one-shot correction: reclaim keeps firing
	// across many cycles as each eviction manufactures next cycle's
	// surplus.
	reclaimCycles := 0
	for _, c := range bare.Cycles {
		if c.Reclaims > 0 {
			reclaimCycles++
		}
	}
	if reclaimCycles < 5 {
		t.Errorf("bare pods: reclaims fired in %d cycles, want >= 5 (sustained cascade)", reclaimCycles)
		bareOK = false
	}
	if !bareOK {
		dumpTrace(t, bare)
	}

	managed := runClosedLoop(t, mk("bare-pods-fixed", true))
	logConverged(t, managed, k)
	ok := assertQuiescent(t, managed, k, 0)
	if endLive := managed.Cycles[len(managed.Cycles)-1].LivePods; endLive != managed.TargetPods {
		t.Errorf("controller-managed: live pods = %d, want %d (demand conserved)",
			endLive, managed.TargetPods)
		ok = false
	}
	if !ok {
		dumpTrace(t, managed)
	}
}

// TestClosedLoop_UnmetOnlyCRs_PhantomSurplus_Canary: the ADR-0039
// pathology. With CRs only for *pending* Pods (the pre-fix
// unschedulable-only controller, taken to its closed-loop limit), a
// fully-bound cluster rolls up empty demand: Phase 3 sees its entire
// Configured supply as phantom surplus and reclaims it; the evictions
// recreate the Pods pending; the next rollup is full demand again —
// a fleet-wide Reclaim↔Bootstrap oscillation on top of demand that
// never changed. CR-per-pod (papers §6.1/§13) converges on the same
// seed.
func TestClosedLoop_UnmetOnlyCRs_PhantomSurplus_Canary(t *testing.T) {
	const cycles, k = 120, 60
	mk := func(name string, crPerPod bool) sim.ClosedLoopScenario {
		return sim.ClosedLoopScenario{
			Name:              name,
			Shapes:            closedLoopShapes(false),
			Clusters:          closedLoopClusters(baseWorkloads(false)),
			Seeds:             closedLoopSeeds(20, 5, 0, false),
			ControllerManaged: true,
			CRPerPod:          crPerPod,
			Cycles:            cycles,
		}
	}

	unmet := runClosedLoop(t, mk("unmet-only-crs", false))
	logConverged(t, unmet, k)
	reclaims := unmet.SumLast(k, func(c sim.CycleStats) int { return c.Reclaims })
	boots := unmet.SumLast(k, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions })
	if reclaims < 100 || boots < 100 {
		t.Errorf("unmet-only CRs: last %d cycles reclaims=%d bootstraps=%d, want sustained churn (>= 100 each)",
			k, reclaims, boots)
		dumpTrace(t, unmet)
	}

	perPod := runClosedLoop(t, mk("cr-per-pod", true))
	logConverged(t, perPod, k)
	if !assertQuiescent(t, perPod, k, 0) {
		dumpTrace(t, perPod)
	}
}

// TestClosedLoop_GangScatterNoOscillation: the ADR-0040 + Addendum
// regression at sim speed. Gang machines are seeded round-robin across
// racks — the hostile #50/#51 topology where per-rack supply (1-2) is
// below the gang size (4), so every gang Need is physically
// unsatisfiable in full. Pre-fix this produced the self-sustaining
// Bootstrap≈Reclaim equilibrium (vacuous crediting, then the domain
// chosen twice per cycle). Post-Addendum the joint once-per-cycle
// domain choice must concentrate each gang and park the residual in
// the aged shortfall buffer (paper §16/§9): zero churn over the
// trailing window, a stable non-zero shortfall, and a zero
// reclaim-matches-unsatisfied probe (ADR-0040 §4).
func TestClosedLoop_GangScatterNoOscillation(t *testing.T) {
	const cycles, k = 300, 100
	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:              "gang-scatter",
		Shapes:            closedLoopShapes(true),
		Clusters:          closedLoopClusters(baseWorkloads(true)),
		Seeds:             closedLoopSeeds(20, 5, gangsPerCluster*gangSize, false), // round-robin gang racks
		ControllerManaged: true,
		CRPerPod:          true,
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	ok := true
	if got := res.SumLast(k, churn); got != 0 {
		t.Errorf("churn over last %d cycles = %d, want 0 (no Bootstrap↔Reclaim oscillation)", k, got)
		ok = false
	}
	last := res.Last(k)
	if last[0].Shortfalls == 0 {
		t.Errorf("shortfalls = 0, want > 0 (the unsatisfiable gang residual must surface as a shortfall)")
		ok = false
	}
	for _, c := range last {
		if c.Shortfalls != last[0].Shortfalls {
			t.Errorf("cycle %d: shortfalls = %d, want constant %d", c.Cycle, c.Shortfalls, last[0].Shortfalls)
			ok = false
			break
		}
	}
	if probe := res.SumLast(k, func(c sim.CycleStats) int { return c.ReclaimMatchesShortfall }); probe != 0 {
		t.Errorf("reclaim-matches-unsatisfied probe = %d over last %d cycles, want 0 (ADR-0040 §4)", probe, k)
		ok = false
	}
	if !ok {
		dumpTrace(t, res)
	}
}

// TestClosedLoop_UnsatisfiableGangIsStableShortfall: one gang (8) is
// larger than any rack's capacity (contiguous blocks of 4), alongside
// two satisfiable gangs and the usual tiny/service demand. Paper §16:
// a Same request that can't be satisfied within the shard becomes a
// shortfall — not a churn source, and not a denial of service for the
// demand around it.
func TestClosedLoop_UnsatisfiableGangIsStableShortfall(t *testing.T) {
	const cycles, k = 300, 100
	const megaGangSize = 2 * gangSize
	workloads := append(baseWorkloads(false),
		sim.WorkloadSpec{Shape: "gang", Objects: 2, Replicas: gangSize},
		sim.WorkloadSpec{Shape: "gang", Objects: 1, Replicas: megaGangSize},
	)
	// Enough gang machines for every gang Pod, but no rack block can
	// host more than gangSize of them.
	seeds := closedLoopSeeds(20, 5, 2*gangSize+megaGangSize, true)
	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:              "unsatisfiable-gang",
		Shapes:            closedLoopShapes(true),
		Clusters:          closedLoopClusters(workloads),
		Seeds:             seeds,
		ControllerManaged: true,
		CRPerPod:          true,
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	ok := true
	if got := res.SumLast(k, churn); got != 0 {
		t.Errorf("churn over last %d cycles = %d, want 0", k, got)
		ok = false
	}
	last := res.Last(k)
	if last[0].Shortfalls == 0 {
		t.Errorf("shortfalls = 0, want > 0 (the oversized gang's residual)")
		ok = false
	}
	for _, c := range last {
		if c.Shortfalls != last[0].Shortfalls {
			t.Errorf("cycle %d: shortfalls = %d, want constant %d", c.Cycle, c.Shortfalls, last[0].Shortfalls)
			ok = false
			break
		}
	}
	// The oversized gangs bind one rack's worth and hold the rest
	// pending — never scattered. Everything else binds fully.
	for _, w := range res.Workloads {
		switch {
		case w.Target == megaGangSize:
			if w.Live != w.Target || w.Bound != gangSize {
				t.Errorf("%s/%s: live=%d bound=%d, want live=%d bound=%d (one rack's worth)",
					w.Cluster, w.Workload, w.Live, w.Bound, w.Target, gangSize)
				ok = false
			}
		default:
			if w.Bound != w.Target {
				t.Errorf("%s/%s: bound=%d, want %d (satisfiable demand fully bound)",
					w.Cluster, w.Workload, w.Bound, w.Target)
				ok = false
			}
		}
	}
	if !ok {
		dumpTrace(t, res)
	}
}

// TestClosedLoop_SubMachineGangsLedgerMatchesReality is the local
// reproduction of the open #45→#53-arc residual: post-ADR-0039 every
// co-location group is its own Need, and at density 10 a 4-pod gang is
// a FRACTION of one machine — yet the claim ledger (SeedClaim /
// claimMatching) claims machines exclusively, so each sub-machine gang
// up-rounds to a whole machine. The cloud signature (uber-5k): bind
// 97.5 % — pods pack fine — while Phase 1 reports 78 % of gangs
// unsatisfiable and Configured inflates toward one-machine-per-gang.
//
// Shape: 12 gangs of 4 per cluster (48 Pods = 4.8 machines of demand
// at density 10) against 6 gang-Configured per cluster — ample
// CAPACITY for all 12 gangs, but exclusive claiming can satisfy at
// most 6+acquired. Correct behaviour: every Pod binds AND the ledger
// agrees (shortfalls → 0, no standing acquisition pressure).
func TestClosedLoop_SubMachineGangsLedgerMatchesReality(t *testing.T) {
	// KNOWN-OPEN finding (the bigfleet-uber #53 residual, reproduced
	// here on 2026-06-11 in 0.4 s): on current code this fails with the
	// cloud's exact signature — bind 97.65 % (cloud measured 97.5 %),
	// 14 Pods pending forever despite ample capacity, a standing
	// shortfall, Configured over-acquired toward one-machine-per-gang,
	// and a cycle-1 reclaim wave with the ADR-0040 §4 probe firing
	// (probe=6). The fix is the open claim-granularity design decision
	// (ADR-0027-level: exclusive per-machine claims vs sub-machine
	// Needs); un-skip when it lands — this test is its acceptance
	// criterion. Run it directly with:
	//
	//	go test ./sim/ -run SubMachineGangs -count=1
	t.Skip("known-open: machine-exclusive claiming up-rounds sub-machine gangs (cascade-arc residual; pending claim-granularity ADR)")

	const cycles, k = 300, 100
	const subGangsPerCluster, subGangSize = 12, 4

	shapes := closedLoopShapes(false)
	shapes = append(shapes, sim.WorkloadShape{
		Name:                       "smallgang",
		PodResources:               map[string]string{"cpu": "2", "memory": "16Gi"},
		InstanceTypes:              []string{"r6i.2xlarge"},
		Zones:                      []string{"zone-a", "zone-b", "zone-c"},
		Priority:                   1000,
		InterruptionPenaltyDollars: 4096,
		ReclamationPenaltyDollars:  32768,
		SameRack:                   true,
	})
	workloads := []sim.WorkloadSpec{
		{Shape: "tiny", Replicas: tinyPodsPerCluster},
		{Shape: "service", Replicas: svcPodsPerCluster},
		{Shape: "smallgang", Objects: subGangsPerCluster, Replicas: subGangSize},
	}
	seeds := closedLoopSeeds(20, 5, 0, false)
	seeds = append(seeds, sim.SeedPool{
		Shape: "smallgang", Density: podDensity, ContiguousRackBlock: 2,
		ConfiguredPerCluster: 6, Idle: 2, Speculative: 2,
	})

	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:              "sub-machine-gangs",
		Shapes:            shapes,
		Clusters:          closedLoopClusters(workloads),
		Seeds:             seeds,
		ControllerManaged: true,
		CRPerPod:          true,
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	end := res.Last(1)[0]
	failed := false
	// Capacity hosts every gang: all Pods bound.
	if frac := float64(end.BoundPods) / float64(res.TargetPods); frac < 0.99 {
		t.Errorf("bind fraction = %.4f (%d/%d), want >= 0.99", frac, end.BoundPods, res.TargetPods)
		failed = true
	}
	// The ledger agrees with reality: no standing gang shortfall, no
	// churn, no acquisition pressure beyond the true capacity need.
	if !assertQuiescent(t, res, k, 0) {
		failed = true
	}
	if failed {
		dumpTrace(t, res)
	}
}
