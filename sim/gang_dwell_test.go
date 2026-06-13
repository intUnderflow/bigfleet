package sim_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// M77g / ADR-0051 — bootstrap-dwell fidelity + the gang fixed-point guard.
//
// ESCALATION (read first): the brief's Part-1 goal was a sim repro that
// reproduces the field's sustained Bootstrap≈Reclaim oscillation (#64) on
// current HEAD with bootstrap dwell ≥ 2, then turns green under the fix.
// The dwell-fidelity model below works (machines genuinely persist in
// Configuring for N cycles — see TestClosedLoop_GangDwell_ModelEngages),
// but it does NOT reproduce the Bootstrap≈Reclaim symptom — not in the
// full closed loop here and not in the engine-only loop (offline probing
// across gang sizes, supply tightness, rack overlap, contention, and
// dwell 0/2/3). In every configuration the deterministic chooser reaches
// a fixed point: either the correct domain (with the fix) or a "wrong but
// stable" sibling-domain assignment (pre-fix). What DOES reproduce at
// engine granularity is the underlying defect — the Same-domain CHOICE
// itself: pre-fix a gang settles on (or, at certain sizes, oscillates
// onto) a sibling gang's domain; the fix pins it to its own. That defect
// is captured deterministically and fail-pre/pass-post in
// pkg/decision/occ TestChooseSameBucket_GangOwnBreaksCoverageTie. The
// Bootstrap≈Reclaim actuation the field showed needs a perturbation the
// offline model still lacks (the deterministic chooser self-damps once
// acquisition equilibrates) — so per the brief, this is reported for
// escalation rather than asserted blind. See the M77g report.
//
// gpu-training whole-node packing: cpu:64 / gpu:8 = one Pod per node
// (ADR-0050 SeedScale shape — the realistic GPU machine, NOT M66.2's
// phantom cpu:800 that packed many gang members onto one node and hid
// the per-machine domain churn).
func gangDwellShapes() []sim.WorkloadShape {
	return []sim.WorkloadShape{{
		Name:                       "gpu-train",
		PodResources:               map[string]string{"cpu": "64", "memory": "256Gi", "nvidia.com/gpu": "8"},
		InstanceTypes:              []string{"a3-highgpu-8g"},
		Zones:                      []string{"zone-a", "zone-b", "zone-c"},
		Priority:                   1000,
		InterruptionPenaltyDollars: 16384,
		ReclamationPenaltyDollars:  32768,
		SameRack:                   true,
	}}
}

// gangDwellScenario: static, fully-covered Same(rack) gang demand on
// whole-node GPU machines, two clusters, a shared Idle/Speculative pool
// that gives each gang more than one fully-covering domain (the
// capped-coverage tie), and a configurable bootstrap dwell.
func gangDwellScenario(stampAttribution bool, dwell, cycles int) sim.ClosedLoopScenario {
	gangs := []int{3, 3, 3, 3}
	workloads := make([]sim.WorkloadSpec, 0, len(gangs))
	for _, n := range gangs {
		workloads = append(workloads, sim.WorkloadSpec{Shape: "gpu-train", Replicas: n})
	}
	return sim.ClosedLoopScenario{
		Name:   "gang-dwell",
		Shapes: gangDwellShapes(),
		Clusters: []sim.ClusterSpec{
			{ID: "c1", Workloads: workloads},
			{ID: "c2", Workloads: workloads},
		},
		Seeds: []sim.SeedPool{{
			Shape:                "gpu-train",
			Density:              1,
			ConfiguredPerCluster: 12, // = per-cluster gang demand
			Idle:                 18, // 6 spare size-3 racks
			Speculative:          36, // 12 more
			ContiguousRackBlock:  3,  // a rack holds exactly one gang
			RacksPerZone:         12,
		}},
		ControllerManaged:          true,
		CRPerPod:                   true,
		RollupArrivalStamps:        true,
		BootstrapDwellCycles:       dwell,
		StampSeededGangAttribution: stampAttribution,
		Cycles:                     cycles,
	}
}

// TestClosedLoop_GangDwellFixedPoint is the M77g gang fixed-point guard:
// with bootstrap dwell ≥ 2 AND gang attribution (the fix), static
// fully-covered gang demand on realistic whole-node packing must be
// quiescent — zero Bootstrap/Reclaim growth, constant shortfalls. This is
// the converged-state property ADR-0051 must preserve through the
// in-flight bootstrap dwell. (It does not exercise the pre-fix defect as a
// FAILING arm — see the escalation note above; the discriminating pin is
// in pkg/decision/occ.)
func TestClosedLoop_GangDwellFixedPoint(t *testing.T) {
	const cycles, k, dwell = 120, 60, 2
	res := runClosedLoop(t, gangDwellScenario(true, dwell, cycles))
	boots := res.SumLast(k, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions })
	recls := res.SumLast(k, func(c sim.CycleStats) int { return c.Reclaims })
	t.Logf("dwell=%d + attribution: trailing %d cycles acq=%d recl=%d", dwell, k, boots, recls)
	if boots+recls != 0 {
		dumpTrace(t, res)
		t.Errorf("not quiescent under dwell=%d + fix: trailing acq=%d recl=%d, want 0", dwell, boots, recls)
	}
}

// TestClosedLoop_GangDwell_ModelEngages proves the bootstrap-dwell
// fidelity model actually holds machines in Configuring across cycles
// (ADR-0051 / M77g): with a fresh-acquisition seed (Configured below
// demand) and dwell ≥ 2, the cycle that completes the engine's
// bootstraps lands at least `dwell`-1 cycles AFTER the cycle that emitted
// them — visible as Configured rising only after a delay while Pods stay
// pending. At dwell 0 (the default) completion is same-cycle, so every
// pre-existing scenario is byte-identical.
func TestClosedLoop_GangDwell_ModelEngages(t *testing.T) {
	scenario := func(dwell int) sim.ClosedLoopScenario {
		sc := gangDwellScenario(false, dwell, 12)
		// Seed nothing Configured: every gang machine is engine-acquired,
		// so its bootstrap runs through the dwell.
		sc.Seeds[0].ConfiguredPerCluster = 0
		sc.Seeds[0].Idle = 24
		sc.Seeds[0].Speculative = 24
		return sc
	}

	// dwell 0: machines acquired in cycle 1 are Configured by the time the
	// cycle's stats are recorded (instant completion).
	res0 := runClosedLoop(t, scenario(0))
	if c1 := res0.Cycles[0]; c1.Configured == 0 {
		t.Errorf("dwell=0: cycle 1 Configured=%d, want >0 (instant completion)", c1.Configured)
	}

	// dwell 2: the machines acquired in cycle 1 are still Configuring at
	// the end of cycle 1 (Configured stays 0 longer), then complete.
	res2 := runClosedLoop(t, scenario(2))
	if c1 := res2.Cycles[0]; c1.Configured != 0 {
		t.Errorf("dwell=2: cycle 1 Configured=%d, want 0 (machines dwelling in Configuring)", c1.Configured)
	}
	// They must eventually complete — the loop still converges.
	last := res2.Cycles[len(res2.Cycles)-1]
	if last.Configured == 0 {
		t.Errorf("dwell=2: machines never completed the dwell (final Configured=0)")
	}
	t.Logf("dwell engages: dwell0 cyc1 configured=%d; dwell2 cyc1 configured=%d final configured=%d",
		res0.Cycles[0].Configured, res2.Cycles[0].Configured, last.Configured)
}
