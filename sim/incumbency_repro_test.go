package sim_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// M77h — incumbency-stable machine selection, closed-loop converged-state
// guard.
//
// REPRO NOTE (read first): the brief's Part-1 goal was a closed-loop sim
// repro of the SUSTAINED within-domain Bootstrap≈Reclaim the field (#65)
// showed after ADR-0051 pinned the domain. It does NOT reproduce here —
// across clean and shared rack layouts, bootstrap dwell 0/2/3, and
// gang over-coverage (ConfiguredPerCluster > demand), the closed loop
// sheds the over-coverage excess ONCE (the early whole-run reclaim) and
// then converges; the deterministic chooser self-damps once acquisition
// equilibrates, the same reason M77f/M77g's offline probes stayed quiet.
// What DOES reproduce — deterministically, fail-pre / pass-post — is the
// engine-granularity defect the actuation rides on: stop-when-covered
// bumping an incumbent when a non-incumbent equivalent matures and
// re-sorts ahead. That pin is
// pkg/decision/occ TestSeedSameProfile_ClaimedSetStableAcrossMaturation /
// _IncumbentKeptOverMaturedEquivalent. This test is the converged-state
// no-regression guard the fix must keep green, not the fix discriminator.
func incumbencyConvergedScenario(dwell int, sharedRacks bool, cycles int) sim.ClosedLoopScenario {
	gangs := []int{3, 3, 3, 3, 3, 3}
	workloads := make([]sim.WorkloadSpec, 0, len(gangs))
	for _, n := range gangs {
		workloads = append(workloads, sim.WorkloadSpec{Shape: "gpu-train", Replicas: n})
	}
	block, racksPerZone := 3, 12 // one gang per rack (clean attribution)
	if sharedRacks {
		block, racksPerZone = 0, 2 // round-robin into few racks → gangs share domains
	}
	return sim.ClosedLoopScenario{
		Name:   "incumbency-converged",
		Shapes: gangDwellShapes(),
		Clusters: []sim.ClusterSpec{
			{ID: "c1", Workloads: workloads},
			{ID: "c2", Workloads: workloads},
		},
		Seeds: []sim.SeedPool{{
			Shape:                "gpu-train",
			Density:              1,
			ConfiguredPerCluster: 24, // > 18 demand → initial over-coverage
			Idle:                 24,
			Speculative:          48,
			ContiguousRackBlock:  block,
			RacksPerZone:         racksPerZone,
		}},
		ControllerManaged:          true,
		CRPerPod:                   true,
		RollupArrivalStamps:        true,
		BootstrapDwellCycles:       dwell,
		StampSeededGangAttribution: true,
		Cycles:                     cycles,
	}
}

// TestClosedLoop_IncumbencyConverged: at static gang demand on realistic
// whole-node GPU packing, under bootstrap dwell and gang over-coverage,
// the loop must converge and the trailing window be quiescent — the
// over-coverage excess is reclaimed once, then zero Bootstrap/Reclaim.
// M77h's incumbency-stable selection must not perturb this; shared-rack
// (mixed-attribution) domains stay quiescent because each gang keeps its
// own machines instead of swapping with a neighbour's as they mature.
func TestClosedLoop_IncumbencyConverged(t *testing.T) {
	const cycles, k = 120, 60
	for _, tc := range []struct {
		name        string
		dwell       int
		sharedRacks bool
	}{
		{"clean-racks-dwell2", 2, false},
		{"shared-racks-dwell2", 2, true},
		{"shared-racks-dwell3", 3, true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			res := runClosedLoop(t, incumbencyConvergedScenario(tc.dwell, tc.sharedRacks, cycles))
			boots := res.SumLast(k, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions })
			recls := res.SumLast(k, func(c sim.CycleStats) int { return c.Reclaims })
			end := res.Last(1)[0]
			t.Logf("%s: trailing %d cycles acq=%d recl=%d shortfalls=%d configured=%d boundPods=%d/%d",
				tc.name, k, boots, recls, end.Shortfalls, end.Configured, end.BoundPods, res.TargetPods)
			if boots+recls != 0 {
				dumpTrace(t, res)
				t.Errorf("%s not quiescent: trailing acq=%d recl=%d, want 0", tc.name, boots, recls)
			}
		})
	}
}
