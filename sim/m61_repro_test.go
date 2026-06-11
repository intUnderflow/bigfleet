package sim_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// TestClosedLoop_OversizedGangsScatteredSupply_ParkWithoutChurn pins
// the concentrate-then-park contract at the scattered-supply shape:
// multi-machine gangs larger than any reachable rack accumulation,
// with AMPLE acquirable supply scattered across many small rack
// blocks. Each gang must concentrate one rack's worth, hold the rest
// pending, park as a stable shortfall, and stop — zero trailing churn.
//
// M61 record: this was written to REPRODUCE the #55 residual (~23/sec
// Bootstrap↔Reclaim churn from unsatisfiable GPU gangs at uber-5k,
// with only 3 fingerprints parked) and the engine PASSED — twice, at
// two shapes (rack-fit-reachable: gangs correctly assembled
// cross-tier 8-machine racks and bound fully; rack-fit-unreachable:
// parked cleanly). The cloud divergence therefore lives in richer
// interactions this scenario omits — multi-cluster competition for
// the same GPU racks, plain single-GPU demand fragmenting rack blocks
// (gpu-inference folds and shares the instance type), and lifecycle
// churn regenerating gangs. M61.1 continues with that shape.
func TestClosedLoop_OversizedGangsScatteredSupply_ParkWithoutChurn(t *testing.T) {
	const cycles, k = 120, 40
	shapes := []sim.WorkloadShape{{
		Name:                       "gpu",
		PodResources:               map[string]string{"nvidia.com/gpu": "8"},
		InstanceTypes:              []string{"a3-highgpu-8g"},
		Priority:                   1000,
		InterruptionPenaltyDollars: 16384,
		ReclamationPenaltyDollars:  32768,
		SameRack:                   true,
	}}
	clusters := []sim.ClusterSpec{
		{ID: "c1", Workloads: []sim.WorkloadSpec{{Shape: "gpu", Objects: 3, Replicas: 12}}},
		{ID: "c2", Workloads: []sim.WorkloadSpec{{Shape: "gpu", Objects: 3, Replicas: 12}}},
	}
	// Rack blocks hold 4 GPU machines and the Idle/Speculative halves
	// can stack two blocks on one rack (8 max); every gang needs 12 —
	// no reachable rack accumulation fits a whole gang. Total supply is
	// ample (80 machines vs 72 Pods of demand) but scattered.
	seeds := []sim.SeedPool{{
		Shape: "gpu", Density: 1,
		Idle: 40, Speculative: 40,
		RacksPerZone: 12, ContiguousRackBlock: 4,
	}}
	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:              "oversized-gangs-scattered-supply",
		Shapes:            shapes,
		Clusters:          clusters,
		Seeds:             seeds,
		ControllerManaged: true,
		CRPerPod:          true,
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	failed := false
	if got := res.SumLast(k, churn); got != 0 {
		t.Errorf("churn over last %d cycles = %d, want 0 (park, don't churn)", k, got)
		failed = true
	}
	last := res.Last(k)
	if last[0].Shortfalls == 0 {
		t.Errorf("shortfalls = 0, want > 0 (oversized gangs must park)")
		failed = true
	}
	for _, c := range last {
		if c.Shortfalls != last[0].Shortfalls {
			t.Errorf("cycle %d: shortfalls = %d, want constant %d", c.Cycle, c.Shortfalls, last[0].Shortfalls)
			failed = true
			break
		}
	}
	if failed {
		dumpTrace(t, res)
	}
}
