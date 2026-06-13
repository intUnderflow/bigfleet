package sim_test

import (
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/sim"
)

// M73 closed-loop scenarios for the paper §8 Idle → Speculative
// release (ADR-0049). Sim cycles ≈ time: the scenarios pass
// nanosecond holds through ClosedLoopScenario.ReleasePolicy (the
// decision-package parameter whose default is the paper constants —
// no production config surface), so "hold expired" lands on the cycle
// after a machine goes Idle. The pre-existing canaries run with the
// nil policy → paper holds (10m/1m) that millisecond-scale sim cycles
// never cross, keeping them byte-identical.

// shortHolds expire one cycle after idle entry: any two cycles are
// more than a nanosecond apart in wall-clock.
func shortHolds() *decision.ReleasePolicy {
	return &decision.ReleasePolicy{
		OnDemandHold: time.Nanosecond,
		SpotHold:     time.Nanosecond,
	}
}

// releaseShape is a plain instance-typed service shape; density 1
// keeps the machine arithmetic 1 Pod = 1 machine.
func releaseShape() []sim.WorkloadShape {
	return []sim.WorkloadShape{{
		Name:                       "svc",
		PodResources:               map[string]string{"cpu": "2", "memory": "8Gi"},
		InstanceTypes:              []string{"m6i.xlarge"},
		Priority:                   1000,
		InterruptionPenaltyDollars: 1024,
		ReclamationPenaltyDollars:  8192,
	}}
}

// TestClosedLoop_IdleRelease_ShrinkageThenHoldExpiry_NoRebuyLoop is
// the full §8 arc in one run (the ADR-0049 pin): demand above the
// fixed seed first absorbs the bare-metal idle headroom (Bootstrap —
// §8 "prefer Idle") and then pulls in elastic capacity (Provision);
// the scale-down sheds the excess in keep-priority order (price-0
// bare metal kept, the priced tail reclaimed to Idle); and once the
// hold expires, exactly the elastic idled machines are Deleted —
// Idle → Speculative, spend released — while the reclaimed bare-metal
// remainder is held forever. The trailing window then pins the other
// half of the contract: nothing re-acquires what was released,
// because acquisition needs a deficit and the steady shrunken demand
// is covered.
func TestClosedLoop_IdleRelease_ShrinkageThenHoldExpiry_NoRebuyLoop(t *testing.T) {
	const cycles, k = 40, 15
	const baseMachines = 12 // bare-metal Configured seed = post-shrink demand
	const bmIdle = 3        // bare-metal idle headroom — absorbed first, never deletable
	const odBurst = 5       // the elastic remainder of the burst
	const burst = bmIdle + odBurst
	const scaleCycle = 10

	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:   "shrink-then-release",
		Shapes: releaseShape(),
		Clusters: []sim.ClusterSpec{{
			ID:        "cl-0",
			Workloads: []sim.WorkloadSpec{{Shape: "svc", Replicas: baseMachines + burst}},
		}},
		Seeds: []sim.SeedPool{{
			Shape: "svc", Density: 1,
			ConfiguredPerCluster: baseMachines,
			Idle:                 bmIdle,      // BareMetal (the default idle tier)
			Speculative:          odBurst + 4, // headroom proves Deletes are hold-driven, not quota-starved
		}},
		ControllerManaged: true,
		CRPerPod:          true,
		Scales:            []sim.TargetScale{{Cycle: scaleCycle, Shape: "svc", Replicas: baseMachines}},
		ReleasePolicy:     shortHolds(),
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	sum := func(f func(sim.CycleStats) int) int { return res.SumLast(cycles, f) }
	ok := true
	if got := sum(func(c sim.CycleStats) int { return c.Bootstraps }); got != bmIdle {
		t.Errorf("bootstraps = %d, want %d (§8: the burst absorbs idle before buying)", got, bmIdle)
		ok = false
	}
	if got := sum(func(c sim.CycleStats) int { return c.Provisions }); got != odBurst {
		t.Errorf("provisions = %d, want exactly the %d-machine elastic remainder", got, odBurst)
		ok = false
	}
	if got := sum(func(c sim.CycleStats) int { return c.Reclaims }); got != burst {
		t.Errorf("reclaims = %d, want %d (the keep-priority tail above the shrunken demand)", got, burst)
		ok = false
	}
	// Only the elastic tier releases; the reclaimed bare-metal machines
	// idle forever (§8: "bare metal: forever") — deletes < reclaims is
	// the held remainder.
	if got := sum(func(c sim.CycleStats) int { return c.Deletes }); got != odBurst {
		t.Errorf("deletes = %d, want %d (on-demand only; the %d bare-metal idles are held forever)",
			got, odBurst, bmIdle)
		ok = false
	}
	// Order: reclaim (→ Idle) strictly precedes release (→ Speculative);
	// nothing releases before the shrink.
	firstReclaim, firstDelete := 0, 0
	for _, c := range res.Cycles {
		if firstReclaim == 0 && c.Reclaims > 0 {
			firstReclaim = c.Cycle
		}
		if firstDelete == 0 && c.Deletes > 0 {
			firstDelete = c.Cycle
		}
	}
	if firstReclaim < scaleCycle || firstDelete <= firstReclaim {
		t.Errorf("first reclaim cycle %d / first delete cycle %d; want reclaim ≥ %d and delete after reclaim (the hold runs from Idle entry)",
			firstReclaim, firstDelete, scaleCycle)
		ok = false
	}
	if !assertQuiescent(t, res, k, 0) {
		ok = false
	}
	end := res.Cycles[len(res.Cycles)-1]
	if end.Configured != baseMachines || end.BoundPods != baseMachines {
		t.Errorf("end configured=%d bound=%d, want %d/%d (shrunken demand fully served by the kept tier)",
			end.Configured, end.BoundPods, baseMachines, baseMachines)
		ok = false
	}
	if !ok {
		dumpTrace(t, res)
	}
}

// TestClosedLoop_IdleRelease_SurplusReleasedOnce_LoopCannotClose pins
// the ADR-0049 no-cap argument in its purest shape: a steady-demand
// fleet with surplus elastic Idle releases it exactly once and never
// re-acquires. The Create↔Delete money loop cannot close by
// construction — Delete only touches machines no demand claimed for a
// full hold window, and Phase 1 only acquires on a deficit, which
// steady covered demand never produces. Zero Bootstraps and zero
// Provisions over the WHOLE run (not just the trailing window) is the
// loop-impossibility pin.
func TestClosedLoop_IdleRelease_SurplusReleasedOnce_LoopCannotClose(t *testing.T) {
	const cycles, k = 40, 20
	const demandMachines = 12
	const surplusIdle = 6

	res := runClosedLoop(t, sim.ClosedLoopScenario{
		Name:   "surplus-idle-no-loop",
		Shapes: releaseShape(),
		Clusters: []sim.ClusterSpec{{
			ID:        "cl-0",
			Workloads: []sim.WorkloadSpec{{Shape: "svc", Replicas: demandMachines}},
		}},
		Seeds: []sim.SeedPool{{
			Shape: "svc", Density: 1,
			ConfiguredPerCluster: demandMachines,
			Idle:                 surplusIdle,
			IdleCapacityType:     machine.CapacityTypeOnDemand,
			Speculative:          4,
		}},
		ControllerManaged: true,
		CRPerPod:          true,
		ReleasePolicy:     shortHolds(),
		Cycles:            cycles,
	})
	logConverged(t, res, k)

	sum := func(f func(sim.CycleStats) int) int { return res.SumLast(cycles, f) }
	ok := true
	if got := sum(func(c sim.CycleStats) int { return c.Deletes }); got != surplusIdle {
		t.Errorf("deletes = %d, want exactly the %d surplus idle machines, once", got, surplusIdle)
		ok = false
	}
	if got := sum(func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions }); got != 0 {
		t.Errorf("acquisitions = %d over the whole run, want 0 — releasing unclaimed idle must never create a deficit to re-buy against", got)
		ok = false
	}
	if got := sum(func(c sim.CycleStats) int { return c.Reclaims + c.Preempts }); got != 0 {
		t.Errorf("reclaims+preempts = %d, want 0 (steady demand never sheds bound capacity)", got)
		ok = false
	}
	if !assertQuiescent(t, res, k, 0) {
		ok = false
	}
	end := res.Cycles[len(res.Cycles)-1]
	if end.Configured != demandMachines || end.BoundPods != demandMachines {
		t.Errorf("end configured=%d bound=%d, want %d/%d (release touched only unbound machines — ADR-0045)",
			end.Configured, end.BoundPods, demandMachines, demandMachines)
		ok = false
	}
	if !ok {
		dumpTrace(t, res)
	}
}
