package sim_test

// DIAGNOSTIC (sim-only, evidence for an author design decision — see the
// brief). Tests the hypothesis that a satisfied gang at STATIC demand
// sustains a Bootstrap≈Reclaim loop because of an OVER-ACQUIRE driven by a
// coverage-accounting gap: a machine the gang acquired last cycle that is
// now traversing the PRE-Configuring runway (Speculative→Creating→Idle) is
// counted by NEITHER seed.go's coverage walk (Configured+Configuring) NOR
// the acquirable pool (Idle+Speculative), and carries no AssignedGroup —
// so it is INVISIBLE, the gang re-derives the full deficit (ADR-0045, no
// memory of in-flight acquisitions), and acquires AGAIN.
//
// The existing closed-loop dwell (BootstrapDwellCycles) holds machines at
// Configuring, which IS a counted state — so it self-damps (the prior
// M77f/g/h finding). ProvisionDwellCycles (added for this experiment) holds
// machines at Creating — the uncounted, unattributed runway. The control
// arm (ProvisionDwellCycles=0, the OLD model) must NOT churn; the dwell>0
// arm is the candidate repro.

import (
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// runwayGangShapes: one whole-node GPU Same(rack) gang shape (cpu:64/gpu:8
// = one Pod per node — the realistic GPU machine, the same shape M77g used
// so per-machine domain dynamics aren't hidden by sub-machine packing).
func runwayGangShapes() []sim.WorkloadShape {
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

// runwayScenario: ONE Same(rack) gang of size D in ONE cluster, fully
// covered at start (ConfiguredPerCluster = D), STATIC demand. The gang's
// rack carries SPARE Speculative capacity (ContiguousRackBlock > D, so the
// first rack holds the D Configured PLUS spare Speculative slots in the
// SAME rack) — so a single incumbent loss is replaceable IN-DOMAIN by ONE
// Speculative machine (deficit stays 1; the gang does not migrate racks).
// Idle = 0 so the replacement is PROVISIONED from Speculative, exercising
// the full Speculative→Creating→Idle runway (the longest invisible window,
// the brief's worst case). A single incumbent loss is injected at
// faultCycle. provisionDwell sets the pre-Configuring (Creating) runway.
func runwayScenario(d, provisionDwell, faultCycle, cycles int) sim.ClosedLoopScenario {
	// One rack holds the gang plus ample in-rack Speculative headroom, so a
	// single loss is replaced in-domain rather than triggering a whole-gang
	// rack migration (which would be a different pathology).
	const specPerGangRack = 40
	return sim.ClosedLoopScenario{
		Name:   "preconfiguring-runway",
		Shapes: runwayGangShapes(),
		Clusters: []sim.ClusterSpec{
			{ID: "c1", Workloads: []sim.WorkloadSpec{{Shape: "gpu-train", Replicas: d}}},
		},
		Seeds: []sim.SeedPool{{
			Shape:                "gpu-train",
			Density:              1,
			ConfiguredPerCluster: d, // exact coverage at start
			Idle:                 0, // scarce: replacements come from Speculative
			Speculative:          200,
			ContiguousRackBlock:  d + specPerGangRack, // gang + in-rack spec headroom
			RacksPerZone:         64,
		}},
		ControllerManaged:          true,
		CRPerPod:                   true,
		RollupArrivalStamps:        true,
		BootstrapDwellCycles:       0, // isolate the PRE-Configuring runway
		ProvisionDwellCycles:       provisionDwell,
		StampSeededGangAttribution: true,
		Faults:                     []sim.FaultEvent{{Cycle: faultCycle, Cluster: "c1", Count: 1}},
		Cycles:                     cycles,
	}
}

// distinctProvisionTargets / distinctBootstrapTargets cannot be read from
// CycleStats (it only carries counts), so the repro counts acquisition
// ACTIONS over the post-fault window as the over-acquire proxy: each
// Provision/Bootstrap is a distinct machine commitment (the engine's dedup
// gate guarantees one action per machine per cycle, and a machine that is
// Provisioned, matures, and is later Bootstrapped is two distinct
// commitments toward the SAME one-machine deficit — exactly the
// over-acquire). The claimed-set-churn signal is the reclaim count: a
// machine reclaimed and re-acquired is the set changing.

func TestPreConfiguringRunway_OverAcquireRepro(t *testing.T) {
	const d, faultCycle, cycles = 7, 30, 120
	// Post-fault measurement window: everything from the fault cycle to the
	// end. The first 30 cycles let the initial fill settle to quiescence.
	postFault := cycles - faultCycle + 1 // include the fault cycle (faults inject before that cycle's Step)

	for _, dwell := range []int{0, 1, 2, 3, 5} {
		dwell := dwell
		t.Run(dwellName(dwell), func(t *testing.T) {
			res := runClosedLoop(t, runwayScenario(d, dwell, faultCycle, cycles))

			// Pre-fault quiescence sanity: the initial fill must converge
			// before the fault, or the measurement is contaminated.
			preFaultChurn := 0
			for _, c := range res.Cycles {
				if c.Cycle >= faultCycle {
					break
				}
				if c.Cycle > 15 { // allow the fill + provision-dwell to drain
					preFaultChurn += c.Churn()
				}
			}

			acq := res.SumLast(postFault, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions })
			provs := res.SumLast(postFault, func(c sim.CycleStats) int { return c.Provisions })
			boots := res.SumLast(postFault, func(c sim.CycleStats) int { return c.Bootstraps })
			recls := res.SumLast(postFault, func(c sim.CycleStats) int { return c.Reclaims })
			// Trailing-window churn (well after the fault): is it SUSTAINED
			// or did it settle?
			const tail = 40
			tailAcq := res.SumLast(tail, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions })
			tailRecl := res.SumLast(tail, func(c sim.CycleStats) int { return c.Reclaims })
			end := res.Last(1)[0]

			t.Logf("dwell=%d D=%d: preFaultChurn(c16..c%d)=%d | post-fault: acq=%d (prov=%d boot=%d) recl=%d | tail%d: acq=%d recl=%d | end configured=%d bound=%d/%d shortfalls=%d",
				dwell, d, faultCycle-1, preFaultChurn, acq, provs, boots, recls, tail, tailAcq, tailRecl,
				end.Configured, end.BoundPods, res.TargetPods, end.Shortfalls)

			// The over-acquire ratio: acquisition actions caused by ONE lost
			// incumbent (deficit = 1 machine). >1 means the gang acquired
			// more distinct machines than its deficit.
			t.Logf("  over-acquire ratio (post-fault acq actions ÷ deficit=1) = %d", acq)

			// GATE (was Logf-only): pin the over-acquire law. A single
			// 1-machine loss legitimately costs 1 acquisition; each cycle
			// the replacement spends in the INVISIBLE Creating runway adds
			// one spurious re-Provision, so acq = dwell+1. This pins the
			// DEFECT — when the Creating runway is counted (the fix), acq
			// must drop to 1 for every dwell and this assertion flips to
			// the fix's acceptance criterion.
			if acq != dwell+1 {
				t.Errorf("over-acquire law broken: acq=%d, want dwell+1=%d (dwell=%d)", acq, dwell+1, dwell)
			}
			if end.Shortfalls != 0 {
				t.Errorf("coverage must always be met: shortfalls=%d (dwell=%d)", end.Shortfalls, dwell)
			}

			dumpTrace(t, res)
		})
	}
}

// TestPreConfiguringRunway_RepeatedChurn is the dev-50 #66 shape: a gang
// under SUSTAINED incumbent churn (a loss every `period` cycles, the
// reclaimActions≈340-over-a-soak signature) rather than a single event. A
// standing deficit keeps the pre-Configuring runway continuously occupied,
// so the over-acquired surplus can reach Configured WHILE a deficit still
// stands — the condition for Phase 3 to reclaim it (the brief's full
// mechanism: redundant machines reach Configured, stop-when-covered leaves
// them unclaimed, Phase 3 reclaims). Measures whether the over-acquire
// compounds into a SUSTAINED Bootstrap≈Reclaim, and whether dwell=0 stays
// at the irreducible 1-replacement-per-loss floor (no over-acquire).
func TestPreConfiguringRunway_RepeatedChurn(t *testing.T) {
	const d, cycles, firstFault = 7, 220, 30
	type arm struct {
		dwell  int
		period int // cycles between incumbent losses
	}
	arms := []arm{
		{0, 1}, // control: loss every cycle, instant Create — the floor
		{2, 1}, // tight churn, runway 2: deficits overlap
		{4, 1}, // tighter overlap
		{4, 2},
		{4, 3},
	}
	type armResult struct {
		dwell, period int
		reclPerLoss   float64
	}
	var results []armResult
	for _, a := range arms {
		a := a
		t.Run(dwellName(a.dwell)+"-period"+itoa(a.period), func(t *testing.T) {
			var faults []sim.FaultEvent
			for c := firstFault; c < cycles-30; c += a.period {
				faults = append(faults, sim.FaultEvent{Cycle: c, Cluster: "c1", Count: 1})
			}
			nFaults := len(faults)
			dwell := a.dwell

			sc := runwayScenario(d, dwell, firstFault, cycles)
			sc.Faults = faults
			res := runClosedLoop(t, sc)

			// Window from the first fault to the end.
			win := cycles - firstFault + 1
			provs := res.SumLast(win, func(c sim.CycleStats) int { return c.Provisions })
			boots := res.SumLast(win, func(c sim.CycleStats) int { return c.Bootstraps })
			recls := res.SumLast(win, func(c sim.CycleStats) int { return c.Reclaims })
			acq := provs + boots
			end := res.Last(1)[0]

			// The floor: each lost incumbent legitimately needs exactly 1
			// replacement machine (1 Provision + 1 Bootstrap = 2 acq
			// actions). Anything beyond nFaults provisions is over-acquire;
			// any Reclaim at static demand is surplus churn.
			t.Logf("dwell=%d period=%d D=%d faults=%d: acq=%d (prov=%d boot=%d) recl=%d | end configured=%d bound=%d/%d shortfalls=%d",
				dwell, a.period, d, nFaults, acq, provs, boots, recls, end.Configured, end.BoundPods, res.TargetPods, end.Shortfalls)
			t.Logf("  per-loss: provisions/loss=%.2f reclaims/loss=%.2f (floor: 1.0 provision/loss, 0 reclaim/loss)",
				float64(provs)/float64(nFaults), float64(recls)/float64(nFaults))
			results = append(results, armResult{dwell: a.dwell, period: a.period, reclPerLoss: float64(recls) / float64(nFaults)})
			dumpTrace(t, res)
		})
	}

	// GATE: the pre-Configuring (Creating) runway lifts reclaims/loss
	// above the instant-Create (dwell0) floor — the sustained
	// Bootstrap≈Reclaim signature (#66/#74). The dwell0 control is the
	// async-actuation/timing floor and must stay small; every dwell>0 arm
	// must exceed it. POST-FIX (Creating counted) the dwell>0 arms drop
	// BACK to the floor — i.e. this assertion flips, and that flip is the
	// fix's acceptance criterion.
	var floor float64
	for _, r := range results {
		if r.dwell == 0 {
			floor = r.reclPerLoss
		}
	}
	if floor > 0.25 {
		t.Errorf("dwell0 (instant-Create) reclaims/loss=%.2f unexpectedly high — the residual floor is more than the async-actuation timing floor", floor)
	}
	for _, r := range results {
		if r.dwell == 0 {
			continue
		}
		if r.reclPerLoss <= floor {
			t.Errorf("dwell=%d period=%d reclaims/loss=%.2f did not exceed the dwell0 floor %.2f — the runway over-acquire signature is absent (mechanism drift, or fixed: flip this gate)", r.dwell, r.period, r.reclPerLoss, floor)
		}
	}
}

// burstScenario isolates the brief's FULL mechanism (over-acquire →
// surplus reaches Configured → Phase 3 reclaim) from the rack-exhaustion
// confound: ALL machines live in ONE rack (ContiguousRackBlock huge), so
// the gang never migrates racks and every replacement is trivially
// in-domain. A SINGLE burst of `lost` simultaneous incumbent losses gives a
// deficit > 1, so when the over-acquired surplus matures it can land at
// Configured (the gang briefly over-covers) rather than parking at Idle as
// a single-machine deficit does. The dwell=0 control must replace exactly
// `lost` machines and emit ZERO reclaims; the dwell>0 arm is the test of
// whether the runway over-acquire turns into Phase-3 reclaim.
func burstScenario(d, provisionDwell, lost, faultCycle, cycles int) sim.ClosedLoopScenario {
	return sim.ClosedLoopScenario{
		Name:   "runway-burst",
		Shapes: runwayGangShapes(),
		Clusters: []sim.ClusterSpec{
			{ID: "c1", Workloads: []sim.WorkloadSpec{{Shape: "gpu-train", Replicas: d}}},
		},
		Seeds: []sim.SeedPool{{
			Shape: "gpu-train", Density: 1,
			ConfiguredPerCluster: d, Idle: 0, Speculative: 400,
			ContiguousRackBlock: 400, // one rack: no migration, replacements in-domain
			RacksPerZone:        64,
		}},
		ControllerManaged:          true,
		CRPerPod:                   true,
		RollupArrivalStamps:        true,
		BootstrapDwellCycles:       0,
		ProvisionDwellCycles:       provisionDwell,
		StampSeededGangAttribution: true,
		Faults:                     []sim.FaultEvent{{Cycle: faultCycle, Cluster: "c1", Count: lost}},
		Cycles:                     cycles,
	}
}

// TestPreConfiguringRunway_BurstReachesConfigured tests whether the
// over-acquire surplus reaches Configured (and is then Phase-3 reclaimed)
// when a SINGLE burst loss opens a deficit > 1, with rack migration ruled
// out. This is the brief's full mechanism in its cleanest isolatable form.
func TestPreConfiguringRunway_BurstReachesConfigured(t *testing.T) {
	const d, lost, faultCycle, cycles = 7, 4, 30, 120
	postFault := cycles - faultCycle + 1 // include the fault cycle (faults inject before that cycle's Step)
	for _, dwell := range []int{0, 2, 4, 6} {
		dwell := dwell
		t.Run(dwellName(dwell), func(t *testing.T) {
			res := runClosedLoop(t, burstScenario(d, dwell, lost, faultCycle, cycles))
			provs := res.SumLast(postFault, func(c sim.CycleStats) int { return c.Provisions })
			boots := res.SumLast(postFault, func(c sim.CycleStats) int { return c.Bootstraps })
			recls := res.SumLast(postFault, func(c sim.CycleStats) int { return c.Reclaims })
			end := res.Last(1)[0]
			t.Logf("dwell=%d burst-lost=%d: prov=%d boot=%d recl=%d | end configured=%d bound=%d/%d",
				dwell, lost, provs, boots, recls, end.Configured, end.BoundPods, res.TargetPods)
			t.Logf("  over-acquire (prov ÷ deficit=%d) = %.2f; reclaims = %d", lost, float64(provs)/float64(lost), recls)
			// GATE: a ONE-TIME deficit (even >1) over-acquires, but the
			// surplus parks at Idle and never reaches Configured — so ZERO
			// reclaims. This pins that a SUSTAINED deficit is what drives
			// the surplus to Configured (the RepeatedChurn arm); without it
			// the runway over-acquire is reclaim-free.
			if recls != 0 {
				t.Errorf("one-time deficit must not reclaim: recl=%d (dwell=%d)", recls, dwell)
			}
			dumpTrace(t, res)
		})
	}
}

func dwellName(d int) string {
	switch d {
	case 0:
		return "dwell0-control"
	default:
		return "dwell" + itoa(d)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
