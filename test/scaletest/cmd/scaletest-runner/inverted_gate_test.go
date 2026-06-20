package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// The inverted SLO gates let two engine-correctness profiles be graded
// that the steady-state gate cannot express, because it treats a standing
// shortfall + flat acquisitions as a demand-side FAILURE:
//
//   - scarcity-5k:   supply < demand by design; a NON-ZERO, CONVERGED
//     standing shortfall is the PASS condition (expectStandingShortfall).
//   - preemption-5k: a high-priority burst against a zero-headroom
//     low-priority fleet; a Preempt-action count > 0 is the PASS
//     condition (expectPreemptions).
//
// These tests pin (a) both profiles parse → validate → merge and carry
// the inverted-gate fields, and (b) pass() honours the inverted fields:
// a confined-and-converged scarcity shortfall passes; an unconverged
// (growing) one fails; a covered (zero-shortfall) scarcity run fails as
// vacuous; the preempt gate fails on zero Preempts and passes on > 0.

// TestInvertedGateProfiles_ValidV2 guards the two engine-correctness
// profiles through the real readProfileV2 → validate → merge path and
// pins the load-bearing inverted-gate fields. Neither is in the
// ladder-only BYO matrix, so this is their parse/merge regression guard.
func TestInvertedGateProfiles_ValidV2(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}

	t.Run("scarcity-5k", func(t *testing.T) {
		t.Parallel()
		p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "scarcity-5k.yaml"))
		if err != nil {
			t.Fatalf("readProfileV2: %v", err)
		}
		if err := p.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		cfg, err := merge(p, s)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if cfg.ClusterCount <= 0 {
			t.Errorf("merged ClusterCount = %d (want > 0)", cfg.ClusterCount)
		}
		if p.Scale.Machines != 5000 {
			t.Errorf("scale.machines = %d (want 5000 — uber-5k shape)", p.Scale.Machines)
		}
		// The inverted shortfall gate must be ON, with a convergence bound.
		if !p.SLO.ExpectStandingShortfall {
			t.Errorf("expectStandingShortfall = false (want true — scarcity is a standing-shortfall run)")
		}
		if p.SLO.ShortfallStabilityMax <= 0 {
			t.Errorf("shortfallStabilityMax = %v (want > 0 — the convergence bound)", p.SLO.ShortfallStabilityMax)
		}
		// The supply-starving knobs: no Speculative pool to grow into, no
		// idle buffer, under-provisioned Configured fraction. This is what
		// makes the shortfall GENUINE undersupply rather than a slow ramp.
		if p.Seed.SpeculativeMultiplier != 0 {
			t.Errorf("seed.speculativeMultiplier = %d (want 0 — no elastic pool, the shortfall must be unclosable)", p.Seed.SpeculativeMultiplier)
		}
		if p.Seed.IdleHeadroomFraction != 0 {
			t.Errorf("seed.idleHeadroomFraction = %v (want 0 — no churn buffer)", p.Seed.IdleHeadroomFraction)
		}
		if p.Seed.ConfiguredFraction <= 0 || p.Seed.ConfiguredFraction >= 1 {
			t.Errorf("seed.configuredFraction = %v (want in (0,1) — under-provisioned at install)", p.Seed.ConfiguredFraction)
		}
		// The preempt gate must NOT be on for the scarcity run.
		if p.SLO.ExpectPreemptions {
			t.Errorf("expectPreemptions = true (want false — scarcity is not a preemption run)")
		}
		// The CONFINEMENT half must be ON, pinned to the realistic catalog's
		// lowest tier ("batch"): the standing shortfall must sit entirely in
		// the lowest priority class, never starving higher-priority demand.
		if p.SLO.ShortfallConfinedBelowPriorityClass != "batch" {
			t.Errorf("shortfallConfinedBelowPriorityClass = %q (want \"batch\" — the sole-throttle confinement assertion)", p.SLO.ShortfallConfinedBelowPriorityClass)
		}
	})

	t.Run("preemption-5k", func(t *testing.T) {
		t.Parallel()
		p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "preemption-5k.yaml"))
		if err != nil {
			t.Fatalf("readProfileV2: %v", err)
		}
		if err := p.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		cfg, err := merge(p, s)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if cfg.ClusterCount <= 0 {
			t.Errorf("merged ClusterCount = %d (want > 0)", cfg.ClusterCount)
		}
		if p.Scale.Machines != 5000 {
			t.Errorf("scale.machines = %d (want 5000 — uber-5k shape)", p.Scale.Machines)
		}
		// The preempt gate must be ON.
		if !p.SLO.ExpectPreemptions {
			t.Errorf("expectPreemptions = false (want true — preemption is the pass condition)")
		}
		// Zero headroom: 100% Configured, no idle, no speculative — the only
		// way to bind the burst is to preempt.
		if p.Seed.ConfiguredFraction != 1.0 {
			t.Errorf("seed.configuredFraction = %v (want 1.0 — fully Configured fleet)", p.Seed.ConfiguredFraction)
		}
		if p.Seed.IdleHeadroomFraction != 0 || p.Seed.SpeculativeMultiplier != 0 {
			t.Errorf("seed headroom = idle %v / spec %d (want both 0 — zero headroom forces preemption)",
				p.Seed.IdleHeadroomFraction, p.Seed.SpeculativeMultiplier)
		}
		// The high-priority burst that forces the preemption.
		if len(p.LoadProfile.Bursts) != 1 {
			t.Fatalf("bursts = %+v (want exactly one high-priority burst)", p.LoadProfile.Bursts)
		}
		b := p.LoadProfile.Bursts[0]
		if b.Archetype != "critical-realtime" {
			t.Errorf("burst archetype = %q (want critical-realtime — the catalog's top-priority archetype)", b.Archetype)
		}
		if b.ExtraTarget <= 0 {
			t.Errorf("burst extraTarget = %d (want > 0 — a slug large enough to force preemption)", b.ExtraTarget)
		}
		// The shortfall gate is NOT inverted here: the burst must end
		// satisfied, so the default shardShortfalls == 0 gate is the
		// "ends satisfied" assertion.
		if p.SLO.ExpectStandingShortfall {
			t.Errorf("expectStandingShortfall = true (want false — the burst must end satisfied)")
		}
	})
}

// TestInvertedGateProfiles_RenderSLO confirms the inverted-gate fields
// survive renderHelmValues into the chart's slo block (the chart passes
// them to the shard/load-driver; without the round-trip the inversion
// would be silently dropped, exactly like the V2-struct gate on bursts).
func TestInvertedGateProfiles_RenderSLO(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "scarcity-5k.yaml"))
	if err != nil {
		t.Fatalf("readProfileV2: %v", err)
	}
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	slo, ok := values["slo"].(sloOverrides)
	if !ok {
		t.Fatalf("slo block missing or wrong type: %T", values["slo"])
	}
	if !slo.ExpectStandingShortfall {
		t.Errorf("rendered slo.ExpectStandingShortfall = false (want true)")
	}
	if slo.ShortfallStabilityMax <= 0 {
		t.Errorf("rendered slo.ShortfallStabilityMax = %v (want > 0)", slo.ShortfallStabilityMax)
	}
	if slo.ShortfallConfinedBelowPriorityClass != "batch" {
		t.Errorf("rendered slo.ShortfallConfinedBelowPriorityClass = %q (want \"batch\")", slo.ShortfallConfinedBelowPriorityClass)
	}
}

// scarcityMetrics is a metric map representing a healthy, converged
// scarcity steady state: a standing shortfall (> 0) that is converged
// (delta within tolerance) AND confined to the lowest priority tier
// (zero shortfall above the floor), every other ADR-0054 gate satisfied.
func scarcityMetrics() map[string]float64 {
	m := passingMetrics()
	m["shardShortfalls"] = 1200        // standing — expected under scarcity
	m["shardShortfallsDelta"] = 5      // converged
	m["shortfallAboveLowestClass"] = 0 // confined to the lowest tier
	return m
}

func TestPass_InvertedShortfall(t *testing.T) {
	const totalCRs, shardReplicas = 1000, 1

	scarcitySLO := func() sloOverrides {
		slo := passingSLO()
		slo.ExpectStandingShortfall = true
		slo.ShortfallStabilityMax = 20
		slo.ShortfallConfinedBelowPriorityClass = "batch"
		return slo
	}

	tests := []struct {
		name       string
		mutate     func(m map[string]float64, slo *sloOverrides)
		wantPass   bool
		wantSubstr string
	}{
		{
			name:     "converged standing shortfall passes",
			mutate:   func(m map[string]float64, slo *sloOverrides) {},
			wantPass: true,
		},
		{
			name: "zero shortfall under expectStandingShortfall is a vacuous failure",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardShortfalls"] = 0 // the seed wasn't actually scarce
			},
			wantPass:   false,
			wantSubstr: "expectStandingShortfall",
		},
		{
			name: "growing (unconverged) shortfall fails",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardShortfallsDelta"] = 500 // demand outrunning the engine
			},
			wantPass:   false,
			wantSubstr: "shardShortfallsDelta",
		},
		{
			name: "delta at the bound passes (strict >)",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardShortfallsDelta"] = 20 // == max
			},
			wantPass: true,
		},
		{
			name: "shortfall-delta sentinel (-1) is skipped",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardShortfallsDelta"] = -1 // failed scrape, must not fail the run
			},
			wantPass: true,
		},
		{
			name: "shortfall sentinel (-1) is skipped even when inverted",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardShortfalls"] = -1 // failed scrape
			},
			wantPass: true,
		},
		{
			name: "every other ADR-0054 gate still applies under inversion",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardConfigurePhaseP99Seconds"] = 20 // > 15, must still fail
			},
			wantPass:   false,
			wantSubstr: "shardConfigurePhaseP99Seconds",
		},
		{
			name: "confined shortfall (zero above the floor) passes",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shortfallAboveLowestClass"] = 0 // sole-throttle honoured
			},
			wantPass: true,
		},
		{
			name: "high-priority shortfall (above the floor) fails — sole-throttle violated",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shortfallAboveLowestClass"] = 3 // service/critical demand starved
			},
			wantPass:   false,
			wantSubstr: "shortfallAboveLowestClass",
		},
		{
			name: "confinement sentinel (-1) is skipped",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shortfallAboveLowestClass"] = -1 // failed scrape, must not fail the run
			},
			wantPass: true,
		},
		{
			name: "confinement gate is opt-in: high-priority shortfall ungated when class unset",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.ShortfallConfinedBelowPriorityClass = ""
				m["shortfallAboveLowestClass"] = 9 // present but ungated, must not fail
			},
			wantPass: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := scarcityMetrics()
			slo := scarcitySLO()
			tc.mutate(m, &slo)
			ok, reason := pass(m, totalCRs, shardReplicas, slo)
			if ok != tc.wantPass {
				t.Fatalf("pass() = %v (%q), want %v", ok, reason, tc.wantPass)
			}
			if !tc.wantPass && !strings.Contains(reason, tc.wantSubstr) {
				t.Fatalf("failure reason %q does not mention %q", reason, tc.wantSubstr)
			}
			if tc.wantPass && reason != "" {
				t.Fatalf("pass() returned a non-empty reason on a pass: %q", reason)
			}
		})
	}
}

// TestPass_DefaultShortfallGateUnchanged confirms the inverted shortfall
// gate is fully opt-in: with ExpectStandingShortfall unset, the original
// "shortfalls != 0 → fail" verdict is byte-identical (a standing
// shortfall is still a failure for a capacity-met run).
func TestPass_DefaultShortfallGateUnchanged(t *testing.T) {
	const totalCRs, shardReplicas = 1000, 1
	m := passingMetrics()
	m["shardShortfalls"] = 7 // standing shortfall
	slo := passingSLO()      // ExpectStandingShortfall not set
	ok, reason := pass(m, totalCRs, shardReplicas, slo)
	if ok {
		t.Fatalf("pass() = true, want false — a standing shortfall must still fail a non-inverted run")
	}
	if !strings.Contains(reason, "shardShortfalls") {
		t.Fatalf("failure reason %q does not mention shardShortfalls", reason)
	}
}

func TestPass_ExpectPreemptions(t *testing.T) {
	const totalCRs, shardReplicas = 1000, 1

	preemptSLO := func() sloOverrides {
		slo := passingSLO()
		slo.ExpectPreemptions = true
		return slo
	}

	tests := []struct {
		name       string
		mutate     func(m map[string]float64, slo *sloOverrides)
		wantPass   bool
		wantSubstr string
	}{
		{
			name: "preempt count > 0 passes",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["preemptActions"] = 200
			},
			wantPass: true,
		},
		{
			name: "zero preempts under expectPreemptions fails",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["preemptActions"] = 0 // the engine never preempted
			},
			wantPass:   false,
			wantSubstr: "preemptActions",
		},
		{
			name: "preempt gate is opt-in: zero preempts pass when unset",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.ExpectPreemptions = false
				m["preemptActions"] = 0 // ungated, must not fail
			},
			wantPass: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := passingMetrics()
			m["preemptActions"] = 1 // default present so the key exists
			slo := preemptSLO()
			tc.mutate(m, &slo)
			ok, reason := pass(m, totalCRs, shardReplicas, slo)
			if ok != tc.wantPass {
				t.Fatalf("pass() = %v (%q), want %v", ok, reason, tc.wantPass)
			}
			if !tc.wantPass && !strings.Contains(reason, tc.wantSubstr) {
				t.Fatalf("failure reason %q does not mention %q", reason, tc.wantSubstr)
			}
			if tc.wantPass && reason != "" {
				t.Fatalf("pass() returned a non-empty reason on a pass: %q", reason)
			}
		})
	}
}

// TestUnmeasuredGated_InvertedKeys confirms the inverted-gate keys are
// flagged unmeasured (sentinel -1) only when their inverted posture is
// declared, and not otherwise — mirroring the set-then-gate discipline.
func TestUnmeasuredGated_InvertedKeys(t *testing.T) {
	t.Run("flagged when inverted posture set", func(t *testing.T) {
		slo := passingSLO()
		slo.ExpectStandingShortfall = true
		slo.ExpectPreemptions = true
		slo.ShortfallConfinedBelowPriorityClass = "batch"
		m := map[string]float64{
			"shardCycleDurationP99Seconds": 0.3,
			"operatorRollupP99Seconds":     0.2,
			"operatorAckP99Seconds":        9.0,
			"shardShortfallsDelta":         -1,
			"shortfallAboveLowestClass":    -1,
			"preemptActions":               -1,
		}
		got := unmeasuredGated(m, slo)
		want := map[string]bool{"shardShortfallsDelta": true, "shortfallAboveLowestClass": true, "preemptActions": true}
		for _, k := range got {
			delete(want, k)
		}
		if len(want) != 0 {
			t.Fatalf("inverted keys not flagged unmeasured: %v (got %v)", want, got)
		}
	})

	t.Run("not flagged when inverted posture unset", func(t *testing.T) {
		slo := passingSLO() // neither inverted field set
		m := map[string]float64{
			"shardShortfallsDelta":      -1,
			"shortfallAboveLowestClass": -1,
			"preemptActions":            -1,
		}
		got := unmeasuredGated(m, slo)
		for _, k := range got {
			if k == "shardShortfallsDelta" || k == "shortfallAboveLowestClass" || k == "preemptActions" {
				t.Fatalf("ungated inverted key %q flagged as unmeasured: %v", k, got)
			}
		}
	})
}
