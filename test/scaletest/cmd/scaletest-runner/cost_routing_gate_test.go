package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// #354 cost-routing engine-correctness gate. The cost-routing-5k profile
// seeds two cost-distinct elastic tiers — Spot (cheap/high-interruption) via
// seed.spotCount and OnDemand (stable) via seed.speculativeCount — and drives
// two workloads that differ ONLY in interruption penalty. The gate asserts
// the engine routed by effective_cost (price + interruption_probability ×
// interruption_penalty): tolerant demand lands on Spot, sensitive on OnDemand.
// These tests pin (a) the profile + catalog parse/validate/merge and carry the
// load-bearing fields, (b) the Spot tier threads into the rendered Helm
// values, (c) pass()/unmeasuredGated/validate honour the gate.

func TestCostRoutingProfile_ValidV2(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	profilePath := filepath.Join(root, "test", "scaletest", "profiles", "cost-routing-5k.yaml")
	p, err := readProfileV2(profilePath)
	if err != nil {
		t.Fatalf("readProfileV2: %v", err)
	}
	if err := p.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := merge(p, s); err != nil {
		t.Fatalf("merge: %v", err)
	}

	if !p.SLO.ExpectCostRouting {
		t.Errorf("expectCostRouting = false (want true)")
	}
	if p.Seed.SpotCount <= 0 {
		t.Errorf("seed.spotCount = %d (want > 0 — the Spot tier the gate routes tolerant demand to)", p.Seed.SpotCount)
	}
	if p.Seed.SpeculativeCount <= 0 {
		t.Errorf("seed.speculativeCount = %d (want > 0 — the OnDemand tier)", p.Seed.SpeculativeCount)
	}
	if p.SLO.CostRoutingTolerantPenaltyBucket == "" || p.SLO.CostRoutingSensitivePenaltyBucket == "" {
		t.Errorf("cost-routing penalty buckets unset: tol=%q sen=%q", p.SLO.CostRoutingTolerantPenaltyBucket, p.SLO.CostRoutingSensitivePenaltyBucket)
	}
	if p.SLO.CostRoutingTolerantPenaltyBucket == p.SLO.CostRoutingSensitivePenaltyBucket {
		t.Errorf("tolerant and sensitive buckets must differ (both %q) — the two workloads must straddle the flip", p.SLO.CostRoutingTolerantPenaltyBucket)
	}
	// preBind would fast-bind to fake-Nodes and bypass the engine's routing
	// decision, making the whole test vacuous.
	if p.Seed.PreBind {
		t.Errorf("seed.preBind = true (want false — preBind bypasses the provisioning/routing path)")
	}
	// No Configured/Idle seed, so demand must walk to the elastic tiers where
	// the effective_cost sort runs (the Idle pool sorts raw price and masks
	// the interruption-probability term).
	if p.Seed.ConfiguredFraction != 0 || p.Seed.IdleHeadroomFraction != 0 {
		t.Errorf("cost-routing must seed no Configured/Idle (got configured=%g idle=%g)", p.Seed.ConfiguredFraction, p.Seed.IdleHeadroomFraction)
	}

	// The catalog parses and carries exactly one tolerant (penalty 0) and one
	// sensitive (penalty > 0) archetype.
	_, typed, err := loadCatalogArchetypes(profilePath, p.Catalog.Archetypes)
	if err != nil {
		t.Fatalf("loadCatalogArchetypes: %v", err)
	}
	if len(typed) != 2 {
		t.Fatalf("cost-routing catalog has %d archetypes (want 2: tolerant + sensitive)", len(typed))
	}
	var zero, nonzero int
	for _, a := range typed {
		if a.InterruptionPenalty == 0 {
			zero++
		} else {
			nonzero++
		}
	}
	if zero != 1 || nonzero != 1 {
		t.Errorf("want exactly one tolerant (penalty 0) and one sensitive (penalty > 0) archetype; got zero=%d nonzero=%d", zero, nonzero)
	}
}

// TestRenderHelmValues_SeedSpot pins that the Spot tier threads from the V2
// profile into the rendered shard values — the round-trip the silent-drop
// guards exist to enforce. The OnDemand tier must still seed alongside it.
func TestRenderHelmValues_SeedSpot(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "cost-routing-5k.yaml"))
	if err != nil {
		t.Fatalf("readProfileV2: %v", err)
	}
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	shard, ok := values["shard"].(map[string]any)
	if !ok {
		t.Fatalf("shard block missing or wrong type: %T", values["shard"])
	}
	if got, _ := shard["seedSpot"].(int); got != p.Seed.SpotCount {
		t.Errorf("rendered shard.seedSpot = %v (want %d — Spot tier absolute per-shard count)", shard["seedSpot"], p.Seed.SpotCount)
	}
	if got, _ := shard["seedSpeculative"].(int); got <= 0 {
		t.Errorf("rendered shard.seedSpeculative = %v (want > 0 — the OnDemand tier must coexist)", shard["seedSpeculative"])
	}
}

// TestRenderHelmValues_SeedSpotAbsentByDefault is the absence pin: a profile
// that does not set seed.spotCount renders seedSpot 0, so every non-cost-routing
// profile is byte-identical and seeds no Spot tier.
func TestRenderHelmValues_SeedSpotAbsentByDefault(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "preemption-5k.yaml"))
	if err != nil {
		t.Fatalf("readProfileV2: %v", err)
	}
	if p.Seed.SpotCount != 0 {
		t.Fatalf("fixture unexpectedly seeds a Spot tier: %d", p.Seed.SpotCount)
	}
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	shard := values["shard"].(map[string]any)
	if got, _ := shard["seedSpot"].(int); got != 0 {
		t.Errorf("rendered shard.seedSpot = %v for a non-cost-routing profile (want 0)", got)
	}
}

func TestPass_ExpectCostRouting(t *testing.T) {
	const totalCRs, shardReplicas = 1000, 1

	crSLO := func() sloOverrides {
		slo := passingSLO()
		slo.ExpectCostRouting = true
		slo.CostRoutingTolerantPenaltyBucket = "0"
		slo.CostRoutingSensitivePenaltyBucket = "8"
		slo.CostRoutingMisroutedMax = 0
		return slo
	}

	tests := []struct {
		name       string
		mutate     func(m map[string]float64, slo *sloOverrides)
		wantPass   bool
		wantSubstr string
	}{
		{
			name:     "correct routing, zero misrouted passes",
			mutate:   func(m map[string]float64, slo *sloOverrides) {},
			wantPass: true,
		},
		{
			name: "any misrouted (over strict max) fails",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["costRoutingMisrouted"] = 3 // tolerant on OnDemand, or sensitive on Spot
			},
			wantPass:   false,
			wantSubstr: "costRoutingMisrouted",
		},
		{
			name: "misrouted within tolerance passes",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.CostRoutingMisroutedMax = 5
				m["costRoutingMisrouted"] = 3 // churn straggler slack
			},
			wantPass: true,
		},
		{
			name: "misrouted at the bound passes (strict >)",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.CostRoutingMisroutedMax = 3
				m["costRoutingMisrouted"] = 3 // == max
			},
			wantPass: true,
		},
		{
			name: "zero correctly-routed is a vacuous failure",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["costRoutingCorrect"] = 0 // nothing provisioned onto the tiers
			},
			wantPass:   false,
			wantSubstr: "costRoutingCorrect",
		},
		{
			name: "correct sentinel (-1) is skipped",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["costRoutingCorrect"] = -1 // failed scrape, must not fail the run
			},
			wantPass: true,
		},
		{
			name: "misrouted sentinel (-1) is skipped",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["costRoutingMisrouted"] = -1 // failed scrape
			},
			wantPass: true,
		},
		{
			name: "gate is opt-in: misrouting ignored when expectCostRouting unset",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.ExpectCostRouting = false
				m["costRoutingMisrouted"] = 999 // ungated, must not fail
				m["costRoutingCorrect"] = 0
			},
			wantPass: true,
		},
		{
			name: "every other ADR-0054 gate still applies",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardConfigurePhaseP99Seconds"] = 20 // > 15, must still fail
			},
			wantPass:   false,
			wantSubstr: "shardConfigurePhaseP99Seconds",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := passingMetrics()
			m["costRoutingCorrect"] = 5000 // routing happened
			m["costRoutingMisrouted"] = 0  // nothing on the wrong tier
			slo := crSLO()
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

// TestUnmeasuredGated_CostRoutingKeys confirms the cost-routing keys are
// flagged unmeasured (sentinel -1) only when ExpectCostRouting is declared.
func TestUnmeasuredGated_CostRoutingKeys(t *testing.T) {
	t.Run("flagged when expectCostRouting set", func(t *testing.T) {
		slo := passingSLO()
		slo.ExpectCostRouting = true
		m := map[string]float64{
			"shardCycleDurationP99Seconds": 0.3,
			"operatorRollupP99Seconds":     0.2,
			"operatorAckP99Seconds":        9.0,
			"costRoutingCorrect":           -1,
			"costRoutingMisrouted":         -1,
		}
		got := unmeasuredGated(m, slo)
		want := map[string]bool{"costRoutingCorrect": true, "costRoutingMisrouted": true}
		for _, k := range got {
			delete(want, k)
		}
		if len(want) != 0 {
			t.Fatalf("cost-routing keys not flagged unmeasured: %v (got %v)", want, got)
		}
	})

	t.Run("not flagged when expectCostRouting unset", func(t *testing.T) {
		slo := passingSLO() // ExpectCostRouting not set
		m := map[string]float64{
			"costRoutingCorrect":   -1,
			"costRoutingMisrouted": -1,
		}
		got := unmeasuredGated(m, slo)
		for _, k := range got {
			if k == "costRoutingCorrect" || k == "costRoutingMisrouted" {
				t.Fatalf("ungated cost-routing key %q flagged as unmeasured: %v", k, got)
			}
		}
	})
}

// TestValidate_CostRoutingRequiresConfig pins that expectCostRouting is
// rejected unless the profile actually wires the two tiers and both penalty
// buckets — else the run would pass vacuously or scrape an empty query.
func TestValidate_CostRoutingRequiresConfig(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	profilePath := filepath.Join(root, "test", "scaletest", "profiles", "cost-routing-5k.yaml")

	// load returns a fresh, valid cost-routing profile each call.
	load := func(t *testing.T) profileV2 {
		t.Helper()
		p, err := readProfileV2(profilePath)
		if err != nil {
			t.Fatalf("readProfileV2: %v", err)
		}
		if err := p.validate(); err != nil {
			t.Fatalf("baseline must validate: %v", err)
		}
		return p
	}

	tests := []struct {
		name   string
		mutate func(p *profileV2)
		substr string
	}{
		{
			name:   "no Spot tier",
			mutate: func(p *profileV2) { p.Seed.SpotCount = 0 },
			substr: "spotCount",
		},
		{
			name: "no OnDemand tier",
			mutate: func(p *profileV2) {
				p.Seed.SpeculativeCount = 0
				p.Seed.SpeculativeMultiplier = 0
			},
			substr: "OnDemand",
		},
		{
			name:   "missing sensitive bucket",
			mutate: func(p *profileV2) { p.SLO.CostRoutingSensitivePenaltyBucket = "" },
			substr: "PenaltyBucket",
		},
		{
			name: "identical buckets",
			mutate: func(p *profileV2) {
				p.SLO.CostRoutingSensitivePenaltyBucket = p.SLO.CostRoutingTolerantPenaltyBucket
			},
			substr: "must differ",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := load(t)
			tc.mutate(&p)
			err := p.validate()
			if err == nil {
				t.Fatalf("validate() = nil, want an error mentioning %q", tc.substr)
			}
			if !strings.Contains(err.Error(), tc.substr) {
				t.Fatalf("validate() error %q does not mention %q", err.Error(), tc.substr)
			}
		})
	}
}
