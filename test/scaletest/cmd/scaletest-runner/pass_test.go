package main

import (
	"strings"
	"testing"
)

// ADR-0054 reframed the steady release gate off the end-to-end pod-bind
// p99 (uncapped-scheduler / reprovision-bound, not BigFleet's
// deliverable) onto BigFleet's capacity-delivery hops. There was no
// existing pass() test, so each new gate is exercised here both
// set-and-breached (it must fail) and unset-and-skipped (a 0 override
// must NOT gate even when the metric is bad). The MIN-direction
// bootstrapSuccessRatio gate gets special attention: it fails BELOW the
// target, the opposite direction from every latency gate.

// passingSLO is a profile that gates every ADR-0054 BigFleet-property
// bar plus the retained engine gates — the cloud-ladder posture.
func passingSLO() sloOverrides {
	return sloOverrides{
		ShardCycleDurationP99Seconds:      5,
		OperatorRollupP99Seconds:          1,
		OperatorAckP99Seconds:             12,
		ShardConfigurePhaseP99Seconds:     15,
		BootstrapSuccessRatio:             0.99,
		OperatorNodeStateUpdateP99Seconds: 1,
		EndToEndPodBindP50Seconds:         10,
	}
}

// passingMetrics is a metric map well inside every gate in passingSLO,
// with the run-validity preconditions (sustained load, all shards
// reporting) satisfied for totalCRs=1000, shardReplicas=1.
func passingMetrics() map[string]float64 {
	return map[string]float64{
		"loadgenCRsActive":                  1000,
		"shardsReportingCycle":              1,
		"shardConfigurePhaseP99Seconds":     0.6,
		"bootstrapSuccessRatio":             0.999,
		"operatorNodeStateUpdateP99Seconds": 0.2,
		"shardShortfalls":                   0,
		"endToEndPodBindP50Seconds":         3.0,
		"shardCycleDurationP99Seconds":      0.3,
		"operatorRollupP99Seconds":          0.2,
		"operatorAckP99Seconds":             9.0,
		"coordinatorApplyErrorRate":         0,
		"operatorOutboxDropsPerSec":         0,
		// internalBindingLatencyP99Seconds is RETIRED to informational —
		// a sky-high value must NOT flip the verdict (ADR-0054).
		"internalBindingLatencyP99Seconds": 900,
	}
}

func TestPass(t *testing.T) {
	const totalCRs, shardReplicas = 1000, 1

	tests := []struct {
		name       string
		mutate     func(m map[string]float64, slo *sloOverrides)
		wantPass   bool
		wantSubstr string // substring expected in the failure reason
	}{
		{
			name:     "all pass",
			mutate:   func(m map[string]float64, slo *sloOverrides) {},
			wantPass: true,
		},
		{
			name: "configure-phase breach",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardConfigurePhaseP99Seconds"] = 20 // > 15
			},
			wantPass:   false,
			wantSubstr: "shardConfigurePhaseP99Seconds",
		},
		{
			name: "configure-phase skipped when override is 0",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.ShardConfigurePhaseP99Seconds = 0
				m["shardConfigurePhaseP99Seconds"] = 9999 // bad, but ungated
			},
			wantPass: true,
		},
		{
			name: "bootstrapSuccessRatio breach (below min)",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["bootstrapSuccessRatio"] = 0.80 // < 0.99 min
			},
			wantPass:   false,
			wantSubstr: "bootstrapSuccessRatio",
		},
		{
			name: "bootstrapSuccessRatio at min is not a breach",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["bootstrapSuccessRatio"] = 0.99 // == min, MIN gate is strict <
			},
			wantPass: true,
		},
		{
			name: "bootstrapSuccessRatio ABOVE min is not a breach (direction check)",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["bootstrapSuccessRatio"] = 1.0 // a high ratio must NOT fail
			},
			wantPass: true,
		},
		{
			name: "bootstrapSuccessRatio skipped when override is 0",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.BootstrapSuccessRatio = 0
				m["bootstrapSuccessRatio"] = 0.01 // collapse, but ungated
			},
			wantPass: true,
		},
		{
			name: "operatorNodeStateUpdate breach",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["operatorNodeStateUpdateP99Seconds"] = 5 // > 1
			},
			wantPass:   false,
			wantSubstr: "operatorNodeStateUpdateP99Seconds",
		},
		{
			name: "operatorNodeStateUpdate skipped when override is 0",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.OperatorNodeStateUpdateP99Seconds = 0
				m["operatorNodeStateUpdateP99Seconds"] = 9999 // bad, but ungated
			},
			wantPass: true,
		},
		{
			name: "shardShortfalls > 0",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardShortfalls"] = 7
			},
			wantPass:   false,
			wantSubstr: "shardShortfalls",
		},
		{
			name: "shardShortfalls gate is always on (no override)",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				// Clear every optional override; shortfalls!=0 must STILL
				// fail (it is unconditional, not opt-in).
				slo.ShardConfigurePhaseP99Seconds = 0
				slo.BootstrapSuccessRatio = 0
				slo.OperatorNodeStateUpdateP99Seconds = 0
				slo.EndToEndPodBindP50Seconds = 0
				m["shardShortfalls"] = 1
			},
			wantPass:   false,
			wantSubstr: "shardShortfalls",
		},
		{
			name: "shardShortfalls sentinel (-1) is skipped",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardShortfalls"] = -1 // failed scrape, not a breach
			},
			wantPass: true,
		},
		{
			name: "endToEndPodBindP50 breach",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["endToEndPodBindP50Seconds"] = 30 // > 10
			},
			wantPass:   false,
			wantSubstr: "endToEndPodBindP50Seconds",
		},
		{
			name: "endToEndPodBindP50 skipped when override is 0",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				slo.EndToEndPodBindP50Seconds = 0
				m["endToEndPodBindP50Seconds"] = 9999 // bad, but ungated
			},
			wantPass: true,
		},
		{
			name: "retired internalBindingLatencyP99Seconds never gates",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				// Even with the legacy override set AND a sky-high value,
				// the retired metric must not flip the verdict.
				slo.InternalBindingLatencyP99Seconds = 15
				m["internalBindingLatencyP99Seconds"] = 1300
			},
			wantPass: true,
		},
		{
			name: "configure-phase sentinel (-1) is skipped",
			mutate: func(m map[string]float64, slo *sloOverrides) {
				m["shardConfigurePhaseP99Seconds"] = -1 // failed scrape
			},
			wantPass: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := passingMetrics()
			slo := passingSLO()
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

// TestUnmeasuredGated_ADR0054 confirms the new gated keys are flagged
// when their override is set and the scrape returned the -1 sentinel,
// and that the retired internalBindingLatencyP99Seconds is NOT flagged
// (it no longer gates, so a -1 on it is not a vacuous pass).
func TestUnmeasuredGated_ADR0054(t *testing.T) {
	slo := passingSLO()
	m := map[string]float64{
		"shardCycleDurationP99Seconds":      0.3,
		"operatorRollupP99Seconds":          0.2,
		"operatorAckP99Seconds":             9.0,
		"reclaimActionsDuringSoak":          0,
		"shardShortfalls":                   -1, // unmeasured, always-gated
		"shardConfigurePhaseP99Seconds":     -1, // unmeasured, gated this run
		"bootstrapSuccessRatio":             -1, // unmeasured, gated this run
		"operatorNodeStateUpdateP99Seconds": -1, // unmeasured, gated this run
		"endToEndPodBindP50Seconds":         -1, // unmeasured, gated this run
		"internalBindingLatencyP99Seconds":  -1, // RETIRED — must NOT be flagged
	}
	got := unmeasuredGated(m, slo)
	want := map[string]bool{
		"shardShortfalls":                   true,
		"shardConfigurePhaseP99Seconds":     true,
		"bootstrapSuccessRatio":             true,
		"operatorNodeStateUpdateP99Seconds": true,
		"endToEndPodBindP50Seconds":         true,
	}
	for _, k := range got {
		if k == "internalBindingLatencyP99Seconds" {
			t.Fatalf("retired internalBindingLatencyP99Seconds was flagged as unmeasured-gated: %v", got)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Fatalf("expected unmeasured keys not flagged: %v (got %v)", want, got)
	}
}

// TestUnmeasuredGated_OptionalKeysSkippedWhenUnset confirms that an
// optional ADR-0054 key is NOT flagged unmeasured when its override is
// 0 (it doesn't gate, so its -1 is irrelevant — the bug the retired
// internalBindingLatencyP99Seconds caused).
func TestUnmeasuredGated_OptionalKeysSkippedWhenUnset(t *testing.T) {
	slo := sloOverrides{
		ShardCycleDurationP99Seconds: 5,
		OperatorRollupP99Seconds:     1,
		OperatorAckP99Seconds:        12,
		// All ADR-0054 optional bars unset.
	}
	m := map[string]float64{
		"shardConfigurePhaseP99Seconds":     -1,
		"bootstrapSuccessRatio":             -1,
		"operatorNodeStateUpdateP99Seconds": -1,
		"endToEndPodBindP50Seconds":         -1,
	}
	got := unmeasuredGated(m, slo)
	for _, k := range got {
		if k == "shardConfigurePhaseP99Seconds" ||
			k == "bootstrapSuccessRatio" ||
			k == "operatorNodeStateUpdateP99Seconds" ||
			k == "endToEndPodBindP50Seconds" {
			t.Fatalf("ungated optional key %q flagged as unmeasured-gated: %v", k, got)
		}
	}
}
