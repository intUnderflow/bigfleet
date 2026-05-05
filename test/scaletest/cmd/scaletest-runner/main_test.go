package main

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// Regression test for the bug that produced -1 for every per-phase
// histogram in summary.json (e.g. shardPhase{1,2,3,Reconcile,Execute}
// P99Seconds). The previous hand-rolled urlEncode left `=`, `"`, `{`
// and `}` unescaped, so prometheus parsed `?query=...{phase=` then
// `"reconcile"}...` as separate URL parameters and saw a malformed
// query.
func TestURLEncodePreservesLabelMatchers(t *testing.T) {
	q := `histogram_quantile(0.99, sum by (le) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket{phase="reconcile"}[5m])))`
	encoded := urlEncode(q)
	if strings.Contains(encoded, `{phase="`) {
		t.Fatalf("urlEncode left {phase=\"…\"} unescaped, prometheus will see split params: %s", encoded)
	}
	decoded, err := url.QueryUnescape(encoded)
	if err != nil {
		t.Fatalf("encoded form is not a valid percent-encoded string: %v", err)
	}
	if decoded != q {
		t.Fatalf("round-trip failed:\n  got: %s\n want: %s", decoded, q)
	}
}

// expectedRampFromCRsPerSec mirrors the formula in resolveRampBudget
// so the test asserts against the same arithmetic instead of a
// hard-coded duration.
func expectedRampFromCRsPerSec(totalCRs int, crsPerSec float64) time.Duration {
	return time.Duration(float64(totalCRs)/crsPerSec*float64(time.Second)) + time.Second
}

// TestResolveRampBudget covers the four clauses of M22's ramp-budget
// formula plus the override path.
func TestResolveRampBudget(t *testing.T) {
	cases := []struct {
		name            string
		profileOverride string
		clusterCount    int
		target          int
		durationSeconds int
		wantBudget      time.Duration
		wantSourceFrag  string
	}{
		{
			name:            "small profile hits the 15-min floor",
			clusterCount:    50,
			target:          1000,
			durationSeconds: 1800,
			wantBudget:      15 * time.Minute,
			wantSourceFrag:  "15-min floor",
		},
		{
			name:            "1M demand triggers the 750 CR/sec clause (~22 min)",
			clusterCount:    100,
			target:          10000,
			durationSeconds: 1800,
			// 1M / 750 ≈ 1333.33s, plus the helper's +1s rounding
			// fudge. Computed via expectedRampFromCRsPerSec so the
			// test reproduces resolveRampBudget's formula and fails
			// loudly if the constants drift.
			wantBudget:     expectedRampFromCRsPerSec(1_000_000, 750.0),
			wantSourceFrag: "750 CR/sec",
		},
		{
			name:            "long soak triggers durationSeconds × 0.5 (60 min)",
			clusterCount:    50,
			target:          1000,
			durationSeconds: 7200, // 2 hr soak → 1 hr ramp
			wantBudget:      60 * time.Minute,
			wantSourceFrag:  "0.5",
		},
		{
			name:            "explicit profile override wins everything",
			profileOverride: "5m",
			clusterCount:    100,
			target:          10000,
			durationSeconds: 7200,
			wantBudget:      5 * time.Minute,
			wantSourceFrag:  "profile.rampBudget",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var prof profileFile
			prof.RampBudget = tc.profileOverride
			prof.KWOK.ClusterCount = tc.clusterCount
			prof.LoadProfile.Target = tc.target
			prof.LoadProfile.DurationSeconds = tc.durationSeconds
			totalCRs := tc.clusterCount * tc.target
			got, source := resolveRampBudget(prof, totalCRs)
			if got != tc.wantBudget {
				t.Errorf("budget = %s, want %s", got, tc.wantBudget)
			}
			if !strings.Contains(source, tc.wantSourceFrag) {
				t.Errorf("source %q does not contain %q", source, tc.wantSourceFrag)
			}
		})
	}
}
