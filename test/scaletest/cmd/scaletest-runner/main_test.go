package main

import (
	"net/url"
	"strings"
	"testing"
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
