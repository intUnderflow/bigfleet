package occ

import "testing"

// ADR-0061 amendment: coveragePerMille is the worst-covered deficit
// dimension (0..1000), so a hole in one resource is not masked by surplus
// in another.
func TestCoveragePerMille(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		have []int64
		want []int64
		exp  int
	}{
		{"full", []int64{8000}, []int64{8000}, 1000},
		{"half", []int64{4000}, []int64{8000}, 500},
		{"worst-dim-governs", []int64{8000, 1000}, []int64{8000, 4000}, 250},
		{"no-supply", nil, []int64{8000}, 0},
		{"no-deficit", nil, nil, 1000},
		{"over-supply-capped", []int64{16000}, []int64{8000}, 1000},
	}
	for _, c := range cases {
		if got := coveragePerMille(c.have, c.want); got != c.exp {
			t.Errorf("%s: coveragePerMille = %d, want %d", c.name, got, c.exp)
		}
	}
}

// topSameCandidates ranks candidate domains best-coverage-first, drops
// empty buckets, and flags the satisfiable ones.
func TestTopSameCandidates(t *testing.T) {
	t.Parallel()
	deficit := []int64{8000}
	buckets := []SameBucket{
		{Value: "rack-1", Count: 1, Total: []int64{4000}}, // 50%
		{Value: "rack-2", Count: 1, Total: []int64{8000}}, // 100% satisfiable
		{Value: "rack-3", Count: 1, Total: []int64{6000}}, // 75%
		{Value: "empty", Count: 0, Total: []int64{8000}},  // skipped (no candidates)
	}
	got := topSameCandidates(buckets, deficit)
	if len(got) != 3 {
		t.Fatalf("candidates = %d, want 3 (the empty bucket is skipped)", len(got))
	}
	if got[0].Domain != "rack-2" || !got[0].Satisfiable || got[0].CoveragePerMille != 1000 {
		t.Errorf("best = %+v, want rack-2 / satisfiable / 1000", got[0])
	}
	if got[1].Domain != "rack-3" || got[1].Satisfiable || got[1].CoveragePerMille != 750 {
		t.Errorf("second = %+v, want rack-3 / unsatisfiable / 750", got[1])
	}
	if got[2].Domain != "rack-1" || got[2].Satisfiable {
		t.Errorf("third = %+v, want rack-1 / unsatisfiable", got[2])
	}
}
