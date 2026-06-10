package decision

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// synthSameMachine is one machine of a synthetic Same-domain pool: the
// domain value it carries and its EffectiveAllocatable vector.
type synthSameMachine struct {
	domain string
	alloc  []needs.ResourceQty
}

// foldSameMachines builds the per-domain bucket aggregates both
// choosers rank, from one shared machine list — the same fold the two
// crediting sites perform inline.
func foldSameMachines(ms []synthSameMachine) ([]sameBucket, []occ.SameBucket) {
	index := map[string]int{}
	var dec []sameBucket
	var oc []occ.SameBucket
	for _, m := range ms {
		i, ok := index[m.domain]
		if !ok {
			i = len(dec)
			index[m.domain] = i
			dec = append(dec, sameBucket{value: m.domain})
			oc = append(oc, occ.SameBucket{Value: m.domain})
		}
		dec[i].count++
		dec[i].total = needs.AddResources(dec[i].total, m.alloc)
		oc[i].Count++
		oc[i].Total = needs.AddResources(oc[i].Total, m.alloc)
	}
	return dec, oc
}

func cpus(n string) []needs.ResourceQty {
	return []needs.ResourceQty{{Name: "cpu", Quantity: n}}
}

func rackMachines(domain string, n int, alloc []needs.ResourceQty) []synthSameMachine {
	out := make([]synthSameMachine, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, synthSameMachine{domain: domain, alloc: alloc})
	}
	return out
}

// TestChooseSameBucketParity feeds the decision- and occ-side ADR-0040
// bucket choosers the same synthetic machine sets and asserts they
// pick the same bucket. The helper is duplicated across the two
// packages (occ must not import decision); this test is the guard that
// keeps the duplication aligned, and it pins the documented rule:
// satisfiable preferred, smallest satisfiable total, else most-
// covering, tiebreak larger count then smallest value.
func TestChooseSameBucketParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		machines []synthSameMachine
		deficit  []needs.ResourceQty
		want     string // expected domain value; "" = no bucket (-1)
	}{
		{
			name: "satisfiable beats bigger unsatisfiable",
			machines: append(
				rackMachines("rack-a", 1, cpus("4")),
				rackMachines("rack-b", 2, cpus("4"))...),
			deficit: cpus("8"),
			want:    "rack-b",
		},
		{
			name: "smallest satisfiable total wins",
			machines: append(
				rackMachines("rack-a", 4, cpus("4")),     // total 16
				rackMachines("rack-b", 3, cpus("4"))...), // total 12
			deficit: cpus("10"),
			want:    "rack-b",
		},
		{
			name: "none satisfiable: most covering wins",
			machines: append(
				rackMachines("rack-a", 1, cpus("4")),
				rackMachines("rack-b", 3, cpus("4"))...),
			deficit: cpus("20"),
			want:    "rack-b",
		},
		{
			name: "score tie: larger machine count wins",
			machines: append(
				rackMachines("rack-a", 1, cpus("8")),
				rackMachines("rack-b", 2, cpus("4"))...), // both total 8
			deficit: cpus("6"),
			want:    "rack-b",
		},
		{
			name: "full tie: lexicographically smallest value",
			machines: append(
				rackMachines("rack-b", 2, cpus("4")),
				rackMachines("rack-a", 2, cpus("4"))...),
			deficit: cpus("8"),
			want:    "rack-a",
		},
		{
			name: "multi-dimension coverage is capped per dimension",
			machines: append(
				// rack-a overflows cpu 4× but covers no memory (capped
				// coverage 1.0); rack-b covers cpu fully and half the
				// memory (coverage 1.5). Uncapped, rack-a's cpu overflow
				// would mask its memory hole and win.
				rackMachines("rack-a", 2, []needs.ResourceQty{{Name: "cpu", Quantity: "32"}}),
				rackMachines("rack-b", 2, []needs.ResourceQty{
					{Name: "cpu", Quantity: "8"}, {Name: "memory", Quantity: "16Gi"},
				})...),
			deficit: []needs.ResourceQty{
				{Name: "cpu", Quantity: "16"}, {Name: "memory", Quantity: "64Gi"},
			},
			want: "rack-b",
		},
		{
			name:     "no candidates",
			machines: nil,
			deficit:  cpus("8"),
			want:     "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Both input orders: the rule is a strict total order over
			// domain values, so the pick must be order-independent.
			orders := [][]synthSameMachine{tc.machines, reversed(tc.machines)}
			for _, ms := range orders {
				decBuckets, occBuckets := foldSameMachines(ms)
				decIdx := chooseSameBucket(decBuckets, tc.deficit)
				occIdx := occ.ChooseSameBucket(occBuckets, tc.deficit)

				decVal, occVal := "", ""
				if decIdx >= 0 {
					decVal = decBuckets[decIdx].value
				}
				if occIdx >= 0 {
					occVal = occBuckets[occIdx].Value
				}
				if decVal != occVal {
					t.Fatalf("parity broken: decision chose %q, occ chose %q", decVal, occVal)
				}
				if decVal != tc.want {
					t.Errorf("chose %q, want %q", decVal, tc.want)
				}
			}
		})
	}
}

func reversed(in []synthSameMachine) []synthSameMachine {
	out := make([]synthSameMachine, len(in))
	for i, m := range in {
		out[len(in)-1-i] = m
	}
	return out
}
