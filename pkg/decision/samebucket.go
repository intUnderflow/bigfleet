package decision

import (
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// sameBucket aggregates one Same-domain's candidate supply for the
// ADR-0040 single-best-bucket choice: the domain value, the candidate
// machine count, and Σ EffectiveAllocatable across those candidates.
type sameBucket struct {
	value string
	count int
	total []needs.ResourceQty
}

// chooseSameBucket mirrors occ.ChooseSameBucket — the ADR-0040
// bucket-choice rule (satisfiable preferred; smallest satisfiable
// total; else most-covering; tiebreak larger count, then smallest
// value) is documented there. Duplicated because occ must not import
// decision (decision.Phase1 calls occ.RunCycle); the two
// implementations must stay aligned — TestChooseSameBucketParity
// asserts they pick the same bucket over a shared synthetic machine
// set.
func chooseSameBucket(buckets []sameBucket, deficit []needs.ResourceQty) int {
	best := -1
	bestSat := false
	bestScore := 0.0
	for i := range buckets {
		b := &buckets[i]
		if b.count == 0 {
			continue
		}
		sat := needs.Covers(b.total, deficit)
		score := sameBucketScore(b.total, deficit, !sat)
		better := false
		switch {
		case best < 0:
			better = true
		case sat != bestSat:
			better = sat
		case score != bestScore:
			// Satisfiable pair: smaller total (less over-commitment)
			// wins. Unsatisfiable pair: larger coverage wins.
			if sat {
				better = score < bestScore
			} else {
				better = score > bestScore
			}
		case b.count != buckets[best].count:
			better = b.count > buckets[best].count
		default:
			better = b.value < buckets[best].value
		}
		if better {
			best = i
			bestSat = sat
			bestScore = score
		}
	}
	return best
}

// sameBucketScore mirrors occ's sameBucketScore — see the rationale
// there (per-dimension total/deficit ratios; capped = coverage for
// unsatisfiable buckets, uncapped = over-commitment for satisfiable).
func sameBucketScore(total, deficit []needs.ResourceQty, capped bool) float64 {
	have := make(map[string]float64, len(total))
	for _, r := range total {
		q, _ := resource.ParseQuantity(r.Quantity)
		have[r.Name] = q.AsApproximateFloat64()
	}
	score := 0.0
	for _, d := range deficit {
		dq, _ := resource.ParseQuantity(d.Quantity)
		want := dq.AsApproximateFloat64()
		if want <= 0 {
			continue
		}
		ratio := have[d.Name] / want
		if capped && ratio > 1 {
			ratio = 1
		}
		score += ratio
	}
	return score
}
