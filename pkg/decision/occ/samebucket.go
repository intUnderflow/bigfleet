package occ

import (
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// SameBucket aggregates one Same-domain's candidate supply for the
// ADR-0040 single-best-bucket choice: the domain value, the candidate
// machine count, and Σ EffectiveAllocatable across those candidates.
type SameBucket struct {
	Value string
	Count int
	Total []needs.ResourceQty
}

// ChooseSameBucket returns the index of the single bucket a
// Same-Profile's supply-crediting walk may credit/claim within, or -1
// when no bucket has candidates. ADR-0040: a co-located Need is served
// by one domain or not at all, so crediting must be domain-aware like
// acquisition (FindSame) instead of walking machines linearly across
// domains.
//
// ADR-0040 Addendum: callers feed it JOINT bucket totals — each
// domain's creditable supply (the Need's cluster's Configured +
// Configuring) plus its acquirable supply (shard-wide unclaimed Idle +
// Speculative, via SameSupplyIndex). The rule below is unchanged;
// only the totals it ranks changed. Choosing over creditable-only
// totals here while acquisition re-picked the best Idle bucket chose
// the domain twice per cycle and oscillated.
//
// The rule — deterministic and cycle-stable, a strict total order over
// distinct domain values:
//
//  1. Prefer a satisfiable bucket: Total covers the full remaining
//     deficit.
//  2. Among satisfiable buckets, the smallest Total — least
//     over-commitment, mirroring the claim loop's stop-when-covered.
//  3. If none is satisfiable, the most-covering Total.
//  4. Tiebreak: larger Count, then lexicographically smallest Value.
//
// This is the crediting mirror of FindSame's acquisition scoring
// (atomic-satisfiable preferred); it ranks on coverage rather than
// price because credited supply is already provisioned. pkg/decision's
// chooseSameBucket (Phase 3's claimMatching) duplicates this helper —
// occ must not import decision — and TestChooseSameBucketParity keeps
// the two aligned.
func ChooseSameBucket(buckets []SameBucket, deficit []needs.ResourceQty) int {
	best := -1
	bestSat := false
	bestScore := 0.0
	for i := range buckets {
		b := &buckets[i]
		if b.Count == 0 {
			continue
		}
		sat := needs.Covers(b.Total, deficit)
		score := sameBucketScore(b.Total, deficit, !sat)
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
		case b.Count != buckets[best].Count:
			better = b.Count > buckets[best].Count
		default:
			better = b.Value < buckets[best].Value
		}
		if better {
			best = i
			bestSat = sat
			bestScore = score
		}
	}
	return best
}

// sameBucketScore reduces a bucket total to a scalar against deficit:
// the sum over deficit's positive dimensions of the total/deficit
// per-dimension ratio. Ratios — not raw quantities — keep cpu, memory
// and extended dimensions commensurable. With capped=true each ratio
// saturates at 1, yielding the coverage score for unsatisfiable
// buckets (overflowing one dimension must not mask a hole in another);
// uncapped it is the over-commitment score minimised among satisfiable
// buckets.
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
