package occ

// SameBucket aggregates one Same-domain's candidate supply for the
// ADR-0040 single-best-bucket choice: the domain value, the candidate
// machine count, and Σ EffectiveAllocatable across those candidates as
// a SameSupplyIndex dimension-interned milli-unit vector (ParseVec).
// Integer vectors keep ChooseSameBucket free of quantity parsing — the
// string-vector version of this machinery re-parsed resource.Quantity
// per bucket per Need and starved the shard (see SameSupplyIndex).
//
// CreditableCount is the number of candidates contributed by the
// creditable half of the joint fold — the Need's cluster's Configured/
// Configuring machines (filled by occ.seedSameProfile and decision's
// claimMatchingSame). The acquirable half (AcquirableTotals, and the
// fold-in at both call sites) never increments it, so a non-zero value
// means "this domain is already serving the Need's cluster" — the
// ADR-0041 rider-3 prefer-creditable signal.
type SameBucket struct {
	Value           string
	Count           int
	CreditableCount int
	Total           []int64
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
// Speculative, via SameSupplyIndex). The choice is made once per Need
// per cycle, and both phases rank with this same function on vectors
// from the same index, so they agree by construction (Phase 3's
// claimMatching imports it directly; the former pkg/decision twin is
// gone).
//
// The rule — deterministic and cycle-stable, a strict total order over
// distinct domain values:
//
//  1. Prefer a satisfiable bucket: Total covers the full remaining
//     deficit.
//  2. Among satisfiable buckets, one with creditable supply
//     (CreditableCount > 0) beats an acquirable-only one.
//  3. Among satisfiable buckets, the smallest Total — least
//     over-commitment, mirroring the claim loop's stop-when-covered.
//  4. If none is satisfiable, the most-covering Total.
//  5. Tiebreak: larger Count, then lexicographically smallest Value.
//
// Rule 2 is the ADR-0041 rider-3 refinement, deliberately stronger
// than the ADR's stated last-place tie-break: it sits between the
// satisfiable test and the score comparison. Sticky-domain semantics —
// a Need's currently-serving domain must not lose to a fresh
// acquirable-only domain that merely scores smaller (rule 3) or sorts
// lower (rule 5) and relocate a healthy gang. Staying put costs
// nothing: excess machines WITHIN the serving domain are still
// reclaimed individually by the claim loop's stop-when-covered. The
// preference is confined to the satisfiable regime on purpose — among
// unsatisfiable buckets, "staying put costs nothing" no longer holds
// (the Need is genuinely better served wherever coverage is larger),
// so the most-covering rule keeps the ADR-0040 Addendum's concentrate-
// then-park behaviour (TestIntegration_SameDomain_NoOscillation pins
// it: a 3-Idle domain must beat a 2-Configured one for a 5-machine
// Need).
//
// This is the crediting mirror of FindSame's acquisition scoring
// (atomic-satisfiable preferred); it ranks on coverage rather than
// price because credited supply is already provisioned.
func ChooseSameBucket(buckets []SameBucket, deficit []int64) int {
	best := -1
	bestSat := false
	bestScore := 0.0
	for i := range buckets {
		b := &buckets[i]
		if b.Count == 0 {
			continue
		}
		sat := VecCovers(b.Total, deficit)
		score := sameBucketScore(b.Total, deficit, !sat)
		better := false
		switch {
		case best < 0:
			better = true
		case sat != bestSat:
			better = sat
		case sat && (b.CreditableCount > 0) != (buckets[best].CreditableCount > 0):
			// ADR-0041 rider 3 (see the rule's doc comment): both
			// satisfiable — the domain already serving the cluster wins
			// before any size comparison.
			better = b.CreditableCount > 0
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
func sameBucketScore(total, deficit []int64, capped bool) float64 {
	score := 0.0
	for i, want := range deficit {
		if want <= 0 {
			continue
		}
		have := int64(0)
		if i < len(total) {
			have = total[i]
		}
		ratio := float64(have) / float64(want)
		if capped && ratio > 1 {
			ratio = 1
		}
		score += ratio
	}
	return score
}
