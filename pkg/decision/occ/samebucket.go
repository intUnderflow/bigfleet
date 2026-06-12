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
// Configuring machines (filled by occ.seedSameProfile, since ADR-0045
// the only crediting site). The acquirable half (AcquirableTotals)
// never increments it, so a non-zero value means "this domain is
// already serving the Need's cluster" — the ADR-0041 rider-3
// prefer-creditable signal.
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
// per cycle, in seedSameProfile — since ADR-0045 the single choosing
// site: Phase 3 keeps whatever the seed's claims kept, so the phases
// agree by construction rather than by mirroring.
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
//  5. Among unsatisfiable buckets of EQUAL coverage, one with
//     creditable supply wins (ADR-0042: the incumbent domain, where
//     the Need's concentrated partial assembly lives).
//  6. Tiebreak: larger Count, then lexicographically smallest Value.
//
// Rule 2 is the ADR-0041 rider-3 refinement, deliberately stronger
// than the ADR's stated last-place tie-break: it sits between the
// satisfiable test and the score comparison. Sticky-domain semantics —
// a Need's currently-serving domain must not lose to a fresh
// acquirable-only domain that merely scores smaller (rule 3) or sorts
// lower (rule 6) and relocate a healthy gang. Staying put costs
// nothing: excess machines WITHIN the serving domain are still
// reclaimed individually by the claim loop's stop-when-covered.
//
// Rule 5 is ADR-0042's unsatisfiable-regime counterpart: switching
// domains is reserved for STRICTLY greater coverage. Most-covering
// (rule 4) keeps the ADR-0040 Addendum's concentrate-then-park
// behaviour — a 3-Idle domain still beats a 2-Configured one for a
// 5-machine Need (TestIntegration_SameDomain_NoOscillation) — but a
// structurally-unsatisfiable gang facing dozens of identical-total
// domains no longer flip-flops between them on count/value noise,
// abandoning its partial assembly for Phase 3 to reclaim each cycle
// (the bigfleet-uber #56 ~27/sec churn anatomy). Pinned to the
// incumbent, in-domain acquirables exhaust, acquisition reaches zero,
// and the Need ages quietly in the shortfall buffer — parking without
// any suppression state.
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
		case (b.CreditableCount > 0) != (buckets[best].CreditableCount > 0):
			// ADR-0042 (rule 5): only reachable for unsatisfiable pairs
			// (satisfiable pairs with differing creditable-presence were
			// caught by rider 3 above) of EQUAL coverage — the incumbent
			// domain wins; switching needs strictly greater coverage.
			better = b.CreditableCount > 0
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
