package occ

// SameBucket aggregates one Same-domain's candidate supply for the
// ADR-0040 single-best-bucket choice: the domain value, the candidate
// machine count, and Σ EffectiveAllocatable across those candidates as
// a SameSupplyIndex dimension-interned milli-unit vector (ParseVec).
// Integer vectors keep ChooseSameBucket free of quantity parsing — the
// string-vector version of this machinery re-parsed resource.Quantity
// per bucket per Need and starved the shard (see SameSupplyIndex).
//
// CreditableCount / CreditableTotal are the contribution of the
// creditable half of the joint fold — the Need's cluster's Configured/
// Configuring machines (filled by occ.seedSameProfile, since ADR-0045
// the only crediting site). The acquirable half (AcquirableTotals)
// never touches them, so they mean "this domain is already serving
// the Need's cluster, this much". CreditableTotal is what the M77a
// bound-coverage rule ranks on: the machines bound to the cluster ARE
// the fulfillment (ADR-0045), so the domain choice must follow them.
// CreditableOwnTotal (ADR-0051 / M77g) is the sub-total of
// CreditableTotal contributed by machines bound to *this gang* — the
// creditable machines whose (Profile.Fingerprint, AssignedGroup) match
// the Need's (fingerprint, Group). CreditableTotal is cluster-granular
// (any same-class gang's machines count), so it cannot tell the
// incumbent domain from a fresh domain holding equal *unrelated*
// supply; CreditableOwnTotal can. ChooseSameBucket breaks a
// capped-creditable-coverage tie on it, pinning a served gang to the
// domain its own bound machines occupy — stable through the bootstrap
// dwell that is the #64 perturbation.
type SameBucket struct {
	Value              string
	Count              int
	CreditableCount    int
	Total              []int64
	CreditableTotal    []int64
	CreditableOwnTotal []int64
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
//  2. Among satisfiable buckets, the greatest CREDITABLE coverage of
//     the deficit (capped per dimension): the domain where the most of
//     the Need's bound supply lives wins. ADR-0045: a bound machine IS
//     the fulfillment, so the domain choice follows the bindings —
//     never the other way around. ADR-0051 (M77g) refines the tie WITHIN
//     this rule: when two satisfiable domains tie at the capped
//     creditable ceiling, the one holding more of THIS gang's own bound
//     machines (capped gang-own coverage) wins — because creditable is
//     cluster-granular and so ties a domain holding the gang's own
//     machines with one holding an equal count of an unrelated same-class
//     gang's. Without it the tie fell through to slack (rule 3) and the
//     gang flipped domains every time the acquirable snapshot moved (the
//     bootstrap-dwell oscillation).
//  3. Among satisfiable buckets of equal creditable AND gang-own
//     coverage, the smallest Total — least over-commitment, mirroring
//     the claim loop's stop-when-covered.
//  4. If none is satisfiable, the most-covering Total.
//  5. Among unsatisfiable buckets of EQUAL coverage, the greatest
//     creditable coverage (ADR-0042: the incumbent domain, where the
//     Need's concentrated partial assembly lives).
//  6. Tiebreak: larger Count, then lexicographically smallest Value.
//
// Rule 2's history is the M77a gang oscillation (the dev-50 V2 gate's
// day-one catch). Its two predecessors keyed on creditable PRESENCE
// only: a satisfiable domain with any creditable supply beat
// acquirable-only domains (ADR-0041 rider 3), and the rest fell to
// rule 3's smallest joint total. On a fleet whose cluster supply is
// spread across domains (zone-round-robin seeds, mixed rack/zone gangs
// matching the same machines), EVERY domain has some creditable
// supply, presence never discriminates, and rule 3 decided — ranking
// on acquirable slack, blind to where the gang's bound machines live.
// Any inventory mutation re-ranked the argmin, the gang flipped
// domains, its bound machines became off-domain strays Phase 3
// faithfully reclaimed, and the reclaim mutated the next domain's
// totals: the Bootstrap≈Reclaim lockstep ping-pong at static demand.
// Ranking on creditable COVERAGE pins a served gang to its machines —
// a fully-bound domain (coverage 1.0) cannot lose to any domain
// holding less of the gang's supply, whatever the slack around it.
// Staying put costs nothing: excess machines WITHIN the domain are
// still released by the claim loop's stop-when-covered. Coverage is
// deliberately capped at the deficit and nothing sharper: an
// investigation-era "tightest assembly" key (smallest creditable
// total among full-coverage buckets, meant to anchor interchangeable
// same-class gangs to their own assemblies) re-introduced a sustained
// two-cycle steal loop — a 3-machine rack gang ranked a rack holding
// exactly 3 of another class's machines "tighter" than the rack
// holding its own 3 plus a neighbour gang's 6 — so it was measured
// and removed (sim/gang_fixedpoint_test.go's base-arrival shape).
//
// Rule 5 is ADR-0042's unsatisfiable-regime counterpart, the same
// coverage refinement applied at equal JOINT coverage: switching
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
	bestCred := 0.0
	bestCredOwn := 0.0
	for i := range buckets {
		b := &buckets[i]
		if b.Count == 0 {
			continue
		}
		sat := VecCovers(b.Total, deficit)
		score := sameBucketScore(b.Total, deficit, !sat)
		// Capped like unsatisfiable coverage: bound supply beyond the
		// deficit buys nothing (stop-when-covered), so a domain holding
		// the whole gang ties with one holding the gang plus strays.
		cred := sameBucketScore(b.CreditableTotal, deficit, true)
		// Rule 2b (ADR-0051 / M77g): gang-own coverage, capped the same
		// way. Distinguishes the incumbent domain — where THIS gang's
		// bound machines live — from a fresh domain holding equal
		// *unrelated* same-class supply (which ties cred but contributes
		// nothing here).
		credOwn := sameBucketScore(b.CreditableOwnTotal, deficit, true)
		better := false
		switch {
		case best < 0:
			better = true
		case sat != bestSat:
			better = sat
		case sat && cred != bestCred:
			// Rule 2 (ADR-0045 / M77a): both satisfiable — the domain
			// holding more of the Need's bound supply wins before any
			// size comparison.
			better = cred > bestCred
		case sat && credOwn != bestCredOwn:
			// Rule 2b (ADR-0051 / M77g): both satisfiable and tied on
			// cluster-granular creditable coverage — the domain holding
			// more of THIS gang's own bound machines wins, ABOVE the
			// joint-Total/acquirable-slack tiebreak below. This is the tie
			// the cluster-granular rule 2 could not break: two domains each
			// fully cover the gang, one with the gang's own machines, one
			// with an equal count of a neighbour gang's. Ranking on slack
			// (rule 3) flipped the gang off its own machines every time the
			// acquirable snapshot moved — the bootstrap-dwell oscillation.
			better = credOwn > bestCredOwn
		case score != bestScore:
			// Satisfiable pair: smaller total (less over-commitment)
			// wins. Unsatisfiable pair: larger coverage wins.
			if sat {
				better = score < bestScore
			} else {
				better = score > bestScore
			}
		case cred != bestCred:
			// Rule 5 (ADR-0042): only reachable for unsatisfiable pairs
			// of EQUAL joint coverage (satisfiable pairs with differing
			// creditable coverage were caught by rule 2 above) — the
			// domain with more of the partial assembly wins; switching
			// needs strictly greater joint coverage.
			better = cred > bestCred
		case b.Count != buckets[best].Count:
			better = b.Count > buckets[best].Count
		default:
			better = b.Value < buckets[best].Value
		}
		if better {
			best = i
			bestSat = sat
			bestScore = score
			bestCred = cred
			bestCredOwn = credOwn
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
