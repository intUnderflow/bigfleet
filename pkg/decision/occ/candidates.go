package occ

import (
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Candidates is one bucket's worth of proposal-ready machines, plus
// the BucketKey the resulting Proposal must carry. Workers translate
// Candidates → Proposal by adding ObservedSeq, Precedence, Mode, and
// the Need reference.
//
// Machines is empty when no eligible candidates exist (the pool was
// exhausted, or no bucket satisfies the Same/Spread constraint). The
// caller treats an empty Candidates as "this Need cannot make
// further progress against this (state, profile) pool right now".
type Candidates struct {
	Machines []machine.ID
	Bucket   BucketKey
}

// FindBasic returns up to enough MatchProfile-passing, minUnit-
// fitting machines from pool to cover deficit. Walks pool.src in
// price/cost order in two passes:
//
//  1. First pass picks unclaimed machines only — the cheap path when
//     contention is low.
//
//  2. If deficit remains, a second pass picks displaceable claimed
//     machines (incumbent has strictly-lower precedence than prec).
//     Displacement at the broker on commit evicts the incumbent.
//
// Two-pass biases toward zero-cost wins: under low contention (most
// of the realistic catalog) no displacement happens; under high
// contention with priority asymmetry the proposer can still claim
// from lower-precedence holders. Avoids cascading displacement chains
// that would burn the retry budget in 1:1 demand-to-supply tests
// (e.g. N Needs and N machines where every Need wants exactly one).
//
// Workers run in parallel so there is no head-cursor amortisation —
// each worker picks independently from the shared pool.
func (p *Pool) FindBasic(state *SharedState, st machine.State, prec Precedence, deficit, minUnit []needs.ResourceQty) Candidates {
	if needs.IsZero(deficit) {
		return Candidates{}
	}
	bucket := BucketKey{State: st, ProfileFP: p.profile.Fingerprint()}
	out := make([]machine.ID, 0, 4)
	remaining := deficit

	// Pass 1: unclaimed machines only.
	for i := range p.src {
		m := &p.src[i]
		if state.IsClaimed(m.ID) {
			continue
		}
		if !matchProfile(p.profile, *m) {
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		if !needs.Covers(alloc, minUnit) {
			continue
		}
		out = append(out, m.ID)
		remaining = needs.SubResources(remaining, alloc)
		if needs.IsZero(remaining) {
			return Candidates{Machines: out, Bucket: bucket}
		}
	}

	// Pass 2: displaceable claimed machines (lower-precedence
	// incumbents).
	for i := range p.src {
		m := &p.src[i]
		if !state.IsClaimed(m.ID) {
			continue
		}
		if !state.DisplaceableBy(m.ID, prec) {
			continue
		}
		if !matchProfile(p.profile, *m) {
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		if !needs.Covers(alloc, minUnit) {
			continue
		}
		out = append(out, m.ID)
		remaining = needs.SubResources(remaining, alloc)
		if needs.IsZero(remaining) {
			break
		}
	}
	return Candidates{Machines: out, Bucket: bucket}
}

// FindSame returns candidates honouring a Profile's Same requirement:
// all returned machines share one value for sameKey.
//
// When domain is non-empty (ADR-0040 Addendum: the pre-pass chose the
// Need's domain once, jointly over creditable + acquirable supply),
// bucket scoring is skipped entirely: only machines whose Same-key
// value equals domain are considered — still matchProfile-filtered,
// claimed-filtered, and taken in the pool's head-sorted order.
// Re-picking the best Idle bucket here was the second half of the
// reclaim↔re-bootstrap oscillation: acquisition landed in a different
// domain than the credited one, Phase 1 assembled a cross-domain
// group, and Phase 3 (correctly strict) reclaimed half of it next
// cycle.
//
// When domain is empty (a Same Need with no bucket anywhere — no
// creditable and no acquirable supply), the original behaviour is the
// fallback: pick the best single-value bucket — atomic-satisfiable
// preferred, then cheapest head, then most-available — and take from
// it. (Successor of the retired pre-OCC phase1Allocator.takeCoLocated.)
// ADR-0040 mirrors this strict single-bucket semantics at the
// supply-crediting site: SeedConfiguredSupply chooses its bucket via
// ChooseSameBucket, which ranks on coverage rather than price because
// credited supply is already provisioned.
//
// Note: cross-bucket scoring re-walks the (claimed-filtered) bucket
// each call. The legacy allocator's coLocatedBuilt cache + per-bucket
// head cursors don't survive concurrency — every worker filters
// fresh via state.IsClaimed. For realistic-catalog scale (few-hundred-
// machine pools, ~3% Same-carrying Needs) this stays under the
// per-Need cost envelope.
func (p *Pool) FindSame(state *SharedState, st machine.State, prec Precedence, deficit, minUnit []needs.ResourceQty, sameKey, domain string) Candidates {
	if needs.IsZero(deficit) {
		return Candidates{}
	}

	if domain != "" {
		out := make([]machine.ID, 0, 4)
		remaining := deficit
		for i := range p.src {
			m := &p.src[i]
			if !state.DisplaceableBy(m.ID, prec) {
				continue
			}
			if !matchProfile(p.profile, *m) {
				continue
			}
			v, ok := lookupAttribute(sameKey, *m)
			if !ok || v != domain {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			if !needs.Covers(alloc, minUnit) {
				continue
			}
			out = append(out, m.ID)
			remaining = needs.SubResources(remaining, alloc)
			if needs.IsZero(remaining) {
				break
			}
		}
		if len(out) == 0 {
			return Candidates{}
		}
		return Candidates{
			Machines: out,
			Bucket: BucketKey{
				State:     st,
				ProfileFP: p.profile.Fingerprint(),
				SameKey:   sameKey,
				SameValue: domain,
			},
		}
	}

	type sameBucket struct {
		value    string
		machines []*machine.Machine
	}
	index := make(map[string]int)
	buckets := make([]sameBucket, 0)
	for i := range p.src {
		m := &p.src[i]
		if !matchProfile(p.profile, *m) {
			continue
		}
		v, ok := lookupAttribute(sameKey, *m)
		if !ok {
			continue
		}
		idx, exists := index[v]
		if !exists {
			idx = len(buckets)
			index[v] = idx
			buckets = append(buckets, sameBucket{value: v})
		}
		buckets[idx].machines = append(buckets[idx].machines, m)
	}

	bestIdx := -1
	bestAtomic := false
	bestHeadPrice := 0.0
	bestAvail := -1
	for i := range buckets {
		b := &buckets[i]
		avail := 0
		var capacity []needs.ResourceQty
		headPrice := 0.0
		headSet := false
		for _, m := range b.machines {
			if !state.DisplaceableBy(m.ID, prec) {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			if !needs.Covers(alloc, minUnit) {
				continue
			}
			if !headSet {
				headPrice = m.PricePerHour
				headSet = true
			}
			avail++
			capacity = needs.AddResources(capacity, alloc)
		}
		if avail == 0 {
			continue
		}
		atomic := needs.Covers(capacity, deficit)
		better := false
		switch {
		case bestIdx < 0:
			better = true
		case atomic && !bestAtomic:
			better = true
		case atomic == bestAtomic:
			bestKey := buckets[bestIdx].value
			if atomic {
				switch {
				case headPrice < bestHeadPrice:
					better = true
				case headPrice == bestHeadPrice && b.value < bestKey:
					better = true
				}
			} else {
				switch {
				case avail > bestAvail:
					better = true
				case avail == bestAvail && headPrice < bestHeadPrice:
					better = true
				case avail == bestAvail && headPrice == bestHeadPrice && b.value < bestKey:
					better = true
				}
			}
		}
		if better {
			bestIdx = i
			bestAtomic = atomic
			bestHeadPrice = headPrice
			bestAvail = avail
		}
	}
	if bestIdx < 0 {
		return Candidates{}
	}

	best := &buckets[bestIdx]
	out := make([]machine.ID, 0, 4)
	remaining := deficit
	for _, m := range best.machines {
		if !state.DisplaceableBy(m.ID, prec) {
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		if !needs.Covers(alloc, minUnit) {
			continue
		}
		out = append(out, m.ID)
		remaining = needs.SubResources(remaining, alloc)
		if needs.IsZero(remaining) {
			break
		}
	}
	return Candidates{
		Machines: out,
		Bucket: BucketKey{
			State:     st,
			ProfileFP: p.profile.Fingerprint(),
			SameKey:   sameKey,
			SameValue: best.value,
		},
	}
}

// FindSpread returns candidates honouring a DoNotSchedule
// TopologySpread: bucket-pick counts never exceed (current min +
// maxSkew). Picks cheapest-head among eligible buckets at each step.
// Picks cheapest-head among topology-spread-eligible buckets at each step.
//
// The bucket key carries the topology key but an empty SameValue —
// Spread proposals touch machines across multiple topology domains,
// so per-value bucketing would mean multiple proposals; instead the
// broker conflict-grain is the (state, profile, topology-key)
// triple. Workers competing for Spread on the same key share one
// CAS line.
func (p *Pool) FindSpread(state *SharedState, st machine.State, prec Precedence, deficit, minUnit []needs.ResourceQty, topoKey string, maxSkew int32) Candidates {
	if needs.IsZero(deficit) {
		return Candidates{}
	}
	skew := int(maxSkew)
	if skew < 1 {
		skew = 1
	}

	type spreadBucket struct {
		machines []*machine.Machine
		head     int
	}
	buckets := make(map[string]*spreadBucket)
	keys := make([]string, 0)
	for i := range p.src {
		m := &p.src[i]
		if !state.DisplaceableBy(m.ID, prec) {
			continue
		}
		if !matchProfile(p.profile, *m) {
			continue
		}
		if !needs.Covers(needs.ResourceQtysFromMap(m.EffectiveAllocatable()), minUnit) {
			continue
		}
		v, ok := lookupAttribute(topoKey, *m)
		if !ok {
			continue
		}
		b, exists := buckets[v]
		if !exists {
			b = &spreadBucket{}
			buckets[v] = b
			keys = append(keys, v)
		}
		b.machines = append(b.machines, m)
	}
	if len(keys) == 0 {
		return Candidates{}
	}

	counts := make(map[string]int, len(keys))
	remaining := deficit
	out := make([]machine.ID, 0, 4)
	for !needs.IsZero(remaining) {
		minCount := -1
		for _, k := range keys {
			c := counts[k]
			if minCount == -1 || c < minCount {
				minCount = c
			}
		}

		var bestKey string
		var bestPrice float64
		bestSet := false
		for _, k := range keys {
			b := buckets[k]
			if b.head >= len(b.machines) {
				continue
			}
			if counts[k] > minCount+skew-1 {
				continue
			}
			head := b.machines[b.head]
			if !bestSet ||
				head.PricePerHour < bestPrice ||
				(head.PricePerHour == bestPrice && k < bestKey) {
				bestSet = true
				bestKey = k
				bestPrice = head.PricePerHour
			}
		}
		if !bestSet {
			break
		}

		b := buckets[bestKey]
		m := b.machines[b.head]
		b.head++
		out = append(out, m.ID)
		counts[bestKey]++
		remaining = needs.SubResources(remaining, needs.ResourceQtysFromMap(m.EffectiveAllocatable()))
	}
	return Candidates{
		Machines: out,
		Bucket: BucketKey{
			State:     st,
			ProfileFP: p.profile.Fingerprint(),
			SameKey:   topoKey,
		},
	}
}

// SameRequirementKey returns the Same operator's key on the profile,
// if any. Used by callers to route between FindBasic and FindSame
// before constructing a proposal, by the ADR-0040 domain-aware
// crediting sites (seedSameProfile here, claimMatchingSame in
// pkg/decision), and per Need per cycle by decision.NormalizeDemand
// (ADR-0041) — hence the RO accessor, so the routing check costs no
// allocation on the hot path.
//
// A Profile carries at most one Same requirement in v1 — the operator
// takes the first co-location term during roll-up (ADR-0024) — so
// returning the first match is exact, not a heuristic.
func SameRequirementKey(p needs.Profile) (string, bool) {
	for _, r := range p.RequirementsRO() {
		if r.Operator == needs.OperatorSame {
			return r.Key, true
		}
	}
	return "", false
}

// StrictSpread returns the topology key + max skew of the first
// DoNotSchedule TopologySpread on the profile, if any. Callers that
// also see a Same key route to FindSame instead (Same is the stronger
// constraint).
func StrictSpread(p needs.Profile) (string, int32, bool) {
	for _, s := range p.Spread() {
		if s.WhenUnsatisfiable == needs.WhenUnsatisfiableDoNotSchedule {
			return s.TopologyKey, s.MaxSkew, true
		}
	}
	return "", 0, false
}
