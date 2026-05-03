package decision

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// phase1Allocator coordinates Phase 1's per-Need consumption against a
// shared inventory snapshot. It maintains:
//
//   - One pool per distinct (state, Profile fingerprint), built lazily
//     on first use.
//   - A single global "claimed" set so a machine matching multiple
//     Profiles is only ever consumed once per cycle.
//
// Pool layout (M11.20):
//
//   - Single-instance-type pools hold a *reference* to the snapshot's
//     pre-sorted (price, ID) bucket. No copy, no sort, no MatchProfile
//     filter at build. The MatchProfile and claimed-set checks happen
//     lazily at take(), advancing a head cursor through the bucket. Build
//     is O(1); take() is O(consumed + cross-pool claims and non-matches
//     skipped at the head).
//   - Multi-instance-type pools concatenate per-type pre-sorted slices
//     into an owned, freshly-allocated slice and re-sort by EffectiveCost
//     (which is what cross-type ordering needs because the per-type
//     interruption_probability differs). Build is O(merged + merged log
//     merged). The take() path is identical.
//
// Phase 1's outer loop walks Needs in priority order; the allocator
// guarantees high-priority Needs see fresh inventory and low-priority
// Needs only see what's left over. Same Phase 1 invariant as M11.16's
// allocator — what changed is only how the per-pool slice is built.
type phase1Allocator struct {
	snap    *inventory.Snapshot
	pools   map[string]*phase1Pool
	claimed map[machine.ID]struct{}
}

// phase1Pool holds the candidate slice for one (state, fingerprint).
// `src` is read-only — for single-type pools it points at the snapshot's
// shared bucket; for multi-type pools it's a freshly-allocated merged
// slice. take() never mutates src; it just advances head and applies
// the lazy MatchProfile + claimed filter.
type phase1Pool struct {
	src     []machine.Machine
	profile needs.Profile
	head    int
}

func newPhase1Allocator(snap *inventory.Snapshot) *phase1Allocator {
	return &phase1Allocator{
		snap:    snap,
		pools:   make(map[string]*phase1Pool),
		claimed: make(map[machine.ID]struct{}),
	}
}

// take returns up to n unclaimed, MatchProfile-passing machines from
// the per-(state, fingerprint) pool. It claims them as it goes.
//
// When the profile carries a Same requirement (paper §8 co-location
// signal), routing redirects through takeCoLocated which enforces
// "all returned machines share the same value for the Same key."
func (a *phase1Allocator) take(
	state machine.State,
	profile needs.Profile,
	n int,
) []machine.Machine {
	if n <= 0 {
		return nil
	}
	if key, ok := sameRequirementKey(profile); ok {
		return a.takeCoLocated(state, profile, n, key)
	}
	pool := a.poolFor(state, profile)
	if pool == nil {
		return nil
	}
	taken := make([]machine.Machine, 0, n)
	for pool.head < len(pool.src) && len(taken) < n {
		m := pool.src[pool.head]
		pool.head++
		if _, claimed := a.claimed[m.ID]; claimed {
			continue
		}
		if !MatchProfile(pool.profile, m) {
			continue
		}
		taken = append(taken, m)
		a.claimed[m.ID] = struct{}{}
	}
	return taken
}

// takeCoLocated honours a Profile's Same requirement (paper §8): all
// returned machines share the same value for the Same key. Picks the
// best single-value bucket: atomic-satisfiable (≥n machines) with the
// cheapest head wins; falls back to the largest bucket for a partial
// fill (the residual becomes a shortfall via the caller's deficit
// arithmetic).
//
// Does NOT advance pool.head — bucketing implies we walk the whole
// pool and skip the head cursor convention. Subsequent regular take()
// calls on the same pool still behave normally; the global claimed set
// catches anything we consumed.
func (a *phase1Allocator) takeCoLocated(state machine.State, profile needs.Profile, n int, sameKey string) []machine.Machine {
	pool := a.poolFor(state, profile)
	if pool == nil {
		return nil
	}

	// Bucket unclaimed, MatchProfile-passing candidates by the Same
	// key's value. pool.src is pre-sorted by (price, id) so each
	// bucket inherits that order.
	buckets := make(map[string][]machine.Machine)
	keys := make([]string, 0)
	for _, m := range pool.src {
		if _, claimed := a.claimed[m.ID]; claimed {
			continue
		}
		if !MatchProfile(pool.profile, m) {
			continue
		}
		v, ok := lookupAttribute(sameKey, m)
		if !ok {
			continue
		}
		if _, exists := buckets[v]; !exists {
			keys = append(keys, v)
		}
		buckets[v] = append(buckets[v], m)
	}
	if len(keys) == 0 {
		return nil
	}

	// Pick the best bucket.
	//   1. Atomic-satisfiable (≥ n) preferred.
	//   2. Within atomic-satisfiable: cheapest head price wins; tie
	//      breaks by smallest size (tighter packing) then by key.
	//   3. If none atomic: pick the largest bucket (best partial fill);
	//      tie breaks by cheapest head price then key.
	var bestKey string
	var best []machine.Machine
	for _, k := range keys {
		b := buckets[k]
		if best == nil {
			best, bestKey = b, k
			continue
		}
		bestAtomic := len(best) >= n
		bAtomic := len(b) >= n
		switch {
		case bAtomic && !bestAtomic:
			best, bestKey = b, k
		case bAtomic == bestAtomic:
			better := false
			if bAtomic {
				// Both atomic: cheapest head, then smaller, then key.
				switch {
				case b[0].PricePerHour < best[0].PricePerHour:
					better = true
				case b[0].PricePerHour == best[0].PricePerHour && len(b) < len(best):
					better = true
				case b[0].PricePerHour == best[0].PricePerHour && len(b) == len(best) && k < bestKey:
					better = true
				}
			} else {
				// Both partial: largest, then cheapest head, then key.
				switch {
				case len(b) > len(best):
					better = true
				case len(b) == len(best) && b[0].PricePerHour < best[0].PricePerHour:
					better = true
				case len(b) == len(best) && b[0].PricePerHour == best[0].PricePerHour && k < bestKey:
					better = true
				}
			}
			if better {
				best, bestKey = b, k
			}
		}
	}

	take := n
	if take > len(best) {
		take = len(best)
	}
	out := make([]machine.Machine, take)
	for i := 0; i < take; i++ {
		out[i] = best[i]
		a.claimed[best[i].ID] = struct{}{}
	}
	return out
}

// sameRequirementKey returns the key of the first Same requirement on
// the profile, if any. MVP: supports a single Same key; profiles with
// multiple Same requirements would need the allocator to honour all of
// them simultaneously, which is a follow-up.
func sameRequirementKey(p needs.Profile) (string, bool) {
	for _, r := range p.Requirements() {
		if r.Operator == needs.OperatorSame {
			return r.Key, true
		}
	}
	return "", false
}

// poolFor returns the cached pool for (state, profile.fingerprint),
// building it on first call.
//
// Single-type pools (the common case — pinned `instance-type In [x]`
// selector) reuse the snapshot's pre-sorted bucket directly: no slice
// copy, no sort. Multi-type pools merge the per-type slices and re-sort
// by EffectiveCost so cross-type ordering is correct.
//
// Pool keys mix the state into the fingerprint so the same Profile's
// idle pool and speculative pool stay separate.
func (a *phase1Allocator) poolFor(
	state machine.State,
	profile needs.Profile,
) *phase1Pool {
	key := poolKey(state, profile)
	if pool, ok := a.pools[key]; ok {
		return pool
	}

	src := a.buildPoolSource(state, profile)
	pool := &phase1Pool{src: src, profile: profile}
	a.pools[key] = pool
	return pool
}

// buildPoolSource returns the candidate slice for one (state, profile).
//
// Idle paths can ride the snapshot's pre-sort directly: idle ordering
// is (price, id), which is exactly what the snapshot publishes. The
// single-type idle hot path returns the shared bucket without copying.
//
// Speculative paths cannot, because EffectiveCost depends on the
// per-machine InterruptionProbability and the per-Profile penalty.
// Within a single instance type two machines may differ in capacity
// type (spot vs on-demand) and therefore in p, so the snapshot's
// (price, id) pre-sort is not a substitute for the EffectiveCost sort.
// Speculative pools always copy + sort here. The cost is amortised
// across all Needs sharing the fingerprint via phase1Allocator.pools.
//
// Multi-type and unpinned profiles fall back to a merged-and-sorted
// slice for both states.
func (a *phase1Allocator) buildPoolSource(state machine.State, profile needs.Profile) []machine.Machine {
	types := pinnedInstanceTypes(profile)

	if state == machine.StateIdle && len(types) == 1 {
		// Hot path. Idle ordering matches the snapshot's pre-sort
		// exactly; reuse the shared bucket without copying.
		return a.snap.ListByStateInstanceType(state, types[0])
	}

	var merged []machine.Machine
	switch {
	case types == nil:
		merged = a.snap.ListByState(state)
	case len(types) == 1:
		// Single-type speculative: still need a copy because the
		// speculative sort below is destructive and the snapshot's
		// bucket is shared.
		src := a.snap.ListByStateInstanceType(state, types[0])
		merged = make([]machine.Machine, len(src))
		copy(merged, src)
	default:
		for _, t := range types {
			merged = append(merged, a.snap.ListByStateInstanceType(state, t)...)
		}
	}
	if len(merged) <= 1 {
		return merged
	}

	switch state {
	case machine.StateIdle:
		sortIdleCandidates(merged)
	case machine.StateSpeculative:
		penalty := BucketUpperBoundDollars(profile.InterruptionPenaltyBucket())
		sortSpeculativeCandidates(merged, penalty)
	default:
		// Other states aren't expected to hit Phase 1's pool path; sort
		// by ID for determinism.
		sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	}
	return merged
}

func poolKey(state machine.State, profile needs.Profile) string {
	// machine.State is small; one-byte prefix avoids fingerprint
	// collisions across states without needing fmt.
	return string([]byte{byte(state)}) + profile.Fingerprint()
}
