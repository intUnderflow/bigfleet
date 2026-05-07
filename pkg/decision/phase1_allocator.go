package decision

import (
	"sort"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
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
//
// coLocated* fields are M44.4 / ADR-0019: cache the MatchProfile-and-
// bucketed-by-sameKey layout, with per-bucket head cursors. Subsequent
// takeCoLocated calls don't re-walk pool.src, and don't re-bucket the
// already-walked machines — they advance head cursors past claimed
// machines (O(claimed) amortised) and pick the best bucket in
// O(buckets) per call. At cloud scale this is the path the operator's
// owner-grouped → Same translation routes every Need through, so the
// optimization matters for every realistic Pod-mode profile.
type phase1Pool struct {
	src              []machine.Machine
	profile          needs.Profile
	head             int
	coLocatedBuilt   bool
	coLocatedSameKey string
	coLocatedBuckets []coLocatedBucket
}

type coLocatedBucket struct {
	key      string
	machines []machine.Machine
	head     int // advances past claimed machines across calls
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
// Topology routing (paper §8):
//
//   - Same requirement → takeCoLocated. All returned machines share
//     the same value for the Same key.
//
//   - DoNotSchedule Spread → takeSpread. Returned machines respect
//     the per-domain MaxSkew constraint; cheapest-eligible-bucket
//     wins at each step.
//
//   - Both Same and Spread on the same Need → Same wins (it's the
//     stronger constraint; spread across values is incompatible
//     with co-location to one). The Spread is logged elsewhere if a
//     warning surface exists; here we silently take the Same path.
//
//   - ScheduleAnyway Spread → standard take (no enforcement; spread
//     is best-effort).
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
	if key, skew, ok := strictSpread(profile); ok {
		return a.takeSpread(state, profile, n, key, skew)
	}
	start := time.Now()
	defer func() {
		metrics.ShardPhase1TakeDuration.WithLabelValues("take").Observe(time.Since(start).Seconds())
		metrics.ShardPhase1Calls.WithLabelValues("take").Inc()
	}()
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
	start := time.Now()
	defer func() {
		metrics.ShardPhase1TakeDuration.WithLabelValues("takeCoLocated").Observe(time.Since(start).Seconds())
		metrics.ShardPhase1Calls.WithLabelValues("takeCoLocated").Inc()
	}()
	pool := a.poolFor(state, profile)
	if pool == nil {
		return nil
	}

	// ADR-0019 (M44.4): cache the MatchProfile-and-bucketed layout
	// once per pool. Subsequent calls advance per-bucket head cursors
	// past claimed machines and pick the best bucket in O(buckets).
	// Pre-cache, takeCoLocated re-walked pool.src on every call;
	// at scaleway-50k cloud scale (50 K Same-everywhere Needs against
	// ~10 K Idle per pool) that was ~5×10⁸ MatchProfile calls per
	// cycle and dominated Phase 1 wall-clock.
	if !pool.coLocatedBuilt || pool.coLocatedSameKey != sameKey {
		index := make(map[string]int)
		pool.coLocatedBuckets = pool.coLocatedBuckets[:0]
		for _, m := range pool.src {
			if !MatchProfile(pool.profile, m) {
				continue
			}
			v, ok := lookupAttribute(sameKey, m)
			if !ok {
				continue
			}
			i, exists := index[v]
			if !exists {
				i = len(pool.coLocatedBuckets)
				index[v] = i
				pool.coLocatedBuckets = append(pool.coLocatedBuckets, coLocatedBucket{key: v})
			}
			pool.coLocatedBuckets[i].machines = append(pool.coLocatedBuckets[i].machines, m)
		}
		pool.coLocatedBuilt = true
		pool.coLocatedSameKey = sameKey
	}

	// Advance each bucket's head cursor past machines claimed in
	// earlier calls. Total advancement across the cycle is O(claims),
	// amortised to O(1) per claim.
	for i := range pool.coLocatedBuckets {
		b := &pool.coLocatedBuckets[i]
		for b.head < len(b.machines) {
			if _, claimed := a.claimed[b.machines[b.head].ID]; !claimed {
				break
			}
			b.head++
		}
	}

	// Pick the best non-empty bucket.
	//   1. Atomic-satisfiable (≥ n unclaimed) preferred.
	//   2. Within atomic-satisfiable: cheapest head price, then
	//      smaller available, then key.
	//   3. If none atomic: pick the largest available, then cheapest
	//      head, then key.
	bestIdx := -1
	for i := range pool.coLocatedBuckets {
		b := &pool.coLocatedBuckets[i]
		avail := len(b.machines) - b.head
		if avail <= 0 {
			continue
		}
		if bestIdx < 0 {
			bestIdx = i
			continue
		}
		bb := &pool.coLocatedBuckets[bestIdx]
		bestAvail := len(bb.machines) - bb.head
		bestAtomic := bestAvail >= n
		bAtomic := avail >= n
		better := false
		switch {
		case bAtomic && !bestAtomic:
			better = true
		case bAtomic == bestAtomic:
			if bAtomic {
				// Both atomic: cheapest head, then smaller available, then key.
				switch {
				case b.machines[b.head].PricePerHour < bb.machines[bb.head].PricePerHour:
					better = true
				case b.machines[b.head].PricePerHour == bb.machines[bb.head].PricePerHour && avail < bestAvail:
					better = true
				case b.machines[b.head].PricePerHour == bb.machines[bb.head].PricePerHour && avail == bestAvail && b.key < bb.key:
					better = true
				}
			} else {
				// Both partial: largest available, then cheapest head, then key.
				switch {
				case avail > bestAvail:
					better = true
				case avail == bestAvail && b.machines[b.head].PricePerHour < bb.machines[bb.head].PricePerHour:
					better = true
				case avail == bestAvail && b.machines[b.head].PricePerHour == bb.machines[bb.head].PricePerHour && b.key < bb.key:
					better = true
				}
			}
		}
		if better {
			bestIdx = i
		}
	}
	if bestIdx < 0 {
		return nil
	}
	best := &pool.coLocatedBuckets[bestIdx]

	avail := len(best.machines) - best.head
	take := n
	if take > avail {
		take = avail
	}
	out := make([]machine.Machine, take)
	for i := 0; i < take; i++ {
		m := best.machines[best.head]
		best.head++
		out[i] = m
		a.claimed[m.ID] = struct{}{}
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

// strictSpread returns the topology key + max skew of the profile's
// first DoNotSchedule TopologySpread entry, if any. MVP: supports a
// single Spread key; profiles with multiple Spread entries would need
// to honour all of them, which is a follow-up.
//
// ScheduleAnyway entries are intentionally not surfaced here — they
// represent a soft preference that the allocator's standard
// cheapest-first behaviour already approximates well enough for v1.
func strictSpread(p needs.Profile) (string, int32, bool) {
	for _, s := range p.Spread() {
		if s.WhenUnsatisfiable == needs.WhenUnsatisfiableDoNotSchedule {
			return s.TopologyKey, s.MaxSkew, true
		}
	}
	return "", 0, false
}

// takeSpread enforces a DoNotSchedule TopologySpread: the per-bucket
// pick count never exceeds (current minimum + maxSkew). At each step
// it picks the cheapest head among buckets that are within the skew
// envelope, so cost ordering is preserved within the constraint.
//
// MaxSkew clamps to ≥1 (a profile that asked for 0 would be
// unsatisfiable by definition).
//
// Same as takeCoLocated, this does NOT advance pool.head — bucketing
// implies a whole-pool walk, and the global claimed set handles
// dedup across Needs.
func (a *phase1Allocator) takeSpread(state machine.State, profile needs.Profile, n int, key string, maxSkew int32) []machine.Machine {
	start := time.Now()
	defer func() {
		metrics.ShardPhase1TakeDuration.WithLabelValues("takeSpread").Observe(time.Since(start).Seconds())
		metrics.ShardPhase1Calls.WithLabelValues("takeSpread").Inc()
	}()
	pool := a.poolFor(state, profile)
	if pool == nil {
		return nil
	}
	skew := int(maxSkew)
	if skew < 1 {
		skew = 1
	}

	type bucketState struct {
		machines []machine.Machine
		head     int
	}
	buckets := make(map[string]*bucketState)
	keys := make([]string, 0)
	for _, m := range pool.src {
		if _, claimed := a.claimed[m.ID]; claimed {
			continue
		}
		if !MatchProfile(pool.profile, m) {
			continue
		}
		v, ok := lookupAttribute(key, m)
		if !ok {
			continue
		}
		b, exists := buckets[v]
		if !exists {
			b = &bucketState{}
			buckets[v] = b
			keys = append(keys, v)
		}
		b.machines = append(b.machines, m)
	}
	if len(keys) == 0 {
		return nil
	}

	counts := make(map[string]int, len(keys))
	out := make([]machine.Machine, 0, n)

	for len(out) < n {
		// minCount is the lowest pick count across ALL buckets in
		// the topology domain — including exhausted buckets whose
		// counts are frozen. The skew constraint is "max - min ≤
		// maxSkew" over the whole domain, not just buckets that
		// still have candidates.
		minCount := -1
		for _, k := range keys {
			c := counts[k]
			if minCount == -1 || c < minCount {
				minCount = c
			}
		}

		// Eligible: counts[k] ≤ minCount + skew - 1 AND has remaining.
		// Pick cheapest head; tie-break on key for determinism.
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
		out = append(out, m)
		a.claimed[m.ID] = struct{}{}
		counts[bestKey]++
	}
	return out
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
	start := time.Now()
	defer func() {
		metrics.ShardPhase1PoolBuildDuration.Observe(time.Since(start).Seconds())
	}()
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
