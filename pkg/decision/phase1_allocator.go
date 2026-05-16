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
	// Scratch maps reused across every takeCoLocated invocation in a
	// single Phase 1 cycle. takeCoLocated runs once per Need; reusing
	// these eliminates per-call map allocations.
	scratchMinUnit   map[string]int64
	scratchDeficit   map[string]int64
	scratchRemaining map[string]int64
}

// phase1Pool holds the candidate slice for one (state, fingerprint).
// `src` is read-only — for single-type pools it points at the snapshot's
// shared bucket; for multi-type pools it's a freshly-allocated merged
// slice. take() never mutates src; it just advances head and applies
// the lazy MatchProfile + claimed filter.
//
// ADR-0027: pools are heterogeneous in machine size — MatchProfile is
// requirement-only now, so an `instance-type In [a, b]` Need's pool
// holds both a-shaped and b-shaped machines. take() sums their actual
// EffectiveAllocatable against the Need's deficit vector; there is no
// per-pool "density" because there is no single per-machine shape.
//
// coLocated* fields are M44.4 / ADR-0019: cache the MatchProfile-and-
// bucketed-by-sameKey layout, with per-bucket head cursors. Subsequent
// takeCoLocated calls don't re-walk pool.src, and don't re-bucket the
// already-walked machines — they advance head cursors past claimed
// machines (O(claimed) amortised) and pick the best bucket. At cloud
// scale this is the path the operator's owner-grouped → Same translation
// routes every Need through, so the optimization matters for every
// realistic Pod-mode profile.
type phase1Pool struct {
	src              []machine.Machine
	profile          needs.Profile
	head             int
	coLocatedBuilt   bool
	coLocatedSameKey string
	coLocatedBuckets []coLocatedBucket
}

// coLocatedBucket caches the aggregate score of a single sameKey
// bucket, maintained incrementally across takeCoLocated calls so the
// score loop is O(buckets) per call instead of O(machines-in-bucket).
//
// Aggregates assume no per-Need minUnit filter — i.e. every bucket
// machine is assumed to pass minUnit. This is true for the realistic-
// catalog case where one Profile fingerprint corresponds to one
// archetype with uniform machine shape, so all Needs hitting this
// pool share the same minUnit shape that every bucket machine clears.
// If a Need's minUnit is smaller than the bucket can serve, the score
// is over-optimistic — the take loop's per-machine CoversParsed check
// still filters correctly, but the bucket might be picked when a
// smaller bucket would have been a tighter fit. Sim goldens guard the
// correctness boundary; the wrong-pick case is bounded by the same
// partial-fill semantics the algorithm already has.
type coLocatedBucket struct {
	key      string
	machines []machine.Machine
	// parsedAllocs[j] is machines[j]'s EffectiveAllocatable in parsed
	// (int64 milli-unit) form. Populated once at bucket build.
	parsedAllocs [][]needs.ParsedQty
	head         int              // advances past claimed machines at the front
	avail        int              // count of unclaimed machines in [head..end]
	capacity     map[string]int64 // sum of unclaimed machines' allocs in [head..end]
}

func newPhase1Allocator(snap *inventory.Snapshot) *phase1Allocator {
	return &phase1Allocator{
		snap:             snap,
		pools:            make(map[string]*phase1Pool),
		claimed:          make(map[machine.ID]struct{}),
		scratchMinUnit:   make(map[string]int64, 4),
		scratchDeficit:   make(map[string]int64, 4),
		scratchRemaining: make(map[string]int64, 4),
	}
}

// take returns unclaimed, MatchProfile-passing machines from the
// per-(state, fingerprint) pool whose summed EffectiveAllocatable covers
// the deficit vector (ADR-0027) — or as many as the pool can offer if it
// cannot fully cover. Each returned machine can host one minUnit (the
// indivisibility floor); a machine too small for minUnit is skipped
// without consuming the pool's head cursor, since a peer Need with a
// smaller minUnit may still use it. take claims what it returns.
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
	deficit, minUnit []needs.ResourceQty,
) []machine.Machine {
	if needs.IsZero(deficit) {
		return nil
	}
	if key, ok := sameRequirementKey(profile); ok {
		return a.takeCoLocated(state, profile, deficit, minUnit, key)
	}
	if key, skew, ok := strictSpread(profile); ok {
		return a.takeSpread(state, profile, deficit, minUnit, key, skew)
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
	remaining := deficit
	var taken []machine.Machine
	// advancing tracks whether pool.head is still moving past a prefix
	// of permanently-done machines (claimed globally, or MatchProfile
	// failures for this pool). Once a minUnit-too-small machine is hit,
	// head stops advancing — that machine must stay visible to a peer
	// Need whose minUnit is smaller.
	advancing := true
	for i := pool.head; i < len(pool.src) && !needs.IsZero(remaining); i++ {
		m := pool.src[i]
		if _, claimed := a.claimed[m.ID]; claimed {
			if advancing {
				pool.head = i + 1
			}
			continue
		}
		if !MatchProfile(pool.profile, m) {
			if advancing {
				pool.head = i + 1
			}
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		if !needs.Covers(alloc, minUnit) {
			advancing = false
			continue
		}
		taken = append(taken, m)
		a.claimed[m.ID] = struct{}{}
		remaining = needs.SubResources(remaining, alloc)
		if advancing {
			pool.head = i + 1
		}
	}
	return taken
}

// takeCoLocated honours a Profile's Same requirement (paper §8): all
// returned machines share one value for the Same key. It picks the best
// single-value bucket — a bucket whose unclaimed, minUnit-passing
// machines have enough summed EffectiveAllocatable to cover the deficit
// ("atomic-satisfiable") wins, cheapest head first; otherwise the bucket
// with the most available machines is taken for a partial fill (the
// residual becomes a shortfall via the caller's deficit arithmetic).
//
// Does NOT advance pool.head — bucketing implies we walk the whole pool
// and skip the head cursor convention. Subsequent regular take() calls
// on the same pool still behave normally; the global claimed set catches
// anything we consumed.
func (a *phase1Allocator) takeCoLocated(state machine.State, profile needs.Profile, deficit, minUnit []needs.ResourceQty, sameKey string) []machine.Machine {
	start := time.Now()
	defer func() {
		metrics.ShardPhase1TakeDuration.WithLabelValues("takeCoLocated").Observe(time.Since(start).Seconds())
		metrics.ShardPhase1Calls.WithLabelValues("takeCoLocated").Inc()
	}()
	pool := a.poolFor(state, profile)
	if pool == nil {
		return nil
	}

	// ADR-0019 (M44.4): cache the MatchProfile-and-bucketed layout once
	// per pool. Subsequent calls advance per-bucket head cursors past
	// claimed machines and read pre-maintained aggregate scores.
	//
	// Build also populates parsedAllocs (per-machine alloc in parsed
	// form) and the avail/capacity aggregates assuming every machine is
	// unclaimed. Subsequent claims maintain the aggregates incrementally
	// inside the take loop below; head-advance just prunes the claimed
	// prefix (no aggregate change since claims already decremented).
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
				pool.coLocatedBuckets = append(pool.coLocatedBuckets, coLocatedBucket{
					key:      v,
					capacity: make(map[string]int64, 4),
				})
			}
			b := &pool.coLocatedBuckets[i]
			alloc := needs.ParseAllocatableMap(m.EffectiveAllocatable())
			b.machines = append(b.machines, m)
			b.parsedAllocs = append(b.parsedAllocs, alloc)
			b.avail++
			needs.AddParsedInto(b.capacity, alloc)
		}
		pool.coLocatedBuilt = true
		pool.coLocatedSameKey = sameKey
	}

	// Advance each bucket's head cursor past machines claimed in earlier
	// calls. Aggregates were already decremented when each claim happened
	// in the take loop, so head-advance does not touch avail/capacity —
	// it just skips a stale prefix to keep machines[head] addressable.
	for i := range pool.coLocatedBuckets {
		b := &pool.coLocatedBuckets[i]
		for b.head < len(b.machines) {
			if _, claimed := a.claimed[b.machines[b.head].ID]; !claimed {
				break
			}
			b.head++
		}
	}

	// Parse the per-Need vectors once into pre-allocated scratch maps.
	minUnitMap := a.scratchMinUnit
	needs.ClearMap(minUnitMap)
	fillParsedMap(minUnitMap, minUnit)
	deficitMap := a.scratchDeficit
	needs.ClearMap(deficitMap)
	fillParsedMap(deficitMap, deficit)

	// Score every non-empty bucket directly from cached aggregates.
	// O(buckets), no per-machine walk — the score loop's previous shape
	// (O(unclaimed-in-bucket) per call) was the dominant cost at
	// uber-50k (bigfleet-uber #17: 9.41 ms/call from per-machine
	// allocation overhead × ~2.5K machines per bucket).
	//   1. atomic-satisfiable preferred;
	//   2. within atomic: cheapest head price, then key;
	//   3. within partial: most available machines, then cheapest head,
	//      then key.
	bestIdx := -1
	bestAtomic := false
	bestHeadPrice := 0.0
	bestAvail := -1
	for i := range pool.coLocatedBuckets {
		b := &pool.coLocatedBuckets[i]
		// b.head past end ⇒ bucket exhausted from this pool's view.
		// Cached avail may be stale (over-counted) when another pool
		// sharing the same snapshot instance-type bucket claims one of
		// our machines: we don't see that claim until head-advance
		// walks past it, and we don't decrement avail/capacity for
		// cross-pool claims. The functional impact is bounded — take
		// loop won't find anything to claim and Phase 1 falls through
		// to Speculative — but we still skip the bucket explicitly.
		if b.avail == 0 || b.head >= len(b.machines) {
			continue
		}
		atomic := needs.CoversMaps(b.capacity, deficitMap)
		// machines[head] is the cheapest unclaimed; pool.src is
		// price-sorted (idle) or EffectiveCost-sorted (speculative),
		// bucketing preserves that order within each value.
		headPrice := b.machines[b.head].PricePerHour
		avail := b.avail
		better := false
		switch {
		case bestIdx < 0:
			better = true
		case atomic && !bestAtomic:
			better = true
		case atomic == bestAtomic:
			bestKey := pool.coLocatedBuckets[bestIdx].key
			if atomic {
				switch {
				case headPrice < bestHeadPrice:
					better = true
				case headPrice == bestHeadPrice && b.key < bestKey:
					better = true
				}
			} else {
				switch {
				case avail > bestAvail:
					better = true
				case avail == bestAvail && headPrice < bestHeadPrice:
					better = true
				case avail == bestAvail && headPrice == bestHeadPrice && b.key < bestKey:
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
		return nil
	}

	// Take from the chosen bucket until the deficit is covered or the
	// bucket is exhausted. Each claim maintains the bucket's avail and
	// capacity aggregates so subsequent score-loop reads stay valid.
	best := &pool.coLocatedBuckets[bestIdx]
	remaining := a.scratchRemaining
	needs.ClearMap(remaining)
	for k, v := range deficitMap {
		remaining[k] = v
	}
	var out []machine.Machine
	for j := best.head; j < len(best.machines); j++ {
		if needs.IsZeroMap(remaining) {
			break
		}
		m := best.machines[j]
		if _, claimed := a.claimed[m.ID]; claimed {
			continue
		}
		alloc := best.parsedAllocs[j]
		if !needs.CoversParsed(alloc, minUnitMap) {
			continue
		}
		out = append(out, m)
		a.claimed[m.ID] = struct{}{}
		needs.SubParsedInto(remaining, alloc)
		// Maintain bucket aggregates.
		best.avail--
		for _, r := range alloc {
			v := best.capacity[r.Name] - r.Milli
			if v < 0 {
				v = 0
			}
			best.capacity[r.Name] = v
		}
	}
	return out
}

// fillParsedMap populates dst with src's parsed milli-values. Assumes
// dst was just cleared; entries are written, not merged.
func fillParsedMap(dst map[string]int64, src []needs.ResourceQty) {
	for _, r := range src {
		dst[r.Name] = needs.ParseQtyMilli(r.Quantity)
	}
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
// pick count never exceeds (current minimum + maxSkew). At each step it
// picks the cheapest head among buckets within the skew envelope, so
// cost ordering is preserved within the constraint, and stops once the
// taken machines' summed EffectiveAllocatable covers the deficit vector.
//
// MaxSkew clamps to ≥1 (a profile that asked for 0 would be
// unsatisfiable by definition).
//
// Same as takeCoLocated, this does NOT advance pool.head — bucketing
// implies a whole-pool walk, and the global claimed set handles dedup
// across Needs. minUnit-too-small machines are filtered at build time so
// they never enter a bucket.
func (a *phase1Allocator) takeSpread(state machine.State, profile needs.Profile, deficit, minUnit []needs.ResourceQty, key string, maxSkew int32) []machine.Machine {
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
		if !needs.Covers(needs.ResourceQtysFromMap(m.EffectiveAllocatable()), minUnit) {
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
	remaining := deficit
	var out []machine.Machine

	for !needs.IsZero(remaining) {
		// minCount is the lowest pick count across ALL buckets in the
		// topology domain — including exhausted buckets whose counts are
		// frozen. The skew constraint is "max - min ≤ maxSkew" over the
		// whole domain, not just buckets that still have candidates.
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
		remaining = needs.SubResources(remaining, needs.ResourceQtysFromMap(m.EffectiveAllocatable()))
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
