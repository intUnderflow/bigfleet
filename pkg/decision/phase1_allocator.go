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
func (a *phase1Allocator) take(
	state machine.State,
	profile needs.Profile,
	n int,
) []machine.Machine {
	if n <= 0 {
		return nil
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
