package decision

import (
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// phase1Allocator coordinates Phase 1's per-Need consumption against a
// shared inventory snapshot. It maintains:
//
//   - One sorted candidate pool per distinct Profile fingerprint, built
//     lazily on first use, sized to the matching subset of the
//     instance-type bucket.
//   - A single global "claimed" set so a machine matching multiple
//     Profiles is only ever consumed once.
//
// The pool is a slice + a head index. take() advances the head, skipping
// any entries that have been claimed by another pool since this pool
// was built. That makes per-claim work amortized O(1) and per-take work
// O(taken + cross-pool claims that overlapped this pool's head).
//
// Phase 1's outer loop walks Needs in priority order; the allocator
// guarantees high-priority Needs see fresh inventory and low-priority
// Needs only see what's left over. This is the Phase 1 invariant the
// naive per-fingerprint cache (M11.15 attempt 2) accidentally broke.
type phase1Allocator struct {
	snap    *inventory.Snapshot
	pools   map[string]*phase1Pool
	claimed map[machine.ID]struct{}
}

type phase1Pool struct {
	machines []machine.Machine // sorted; build order is stable
	head     int               // next index to consider; pre-head is exhausted
}

func newPhase1Allocator(snap *inventory.Snapshot) *phase1Allocator {
	return &phase1Allocator{
		snap:    snap,
		pools:   make(map[string]*phase1Pool),
		claimed: make(map[machine.ID]struct{}),
	}
}

// take returns up to n unclaimed machines from the per-Profile pool for
// (state, profile). Claims them as part of the call. sortFn orders the
// pool on first build; subsequent take()s reuse the sorted slice.
func (a *phase1Allocator) take(
	state machine.State,
	profile needs.Profile,
	n int,
	sortFn func([]machine.Machine),
) []machine.Machine {
	if n <= 0 {
		return nil
	}
	pool := a.poolFor(state, profile, sortFn)
	if pool == nil {
		return nil
	}
	taken := make([]machine.Machine, 0, n)
	for pool.head < len(pool.machines) && len(taken) < n {
		m := pool.machines[pool.head]
		pool.head++
		if _, claimed := a.claimed[m.ID]; claimed {
			continue
		}
		taken = append(taken, m)
		a.claimed[m.ID] = struct{}{}
	}
	return taken
}

// poolFor returns the cached pool for (state, profile.fingerprint),
// building it if necessary. The build pass filters by both MatchProfile
// (per-Need correctness) and the global claimed set (so an early
// high-priority claim from another fingerprint doesn't appear in a
// later-built low-priority pool).
//
// Pool keys mix the state into the fingerprint so the same Profile's
// idle pool and speculative pool stay separate.
func (a *phase1Allocator) poolFor(
	state machine.State,
	profile needs.Profile,
	sortFn func([]machine.Machine),
) *phase1Pool {
	key := poolKey(state, profile)
	if pool, ok := a.pools[key]; ok {
		return pool
	}
	raw := candidatePool(a.snap, state, profile)
	pool := &phase1Pool{machines: make([]machine.Machine, 0, len(raw))}
	for _, m := range raw {
		if _, claimed := a.claimed[m.ID]; claimed {
			continue
		}
		if !MatchProfile(profile, m) {
			continue
		}
		pool.machines = append(pool.machines, m)
	}
	if sortFn != nil {
		sortFn(pool.machines)
	}
	a.pools[key] = pool
	return pool
}

func poolKey(state machine.State, profile needs.Profile) string {
	// machine.State is small; one-byte prefix avoids fingerprint
	// collisions across states without needing fmt.
	return string([]byte{byte(state)}) + profile.Fingerprint()
}
