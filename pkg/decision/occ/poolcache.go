package occ

import (
	"sort"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Pool holds the cost-sorted candidate slice for one (state,
// Profile.Fingerprint). Build is one-time per cycle; src is immutable
// after construction so worker goroutines can read it lock-free.
//
// The candidate-find functions (FindBasic, FindSame, FindSpread) walk
// src filtering by SharedState.IsClaimed + MatchProfile + the per-Need
// MinUnit floor + any Same/Spread constraints. No per-bucket head
// cursor (unlike phase1Pool) — concurrent workers each re-walk the
// pool from the start; the global claimed-set bears the dedup load.
type Pool struct {
	src     []machine.Machine
	profile needs.Profile
}

// Profile returns the Profile this pool was built for. Workers use it
// to MatchProfile-filter src lazily at find time.
func (p *Pool) Profile() needs.Profile { return p.profile }

// PoolCache caches per-cycle Pool instances keyed by (state, profile
// fingerprint). One PoolCache per RunCycle invocation. Concurrent
// workers ask for pools as they process Needs; identical lookups
// share a single Pool, identical first-time lookups serialise on a
// per-key sync.Once so concurrent workers don't redo pool sorts.
type PoolCache struct {
	snap *inventory.Snapshot

	mu    sync.Mutex
	pools map[string]*lazyPool
}

// lazyPool holds the once-per-key build guard plus the eventual
// result. The Pool field is written only inside once.Do and read
// after; once.Do's happens-before semantics make this race-free
// without further synchronisation.
type lazyPool struct {
	once sync.Once
	pool *Pool
}

// NewPoolCache returns an empty cache wrapping snap. The snapshot is
// retained for the cache's lifetime and read on first lookup of each
// pool key.
func NewPoolCache(snap *inventory.Snapshot) *PoolCache {
	return &PoolCache{
		snap:  snap,
		pools: make(map[string]*lazyPool),
	}
}

// Get returns the Pool for (state, profile), building it lazily on
// first call. Subsequent calls for the same key return the cached
// Pool. Safe for concurrent use.
func (c *PoolCache) Get(state machine.State, profile needs.Profile) *Pool {
	key := poolKey(state, profile)

	c.mu.Lock()
	lp, ok := c.pools[key]
	if !ok {
		lp = &lazyPool{}
		c.pools[key] = lp
	}
	c.mu.Unlock()

	lp.once.Do(func() {
		lp.pool = c.buildPool(state, profile)
	})
	return lp.pool
}

// buildPool produces the sorted candidate slice for (state, profile).
// Idle + a single pinned instance type reuses the snapshot's pre-
// sorted shared bucket (no copy, no resort). Speculative always
// copies and re-sorts because EffectiveCost depends on the per-
// machine InterruptionProbability times the per-Profile penalty.
// Multi-type and unpinned profiles fall back to a merged-and-sorted
// slice for both states.
func (c *PoolCache) buildPool(state machine.State, profile needs.Profile) *Pool {
	types := pinnedInstanceTypes(profile)

	if state == machine.StateIdle && len(types) == 1 {
		return &Pool{
			src:     c.snap.ListByStateInstanceType(state, types[0]),
			profile: profile,
		}
	}

	var merged []machine.Machine
	switch {
	case types == nil:
		merged = c.snap.ListByState(state)
	case len(types) == 1:
		src := c.snap.ListByStateInstanceType(state, types[0])
		merged = make([]machine.Machine, len(src))
		copy(merged, src)
	default:
		for _, t := range types {
			merged = append(merged, c.snap.ListByStateInstanceType(state, t)...)
		}
	}
	if len(merged) <= 1 {
		return &Pool{src: merged, profile: profile}
	}

	switch state {
	case machine.StateIdle:
		sortIdleCandidates(merged)
	case machine.StateSpeculative:
		penalty := profile.InterruptionPenaltyBucket().UpperBoundDollars()
		sortSpeculativeCandidates(merged, penalty)
	default:
		sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	}
	return &Pool{src: merged, profile: profile}
}

func poolKey(state machine.State, profile needs.Profile) string {
	return string([]byte{byte(state)}) + profile.Fingerprint()
}

// pinnedInstanceTypes returns the explicit instance-type values from
// a Profile's `node.kubernetes.io/instance-type In [...]` requirement,
// or nil if the Profile doesn't pin to a finite set we can index on.
func pinnedInstanceTypes(p needs.Profile) []string {
	for _, r := range p.RequirementsRO() {
		if r.Key != "node.kubernetes.io/instance-type" {
			continue
		}
		if r.Operator == needs.OperatorIn && len(r.Values) > 0 {
			return r.Values
		}
		return nil
	}
	return nil
}

func sortIdleCandidates(s []machine.Machine) {
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].PricePerHour != s[j].PricePerHour {
			return s[i].PricePerHour < s[j].PricePerHour
		}
		return s[i].ID < s[j].ID
	})
}

func sortSpeculativeCandidates(s []machine.Machine, interruptionPenaltyDollars float64) {
	sort.SliceStable(s, func(i, j int) bool {
		ai := s[i].EffectiveCost(interruptionPenaltyDollars)
		aj := s[j].EffectiveCost(interruptionPenaltyDollars)
		if ai != aj {
			return ai < aj
		}
		return s[i].ID < s[j].ID
	})
}
