// Package inventory holds the shard's in-memory machine store.
//
// The shard owns every machine assigned to it; the inventory is the
// authoritative local view (the provider is the source of truth across
// the wire, but reconciliation hits this in-memory cache rather than the
// provider directly on the hot path).
//
// Indexes are intentionally minimal in M2. Plan §10.4 calls for richer
// indexing once Phase 2 runs at full scale; we'll add (instance_type,
// zone, capacity_type, priority-of-cluster) indexes when profiling
// confirms the need.
package inventory

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Inventory is a thread-safe collection of machines indexed by ID.
// All readers receive defensive copies via Snapshot(); writes go
// through Apply for state-machine validation.
//
// ADR-0003 (background fold goroutine + CycleSnapshot) was superseded by
// M44.4 Drop A, which switched the shard cycle to the synchronous
// Snapshot() API. The fold goroutine and its live byState /
// byClusterState / byStateInstanceTp indexes have been removed; the
// shard hot path and all callers use Snapshot() for consistent reads.
type Inventory struct {
	mu   sync.RWMutex
	byID map[machine.ID]machine.Machine
}

// New returns an empty inventory.
func New() *Inventory {
	return &Inventory{
		byID: make(map[machine.ID]machine.Machine),
	}
}

// ErrNotFound is returned when a machine ID is not in the inventory.
var ErrNotFound = errors.New("machine not found")

// Insert adds a new machine. Fails if a machine with the same ID already
// exists or if the machine fails its own structural invariant.
func (i *Inventory) Insert(m machine.Machine) error {
	if err := m.Invariant(); err != nil {
		return fmt.Errorf("inventory: %w", err)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, exists := i.byID[m.ID]; exists {
		return fmt.Errorf("inventory: machine %s already exists", m.ID)
	}
	i.byID[m.ID] = m
	return nil
}

// Apply replaces the existing machine with the given record. Validates
// the new state, the structural invariant, and the state machine
// transition when the state changes.
func (i *Inventory) Apply(m machine.Machine) error {
	if err := m.Invariant(); err != nil {
		return fmt.Errorf("inventory: %w", err)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	old, exists := i.byID[m.ID]
	if !exists {
		return fmt.Errorf("inventory: %w: %s", ErrNotFound, m.ID)
	}
	if old.State != m.State {
		if err := machine.CheckTransition(old.State, m.State); err != nil {
			return fmt.Errorf("inventory: %w", err)
		}
	}
	i.byID[m.ID] = m
	return nil
}

// Remove drops the machine from the inventory.
func (i *Inventory) Remove(id machine.ID) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, exists := i.byID[id]; !exists {
		return fmt.Errorf("inventory: %w: %s", ErrNotFound, id)
	}
	delete(i.byID, id)
	return nil
}

// Get returns a copy of the machine with the given ID, or ErrNotFound.
func (i *Inventory) Get(id machine.ID) (machine.Machine, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	m, ok := i.byID[id]
	if !ok {
		return machine.Machine{}, fmt.Errorf("inventory: %w: %s", ErrNotFound, id)
	}
	return m, nil
}

// Len returns the total number of machines.
func (i *Inventory) Len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.byID)
}

// Snapshot builds a fresh snapshot under the inventory's read lock
// and returns it. O(N) — every call walks the entire byID map. This
// is the only snapshot API; all callers including the shard cycle hot
// path use it for a consistent view of the inventory.
//
// ADR-0003's background fold goroutine and CycleSnapshot() were
// superseded by M44.4 Drop A: the shard cycle switched to this
// synchronous API because the stale cached pointer caused ~50% wasted
// Bootstraps at real write rates.
func (i *Inventory) Snapshot() *Snapshot {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.snapshotLocked()
}

// snapshotLocked builds a fresh snapshot from the current live indexes.
// Caller must hold i.mu.RLock.
func (i *Inventory) snapshotLocked() *Snapshot {
	machines := make([]machine.Machine, 0, len(i.byID))
	byID := make(map[machine.ID]int, len(i.byID))
	byState := make(map[machine.State][]machine.ID)
	byClusterState := make(map[machine.ClusterID]map[machine.State][]machine.ID)
	byStateInstanceTp := make(map[machine.State]map[string][]machine.ID)

	ids := make([]machine.ID, 0, len(i.byID))
	for id := range i.byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	for idx, id := range ids {
		m := i.byID[id]
		machines = append(machines, m)
		byID[id] = idx
		byState[m.State] = append(byState[m.State], id)
		if m.Cluster != "" {
			byCluster, ok := byClusterState[m.Cluster]
			if !ok {
				byCluster = make(map[machine.State][]machine.ID)
				byClusterState[m.Cluster] = byCluster
			}
			byCluster[m.State] = append(byCluster[m.State], id)
		}
		if m.Profile.InstanceType != "" {
			byType, ok := byStateInstanceTp[m.State]
			if !ok {
				byType = make(map[string][]machine.ID)
				byStateInstanceTp[m.State] = byType
			}
			byType[m.Profile.InstanceType] = append(byType[m.Profile.InstanceType], id)
		}
	}
	// Pre-build sorted per-(state, instance-type) buckets so the
	// Phase 1 hot path can read O(1) without re-allocating + re-sorting
	// per Need. See M11.20.
	//
	// M44.4 Drop A snapread regression: sort an []int permutation of
	// indices into the master `machines` slice rather than a []Machine
	// directly. sort.Slice swaps Machine structs (~250 B each) via
	// reflection memmove; sorting ints (8 B) is ~30× cheaper at this
	// scale (60 K machines, ~2 K per bucket × ~50 buckets). The final
	// pass materialises the sorted []Machine in cache-friendly order.
	buckets := make(map[machine.State]map[string][]machine.Machine, len(byStateInstanceTp))
	for state, byType := range byStateInstanceTp {
		typed := make(map[string][]machine.Machine, len(byType))
		for instType, idList := range byType {
			idx := make([]int, len(idList))
			for k, id := range idList {
				idx[k] = byID[id]
			}
			slices.SortFunc(idx, func(a, b int) int {
				ma, mb := &machines[a], &machines[b]
				if ma.PricePerHour != mb.PricePerHour {
					if ma.PricePerHour < mb.PricePerHour {
						return -1
					}
					return 1
				}
				if ma.ID < mb.ID {
					return -1
				} else if ma.ID > mb.ID {
					return 1
				}
				return 0
			})
			ms := make([]machine.Machine, len(idx))
			for k, i := range idx {
				ms[k] = machines[i]
			}
			typed[instType] = ms
		}
		buckets[state] = typed
	}

	// Per-(cluster, state, instance-type) buckets for Phase 3's M27
	// fast path. Same (price asc, ID asc) sort as the global per-
	// (state, instance-type) buckets so callers can stream them
	// straight into Phase 3's keep-priority loop. At 500K configured
	// across 50 clusters and 5 instance types the cluster-scoped
	// buckets are ~2K each instead of ~100K — Phase 3's per-cluster
	// inner walk drops 5× when groups pin to single instance types
	// (the typical production shape).
	// Per-(cluster, state) buckets pre-sorted by Phase 3's keep-priority
	// order. Avoids the per-call ListByClusterState alloc + sort that
	// dominated Phase 3's hot path at 500K configured / 50 clusters
	// before M27.
	clusterStateBuckets := make(map[machine.ClusterID]map[machine.State][]machine.Machine, len(byClusterState))
	for cl, stateMap := range byClusterState {
		out := make(map[machine.State][]machine.Machine, len(stateMap))
		for st, idList := range stateMap {
			idx := make([]int, len(idList))
			for k, id := range idList {
				idx[k] = byID[id]
			}
			slices.SortFunc(idx, func(a, b int) int {
				ma, mb := &machines[a], &machines[b]
				if ma.PricePerHour != mb.PricePerHour {
					if ma.PricePerHour < mb.PricePerHour {
						return -1
					}
					return 1
				}
				if ma.AssignedReclamationPenaltyDollars != mb.AssignedReclamationPenaltyDollars {
					if ma.AssignedReclamationPenaltyDollars > mb.AssignedReclamationPenaltyDollars {
						return -1
					}
					return 1
				}
				if ma.ID < mb.ID {
					return -1
				} else if ma.ID > mb.ID {
					return 1
				}
				return 0
			})
			ms := make([]machine.Machine, len(idx))
			for k, i := range idx {
				ms[k] = machines[i]
			}
			out[st] = ms
		}
		clusterStateBuckets[cl] = out
	}

	// M44.4 snapread regression: bucketsByClusterStateInstanceTp was
	// prepared as an "M27 fast path for Phase 3 when groups pin to a
	// single instance type", but no caller ever read it — Phase 3
	// actually uses bucketsByClusterState (no per-type filter) and
	// filters in-loop. Building the per-cluster × state × instance-
	// type slices cost ~30 % of every snapshot for zero downstream
	// benefit. Removed alongside ListByClusterStateInstanceType.

	// M30.1: pre-compute per-(state, instance-type) and per-state
	// minimum AssignedPriority. Phase 2's preemption walk skips a
	// candidate pool entirely when the bucket's min priority ≥ the
	// preemptor's priority — i.e. there are no preemptable victims.
	// At the M29 burst shape (450K Configured at priority 1000000,
	// demand at priority 1000) this collapses the per-Need walk
	// from O(450K) to O(1).
	minPriorityByStateInstanceTp := make(map[machine.State]map[string]int32, len(buckets))
	minPriorityByState := make(map[machine.State]int32, len(buckets))
	for state, byType := range buckets {
		stateMin := int32(math.MaxInt32)
		typed := make(map[string]int32, len(byType))
		for instType, ms := range byType {
			tmin := int32(math.MaxInt32)
			for _, m := range ms {
				if m.AssignedPriority < tmin {
					tmin = m.AssignedPriority
				}
			}
			typed[instType] = tmin
			if tmin < stateMin {
				stateMin = tmin
			}
		}
		minPriorityByStateInstanceTp[state] = typed
		// Account for state machines without instance type (won't
		// land in any per-type bucket but still walked by the
		// unpinned fallback path).
		for _, id := range byState[state] {
			m := machines[byID[id]]
			if m.Profile.InstanceType != "" {
				continue
			}
			if m.AssignedPriority < stateMin {
				stateMin = m.AssignedPriority
			}
		}
		minPriorityByState[state] = stateMin
	}

	return &Snapshot{
		machines:                     machines,
		byID:                         byID,
		byState:                      byState,
		byClusterState:               byClusterState,
		byStateInstanceTp:            byStateInstanceTp,
		bucketsByStateInstanceTp:     buckets,
		bucketsByClusterState:        clusterStateBuckets,
		minPriorityByStateInstanceTp: minPriorityByStateInstanceTp,
		minPriorityByState:           minPriorityByState,
	}
}

// Snapshot is a read-only view of the inventory captured at one instant.
//
// Most accessors return freshly-allocated slices the caller may mutate.
// ListByStateInstanceType is the exception: it returns the pre-sorted
// shared bucket from bucketsByStateInstanceTp and the caller MUST NOT
// mutate it. See M11.20 — the per-bucket sort is paid once per fold so
// Phase 1's per-Need cursor consumption stays O(1) amortised.
type Snapshot struct {
	machines          []machine.Machine
	byID              map[machine.ID]int
	byState           map[machine.State][]machine.ID
	byClusterState    map[machine.ClusterID]map[machine.State][]machine.ID
	byStateInstanceTp map[machine.State]map[string][]machine.ID

	// bucketsByStateInstanceTp holds, for each (state, instance-type)
	// pair, a `[]machine.Machine` sorted by (price ascending, ID
	// ascending). This is exactly the order Phase 1 wants for both
	// idle and speculative single-type pools: same-type machines have
	// the same `interruption_probability`, so EffectiveCost(m, penalty)
	// is monotonic in price for any penalty, and id is the deterministic
	// tiebreak. Multi-type pools still merge across types at allocator
	// build time.
	bucketsByStateInstanceTp map[machine.State]map[string][]machine.Machine

	// bucketsByClusterState is the per-(cluster, state) bucket
	// pre-sorted by Phase 3's keep-priority order
	// (PricePerHour asc, AssignedReclamationPenaltyDollars desc, ID asc).
	// M27 hot path: replaces a per-call ListByClusterState alloc + sort
	// with a shared-slice read. Mutators MUST NOT modify the returned
	// slice.
	bucketsByClusterState map[machine.ClusterID]map[machine.State][]machine.Machine

	// minPriorityByStateInstanceTp[state][instanceType] is the lowest
	// AssignedPriority across all machines in that bucket. M30.1's
	// Phase 2 short-circuit reads this to skip the candidate-pool
	// walk when no machine could be a preemption victim. Empty buckets
	// are absent from the map (callers treat absent as math.MaxInt32).
	minPriorityByStateInstanceTp map[machine.State]map[string]int32

	// minPriorityByState[state] is the lowest AssignedPriority across
	// all machines in that state, including machines with no instance
	// type. Used by Phase 2's unpinned fallback to short-circuit when
	// the entire state has no preemptable victim. Empty state →
	// absent (caller treats absent as math.MaxInt32).
	minPriorityByState map[machine.State]int32
}

// ListByStateInstanceType returns machines in the given state matching
// the given instance type, pre-sorted by (price ascending, ID
// ascending). Used by Phase 1 to skip the all-inventory walk when a
// Need's selector pins to specific instance type(s).
//
// The returned slice is the snapshot's shared, pre-sorted bucket. The
// caller MUST NOT mutate it (in particular, MUST NOT re-sort it in
// place). Phase 1's allocator copies the prefix it consumes; multi-type
// pool builders concatenate copies of the per-type slices and only sort
// the merged copy. See M11.20.
//
// Returns nil if no machines match.
func (s *Snapshot) ListByStateInstanceType(state machine.State, instanceType string) []machine.Machine {
	byType := s.bucketsByStateInstanceTp[state]
	if byType == nil {
		return nil
	}
	return byType[instanceType]
}

// CountByStateInstanceType returns the number of machines in the
// given (state, instance-type) bucket. O(1) — read of the pre-built
// per-bucket slice.
func (s *Snapshot) CountByStateInstanceType(state machine.State, instanceType string) int {
	byType := s.bucketsByStateInstanceTp[state]
	if byType == nil {
		return 0
	}
	return len(byType[instanceType])
}

// CountByState returns the number of machines in the given state.
// O(1) — read of the pre-built per-state slice.
func (s *Snapshot) CountByState(state machine.State) int {
	return len(s.byState[state])
}

// CountByClusterStateMatching counts machines in (cluster, state) for
// which matches(m) returns true. O(K) where K is the cluster's machines
// in that state — bounded by the cluster's own population, not the
// fleet's. Phase 1 uses this to count Configured machines that satisfy
// a Need's profile (so its deficit calc respects per-Need profile
// fingerprint rather than the cluster's aggregate Configured count).
func (s *Snapshot) CountByClusterStateMatching(cluster machine.ClusterID, state machine.State, matches func(m machine.Machine) bool) int {
	byState := s.bucketsByClusterState[cluster]
	if byState == nil {
		return 0
	}
	bucket := byState[state]
	n := 0
	for i := range bucket {
		if matches(bucket[i]) {
			n++
		}
	}
	return n
}

// Len returns the number of machines.
func (s *Snapshot) Len() int { return len(s.machines) }

// All returns every machine.
func (s *Snapshot) All() []machine.Machine {
	out := make([]machine.Machine, len(s.machines))
	copy(out, s.machines)
	return out
}

// Get returns the machine with the given ID, or ok = false.
func (s *Snapshot) Get(id machine.ID) (machine.Machine, bool) {
	idx, ok := s.byID[id]
	if !ok {
		return machine.Machine{}, false
	}
	return s.machines[idx], true
}

// ListByState returns every machine in the given state.
func (s *Snapshot) ListByState(state machine.State) []machine.Machine {
	ids := s.byState[state]
	out := make([]machine.Machine, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.machines[s.byID[id]])
	}
	return out
}

// ListByClusterState returns the machines for the given cluster in the
// given state.
func (s *Snapshot) ListByClusterState(cluster machine.ClusterID, state machine.State) []machine.Machine {
	byState := s.byClusterState[cluster]
	if byState == nil {
		return nil
	}
	ids := byState[state]
	out := make([]machine.Machine, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.machines[s.byID[id]])
	}
	return out
}

// ClusterIDs returns the set of cluster IDs that own at least one
// machine in any state (Configured / Idle / etc.). M27 fast path —
// avoids the per-cycle walk of every configured machine just to learn
// the set of clusters present.
func (s *Snapshot) ClusterIDs() map[machine.ClusterID]struct{} {
	out := make(map[machine.ClusterID]struct{}, len(s.byClusterState))
	for cl := range s.byClusterState {
		out[cl] = struct{}{}
	}
	return out
}

// SortedClusterStateBucket returns the cluster's machines in the given
// state, pre-sorted by Phase 3's keep-priority order (price asc,
// reclamation_penalty desc, ID asc). Returned slice is the shared
// pre-built bucket; caller MUST NOT mutate. Returns nil if no
// machines match.
func (s *Snapshot) SortedClusterStateBucket(cluster machine.ClusterID, state machine.State) []machine.Machine {
	byState := s.bucketsByClusterState[cluster]
	if byState == nil {
		return nil
	}
	return byState[state]
}

// CountByClusterState returns the count of machines for the cluster in
// the given state.
// MinAssignedPriority returns the lowest AssignedPriority across all
// machines in (state). Empty state → math.MaxInt32. Used by Phase 2's
// unpinned fallback to skip the pool walk when no candidate could
// possibly be preempted by a given preemptor.
func (s *Snapshot) MinAssignedPriority(state machine.State) int32 {
	v, ok := s.minPriorityByState[state]
	if !ok {
		return int32(math.MaxInt32)
	}
	return v
}

// MinAssignedPriorityByInstanceType returns the lowest AssignedPriority
// across machines in (state, instanceType). Empty bucket →
// math.MaxInt32. Phase 2 reads this to short-circuit the per-Need walk
// when a pinned bucket has no preemptable victim.
func (s *Snapshot) MinAssignedPriorityByInstanceType(state machine.State, instanceType string) int32 {
	byType, ok := s.minPriorityByStateInstanceTp[state]
	if !ok {
		return int32(math.MaxInt32)
	}
	v, ok := byType[instanceType]
	if !ok {
		return int32(math.MaxInt32)
	}
	return v
}

func (s *Snapshot) CountByClusterState(cluster machine.ClusterID, state machine.State) int {
	byState := s.byClusterState[cluster]
	if byState == nil {
		return 0
	}
	return len(byState[state])
}
