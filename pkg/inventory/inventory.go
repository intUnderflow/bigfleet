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
	"sort"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Inventory is a thread-safe collection of machines indexed by ID,
// (cluster, state), and (state, instance-type). All readers receive
// defensive copies; writes go through Apply for state-machine
// validation.
//
// The (state, instance-type) index is the load-bearing one for Phase 1
// at scale: real Needs almost always carry a
// `node.kubernetes.io/instance-type In [...]` selector, and matching
// against the index turns the per-Need candidate scan from O(N) over
// total inventory into O(K) over the matching instance type's bucket.
type Inventory struct {
	mu                sync.RWMutex
	byID              map[machine.ID]machine.Machine
	byState           map[machine.State]map[machine.ID]struct{}
	byClusterState    map[machine.ClusterID]map[machine.State]map[machine.ID]struct{}
	byStateInstanceTp map[machine.State]map[string]map[machine.ID]struct{}
}

// New returns an empty inventory.
func New() *Inventory {
	return &Inventory{
		byID:              make(map[machine.ID]machine.Machine),
		byState:           make(map[machine.State]map[machine.ID]struct{}),
		byClusterState:    make(map[machine.ClusterID]map[machine.State]map[machine.ID]struct{}),
		byStateInstanceTp: make(map[machine.State]map[string]map[machine.ID]struct{}),
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
	i.indexAdd(m)
	return nil
}

// Apply replaces the existing machine with the given record. Validates
// the new state, the structural invariant, and (if both old and new
// states are known) the state machine transition. Apply is the only
// mutation entry point that updates indexes.
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
	i.indexRemove(old)
	i.byID[m.ID] = m
	i.indexAdd(m)
	return nil
}

// Remove drops the machine from the inventory.
func (i *Inventory) Remove(id machine.ID) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	old, exists := i.byID[id]
	if !exists {
		return fmt.Errorf("inventory: %w: %s", ErrNotFound, id)
	}
	delete(i.byID, id)
	i.indexRemove(old)
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

// ListByState returns all machines currently in the given state, sorted
// by ID for determinism.
func (i *Inventory) ListByState(state machine.State) []machine.Machine {
	i.mu.RLock()
	defer i.mu.RUnlock()
	ids := i.byState[state]
	if len(ids) == 0 {
		return nil
	}
	out := make([]machine.Machine, 0, len(ids))
	for id := range ids {
		out = append(out, i.byID[id])
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}

// ListByClusterState returns all machines for the given cluster currently
// in the given state, sorted by ID.
func (i *Inventory) ListByClusterState(cluster machine.ClusterID, state machine.State) []machine.Machine {
	i.mu.RLock()
	defer i.mu.RUnlock()
	byState := i.byClusterState[cluster]
	if byState == nil {
		return nil
	}
	ids := byState[state]
	if len(ids) == 0 {
		return nil
	}
	out := make([]machine.Machine, 0, len(ids))
	for id := range ids {
		out = append(out, i.byID[id])
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}

// Len returns the total number of machines.
func (i *Inventory) Len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.byID)
}

// CountByState returns the count of machines in the given state.
func (i *Inventory) CountByState(state machine.State) int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.byState[state])
}

// Snapshot returns an immutable point-in-time view of the inventory.
// Callers may walk the returned Snapshot without further locking.
func (i *Inventory) Snapshot() *Snapshot {
	i.mu.RLock()
	defer i.mu.RUnlock()
	machines := make([]machine.Machine, 0, len(i.byID))
	byID := make(map[machine.ID]int, len(i.byID))
	byState := make(map[machine.State][]machine.ID, len(i.byState))
	byClusterState := make(map[machine.ClusterID]map[machine.State][]machine.ID, len(i.byClusterState))
	byStateInstanceTp := make(map[machine.State]map[string][]machine.ID, len(i.byStateInstanceTp))

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
	return &Snapshot{
		machines:          machines,
		byID:              byID,
		byState:           byState,
		byClusterState:    byClusterState,
		byStateInstanceTp: byStateInstanceTp,
	}
}

func (i *Inventory) indexAdd(m machine.Machine) {
	if i.byState[m.State] == nil {
		i.byState[m.State] = make(map[machine.ID]struct{})
	}
	i.byState[m.State][m.ID] = struct{}{}

	if m.Cluster != "" {
		byState, ok := i.byClusterState[m.Cluster]
		if !ok {
			byState = make(map[machine.State]map[machine.ID]struct{})
			i.byClusterState[m.Cluster] = byState
		}
		if byState[m.State] == nil {
			byState[m.State] = make(map[machine.ID]struct{})
		}
		byState[m.State][m.ID] = struct{}{}
	}

	if m.Profile.InstanceType != "" {
		byType, ok := i.byStateInstanceTp[m.State]
		if !ok {
			byType = make(map[string]map[machine.ID]struct{})
			i.byStateInstanceTp[m.State] = byType
		}
		if byType[m.Profile.InstanceType] == nil {
			byType[m.Profile.InstanceType] = make(map[machine.ID]struct{})
		}
		byType[m.Profile.InstanceType][m.ID] = struct{}{}
	}
}

func (i *Inventory) indexRemove(m machine.Machine) {
	if states := i.byState[m.State]; states != nil {
		delete(states, m.ID)
		if len(states) == 0 {
			delete(i.byState, m.State)
		}
	}
	if m.Cluster != "" {
		if byState := i.byClusterState[m.Cluster]; byState != nil {
			if ids := byState[m.State]; ids != nil {
				delete(ids, m.ID)
				if len(ids) == 0 {
					delete(byState, m.State)
				}
			}
			if len(byState) == 0 {
				delete(i.byClusterState, m.Cluster)
			}
		}
	}
	if m.Profile.InstanceType != "" {
		if byType := i.byStateInstanceTp[m.State]; byType != nil {
			if ids := byType[m.Profile.InstanceType]; ids != nil {
				delete(ids, m.ID)
				if len(ids) == 0 {
					delete(byType, m.Profile.InstanceType)
				}
			}
			if len(byType) == 0 {
				delete(i.byStateInstanceTp, m.State)
			}
		}
	}
}

// Snapshot is a read-only view of the inventory captured at one instant.
// All accessors return slices the caller may mutate.
type Snapshot struct {
	machines          []machine.Machine
	byID              map[machine.ID]int
	byState           map[machine.State][]machine.ID
	byClusterState    map[machine.ClusterID]map[machine.State][]machine.ID
	byStateInstanceTp map[machine.State]map[string][]machine.ID
}

// ListByStateInstanceType returns machines in the given state matching
// the given instance type. Used by Phase 1 to skip the all-inventory
// walk when a Need's selector pins to specific instance type(s).
// Returns nil if no machines match.
func (s *Snapshot) ListByStateInstanceType(state machine.State, instanceType string) []machine.Machine {
	byType := s.byStateInstanceTp[state]
	if byType == nil {
		return nil
	}
	ids := byType[instanceType]
	if len(ids) == 0 {
		return nil
	}
	out := make([]machine.Machine, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.machines[s.byID[id]])
	}
	return out
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

// CountByClusterState returns the count of machines for the cluster in
// the given state.
func (s *Snapshot) CountByClusterState(cluster machine.ClusterID, state machine.State) int {
	byState := s.byClusterState[cluster]
	if byState == nil {
		return 0
	}
	return len(byState[state])
}
