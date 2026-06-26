// Package coordinator implements BigFleet's Tier 1 — the global
// coordinator described in the BigFleet paper §6.
//
// The coordinator does NOT make provisioning decisions. It owns:
//
//   - Shard membership (the set of running shards, their addresses).
//   - Cluster → shard map (set on a cluster's first contact, permanent
//     unless a shard splits).
//   - Topology-domain → shard map (per plan §0.1 A; ~100K entries at
//     full fleet scale, not 100M).
//
// State changes go through Raft so the three coordinator replicas stay
// consistent. Shard reports / shortfalls are *soft state* held only on
// the leader — they're recomputed from fresh reports after a failover.
//
// Static stability (CLAUDE.md hard rule): nothing in pkg/shard imports
// pkg/coordinator. Shards keep operating against their existing
// allocations when the coordinator is down; only cross-shard
// rebalancing pauses.
package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ShardID is a stable identifier for a registered shard.
type ShardID string

// ClusterID identifies a Kubernetes cluster from BigFleet's POV.
// Mirrors machine.ClusterID at a different package boundary so the
// coordinator doesn't depend on pkg/machine (kept lightweight on
// purpose).
type ClusterID string

// DomainKey identifies a topology domain (rack, zone-slice, etc.).
// Concretely a (label-key, label-value) pair.
type DomainKey struct {
	Key   string
	Value string
}

// String returns a stable canonical form.
func (d DomainKey) String() string { return d.Key + "=" + d.Value }

// State is the coordinator's durable, Raft-replicated state. All access
// must go through methods on *State for thread safety.
type State struct {
	mu sync.RWMutex

	// Shards is the membership table. Key = ShardID.
	shards map[ShardID]ShardEntry

	// ClusterToShard binds clusters to shards. Set permanently on first
	// contact; only mutated by shard splits / merges.
	clusterToShard map[ClusterID]ShardID

	// DomainToShard assigns topology domains to shards. ~100K entries
	// at 100M-node scale per plan §0.1 A.
	domainToShard map[DomainKey]ShardID
}

// ShardEntry is the per-shard record persisted in coordinator state.
// Soft state (latest summary, shortfalls) lives in pkg/coordinator's
// in-memory leader-only structures, not here.
type ShardEntry struct {
	ID            ShardID
	Address       string // gRPC host:port
	RegisteredAt  time.Time
	LastHeartbeat time.Time // last successful ReportShard
}

// NewState constructs an empty State.
func NewState() *State {
	return &State{
		shards:         make(map[ShardID]ShardEntry),
		clusterToShard: make(map[ClusterID]ShardID),
		domainToShard:  make(map[DomainKey]ShardID),
	}
}

// Errors surfaced to callers.
var (
	ErrShardExists           = errors.New("coordinator: shard already registered")
	ErrShardNotFound         = errors.New("coordinator: shard not found")
	ErrClusterAlreadyBound   = errors.New("coordinator: cluster already bound to a different shard")
	ErrDomainAlreadyAssigned = errors.New("coordinator: domain already assigned to another shard")
)

// AddShard registers a new shard. Returns ErrShardExists if a shard
// with the same ID is already registered.
func (s *State) AddShard(e ShardEntry) error {
	if e.ID == "" {
		return errors.New("coordinator: ShardEntry.ID required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[e.ID]; ok {
		return fmt.Errorf("%w: %s", ErrShardExists, e.ID)
	}
	if e.RegisteredAt.IsZero() {
		e.RegisteredAt = time.Now().UTC()
	}
	s.shards[e.ID] = e
	return nil
}

// RemoveShard deletes a shard. Returns ErrShardNotFound if unknown.
func (s *State) RemoveShard(id ShardID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[id]; !ok {
		return fmt.Errorf("%w: %s", ErrShardNotFound, id)
	}
	delete(s.shards, id)
	// Clean up any clusters / domains pointing at the gone shard.
	for c, sh := range s.clusterToShard {
		if sh == id {
			delete(s.clusterToShard, c)
		}
	}
	for d, sh := range s.domainToShard {
		if sh == id {
			delete(s.domainToShard, d)
		}
	}
	return nil
}

// MarkHeartbeat updates the LastHeartbeat field on a shard. No-op if
// the shard doesn't exist (heartbeats from unknown shards are ignored
// by callers).
func (s *State) MarkHeartbeat(id ShardID, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.shards[id]; ok {
		e.LastHeartbeat = t
		s.shards[id] = e
	}
}

// Shards returns a snapshot of all registered shards, sorted by ID
// for determinism.
func (s *State) Shards() []ShardEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ShardEntry, 0, len(s.shards))
	for _, e := range s.shards {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Shard returns one shard's entry, ok=false if not found.
func (s *State) Shard(id ShardID) (ShardEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.shards[id]
	return e, ok
}

// BindCluster assigns a cluster to a shard. The first-contact contract
// (paper §16) — once bound, a cluster never moves except via shard
// split/merge. ErrClusterAlreadyBound if the cluster is already bound
// to a *different* shard; binding to the same shard is idempotent.
func (s *State) BindCluster(c ClusterID, sh ShardID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[sh]; !ok {
		return fmt.Errorf("%w: %s", ErrShardNotFound, sh)
	}
	if cur, ok := s.clusterToShard[c]; ok {
		if cur == sh {
			return nil
		}
		return fmt.Errorf("%w: %s currently bound to %s, asked %s",
			ErrClusterAlreadyBound, c, cur, sh)
	}
	s.clusterToShard[c] = sh
	return nil
}

// ClusterShard returns the shard a cluster is bound to. ok=false if
// the cluster has not been bound yet.
func (s *State) ClusterShard(c ClusterID) (ShardID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sh, ok := s.clusterToShard[c]
	return sh, ok
}

// AssignDomain assigns a topology domain to a shard. If the domain is
// already assigned to a different shard, returns ErrDomainAlreadyAssigned.
// Re-assigning to the same shard is idempotent.
func (s *State) AssignDomain(d DomainKey, sh ShardID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards[sh]; !ok {
		return fmt.Errorf("%w: %s", ErrShardNotFound, sh)
	}
	if cur, ok := s.domainToShard[d]; ok && cur != sh {
		return fmt.Errorf("%w: %s currently on %s, asked %s",
			ErrDomainAlreadyAssigned, d, cur, sh)
	}
	s.domainToShard[d] = sh
	return nil
}

// UnassignDomain removes a domain assignment.
func (s *State) UnassignDomain(d DomainKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.domainToShard, d)
}

// DomainShard returns the shard a domain is assigned to.
func (s *State) DomainShard(d DomainKey) (ShardID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sh, ok := s.domainToShard[d]
	return sh, ok
}

// DomainsForShard returns all domains assigned to the given shard,
// sorted for determinism.
func (s *State) DomainsForShard(sh ShardID) []DomainKey {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DomainKey, 0)
	for d, owner := range s.domainToShard {
		if owner == sh {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Value < out[j].Value
	})
	return out
}
