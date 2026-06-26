package coordinator_test

import (
	"errors"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
)

func TestState_AddRemoveShard(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	if err := s.AddShard(coordinator.ShardEntry{ID: "shard-1", Address: "host:1"}); err != nil {
		t.Fatalf("AddShard: %v", err)
	}
	if err := s.AddShard(coordinator.ShardEntry{ID: "shard-1", Address: "host:1"}); !errors.Is(err, coordinator.ErrShardExists) {
		t.Errorf("expected ErrShardExists on duplicate, got %v", err)
	}
	if err := s.RemoveShard("shard-1"); err != nil {
		t.Errorf("RemoveShard: %v", err)
	}
	if err := s.RemoveShard("shard-1"); !errors.Is(err, coordinator.ErrShardNotFound) {
		t.Errorf("expected ErrShardNotFound, got %v", err)
	}
}

func TestState_BindClusterIdempotent(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	_ = s.AddShard(coordinator.ShardEntry{ID: "a"})
	_ = s.AddShard(coordinator.ShardEntry{ID: "b"})

	if err := s.BindCluster("c1", "a"); err != nil {
		t.Fatalf("first bind: %v", err)
	}
	// Re-binding to same shard is a no-op.
	if err := s.BindCluster("c1", "a"); err != nil {
		t.Errorf("idempotent rebind to same shard failed: %v", err)
	}
	// Re-binding to a different shard fails.
	if err := s.BindCluster("c1", "b"); !errors.Is(err, coordinator.ErrClusterAlreadyBound) {
		t.Errorf("expected ErrClusterAlreadyBound, got %v", err)
	}
}

func TestState_BindCluster_RejectsUnknownShard(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	if err := s.BindCluster("c1", "nope"); !errors.Is(err, coordinator.ErrShardNotFound) {
		t.Errorf("expected ErrShardNotFound, got %v", err)
	}
}

func TestState_AssignDomain(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	_ = s.AddShard(coordinator.ShardEntry{ID: "a"})
	_ = s.AddShard(coordinator.ShardEntry{ID: "b"})

	d := coordinator.DomainKey{Key: "topology.kubernetes.io/rack", Value: "r-1"}
	if err := s.AssignDomain(d, "a"); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	// Re-assigning to same shard is idempotent.
	if err := s.AssignDomain(d, "a"); err != nil {
		t.Errorf("idempotent reassign failed: %v", err)
	}
	// Re-assigning to a different shard fails.
	if err := s.AssignDomain(d, "b"); !errors.Is(err, coordinator.ErrDomainAlreadyAssigned) {
		t.Errorf("expected ErrDomainAlreadyAssigned, got %v", err)
	}

	if got := s.DomainsForShard("a"); len(got) != 1 || got[0] != d {
		t.Errorf("DomainsForShard: got %+v", got)
	}

	s.UnassignDomain(d)
	if _, ok := s.DomainShard(d); ok {
		t.Errorf("expected domain to be unassigned")
	}
}

func TestState_RemoveShard_CleansBindingsAndDomains(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	_ = s.AddShard(coordinator.ShardEntry{ID: "a"})
	_ = s.AddShard(coordinator.ShardEntry{ID: "b"})

	_ = s.BindCluster("c1", "a")
	_ = s.AssignDomain(coordinator.DomainKey{Key: "rack", Value: "r-1"}, "a")

	if err := s.RemoveShard("a"); err != nil {
		t.Fatalf("RemoveShard: %v", err)
	}
	if _, ok := s.ClusterShard("c1"); ok {
		t.Errorf("cluster binding should be removed when its shard is removed")
	}
	if _, ok := s.DomainShard(coordinator.DomainKey{Key: "rack", Value: "r-1"}); ok {
		t.Errorf("domain assignment should be removed when its shard is removed")
	}
}

func TestState_MarkHeartbeat(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	now := time.Now().UTC().Truncate(time.Millisecond)
	_ = s.AddShard(coordinator.ShardEntry{ID: "a"})
	s.MarkHeartbeat("a", now)
	e, _ := s.Shard("a")
	if !e.LastHeartbeat.Equal(now) {
		t.Errorf("LastHeartbeat = %v, want %v", e.LastHeartbeat, now)
	}
	// Heartbeat for unknown shard is silently ignored.
	s.MarkHeartbeat("nope", now)
}
