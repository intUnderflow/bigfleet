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

func TestState_QuotaSetGet(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	k := coordinator.QuotaKey{Provider: "aws", Region: "us-east-1"}
	s.SetQuota(k, map[coordinator.ShardID]int32{"a": 100, "b": 200})
	got, ok := s.Quota(k)
	if !ok {
		t.Fatal("expected quota to exist")
	}
	if got.PerShard["a"] != 100 || got.PerShard["b"] != 200 {
		t.Errorf("quota mismatch: %+v", got)
	}
	// Mutating returned copy should not affect state.
	got.PerShard["a"] = 999
	again, _ := s.Quota(k)
	if again.PerShard["a"] != 100 {
		t.Errorf("returned quota was not isolated; mutation leaked")
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

func TestState_ProvidersRoundtrip(t *testing.T) {
	t.Parallel()
	s := coordinator.NewState()
	if err := s.UpsertProvider(coordinator.ProviderEntry{Name: "aws-east", Address: "host:1", Region: "us-east-1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertProvider(coordinator.ProviderEntry{Name: "gcp-west", Address: "host:2", Region: "us-west-1"}); err != nil {
		t.Fatal(err)
	}
	got := s.Providers()
	if len(got) != 2 {
		t.Errorf("Providers len = %d, want 2", len(got))
	}
	if got[0].Name != "aws-east" || got[1].Name != "gcp-west" {
		t.Errorf("Providers ordering: %+v", got)
	}
}
