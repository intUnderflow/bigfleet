package coordinator_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
)

// freePort returns an available local TCP port.
func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// newSingleNodeCoordinator brings up a fresh single-node Raft cluster
// and waits for leadership. Returns the coordinator and a cleanup
// function. No t.Parallel because hashicorp/raft + go-metrics share
// package-level state that the race detector flags across multiple
// instances.
func newSingleNodeCoordinator(t *testing.T) (*coordinator.Coordinator, context.Context, context.CancelFunc) {
	t.Helper()
	c, err := coordinator.New(coordinator.Config{
		NodeID:          "node-1",
		DataDir:         filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress: freePort(t),
		Bootstrap:       true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	if err := c.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if !c.IsLeader() {
		t.Fatalf("expected to be leader")
	}
	return c, ctx, cancel
}

func TestCoordinator_BootstrapAndApply(t *testing.T) {
	c, ctx, _ := newSingleNodeCoordinator(t)

	if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{
		ID: "shard-a", Address: "host:1",
	})); err != nil {
		t.Fatalf("AddShard: %v", err)
	}
	if err := c.Apply(ctx, coordinator.MakeBindClusterCommand("cluster-x", "shard-a")); err != nil {
		t.Fatalf("BindCluster: %v", err)
	}
	if err := c.Apply(ctx, coordinator.MakeAssignDomainCommand(
		coordinator.DomainKey{Key: "rack", Value: "r-1"}, "shard-a",
	)); err != nil {
		t.Fatalf("AssignDomain: %v", err)
	}
	if err := c.Apply(ctx, coordinator.MakeUpsertProviderCommand(coordinator.ProviderEntry{
		Name: "fake-east", Address: "127.0.0.1:9000", Region: "us-east-1",
	})); err != nil {
		t.Fatalf("UpsertProvider: %v", err)
	}

	st := c.State()
	if shards := st.Shards(); len(shards) != 1 || shards[0].ID != "shard-a" {
		t.Errorf("Shards: %+v", shards)
	}
	if got, ok := st.ClusterShard("cluster-x"); !ok || got != "shard-a" {
		t.Errorf("ClusterShard = %s ok=%v", got, ok)
	}
	if got, ok := st.DomainShard(coordinator.DomainKey{Key: "rack", Value: "r-1"}); !ok || got != "shard-a" {
		t.Errorf("DomainShard = %s ok=%v", got, ok)
	}
	if got := st.Providers(); len(got) != 1 {
		t.Errorf("Providers len = %d, want 1", len(got))
	}
}

func TestCoordinator_FSMRejectsDuplicateShard(t *testing.T) {
	c, ctx, _ := newSingleNodeCoordinator(t)
	if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{ID: "a"})); err != nil {
		t.Fatalf("first AddShard: %v", err)
	}
	if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{ID: "a"})); err == nil {
		t.Fatalf("expected duplicate AddShard to fail")
	}
}

func TestCoordinator_NextSequenceMonotonic(t *testing.T) {
	t.Parallel() // pure FSM, no Raft, safe to parallelise.
	fsm := coordinator.NewFSM()
	prev := int64(0)
	for i := 0; i < 100; i++ {
		v := fsm.NextSequence()
		if v <= prev {
			t.Fatalf("NextSequence not monotonic: prev=%d v=%d", prev, v)
		}
		prev = v
	}
}
