package coordinator_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// TestRebalancer_EmitsTransferOnShortfall: two shards register, one
// has a shortfall, the other has FreeMachines, the rebalancer emits
// a TransferOwnership instruction within one tick.
func TestRebalancer_EmitsTransferOnShortfall(t *testing.T) {
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
	defer cancel()

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	if err := c.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// Register two shards.
	if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{
		ID: "shard-shortfall", Address: "host:1",
	})); err != nil {
		t.Fatalf("AddShard A: %v", err)
	}
	if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{
		ID: "shard-donor", Address: "host:2",
	})); err != nil {
		t.Fatalf("AddShard B: %v", err)
	}

	srv := coordinator.NewGRPCServer(c)

	// Inject soft state directly as if reports had arrived: donor has
	// free machines; shortfall shard has a shortfall.
	if _, err := srv.ReportShard(ctx, &pb.ShardReport{
		ShardId: "shard-donor", Cycle: 1,
		Summary: &pb.ShardSummary{TotalMachines: 100, FreeMachines: 30},
	}); err != nil {
		t.Fatalf("inject donor: %v", err)
	}
	if _, err := srv.ReportShard(ctx, &pb.ShardReport{
		ShardId: "shard-shortfall", Cycle: 1,
		Summary: &pb.ShardSummary{TotalMachines: 100, FreeMachines: 0},
		Shortfalls: []*pb.Shortfall{{
			Priority: 1_000_000, Count: 10, AgeCycles: 2,
		}},
	}); err != nil {
		t.Fatalf("inject shortfall: %v", err)
	}

	rb := coordinator.NewRebalancer(c, srv, coordinator.RebalancerConfig{
		Interval: 50 * time.Millisecond,
	})
	go func() { _ = rb.Run(ctx) }()

	// Wait for the rebalancer to enqueue a TransferOwnership for the
	// shortfall shard.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.PendingForShard("shard-shortfall") > 0 &&
			srv.PendingForShard("shard-donor") > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := srv.PendingForShard("shard-shortfall"); got == 0 {
		t.Errorf("rebalancer did not enqueue an instruction for shard-shortfall")
	}
	if got := srv.PendingForShard("shard-donor"); got == 0 {
		t.Errorf("rebalancer did not enqueue an instruction for shard-donor")
	}
}

// TestRebalancer_NoDonor_NoOp: shortfall on the only shard with
// reports → nothing to do.
func TestRebalancer_NoDonor_NoOp(t *testing.T) {
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
	defer cancel()
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	if err := c.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{
		ID: "shard-only", Address: "host:1",
	})); err != nil {
		t.Fatalf("AddShard: %v", err)
	}
	srv := coordinator.NewGRPCServer(c)
	if _, err := srv.ReportShard(ctx, &pb.ShardReport{
		ShardId: "shard-only", Cycle: 1,
		Summary:    &pb.ShardSummary{TotalMachines: 50, FreeMachines: 0},
		Shortfalls: []*pb.Shortfall{{Priority: 1_000_000, Count: 5}},
	}); err != nil {
		t.Fatalf("inject report: %v", err)
	}

	rb := coordinator.NewRebalancer(c, srv, coordinator.RebalancerConfig{
		Interval: 50 * time.Millisecond,
	})
	go func() { _ = rb.Run(ctx) }()

	// Give the rebalancer a few ticks; it shouldn't emit anything.
	time.Sleep(300 * time.Millisecond)
	if got := srv.PendingForShard("shard-only"); got != 0 {
		t.Errorf("PendingForShard = %d, want 0 (no donor → no instruction)", got)
	}
}
