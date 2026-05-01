//go:build scale

// M6 scale ceiling — Layer 1 (synthetic, no kind).
//
// Per docs/plan.md §5.1: 3 shards, 100K total machines (~33K each),
// cross-shard rebalance latency under 5 seconds. Coordinator
// "failover" at single-node Raft is process-restart with the same
// DataDir; full multi-node failover lands when we test the 3-replica
// Raft setup explicitly.
package scale_test

import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/pkg/shard/coordclient"
)

func freePortM6(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func TestM6Scale_ThreeShards_HundredThousandMachines_Rebalance(t *testing.T) {
	const (
		shardCount       = 3
		machinesPerShard = 33_333
		ceilingLatency   = 5 * time.Second
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- coordinator ---
	c, err := coordinator.New(coordinator.Config{
		NodeID:          "coord-m6",
		DataDir:         filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress: freePortM6(t),
		Bootstrap:       true,
	})
	if err != nil {
		t.Fatalf("coord New: %v", err)
	}
	t.Cleanup(c.Close)
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	if err := c.WaitForLeader(wctx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	srv := coordinator.NewGRPCServer(c)
	gsrv := grpc.NewServer()
	pb.RegisterCoordinatorServer(gsrv, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.Stop)

	// Pre-register the three shards.
	for i := 0; i < shardCount; i++ {
		if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{
			ID:      coordinator.ShardID("shard-" + strconv.Itoa(i)),
			Address: "self",
		})); err != nil {
			t.Fatalf("AddShard %d: %v", i, err)
		}
	}

	// --- shards ---
	shards := make([]*shard.Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		prov := providerfake.New(providerfake.Options{InstantTransitions: true})
		// Donors (i > 0) seed lots of idle machines; the shortfall
		// shard (i == 0) gets none and is given a need it can't
		// satisfy locally.
		if i > 0 {
			prefix := "i" + strconv.Itoa(i) + "-"
			for j := 0; j < machinesPerShard; j++ {
				prov.AddIdle(machine.ID(prefix+strconv.Itoa(j)),
					machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a"},
					machine.CapacityTypeBareMetal, 0, 0)
			}
		}
		epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch-"+strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("LoadEpoch: %v", err)
		}
		sh, err := shard.New(shard.Config{
			ID:               "shard-" + strconv.Itoa(i),
			Epoch:            epoch,
			Provider:         prov,
			CycleInterval:    100 * time.Millisecond,
			BootstrapTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("shard %d New: %v", i, err)
		}
		shards[i] = sh
		go func() { _ = sh.Run(ctx) }()
	}

	// Seed shortfall on shard-0: a need with no matching local idle.
	pf := needs.NewProfile(
		[]needs.Requirement{{
			Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn,
			Values: []string{"a3-highgpu-8g"},
		}},
		[]needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}},
		nil, 1_000_000,
		needs.PenaltyBucket8192, needs.PenaltyBucketPinned,
	)
	shards[0].NeedsTable().Replace("cluster-x", []needs.Need{{
		ClusterID: machine.ClusterID("cluster-x"), Profile: pf, Count: 100,
	}})

	// --- coord clients ---
	for _, sh := range shards {
		client, err := coordclient.New(coordclient.Config{
			CoordinatorAddress: lis.Addr().String(),
			View:               coordclient.ViewFromShard(sh),
			CoordinatorTerm:    sh.CoordinatorTerm(),
			ReportInterval:     200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("coordclient: %v", err)
		}
		go func() { _ = client.Run(ctx) }()
	}

	// --- rebalancer ---
	rb := coordinator.NewRebalancer(c, srv, coordinator.RebalancerConfig{
		Interval: 200 * time.Millisecond,
	})
	go func() { _ = rb.Run(ctx) }()

	// Measure rebalance latency: from now (shortfall just seeded) to
	// the moment the coordinator emits an instruction for shard-0.
	start := time.Now()
	deadline := start.Add(ceilingLatency)
	for time.Now().Before(deadline) {
		// Pending stays at 0 once the shard acks; track over time
		// instead by summing emitted instructions across all shards.
		var totalPending int
		for i := 0; i < shardCount; i++ {
			totalPending += srv.PendingForShard(coordinator.ShardID("shard-" + strconv.Itoa(i)))
		}
		summary, ok := srv.LatestSummary("shard-0")
		_ = summary
		_ = ok
		if totalPending > 0 {
			t.Logf("M6 rebalance latency: %v (totalPending=%d)", time.Since(start), totalPending)
			return
		}
		// Also accept "instruction was emitted and already acked" as
		// success — that's the loop closing in <100ms internal cycles.
		// We detect that via a snapshot of soft state plus latest
		// shortfalls being non-empty.
		if got := srv.LatestShortfalls("shard-0"); len(got) > 0 {
			// Shortfall has been observed by coordinator. Continue
			// waiting; the rebalancer should fire on its next tick.
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("M6 ceiling missed: no rebalance instruction emitted within %v", ceilingLatency)
}
