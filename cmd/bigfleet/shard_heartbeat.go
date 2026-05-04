package main

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// runCoordinatorHeartbeat periodically pushes ReportShard to the
// coordinator. The first successful heartbeat registers this shard's
// (id, address) tuple in coordinator state via Raft; subsequent
// heartbeats refresh LastHeartbeat. Loss of the coordinator does not
// stall the shard's hot path — failed heartbeats are logged and the
// goroutine keeps trying on the next tick. This is deliberate: per
// the "static stability is non-negotiable" hard rule, the shard must
// keep running cycles even with BigFleet's coordinator entirely down.
//
// Returns when ctx is cancelled. Safe to call with empty coordinatorAddr;
// in that case the loop exits immediately so single-shard / dev runs
// without a coordinator continue to work unchanged.
func runCoordinatorHeartbeat(
	ctx context.Context,
	coordinatorAddr string,
	shardID, advertiseAddr string,
	epoch *fencing.Epoch,
	interval time.Duration,
	logger *slog.Logger,
) {
	if coordinatorAddr == "" {
		return
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	conn, err := grpc.NewClient(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("coordinator dial failed; heartbeats disabled", "addr", coordinatorAddr, "err", err)
		return
	}
	defer func() { _ = conn.Close() }()
	cli := pb.NewCoordinatorClient(conn)

	var cycle int64
	fire := func() {
		cycle++
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := cli.ReportShard(rctx, &pb.ShardReport{
			ShardId:            shardID,
			ShardAddress:       advertiseAddr,
			Cycle:              cycle,
			ShardEpoch:         epoch.Value(),
			TimestampUnixNanos: time.Now().UnixNano(),
		}); err != nil {
			logger.Warn("coordinator heartbeat failed", "err", err, "cycle", cycle)
			return
		}
		if cycle == 1 {
			logger.Info("coordinator registration accepted", "shard_id", shardID, "advertise_addr", advertiseAddr)
		}
	}

	// Fire once immediately so registration lands without waiting a
	// full interval. Then on the ticker.
	fire()

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fire()
		}
	}
}
