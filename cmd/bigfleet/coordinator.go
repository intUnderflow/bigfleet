package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

func runCoordinator(args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", ":7790", "address for the coordinator gRPC service (shard ↔ coordinator chatter)")
	metricsAddr := fs.String("metrics-addr", ":8790", "address for the Prometheus /metrics endpoint (\"0\" disables)")
	raftBind := fs.String("raft-bind", ":7791", "address for the Raft transport to bind on")
	raftAdvertise := fs.String("raft-advertise", "", "address advertised to Raft peers (defaults to --raft-bind)")
	nodeID := fs.String("id", "node-1", "stable per-replica identifier")
	dataDir := fs.String("data-dir", "./coord-data", "directory for Raft logs / snapshots / BoltDB stores")
	bootstrap := fs.Bool("bootstrap", false, "bootstrap a fresh single-node Raft cluster on first run (use only on the very first replica)")
	rebalanceInterval := fs.Duration("rebalance-interval", 0, "rebalance loop interval (default 5s; 0 = disabled)")
	snapshotExportDir := fs.String("snapshot-export-dir", "", "if set, the leader periodically writes Raft snapshots here for DR (paper §10.8). Conventionally a path mounted from durable object storage; empty disables export.")
	snapshotExportInterval := fs.Duration("snapshot-export-interval", 0, "interval between snapshot exports (default 5m; ignored if --snapshot-export-dir is empty)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errFlagParse, err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("data-dir: %w", err)
	}
	c, err := coordinator.New(coordinator.Config{
		NodeID:                 *nodeID,
		DataDir:                filepath.Clean(*dataDir),
		RaftBindAddress:        *raftBind,
		AdvertiseAddress:       *raftAdvertise,
		Bootstrap:              *bootstrap,
		SnapshotExportDir:      *snapshotExportDir,
		SnapshotExportInterval: *snapshotExportInterval,
		Logger:                 logger,
	})
	if err != nil {
		return err
	}

	srv := coordinator.NewGRPCServer(c)
	gsrv := grpc.NewServer()
	pb.RegisterCoordinatorServer(gsrv, srv)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.Info("coordinator listening", "grpc_addr", lis.Addr().String(),
		"raft_bind", *raftBind, "node_id", *nodeID, "data_dir", *dataDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelSig, sigs := signalContext()
	defer cancelSig()

	errCh := make(chan error, 4)
	go func() { errCh <- c.Run(ctx) }()
	go func() { errCh <- gsrv.Serve(lis) }()
	if *metricsAddr != "0" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		metricsSrv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		logger.Info("metrics serving", "addr", *metricsAddr)
		go func() { errCh <- metricsSrv.ListenAndServe() }()
		defer func() { _ = metricsSrv.Shutdown(context.Background()) }()
	}

	if *rebalanceInterval != 0 || true {
		rb := coordinator.NewRebalancer(c, srv, coordinator.RebalancerConfig{
			Interval: *rebalanceInterval, Logger: logger,
		})
		go func() { errCh <- rb.Run(ctx) }()
	}

	select {
	case <-sigs:
		logger.Info("signal received, shutting down")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("component exited", "err", err)
		}
	}
	cancel()
	gsrv.GracefulStop()
	return nil
}
