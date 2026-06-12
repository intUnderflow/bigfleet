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
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// readinessService is the grpc_health_v1 service name the chart's
// readinessProbe checks. The default ("") service is the liveness
// signal and stays SERVING for the life of the process.
const readinessService = "bigfleet.v1alpha1.Coordinator"

func runCoordinator(args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", ":7790", "address for the coordinator gRPC service (shard ↔ coordinator chatter)")
	metricsAddr := fs.String("metrics-addr", ":8790", "address for the Prometheus /metrics endpoint (\"0\" disables)")
	raftBind := fs.String("raft-bind", ":7791", "address for the Raft transport to bind on")
	raftAdvertise := fs.String("raft-advertise", "", "address advertised to Raft peers (defaults to --raft-bind)")
	nodeID := fs.String("id", "node-1", "stable per-replica identifier")
	dataDir := fs.String("data-dir", "./coord-data", "directory for Raft logs / snapshots / BoltDB stores")
	bootstrap := fs.Bool("bootstrap", false, "bootstrap a fresh single-node Raft cluster when the data dir has no existing state. With --join-addr set, only StatefulSet ordinal 0 honours this (ADR-0047); without it, use only on the very first replica.")
	joinAddr := fs.String("join-addr", "", "host:port of the coordinator gRPC Service used to form a multi-replica Raft cluster (M75, ADR-0047). When set, --id must end in a StatefulSet ordinal: only ordinal 0 honours --bootstrap, and every replica runs a membership reconciler that asks the leader at this address to AddVoter it until it is a voter at its current advertise address (retried with backoff; idempotent across restarts; also heals the address change a pod restart causes). Empty keeps the single-node dev behaviour.")
	snapshotExportDir := fs.String("snapshot-export-dir", "", "if set, the leader periodically writes Raft snapshots here for DR (paper §10.8). Conventionally a path mounted from durable object storage; empty disables export.")
	snapshotExportInterval := fs.Duration("snapshot-export-interval", 0, "interval between snapshot exports (default 5m; ignored if --snapshot-export-dir is empty)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errFlagParse, err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// ADR-0047 quorum formation: the chart passes identical args to
	// every replica (no shell in the distroless image to vary them),
	// so the binary splits on the StatefulSet ordinal in --id: only
	// ordinal 0 may bootstrap, while the join/membership reconciler
	// runs on every ordinal (it also heals the pod-IP churn a restart
	// causes — see pkg/coordinator/join.go). Without --join-addr
	// nothing changes — dev and all-in-one runs keep plain --bootstrap.
	effectiveBootstrap := *bootstrap
	if *joinAddr != "" {
		ordinal, err := parseStatefulSetOrdinal(*nodeID)
		if err != nil {
			return fmt.Errorf("--join-addr requires --id with a StatefulSet ordinal suffix (got %q): %w", *nodeID, err)
		}
		if ordinal != 0 {
			effectiveBootstrap = false
		}
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("data-dir: %w", err)
	}
	c, err := coordinator.New(coordinator.Config{
		NodeID:                 *nodeID,
		DataDir:                filepath.Clean(*dataDir),
		RaftBindAddress:        *raftBind,
		AdvertiseAddress:       *raftAdvertise,
		Bootstrap:              effectiveBootstrap,
		JoinAddress:            *joinAddr,
		SnapshotExportDir:      *snapshotExportDir,
		SnapshotExportInterval: *snapshotExportInterval,
		Logger:                 logger,
	})
	if err != nil {
		return err
	}

	srv := coordinator.NewGRPCServer(c)
	gsrv := grpc.NewServer(grpcutil.ServerOptions()...)
	pb.RegisterCoordinatorServer(gsrv, srv)

	// M75 probes: standard grpc_health_v1 on the same gRPC port.
	// Liveness uses the default ("") service — SERVING from the moment
	// the server is up, never coupled to Raft so quorum loss doesn't
	// restart-loop the replicas that could re-form it. Readiness uses
	// the named Coordinator service — SERVING only once this replica
	// observes a leader (itself or a peer), which gates a joiner until
	// it is a voter and paces rolling upgrades on actual recovery.
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(gsrv, healthSrv)
	healthSrv.SetServingStatus(readinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			st := healthpb.HealthCheckResponse_NOT_SERVING
			if c.LeaderAddress() != "" {
				st = healthpb.HealthCheckResponse_SERVING
			}
			healthSrv.SetServingStatus(readinessService, st)
		}
	}()

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
