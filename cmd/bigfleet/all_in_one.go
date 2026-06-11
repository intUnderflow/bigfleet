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
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// runAllInOne runs the coordinator and one shard in the same process,
// against an in-process fake provider. The intended use is local dev
// (laptop kind cluster, the quickstart walkthrough) — it gives a
// single command that brings the whole BigFleet system up so the
// operator on the target cluster has something to dial.
//
// Production deployments run coordinator and shard as separate
// binaries on separate Pods; see deploy/helm/bigfleet.
func runAllInOne(args []string) error {
	fs := flag.NewFlagSet("all-in-one", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	shardListen := fs.String("shard-listen", ":7780", "address for the Shard.Session gRPC service")
	coordListen := fs.String("coordinator-listen", ":7790", "address for the coordinator gRPC service")
	raftBind := fs.String("raft-bind", ":7791", "address for the coordinator's Raft transport")
	shardMetrics := fs.String("shard-metrics-addr", ":8780", "shard /metrics endpoint (\"0\" disables)")
	coordMetrics := fs.String("coordinator-metrics-addr", ":8790", "coordinator /metrics endpoint (\"0\" disables)")
	dataDir := fs.String("data-dir", "./bigfleet-data", "directory for shard + coordinator persistent state")
	shardID := fs.String("shard-id", "shard-0", "this shard's stable identifier")
	nodeID := fs.String("coordinator-id", "node-1", "coordinator's node identifier")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errFlagParse, err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("data-dir: %w", err)
	}

	// ---- coordinator ----
	coordDir := filepath.Join(*dataDir, "coord")
	if err := os.MkdirAll(coordDir, 0o755); err != nil {
		return fmt.Errorf("coord data-dir: %w", err)
	}
	// Bind ":7791" is not Raft-advertisable; for the single-node
	// all-in-one process there are no peers, but the Raft library
	// still validates the address at startup. Pin to loopback.
	raftAdvertise := *raftBind
	if raftAdvertise == "" || raftAdvertise == ":7791" || raftAdvertise == "0.0.0.0:7791" {
		raftAdvertise = "127.0.0.1:7791"
	}
	c, err := coordinator.New(coordinator.Config{
		NodeID:           *nodeID,
		DataDir:          coordDir,
		RaftBindAddress:  *raftBind,
		AdvertiseAddress: raftAdvertise,
		Bootstrap:        true,
		Logger:           logger.With("component", "coordinator"),
	})
	if err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	coordSrv := coordinator.NewGRPCServer(c)
	coordGRPC := grpc.NewServer(grpcutil.ServerOptions()...)
	pb.RegisterCoordinatorServer(coordGRPC, coordSrv)
	coordLis, err := net.Listen("tcp", *coordListen)
	if err != nil {
		return fmt.Errorf("coordinator listen: %w", err)
	}

	// ---- shard ----
	shardDir := filepath.Join(*dataDir, "shard")
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return fmt.Errorf("shard data-dir: %w", err)
	}
	epoch, err := fencing.LoadEpoch(filepath.Join(shardDir, "epoch"))
	if err != nil {
		return fmt.Errorf("shard epoch: %w", err)
	}
	// The all-in-one is the demo: it keeps the in-process fake on
	// purpose. Dialing a real provider (--provider-addr, M71) is the
	// `shard` subcommand's concern.
	prov := fake.New(fake.Options{InstantTransitions: true})
	// Seed a small idle inventory so the quickstart sees demand
	// satisfied from the first cycle. Each pool covers one common
	// dev profile shape; extend by editing this function.
	seedDevInventory(prov)
	sh, err := shard.New(shard.Config{
		ID:       *shardID,
		Epoch:    epoch,
		Provider: prov,
		Logger:   logger.With("component", "shard"),
		// ADR-0046 safety rails at their production defaults — every
		// deployment boundary ships rails-on; only the library zero
		// value (sim, unit tests) leaves them off.
		ReclaimCapFraction: shard.DefaultReclaimCapFraction,
		EmptyRollupGuard:   true,
	})
	if err != nil {
		return fmt.Errorf("shard: %w", err)
	}
	shardGRPC := grpc.NewServer(grpcutil.ServerOptions()...)
	pb.RegisterShardServer(shardGRPC, sh)
	shardLis, err := net.Listen("tcp", *shardListen)
	if err != nil {
		return fmt.Errorf("shard listen: %w", err)
	}

	logger.Info("all-in-one ready",
		"coordinator", coordLis.Addr().String(),
		"shard", shardLis.Addr().String(),
		"raft", *raftBind,
		"data_dir", *dataDir,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelSig, sigs := signalContext()
	defer cancelSig()

	errCh := make(chan error, 6)
	go func() { errCh <- c.Run(ctx) }()
	go func() { errCh <- coordGRPC.Serve(coordLis) }()
	go func() { errCh <- sh.Run(ctx) }()
	go func() { errCh <- shardGRPC.Serve(shardLis) }()

	if *coordMetrics != "0" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{Addr: *coordMetrics, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		logger.Info("coordinator metrics serving", "addr", *coordMetrics)
		go func() { errCh <- srv.ListenAndServe() }()
		defer func() { _ = srv.Shutdown(context.Background()) }()
	}
	if *shardMetrics != "0" && *shardMetrics != *coordMetrics {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{Addr: *shardMetrics, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		logger.Info("shard metrics serving", "addr", *shardMetrics)
		go func() { errCh <- srv.ListenAndServe() }()
		defer func() { _ = srv.Shutdown(context.Background()) }()
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
	coordGRPC.GracefulStop()
	shardGRPC.GracefulStop()
	return nil
}

// seedDevInventory mints a small idle pool for the dev / quickstart
// experience: 8 a3-highgpu-8g machines (the GPU shape used in the
// quickstart CR example) and 8 m6i.large machines (a generic CPU
// shape). All zero-cost, zero-interruption-probability so Phase 1
// always picks them first.
func seedDevInventory(prov *fake.Provider) {
	for i := 0; i < 8; i++ {
		prov.AddIdle(
			machine.ID("dev-gpu-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: "a3-highgpu-8g",
				Zone:         "dev-zone-a",
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    map[string]string{"nvidia.com/gpu": "8"},
			},
			machine.CapacityTypeBareMetal, 0, 0,
		)
		prov.AddIdle(
			machine.ID("dev-cpu-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: "m6i.large",
				Zone:         "dev-zone-a",
				CapacityType: machine.CapacityTypeOnDemand,
				Resources:    map[string]string{"cpu": "2", "memory": "8Gi"},
			},
			machine.CapacityTypeOnDemand, 0.10, 0,
		)
	}
}
