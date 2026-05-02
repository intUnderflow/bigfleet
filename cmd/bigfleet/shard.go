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

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// seedFakeInventory mints n synthetic idle machines into the in-process
// fake provider AND seeds them into the shard's inventory so Phase 1
// can pick from them on the first cycle. Used by the scale-test
// harness to populate the shard with a realistic-shape pool without a
// real provider.
//
// Spread: round-robin across 5 instance types × 3 zones × bare-metal
// capacity type. Profile fingerprints are stable so the per-fingerprint
// pool cache (M11.16) sees real diversity instead of one giant bucket.
func seedFakeInventory(prov *fake.Provider, sh *shard.Shard, n int, logger *slog.Logger) {
	types := []string{"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge"}
	zones := []string{"zone-a", "zone-b", "zone-c"}
	resources := map[string]map[string]string{
		"a3-highgpu-8g":  {"nvidia.com/gpu": "8"},
		"m6i.large":      {"cpu": "2", "memory": "8Gi"},
		"c6i.4xlarge":    {"cpu": "16", "memory": "32Gi"},
		"n2-standard-32": {"cpu": "32", "memory": "128Gi"},
		"r6i.xlarge":     {"cpu": "4", "memory": "32Gi"},
	}
	logger.Info("seeding fake inventory", "count", n)
	for i := 0; i < n; i++ {
		t := types[i%len(types)]
		z := zones[i%len(zones)]
		profile := machine.Profile{
			InstanceType: t,
			Zone:         z,
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    resources[t],
		}
		id := machine.ID("seed-" + strconv.Itoa(i))
		prov.AddIdle(id, profile, machine.CapacityTypeBareMetal, 0, 0)
		_ = sh.SeedInventory(machine.Machine{
			ID:      id,
			State:   machine.StateIdle,
			Profile: profile,
		})
	}
	logger.Info("seed complete", "count", n)
}

// runShard runs the shard controller. The in-process fake provider
// lives at pkg/provider/fake and is used here for laptop / kind / dev
// installs only; production deployments dial an out-of-tree provider
// over gRPC via pkg/provider/grpcadapter (see provider-author-guide).
func runShard(args []string) error {
	fs := flag.NewFlagSet("shard", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", ":7780", "address to listen on for the Shard.Session gRPC service")
	metricsAddr := fs.String("metrics-addr", ":8780", "address for the Prometheus /metrics endpoint (\"0\" disables)")
	shardID := fs.String("id", "shard-0", "this shard's stable identifier")
	dataDir := fs.String("data-dir", "./data", "directory for shard-local persistent state (epoch counter)")
	seedMachines := fs.Int("seed-machines", 0, "scaletest: pre-seed the in-process fake provider with N synthetic idle machines spread across instance types and zones; 0 disables")
	maxActionsPerCycle := fs.Int("max-actions-per-cycle", 0, "cap total decision actions executed per cycle so a ramp burst doesn't blow past the cycle SLO; 0 = unlimited (production default). Surplus actions roll into the next cycle.")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errFlagParse, err)
	}
	if *listen == "" {
		return errors.New("--listen is required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("data-dir: %w", err)
	}
	epoch, err := fencing.LoadEpoch(filepath.Join(*dataDir, "epoch"))
	if err != nil {
		return err
	}

	// In-memory fake provider. M5 swaps this for a real gRPC client
	// adapter once an out-of-tree provider is available.
	prov := fake.New(fake.Options{InstantTransitions: true})

	sh, err := shard.New(shard.Config{
		ID:                 *shardID,
		Epoch:              epoch,
		Provider:           prov,
		Logger:             logger,
		MaxActionsPerCycle: *maxActionsPerCycle,
	})
	if err != nil {
		return err
	}

	if *seedMachines > 0 {
		seedFakeInventory(prov, sh, *seedMachines, logger)
	}

	srv := grpc.NewServer()
	pb.RegisterShardServer(srv, sh)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.Info("listening", "addr", lis.Addr().String(), "shard_id", *shardID, "epoch", epoch.Value())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelSig, sigs := signalContext()
	defer cancelSig()

	errCh := make(chan error, 3)
	go func() { errCh <- sh.Run(ctx) }()
	go func() { errCh <- srv.Serve(lis) }()
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
	srv.GracefulStop()
	return nil
}
