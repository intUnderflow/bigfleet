package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// runShard runs the shard controller. M3 deliberately uses an in-memory
// fake provider (no out-of-tree provider gRPC adapter exists yet); M5
// wires the real adapter and removes the fake from this binary's
// production path. Until then the fake is the only way to run the shard
// for local development.
func runShard(args []string) error {
	fs := flag.NewFlagSet("shard", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", ":7780", "address to listen on for the Shard.Session gRPC service")
	shardID := fs.String("id", "shard-0", "this shard's stable identifier")
	dataDir := fs.String("data-dir", "./data", "directory for shard-local persistent state (epoch counter)")
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
		ID:       *shardID,
		Epoch:    epoch,
		Provider: prov,
		Logger:   logger,
	})
	if err != nil {
		return err
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

	errCh := make(chan error, 2)
	go func() { errCh <- sh.Run(ctx) }()
	go func() { errCh <- srv.Serve(lis) }()

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
