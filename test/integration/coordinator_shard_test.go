//go:build integration

// Package integration_test holds in-process integration tests that
// wire two or more BigFleet components together without a real
// Kubernetes cluster.
package integration_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/pkg/shard/coordclient"
)

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

// TestEndToEnd_TwoShardsSelfRegister: M12 contract — two shard
// processes start with no out-of-band registration and each appears
// in coordinator state with its own ID and AdvertiseAddress after one
// heartbeat round. Locks in that the coordclient stamps
// ShardAddress on every report and that the coordinator's
// auto-AddShard (M12.2) creates two distinct registry entries.
func TestEndToEnd_TwoShardsSelfRegister(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := coordinator.New(coordinator.Config{
		NodeID:          "node-1",
		DataDir:         filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress: freePort(t),
		Bootstrap:       true,
	})
	if err != nil {
		t.Fatalf("coord New: %v", err)
	}
	t.Cleanup(c.Close)
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	if err := c.WaitForLeader(waitCtx); err != nil {
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

	// Two shards, no AddShard pre-applied. Each picks a distinct
	// AdvertiseAddress that we'll verify lands in coordinator state.
	starts := []struct {
		id        string
		advertise string
	}{
		{"shard-0", "bigfleet-shard-0.bigfleet-shard-headless.default.svc:7780"},
		{"shard-1", "bigfleet-shard-1.bigfleet-shard-headless.default.svc:7780"},
	}
	for _, s := range starts {
		prov := providerfake.New(providerfake.Options{InstantTransitions: true})
		epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch-"+s.id))
		if err != nil {
			t.Fatalf("LoadEpoch %s: %v", s.id, err)
		}
		sh, err := shard.New(shard.Config{
			ID: s.id, Epoch: epoch, Provider: prov,
			CycleInterval: 50 * time.Millisecond, BootstrapTimeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("shard New %s: %v", s.id, err)
		}
		go func() { _ = sh.Run(ctx) }()
		cli, err := coordclient.New(coordclient.Config{
			CoordinatorAddress: lis.Addr().String(),
			AdvertiseAddress:   s.advertise,
			View:               coordclient.ViewFromShard(sh),
			CoordinatorTerm:    sh.CoordinatorTerm(),
			ReportInterval:     50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("coordclient %s: %v", s.id, err)
		}
		go func() { _ = cli.Run(ctx) }()
	}

	// Within one report interval round-trip both shards should be
	// visible in coordinator state. Allow up to 2s for the FSM apply
	// + heartbeat round-trip on slow CI.
	deadline := time.Now().Add(2 * time.Second)
	var sawBoth bool
	for time.Now().Before(deadline) {
		e0, ok0 := c.State().Shard("shard-0")
		e1, ok1 := c.State().Shard("shard-1")
		if ok0 && ok1 {
			if e0.Address != starts[0].advertise {
				t.Errorf("shard-0 Address = %q, want %q", e0.Address, starts[0].advertise)
			}
			if e1.Address != starts[1].advertise {
				t.Errorf("shard-1 Address = %q, want %q", e1.Address, starts[1].advertise)
			}
			sawBoth = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawBoth {
		t.Fatalf("did not see both shards self-register within 2s")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
