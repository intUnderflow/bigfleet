//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// TestCoordinator_ThreeNodeQuorum_JoinAndFailover is the M75 /
// ADR-0047 contract for the 3-replica chart: ordinal 0 bootstraps,
// ordinals 1 and 2 start with empty state and JOIN by asking the
// leader to AddVoter them (the previous behaviour — three independent
// single-node clusters — was the production-readiness arc-5 finding).
// Then the leader is killed and the remaining two must elect a new
// leader and commit a write: the actual point of running three.
func TestCoordinator_ThreeNodeQuorum_JoinAndFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// node-0: bootstraps, and serves the admin gRPC surface the
	// joiners dial (in the chart this address is the headless
	// Service; attempts retry until they land on the leader).
	node0Ctx, killNode0 := context.WithCancel(ctx)
	defer killNode0()
	c0, err := coordinator.New(coordinator.Config{
		NodeID:          "coord-0",
		DataDir:         filepath.Join(t.TempDir(), "raft-0"),
		RaftBindAddress: freePort(t),
		Bootstrap:       true,
	})
	if err != nil {
		t.Fatalf("node-0 New: %v", err)
	}
	go func() { _ = c0.Run(node0Ctx) }()
	t.Cleanup(c0.Close)

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	if err := c0.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("node-0 WaitForLeader: %v", err)
	}

	gsrv := grpc.NewServer()
	pb.RegisterCoordinatorServer(gsrv, coordinator.NewGRPCServer(c0))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.Stop)

	// nodes 1 and 2: no bootstrap, empty data dirs, join via the
	// leader's gRPC address — the ordinal>0 path.
	followers := make([]*coordinator.Coordinator, 0, 2)
	for i := 1; i <= 2; i++ {
		c, err := coordinator.New(coordinator.Config{
			NodeID:          fmt.Sprintf("coord-%d", i),
			DataDir:         filepath.Join(t.TempDir(), fmt.Sprintf("raft-%d", i)),
			RaftBindAddress: freePort(t),
			JoinAddress:     lis.Addr().String(),
		})
		if err != nil {
			t.Fatalf("node-%d New: %v", i, err)
		}
		go func() { _ = c.Run(ctx) }()
		t.Cleanup(c.Close)
		followers = append(followers, c)
	}

	// Quorum formation: the leader's configuration must converge on
	// exactly 3 voters.
	deadline := time.Now().Add(20 * time.Second)
	for {
		future := c0.Raft().GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("GetConfiguration: %v", err)
		}
		if n := len(future.Configuration().Servers); n == 3 {
			break
		}
		if time.Now().After(deadline) {
			future := c0.Raft().GetConfiguration()
			t.Fatalf("3-voter quorum never formed; configuration: %+v", future.Configuration().Servers)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// A write through the original leader, so failover has state to
	// preserve.
	applyCtx, applyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer applyCancel()
	if err := c0.Apply(applyCtx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{ID: "shard-pre", Address: "host:1"})); err != nil {
		t.Fatalf("Apply via node-0: %v", err)
	}

	// Kill the leader. Close() also closes the Raft transport
	// listener, so peers see a dead node, not a hung one.
	killNode0()
	gsrv.Stop()
	c0.Close()

	// The surviving two are a majority of three: a new leader must
	// emerge.
	var newLeader *coordinator.Coordinator
	deadline = time.Now().Add(20 * time.Second)
	for newLeader == nil {
		for _, c := range followers {
			if c.IsLeader() {
				newLeader = c
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no new leader elected after killing node-0")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// And it must commit writes — quorum is 2/3, satisfied by the
	// survivors — while still holding the pre-failover state.
	applyCtx2, applyCancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer applyCancel2()
	if err := newLeader.Apply(applyCtx2, coordinator.MakeAddShardCommand(coordinator.ShardEntry{ID: "shard-post", Address: "host:2"})); err != nil {
		t.Fatalf("Apply via new leader: %v", err)
	}
	if _, ok := newLeader.State().Shard("shard-pre"); !ok {
		t.Errorf("pre-failover write lost after leader change")
	}
	if _, ok := newLeader.State().Shard("shard-post"); !ok {
		t.Errorf("post-failover write not visible in new leader's state")
	}
}

// TestCoordinator_RejoinAfterAddressChange_HealsConfiguration pins the
// reconciler half of ADR-0047's join loop: a replica that restarts on
// a NEW address (what a Kubernetes pod restart does to the resolved
// pod IP the raft TCP transport advertises) re-joins and the leader's
// configuration is rewritten to the new address — without it, the
// cluster keeps dialing the dead one forever.
func TestCoordinator_RejoinAfterAddressChange_HealsConfiguration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leader, err := coordinator.New(coordinator.Config{
		NodeID:          "coord-0",
		DataDir:         filepath.Join(t.TempDir(), "raft-0"),
		RaftBindAddress: freePort(t),
		Bootstrap:       true,
	})
	if err != nil {
		t.Fatalf("leader New: %v", err)
	}
	go func() { _ = leader.Run(ctx) }()
	t.Cleanup(leader.Close)
	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	if err := leader.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	gsrv := grpc.NewServer()
	pb.RegisterCoordinatorServer(gsrv, coordinator.NewGRPCServer(leader))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.Stop)

	// First life of coord-1: join at address A.
	followerDir := filepath.Join(t.TempDir(), "raft-1")
	addrA := freePort(t)
	followerCtx, stopFollower := context.WithCancel(ctx)
	follower, err := coordinator.New(coordinator.Config{
		NodeID:          "coord-1",
		DataDir:         followerDir,
		RaftBindAddress: addrA,
		JoinAddress:     lis.Addr().String(),
	})
	if err != nil {
		t.Fatalf("follower New: %v", err)
	}
	go func() { _ = follower.Run(followerCtx) }()

	waitForServerAddress(t, leader, "coord-1", addrA)

	// Restart on address B with the SAME data dir — the pod-restart
	// shape: identity and state survive, the address doesn't.
	stopFollower()
	follower.Close()
	addrB := freePort(t)
	follower2, err := coordinator.New(coordinator.Config{
		NodeID:          "coord-1",
		DataDir:         followerDir,
		RaftBindAddress: addrB,
		JoinAddress:     lis.Addr().String(),
	})
	if err != nil {
		t.Fatalf("follower restart New: %v", err)
	}
	go func() { _ = follower2.Run(ctx) }()
	t.Cleanup(follower2.Close)

	waitForServerAddress(t, leader, "coord-1", addrB)
}

// waitForServerAddress polls the leader's configuration until nodeID
// appears at wantAddr.
func waitForServerAddress(t *testing.T, leader *coordinator.Coordinator, nodeID, wantAddr string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		future := leader.Raft().GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("GetConfiguration: %v", err)
		}
		for _, s := range future.Configuration().Servers {
			if string(s.ID) == nodeID && string(s.Address) == wantAddr {
				return
			}
		}
		if time.Now().After(deadline) {
			future := leader.Raft().GetConfiguration()
			t.Fatalf("%s never reached address %s; configuration: %+v", nodeID, wantAddr, future.Configuration().Servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
