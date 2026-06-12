package coordinator_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
)

// TestAddVoter_ExistingVoterIsIdempotent pins the hashicorp/raft
// semantics ADR-0047's join path relies on: AddVoter of a server that
// is already a voter is a no-op address update, not an error and not
// a duplicate membership entry. The joinLoop runs unconditionally on
// every ordinal-N start, so a re-join after a pod restart hits
// exactly this path.
func TestAddVoter_ExistingVoterIsIdempotent(t *testing.T) {
	leader, ctx, _ := newSingleNodeCoordinator(t)

	followerAddr := freePort(t)
	follower, err := coordinator.New(coordinator.Config{
		NodeID:          "node-2",
		DataDir:         filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress: followerAddr,
		// No Bootstrap: starts with an empty configuration and waits
		// to be voted in, like an ordinal-1 pod.
	})
	if err != nil {
		t.Fatalf("follower New: %v", err)
	}
	t.Cleanup(follower.Close)

	if err := leader.AddVoter("node-2", followerAddr); err != nil {
		t.Fatalf("first AddVoter: %v", err)
	}
	if err := leader.AddVoter("node-2", followerAddr); err != nil {
		t.Fatalf("second AddVoter (re-join) should be idempotent, got: %v", err)
	}

	future := leader.Raft().GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	servers := future.Configuration().Servers
	if len(servers) != 2 {
		t.Fatalf("expected exactly 2 servers after duplicate AddVoter, got %d: %+v", len(servers), servers)
	}

	// The follower should observe the leader once the membership
	// change replicates — the joinLoop's exit condition.
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := follower.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("follower never observed a leader: %v", err)
	}
}
