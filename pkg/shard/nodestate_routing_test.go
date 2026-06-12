package shard

import (
	"context"
	"sync"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// captureSessionStream is a minimal pb.Shard_SessionServer fake that
// records every frame the shard sends. Only Send is implemented — the
// tests below drive the shard directly, never the Session RPC loop.
type captureSessionStream struct {
	pb.Shard_SessionServer
	mu   sync.Mutex
	sent []*pb.ShardMessage
}

func (c *captureSessionStream) Send(m *pb.ShardMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, m)
	return nil
}

func (c *captureSessionStream) nodeStates(machineID string) []pb.MachineState {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []pb.MachineState
	for _, m := range c.sent {
		if u := m.GetNodeStateUpdate(); u != nil && u.GetMachineId() == machineID {
			out = append(out, u.GetState())
		}
	}
	return out
}

// TestExecuteDrain_TerminalUpdateRoutesToPreviousCluster pins the M68b
// fix from the philosophy-conformance audit (state-machine-ledger
// lens): the post-Drain transition's mut clears Machine.Cluster BEFORE
// notifyNodeState runs, so the terminal Draining→Idle frame — the one
// the operator's UpcomingNode GC maps to Drained and deletes on — could
// never route to the cluster's session. applyTransition now captures
// the binding before the mutation and routes the update to the
// previous cluster.
func TestExecuteDrain_TerminalUpdateRoutesToPreviousCluster(t *testing.T) {
	t.Parallel()
	sh, _ := newDrainTestShard(t)
	stream := &captureSessionStream{}
	sess := newOperatorSession("cluster-a", stream)
	sh.installSession("cluster-a", sess)

	err := sh.execute(context.Background(), decision.Action{
		Kind:        decision.ActionKindReclaim,
		MachineID:   "m1",
		Cluster:     "cluster-a",
		GracePeriod: decision.ReclaimGrace,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	got, err := sh.inv.Get("m1")
	if err != nil {
		t.Fatalf("inventory get: %v", err)
	}
	if got.State != machine.StateIdle || got.Cluster != "" {
		t.Fatalf("machine after drain = (%s, cluster %q), want (Idle, unbound)", got.State, got.Cluster)
	}

	states := stream.nodeStates("m1")
	if len(states) < 2 {
		t.Fatalf("NodeStateUpdates for m1 = %v, want Draining followed by the terminal Idle", states)
	}
	if states[0] != pb.MachineState_MACHINE_STATE_DRAINING {
		t.Errorf("first update = %v, want DRAINING", states[0])
	}
	if last := states[len(states)-1]; last != pb.MachineState_MACHINE_STATE_IDLE {
		t.Errorf("terminal update = %v, want IDLE (operator maps Draining→Idle to Drained and GCs the UpcomingNode)", last)
	}
}
