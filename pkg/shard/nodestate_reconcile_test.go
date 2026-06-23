package shard

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

func newNodeStateTestShard(t *testing.T) *Shard {
	t.Helper()
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	s, err := New(Config{
		ID:               "shard-nodestate-test",
		Epoch:            epoch,
		Provider:         fake.New(fake.Options{}),
		CycleInterval:    50 * time.Millisecond,
		BootstrapTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func configuredMachine(id, cluster string) machine.Machine {
	return machine.Machine{
		ID:      machine.ID(id),
		State:   machine.StateConfigured,
		Cluster: machine.ClusterID(cluster),
		Host:    machine.HostRef{Provider: "fake", Ref: id},
		Profile: machine.Profile{
			InstanceType: "m5.large",
			Zone:         "us-east-1a",
			Resources:    map[string]string{"cpu": "4"},
		},
	}
}

// TestApplyReconciledMachine_NotifiesOnAsyncTerminalConfigured pins the
// ADR-0057 fix. An async (providerkit) provider returns a TransitionAck in
// the transitional state and reaches terminal Configured only out-of-band,
// observed via reconcile — never via the worker's applyTransition. Before
// ADR-0057, applyReconciledMachine updated inventory but emitted no
// NodeStateUpdate, so the operator never learned the node was Configured and
// the workload never scheduled. The reconcile path must now notify.
func TestApplyReconciledMachine_NotifiesOnAsyncTerminalConfigured(t *testing.T) {
	t.Parallel()
	s := newNodeStateTestShard(t)

	// Worker issued Configure to an async provider; the machine sits at
	// Configuring locally while provider.Configure completes out-of-band.
	configuring := configuredMachine("m-async", "cluster-a")
	configuring.State = machine.StateConfiguring
	if err := s.inv.Insert(configuring); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	stream := &captureSessionStream{}
	s.installSession("cluster-a", newOperatorSession("cluster-a", stream))

	// The async provider completed; the next reconcile observes Configured.
	// (Not in pendingActions, so reconcile does not skip it.)
	s.applyReconciledMachine(configuredMachine("m-async", "cluster-a"))

	if got, _ := s.inv.Get("m-async"); got.State != machine.StateConfigured {
		t.Fatalf("inventory state = %v, want Configured", got.State)
	}
	states := stream.nodeStates("m-async")
	if len(states) != 1 || states[0] != pb.MachineState_MACHINE_STATE_CONFIGURED {
		t.Fatalf("NodeStateUpdates for m-async = %v, want exactly [CONFIGURED] "+
			"(ADR-0057: reconcile-observed terminal Configured must reach the operator)", states)
	}
}

// TestResyncNodeState_ReplaysBoundMachinesOnConnect pins the second half of
// ADR-0057: on operator (re)connect the shard replays the current state of
// every machine bound to that cluster, so an operator that connects after the
// shard already learned a node's state catches up. Unbound machines (Idle /
// other clusters) must not be sent.
func TestResyncNodeState_ReplaysBoundMachinesOnConnect(t *testing.T) {
	t.Parallel()
	s := newNodeStateTestShard(t)

	if err := s.inv.Insert(configuredMachine("m-bound", "cluster-a")); err != nil {
		t.Fatalf("Insert bound: %v", err)
	}
	// A machine bound to a different cluster — must not be replayed to cluster-a.
	if err := s.inv.Insert(configuredMachine("m-other", "cluster-b")); err != nil {
		t.Fatalf("Insert other-cluster: %v", err)
	}
	// An unbound Idle machine — carries no cluster, must not be replayed.
	idle := machine.Machine{
		ID:      "m-idle",
		State:   machine.StateIdle,
		Host:    machine.HostRef{Provider: "fake", Ref: "m-idle"},
		Profile: machine.Profile{InstanceType: "m5.large", Zone: "us-east-1a"},
	}
	if err := s.inv.Insert(idle); err != nil {
		t.Fatalf("Insert idle: %v", err)
	}

	stream := &captureSessionStream{}
	s.installSession("cluster-a", newOperatorSession("cluster-a", stream))

	s.resyncNodeState("cluster-a")

	if states := stream.nodeStates("m-bound"); len(states) != 1 || states[0] != pb.MachineState_MACHINE_STATE_CONFIGURED {
		t.Errorf("resync of bound machine = %v, want exactly [CONFIGURED]", states)
	}
	if states := stream.nodeStates("m-other"); len(states) != 0 {
		t.Errorf("resync sent updates for a machine bound to another cluster: %v", states)
	}
	if states := stream.nodeStates("m-idle"); len(states) != 0 {
		t.Errorf("resync sent updates for an unbound Idle machine: %v", states)
	}
}

// TestApplyReconciledMachine_DrainToIdleClearsAssignment pins ADR-0059's
// reconcile half. When an async drain completes — a bound machine observed
// transitioning to terminal Idle — the assignment (priority + penalties +
// gang) must clear WITH the binding. A real provider's Idle view carries no
// Assigned* (they ride only in shard_metadata), so reconcile must NOT
// re-apply the prior bound record's assignment onto the now-unbound Idle
// machine; doing so would leave a drained Idle slot carrying a stale priority
// / penalty bucket (mis-skewing the per-penalty inventory metric and any
// Assigned*-keyed logic until re-bound).
func TestApplyReconciledMachine_DrainToIdleClearsAssignment(t *testing.T) {
	t.Parallel()
	s := newNodeStateTestShard(t)

	// A bound, Draining machine carrying assignment (an async drain in flight).
	draining := configuredMachine("m-drain", "cluster-a")
	draining.State = machine.StateDraining
	draining.AssignedPriority = 100
	draining.AssignedInterruptionPenaltyDollars = 64
	draining.AssignedReclamationPenaltyDollars = 128
	draining.AssignedGroup = "g1"
	if err := s.inv.Insert(draining); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// The provider completes the drain: terminal Idle, binding gone, and — as
	// with every provider view — no Assigned* in the record.
	idle := configuredMachine("m-drain", "cluster-a")
	idle.State = machine.StateIdle
	idle.Cluster = ""
	s.applyReconciledMachine(idle)

	got, err := s.inv.Get("m-drain")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != machine.StateIdle {
		t.Fatalf("state = %v, want Idle", got.State)
	}
	if got.AssignedPriority != 0 || got.AssignedInterruptionPenaltyDollars != 0 ||
		got.AssignedReclamationPenaltyDollars != 0 || got.AssignedGroup != "" {
		t.Errorf("drain-to-Idle kept stale assignment (ADR-0059): priority=%d intPen=%g recPen=%g group=%q — want all cleared",
			got.AssignedPriority, got.AssignedInterruptionPenaltyDollars, got.AssignedReclamationPenaltyDollars, got.AssignedGroup)
	}
}

// TestApplyReconciledMachine_ConfiguringToConfiguredPreservesAssignment is the
// regression guard for ADR-0059's reconcile gate: a bound→bound transition —
// the ADR-0057 async-CONFIGURE completion (Configuring → Configured) — must
// STILL preserve the assignment, because the provider's Configured view
// carries none (shard_metadata is nil'd at ingest) and the worker stamped it
// onto the Configuring record. Clearing it here would zero preemption
// protection fleet-wide on every async configure.
func TestApplyReconciledMachine_ConfiguringToConfiguredPreservesAssignment(t *testing.T) {
	t.Parallel()
	s := newNodeStateTestShard(t)

	configuring := configuredMachine("m-cfg", "cluster-a")
	configuring.State = machine.StateConfiguring
	configuring.AssignedPriority = 1000
	configuring.AssignedInterruptionPenaltyDollars = 256
	configuring.AssignedReclamationPenaltyDollars = 512
	configuring.AssignedGroup = "gang-7"
	if err := s.inv.Insert(configuring); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Provider observes terminal Configured (no Assigned* in its view).
	s.applyReconciledMachine(configuredMachine("m-cfg", "cluster-a"))

	got, err := s.inv.Get("m-cfg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != machine.StateConfigured {
		t.Fatalf("state = %v, want Configured", got.State)
	}
	if got.AssignedPriority != 1000 || got.AssignedInterruptionPenaltyDollars != 256 ||
		got.AssignedReclamationPenaltyDollars != 512 || got.AssignedGroup != "gang-7" {
		t.Errorf("Configuring→Configured dropped assignment (must preserve, ADR-0057): priority=%d intPen=%g recPen=%g group=%q",
			got.AssignedPriority, got.AssignedInterruptionPenaltyDollars, got.AssignedReclamationPenaltyDollars, got.AssignedGroup)
	}
}
