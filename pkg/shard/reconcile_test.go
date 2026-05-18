package shard

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

// TestApplyReconciledMachine_SkipsPending covers the bigfleet-uber #23
// fix. While a worker is mid-action on a machine (in pendingActions),
// the provider's List view lags the in-flight RPC and reports a
// pre-RPC state — typically Idle while provider.Configure is
// running. Before the fix, reconcile would overwrite local
// Configuring → Idle and the post-Configure transition would fail
// with "invalid state transition: Idle → Configured". The fix:
// reconcile skips machines in pendingActions; the worker's local
// applyTransition is authoritative until execute completes.
func TestApplyReconciledMachine_SkipsPending(t *testing.T) {
	t.Parallel()

	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	s, err := New(Config{
		ID:               "shard-test",
		Epoch:            epoch,
		Provider:         fake.New(fake.Options{}),
		CycleInterval:    50 * time.Millisecond,
		BootstrapTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Seed inventory: machine is Configuring (worker has just done
	// the Idle → Configuring transition and is now running
	// provider.Configure).
	configuring := machine.Machine{
		ID:    "m-1",
		State: machine.StateConfiguring,
		Profile: machine.Profile{
			InstanceType: "m5.large",
			Zone:         "us-east-1a",
			Resources:    map[string]string{"cpu": "4"},
		},
		Cluster: "c-1",
		Host:    machine.HostRef{Provider: "fake", Ref: "m-1"},
	}
	if err := s.inv.Insert(configuring); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Mark in-flight (the executor pool would have done this when the
	// action was enqueued).
	s.pendingMu.Lock()
	s.pendingActions["m-1"] = struct{}{}
	s.pendingMu.Unlock()

	// Reconcile sees the provider's stale view: still Idle, because
	// provider.Configure hasn't ack'd yet.
	stale := configuring
	stale.State = machine.StateIdle
	stale.Cluster = ""
	s.applyReconciledMachine(stale)

	// Expectation: local state is still Configuring (not overwritten).
	got, err := s.inv.Get("m-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != machine.StateConfiguring {
		t.Errorf("local state = %v, want StateConfiguring (reconcile must not overwrite while pending)", got.State)
	}
	if got.Cluster != "c-1" {
		t.Errorf("cluster cleared by reconcile: got %q, want c-1", got.Cluster)
	}

	// After the worker completes, pendingActions clears. The next
	// reconcile is now free to apply provider state — though in
	// practice the worker's post-Configure transition will have
	// already aligned local + provider.
	s.pendingMu.Lock()
	delete(s.pendingActions, "m-1")
	s.pendingMu.Unlock()

	// Simulate the worker having finished post-Configure transition
	// (machine is now Configured locally; provider catches up so
	// reconcile sees Configured too).
	if err := s.inv.Apply(machine.Machine{
		ID:      "m-1",
		State:   machine.StateConfigured,
		Profile: configuring.Profile,
		Cluster: "c-1",
		Host:    configuring.Host,
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Now reconcile with provider view = Configured. State already
	// matches; fast path skips. Sanity check the post-pending
	// behaviour isn't broken.
	reconciled := configuring
	reconciled.State = machine.StateConfigured
	s.applyReconciledMachine(reconciled)
	got, _ = s.inv.Get("m-1")
	if got.State != machine.StateConfigured {
		t.Errorf("post-pending state = %v, want Configured", got.State)
	}
}
