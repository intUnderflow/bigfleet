package shard

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

// newDrainTestShard builds a shard with one Configured machine for
// cluster-a, no operator session, and a log capture buffer.
func newDrainTestShard(t *testing.T) (*Shard, *bytes.Buffer) {
	t.Helper()
	profile := machine.Profile{InstanceType: "a3-highgpu-8g", Resources: map[string]string{"nvidia.com/gpu": "8"}}
	prov := fake.New(fake.Options{InstantTransitions: true})
	prov.AddConfigured("m1", profile, machine.CapacityTypeBareMetal, 0, 0, "cluster-a", 100, 0, 0)

	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	var buf bytes.Buffer
	sh, err := New(Config{
		ID:       "shard-drain-test",
		Epoch:    epoch,
		Provider: prov,
		Logger:   slog.New(slog.NewTextHandler(&buf, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := sh.SeedInventory(machine.Machine{
		ID:      "m1",
		State:   machine.StateConfigured,
		Cluster: "cluster-a",
		Host:    machine.HostRef{Provider: "fake", Ref: "m1"},
		Profile: profile,
	}); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}
	return sh, &buf
}

// TestExecuteDrain_Reclaim_NoSessionFallback pins the M69 fallback: with
// no operator session, a Reclaim still drains via the provider
// (Configured → Draining → Idle) but the skipped cordon/PDB/evict pass
// (ADR-0009) is logged distinctly so operators can alert on ungraceful
// reclaims.
func TestExecuteDrain_Reclaim_NoSessionFallback(t *testing.T) {
	t.Parallel()
	sh, buf := newDrainTestShard(t)

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
	if got.State != machine.StateIdle {
		t.Errorf("state = %s, want Idle", got.State)
	}
	if got.Cluster != "" {
		t.Errorf("cluster = %q, want cleared", got.Cluster)
	}
	if !strings.Contains(buf.String(), "reclaim fallback") {
		t.Errorf("expected distinct 'reclaim fallback' log line, got:\n%s", buf.String())
	}
}

// TestExecuteDrain_Preempt_NoSessionStaysSilent pins that the historic
// Preempt fallback is unchanged by M69: no session → drain proceeds with
// no reclaim-fallback log (the alertable line is reserved for the
// voluntary path).
func TestExecuteDrain_Preempt_NoSessionStaysSilent(t *testing.T) {
	t.Parallel()
	sh, buf := newDrainTestShard(t)

	err := sh.execute(context.Background(), decision.Action{
		Kind:              decision.ActionKindPreempt,
		MachineID:         "m1",
		Cluster:           "cluster-a",
		GracePeriod:       decision.DrainGrace(1_000_000, 100),
		PreemptorPriority: 1_000_000,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	got, err := sh.inv.Get("m1")
	if err != nil {
		t.Fatalf("inventory get: %v", err)
	}
	if got.State != machine.StateIdle {
		t.Errorf("state = %s, want Idle", got.State)
	}
	if strings.Contains(buf.String(), "reclaim fallback") {
		t.Errorf("Preempt fallback must not emit the reclaim-fallback line, got:\n%s", buf.String())
	}
}

// newAsyncDrainTestShard builds a shard whose fake provider drains
// ASYNCHRONOUSLY (DrainStaged): Drain returns a transitional Draining ack and
// the binding clears only when CompleteStaged advances it to terminal Idle —
// the providerkit contract the synchronous default masks. m1 is Configured
// for cluster-a and carries assignment so the drain-completion clear is
// observable.
func newAsyncDrainTestShard(t *testing.T) (*Shard, *fake.Provider) {
	t.Helper()
	profile := machine.Profile{InstanceType: "a3-highgpu-8g", Resources: map[string]string{"nvidia.com/gpu": "8"}}
	prov := fake.New(fake.Options{InstantTransitions: true, DrainStaged: true})
	prov.AddConfigured("m1", profile, machine.CapacityTypeBareMetal, 0, 0, "cluster-a", 100, 64, 128)

	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := New(Config{
		ID:       "shard-async-drain-test",
		Epoch:    epoch,
		Provider: prov,
		Logger:   slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := sh.SeedInventory(machine.Machine{
		ID:                                 "m1",
		State:                              machine.StateConfigured,
		Cluster:                            "cluster-a",
		Host:                               machine.HostRef{Provider: "fake", Ref: "m1"},
		Profile:                            profile,
		AssignedPriority:                   100,
		AssignedInterruptionPenaltyDollars: 64,
		AssignedReclamationPenaltyDollars:  128,
	}); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}
	return sh, prov
}

// TestExecuteDrain_AsyncProvider_DrainsViaReconcile pins ADR-0059 end to end.
// Against an async provider (Drain returns a transitional Draining ack),
// executeDrain must NOT apply the terminal binding-clear — doing so sets
// Draining-without-a-cluster and trips machine.Invariant, the bug that froze
// every Reclaim/Preempt against a real providerkit provider. The machine
// stays Draining-WITH-cluster; the terminal Idle is finalized via reconcile,
// which clears the binding and the assignment.
func TestExecuteDrain_AsyncProvider_DrainsViaReconcile(t *testing.T) {
	t.Parallel()
	sh, prov := newAsyncDrainTestShard(t)
	ctx := context.Background()

	if err := sh.execute(ctx, decision.Action{
		Kind:        decision.ActionKindReclaim,
		MachineID:   "m1",
		Cluster:     "cluster-a",
		GracePeriod: decision.ReclaimGrace,
	}); err != nil {
		t.Fatalf("async drain must not error (ADR-0059: the terminal clear must not land on the Draining ack): %v", err)
	}

	// Dispatched, not yet complete: Draining, WITH its cluster (invariant holds).
	got, err := sh.inv.Get("m1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != machine.StateDraining {
		t.Fatalf("state = %s, want Draining (async drain in flight)", got.State)
	}
	if got.Cluster != "cluster-a" {
		t.Errorf("cluster = %q, want preserved while Draining (Draining must have a cluster)", got.Cluster)
	}

	// The provider's teardown completes; the next reconcile observes Idle and
	// finalizes the drain.
	if !prov.CompleteStaged("m1") {
		t.Fatalf("CompleteStaged did not advance the staged drain")
	}
	drained, err := prov.Get(ctx, "m1")
	if err != nil {
		t.Fatalf("provider Get: %v", err)
	}
	sh.applyReconciledMachine(drained)

	got, err = sh.inv.Get("m1")
	if err != nil {
		t.Fatalf("get after reconcile: %v", err)
	}
	if got.State != machine.StateIdle {
		t.Errorf("state = %s, want Idle (drain finalized via reconcile)", got.State)
	}
	if got.Cluster != "" {
		t.Errorf("cluster = %q, want cleared on drain completion", got.Cluster)
	}
	if got.AssignedPriority != 0 || got.AssignedInterruptionPenaltyDollars != 0 || got.AssignedReclamationPenaltyDollars != 0 {
		t.Errorf("assignment not cleared on drain-to-Idle: priority=%d intPen=%g recPen=%g",
			got.AssignedPriority, got.AssignedInterruptionPenaltyDollars, got.AssignedReclamationPenaltyDollars)
	}
}
