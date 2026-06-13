package shard

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

// newDeleteTestShard builds a shard with one Idle on-demand machine
// seeded in both the fake provider and the inventory — the shape an
// M73 release acts on.
func newDeleteTestShard(t *testing.T) (*Shard, *fake.Provider) {
	t.Helper()
	profile := machine.Profile{
		InstanceType: "m6i.xlarge",
		CapacityType: machine.CapacityTypeOnDemand,
		Resources:    map[string]string{"cpu": "4"},
	}
	prov := fake.New(fake.Options{InstantTransitions: true})
	prov.AddIdle("m1", profile, machine.CapacityTypeOnDemand, 1.0, 0.05)

	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := New(Config{
		ID:       "shard-delete-test",
		Epoch:    epoch,
		Provider: prov,
		Logger:   slog.New(slog.NewTextHandler(discardWriter{}, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := sh.SeedInventory(machine.Machine{
		ID:           "m1",
		State:        machine.StateIdle,
		Host:         machine.HostRef{Provider: "fake", Ref: "m1"},
		Profile:      profile,
		PricePerHour: 1.0,
	}); err != nil {
		t.Fatalf("SeedInventory: %v", err)
	}
	return sh, prov
}

func deleteAction() decision.Action {
	return decision.Action{
		Kind:      decision.ActionKindDelete,
		MachineID: "m1",
		Reason:    "phase3.release",
	}
}

// TestExecuteDelete_WalksIdleToSpeculative: the §7/§8 release — Idle →
// Deleting → Speculative via provider.Delete; the host is released
// entirely (the record survives as a quota slot, paying nothing), on
// both the shard's and the provider's side.
func TestExecuteDelete_WalksIdleToSpeculative(t *testing.T) {
	t.Parallel()
	sh, prov := newDeleteTestShard(t)

	if err := sh.execute(context.Background(), deleteAction()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	got, err := sh.inv.Get("m1")
	if err != nil {
		t.Fatalf("inventory get: %v", err)
	}
	if got.State != machine.StateSpeculative {
		t.Errorf("state = %s, want Speculative", got.State)
	}
	if !got.Host.Empty() {
		t.Errorf("host = %+v, want released entirely (§7 Delete)", got.Host)
	}
	pm, err := prov.Get(context.Background(), "m1")
	if err != nil {
		t.Fatalf("provider get: %v", err)
	}
	if pm.State != machine.StateSpeculative {
		t.Errorf("provider state = %s, want Speculative", pm.State)
	}
}

// A duplicate dispatch after the release completed is a no-op, not an
// error — the action's intent already holds.
func TestExecuteDelete_IdempotentOnRetry(t *testing.T) {
	t.Parallel()
	sh, _ := newDeleteTestShard(t)
	ctx := context.Background()

	if err := sh.execute(ctx, deleteAction()); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if err := sh.execute(ctx, deleteAction()); err != nil {
		t.Fatalf("second execute (idempotent retry): %v", err)
	}
	got, _ := sh.inv.Get("m1")
	if got.State != machine.StateSpeculative {
		t.Errorf("state after retry = %s, want Speculative (untouched)", got.State)
	}
}

// A Delete against a machine that is no longer Idle (a competing
// Bootstrap claimed it between decide and execute) is a no-op; the
// bound machine must never be deleted out from under its cluster.
func TestExecuteDelete_NonIdleIsNoOp(t *testing.T) {
	t.Parallel()
	sh, _ := newDeleteTestShard(t)
	if err := sh.applyTransition("m1", machine.StateConfiguring, func(m *machine.Machine) {
		m.Cluster = "cluster-a"
	}); err != nil {
		t.Fatalf("applyTransition: %v", err)
	}

	if err := sh.execute(context.Background(), deleteAction()); err != nil {
		t.Fatalf("execute: %v", err)
	}
	got, _ := sh.inv.Get("m1")
	if got.State != machine.StateConfiguring {
		t.Errorf("state = %s, want Configuring (delete must not touch a claimed machine)", got.State)
	}
}

// A provider-side Delete failure marks the machine Failed with the
// error recorded — same posture as Create/Configure/Drain failures.
// This includes ErrNotSupported: the policy never emits Delete for
// fixed capacity, so a rejection means the provider's CapacityType
// declaration and its Delete support disagree.
func TestExecuteDelete_ProviderFailureMarksFailed(t *testing.T) {
	t.Parallel()
	sh, prov := newDeleteTestShard(t)
	injected := errors.New("synthetic delete failure")
	prov.FailNext("m1", machine.StateSpeculative, injected)

	err := sh.execute(context.Background(), deleteAction())
	if err == nil {
		t.Fatal("execute: expected error from injected provider failure")
	}
	got, _ := sh.inv.Get("m1")
	if got.State != machine.StateFailed {
		t.Errorf("state = %s, want Failed", got.State)
	}
	if !strings.Contains(got.LastError, "synthetic delete failure") {
		t.Errorf("LastError = %q, want the provider error recorded", got.LastError)
	}
}
