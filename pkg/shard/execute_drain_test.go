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
