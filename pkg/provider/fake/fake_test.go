package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

func TestFake_LifecycleHappyPath_Instant(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	prof := machine.Profile{InstanceType: "p5.48xlarge", Zone: "us-east-1a", CapacityType: machine.CapacityTypeOnDemand}
	p.AddSpeculative("m-1", prof, machine.CapacityTypeOnDemand, 6.0, 0.0)

	// Speculative → Idle
	ack, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ack.Machine.State != machine.StateIdle {
		t.Errorf("Create returned state=%s, want Idle", ack.Machine.State)
	}

	// Idle → Configured
	ack, err = p.Configure(ctx, provider.ConfigureRequest{MachineID: "m-1", ClusterID: "cluster-a"})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if ack.Machine.State != machine.StateConfigured {
		t.Errorf("Configure returned state=%s, want Configured", ack.Machine.State)
	}
	if ack.Machine.Cluster != "cluster-a" {
		t.Errorf("Cluster = %s, want cluster-a", ack.Machine.Cluster)
	}

	// Configured → Idle
	ack, err = p.Drain(ctx, provider.DrainRequest{MachineID: "m-1", GracePeriod: 30})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if ack.Machine.State != machine.StateIdle {
		t.Errorf("Drain returned state=%s, want Idle", ack.Machine.State)
	}
	if ack.Machine.Cluster != "" {
		t.Errorf("Cluster = %s, want empty after drain", ack.Machine.Cluster)
	}

	// Idle → Speculative
	ack, err = p.Delete(ctx, "m-1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ack.Machine.State != machine.StateSpeculative {
		t.Errorf("Delete returned state=%s, want Speculative", ack.Machine.State)
	}
	if !ack.Machine.Host.Empty() {
		t.Errorf("Host = %+v, want empty after delete", ack.Machine.Host)
	}
}

func TestFake_Idempotent_RepeatedCreate(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	p.AddSpeculative("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)

	first, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1"})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	second, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1"})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if first.OperationID != second.OperationID {
		t.Errorf("OperationID differs across idempotent retries: first=%s second=%s", first.OperationID, second.OperationID)
	}
}

func TestFake_FailNext_InjectsError(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	p.AddSpeculative("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)

	p.FailNext("m-1", machine.StateIdle, fake.ErrSyntheticFailure)
	_, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1"})
	if !errors.Is(err, fake.ErrSyntheticFailure) {
		t.Fatalf("expected ErrSyntheticFailure, got %v", err)
	}
	// After consuming the injected error, the next call should succeed.
	_, err = p.Create(ctx, provider.CreateRequest{MachineID: "m-1"})
	if err != nil {
		t.Errorf("expected retry to succeed, got %v", err)
	}
}

func TestFake_List_FiltersByStateAndZone(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	p.AddIdle("i-east", machine.Profile{InstanceType: "m6i", Zone: "us-east-1a"}, machine.CapacityTypeBareMetal, 0, 0)
	p.AddIdle("i-west", machine.Profile{InstanceType: "m6i", Zone: "us-west-2a"}, machine.CapacityTypeBareMetal, 0, 0)
	p.AddSpeculative("s-east", machine.Profile{InstanceType: "m6i", Zone: "us-east-1a"}, machine.CapacityTypeOnDemand, 1, 0)

	resp, err := p.List(ctx, provider.ListFilter{
		States: []machine.State{machine.StateIdle},
		Zone:   "us-east-1a",
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.Machines) != 1 || resp.Machines[0].ID != "i-east" {
		t.Errorf("List returned %v, want only i-east", resp.Machines)
	}
}

func TestFake_Create_NotFound(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	_, err := p.Create(context.Background(), provider.CreateRequest{MachineID: "missing"})
	if !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestFake_RevisionAdvancesWithChanges(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	p.AddSpeculative("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)

	r1, _ := p.List(ctx, provider.ListFilter{})
	if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	r2, _ := p.List(ctx, provider.ListFilter{})
	if string(r1.Revision) == string(r2.Revision) {
		t.Errorf("revision did not advance after Create")
	}
}

// TestFake_List_SinceRevisionReturnsDeltasOnly covers the M11.22 path:
// callers passing a prior cursor see only machines mutated since that
// cursor. Cold start (empty cursor) still returns full state.
func TestFake_List_SinceRevisionReturnsDeltasOnly(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()

	// Seed two machines and snapshot the cursor.
	p.AddIdle("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)
	p.AddIdle("m-2", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)
	cold, _ := p.List(ctx, provider.ListFilter{})
	if len(cold.Machines) != 2 {
		t.Fatalf("cold list = %d, want 2", len(cold.Machines))
	}

	// No mutations between calls → since_revision returns empty delta.
	idle, _ := p.List(ctx, provider.ListFilter{SinceRevision: cold.Revision})
	if len(idle.Machines) != 0 {
		t.Errorf("idle delta = %d, want 0 (no mutations since cursor)", len(idle.Machines))
	}

	// Mutate m-1 only; delta should contain m-1 but not m-2.
	if _, err := p.Configure(ctx, provider.ConfigureRequest{MachineID: "m-1", ClusterID: "cluster-a"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	delta, _ := p.List(ctx, provider.ListFilter{SinceRevision: cold.Revision})
	if len(delta.Machines) != 1 || delta.Machines[0].ID != "m-1" {
		t.Errorf("delta = %+v, want exactly m-1", delta.Machines)
	}

	// A delta from the new cursor should be empty; the prior cursor
	// must keep returning the same delta (cursors are immutable views).
	delta2, _ := p.List(ctx, provider.ListFilter{SinceRevision: delta.Revision})
	if len(delta2.Machines) != 0 {
		t.Errorf("post-delta cursor = %d, want 0", len(delta2.Machines))
	}
}
