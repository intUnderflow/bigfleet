package fake_test

import (
	"context"
	"errors"
	"strconv"
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
	ack, err = p.Delete(ctx, provider.DeleteRequest{MachineID: "m-1"})
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

// TestFake_List_SinceRevisionDedupsRepeatedMutations: a machine
// mutated twice within the cursor window should appear once in the
// delta — the revLog index dedups by ID.
func TestFake_List_SinceRevisionDedupsRepeatedMutations(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	p.AddIdle("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)
	cold, _ := p.List(ctx, provider.ListFilter{})

	// Two mutations on the same machine: Configure then Drain.
	if _, err := p.Configure(ctx, provider.ConfigureRequest{MachineID: "m-1", ClusterID: "c-1"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if _, err := p.Drain(ctx, provider.DrainRequest{MachineID: "m-1"}); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	delta, _ := p.List(ctx, provider.ListFilter{SinceRevision: cold.Revision})
	if len(delta.Machines) != 1 {
		t.Fatalf("delta = %d machines, want 1 (deduped)", len(delta.Machines))
	}
	if delta.Machines[0].ID != "m-1" {
		t.Errorf("got %s, want m-1", delta.Machines[0].ID)
	}
}

// TestFake_ConcurrentListGet covers the RWMutex behaviour: many
// concurrent List + Get calls succeed without deadlock or race
// (catches the obvious regressions if the lock is downgraded
// incorrectly).
func TestFake_ConcurrentListGet(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		p.AddIdle(machine.ID("m-"+strconv.Itoa(i)),
			machine.Profile{InstanceType: "p5"},
			machine.CapacityTypeOnDemand, 6.0, 0.0)
	}

	const goroutines = 8
	const iterations = 200
	done := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func(gi int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < iterations; i++ {
				if i%2 == 0 {
					if _, err := p.List(ctx, provider.ListFilter{}); err != nil {
						t.Errorf("List: %v", err)
						return
					}
				} else {
					id := machine.ID("m-" + strconv.Itoa((gi+i)%50))
					if _, err := p.Get(ctx, id); err != nil {
						t.Errorf("Get: %v", err)
						return
					}
				}
			}
		}(g)
	}
	for g := 0; g < goroutines; g++ {
		<-done
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

// Paper §11 / M71 fencing contract: the fake tracks a per-shard_id
// high-water mark of accepted (epoch, seq) tokens and rejects anything
// not strictly newer with ErrFenced. Zero tokens (the in-process
// harness path) bypass fencing entirely.
func TestFake_Fencing_Contract(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fence := func(epoch, seq int64) provider.Fence {
		return provider.Fence{ShardID: "shard-0", ShardEpoch: epoch, SequenceNumber: seq}
	}
	newFake := func(t *testing.T) *fake.Provider {
		t.Helper()
		p := fake.New(fake.Options{InstantTransitions: true})
		p.AddSpeculative("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)
		return p
	}

	t.Run("unknown shard_id accepted and establishes the mark", func(t *testing.T) {
		t.Parallel()
		p := newFake(t)
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(3, 7)}); err != nil {
			t.Fatalf("first contact: %v", err)
		}
		// The first contact's token is the mark: replaying it must fail.
		_, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(3, 7)})
		if !errors.Is(err, provider.ErrFenced) {
			t.Fatalf("replay of first token: got %v, want ErrFenced", err)
		}
	})

	t.Run("stale epoch rejected", func(t *testing.T) {
		t.Parallel()
		p := newFake(t)
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(2, 1)}); err != nil {
			t.Fatalf("establish: %v", err)
		}
		_, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(1, 99)})
		if !errors.Is(err, provider.ErrFenced) {
			t.Fatalf("stale epoch: got %v, want ErrFenced", err)
		}
	})

	t.Run("stale sequence within same epoch rejected", func(t *testing.T) {
		t.Parallel()
		p := newFake(t)
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(1, 5)}); err != nil {
			t.Fatalf("establish: %v", err)
		}
		_, err := p.Configure(ctx, provider.ConfigureRequest{MachineID: "m-1", ClusterID: "c", Fence: fence(1, 4)})
		if !errors.Is(err, provider.ErrFenced) {
			t.Fatalf("stale seq: got %v, want ErrFenced", err)
		}
	})

	t.Run("new epoch resets the sequence space", func(t *testing.T) {
		t.Parallel()
		p := newFake(t)
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(1, 1000)}); err != nil {
			t.Fatalf("establish: %v", err)
		}
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(2, 1)}); err != nil {
			t.Fatalf("new epoch with low seq: got %v, want idempotent accept", err)
		}
	})

	t.Run("fence checked before not-found", func(t *testing.T) {
		t.Parallel()
		p := newFake(t)
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(5, 1)}); err != nil {
			t.Fatalf("establish: %v", err)
		}
		// A zombie must not learn whether a machine exists.
		_, err := p.Delete(ctx, provider.DeleteRequest{MachineID: "missing", Fence: fence(4, 9)})
		if !errors.Is(err, provider.ErrFenced) {
			t.Fatalf("stale token on unknown machine: got %v, want ErrFenced (not ErrNotFound)", err)
		}
	})

	t.Run("zero fence bypasses (in-process harness path)", func(t *testing.T) {
		t.Parallel()
		p := newFake(t)
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(9, 9)}); err != nil {
			t.Fatalf("establish: %v", err)
		}
		if _, err := p.Configure(ctx, provider.ConfigureRequest{MachineID: "m-1", ClusterID: "c"}); err != nil {
			t.Fatalf("unfenced call after fenced ones: %v", err)
		}
	})

	t.Run("per-shard isolation", func(t *testing.T) {
		t.Parallel()
		p := newFake(t)
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: fence(9, 9)}); err != nil {
			t.Fatalf("establish shard-0: %v", err)
		}
		other := provider.Fence{ShardID: "shard-1", ShardEpoch: 1, SequenceNumber: 1}
		if _, err := p.Create(ctx, provider.CreateRequest{MachineID: "m-1", Fence: other}); err != nil {
			t.Fatalf("shard-1's first contact must not be fenced by shard-0's mark: %v", err)
		}
	})
}

// M29 Configured-seed path: machines should appear via List(Configured)
// with the cluster + priority + penalty values populated, so the shard
// snapshot has everything Phase 2/3 need to score them.
func TestFake_AddConfigured_ExposesAssignedMetadata(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	prof := machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "zone-a"}
	p.AddConfigured("conf-x", prof, machine.CapacityTypeBareMetal, 0, 0, "kwok-cluster-7", 1000000, 8192, 65536)

	listed, err := p.List(ctx, provider.ListFilter{States: []machine.State{machine.StateConfigured}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed.Machines) != 1 {
		t.Fatalf("Configured listing = %d, want 1", len(listed.Machines))
	}
	got := listed.Machines[0]
	if got.ID != "conf-x" {
		t.Errorf("ID = %q, want conf-x", got.ID)
	}
	if got.Cluster != "kwok-cluster-7" {
		t.Errorf("Cluster = %q, want kwok-cluster-7", got.Cluster)
	}
	if got.AssignedPriority != 1000000 {
		t.Errorf("AssignedPriority = %d, want 1000000", got.AssignedPriority)
	}
	if got.AssignedInterruptionPenaltyDollars != 8192 {
		t.Errorf("AssignedInterruptionPenaltyDollars = %v, want 8192", got.AssignedInterruptionPenaltyDollars)
	}
	if got.AssignedReclamationPenaltyDollars != 65536 {
		t.Errorf("AssignedReclamationPenaltyDollars = %v, want 65536", got.AssignedReclamationPenaltyDollars)
	}
	idle, _ := p.List(ctx, provider.ListFilter{States: []machine.State{machine.StateIdle}})
	if len(idle.Machines) != 0 {
		t.Errorf("Idle listing = %d, want 0 (Configured seed must not leak into Idle)", len(idle.Machines))
	}
}

// M72: Configure stores shard_metadata verbatim — unknown keys included,
// nothing decoded — and Get/List echo it until Drain ends the binding.
// The fake is the conformance reference for store-and-echo-never-
// interpret, so this test pins the verbatim part hard.
func TestFake_ShardMetadata_StoredVerbatimAndClearedOnDrain(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	ctx := context.Background()
	p.AddIdle("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)

	md := map[string]string{
		machine.ShardMetadataKeyAssignedPriority: "1000000",
		"x-future-shard/unknown-key":             "must-survive",
	}
	if _, err := p.Configure(ctx, provider.ConfigureRequest{
		MachineID: "m-1", ClusterID: "c-1", ShardMetadata: md,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	// Mutating the caller's map after Configure must not reach the store.
	md["x-future-shard/unknown-key"] = "mutated-after-the-fact"

	got, err := p.Get(ctx, "m-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ShardMetadata["x-future-shard/unknown-key"] != "must-survive" {
		t.Errorf("echo not verbatim-at-Configure-time: %v", got.ShardMetadata)
	}
	if got.ShardMetadata[machine.ShardMetadataKeyAssignedPriority] != "1000000" {
		t.Errorf("well-known key lost: %v", got.ShardMetadata)
	}
	// Never interpreted: the fake must NOT have decoded the priority.
	if got.AssignedPriority != 0 {
		t.Errorf("fake interpreted shard_metadata: AssignedPriority = %d", got.AssignedPriority)
	}

	if _, err := p.Drain(ctx, provider.DrainRequest{MachineID: "m-1", GracePeriod: 1}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	got, err = p.Get(ctx, "m-1")
	if err != nil {
		t.Fatalf("Get after Drain: %v", err)
	}
	if got.Cluster != "" || len(got.ShardMetadata) != 0 {
		t.Errorf("per-assignment state survived Drain: cluster=%q metadata=%v", got.Cluster, got.ShardMetadata)
	}
}

// M72: AddConfigured seeds the shard_metadata echo a real provider would
// be holding for a machine some shard previously configured, so
// harness-seeded fleets are restart-rebuildable over the wire.
func TestFake_AddConfigured_SeedsShardMetadataEcho(t *testing.T) {
	t.Parallel()
	p := fake.New(fake.Options{InstantTransitions: true})
	p.AddConfigured("conf-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeBareMetal,
		0, 0, "c-7", 1_000_000, 8192, 65536)

	got, err := p.Get(context.Background(), "conf-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	check := machine.Machine{ShardMetadata: got.ShardMetadata}
	if err := check.DecodeShardMetadata(); err != nil {
		t.Fatalf("DecodeShardMetadata: %v", err)
	}
	if check.AssignedPriority != 1_000_000 ||
		check.AssignedInterruptionPenaltyDollars != 8192 ||
		check.AssignedReclamationPenaltyDollars != 65536 {
		t.Errorf("seeded echo decodes to %+v, want (1000000, 8192, 65536)", check)
	}
}
