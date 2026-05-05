package inventory_test

import (
	"errors"
	"math"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
)

func newSpec(id machine.ID) machine.Machine {
	return machine.Machine{ID: id, State: machine.StateSpeculative}
}

func newIdle(id machine.ID) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: machine.StateIdle,
		Host:  machine.HostRef{Provider: "fake", Ref: string(id)},
	}
}

func newConfigured(id machine.ID, cluster machine.ClusterID) machine.Machine {
	return machine.Machine{
		ID:      id,
		State:   machine.StateConfigured,
		Host:    machine.HostRef{Provider: "fake", Ref: string(id)},
		Cluster: cluster,
	}
}

func TestInsert_RejectsDuplicate(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	if err := inv.Insert(newSpec("m-1")); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := inv.Insert(newSpec("m-1")); err == nil {
		t.Fatal("expected duplicate insert to fail")
	}
}

func TestInsert_RejectsInvariantViolation(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	bad := machine.Machine{ID: "m-1", State: machine.StateIdle} // missing host
	if err := inv.Insert(bad); err == nil {
		t.Fatal("expected invariant violation to be rejected")
	}
}

func TestApply_RejectsInvalidTransition(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	if err := inv.Insert(newConfigured("m-1", "cluster-a")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	bad := newIdle("m-1") // Configured → Idle is invalid; must go via Draining
	if err := inv.Apply(bad); err == nil {
		t.Fatal("expected invalid transition to be rejected")
	}
}

func TestApply_AllowsValidTransition(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	m := newSpec("m-1")
	if err := inv.Insert(m); err != nil {
		t.Fatalf("seed: %v", err)
	}
	m.State = machine.StateCreating
	if err := inv.Apply(m); err != nil {
		t.Errorf("Speculative → Creating should be allowed, got %v", err)
	}
}

func TestRemove_NotFound(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	err := inv.Remove("missing")
	if !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestListByState_AndCluster(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i, m := range []machine.Machine{
		newSpec("s-1"),
		newSpec("s-2"),
		newIdle("i-1"),
		newConfigured("c-1", "cluster-a"),
		newConfigured("c-2", "cluster-a"),
		newConfigured("c-3", "cluster-b"),
	} {
		if err := inv.Insert(m); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	if got := inv.CountByState(machine.StateSpeculative); got != 2 {
		t.Errorf("speculative count = %d, want 2", got)
	}
	if got := inv.CountByState(machine.StateIdle); got != 1 {
		t.Errorf("idle count = %d, want 1", got)
	}
	if got := inv.CountByState(machine.StateConfigured); got != 3 {
		t.Errorf("configured count = %d, want 3", got)
	}

	clusterA := inv.ListByClusterState("cluster-a", machine.StateConfigured)
	if len(clusterA) != 2 {
		t.Errorf("cluster-a configured = %d, want 2", len(clusterA))
	}
	clusterB := inv.ListByClusterState("cluster-b", machine.StateConfigured)
	if len(clusterB) != 1 {
		t.Errorf("cluster-b configured = %d, want 1", len(clusterB))
	}
}

func TestSnapshot_IsolatedFromMutations(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	if err := inv.Insert(newConfigured("m-1", "cluster-a")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	snap := inv.Snapshot()
	if snap.Len() != 1 {
		t.Fatalf("snapshot len = %d, want 1", snap.Len())
	}
	if got := snap.CountByClusterState("cluster-a", machine.StateConfigured); got != 1 {
		t.Errorf("snapshot cluster-a configured = %d, want 1", got)
	}

	// Mutate live inventory; snapshot must not change.
	if err := inv.Apply(machine.Machine{
		ID:      "m-1",
		State:   machine.StateDraining,
		Host:    machine.HostRef{Provider: "fake", Ref: "m-1"},
		Cluster: "cluster-a",
	}); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if got := snap.CountByClusterState("cluster-a", machine.StateConfigured); got != 1 {
		t.Errorf("snapshot mutated post-snapshot: configured = %d, want 1", got)
	}
}

// Phase 3 conservation: removing exactly the machines a Phase 3 pass
// would reclaim leaves the inventory size at (before - reclaimed). The
// test models a reclamation as a sequence of Apply (configured →
// draining → idle).
func TestPhase3_Conservation(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 10; i++ {
		if err := inv.Insert(newConfigured(machine.ID(byteToID('a', byte(i))), "cluster-a")); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	before := inv.Len()
	reclaim := 4
	for i := 0; i < reclaim; i++ {
		id := machine.ID(byteToID('a', byte(i)))
		// Configured → Draining
		if err := inv.Apply(machine.Machine{
			ID: id, State: machine.StateDraining,
			Host: machine.HostRef{Provider: "fake", Ref: string(id)}, Cluster: "cluster-a",
		}); err != nil {
			t.Fatalf("drain %d: %v", i, err)
		}
		// Draining → Idle (cluster cleared)
		if err := inv.Apply(machine.Machine{
			ID: id, State: machine.StateIdle,
			Host: machine.HostRef{Provider: "fake", Ref: string(id)},
		}); err != nil {
			t.Fatalf("idle %d: %v", i, err)
		}
	}
	if inv.Len() != before {
		t.Errorf("Len changed unexpectedly: before=%d after=%d (Phase 3 should not change total count, only state)", before, inv.Len())
	}
	if got := inv.CountByState(machine.StateConfigured); got != before-reclaim {
		t.Errorf("configured count = %d, want %d", got, before-reclaim)
	}
	if got := inv.CountByState(machine.StateIdle); got != reclaim {
		t.Errorf("idle count = %d, want %d", got, reclaim)
	}
}

func TestConcurrent_NoRace(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 50; i++ {
		_ = inv.Insert(newSpec(machine.ID(byteToID('s', byte(i)))))
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			_ = inv.Snapshot()
		}
		close(done)
	}()
	for i := 0; i < 50; i++ {
		_ = inv.Apply(machine.Machine{
			ID:    machine.ID(byteToID('s', byte(i))),
			State: machine.StateCreating,
		})
	}
	<-done
}

func byteToID(prefix byte, n byte) string {
	return string([]byte{prefix, '-', '0' + n/100, '0' + (n/10)%10, '0' + n%10})
}

// TestSnapshot_MinAssignedPriority covers the M30.1 indexes used by
// Phase 2's short-circuit. Empty buckets must read math.MaxInt32; a
// pinned bucket reads its own min; the per-state min covers all of
// (state) including any machines without an instance type.
func TestSnapshot_MinAssignedPriority(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	defer inv.Stop()
	mk := func(id, instType string, priority int32) machine.Machine {
		return machine.Machine{
			ID:               machine.ID(id),
			State:            machine.StateConfigured,
			Host:             machine.HostRef{Provider: "fake", Ref: id},
			Cluster:          machine.ClusterID("c-1"),
			Profile:          machine.Profile{InstanceType: instType, Zone: "z-a", CapacityType: machine.CapacityTypeBareMetal},
			AssignedPriority: priority,
		}
	}
	if err := inv.Insert(mk("a-hi", "a3-highgpu-8g", 1_000_000)); err != nil {
		t.Fatal(err)
	}
	if err := inv.Insert(mk("a-lo", "a3-highgpu-8g", 100)); err != nil {
		t.Fatal(err)
	}
	if err := inv.Insert(mk("b-mid", "m6i.large", 500)); err != nil {
		t.Fatal(err)
	}
	snap := inv.Snapshot()

	if got := snap.MinAssignedPriorityByInstanceType(machine.StateConfigured, "a3-highgpu-8g"); got != 100 {
		t.Errorf("a3 min = %d, want 100", got)
	}
	if got := snap.MinAssignedPriorityByInstanceType(machine.StateConfigured, "m6i.large"); got != 500 {
		t.Errorf("m6i min = %d, want 500", got)
	}
	if got := snap.MinAssignedPriorityByInstanceType(machine.StateConfigured, "absent"); got != math.MaxInt32 {
		t.Errorf("absent type min = %d, want MaxInt32", got)
	}
	if got := snap.MinAssignedPriority(machine.StateConfigured); got != 100 {
		t.Errorf("StateConfigured min = %d, want 100", got)
	}
	if got := snap.MinAssignedPriority(machine.StateIdle); got != math.MaxInt32 {
		t.Errorf("empty StateIdle min = %d, want MaxInt32", got)
	}
}
