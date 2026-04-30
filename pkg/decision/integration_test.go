package decision_test

import (
	"context"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

// integration scenarios wire the engine to a fake provider and walk a
// few of the paper's worked examples end-to-end. They double as the
// proof that pkg/needs + pkg/inventory + pkg/decision + pkg/provider
// compose without an actual shard, and that the actions Phase 1/2/3
// emit are something a downstream component can execute.

// "Training job with topology" — paper §10. 64 GPU nodes needed; the
// fleet has 64 idle GPU machines available. Phase 1 emits 64 Bootstrap
// actions and the next reconciliation completes the job.
func TestIntegration_TrainingJobWithTopology(t *testing.T) {
	t.Parallel()
	prov := fake.New(fake.Options{InstantTransitions: true})
	for i := 0; i < 64; i++ {
		prov.AddIdle(machine.ID(idStr("gpu-", i)),
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a"},
			machine.CapacityTypeBareMetal, 0, 0)
	}

	inv := mustSyncInventory(t, prov)
	pf := gpuProfile(1_000_000)
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-train", Profile: pf, Count: 64},
	})
	if got := len(r.Actions); got != 64 {
		t.Fatalf("phase1: actions = %d, want 64", got)
	}
	if got := len(r.Unsatisfied); got != 0 {
		t.Errorf("phase1: unsatisfied = %d, want 0", got)
	}
	executeActions(t, prov, inv, pf, r.Actions)

	// After execution, every machine should be Configured for cluster-train.
	if got := inv.CountByState(machine.StateConfigured); got != 64 {
		t.Errorf("post-execute: configured = %d, want 64", got)
	}
}

// "Capacity stockout" — paper §10. 64 needed; only 32 idle. Phase 1
// emits 32 actions and reports 32 unsatisfied. Phase 2 finds nothing
// to preempt (no lower-priority occupants). The shard would emit a
// shortfall.
func TestIntegration_CapacityStockout(t *testing.T) {
	t.Parallel()
	prov := fake.New(fake.Options{InstantTransitions: true})
	for i := 0; i < 32; i++ {
		prov.AddIdle(machine.ID(idStr("gpu-", i)),
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a"},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	inv := mustSyncInventory(t, prov)

	r1 := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-train", Profile: gpuProfile(1_000_000), Count: 64},
	})
	if got := len(r1.Actions); got != 32 {
		t.Fatalf("phase1: actions = %d, want 32", got)
	}
	if got := len(r1.Unsatisfied); got != 1 || r1.Unsatisfied[0].Deficit != 32 {
		t.Errorf("phase1: unsatisfied = %+v, want deficit=32", r1.Unsatisfied)
	}
	r2 := decision.Phase2(inv.Snapshot(), r1.Unsatisfied, decision.DefaultPhase2Options())
	if got := len(r2.Actions); got != 0 {
		t.Errorf("phase2: preempt = %d, want 0 (no victims to take)", got)
	}
	if got := len(r2.Unresolved); got != 1 {
		t.Errorf("phase2: unresolved = %d, want 1 (becomes a shortfall)", got)
	}
}

// "Withdrawal" — paper §10. The need set shrinks to zero; Phase 3
// reclaims every configured machine for the cluster.
func TestIntegration_Withdrawal(t *testing.T) {
	t.Parallel()
	prov := fake.New(fake.Options{InstantTransitions: true})
	for i := 0; i < 64; i++ {
		prov.AddIdle(machine.ID(idStr("gpu-", i)),
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a"},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	inv := mustSyncInventory(t, prov)
	pf := gpuProfile(1_000_000)
	// First, configure all 64 for the training cluster.
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-train", Profile: pf, Count: 64},
	})
	executeActions(t, prov, inv, pf, r.Actions)

	// Now training finishes — roll-up has no needs. Phase 3 reclaims all.
	r3 := decision.Phase3(inv.Snapshot(), nil)
	if got := len(r3.Actions); got != 64 {
		t.Fatalf("phase3: reclaim = %d, want 64", got)
	}
}

// "Priority inversion" — paper §8. 4 GPU machines configured for a
// low-priority batch cluster; a high-priority training cluster needs
// them. Phase 2 preempts; the next Phase 1 (after the drain has
// completed) places them with the new cluster.
func TestIntegration_PriorityInversion(t *testing.T) {
	t.Parallel()
	prov := fake.New(fake.Options{InstantTransitions: true})
	for i := 0; i < 4; i++ {
		prov.AddIdle(machine.ID(idStr("gpu-", i)),
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a"},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	inv := mustSyncInventory(t, prov)

	// Configure all 4 for cluster-batch at priority 100K (so the gap is
	// big enough to be preemptible).
	batchPF := gpuProfile(100_000)
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-batch", Profile: batchPF, Count: 4},
	})
	executeActions(t, prov, inv, batchPF, r.Actions)

	// Now cluster-train at priority 1M wants the same 4 machines.
	trainPF := gpuProfile(1_000_000)
	r1 := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-batch", Profile: batchPF, Count: 4},
		{ClusterID: "cluster-train", Profile: trainPF, Count: 4},
	})
	if got := len(r1.Unsatisfied); got != 1 {
		t.Fatalf("phase1: expected 1 unsatisfied (train), got %d", got)
	}

	// Phase 2 picks 4 victims at priority 100K and emits Preempt actions.
	r2 := decision.Phase2(inv.Snapshot(), r1.Unsatisfied, decision.DefaultPhase2Options())
	if got := len(r2.Actions); got != 4 {
		t.Fatalf("phase2: preempt actions = %d, want 4", got)
	}
	for _, a := range r2.Actions {
		if a.Kind != decision.ActionKindPreempt {
			t.Errorf("expected Preempt, got %s", a.Kind)
		}
	}
	// Drain them all.
	for _, a := range r2.Actions {
		if _, err := prov.Drain(context.Background(), provider.DrainRequest{MachineID: a.MachineID}); err != nil {
			t.Fatalf("drain %s: %v", a.MachineID, err)
		}
		// Mirror the provider's state into the inventory.
		m, _ := prov.Get(context.Background(), a.MachineID)
		if err := inv.Apply(machine.Machine{
			ID:    m.ID,
			State: machine.StateDraining,
			Host:  m.Host, Cluster: a.Cluster,
		}); err != nil {
			t.Fatalf("inv apply draining: %v", err)
		}
		if err := inv.Apply(m); err != nil {
			t.Fatalf("inv apply idle: %v", err)
		}
	}

	// Next cycle: Phase 1 picks up the now-Idle machines for cluster-train.
	r3 := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-train", Profile: trainPF, Count: 4},
	})
	if got := len(r3.Actions); got != 4 {
		t.Fatalf("phase1 round 2: actions = %d, want 4", got)
	}
}

// executeActions applies Phase 1 actions through the fake provider and
// mirrors the resulting state into the in-memory inventory. Models the
// shard's "ack action → record state change" half of the loop without
// actually shipping the shard. Walks transitions through their
// intermediate transitional states so the inventory's state-machine
// invariants stay honest.
func executeActions(t *testing.T, prov *fake.Provider, inv *inventory.Inventory, pf needs.Profile, actions []decision.Action) {
	t.Helper()
	ctx := context.Background()
	intPen := decision.BucketUpperBoundDollars(pf.InterruptionPenaltyBucket())
	recPen := decision.BucketUpperBoundDollars(pf.ReclamationPenaltyBucket())
	for _, a := range actions {
		switch a.Kind {
		case decision.ActionKindProvision:
			// Speculative → Creating → Idle → Configuring → Configured
			ack, err := prov.Create(ctx, provider.CreateRequest{MachineID: a.MachineID})
			if err != nil {
				t.Fatalf("create %s: %v", a.MachineID, err)
			}
			stepInventory(t, inv, ack.Machine.ID, machine.StateCreating, ack.Machine.Host, "")
			stepInventory(t, inv, ack.Machine.ID, machine.StateIdle, ack.Machine.Host, "")
			fallthrough
		case decision.ActionKindBootstrap:
			ack, err := prov.Configure(ctx, provider.ConfigureRequest{MachineID: a.MachineID, ClusterID: a.Cluster})
			if err != nil {
				t.Fatalf("configure %s: %v", a.MachineID, err)
			}
			stepInventory(t, inv, ack.Machine.ID, machine.StateConfiguring, ack.Machine.Host, a.Cluster)
			stepInventory(t, inv, ack.Machine.ID, machine.StateConfigured, ack.Machine.Host, a.Cluster)
			// Stamp Phase-2/3 reasoning fields.
			final, _ := inv.Get(ack.Machine.ID)
			final.AssignedPriority = pf.Priority()
			final.AssignedInterruptionPenaltyDollars = intPen
			final.AssignedReclamationPenaltyDollars = recPen
			if err := inv.Apply(final); err != nil {
				t.Fatalf("stamp penalties %s: %v", a.MachineID, err)
			}
		default:
			t.Fatalf("unsupported action kind for executeActions: %s", a.Kind)
		}
	}
}

// stepInventory inserts or transitions the machine to the target state.
func stepInventory(t *testing.T, inv *inventory.Inventory, id machine.ID, target machine.State, host machine.HostRef, cluster machine.ClusterID) {
	t.Helper()
	cur, err := inv.Get(id)
	if err != nil {
		// Insert the machine at this state if it doesn't exist yet.
		m := machine.Machine{ID: id, State: target, Host: host, Cluster: cluster}
		if err := inv.Insert(m); err != nil {
			t.Fatalf("insert %s @ %s: %v", id, target, err)
		}
		return
	}
	if cur.State == target {
		return
	}
	cur.State = target
	cur.Host = host
	cur.Cluster = cluster
	if err := inv.Apply(cur); err != nil {
		t.Fatalf("step %s %s → %s: %v", id, cur.State, target, err)
	}
}

// mustSyncInventory pulls the provider's full state into a fresh
// inventory.
func mustSyncInventory(t *testing.T, prov *fake.Provider) *inventory.Inventory {
	t.Helper()
	resp, err := prov.List(context.Background(), provider.ListFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	inv := inventory.New()
	for _, m := range resp.Machines {
		if err := inv.Insert(m); err != nil {
			t.Fatalf("insert %s: %v", m.ID, err)
		}
	}
	return inv
}

func idStr(prefix string, n int) string {
	const digits = "0123456789"
	return prefix + string(digits[(n/100)%10]) + string(digits[(n/10)%10]) + string(digits[n%10])
}
