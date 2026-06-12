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
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a", Resources: map[string]string{"nvidia.com/gpu": "8"}},
			machine.CapacityTypeBareMetal, 0, 0)
	}

	inv := mustSyncInventory(t, prov)
	pf := gpuProfile(1_000_000)
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-train", pf, 64),
	})
	if got := len(r.Actions); got != 64 {
		t.Fatalf("phase1: actions = %d, want 64", got)
	}
	if got := len(r.Unsatisfied); got != 0 {
		t.Errorf("phase1: unsatisfied = %d, want 0", got)
	}
	executeActions(t, prov, inv, pf, r.Actions)

	// After execution, every machine should be Configured for cluster-train.
	if got := inv.Snapshot().CountByState(machine.StateConfigured); got != 64 {
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
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a", Resources: map[string]string{"nvidia.com/gpu": "8"}},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	inv := mustSyncInventory(t, prov)

	r1 := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-train", gpuProfile(1_000_000), 64),
	})
	if got := len(r1.Actions); got != 32 {
		t.Fatalf("phase1: actions = %d, want 32", got)
	}
	if got := len(r1.Unsatisfied); got != 1 || gpuQty(r1.Unsatisfied[0].Deficit) != "256" {
		t.Errorf("phase1: unsatisfied = %+v, want deficit nvidia.com/gpu=256 (32 units)", r1.Unsatisfied)
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
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a", Resources: map[string]string{"nvidia.com/gpu": "8"}},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	inv := mustSyncInventory(t, prov)
	pf := gpuProfile(1_000_000)
	// First, configure all 64 for the training cluster.
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-train", pf, 64),
	})
	executeActions(t, prov, inv, pf, r.Actions)

	// Now training finishes — roll-up has no needs. The empty demand
	// claims nothing, so Phase 3 reclaims all (ADR-0045 shrinkage).
	r3 := runPhase3(t, inv.Snapshot(), nil, decision.AlwaysReady)
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
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a", Resources: map[string]string{"nvidia.com/gpu": "8"}},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	inv := mustSyncInventory(t, prov)

	// Configure all 4 for cluster-batch at priority 100K (so the gap is
	// big enough to be preemptible).
	batchPF := gpuProfile(100_000)
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-batch", batchPF, 4),
	})
	executeActions(t, prov, inv, batchPF, r.Actions)

	// Now cluster-train at priority 1M wants the same 4 machines.
	trainPF := gpuProfile(1_000_000)
	r1 := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-batch", batchPF, 4),
		gpuNeed("cluster-train", trainPF, 4),
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
		gpuNeed("cluster-train", trainPF, 4),
	})
	if got := len(r3.Actions); got != 4 {
		t.Fatalf("phase1 round 2: actions = %d, want 4", got)
	}
}

// ADR-0040 Addendum no-oscillation regression. cluster-x has 2
// Configured machines matching a Same Need in zone-a and the shard
// has 3 Idle in zone-b; the gang's aggregate is 5 machines' worth
// with a 1-machine MinUnit. The joint domain choice (creditable +
// acquirable) must pick zone-b (most-covering: 3 > 2), acquisition
// must stay confined there, Phase 3 — ranking by the identical joint
// score — reclaims zone-a's 2 as off-domain scatter, and once the
// supply settles (zone-b's 3 credited, zone-a's 2 drained back to
// Idle) subsequent cycles are action-free: no bootstrap/reclaim
// flip-flop between the domains.
//
// Pre-Addendum this exact shape oscillated at cycle rate (~14/sec at
// uber-5k): the pre-pass chose the creditable zone-a, FindSame
// independently chose the Idle-rich zone-b, Phase 1 assembled a
// cross-domain group, and Phase 3 (strict since ADR-0040) reclaimed
// the off-domain half every cycle.
func TestIntegration_SameDomain_NoOscillation(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 2; i++ {
		_ = inv.Insert(configuredGpuInZone("a-"+idN(i), "cluster-x", "zone-a"))
	}
	for i := 0; i < 3; i++ {
		_ = inv.Insert(gpuMachineInZone("b-"+idN(i), "zone-b", 1.0))
	}
	demand := []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		5,
	)}

	// applyCycle runs Phase 1 + Phase 3 on the current snapshot, then
	// folds the actions back into the inventory: Bootstraps become
	// Configured for the Need's cluster; Reclaims drain back to Idle
	// (still in zone-a — live bait that a re-picking allocator would
	// re-bootstrap).
	applyCycle := func() (bootstraps, reclaims []decision.Action) {
		t.Helper()
		snap := inv.Snapshot()
		p1 := decision.Phase1(snap, demand)
		p3 := decision.Phase3(snap, p1.Claimed, decision.AlwaysReady)
		for _, a := range p1.Actions {
			if a.Kind != decision.ActionKindBootstrap {
				t.Fatalf("unexpected non-Bootstrap Phase 1 action: %+v", a)
			}
			bootstraps = append(bootstraps, a)
			m, _ := snap.Get(a.MachineID)
			stepInventory(t, inv, a.MachineID, machine.StateConfiguring, m.Host, a.Cluster)
			stepInventory(t, inv, a.MachineID, machine.StateConfigured, m.Host, a.Cluster)
		}
		for _, a := range p3.Actions {
			reclaims = append(reclaims, a)
			m, err := inv.Get(a.MachineID)
			if err != nil {
				t.Fatalf("reclaimed machine %s not in inventory: %v", a.MachineID, err)
			}
			m.State = machine.StateDraining
			if err := inv.Apply(m); err != nil {
				t.Fatalf("drain %s: %v", a.MachineID, err)
			}
			m.State = machine.StateIdle
			m.Cluster = ""
			if err := inv.Apply(m); err != nil {
				t.Fatalf("idle %s: %v", a.MachineID, err)
			}
		}
		return bootstraps, reclaims
	}

	// Cycle 1: the joint choice concentrates on zone-b.
	boots, recls := applyCycle()
	if len(boots) != 3 {
		t.Fatalf("cycle 1 bootstraps = %d, want 3 (all of zone-b's Idle)", len(boots))
	}
	for _, a := range boots {
		m, _ := inv.Get(a.MachineID)
		if m.Profile.Zone != "zone-b" {
			t.Errorf("cycle 1 bootstrapped %s in %s; acquisition must be confined to the joint choice zone-b", a.MachineID, m.Profile.Zone)
		}
	}
	if len(recls) != 2 {
		t.Fatalf("cycle 1 reclaims = %d, want 2 (zone-a's off-domain Configured)", len(recls))
	}
	for _, a := range recls {
		m, _ := inv.Get(a.MachineID)
		if m.Profile.Zone != "zone-a" {
			t.Errorf("cycle 1 reclaimed %s in %s, want only zone-a scatter", a.MachineID, m.Profile.Zone)
		}
	}

	// Cycles 2+: zone-b's 3 are credited, zone-a's 2 sit Idle, the
	// 2-machine residual is a stable shortfall. Steady state = no
	// further actions of either kind.
	for cycle := 2; cycle <= 4; cycle++ {
		boots, recls = applyCycle()
		if len(boots) != 0 || len(recls) != 0 {
			t.Fatalf("cycle %d: bootstraps = %d, reclaims = %d, want 0/0 — reclaim↔re-bootstrap oscillation is back", cycle, len(boots), len(recls))
		}
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
			final.AssignedNeedFingerprint = pf.Fingerprint()
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
