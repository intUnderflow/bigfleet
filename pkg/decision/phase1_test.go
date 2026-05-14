package decision_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// gpuUnit is the per-replica resource shape of the standard "8-GPU H100"
// need: one a3-highgpu-8g machine hosts exactly one unit.
var gpuUnit = []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}}

// gpuProfile returns the standard "8-GPU H100" need profile used in most
// of the paper's examples.
func gpuProfile(priority int32) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"a3-highgpu-8g"},
		}},
		nil, priority,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
}

// gpuNeed builds a Need for `count` 8-GPU units under the given profile.
func gpuNeed(cluster machine.ClusterID, pf needs.Profile, count int) needs.Need {
	return needs.Need{
		ClusterID:          cluster,
		Profile:            pf,
		AggregateResources: needs.ScaleResources(gpuUnit, count),
		MinUnit:            gpuUnit,
	}
}

// gpuQty pulls the nvidia.com/gpu quantity string out of a resource
// vector — used to assert deficits in 8-GPU-unit terms.
func gpuQty(v []needs.ResourceQty) string {
	for _, r := range v {
		if r.Name == "nvidia.com/gpu" {
			return r.Quantity
		}
	}
	return ""
}

func gpuMachine(id machine.ID, state machine.State, cluster machine.ClusterID, capType machine.CapacityType, price float64) machine.Machine {
	m := machine.Machine{
		ID:    id,
		State: state,
		Profile: machine.Profile{
			InstanceType: "a3-highgpu-8g",
			Zone:         "us-east-1a",
			CapacityType: capType,
			Resources:    map[string]string{"nvidia.com/gpu": "8"},
		},
		PricePerHour: price,
	}
	if state != machine.StateSpeculative && state != machine.StateCreating {
		m.Host = machine.HostRef{Provider: "fake", Ref: string(id)}
	}
	if state == machine.StateConfigured || state == machine.StateDraining {
		m.Cluster = cluster
	}
	return m
}

// Phase 1 with idle inventory only: emits Bootstrap actions.
func TestPhase1_IdleOnly(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(gpuMachine(idN(i), machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))
	}
	snap := inv.Snapshot()

	r := decision.Phase1(snap, []needs.Need{
		gpuNeed("cluster-a", gpuProfile(1_000_000), 3),
	})
	if got := len(r.Actions); got != 3 {
		t.Fatalf("actions = %d, want 3", got)
	}
	for _, a := range r.Actions {
		if a.Kind != decision.ActionKindBootstrap {
			t.Errorf("expected Bootstrap, got %s", a.Kind)
		}
		if a.Cluster != "cluster-a" {
			t.Errorf("expected cluster-a, got %s", a.Cluster)
		}
	}
	if len(r.Unsatisfied) != 0 {
		t.Errorf("unsatisfied = %d, want 0", len(r.Unsatisfied))
	}
}

// Phase 1 with no idle and only speculative: emits Provision actions
// preferring the cheapest effective cost.
func TestPhase1_SpeculativeFallback_PrefersCheapest(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	cheap := gpuMachine("spec-cheap", machine.StateSpeculative, "", machine.CapacityTypeSpot, 1.0)
	cheap.InterruptionProbability = 0.0 // make this unambiguously cheapest
	expensive := gpuMachine("spec-expensive", machine.StateSpeculative, "", machine.CapacityTypeOnDemand, 6.0)
	_ = inv.Insert(cheap)
	_ = inv.Insert(expensive)

	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-a", gpuProfile(1_000_000), 1),
	})
	if got := len(r.Actions); got != 1 {
		t.Fatalf("actions = %d, want 1", got)
	}
	if r.Actions[0].MachineID != "spec-cheap" {
		t.Errorf("picked %s, want spec-cheap", r.Actions[0].MachineID)
	}
	if r.Actions[0].Kind != decision.ActionKindProvision {
		t.Errorf("kind = %s, want Provision", r.Actions[0].Kind)
	}
}

// High-penalty workload: spot loses despite lower price.
func TestPhase1_HighPenalty_PrefersOnDemand(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	spot := gpuMachine("spec-spot", machine.StateSpeculative, "", machine.CapacityTypeSpot, 1.80)
	spot.InterruptionProbability = 0.10
	onDemand := gpuMachine("spec-od", machine.StateSpeculative, "", machine.CapacityTypeOnDemand, 6.00)
	onDemand.InterruptionProbability = 0.0
	_ = inv.Insert(spot)
	_ = inv.Insert(onDemand)

	// PenaltyBucket8388608 is $8,388,608 — well above the cross-over
	// where on-demand wins.
	pf := needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"a3-highgpu-8g"},
		}},
		nil, 1_000_000,
		needs.PenaltyBucket8388608,
		needs.PenaltyBucketPinned,
	)
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-a", pf, 1),
	})
	if got := len(r.Actions); got != 1 {
		t.Fatalf("actions = %d, want 1", got)
	}
	if r.Actions[0].MachineID != "spec-od" {
		t.Errorf("picked %s, want spec-od (high penalty should favour on-demand)", r.Actions[0].MachineID)
	}
}

// Idle preferred over speculative even when speculative is cheaper.
func TestPhase1_IdleBeatsSpeculative(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	idle := gpuMachine("i-1", machine.StateIdle, "", machine.CapacityTypeOnDemand, 6.00)
	cheap := gpuMachine("s-1", machine.StateSpeculative, "", machine.CapacityTypeSpot, 1.00)
	cheap.InterruptionProbability = 0.05
	_ = inv.Insert(idle)
	_ = inv.Insert(cheap)

	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-a", gpuProfile(1_000_000), 1),
	})
	if got := len(r.Actions); got != 1 {
		t.Fatalf("actions = %d, want 1", got)
	}
	if r.Actions[0].Kind != decision.ActionKindBootstrap || r.Actions[0].MachineID != "i-1" {
		t.Errorf("expected idle bootstrap, got %s on %s", r.Actions[0].Kind, r.Actions[0].MachineID)
	}
}

// Already-satisfied needs produce no actions.
func TestPhase1_NoOpWhenSatisfied(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(gpuMachine("c-1", machine.StateConfigured, "cluster-a", machine.CapacityTypeBareMetal, 0))
	_ = inv.Insert(gpuMachine("c-2", machine.StateConfigured, "cluster-a", machine.CapacityTypeBareMetal, 0))

	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-a", gpuProfile(1_000_000), 2),
	})
	if len(r.Actions) != 0 {
		t.Errorf("expected zero actions, got %d", len(r.Actions))
	}
}

// Capacity stockout: enough idle for some, the rest become unsatisfied.
func TestPhase1_PartialStockout(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(gpuMachine("i-1", machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))
	_ = inv.Insert(gpuMachine("i-2", machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))

	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-a", gpuProfile(1_000_000), 5),
	})
	if got := len(r.Actions); got != 2 {
		t.Fatalf("actions = %d, want 2", got)
	}
	if got := len(r.Unsatisfied); got != 1 || gpuQty(r.Unsatisfied[0].Deficit) != "24" {
		t.Errorf("unsatisfied = %+v, want 1 entry with deficit nvidia.com/gpu=24 (3 units)", r.Unsatisfied)
	}
}

// High-priority need wins shared idle pool.
func TestPhase1_HighPriorityWinsContestedPool(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(gpuMachine("i-1", machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))
	_ = inv.Insert(gpuMachine("i-2", machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))

	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		gpuNeed("cluster-low", gpuProfile(0), 2),
		gpuNeed("cluster-high", gpuProfile(1_000_000), 2),
	})

	cluster := map[machine.ClusterID]int{}
	for _, a := range r.Actions {
		cluster[a.Cluster]++
	}
	if cluster["cluster-high"] != 2 {
		t.Errorf("cluster-high got %d actions, want 2", cluster["cluster-high"])
	}
	if cluster["cluster-low"] != 0 {
		t.Errorf("cluster-low got %d actions, want 0 (high-priority should drain pool first)", cluster["cluster-low"])
	}
	if len(r.Unsatisfied) != 1 || r.Unsatisfied[0].Need.ClusterID != "cluster-low" {
		t.Errorf("unsatisfied should be cluster-low only, got %+v", r.Unsatisfied)
	}
}

func idN(n int) machine.ID {
	const digits = "0123456789"
	return machine.ID("i-" + string(digits[n/10]) + string(digits[n%10]))
}

// M44.4 Drop D regression: many CRs sharing a fingerprint each become a
// separate Need with Count=1 (different ownerRef UIDs → different Groups
// → Aggregate keeps them separate). Phase 1 must compute deficit at
// the (cluster, fingerprint) level, distributed across the Needs in
// priority order — not per-Need.
//
// Pre-Drop-D, with Count=1 per Need and 5 already configured for the
// fingerprint, every Need's per-need deficit was `1 - 5 = -4` and Phase
// 1 emitted zero actions even though 25 Needs were unsatisfied. This
// test would have caught the cliff scaleway-50k hit at ~19 K bootstraps.
func TestPhase1_ManyNeedsSharingFingerprint_DistributesAcrossNeeds(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// 5 already configured for the fingerprint (already-bound supply).
	pf := gpuProfile(1_000_000)
	fp := pf.Fingerprint()
	for i := 0; i < 5; i++ {
		m := gpuMachine(idN(i), machine.StateConfigured, "cluster-a", machine.CapacityTypeBareMetal, 0)
		m.AssignedNeedFingerprint = fp
		_ = inv.Insert(m)
	}
	// 25 Idle waiting to be picked up.
	for i := 5; i < 30; i++ {
		_ = inv.Insert(gpuMachine(idN(i), machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))
	}

	// 30 Needs, each one 8-GPU unit, each with a unique Group (the
	// load-driver case: every Pod is its own ownerRef UID).
	allNeeds := make([]needs.Need, 0, 30)
	for i := 0; i < 30; i++ {
		allNeeds = append(allNeeds, needs.Need{
			ClusterID:          "cluster-a",
			Profile:            pf,
			AggregateResources: gpuUnit,
			MinUnit:            gpuUnit,
			Group:              "pod-" + string(rune('a'+i%26)),
		})
	}
	r := decision.Phase1(inv.Snapshot(), allNeeds)

	// 5 already configured + 25 freshly emitted = 30 total demand met.
	if got := len(r.Actions); got != 25 {
		t.Fatalf("actions = %d, want 25 (30 demand - 5 supply)", got)
	}
	for _, a := range r.Actions {
		if a.Kind != decision.ActionKindBootstrap {
			t.Errorf("expected Bootstrap, got %s", a.Kind)
		}
	}
	if len(r.Unsatisfied) != 0 {
		t.Errorf("unsatisfied = %d, want 0 (Idle pool covers everything)", len(r.Unsatisfied))
	}
}

// TestPhase1_DenseMachine_OneAbsorbsManyPods exercises the M45.1 vector
// math: a single Configured machine with Allocatable > Profile.Resources
// absorbs `density` Pods of demand. The shard emits fewer Bootstraps
// than Need.Count when matching machines pack multiple replicas.
func TestPhase1_DenseMachine_OneAbsorbsManyPods(t *testing.T) {
	t.Parallel()

	// Per-replica unit: each Pod wants 1 CPU + 4 GiB.
	unit := []needs.ResourceQty{
		{Name: "cpu", Quantity: "1"},
		{Name: "memory", Quantity: "4Gi"},
	}
	profile := needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"c6a.4xlarge"},
		}},
		nil,
		1000,
		needs.PenaltyBucket64,
		needs.PenaltyBucketPinned,
	)

	// One Configured machine, already-bound to a Need with this profile's
	// fingerprint. Allocatable is c6a.4xlarge-shaped: 16 CPU / 32 GiB,
	// density = 8 (memory bottleneck).
	configuredID := machine.ID("dense-1")
	m := machine.Machine{
		ID:    configuredID,
		State: machine.StateConfigured,
		Host:  machine.HostRef{Provider: "fake", Ref: "dense-1"},
		Profile: machine.Profile{
			InstanceType: "c6a.4xlarge",
			Resources:    map[string]string{"cpu": "1", "memory": "4Gi"},
		},
		Cluster:                 "cluster-A",
		AssignedNeedFingerprint: profile.Fingerprint(),
		Allocatable:             map[string]string{"cpu": "16", "memory": "32Gi"},
	}
	inv := inventory.New()
	if err := inv.Insert(m); err != nil {
		t.Fatalf("insert: %v", err)
	}
	snap := inv.Snapshot()

	// Demand: 10 Pods (aggregate 10 CPU / 40 GiB). The existing dense
	// machine covers 8 Pods of it (32 GiB allocatable / 4 GiB per Pod);
	// we should see Phase 1 emit Bootstraps only for the remaining 2.
	need := needs.Need{
		ClusterID:          "cluster-A",
		Profile:            profile,
		AggregateResources: needs.ScaleResources(unit, 10),
		MinUnit:            unit,
	}
	res := decision.Phase1(snap, []needs.Need{need})

	// With no Idle/Speculative inventory in the snap, Phase 1 can't fill
	// the remaining 2 Pods — they become Unsatisfied. The point of the
	// test is to confirm the deficit is the residual aggregate vector
	// 10·unit - allocatable = {cpu:10-16→0, memory:40Gi-32Gi=8Gi}, i.e.
	// memory is the binding dimension and 8 GiB == 2 Pods remain.
	if len(res.Actions) != 0 {
		t.Errorf("no Idle/Spec in inventory; expected 0 actions, got %d", len(res.Actions))
	}
	if len(res.Unsatisfied) != 1 {
		t.Fatalf("expected 1 unsatisfied Need, got %d", len(res.Unsatisfied))
	}
	mem := ""
	for _, r := range res.Unsatisfied[0].Deficit {
		if r.Name == "memory" {
			mem = r.Quantity
		}
	}
	if mem != "8Gi" {
		t.Errorf("Unsatisfied.Deficit memory = %q, want 8Gi (10 Pods - density 8 absorbed)", mem)
	}
}

// TestPhase1_PerPodCRs_ClaimGreedilyOncePerMachine encodes the
// ADR-0027 supply contract: aggregate supply is `Σ Machine.Allocatable`
// over matching machines, counted *once per machine*, and the allocator
// claims each unit of supply to exactly one demand. The M45.4
// "surplus-credit" behaviour — where a single bootstrapped machine's
// spare density was credited to peer Needs of the same fingerprint —
// was the over-credit bug ADR-0027 removed (ADR-0027 §"What goes wrong",
// the surplus-credit logic absorbed genuinely-stuck Pods against phantom
// capacity).
//
// Consequence for Pod-mode load: 100 per-Pod CRs, each its own ownerRef
// UID → its own Group → its own single-unit Need, do NOT pack onto
// fewer machines within one cycle. Each Need's take() claims a whole
// machine, so 50 Idle machines satisfy 50 Needs and the other 50 become
// Unsatisfied. The convergence loop still closes — every cycle the
// roll-up re-lists the stuck Pods and the Idle/Speculative pools refill
// — but it does so without the phantom over-credit. To pack 100 Pods of
// one workload onto 10 dense machines the operator must aggregate them
// into one Need (shared Group); that path is the second half of this
// test.
func TestPhase1_PerPodCRs_ClaimGreedilyOncePerMachine(t *testing.T) {
	t.Parallel()

	unit := []needs.ResourceQty{
		{Name: "cpu", Quantity: "1"},
		{Name: "memory", Quantity: "4Gi"},
	}
	profile := needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"c6a.4xlarge"},
		}},
		nil,
		1000,
		needs.PenaltyBucket64,
		needs.PenaltyBucketPinned,
	)

	newInv := func() *inventory.Snapshot {
		inv := inventory.New()
		for i := 0; i < 50; i++ {
			m := machine.Machine{
				ID:    machine.ID("idle-" + string(rune('A'+i%26)) + string(rune('a'+i/26))),
				State: machine.StateIdle,
				Host:  machine.HostRef{Provider: "fake", Ref: "idle"},
				Profile: machine.Profile{
					InstanceType: "c6a.4xlarge",
					Resources:    map[string]string{"cpu": "1", "memory": "4Gi"},
				},
				Allocatable: map[string]string{"cpu": "10", "memory": "40Gi"},
			}
			if err := inv.Insert(m); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		return inv.Snapshot()
	}

	// Per-Pod shape: 100 distinct Groups, each a single-unit Need. Each
	// take() claims a whole machine — 50 machines → 50 Bootstraps, 50
	// Unsatisfied. No surplus credit.
	perPod := make([]needs.Need, 0, 100)
	for i := 0; i < 100; i++ {
		perPod = append(perPod, needs.Need{
			ClusterID:          "cluster-A",
			Profile:            profile,
			AggregateResources: unit,
			MinUnit:            unit,
			Group:              "pod-" + string(rune('A'+i%26)) + string(rune('a'+i/26)),
		})
	}
	res := decision.Phase1(newInv(), perPod)
	if len(res.Actions) != 50 {
		t.Errorf("per-Pod CRs: Phase 1 emitted %d Bootstraps, want 50 (one machine claimed per Need, 50 Idle)", len(res.Actions))
	}
	if len(res.Unsatisfied) != 50 {
		t.Errorf("per-Pod CRs: expected 50 Unsatisfied (no surplus credit under ADR-0027), got %d", len(res.Unsatisfied))
	}

	// Aggregated shape: the 100 Pods of one workload share a Group, so
	// the operator's rollup folds them into one Need for 100 units. That
	// Need's deficit vector is diffed against summed Allocatable, so 10
	// dense machines (density 10) cover it exactly.
	aggregated := []needs.Need{{
		ClusterID:          "cluster-A",
		Profile:            profile,
		AggregateResources: needs.ScaleResources(unit, 100),
		MinUnit:            unit,
		Group:              "workload-1",
	}}
	resAgg := decision.Phase1(newInv(), aggregated)
	if len(resAgg.Actions) != 10 {
		t.Errorf("aggregated Need: Phase 1 emitted %d Bootstraps, want 10 (100 units / density 10)", len(resAgg.Actions))
	}
	if len(resAgg.Unsatisfied) != 0 {
		t.Errorf("aggregated Need: expected 0 Unsatisfied (inventory has headroom), got %d", len(resAgg.Unsatisfied))
	}
}
