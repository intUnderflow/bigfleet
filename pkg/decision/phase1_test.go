package decision_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// gpuProfile returns the standard "8-GPU H100" need profile used in most
// of the paper's examples.
func gpuProfile(priority int32) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"a3-highgpu-8g"},
		}},
		nil, nil, priority,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
}

func gpuMachine(id machine.ID, state machine.State, cluster machine.ClusterID, capType machine.CapacityType, price float64) machine.Machine {
	m := machine.Machine{
		ID:    id,
		State: state,
		Profile: machine.Profile{
			InstanceType: "a3-highgpu-8g",
			Zone:         "us-east-1a",
			CapacityType: capType,
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
		{ClusterID: "cluster-a", Profile: gpuProfile(1_000_000), Count: 3},
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
		{ClusterID: "cluster-a", Profile: gpuProfile(1_000_000), Count: 1},
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
		nil, nil, 1_000_000,
		needs.PenaltyBucket8388608,
		needs.PenaltyBucketPinned,
	)
	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-a", Profile: pf, Count: 1},
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
		{ClusterID: "cluster-a", Profile: gpuProfile(1_000_000), Count: 1},
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
		{ClusterID: "cluster-a", Profile: gpuProfile(1_000_000), Count: 2},
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
		{ClusterID: "cluster-a", Profile: gpuProfile(1_000_000), Count: 5},
	})
	if got := len(r.Actions); got != 2 {
		t.Fatalf("actions = %d, want 2", got)
	}
	if got := len(r.Unsatisfied); got != 1 || r.Unsatisfied[0].Deficit != 3 {
		t.Errorf("unsatisfied = %+v, want 1 entry with deficit=3", r.Unsatisfied)
	}
}

// High-priority need wins shared idle pool.
func TestPhase1_HighPriorityWinsContestedPool(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(gpuMachine("i-1", machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))
	_ = inv.Insert(gpuMachine("i-2", machine.StateIdle, "", machine.CapacityTypeBareMetal, 0))

	r := decision.Phase1(inv.Snapshot(), []needs.Need{
		{ClusterID: "cluster-low", Profile: gpuProfile(0), Count: 2},
		{ClusterID: "cluster-high", Profile: gpuProfile(1_000_000), Count: 2},
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
