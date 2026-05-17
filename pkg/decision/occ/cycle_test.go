package occ_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// configuredMachine constructs a Configured machine in cluster with the
// 4-cpu / 16Gi shape used throughout the OCC cycle tests.
func configuredMachine(id machine.ID, cluster machine.ClusterID) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: machine.StateConfigured,
		Profile: machine.Profile{
			InstanceType: "m5.large",
			Zone:         "us-east-1a",
			CapacityType: machine.CapacityTypeOnDemand,
			Resources:    map[string]string{"cpu": "4", "memory": "16Gi"},
		},
		Cluster:      cluster,
		PricePerHour: 1.0,
		Host:         machine.HostRef{Provider: "fake", Ref: string(id)},
	}
}

func cpuNeed(cluster machine.ClusterID, profile needs.Profile, cpuStr string) needs.Need {
	return needs.Need{
		ClusterID:          cluster,
		Profile:            profile,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: cpuStr}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
	}
}

func TestRunCycle_EmptyNeeds(t *testing.T) {
	t.Parallel()
	snap := snapWith()
	result := occ.RunCycle(snap, nil)
	if len(result.Results) != 0 {
		t.Fatalf("RunCycle on empty Needs returned %d results, want 0", len(result.Results))
	}
}

func TestRunCycle_FullyAbsorbedByExistingSupply(t *testing.T) {
	t.Parallel()
	snap := snapWith(configuredMachine("m1", "c1"))
	n := cpuNeed("c1", smallProfile(100), "4")
	result := occ.RunCycle(snap, []needs.Need{n})

	if len(result.Results) != 1 {
		t.Fatalf("Results len = %d, want 1", len(result.Results))
	}
	r := result.Results[0]
	if len(r.BootstrapMachines) != 0 {
		t.Errorf("BootstrapMachines = %v, want empty (Configured supply absorbed)", r.BootstrapMachines)
	}
	if len(r.ProvisionMachines) != 0 {
		t.Errorf("ProvisionMachines = %v, want empty", r.ProvisionMachines)
	}
	if r.Unsatisfied {
		t.Errorf("Unsatisfied = true, want false")
	}
	if !needs.IsZero(r.Deficit) {
		t.Errorf("Deficit = %v, want zero", r.Deficit)
	}
}

func TestRunCycle_CoveredByIdle(t *testing.T) {
	t.Parallel()
	snap := snapWith(
		idleMachine("idle-1", 1.0, nil),
		idleMachine("idle-2", 2.0, nil),
	)
	n := cpuNeed("c1", smallProfile(100), "4")
	result := occ.RunCycle(snap, []needs.Need{n})

	r := result.Results[0]
	if len(r.BootstrapMachines) != 1 {
		t.Fatalf("BootstrapMachines = %v, want exactly 1", r.BootstrapMachines)
	}
	if r.BootstrapMachines[0] != "idle-1" {
		t.Errorf("BootstrapMachines[0] = %q, want idle-1 (cheapest)", r.BootstrapMachines[0])
	}
	if r.Unsatisfied {
		t.Errorf("Unsatisfied = true, want false")
	}
}

func TestRunCycle_PartiallyCovered_RecordsUnsatisfied(t *testing.T) {
	t.Parallel()
	// One Idle machine (4 cpu), Need wants 8 cpu → can cover half.
	snap := snapWith(idleMachine("idle-1", 1.0, nil))
	n := cpuNeed("c1", smallProfile(100), "8")
	result := occ.RunCycle(snap, []needs.Need{n})

	r := result.Results[0]
	if !r.Unsatisfied {
		t.Errorf("Unsatisfied = false, want true (deficit not fully covered)")
	}
	if len(r.BootstrapMachines) != 1 {
		t.Errorf("BootstrapMachines = %v, want 1", r.BootstrapMachines)
	}
	// Deficit should be 4 cpu remaining.
	want := []needs.ResourceQty{{Name: "cpu", Quantity: "4"}}
	if !equalResources(r.Deficit, want) {
		t.Errorf("Deficit = %v, want %v", r.Deficit, want)
	}
}

func TestRunCycle_NoInventory_AllUnsatisfied(t *testing.T) {
	t.Parallel()
	snap := snapWith()
	n := cpuNeed("c1", smallProfile(100), "4")
	result := occ.RunCycle(snap, []needs.Need{n})

	r := result.Results[0]
	if !r.Unsatisfied {
		t.Errorf("Unsatisfied = false, want true (no inventory)")
	}
	if len(r.BootstrapMachines)+len(r.ProvisionMachines) != 0 {
		t.Errorf("got committed machines from empty inventory: %v / %v",
			r.BootstrapMachines, r.ProvisionMachines)
	}
}

func TestRunCycle_PriorityWinsContestedPool(t *testing.T) {
	t.Parallel()
	// One Idle machine; two Needs racing for it. The higher-priority
	// Need should win regardless of OCC commit order.
	snap := snapWith(idleMachine("idle-1", 1.0, nil))
	highPri := cpuNeed("c-high", smallProfile(500), "4")
	lowPri := cpuNeed("c-low", smallProfile(100), "4")

	for trial := 0; trial < 16; trial++ {
		result := occ.RunCycle(snap, []needs.Need{lowPri, highPri})
		var winner machine.ClusterID
		for _, r := range result.Results {
			if len(r.BootstrapMachines) == 1 {
				winner = r.Need.ClusterID
				break
			}
		}
		if winner != "c-high" {
			t.Fatalf("trial %d: idle machine went to %q, want c-high",
				trial, winner)
		}
	}
}

func TestRunCycle_DispatchScalesToWorkerCount(t *testing.T) {
	t.Parallel()
	// 100 Needs, each cluster has one Idle machine. Verify all get
	// processed (none lost in the queue).
	var machines []machine.Machine
	var ns []needs.Need
	for i := 0; i < 100; i++ {
		cid := machine.ClusterID("c-" + idStr(i))
		mid := machine.ID("m-" + idStr(i))
		machines = append(machines, idleMachine(mid, float64(i)+1, nil))
		_ = cid
		ns = append(ns, cpuNeed(cid, smallProfile(int32(100+i)), "4"))
	}
	snap := snapWith(machines...)

	result := occ.RunCycle(snap, ns, occ.WithWorkers(8))
	if len(result.Results) != 100 {
		t.Fatalf("Results len = %d, want 100", len(result.Results))
	}
	// All 100 Idle machines should have been claimed.
	total := 0
	for _, r := range result.Results {
		total += len(r.BootstrapMachines)
	}
	if total != 100 {
		t.Errorf("total BootstrapMachines = %d, want 100", total)
	}
}

func TestRunCycle_OutcomeEquivalenceAcrossRuns(t *testing.T) {
	t.Parallel()
	// Outcome-equivalence (ADR-0029): across multiple runs of the
	// same inputs, every Need's satisfied/unsatisfied bit is
	// invariant. Specific machine IDs and (under concurrent
	// displacement races) the exact machine count per Need may
	// vary; the OCC contract guarantees the satisfaction outcome
	// is deterministic modulo commit ordering.
	var machines []machine.Machine
	for i := 0; i < 20; i++ {
		machines = append(machines, idleMachine(machine.ID("m-"+idStr(i)), float64(i)+1, nil))
	}
	snap := snapWith(machines...)
	var ns []needs.Need
	for i := 0; i < 5; i++ {
		cid := machine.ClusterID("c-" + idStr(i))
		ns = append(ns, cpuNeed(cid, smallProfile(int32(100+i*10)), "8"))
	}

	// First run establishes the reference satisfaction map.
	want := make(map[machine.ClusterID]bool)
	for _, r := range occ.RunCycle(snap, ns).Results {
		want[r.Need.ClusterID] = !r.Unsatisfied
	}
	for trial := 0; trial < 10; trial++ {
		for _, r := range occ.RunCycle(snap, ns).Results {
			cid := r.Need.ClusterID
			got := !r.Unsatisfied
			if got != want[cid] {
				t.Errorf("trial %d cluster %s: satisfied=%v, want %v",
					trial, cid, got, want[cid])
			}
		}
	}
}

// idStr renders an integer as a hex-ish string used for unique IDs
// in concurrency tests; cheaper than strconv.Itoa for the inline use
// here and avoids zero-padding surprises.
func idStr(i int) string {
	if i == 0 {
		return "0"
	}
	const hex = "0123456789abcdef"
	buf := make([]byte, 0, 4)
	for i > 0 {
		buf = append([]byte{hex[i%16]}, buf...)
		i /= 16
	}
	return string(buf)
}

// equalResources compares resource vectors after stripping zero-
// quantity entries (which subtraction can leave behind when a
// machine has dimensions the demand vector doesn't ask for).
func equalResources(a, b []needs.ResourceQty) bool {
	am := nonZeroMap(a)
	bm := nonZeroMap(b)
	if len(am) != len(bm) {
		return false
	}
	for k, v := range am {
		if bm[k] != v {
			return false
		}
	}
	return true
}

func nonZeroMap(v []needs.ResourceQty) map[string]string {
	out := make(map[string]string, len(v))
	for _, q := range v {
		if q.Quantity == "0" || q.Quantity == "" {
			continue
		}
		out[q.Name] = q.Quantity
	}
	return out
}
