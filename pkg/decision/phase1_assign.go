package decision

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Phase1Result is the output of a Phase 1 pass: the actions to execute
// and the unsatisfied per-need deficits Phase 2 may resolve via
// preemption.
type Phase1Result struct {
	Actions     []Action
	Unsatisfied []UnsatisfiedNeed
}

// UnsatisfiedNeed is a Need whose Phase 1 deficit could not be filled
// from idle or speculative inventory.
type UnsatisfiedNeed struct {
	Need    needs.Need
	Deficit int
}

// Phase1 walks the priority-sorted needs and emits Bootstrap (idle →
// configured) and Provision (speculative → configured) actions to fill
// each cluster's deficit.
//
// Performance: backed by phase1Allocator, which caches per-Profile
// candidate pools across the Needs loop and shares a global claimed set
// so high-priority Needs drain inventory before low-priority ones see
// it. Bench (pkg/shard/cycle_bench_test.go) at 500K inventory + 50K
// demand: ~948 s pre-M11.16 (just the M11.15 instance-type index) vs
// the new allocator's expected order-of-magnitude improvement.
func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
	sorted := make([]needs.Need, len(allNeeds))
	copy(sorted, allNeeds)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Profile.Priority() > sorted[j].Profile.Priority()
	})

	alloc := newPhase1Allocator(snap)

	result := Phase1Result{}
	for _, n := range sorted {
		have := snap.CountByClusterState(n.ClusterID, machine.StateConfigured)
		deficit := n.Count - have
		if deficit <= 0 {
			continue
		}
		profile := n.Profile

		// Idle first: cheapest path (one Configure call, no Create).
		idle := alloc.take(machine.StateIdle, profile, deficit, sortIdleCandidates)
		for _, m := range idle {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindBootstrap,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.idle",
			})
		}
		deficit -= len(idle)
		if deficit == 0 {
			continue
		}

		// Fall back to speculative: pick by lowest effective_cost.
		penalty := BucketUpperBoundDollars(profile.InterruptionPenaltyBucket())
		spec := alloc.take(machine.StateSpeculative, profile, deficit, func(s []machine.Machine) {
			sortSpeculativeCandidates(s, penalty)
		})
		for _, m := range spec {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindProvision,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.speculative",
			})
		}
		deficit -= len(spec)

		if deficit > 0 {
			result.Unsatisfied = append(result.Unsatisfied, UnsatisfiedNeed{
				Need:    n,
				Deficit: deficit,
			})
		}
	}

	return result
}

// candidatePool returns the inventory slice to consider for a given
// Profile, using the inventory's instance-type index when the Need's
// selectors pin to one or more `node.kubernetes.io/instance-type`
// values. Falls back to the all-state list when the selector is missing
// or uses an operator we can't index against (NotIn, Exists,
// DoesNotExist, Same). The fallback preserves correctness; the speedup
// only kicks in for the common-case In selector that real workloads
// almost always carry.
func candidatePool(snap *inventory.Snapshot, state machine.State, p needs.Profile) []machine.Machine {
	types := pinnedInstanceTypes(p)
	if types == nil {
		return snap.ListByState(state)
	}
	if len(types) == 1 {
		return snap.ListByStateInstanceType(state, types[0])
	}
	var out []machine.Machine
	for _, t := range types {
		out = append(out, snap.ListByStateInstanceType(state, t)...)
	}
	return out
}

// pinnedInstanceTypes returns the explicit instance-type values from
// a Profile's `node.kubernetes.io/instance-type In [...]` requirement,
// or nil if the Profile doesn't pin to a finite set we can index on.
func pinnedInstanceTypes(p needs.Profile) []string {
	for _, r := range p.Requirements() {
		if r.Key != "node.kubernetes.io/instance-type" {
			continue
		}
		if r.Operator == needs.OperatorIn && len(r.Values) > 0 {
			return r.Values
		}
		return nil
	}
	return nil
}

func sortIdleCandidates(s []machine.Machine) {
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].PricePerHour != s[j].PricePerHour {
			return s[i].PricePerHour < s[j].PricePerHour
		}
		return s[i].ID < s[j].ID
	})
}

func sortSpeculativeCandidates(s []machine.Machine, interruptionPenaltyDollars float64) {
	sort.SliceStable(s, func(i, j int) bool {
		ai := EffectiveCost(s[i], interruptionPenaltyDollars)
		aj := EffectiveCost(s[j], interruptionPenaltyDollars)
		if ai != aj {
			return ai < aj
		}
		return s[i].ID < s[j].ID
	})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
