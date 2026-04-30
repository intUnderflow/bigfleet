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
// from idle or speculative inventory. The shard either escalates to
// Phase 2 (in-shard preemption) or, if Phase 2 also fails, emits a
// Shortfall to the coordinator.
type UnsatisfiedNeed struct {
	Need    needs.Need
	Deficit int
}

// Phase1 walks the priority-sorted needs and emits Bootstrap (idle →
// configured) and Provision (speculative → configured) actions to fill
// each cluster's deficit. The function does not mutate the inventory
// snapshot; the shard applies the resulting actions through the inventory
// itself once the provider acknowledges them.
//
// Within a cycle, Phase 1 reserves machines as it picks them so two
// needs cannot both claim the same machine. Reservations are local to
// the call.
func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
	// Snapshot caller may pass needs in any order; sort by priority desc
	// so a high-priority cluster gets first refusal of available capacity.
	sorted := make([]needs.Need, len(allNeeds))
	copy(sorted, allNeeds)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Profile.Priority() > sorted[j].Profile.Priority()
	})

	claimed := newClaimSet()
	idleByState := snap.ListByState(machine.StateIdle)
	specByState := snap.ListByState(machine.StateSpeculative)

	result := Phase1Result{}
	for _, n := range sorted {
		have := snap.CountByClusterState(n.ClusterID, machine.StateConfigured)
		// Excess (have > need.count) is Phase 3's problem.
		deficit := n.Count - have
		if deficit <= 0 {
			continue
		}

		// Idle first: cheapest path (one Configure call, no Create).
		idleCands := candidatesFromList(idleByState, n.Profile, claimed)
		sortIdleCandidates(idleCands)
		take := minInt(len(idleCands), deficit)
		for _, m := range idleCands[:take] {
			claimed.add(m.ID)
			result.Actions = append(result.Actions, Action{
				Kind:      ActionKindBootstrap,
				MachineID: m.ID,
				Cluster:   n.ClusterID,
				Reason:    "phase1.idle",
			})
		}
		deficit -= take
		if deficit == 0 {
			continue
		}

		// Fall back to speculative: pick by lowest effective_cost using
		// the bucket's upper bound as the penalty estimate.
		specCands := candidatesFromList(specByState, n.Profile, claimed)
		penalty := BucketUpperBoundDollars(n.Profile.InterruptionPenaltyBucket())
		sortSpeculativeCandidates(specCands, penalty)
		take = minInt(len(specCands), deficit)
		for _, m := range specCands[:take] {
			claimed.add(m.ID)
			result.Actions = append(result.Actions, Action{
				Kind:      ActionKindProvision,
				MachineID: m.ID,
				Cluster:   n.ClusterID,
				Reason:    "phase1.speculative",
			})
		}
		deficit -= take

		if deficit > 0 {
			result.Unsatisfied = append(result.Unsatisfied, UnsatisfiedNeed{
				Need:    n,
				Deficit: deficit,
			})
		}
	}

	return result
}

// claimSet tracks which machines a Phase 1 pass has already promised
// out, so two competing needs in the same cycle don't double-book.
type claimSet struct {
	claims map[machine.ID]struct{}
}

func newClaimSet() *claimSet {
	return &claimSet{claims: make(map[machine.ID]struct{})}
}

func (c *claimSet) add(id machine.ID) { c.claims[id] = struct{}{} }
func (c *claimSet) has(id machine.ID) bool {
	_, ok := c.claims[id]
	return ok
}

// candidatesFromList filters the input slice to machines whose profile
// satisfies the need and that haven't been claimed already.
func candidatesFromList(in []machine.Machine, p needs.Profile, claimed *claimSet) []machine.Machine {
	out := make([]machine.Machine, 0, len(in))
	for _, m := range in {
		if claimed.has(m.ID) {
			continue
		}
		if !MatchProfile(p, m) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// sortIdleCandidates orders idle candidates by ascending effective cost
// of holding (a placeholder that we can refine later). For now: bare
// metal first (cost 0), then ascending price. Ties broken by ID for
// determinism. Reclamation-penalty tiebreak is a Phase-3 concern; in
// Phase 1 every idle machine is fungible apart from price.
func sortIdleCandidates(s []machine.Machine) {
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].PricePerHour != s[j].PricePerHour {
			return s[i].PricePerHour < s[j].PricePerHour
		}
		return s[i].ID < s[j].ID
	})
}

// sortSpeculativeCandidates orders speculative candidates by ascending
// effective_cost given the workload's interruption penalty.
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
