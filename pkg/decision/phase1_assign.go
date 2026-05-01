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
// Performance: per-Need candidate scans use the inventory's
// instance-type index when the Need's selector pins to one or more
// instance types — turning the dominant per-Need walk from O(N) over
// total inventory into O(K) over the matching type's bucket. Bench:
// pkg/shard/cycle_bench_test.go shows ~2× cycle-time improvement at
// 50K inventory vs the unindexed baseline. Going further (caching
// per-Profile candidate pools across needs that share fingerprints) is
// tracked as M11.16; doing it correctly requires coordinating claims
// across overlapping Profiles, which is a deeper refactor.
func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
	sorted := make([]needs.Need, len(allNeeds))
	copy(sorted, allNeeds)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Profile.Priority() > sorted[j].Profile.Priority()
	})

	claimed := newClaimSet()

	result := Phase1Result{}
	for _, n := range sorted {
		have := snap.CountByClusterState(n.ClusterID, machine.StateConfigured)
		deficit := n.Count - have
		if deficit <= 0 {
			continue
		}

		idleCands := candidatesFromList(candidatePool(snap, machine.StateIdle, n.Profile), n.Profile, claimed)
		sortIdleCandidates(idleCands)
		take := minInt(len(idleCands), deficit)
		profile := n.Profile
		for _, m := range idleCands[:take] {
			claimed.add(m.ID)
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindBootstrap,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.idle",
			})
		}
		deficit -= take
		if deficit == 0 {
			continue
		}

		specCands := candidatesFromList(candidatePool(snap, machine.StateSpeculative, n.Profile), n.Profile, claimed)
		penalty := BucketUpperBoundDollars(n.Profile.InterruptionPenaltyBucket())
		sortSpeculativeCandidates(specCands, penalty)
		take = minInt(len(specCands), deficit)
		for _, m := range specCands[:take] {
			claimed.add(m.ID)
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindProvision,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.speculative",
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
