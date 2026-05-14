package decision

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
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
// from existing supply, idle, or speculative inventory.
//
// ADR-0027: Deficit is the residual resource vector — what aggregate
// capacity is still missing — not a Pod count.
type UnsatisfiedNeed struct {
	Need    needs.Need
	Deficit []needs.ResourceQty
}

// Phase1 walks the priority-sorted needs and emits Bootstrap (idle →
// configured) and Provision (speculative → configured) actions to fill
// each cluster's deficit.
//
// ADR-0027: demand is a resource vector. For each Need the deficit is
// `AggregateResources` minus the EffectiveAllocatable of the existing
// matching machines, then minus what Idle and Speculative `take` can
// cover. Supply is credited via the allocator's global claimed set, so
// each machine's capacity is counted for exactly one Need — the
// per-fingerprint over-credit that masked real shortfalls is gone.
//
// Performance: backed by phase1Allocator, which caches per-Profile
// candidate pools across the Needs loop and shares the claimed set so
// high-priority Needs drain inventory before low-priority ones see it.
func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
	sorted := make([]needs.Need, len(allNeeds))
	copy(sorted, allNeeds)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Profile.Priority() > sorted[j].Profile.Priority()
	})

	alloc := newPhase1Allocator(snap)
	result := Phase1Result{}

	for _, n := range sorted {
		profile := n.Profile

		// Existing Configured / Configuring machines in this cluster that
		// match the Need's requirements and can host one MinUnit credit
		// toward the demand vector first. Each is claimed, so a peer Need
		// of the same (cluster, fingerprint) — distinct only by Group —
		// can't double-count it. This claimed-set credit replaces the old
		// per-fingerprint supplyRemaining bookkeeping.
		deficit := alloc.creditExistingSupply(n.ClusterID, profile, n.AggregateResources, n.MinUnit)
		if needs.IsZero(deficit) {
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("absorbed_by_supply").Inc()
			continue
		}

		// Idle first: cheapest path (one Configure call, no Create).
		idle := alloc.take(machine.StateIdle, profile, deficit, n.MinUnit)
		if len(idle) == 0 {
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("take_returned_zero").Inc()
		} else {
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("emitted_idle").Inc()
		}
		for _, m := range idle {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindBootstrap,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.idle",
			})
			deficit = needs.SubResources(deficit, needs.ResourceQtysFromMap(m.EffectiveAllocatable()))
		}
		if needs.IsZero(deficit) {
			continue
		}

		// Fall back to speculative: pick by lowest effective_cost.
		spec := alloc.take(machine.StateSpeculative, profile, deficit, n.MinUnit)
		if len(spec) > 0 {
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("emitted_spec").Inc()
		}
		for _, m := range spec {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindProvision,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.speculative",
			})
			deficit = needs.SubResources(deficit, needs.ResourceQtysFromMap(m.EffectiveAllocatable()))
		}
		if needs.IsZero(deficit) {
			continue
		}

		metrics.ShardPhase1NeedOutcomes.WithLabelValues("unsatisfied").Inc()
		result.Unsatisfied = append(result.Unsatisfied, UnsatisfiedNeed{
			Need:    n,
			Deficit: deficit,
		})
	}

	metrics.ShardPhase1EmitsPerCycle.Observe(float64(len(result.Actions)))

	return result
}

// creditExistingSupply credits the Configured and Configuring machines
// in cluster that match profile's requirements and can host one minUnit
// against deficit, claiming each so a peer Need doesn't double-count it.
// It returns the remaining deficit vector.
//
// ADR-0027: supply is the sum of matching machines' EffectiveAllocatable,
// counted once per machine via the global claimed set — not a
// per-fingerprint Pod count. Configuring machines count too: their
// Bootstrap is in flight and committed to this demand, so crediting them
// here is what stops Phase 1 re-emitting on the next cycle (Drop H).
func (a *phase1Allocator) creditExistingSupply(
	cluster machine.ClusterID,
	profile needs.Profile,
	demand, minUnit []needs.ResourceQty,
) []needs.ResourceQty {
	remaining := demand
	for _, state := range []machine.State{machine.StateConfigured, machine.StateConfiguring} {
		for _, m := range a.snap.ListByClusterState(cluster, state) {
			if needs.IsZero(remaining) {
				return remaining
			}
			if _, claimed := a.claimed[m.ID]; claimed {
				continue
			}
			if !MatchProfile(profile, m) {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			if !needs.Covers(alloc, minUnit) {
				continue
			}
			a.claimed[m.ID] = struct{}{}
			remaining = needs.SubResources(remaining, alloc)
		}
	}
	return remaining
}

// pinnedInstanceTypes returns the explicit instance-type values from
// a Profile's `node.kubernetes.io/instance-type In [...]` requirement,
// or nil if the Profile doesn't pin to a finite set we can index on.
func pinnedInstanceTypes(p needs.Profile) []string {
	for _, r := range p.RequirementsRO() {
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
