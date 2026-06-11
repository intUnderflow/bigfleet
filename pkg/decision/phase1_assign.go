package decision

import (
	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
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
	// SameDomain is the joint pre-pass's chosen domain for a
	// Same-Profile Need ("" otherwise); Acquired is how many machines
	// the cycle claimed toward the Need before exhausting. Both feed
	// the ADR-0042 per-gang attribution probe — a flipping domain
	// with non-zero acquisition is the #56 churn loop.
	SameDomain string
	Acquired   int
}

// Phase1 emits Bootstrap (idle → configured) and Provision
// (speculative → configured) actions to fill each Need's deficit,
// then records any unfilled residual as an UnsatisfiedNeed for
// Phase 2 / shortfall escalation.
//
// As of ADR-0029, Phase 1 runs as an Omega-style optimistic-
// concurrency-control scheduler: workers race over a shared queue
// of Needs, each proposing to a commit broker that enforces
// priority on conflict (rather than a single-threaded
// priority-sorted outer loop). The cycle barrier guarantees a
// coherent post-Phase-1 claimed-set for Phase 2 / Phase 3 to read.
// The ADR-0027 stage 5.1 attribution invariant is preserved because
// both phases consume the same post-barrier claimed-set.
//
// The cost formula and ADR-0027 resource-vector demand model are
// unchanged from the pre-OCC Phase 1; only the iteration shape
// changed.
func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
	cycle := occ.RunCycle(snap, allNeeds)

	result := Phase1Result{}
	for _, r := range cycle.Results {
		if r.Need == nil {
			continue
		}
		profile := r.Need.Profile

		// Existing-supply credit is the OCC pre-pass; nothing to emit.
		// Idle commits become Bootstrap actions.
		for _, mid := range r.BootstrapMachines {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindBootstrap,
				MachineID:     mid,
				Cluster:       r.Need.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.idle",
			})
		}
		// Speculative commits become Provision actions.
		for _, mid := range r.ProvisionMachines {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindProvision,
				MachineID:     mid,
				Cluster:       r.Need.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.speculative",
			})
		}

		// Outcome counter classification matches the pre-OCC
		// allocator's categories so existing dashboards keep
		// working.
		switch {
		case r.Unsatisfied:
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("unsatisfied").Inc()
			result.Unsatisfied = append(result.Unsatisfied, UnsatisfiedNeed{
				Need:       *r.Need,
				Deficit:    r.Deficit,
				SameDomain: r.SameDomain,
				Acquired:   len(r.BootstrapMachines) + len(r.ProvisionMachines),
			})
		case len(r.BootstrapMachines) == 0 && len(r.ProvisionMachines) == 0:
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("absorbed_by_supply").Inc()
		case len(r.ProvisionMachines) > 0:
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("emitted_spec").Inc()
		default:
			metrics.ShardPhase1NeedOutcomes.WithLabelValues("emitted_idle").Inc()
		}
	}

	metrics.ShardPhase1EmitsPerCycle.Observe(float64(len(result.Actions)))
	return result
}

// pinnedInstanceTypes returns the explicit instance-type values from
// a Profile's `node.kubernetes.io/instance-type In [...]` requirement,
// or nil if the Profile doesn't pin to a finite set we can index on.
// Used by Phase 2 / Phase 3 for inventory bucket lookups; occ has its
// own copy (occ/poolcache.go) to avoid a decision↔occ import cycle.
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
