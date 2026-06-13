package decision

import (
	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
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

	// Claimed is the cycle's final claimed-set: every machine the
	// attribution walk counted toward a Need — Configured/Configuring
	// credited by the pre-pass, Idle/Speculative committed by workers.
	// ADR-0045: this is the engine's single supply attribution; Phase 3
	// takes it as input and reclaims exactly the Configured machines
	// absent from it (the cluster's bound capacity in excess of its
	// demand). One arithmetic, two consumers — the phases cannot
	// disagree about which machine serves which Need.
	Claimed map[machine.ID]struct{}

	// SatisfiedGangs carries the chosen domain + claimed machine-set of
	// every SATISFIED Same-Need this cycle (#325 / the M77g diagnosis).
	// The existing UnsatisfiedNeed.SameDomain only surfaces the choice
	// for UNsatisfied gangs; bigfleet-uber #61/#63 established the
	// Bootstrap≈Reclaim oscillation occurs while gangs are satisfied
	// (p1_unsatisfied_same=0 every cycle), so the oscillating domain
	// flip is invisible to the unsatisfied-only probe. This is the
	// satisfied equivalent: read-only/diagnostic, consumed only by the
	// flag-gated phase-attribution probe, no engine behaviour rides on
	// it.
	SatisfiedGangs []SatisfiedGang
}

// SatisfiedGang is the per-cycle attribution of one SATISFIED Same-Need
// (#325): the joint pre-pass's chosen domain and the complete set of
// machines claimed toward the gang. Mirrors UnsatisfiedNeed.SameDomain
// for the satisfied case so the M77g field diagnostic can observe,
// cycle-to-cycle, whether a served gang's chosen_domain flips and
// whether its claimed machines churn — the anatomy of the static-demand
// reclaim↔re-bootstrap loop ChooseSameBucket rule 2 was meant to pin.
type SatisfiedGang struct {
	Need     needs.Need
	Domain   string
	Machines []machine.ID
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
	SameDomain      string
	Acquired        int
	SameSatisfiable bool
}

// Phase1 emits Bootstrap (idle → configured) and Provision
// (speculative → configured) actions to fill each Need's deficit,
// then records any unfilled residual as an UnsatisfiedNeed for
// Phase 2 / shortfall escalation.
//
// The accounting rule is ADR-0045's: capacity counts for a cluster
// iff it is bound to that cluster. The pre-pass credits the cluster's
// Configured AND Configuring machines at gross EffectiveAllocatable —
// a binding counts from the moment it is made, before the node
// exists, so double-supply is impossible by construction. Bound <
// demand → fulfill the difference; bound ≥ demand → BigFleet's job is
// done. Deliberately NOT modeled: per-machine consumption, residual
// fit, whether the cluster's scheduler can actually place its pods on
// the bound capacity — BigFleet is not a scheduler, and
// satisfied-but-stuck (fragmentation at equal priority) is the
// cluster's problem to resolve.
//
// As of ADR-0029, Phase 1 runs as an Omega-style optimistic-
// concurrency-control scheduler: workers race over a shared queue
// of Needs, each proposing to a commit broker that enforces
// priority on conflict (rather than a single-threaded
// priority-sorted outer loop). The cycle barrier guarantees a
// coherent post-Phase-1 claimed-set, which is also what Phase 3
// diffs against the Configured inventory (ADR-0045 single
// attribution).
//
// The cost formula and ADR-0027 resource-vector demand model are
// unchanged from the pre-OCC Phase 1; only the iteration shape
// changed.
func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
	cycle := occ.RunCycle(snap, allNeeds)

	result := Phase1Result{Claimed: cycle.Claimed}
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
				SourceGroup:   r.Need.Group, // ADR-0051: gang attribution
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
				SourceGroup:   r.Need.Group, // ADR-0051: gang attribution
				Reason:        "phase1.speculative",
			})
		}

		if r.Unsatisfied {
			result.Unsatisfied = append(result.Unsatisfied, UnsatisfiedNeed{
				Need:            *r.Need,
				Deficit:         r.Deficit,
				SameDomain:      r.SameDomain,
				Acquired:        len(r.BootstrapMachines) + len(r.ProvisionMachines),
				SameSatisfiable: r.SameSatisfiable,
			})
			continue
		}
		// #325: a SATISFIED Same-Need — record its domain + full claimed
		// set for the satisfied-gang probe. This is where the M77g
		// oscillation hides: p1_unsatisfied_same is 0 every cycle, so the
		// domain flip never reaches the UnsatisfiedNeed path above.
		if _, ok := occ.SameRequirementKey(profile); ok {
			result.SatisfiedGangs = append(result.SatisfiedGangs, SatisfiedGang{
				Need:     *r.Need,
				Domain:   r.SameDomain,
				Machines: r.ClaimedMachines,
			})
		}
	}
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
