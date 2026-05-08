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

	// M44.4 Drop D: deficit math is per (cluster, fingerprint) supply,
	// distributed across Needs in priority order — not per-Need.
	//
	// Drop C counted machines with `AssignedNeedFingerprint == fp` but
	// compared that count to ONE Need's count (typically 1). When many
	// CRs share a fingerprint — the load-driver case: each Pod gets its
	// own CR with its own ownerRef UID, so Aggregate keeps every Need
	// with Count=1 — the count-1000-bound-200 cluster looked like
	// `deficit = 1 - 200 = -199` on every single one of the 1000 Needs.
	// Phase 1 skipped them all even though 800 were unsatisfied.
	// Surfaced as the scaleway-50k cliff at ~19 K bootstraps with
	// Phase 1 emit rate 0/sec.
	//
	// Correct math: for each (cluster, fingerprint), `have` is the
	// supply (configured machines bound to that fp); `total` is the
	// sum of Need.Count across every Need with that fp; deficit =
	// total - have. Walked in priority order, each Need claims
	// available supply first, then deficit-allocates from Idle/spec.
	//
	// Per-fingerprint state is created lazily on first sight and
	// remembered across Needs sharing the fingerprint, so high-priority
	// Needs claim the supply before low-priority Needs see it — Phase 2
	// preemption then sees the right Unsatisfied set.
	type fpKey struct {
		cluster machine.ClusterID
		fp      string
	}
	type fpState struct {
		supplyRemaining int
	}
	state := make(map[fpKey]*fpState, len(sorted))

	result := Phase1Result{}
	for _, n := range sorted {
		profile := n.Profile
		fp := profile.Fingerprint()
		k := fpKey{n.ClusterID, fp}
		s, ok := state[k]
		if !ok {
			supply := snap.CountByClusterStateMatching(n.ClusterID, machine.StateConfigured, func(m machine.Machine) bool {
				return m.AssignedNeedFingerprint == fp
			})
			s = &fpState{supplyRemaining: supply}
			state[k] = s
		}
		// Claim from existing supply first.
		fromSupply := n.Count
		if fromSupply > s.supplyRemaining {
			fromSupply = s.supplyRemaining
		}
		if fromSupply < 0 {
			fromSupply = 0
		}
		s.supplyRemaining -= fromSupply
		deficit := n.Count - fromSupply
		if deficit <= 0 {
			continue
		}

		// Idle first: cheapest path (one Configure call, no Create).
		idle := alloc.take(machine.StateIdle, profile, deficit)
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
		spec := alloc.take(machine.StateSpeculative, profile, deficit)
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

// profileIsInstanceTypePinOnly reports whether the profile's only
// match obligations are an `instance-type In [...]` pin — i.e. zero
// resources, zero spread constraints, and no requirement other than
// the instance-type pin. When true, MatchProfile is redundant for
// any machine whose instance type is in the pin: the prefilter alone
// is sufficient. M30.2 Phase 3 fast path reads this to skip per-
// machine MatchProfile calls in the M29 burst regime (the load-driver
// emits this exact shape for every CR it creates).
func profileIsInstanceTypePinOnly(p needs.Profile) bool {
	if len(p.ResourcesRO()) > 0 {
		return false
	}
	reqs := p.RequirementsRO()
	if len(reqs) != 1 {
		return false
	}
	r := reqs[0]
	return r.Key == "node.kubernetes.io/instance-type" &&
		r.Operator == needs.OperatorIn &&
		len(r.Values) > 0
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
