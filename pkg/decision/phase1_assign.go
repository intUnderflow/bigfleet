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
		profResources := profileResourcesToMap(profile.ResourcesRO())
		k := fpKey{n.ClusterID, fp}
		s, ok := state[k]
		if !ok {
			// M44.4 Drop H: count Configured AND Configuring with the
			// matching fingerprint as supply. Configuring machines are
			// committed to satisfy their Need (the Bootstrap action is
			// in flight); pre-Drop-H, Phase 1 saw them as "not yet
			// supply" and emitted another Bootstrap on a fresh Idle
			// machine for the same Need on the next cycle. Combined
			// with the pendingActions dedup, this caused 61 % of
			// Bootstrap emits to be deduped silently — the cycle's
			// emit cap (maxActionsPerCycle) was burned on duplicates
			// instead of new demand.
			//
			// ADR-0022 / M45.1: supply is now in Pod-units (sum of each
			// matching machine's density), not machine count. For
			// pre-ADR-0022 inventory where machine.EffectiveAllocatable()
			// equals profile.Resources, density = 1 per machine and
			// supply equals the old machine-count value — behaviour
			// preserved. M45.4 introduces seeded inventory with density
			// > 1 and the same code path keeps working without change.
			supply := 0
			matchPodSupply := func(state machine.State) {
				for _, m := range snap.ListByClusterState(n.ClusterID, state) {
					if m.AssignedNeedFingerprint != fp {
						continue
					}
					d := PodsPerMachine(profResources, m.EffectiveAllocatable())
					if d <= 0 {
						d = 1
					}
					supply += d
				}
			}
			matchPodSupply(machine.StateConfigured)
			matchPodSupply(machine.StateConfiguring)
			s = &fpState{supplyRemaining: supply}
			state[k] = s
		}
		// Claim from existing supply (Pod-units) first.
		fromSupply := n.Count
		if fromSupply > s.supplyRemaining {
			fromSupply = s.supplyRemaining
		}
		if fromSupply < 0 {
			fromSupply = 0
		}
		s.supplyRemaining -= fromSupply
		deficitPods := n.Count - fromSupply
		if deficitPods <= 0 {
			continue
		}

		// ADR-0022 / M45.1: translate Pod deficit to machine count using
		// MachinesForAggregate. For pre-M45 inventory where matching
		// machines have Allocatable == profile.Resources, density = 1
		// and machinesNeeded == deficitPods (preserves the historical
		// 1 Pod = 1 machine math). M45.4 will introduce seeded inventory
		// with density > 1; this same call returns a smaller machine
		// count there, and the take loop below tracks actual absorption
		// per machine.
		machinesNeeded := MachinesForAggregate(profResources, profResources, deficitPods)

		// Idle first: cheapest path (one Configure call, no Create).
		idle := alloc.take(machine.StateIdle, profile, machinesNeeded)
		for _, m := range idle {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindBootstrap,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.idle",
			})
			d := PodsPerMachine(profResources, m.EffectiveAllocatable())
			if d <= 0 {
				d = 1
			}
			deficitPods -= d
		}
		if deficitPods <= 0 {
			// ADR-0022 / M45.4: the machine(s) we just took absorbed
			// more Pods than this single Need wanted (density > 1 +
			// Count=1 Needs from per-Pod CR fan-out). Credit the
			// surplus back to the per-fp supply pool so peer Needs of
			// the same (cluster, fp) consume it before allocating
			// fresh machines. Without this, dev-5k at density=10 with
			// 5000 single-Pod Needs emits ~5000 Bootstraps instead of
			// the ~520 the math wants — Phase 1 stalls because Idle
			// inventory runs out and 4400+ Needs go Unsatisfied each
			// cycle.
			s.supplyRemaining += -deficitPods
			continue
		}

		// Fall back to speculative: pick by lowest effective_cost.
		machinesNeeded = MachinesForAggregate(profResources, profResources, deficitPods)
		spec := alloc.take(machine.StateSpeculative, profile, machinesNeeded)
		for _, m := range spec {
			result.Actions = append(result.Actions, Action{
				Kind:          ActionKindProvision,
				MachineID:     m.ID,
				Cluster:       n.ClusterID,
				SourceProfile: &profile,
				Reason:        "phase1.speculative",
			})
			d := PodsPerMachine(profResources, m.EffectiveAllocatable())
			if d <= 0 {
				d = 1
			}
			deficitPods -= d
		}

		if deficitPods <= 0 {
			// Same surplus-credit story as the Idle branch above.
			s.supplyRemaining += -deficitPods
			continue
		}

		result.Unsatisfied = append(result.Unsatisfied, UnsatisfiedNeed{
			Need: n,
			// Phase 2 / shortfall protocol still operates in Pod
			// units here. Translating to machine units for the
			// downstream coordinator interaction is M45.2+ work.
			Deficit: deficitPods,
		})
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
