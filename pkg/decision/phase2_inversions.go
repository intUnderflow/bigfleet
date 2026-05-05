package decision

import (
	"sort"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Phase2Result is the output of a Phase 2 pass: preempt actions plus
// the still-unsatisfied needs that even preemption could not fix
// (those become shortfalls reported to the coordinator).
type Phase2Result struct {
	Actions    []Action
	Unresolved []UnsatisfiedNeed
}

// Phase2Options tunes the victim-scoring computation. The defaults match
// the locked-in formulas; operators may scale weights based on saturation
// state (under heavy pressure, drain speed should dominate; under normal
// conditions, priority dominates).
type Phase2Options struct {
	Weights                VictimWeights
	EstimatedDrainDuration time.Duration // applied to all victims; refined later when we have per-workload data
}

// DefaultPhase2Options returns the safe defaults the shard uses out of the box.
func DefaultPhase2Options() Phase2Options {
	return Phase2Options{
		Weights:                DefaultVictimWeights(),
		EstimatedDrainDuration: 30 * time.Second,
	}
}

// Phase2 attempts to satisfy the unresolved needs from Phase 1 by
// preempting lower-priority configured machines. It does not actually
// move the machine into the target cluster — that is the responsibility
// of the next cycle's Phase 1 once the victims become Idle. Phase 2
// only emits the Preempt actions; the resulting Idle machines feed the
// next cycle.
//
// Performance (M27): when multiple unresolved Needs share a (Profile
// fingerprint, preemptor priority) key, the per-key MatchProfile +
// VictimScore + sort work is paid once and the cached sorted slice is
// drained across all sharing Needs. At the M13.gate scale shape (500K
// configured / 100 unresolved / 5 instance types, with all preemptors
// at the same priority) this collapses 100 × 100K = 10M MatchProfile
// calls to 5 × 100K = 500K, and the per-Need work becomes a head-walk
// of an already-sorted slice with claim-skipping rather than a full
// 100K-candidate scan + heap top-K. ~5× cycle-time reduction at this
// shape; the gap closes for workloads where fingerprint × priority
// diversity is high (each Need its own key → no cache reuse).
func Phase2(snap *inventory.Snapshot, unresolved []UnsatisfiedNeed, opts Phase2Options) Phase2Result {
	if len(unresolved) == 0 {
		return Phase2Result{}
	}
	// Track victims claimed in this Phase 2 pass so two competing
	// preemptors don't double-up on the same machine.
	claimed := make(map[machine.ID]struct{})

	// Lazy: only materialise the all-state Configured slice if a need
	// reaches the unpinned fallback. With every-need-pinned (the common
	// shape) we skip a 500K alloc.
	var configured []machine.Machine
	configuredOnce := false
	getConfigured := func() []machine.Machine {
		if !configuredOnce {
			configured = snap.ListByState(machine.StateConfigured)
			configuredOnce = true
		}
		return configured
	}

	// Sort unresolved by priority desc so the highest-priority preemptor
	// gets first refusal of the cheapest victims.
	resolved := make([]UnsatisfiedNeed, len(unresolved))
	copy(resolved, unresolved)
	sort.SliceStable(resolved, func(i, j int) bool {
		return resolved[i].Need.Profile.Priority() > resolved[j].Need.Profile.Priority()
	})

	// Per-(fingerprint, preemptor-priority) cache of pre-scored,
	// pre-sorted candidate slices. Key encodes both because VictimScore
	// depends on the preemptor's priority via the priority-gap term —
	// two preemptors with the same Profile but different priorities
	// produce different sort orders.
	type cacheKey struct {
		fingerprint string
		priority    int32
	}
	scoredCache := make(map[cacheKey][]scoredVictim)

	out := Phase2Result{}
	for _, u := range resolved {
		preemptorPriority := u.Need.Profile.Priority()
		key := cacheKey{fingerprint: u.Need.Profile.Fingerprint(), priority: preemptorPriority}

		sorted, hit := scoredCache[key]
		if !hit {
			// M30.1 short-circuit: if the candidate pool's minimum
			// AssignedPriority ≥ the preemptor's priority, no machine
			// in the pool can be a victim (Phase 2 only preempts
			// strictly lower priority). Skip the candidate-pool walk
			// entirely and cache an empty result. At the M29 burst
			// shape (450K Configured at 1000000, demand at 1000) this
			// collapses Phase 2 from O(450K) per Need to O(1).
			types := pinnedInstanceTypes(u.Need.Profile)
			var candidatePool []machine.Machine
			if !phase2BucketHasNoVictim(snap, types, preemptorPriority) {
				// Build the candidate pool. When the need pins to one
				// or more instance types, the snapshot's pre-built
				// bucket gives us a much smaller starting set than
				// the full Configured slice. Multi-type concatenates
				// per-type buckets (no MatchProfile yet — that
				// filter is below).
				switch {
				case len(types) == 1:
					candidatePool = snap.ListByStateInstanceType(machine.StateConfigured, types[0])
				case len(types) > 1:
					for _, t := range types {
						candidatePool = append(candidatePool, snap.ListByStateInstanceType(machine.StateConfigured, t)...)
					}
				default:
					candidatePool = getConfigured()
				}
			}

			// Score every candidate that passes the priority +
			// MatchProfile filter, then sort by score desc with ID asc
			// as the deterministic tiebreak. Sharing-key Needs walk
			// this from the top, skipping claimed entries — the
			// MatchProfile and score work is paid once per key.
			sorted = make([]scoredVictim, 0, len(candidatePool))
			for _, m := range candidatePool {
				if m.AssignedPriority >= preemptorPriority {
					continue
				}
				if !MatchProfile(u.Need.Profile, m) {
					continue
				}
				cand := VictimCandidate{
					Machine:                m,
					AssignedPriority:       m.AssignedPriority,
					InterruptionPenalty:    m.AssignedInterruptionPenaltyDollars,
					ReclamationPenalty:     m.AssignedReclamationPenaltyDollars,
					EstimatedDrainDuration: opts.EstimatedDrainDuration,
				}
				score := VictimScore(preemptorPriority, cand, opts.Weights)
				sorted = append(sorted, scoredVictim{m: m, score: score})
			}
			sort.Slice(sorted, func(i, j int) bool {
				if sorted[i].score != sorted[j].score {
					return sorted[i].score > sorted[j].score
				}
				return sorted[i].m.ID < sorted[j].m.ID
			})
			scoredCache[key] = sorted
		}

		// Drain Deficit victims from the cached head, skipping any
		// already claimed in this Phase 2 pass.
		picks := make([]scoredVictim, 0, u.Deficit)
		for _, sv := range sorted {
			if len(picks) >= u.Deficit {
				break
			}
			if _, taken := claimed[sv.m.ID]; taken {
				continue
			}
			picks = append(picks, sv)
		}

		for _, c := range picks {
			claimed[c.m.ID] = struct{}{}
			out.Actions = append(out.Actions, Action{
				Kind:              ActionKindPreempt,
				MachineID:         c.m.ID,
				Cluster:           c.m.Cluster,
				GracePeriod:       DrainGrace(preemptorPriority, c.m.AssignedPriority),
				PreemptorPriority: preemptorPriority,
				Reason:            "phase2.inversion",
			})
		}
		if len(picks) < u.Deficit {
			out.Unresolved = append(out.Unresolved, UnsatisfiedNeed{
				Need:    u.Need,
				Deficit: u.Deficit - len(picks),
			})
		}
	}
	return out
}

// phase2BucketHasNoVictim reports whether the candidate pool for a
// preemptor at preemptorPriority is provably empty given the snapshot's
// pre-computed minimum AssignedPriority indexes. When the pool's min
// priority ≥ preemptorPriority, no machine in the pool could be a
// victim (Phase 2 only preempts strictly lower priorities), so the
// per-Need MatchProfile walk can be skipped entirely.
//
// Returns true iff the pool is provably empty. A return of false
// means "walk the pool to find out" — there might still be no
// victims after MatchProfile filtering.
func phase2BucketHasNoVictim(snap *inventory.Snapshot, pinnedTypes []string, preemptorPriority int32) bool {
	switch len(pinnedTypes) {
	case 0:
		return snap.MinAssignedPriority(machine.StateConfigured) >= preemptorPriority
	case 1:
		return snap.MinAssignedPriorityByInstanceType(machine.StateConfigured, pinnedTypes[0]) >= preemptorPriority
	default:
		// Multi-type: pool is empty only if EVERY pinned type's bucket
		// has no preemptable victim.
		for _, t := range pinnedTypes {
			if snap.MinAssignedPriorityByInstanceType(machine.StateConfigured, t) < preemptorPriority {
				return false
			}
		}
		return true
	}
}

// scoredVictim pairs a candidate machine with its VictimScore. M27
// drops the heap-based top-K (replaced by per-(fingerprint, priority)
// pre-sorted slices that Needs walk from the head) — the type is kept
// because the cache stores []scoredVictim directly.
type scoredVictim struct {
	m     machine.Machine
	score float64
}
