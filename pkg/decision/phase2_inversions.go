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
func Phase2(snap *inventory.Snapshot, unresolved []UnsatisfiedNeed, opts Phase2Options) Phase2Result {
	if len(unresolved) == 0 {
		return Phase2Result{}
	}
	// Track victims claimed in this Phase 2 pass so two competing
	// preemptors don't double-up on the same machine.
	claimed := make(map[machine.ID]struct{})

	configured := snap.ListByState(machine.StateConfigured)

	// Sort unresolved by priority desc so the highest-priority preemptor
	// gets first refusal of the cheapest victims.
	resolved := make([]UnsatisfiedNeed, len(unresolved))
	copy(resolved, unresolved)
	sort.SliceStable(resolved, func(i, j int) bool {
		return resolved[i].Need.Profile.Priority() > resolved[j].Need.Profile.Priority()
	})

	out := Phase2Result{}
	for _, u := range resolved {
		preemptorPriority := u.Need.Profile.Priority()

		type scored struct {
			m     machine.Machine
			score float64
		}
		var candidates []scored
		for _, m := range configured {
			if _, taken := claimed[m.ID]; taken {
				continue
			}
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
			candidates = append(candidates, scored{
				m:     m,
				score: VictimScore(preemptorPriority, cand, opts.Weights),
			})
		}
		// Highest score = best victim.
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].score != candidates[j].score {
				return candidates[i].score > candidates[j].score
			}
			return candidates[i].m.ID < candidates[j].m.ID
		})

		take := minInt(len(candidates), u.Deficit)
		for _, c := range candidates[:take] {
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
		if take < u.Deficit {
			out.Unresolved = append(out.Unresolved, UnsatisfiedNeed{
				Need:    u.Need,
				Deficit: u.Deficit - take,
			})
		}
	}
	return out
}
