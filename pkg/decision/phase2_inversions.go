package decision

import (
	"container/heap"
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

	out := Phase2Result{}
	for _, u := range resolved {
		preemptorPriority := u.Need.Profile.Priority()

		// Candidate slice: when the need pins to a single instance
		// type, reuse the snapshot's pre-built bucket directly (no
		// copy). Multi-type concatenates per-type buckets. Unpinned
		// falls back to the all-state Configured slice (lazily
		// materialised — see getConfigured).
		var candidatePool []machine.Machine
		switch types := pinnedInstanceTypes(u.Need.Profile); {
		case len(types) == 1:
			candidatePool = snap.ListByStateInstanceType(machine.StateConfigured, types[0])
		case len(types) > 1:
			for _, t := range types {
				candidatePool = append(candidatePool, snap.ListByStateInstanceType(machine.StateConfigured, t)...)
			}
		default:
			candidatePool = getConfigured()
		}

		// Min-heap of size Deficit holding the best candidates so far.
		// Replaces the per-need full sort: we only need the top-K, and
		// K (Deficit) is typically tiny relative to the candidate pool.
		topK := &victimMinHeap{}

		for _, m := range candidatePool {
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
			score := VictimScore(preemptorPriority, cand, opts.Weights)
			if topK.Len() < u.Deficit {
				heap.Push(topK, scoredVictim{m: m, score: score})
				continue
			}
			// Heap is full: replace the lowest score iff this candidate
			// is better.
			if (*topK)[0].score < score ||
				((*topK)[0].score == score && m.ID < (*topK)[0].m.ID) {
				(*topK)[0] = scoredVictim{m: m, score: score}
				heap.Fix(topK, 0)
			}
		}

		// Drain the heap into highest-score-first order for stable
		// emit ordering.
		picks := make([]scoredVictim, topK.Len())
		for i := len(picks) - 1; i >= 0; i-- {
			picks[i] = heap.Pop(topK).(scoredVictim)
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

// scoredVictim pairs a candidate machine with its VictimScore. Used
// as the heap element type during Phase 2 top-K selection.
type scoredVictim struct {
	m     machine.Machine
	score float64
}

// victimMinHeap orders by ascending score so the worst candidate is at
// index 0 — when the heap is full and a better candidate arrives, we
// replace [0] and heap.Fix. ID tiebreak keeps the heap deterministic
// when scores tie.
type victimMinHeap []scoredVictim

func (h victimMinHeap) Len() int { return len(h) }
func (h victimMinHeap) Less(i, j int) bool {
	if h[i].score != h[j].score {
		return h[i].score < h[j].score
	}
	return h[i].m.ID > h[j].m.ID
}
func (h victimMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *victimMinHeap) Push(x any)   { *h = append(*h, x.(scoredVictim)) }
func (h *victimMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
