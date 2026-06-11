// Package decision implements the shard's worker loop: Phase 1
// (assignment), Phase 2 (priority inversions), Phase 3 (reclaim excess),
// plus the cost / victim-score / drain-grace formulas they share.
//
// The formulas here are the ones the design memory locks in. They are
// not pluggable. If you find yourself wanting to "abstract" them, stop
// and re-read the project memory under YAGNI.
package decision

import (
	"time"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BucketUpperBoundDollars wraps needs.PenaltyBucket.UpperBoundDollars
// as a free function for ergonomic use throughout the decision
// package.
func BucketUpperBoundDollars(b needs.PenaltyBucket) float64 {
	return b.UpperBoundDollars()
}

// VictimWeights tunes the relative importance of the four dimensions in
// the victim score. Defaults are sensible starting points; the shard
// exposes them as configuration so operators can tune for their workload
// mix without code changes.
type VictimWeights struct {
	Priority     float64
	DrainSpeed   float64
	Interruption float64
	Reclamation  float64
}

// DefaultVictimWeights are the starting values for victim scoring.
// Priority dominates under normal conditions; drain speed and the two
// penalties contribute, but at proportional weight. Saturation handling
// (where speed dominates) is the caller's job: the caller scales weights
// before calling Score.
func DefaultVictimWeights() VictimWeights {
	return VictimWeights{
		Priority:     1.0,
		DrainSpeed:   0.1,
		Interruption: 0.1,
		Reclamation:  0.1,
	}
}

// VictimCandidate captures the info Phase 2 needs to score a victim.
// The shard builds these from inventory + the originating CR's penalties.
type VictimCandidate struct {
	Machine                machine.Machine
	AssignedPriority       int32   // priority of the workload currently using the machine
	InterruptionPenalty    float64 // dollars (already converted from bucket)
	ReclamationPenalty     float64
	EstimatedDrainDuration time.Duration
}

// VictimScore computes the score for one candidate against one preemptor.
// Higher = better candidate. The formula is:
//
//	score = priority_gap * w.Priority
//	      + (1 / drain_seconds) * w.DrainSpeed
//	      + (1 / interruption_penalty) * w.Interruption
//	      + (1 / reclamation_penalty) * w.Reclamation
//
// Reciprocals: smaller drain time / smaller penalty = higher contribution.
// To avoid div-by-zero, we floor each denominator at a small positive
// value. The floor for penalties is $0.01 (lower than any practical CR);
// the floor for drain is 1s (any node drains at least that fast).
func VictimScore(preemptorPriority int32, c VictimCandidate, w VictimWeights) float64 {
	gap := float64(preemptorPriority) - float64(c.AssignedPriority)
	if gap < 0 {
		gap = 0
	}
	drain := c.EstimatedDrainDuration.Seconds()
	if drain < 1 {
		drain = 1
	}
	pen := c.InterruptionPenalty
	if pen < 0.01 {
		pen = 0.01
	}
	rec := c.ReclamationPenalty
	if rec < 0.01 {
		rec = 0.01
	}
	return gap*w.Priority +
		(1.0/drain)*w.DrainSpeed +
		(1.0/pen)*w.Interruption +
		(1.0/rec)*w.Reclamation
}

// DrainGrace returns the kubelet graceful-shutdown grace period for a
// preemption with the given priority gap. Locked-in table from the
// BigFleet paper §8:
//
//	gap > 900K  → 10s   (extreme — best-effort yields to production)
//	gap > 500K  → 30s   (large)
//	gap > 100K  → 2m    (moderate)
//	otherwise   → 10m   (small / equal — full graceful)
func DrainGrace(preemptorPriority, victimPriority int32) time.Duration {
	gap := preemptorPriority - victimPriority
	switch {
	case gap > 900_000:
		return 10 * time.Second
	case gap > 500_000:
		return 30 * time.Second
	case gap > 100_000:
		return 2 * time.Minute
	default:
		return 10 * time.Minute
	}
}
