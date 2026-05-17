package occ

import "github.com/intUnderflow/bigfleet/pkg/machine"

// Broker is the single synchronization point for commits. Workers
// submit Proposals; the broker validates the bucket's seqno, plans
// the commit against the claimed-set, and either applies it
// atomically or returns a Conflict for the worker to retry.
//
// One Broker is created per cycle, wrapping the cycle's SharedState.
// All methods are safe for concurrent use.
//
// M46.1 ships a minimal broker: seqno CAS, plain commit / abort on
// claim collisions, and the dual-mode (Incremental vs AllOrNothing)
// gate. Priority-on-conflict displacement of incumbent claims —
// where a higher-precedence proposal evicts a lower-precedence
// incumbent — lands in M46.2 with property tests for the ADR-0027
// stage 5.1 attribution invariant. Until then, every claimed
// machine is treated as conflicted regardless of precedence.
type Broker struct {
	state *SharedState
}

// NewBroker wraps state in a Broker. The state must be fresh — a
// Broker is single-cycle and assumes no foreign mutations to state.
func NewBroker(state *SharedState) *Broker {
	return &Broker{state: state}
}

// Propose validates p against the current cycle state and either
// commits it or returns a Conflict for retry. The broker is the
// only legitimate mutator of state's claimed-set and bucket
// seqnos.
//
// The control flow follows the plan-then-commit discipline that
// will carry through M46.2: classify every machine against current
// state without mutating, decide commit-or-abort based on Mode,
// then apply atomically. M46.1's classification is binary
// (unclaimed vs claimed); M46.2 adds the displaceable third bucket.
func (b *Broker) Propose(p Proposal) Result {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()

	// 1. Seqno CAS. Any change to this bucket since the worker
	//    read it invalidates the proposal.
	currentSeq := b.state.bucketSeq[p.Bucket]
	if currentSeq != p.ObservedSeq {
		return Result{Status: StatusConflict, NewSeq: currentSeq}
	}

	// 2. Plan: classify each machine without mutating state.
	var canClaim []machine.ID
	var conflicted []machine.ID
	for _, mid := range p.Machines {
		if _, claimed := b.state.claimedBy[mid]; claimed {
			conflicted = append(conflicted, mid)
		} else {
			canClaim = append(canClaim, mid)
		}
	}

	// 3. Mode-specific commit decision (still no mutations).
	//
	//    ModeAllOrNothing aborts on any conflict; ModeIncremental
	//    aborts only when nothing is claimable (otherwise it
	//    commits the unclaimed subset).
	switch p.Mode {
	case ModeAllOrNothing:
		if len(conflicted) > 0 {
			return Result{Status: StatusConflict, NewSeq: currentSeq}
		}
	case ModeIncremental:
		if len(canClaim) == 0 {
			return Result{Status: StatusConflict, NewSeq: currentSeq}
		}
	}

	// 4. Commit atomically. Claim each unclaimed machine and bump
	//    the bucket seqno so any racing worker observes the change.
	c := claim{need: p.Need, precedence: p.Precedence}
	for _, mid := range canClaim {
		b.state.claimedBy[mid] = c
	}
	b.state.bucketSeq[p.Bucket] = currentSeq + 1

	return Result{
		Status:     StatusCommitted,
		Committed:  canClaim,
		Conflicted: conflicted,
		NewSeq:     currentSeq + 1,
	}
}
