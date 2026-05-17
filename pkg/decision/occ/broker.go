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
// Proposals carry a Mode that selects the broker's transaction
// semantics (ADR-0029, Omega [1, §3.4]):
//
//   - ModeIncremental commits the displaceable + unclaimed subset
//     and reports the immovable conflicts back.
//
//   - ModeAllOrNothing aborts if any one machine is held by an
//     incumbent the proposal cannot displace.
//
// Priority-on-conflict displacement (`bigfleet.md` §16) is enforced
// at commit time, not by sorting Needs into priority order
// up-front: when a higher-precedence proposal proposes a machine
// already claimed by a lower-precedence incumbent, the incumbent's
// claim is released and the displaced Need is returned in
// Result.Displaced for the proposer's worker to re-queue.
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
// The control flow is plan-then-commit (ADR-0029): every machine
// is classified against current state without mutating, the Mode
// decides commit-or-abort, then the mutation is applied
// atomically. No rollback path is needed because no state changes
// happen during classification.
func (b *Broker) Propose(p Proposal) Result {
	b.state.mu.Lock()
	defer b.state.mu.Unlock()

	// 1. Seqno CAS. Any change to this bucket since the worker
	//    read it invalidates the proposal.
	currentSeq := b.state.bucketSeq[p.Bucket]
	if currentSeq != p.ObservedSeq {
		return Result{Status: StatusConflict, NewSeq: currentSeq}
	}

	// 2. Plan: classify each machine against current state
	//    without mutating. Three buckets:
	//
	//      - newClaim:   currently unclaimed
	//      - displace:   claimed by a strictly-lower-precedence
	//                    incumbent we can evict
	//      - conflicted: claimed by an incumbent we cannot
	//                    displace (equal or higher precedence)
	type displacement struct {
		mid machine.ID
		old claim
	}
	var newClaim []machine.ID
	var displace []displacement
	var conflicted []machine.ID
	for _, mid := range p.Machines {
		inc, claimed := b.state.claimedBy[mid]
		if !claimed {
			newClaim = append(newClaim, mid)
			continue
		}
		// Equal precedence does NOT displace — first-mover wins
		// at equal precedence. Only strictly-lower-precedence
		// incumbents are evictable.
		if inc.precedence.Less(p.Precedence) {
			displace = append(displace, displacement{mid: mid, old: inc})
		} else {
			conflicted = append(conflicted, mid)
		}
	}

	// 3. Mode-specific commit decision (still no mutations).
	switch p.Mode {
	case ModeAllOrNothing:
		if len(conflicted) > 0 {
			return Result{Status: StatusConflict, NewSeq: currentSeq}
		}
	case ModeIncremental:
		if len(newClaim)+len(displace) == 0 {
			return Result{Status: StatusConflict, NewSeq: currentSeq}
		}
	}

	// 4. Commit atomically.
	//
	//    Release displaced incumbents first (their machines
	//    become un-claimed); then claim every machine the
	//    proposal can take. Build the Displaced list so the
	//    proposer's worker can re-queue them.
	displaced := make([]QueuedNeed, 0, len(displace))
	for _, d := range displace {
		delete(b.state.claimedBy, d.mid)
		displaced = append(displaced, QueuedNeed{
			Need:        d.old.need,
			RetriesLeft: d.old.retriesLeft - 1,
		})
	}
	newRecord := claim{
		need:        p.Need,
		precedence:  p.Precedence,
		retriesLeft: p.RetriesLeft,
	}
	committed := make([]machine.ID, 0, len(newClaim)+len(displace))
	committed = append(committed, newClaim...)
	for _, d := range displace {
		committed = append(committed, d.mid)
	}
	for _, mid := range committed {
		b.state.claimedBy[mid] = newRecord
	}
	b.state.bucketSeq[p.Bucket] = currentSeq + 1

	return Result{
		Status:     StatusCommitted,
		Committed:  committed,
		Conflicted: conflicted,
		Displaced:  displaced,
		NewSeq:     currentSeq + 1,
	}
}
