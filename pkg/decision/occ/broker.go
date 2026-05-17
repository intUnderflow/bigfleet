package occ

import (
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

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
		metrics.ShardPhase1OCCProposalsTotal.WithLabelValues("conflict").Inc()
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
			metrics.ShardPhase1OCCProposalsTotal.WithLabelValues("conflict").Inc()
			return Result{Status: StatusConflict, NewSeq: currentSeq}
		}
	case ModeIncremental:
		if len(newClaim)+len(displace) == 0 {
			metrics.ShardPhase1OCCProposalsTotal.WithLabelValues("conflict").Inc()
			return Result{Status: StatusConflict, NewSeq: currentSeq}
		}
	}

	// 4. Commit atomically.
	//
	//    Release displaced incumbents first (their machines
	//    become un-claimed and drop out of claimedByNeed); then
	//    claim every machine the proposal can take into both
	//    claimedBy and claimedByNeed. Build the Displaced list
	//    so the proposer's worker can re-queue them. Multiple
	//    machines displaced from the same incumbent collapse to
	//    one Displaced entry (the worker dedupes anyway, but
	//    deduping here saves the queue an entry).
	displacedByNeed := make(map[*needs.Need]int)
	for _, d := range displace {
		delete(b.state.claimedBy, d.mid)
		if set, ok := b.state.claimedByNeed[d.old.need]; ok {
			delete(set, d.mid)
			if len(set) == 0 {
				delete(b.state.claimedByNeed, d.old.need)
			}
		}
		newRetries := d.old.retriesLeft - 1
		if cur, ok := displacedByNeed[d.old.need]; !ok || newRetries < cur {
			displacedByNeed[d.old.need] = newRetries
		}
	}
	displaced := make([]QueuedNeed, 0, len(displacedByNeed))
	for n, retries := range displacedByNeed {
		displaced = append(displaced, QueuedNeed{Need: n, RetriesLeft: retries})
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
	if b.state.claimedByNeed[p.Need] == nil {
		b.state.claimedByNeed[p.Need] = make(map[machine.ID]struct{}, len(committed))
	}
	reverse := b.state.claimedByNeed[p.Need]
	for _, mid := range committed {
		b.state.claimedBy[mid] = newRecord
		reverse[mid] = struct{}{}
	}
	b.state.bucketSeq[p.Bucket] = currentSeq + 1

	metrics.ShardPhase1OCCProposalsTotal.WithLabelValues("committed").Inc()
	if n := len(displaced); n > 0 {
		metrics.ShardPhase1OCCDisplacementsTotal.Add(float64(n))
	}

	return Result{
		Status:     StatusCommitted,
		Committed:  committed,
		Conflicted: conflicted,
		Displaced:  displaced,
		NewSeq:     currentSeq + 1,
	}
}
