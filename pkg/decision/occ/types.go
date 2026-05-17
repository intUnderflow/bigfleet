// Package occ implements Phase 1 as an Omega-style optimistic-
// concurrency-control scheduler. See ADR-0029.
//
// Workers race over a single shared queue of Needs, each computing a
// Proposal against the per-cycle shared state and submitting it to the
// commit broker. The broker is the synchronization point: a single
// mutex serialises commits, per-bucket sequence numbers detect stale
// reads, and a precedence comparison decides conflict winners.
//
// This file holds the public types only — the runtime lives in
// broker.go (commit logic), state.go (shared state), and worker.go
// (pool + dispatch). M46.1 ships the types, state plumbing, and a
// minimal seqno-CAS broker; plan-then-commit, dual modes, and
// priority-on-conflict displacement land in M46.2.
package occ

import (
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// BucketKey identifies a co-location bucket for fine-grained conflict
// detection. The grain mirrors phase1Allocator's coLocatedBucket
// identity (ADR-0024 / ADR-0019): one bucket per
// (state, profile fingerprint, Same key, Same value). Needs that don't
// carry a Same requirement share a single per-(state, profile) bucket
// with an empty SameKey/SameValue.
type BucketKey struct {
	State     machine.State
	ProfileFP string
	SameKey   string
	SameValue string
}

// ProposalMode selects the broker's transaction semantics for one
// Proposal (ADR-0029, Omega [1, §3.4]).
type ProposalMode int

const (
	// ModeIncremental commits the unclaimed subset of the proposal and
	// returns the conflicted machines so the worker can record the
	// residual deficit as a shortfall. Default for Needs whose partial
	// fill is semantically fine.
	ModeIncremental ProposalMode = iota

	// ModeAllOrNothing aborts the entire proposal if any one machine
	// is conflicted. Reserved for gang-scheduled Needs (MinUnit spans
	// multiple machines AND a Same requirement is set); partial fill
	// would be semantically wrong, not just suboptimal.
	ModeAllOrNothing
)

// Precedence is the ordering used to break commit-time conflicts.
// Higher precedence wins. Comparison is lexicographic on
// (Priority, InterruptionPenalty bucket, ReclamationPenalty bucket).
//
// The bucket fields hold the raw PenaltyBucket value, not the dollar
// upper bound — bucket ordinal is monotone in dollars and avoids the
// float conversion on every conflict check.
type Precedence struct {
	Priority            int32
	InterruptionPenalty needs.PenaltyBucket
	ReclamationPenalty  needs.PenaltyBucket
}

// Less reports whether p sorts strictly before other — i.e. other
// outranks p and may displace p at the broker.
func (p Precedence) Less(other Precedence) bool {
	if p.Priority != other.Priority {
		return p.Priority < other.Priority
	}
	if p.InterruptionPenalty != other.InterruptionPenalty {
		return p.InterruptionPenalty < other.InterruptionPenalty
	}
	return p.ReclamationPenalty < other.ReclamationPenalty
}

// PrecedenceFromProfile derives the precedence triple from a Profile.
// Pure projection — no shared state, safe to call from any worker.
func PrecedenceFromProfile(p needs.Profile) Precedence {
	return Precedence{
		Priority:            p.Priority(),
		InterruptionPenalty: p.InterruptionPenaltyBucket(),
		ReclamationPenalty:  p.ReclamationPenaltyBucket(),
	}
}

// Proposal is a worker's bid to claim a set of machines for one Need.
// All fields are populated at construction; the broker treats Proposal
// as immutable.
type Proposal struct {
	Need        *needs.Need
	Bucket      BucketKey
	Machines    []machine.ID
	ObservedSeq uint64
	Precedence  Precedence
	Mode        ProposalMode

	// RetriesLeft is the proposer's remaining retry budget when this
	// proposal is submitted. The broker stores it in the claim
	// record if the proposal commits, so a later displacement can
	// emit a Displaced QueuedNeed with the right (decremented)
	// budget. RetriesLeft ≤ 0 means "don't requeue on displacement —
	// emit shortfall instead" (encoded by the caller's read of
	// Result.Displaced).
	RetriesLeft int
}

// QueuedNeed wraps a Need with its cycle-local retry budget. The
// worker pool pulls QueuedNeed values off the queue, runs the
// proposer's retry loop, and on displacement re-queues the
// QueuedNeeds the broker returns via Result.Displaced.
type QueuedNeed struct {
	Need        *needs.Need
	RetriesLeft int
}

// ResultStatus is the broker's verdict on a Proposal.
type ResultStatus int

const (
	// StatusCommitted means the proposal mutated state: at least one
	// machine was claimed (for ModeIncremental, possibly fewer than
	// requested; for ModeAllOrNothing, exactly all of them).
	StatusCommitted ResultStatus = iota

	// StatusConflict means the proposal failed without mutating state.
	// The worker should re-read the bucket and retry. The caller is
	// responsible for tracking retry budget exhaustion → shortfall.
	StatusConflict
)

// NeedResult is the per-Need outcome of one cycle. The OCC worker
// populates one of these per Need it processes; decision.Phase1
// converts them into Action + UnsatisfiedNeed slices for emission.
//
// Configured/Configuring machines credited via the existing-supply
// pre-pass don't appear here — they're seeded directly on
// SharedState before workers start and never produce an Action.
//
// BootstrapMachines and ProvisionMachines correspond to Idle and
// Speculative commits respectively (mirroring the legacy
// ActionKindBootstrap / ActionKindProvision distinction). They are
// emitted as the worker successfully commits; on conflict + retry,
// only committed machines accumulate here.
//
// Unsatisfied=true plus Deficit captures the residual that the
// worker couldn't cover before exhausting its retry budget or its
// candidate pool. The shortfall buffer (`pkg/shortfall`) consumes
// these.
type NeedResult struct {
	Need              *needs.Need
	BootstrapMachines []machine.ID
	ProvisionMachines []machine.ID
	Unsatisfied       bool
	Deficit           []needs.ResourceQty
}

// Result is what Propose returns.
//
//   - Committed: machines now owned by the proposing Need (subset of
//     Proposal.Machines for ModeIncremental; exactly Proposal.Machines
//     for ModeAllOrNothing successes).
//
//   - Conflicted: machines whose incumbent claim is at least as high
//     precedence as the proposer — immovable. Empty for ModeAllOrNothing
//     successes; possibly non-empty for ModeIncremental.
//
//   - Displaced: incumbent Needs whose claims this commit evicted.
//     The broker has already released the underlying machine claims;
//     the proposer's worker is responsible for re-queueing each
//     QueuedNeed (or emitting it as shortfall if RetriesLeft ≤ 0).
//     Always empty on StatusConflict.
//
//   - NewSeq: the bucket's seqno after the commit (or the current
//     seqno on conflict). The worker uses it as the ObservedSeq for
//     any retry.
type Result struct {
	Status     ResultStatus
	Committed  []machine.ID
	Conflicted []machine.ID
	Displaced  []QueuedNeed
	NewSeq     uint64
}
