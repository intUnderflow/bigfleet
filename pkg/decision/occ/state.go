package occ

import (
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// SharedState is the per-cycle state shared across all workers. It
// holds the inventory snapshot (read-only for the cycle), the
// claimed-set (mutated only via the broker), and the per-bucket
// sequence numbers used for fine-grained conflict detection
// (ADR-0029 [1, §5.2]).
//
// Readers of the snapshot need no synchronisation — the snapshot is
// captured immutable at cycle start. The claimed-set and bucketSeq
// maps are guarded by mu; the broker holds mu for the duration of
// every Propose call.
type SharedState struct {
	snap *inventory.Snapshot

	mu        sync.Mutex
	claimedBy map[machine.ID]claim
	bucketSeq map[BucketKey]uint64
}

// claim is the per-machine record the broker stores when it
// commits a proposal. The fields support displacement: precedence
// determines whether a later, higher-precedence proposal may evict
// this claim, and retriesLeft is forwarded to the Displaced
// QueuedNeed (decremented by one) so the evicted incumbent's
// worker can re-process it with a smaller budget.
type claim struct {
	need        *needs.Need
	precedence  Precedence
	retriesLeft int
}

// NewSharedState builds a fresh per-cycle SharedState wrapping snap.
// The caller retains the snapshot and is responsible for its
// lifetime; SharedState only reads from it.
func NewSharedState(snap *inventory.Snapshot) *SharedState {
	return &SharedState{
		snap:      snap,
		claimedBy: make(map[machine.ID]claim),
		bucketSeq: make(map[BucketKey]uint64),
	}
}

// SeedClaim records an initial claim outside the broker's plan-then-
// commit path. Used by the pre-pass that credits existing supply
// (matching Configured/Configuring machines for each Need's cluster)
// before the OCC worker pool starts. Not safe for concurrent use:
// the pre-pass is single-threaded by construction.
//
// The seeded claim participates in normal displacement logic — a
// later higher-precedence proposal will evict it just like any
// other claim. retriesLeft is the budget the displaced incumbent
// will inherit if that happens.
func (s *SharedState) SeedClaim(mid machine.ID, n *needs.Need, prec Precedence, retriesLeft int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimedBy[mid] = claim{need: n, precedence: prec, retriesLeft: retriesLeft}
}

// Snapshot returns the cycle's immutable inventory snapshot. Callers
// read it without taking the state lock.
func (s *SharedState) Snapshot() *inventory.Snapshot {
	return s.snap
}

// BucketSeq returns the current sequence number for bucket. Workers
// capture this at proposal-construction time and submit it as
// ObservedSeq; the broker rejects any proposal whose ObservedSeq
// doesn't match the bucket's current seqno.
//
// Returns 0 for buckets that have never been touched (the implicit
// initial seqno for every bucket).
func (s *SharedState) BucketSeq(b BucketKey) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bucketSeq[b]
}

// IsClaimed reports whether mid has been claimed in this cycle.
//
// This is the cheap snapshot for worker-side filtering — the broker
// performs the authoritative check inside Propose under mu, so a
// stale read here only costs an extra round-trip to the broker.
func (s *SharedState) IsClaimed(mid machine.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.claimedBy[mid]
	return ok
}

// PrecedenceAt reports whether mid is claimed by an incumbent with
// precedence ≥ prec — i.e. the incumbent is immovable from a
// prec-priority proposer. Returns false if mid is unclaimed.
//
// Workers use this to short-circuit retries: a machine held by an
// incumbent that the worker cannot displace is a permanent local
// loss (until the cycle ends), so further retries on that machine
// would only burn budget.
func (s *SharedState) PrecedenceAt(mid machine.ID, prec Precedence) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.claimedBy[mid]
	if !ok {
		return false
	}
	return !inc.precedence.Less(prec)
}

// OwnersForTest returns a snapshot of the claimed-set as a
// machine→Precedence map. Test-only inspection helper; not on any
// hot path.
func (s *SharedState) OwnersForTest() map[machine.ID]Precedence {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[machine.ID]Precedence, len(s.claimedBy))
	for k, v := range s.claimedBy {
		out[k] = v.precedence
	}
	return out
}
