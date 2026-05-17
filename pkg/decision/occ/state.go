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

// claim is the per-machine record retained for precedence comparison
// on conflict (M46.2 displacement uses precedence; M46.1 only stores
// it so the structure is stable across the milestone boundary).
type claim struct {
	need       *needs.Need
	precedence Precedence
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
