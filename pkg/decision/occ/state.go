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
// captured immutable at cycle start. The claimed-set, bucketSeq, and
// reverse-index maps are guarded by mu; the broker holds mu for the
// duration of every Propose call.
//
// The reverse index (claimedByNeed) lets RunCycle compute each
// Need's residual deficit in O(claimsForNeed) rather than walking
// all claims. The broker keeps the forward (claimedBy) and reverse
// (claimedByNeed) maps in sync atomically inside each Propose call.
type SharedState struct {
	snap *inventory.Snapshot

	mu            sync.Mutex
	claimedBy     map[machine.ID]claim
	claimedByNeed map[*needs.Need]map[machine.ID]struct{}
	bucketSeq     map[BucketKey]uint64
	sameDomain    map[*needs.Need]string
	// sameSatisfiable records whether the joint pre-pass found ANY
	// bucket whose total covers the Need's deficit (ADR-0042 Addendum:
	// the structural-unsatisfiability signal the shard's parking age
	// keys on — a gang that merely lost claim races still sees a
	// satisfiable bucket and must not age toward parking).
	sameSatisfiable map[*needs.Need]bool
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
		snap:            snap,
		claimedBy:       make(map[machine.ID]claim),
		claimedByNeed:   make(map[*needs.Need]map[machine.ID]struct{}),
		bucketSeq:       make(map[BucketKey]uint64),
		sameDomain:      make(map[*needs.Need]string),
		sameSatisfiable: make(map[*needs.Need]bool),
	}
}

// recordSameDomain stores the joint domain choice the pre-pass made
// for a Same-Profile Need (ADR-0040 Addendum: the domain is chosen
// once per Need per cycle; credit and acquisition are both confined
// to it). Written only by the single-threaded pre-pass; mu still
// guards it so worker reads need no ordering argument beyond the
// lock.
func (s *SharedState) recordSameDomain(n *needs.Need, domain string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sameDomain[n] = domain
}

// recordSameSatisfiable stores the pre-pass's structural-
// satisfiability verdict (see the sameSatisfiable field).
func (s *SharedState) recordSameSatisfiable(n *needs.Need, sat bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sameSatisfiable[n] = sat
}

// SameSatisfiableFor reports the pre-pass verdict for n; false for
// plain Needs and Needs with no buckets at all.
func (s *SharedState) SameSatisfiableFor(n *needs.Need) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sameSatisfiable[n]
}

// SameDomainFor returns the Same-domain the pre-pass chose for n, or
// "" when no domain exists anywhere for the Need (no creditable and
// no acquirable bucket — FindSame then keeps its best-bucket
// behaviour as the fallback). Keyed by Need pointer like the rest of
// the per-Need tracking; the pointers come from RunCycle's stable
// needPtrs slice and survive displacement re-queues unchanged
// (Broker.Propose round-trips claim.need into Result.Displaced).
func (s *SharedState) SameDomainFor(n *needs.Need) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sameDomain[n]
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
	if s.claimedByNeed[n] == nil {
		s.claimedByNeed[n] = make(map[machine.ID]struct{})
	}
	s.claimedByNeed[n][mid] = struct{}{}
}

// ClaimedFor returns a snapshot of the machine IDs currently
// claimed for n. Used by the per-Need worker flow to compute the
// residual deficit (AggregateResources minus Σ Allocatable across
// claimed machines) without walking the entire claimed-set.
//
// The returned slice is a fresh copy; the caller may mutate it
// freely. Order is not stable.
func (s *SharedState) ClaimedFor(n *needs.Need) []machine.ID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]machine.ID, 0, len(s.claimedByNeed[n]))
	for mid := range s.claimedByNeed[n] {
		out = append(out, mid)
	}
	return out
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

// DisplaceableBy reports whether a proposer with precedence prec
// could legitimately claim mid: either mid is unclaimed, or its
// incumbent has strictly lower precedence (so displacement at the
// broker would succeed). This is the candidate-finder's filter:
// FindBasic / FindSame / FindSpread call it instead of IsClaimed so
// that high-priority Needs propose for machines lower-priority
// Needs are sitting on, letting the broker's displacement logic
// enforce priority on conflict (bigfleet.md §16).
func (s *SharedState) DisplaceableBy(mid machine.ID, prec Precedence) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.claimedBy[mid]
	if !ok {
		return true
	}
	return inc.precedence.Less(prec)
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
