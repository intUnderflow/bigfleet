package occ_test

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// preClaim seeds an incumbent claim used by displacement tests. It
// allocates a fresh *needs.Need so post-displacement Result.Displaced
// entries have a non-nil Need pointer to check identity on. Reads
// the bucket's current seqno from state so chained preClaims on the
// same bucket compose.
func preClaim(t *testing.T, state *occ.SharedState, b *occ.Broker, bucket occ.BucketKey, mids []machine.ID, prec occ.Precedence, retries int) (*needs.Need, uint64) {
	t.Helper()
	n := &needs.Need{}
	r := b.Propose(occ.Proposal{
		Need:        n,
		Bucket:      bucket,
		Machines:    mids,
		ObservedSeq: state.BucketSeq(bucket),
		Mode:        occ.ModeIncremental,
		Precedence:  prec,
		RetriesLeft: retries,
	})
	if r.Status != occ.StatusCommitted {
		t.Fatalf("seed commit failed: Status = %v", r.Status)
	}
	return n, r.NewSeq
}

func TestBroker_HigherPriorityDisplacesIncumbent(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	lowPri := occ.Precedence{Priority: 100}
	highPri := occ.Precedence{Priority: 200}
	_, seq := preClaim(t, s, b, bucket, []machine.ID{"m1"}, lowPri, 10)

	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1"}, ObservedSeq: seq,
		Precedence:  highPri,
		Mode:        occ.ModeIncremental,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed", r.Status)
	}
	if len(r.Committed) != 1 || r.Committed[0] != "m1" {
		t.Fatalf("Committed = %v, want [m1]", r.Committed)
	}
	if len(r.Displaced) != 1 {
		t.Fatalf("Displaced = %v, want 1 incumbent", r.Displaced)
	}
	if got, want := r.Displaced[0].RetriesLeft, 9; got != want {
		t.Errorf("displaced RetriesLeft = %d, want %d (decremented)", got, want)
	}
	if !s.IsClaimed("m1") {
		t.Fatal("m1 not claimed by displacing proposal")
	}
}

func TestBroker_EqualPrecedenceDoesNotDisplace(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	prec := occ.Precedence{Priority: 100}
	_, seq := preClaim(t, s, b, bucket, []machine.ID{"m1"}, prec, 10)

	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1"}, ObservedSeq: seq,
		Precedence:  prec,
		Mode:        occ.ModeIncremental,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusConflict {
		t.Fatalf("equal-precedence: Status = %v, want Conflict", r.Status)
	}
	if len(r.Displaced) != 0 {
		t.Fatalf("equal-precedence emitted Displaced = %v", r.Displaced)
	}
	if !s.IsClaimed("m1") {
		t.Fatal("incumbent m1 should still be claimed")
	}
}

func TestBroker_LowerPrecedenceDoesNotDisplace(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	highPri := occ.Precedence{Priority: 200}
	lowPri := occ.Precedence{Priority: 100}
	_, seq := preClaim(t, s, b, bucket, []machine.ID{"m1"}, highPri, 10)

	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1"}, ObservedSeq: seq,
		Precedence:  lowPri,
		Mode:        occ.ModeIncremental,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusConflict {
		t.Fatalf("lower-precedence: Status = %v, want Conflict", r.Status)
	}
}

func TestBroker_PenaltyBucketsBreakPriorityTies(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	// Same priority, lower interruption penalty bucket — incumbent is
	// the weaker one, gets displaced.
	weak := occ.Precedence{Priority: 100, InterruptionPenalty: 1}
	strong := occ.Precedence{Priority: 100, InterruptionPenalty: 5}
	_, seq := preClaim(t, s, b, bucket, []machine.ID{"m1"}, weak, 10)

	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1"}, ObservedSeq: seq,
		Precedence:  strong,
		Mode:        occ.ModeIncremental,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed", r.Status)
	}
	if len(r.Displaced) != 1 {
		t.Fatalf("Displaced = %v, want 1", r.Displaced)
	}
}

func TestBroker_AllOrNothingWithDisplacement_HappyPath(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	lowPri := occ.Precedence{Priority: 100}
	highPri := occ.Precedence{Priority: 200}

	// Seed two incumbents at low priority on m1, m2.
	_, seq := preClaim(t, s, b, bucket, []machine.ID{"m1", "m2"}, lowPri, 10)

	// High-priority gang proposal claims [m1, m2, m3] atomically.
	// m1 and m2 must be displaced; m3 was free; AllOrNothing
	// commits the lot.
	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1", "m2", "m3"}, ObservedSeq: seq,
		Precedence:  highPri,
		Mode:        occ.ModeAllOrNothing,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed", r.Status)
	}
	if len(r.Committed) != 3 {
		t.Fatalf("Committed = %v, want 3", r.Committed)
	}
	// Both m1 and m2 belonged to the same incumbent Need; the broker
	// dedupes displaced entries by Need, so one entry covers both.
	if len(r.Displaced) != 1 {
		t.Fatalf("Displaced = %v, want 1 (single Need across m1+m2)", r.Displaced)
	}
	for _, mid := range []machine.ID{"m1", "m2", "m3"} {
		if !s.IsClaimed(mid) {
			t.Errorf("AllOrNothing left %q unclaimed", mid)
		}
	}
}

func TestBroker_AllOrNothingAbortsOnImmovableIncumbent(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	lowPri := occ.Precedence{Priority: 100}
	highPri := occ.Precedence{Priority: 200}

	// Pre-claim m1 at HIGH priority (immovable from a same-priority
	// proposer) and m2 at LOW priority (would be displaceable).
	_, _ = preClaim(t, s, b, bucket, []machine.ID{"m1"}, highPri, 10)
	_, _ = preClaim(t, s, b, bucket, []machine.ID{"m2"}, lowPri, 10)

	// AllOrNothing at same priority as m1's incumbent. m1 is
	// immovable → abort. m2 must NOT be displaced.
	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1", "m2", "m3"}, ObservedSeq: s.BucketSeq(bucket),
		Precedence:  highPri,
		Mode:        occ.ModeAllOrNothing,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusConflict {
		t.Fatalf("Status = %v, want Conflict", r.Status)
	}
	if len(r.Displaced) != 0 {
		t.Fatalf("aborted AllOrNothing emitted Displaced = %v", r.Displaced)
	}
	if s.IsClaimed("m3") {
		t.Fatal("aborted AllOrNothing leaked claim on m3")
	}
	// m2 must still be claimed by the original low-priority incumbent.
	if !s.IsClaimed("m2") {
		t.Fatal("aborted AllOrNothing displaced m2 without committing")
	}
}

func TestBroker_IncrementalMixedDisplaceAndConflict(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	lowPri := occ.Precedence{Priority: 100}
	midPri := occ.Precedence{Priority: 150}
	highPri := occ.Precedence{Priority: 200}

	// Pre-claim m1@high (immovable from mid), m2@low (displaceable
	// by mid), m3 is free.
	_, _ = preClaim(t, s, b, bucket, []machine.ID{"m1"}, highPri, 10)
	_, _ = preClaim(t, s, b, bucket, []machine.ID{"m2"}, lowPri, 10)

	// Mid-priority Incremental over [m1, m2, m3]: should commit
	// m2 (displaced) + m3 (new); m1's incumbent is immovable, so m1
	// is absent from Committed and keeps its high-priority owner.
	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1", "m2", "m3"}, ObservedSeq: s.BucketSeq(bucket),
		Precedence:  midPri,
		Mode:        occ.ModeIncremental,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed (partial)", r.Status)
	}
	committed := sortedIDs(r.Committed)
	if !equalIDs(committed, []machine.ID{"m2", "m3"}) {
		t.Errorf("Committed = %v, want [m2 m3]", committed)
	}
	if got := s.OwnersForTest()["m1"]; got != highPri {
		t.Errorf("m1 owner precedence = %+v, want immovable incumbent %+v", got, highPri)
	}
	if len(r.Displaced) != 1 {
		t.Fatalf("Displaced = %v, want 1 (m2's old owner)", r.Displaced)
	}
}

func TestBroker_DisplacedAtZeroRetriesEmitsBudgetZero(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	lowPri := occ.Precedence{Priority: 100}
	highPri := occ.Precedence{Priority: 200}

	// Seed incumbent with retriesLeft=1 (one retry remaining).
	_, seq := preClaim(t, s, b, bucket, []machine.ID{"m1"}, lowPri, 1)

	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1"}, ObservedSeq: seq,
		Precedence:  highPri,
		Mode:        occ.ModeIncremental,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed", r.Status)
	}
	if len(r.Displaced) != 1 {
		t.Fatalf("Displaced = %v, want 1", r.Displaced)
	}
	if got := r.Displaced[0].RetriesLeft; got != 0 {
		t.Errorf("Displaced.RetriesLeft = %d, want 0 (exhausted — caller emits shortfall)", got)
	}
}

func TestBroker_DisplacementMutationsAreAtomic(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	// Two incumbents owned by Need A on m1+m2.
	lowPri := occ.Precedence{Priority: 100}
	_, seq := preClaim(t, s, b, bucket, []machine.ID{"m1", "m2"}, lowPri, 10)

	// High-priority Need B displaces both. Both displacement
	// records must reference the same incumbent Need pointer.
	highPri := occ.Precedence{Priority: 200}
	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1", "m2"}, ObservedSeq: seq,
		Precedence:  highPri,
		Mode:        occ.ModeAllOrNothing,
		RetriesLeft: 10,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed", r.Status)
	}
	// Broker dedupes displaced incumbents by Need pointer: m1 + m2
	// both belonged to the same Need, so one Displaced entry covers
	// the pair (with retriesLeft = min retries across the
	// displaced machines).
	if len(r.Displaced) != 1 {
		t.Fatalf("Displaced = %v, want 1 (deduped by Need)", r.Displaced)
	}
	for _, d := range r.Displaced {
		if d.Need == nil {
			t.Errorf("Displaced entry has nil Need")
		}
	}
	if !s.IsClaimed("m1") || !s.IsClaimed("m2") {
		t.Fatal("post-displacement claims missing")
	}
}

// TestBroker_PriorityIsMonotoneUnderConcurrency stresses the broker
// with many workers racing across multiple priority bands. After the
// race, every claim's precedence must be ≥ any proposer that
// attempted that machine, i.e. priority on conflict is monotone —
// no low-precedence claim survives in the presence of a
// higher-precedence proposer that fought for the same machine.
func TestBroker_PriorityIsMonotoneUnderConcurrency(t *testing.T) {
	t.Parallel()
	state, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	const totalMachines = 100
	const proposalsPerLevel = 64
	priorities := []int32{50, 100, 150, 200}

	var wg sync.WaitGroup
	for _, pri := range priorities {
		pri := pri
		for w := 0; w < proposalsPerLevel; w++ {
			w := w
			wg.Add(1)
			go func() {
				defer wg.Done()
				prec := occ.Precedence{Priority: pri}
				for i := 0; i < totalMachines; i++ {
					mid := machine.ID("m-" + strconv.Itoa((i+w*7)%totalMachines))
					// Bounded retry loop; conflict means seqno
					// moved OR target was immovable. Either way
					// the loop budget keeps us terminating.
					for retry := 0; retry < 50; retry++ {
						seq := state.BucketSeq(bucket)
						r := b.Propose(occ.Proposal{
							Bucket: bucket, Machines: []machine.ID{mid},
							ObservedSeq: seq,
							Precedence:  prec,
							Mode:        occ.ModeIncremental,
							RetriesLeft: 10,
						})
						if r.Status == occ.StatusCommitted && len(r.Committed) > 0 {
							break
						}
						// Immovable incumbent? Move on — we can't
						// win this machine at this priority.
						// (!DisplaceableBy ⇔ claimed by a
						// ≥-precedence owner.)
						if !state.DisplaceableBy(mid, prec) {
							break
						}
					}
				}
			}()
		}
	}
	wg.Wait()

	// Invariant: every claimed machine is owned by the highest
	// priority that competed for it. With our test setup every
	// proposer touched every machine, so every final claim should
	// belong to the highest priority (200).
	owners := state.OwnersForTest()
	if len(owners) == 0 {
		t.Fatal("no machine ended up claimed")
	}
	for mid, prec := range owners {
		if prec.Priority != 200 {
			t.Errorf("machine %q owned by priority %d; want 200 (highest competitor)", mid, prec.Priority)
		}
	}
}

// TestBroker_ConservationOfClaimedSet races many workers proposing
// single-machine claims and asserts the per-commit conservation
// law: each successful Propose adds (len(Committed) − len(Displaced))
// entries to claimedBy. Under single-machine proposals each Displaced
// entry represents exactly one released machine, so Σ Committed −
// Σ Displaced equals the final |claimedBy| count. This is the
// broker-side contribution to the ADR-0027 stage 5.1 attribution
// invariant: Phase 1's accounting of who owns what stays coherent
// under arbitrary concurrent mutation. Multi-machine proposals have
// a deduplication factor (multiple machines from one Need collapse
// to one Displaced entry); separate tests cover that.
func TestBroker_ConservationOfClaimedSet(t *testing.T) {
	t.Parallel()
	state, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	const workers = 16
	const machines = 50

	var committedTotal atomic.Int64
	var displacedTotal atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			prec := occ.Precedence{Priority: int32(50 + 10*w)} // distinct priorities
			for i := 0; i < machines; i++ {
				mid := machine.ID("m-" + strconv.Itoa(i))
				for retry := 0; retry < 30; retry++ {
					seq := state.BucketSeq(bucket)
					r := b.Propose(occ.Proposal{
						Bucket: bucket, Machines: []machine.ID{mid},
						ObservedSeq: seq, Mode: occ.ModeIncremental,
						Precedence:  prec,
						RetriesLeft: 10,
					})
					if r.Status == occ.StatusCommitted {
						committedTotal.Add(int64(len(r.Committed)))
						displacedTotal.Add(int64(len(r.Displaced)))
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	owners := state.OwnersForTest()
	expected := committedTotal.Load() - displacedTotal.Load()
	if int64(len(owners)) != expected {
		t.Fatalf("|claimedBy| = %d, want %d (Σ Committed=%d − Σ Displaced=%d)",
			len(owners), expected, committedTotal.Load(), displacedTotal.Load())
	}
	if len(owners) > machines {
		t.Fatalf("|claimedBy| = %d exceeds machine universe = %d", len(owners), machines)
	}
}

func sortedIDs(in []machine.ID) []machine.ID {
	out := make([]machine.ID, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func equalIDs(a, b []machine.ID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
