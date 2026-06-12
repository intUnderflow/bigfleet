package occ_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

func freshBroker() (*occ.SharedState, *occ.Broker) {
	s := occ.NewSharedState(inventory.New().Snapshot())
	return s, occ.NewBroker(s)
}

func TestBroker_HappyPathCommit(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	r := b.Propose(occ.Proposal{
		Bucket:      bucket,
		Machines:    []machine.ID{"m1", "m2"},
		ObservedSeq: 0,
		Mode:        occ.ModeIncremental,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed", r.Status)
	}
	if len(r.Committed) != 2 {
		t.Fatalf("Committed = %v, want 2 machines", r.Committed)
	}
	if r.NewSeq != 1 {
		t.Fatalf("NewSeq = %d, want 1", r.NewSeq)
	}
	if got := s.BucketSeq(bucket); got != 1 {
		t.Fatalf("BucketSeq after commit = %d, want 1", got)
	}
	if !s.IsClaimed("m1") || !s.IsClaimed("m2") {
		t.Fatalf("machines not claimed: m1=%v m2=%v", s.IsClaimed("m1"), s.IsClaimed("m2"))
	}
}

func TestBroker_StaleSeqnoConflicts(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	// First commit moves seqno 0 → 1.
	_ = b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1"}, ObservedSeq: 0,
		Mode: occ.ModeIncremental,
	})

	// Second commit submits with stale seqno=0.
	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m2"}, ObservedSeq: 0,
		Mode: occ.ModeIncremental,
	})

	if r.Status != occ.StatusConflict {
		t.Fatalf("Status = %v, want Conflict", r.Status)
	}
	if r.NewSeq != 1 {
		t.Fatalf("NewSeq on conflict = %d, want 1 (current seqno)", r.NewSeq)
	}
	if len(r.Committed) != 0 {
		t.Fatalf("conflict left Committed = %v, want empty (no mutation)", r.Committed)
	}
	if s.IsClaimed("m2") {
		t.Fatal("conflict leaked a claim on m2")
	}
}

func TestBroker_IncrementalPartialCommit(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	a := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-a"}
	c := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-c"}

	// Bucket a claims m2.
	_ = b.Propose(occ.Proposal{
		Bucket: a, Machines: []machine.ID{"m2"}, ObservedSeq: 0,
		Mode: occ.ModeIncremental,
	})

	// Need C proposes [m1, m2, m3] from a different bucket. m2 is
	// already claimed by an immovable (equal-precedence) incumbent →
	// partial commit of exactly [m1, m3].
	r := b.Propose(occ.Proposal{
		Bucket: c, Machines: []machine.ID{"m1", "m2", "m3"}, ObservedSeq: 0,
		Mode: occ.ModeIncremental,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed (partial)", r.Status)
	}
	if got := sortedIDs(r.Committed); !equalIDs(got, []machine.ID{"m1", "m3"}) {
		t.Fatalf("Committed = %v, want [m1 m3] (m2 immovable)", got)
	}
	if !s.IsClaimed("m1") || !s.IsClaimed("m3") {
		t.Fatal("partial-commit machines not claimed")
	}
}

func TestBroker_AllOrNothingHappyPath(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp", SameKey: "rack", SameValue: "r1"}

	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: []machine.ID{"m1", "m2", "m3"}, ObservedSeq: 0,
		Mode: occ.ModeAllOrNothing,
	})

	if r.Status != occ.StatusCommitted {
		t.Fatalf("Status = %v, want Committed", r.Status)
	}
	if len(r.Committed) != 3 {
		t.Fatalf("Committed = %v, want all 3", r.Committed)
	}
	for _, mid := range []machine.ID{"m1", "m2", "m3"} {
		if !s.IsClaimed(mid) {
			t.Errorf("AllOrNothing didn't claim %q", mid)
		}
	}
}

func TestBroker_AllOrNothingAbortsOnAnyConflict(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	a := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-a"}
	c := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-c"}

	// Bucket a claims m2.
	_ = b.Propose(occ.Proposal{
		Bucket: a, Machines: []machine.ID{"m2"}, ObservedSeq: 0,
		Mode: occ.ModeIncremental,
	})

	// AllOrNothing proposal touching m2 must abort entirely.
	r := b.Propose(occ.Proposal{
		Bucket: c, Machines: []machine.ID{"m1", "m2", "m3"}, ObservedSeq: 0,
		Mode: occ.ModeAllOrNothing,
	})

	if r.Status != occ.StatusConflict {
		t.Fatalf("Status = %v, want Conflict", r.Status)
	}
	if s.IsClaimed("m1") || s.IsClaimed("m3") {
		t.Fatal("AllOrNothing abort leaked claims for m1/m3")
	}
	if got := s.BucketSeq(c); got != 0 {
		t.Fatalf("seq(c) after abort = %d, want 0 (no mutation)", got)
	}
}

func TestBroker_AllMachinesClaimed_BothModesConflict(t *testing.T) {
	t.Parallel()
	for _, mode := range []occ.ProposalMode{occ.ModeIncremental, occ.ModeAllOrNothing} {
		mode := mode
		t.Run(modeName(mode), func(t *testing.T) {
			t.Parallel()
			_, b := freshBroker()
			a := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-a"}
			c := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-c"}

			// Pre-claim m1 + m2 via bucket a.
			_ = b.Propose(occ.Proposal{
				Bucket: a, Machines: []machine.ID{"m1", "m2"}, ObservedSeq: 0,
				Mode: occ.ModeIncremental,
			})

			r := b.Propose(occ.Proposal{
				Bucket: c, Machines: []machine.ID{"m1", "m2"}, ObservedSeq: 0,
				Mode: mode,
			})
			if r.Status != occ.StatusConflict {
				t.Fatalf("Status = %v, want Conflict", r.Status)
			}
		})
	}
}

func TestBroker_SeqnoMonotonePerBucket(t *testing.T) {
	t.Parallel()
	s, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	for i := 0; i < 5; i++ {
		r := b.Propose(occ.Proposal{
			Bucket: bucket, Machines: []machine.ID{machine.ID("m-" + strconv.Itoa(i))},
			ObservedSeq: uint64(i), Mode: occ.ModeIncremental,
		})
		if r.Status != occ.StatusCommitted {
			t.Fatalf("commit %d: Status = %v", i, r.Status)
		}
		if r.NewSeq != uint64(i+1) {
			t.Fatalf("commit %d: NewSeq = %d, want %d", i, r.NewSeq, i+1)
		}
	}
	if got := s.BucketSeq(bucket); got != 5 {
		t.Fatalf("final seq = %d, want 5", got)
	}
}

// TestBroker_ConcurrentNoDoubleClaim races many workers proposing
// overlapping machines through one broker. The invariant: each
// machine is claimed at most once, no matter how aggressively
// proposals race.
//
// Run under -race to catch any unsynchronised access to the shared
// claimedBy / bucketSeq maps.
func TestBroker_ConcurrentNoDoubleClaim(t *testing.T) {
	t.Parallel()
	state, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	const workers = 32
	const totalMachines = 200
	const retryBudget = 200

	committedByWorker := make([][]machine.ID, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < totalMachines; i++ {
				mid := machine.ID("m-" + strconv.Itoa(i))
				for retry := 0; retry < retryBudget; retry++ {
					seq := state.BucketSeq(bucket)
					r := b.Propose(occ.Proposal{
						Bucket:      bucket,
						Machines:    []machine.ID{mid},
						ObservedSeq: seq,
						Mode:        occ.ModeIncremental,
					})
					if r.Status == occ.StatusCommitted && len(r.Committed) > 0 {
						committedByWorker[w] = append(committedByWorker[w], r.Committed[0])
						break
					}
					if state.IsClaimed(mid) {
						// Lost the race for this machine; move on.
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	// Invariants: no machine in two workers' committed sets; total
	// committed ≤ totalMachines.
	owner := make(map[machine.ID]int)
	total := 0
	for w := 0; w < workers; w++ {
		for _, mid := range committedByWorker[w] {
			if prev, ok := owner[mid]; ok {
				t.Errorf("double-claim: %q committed by workers %d and %d", mid, prev, w)
			}
			owner[mid] = w
			total++
		}
	}
	if total > totalMachines {
		t.Fatalf("total committed = %d > universe = %d (double-claim)", total, totalMachines)
	}
	// With 32 workers retrying 200 times each, every machine ought
	// to find an owner — sanity bound.
	if total == 0 {
		t.Fatal("no machine was committed; broker is broken")
	}
}

func TestBroker_IncrementalEmptyMachinesIsConflict(t *testing.T) {
	t.Parallel()
	_, b := freshBroker()
	bucket := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp"}

	r := b.Propose(occ.Proposal{
		Bucket: bucket, Machines: nil, ObservedSeq: 0,
		Mode: occ.ModeIncremental,
	})

	if r.Status != occ.StatusConflict {
		t.Fatalf("empty Incremental: Status = %v, want Conflict (nothing to claim)", r.Status)
	}
}

func TestPrecedence_Less(t *testing.T) {
	t.Parallel()
	a := occ.Precedence{Priority: 100, InterruptionPenalty: needs.PenaltyBucket1024, ReclamationPenalty: needs.PenaltyBucket1}
	higherPri := occ.Precedence{Priority: 200, InterruptionPenalty: needs.PenaltyBucket1, ReclamationPenalty: needs.PenaltyBucket1}
	higherIPen := occ.Precedence{Priority: 100, InterruptionPenalty: needs.PenaltyBucket2048, ReclamationPenalty: needs.PenaltyBucket1}
	higherRPen := occ.Precedence{Priority: 100, InterruptionPenalty: needs.PenaltyBucket1024, ReclamationPenalty: needs.PenaltyBucket8}

	if !a.Less(higherPri) {
		t.Error("priority 100 < 200 should be Less")
	}
	if higherPri.Less(a) {
		t.Error("priority 200 should not be Less than 100 regardless of penalty")
	}
	if !a.Less(higherIPen) {
		t.Error("equal priority, lower interruption penalty should be Less")
	}
	if !a.Less(higherRPen) {
		t.Error("equal priority+interruption, lower reclamation penalty should be Less")
	}
	if a.Less(a) {
		t.Error("equal precedence should not be Less than itself")
	}
}

func modeName(m occ.ProposalMode) string {
	switch m {
	case occ.ModeIncremental:
		return "Incremental"
	case occ.ModeAllOrNothing:
		return "AllOrNothing"
	}
	return "unknown"
}
