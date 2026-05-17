package occ_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
)

func TestSharedState_FreshIsEmpty(t *testing.T) {
	t.Parallel()
	s := occ.NewSharedState(inventory.New().Snapshot())

	if s.Snapshot() == nil {
		t.Fatal("Snapshot returned nil")
	}
	if got := s.BucketSeq(occ.BucketKey{}); got != 0 {
		t.Fatalf("BucketSeq on fresh state = %d, want 0", got)
	}
	if s.IsClaimed("m-1") {
		t.Fatal("IsClaimed on fresh state returned true")
	}
}

func TestSharedState_BucketsAreIndependent(t *testing.T) {
	t.Parallel()
	s := occ.NewSharedState(inventory.New().Snapshot())
	broker := occ.NewBroker(s)

	a := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-a"}
	b := occ.BucketKey{State: machine.StateIdle, ProfileFP: "fp-b"}

	r := broker.Propose(occ.Proposal{Bucket: a, Machines: []machine.ID{"m1"}, Mode: occ.ModeIncremental})
	if r.Status != occ.StatusCommitted {
		t.Fatalf("commit to a status = %v, want Committed", r.Status)
	}
	if got := s.BucketSeq(a); got != 1 {
		t.Fatalf("seq(a) after commit = %d, want 1", got)
	}
	if got := s.BucketSeq(b); got != 0 {
		t.Fatalf("seq(b) after commit to a = %d, want 0", got)
	}
}
