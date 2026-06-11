package shard

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// TestParkingBookkeeping pins the ADR-0042 Addendum age machinery:
// a Same-Need class parks after parkAfterCycles consecutive
// unsatisfiable cycles, un-parks for exactly one cycle at each
// re-probe boundary, and is forgotten the cycle it stops being
// unsatisfiable (so fresh supply un-parks it permanently).
func TestParkingBookkeeping(t *testing.T) {
	s := &Shard{unsatSameAge: make(map[string]int)}

	gang := needs.Need{
		ClusterID: "c1",
		Group:     "gang-1",
		Profile: needs.NewProfile([]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"a3-highgpu-8g"}},
			{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame},
		}, nil, 1000, needs.PenaltyBucket1024, needs.PenaltyBucket1),
		AggregateResources: []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "96"}},
		MinUnit:            []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}},
	}
	plain := needs.Need{
		ClusterID: "c1",
		Profile: needs.NewProfile([]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"m6i.large"}},
		}, nil, 1000, needs.PenaltyBucket1024, needs.PenaltyBucket1),
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "100"}},
	}

	unsat := decision.Phase1Result{Unsatisfied: []decision.UnsatisfiedNeed{
		{Need: gang}, {Need: plain},
	}}

	parkedAt := func(cycle int) bool {
		demand := []needs.Need{gang, plain}
		s.stampParkedNeeds(demand)
		if demand[1].AcquisitionParked {
			t.Fatalf("cycle %d: plain Need parked — parking is Same-only", cycle)
		}
		return demand[0].AcquisitionParked
	}

	// Ages 1..parkAfterCycles-1: not parked yet (debounce).
	for c := 1; c < parkAfterCycles; c++ {
		s.recordUnsatSameAges(unsat)
		if parkedAt(c) {
			t.Fatalf("cycle %d (age %d): parked before the threshold", c, c)
		}
	}
	// Threshold reached: parked, and stays parked except at re-probe
	// boundaries (age %% reprobeEveryCycles == 0), where it un-parks
	// for exactly that cycle.
	for c := parkAfterCycles; c <= 3*reprobeEveryCycles; c++ {
		s.recordUnsatSameAges(unsat)
		want := c%reprobeEveryCycles != 0
		if got := parkedAt(c); got != want {
			t.Fatalf("cycle %d (age %d): parked=%v, want %v", c, c, got, want)
		}
	}
	// Progress resets: an unsatisfied cycle that still acquired
	// machines (concentration ongoing) must clear the age — parking is
	// concentrate-THEN-park, never mid-concentration.
	s.recordUnsatSameAges(decision.Phase1Result{Unsatisfied: []decision.UnsatisfiedNeed{
		{Need: gang, Acquired: 4}, {Need: plain},
	}})
	if len(s.unsatSameAge) != 0 {
		t.Fatalf("progressing class not reset: %v", s.unsatSameAge)
	}
	for c := 1; c <= parkAfterCycles; c++ {
		s.recordUnsatSameAges(unsat)
	}
	if !parkedAt(-1) {
		t.Fatal("class must re-park after the debounce re-runs post-progress")
	}

	// The class resolves (not unsatisfied this cycle): forgotten, so a
	// later relapse starts the debounce from zero.
	s.recordUnsatSameAges(decision.Phase1Result{})
	if len(s.unsatSameAge) != 0 {
		t.Fatalf("resolved class not forgotten: %v", s.unsatSameAge)
	}
	s.recordUnsatSameAges(unsat)
	if parkedAt(0) {
		t.Fatal("relapsed class parked at age 1 — debounce must restart")
	}
}

// TestUnsatSameKey_GroupDisambiguates pins that two gangs of identical
// profile but different Group age independently (the wire now carries
// group — ADR-0042 Addendum).
func TestUnsatSameKey_GroupDisambiguates(t *testing.T) {
	prof := needs.NewProfile([]needs.Requirement{
		{Key: "topology.bigfleet/rack", Operator: needs.OperatorSame},
	}, nil, 1000, needs.PenaltyBucket1024, needs.PenaltyBucket1)
	a := needs.Need{ClusterID: "c1", Group: "g-a", Profile: prof}
	b := needs.Need{ClusterID: "c1", Group: "g-b", Profile: prof}
	if unsatSameKey(&a) == unsatSameKey(&b) {
		t.Fatal("distinct groups share an age key")
	}
	c := a // identical content, distinct value: the key must be content-derived
	if unsatSameKey(&a) != unsatSameKey(&c) {
		t.Fatal("key not stable across copies of the same Need")
	}
}
