package shard

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

func nvProfile(priority int32, same bool) needs.Profile {
	reqs := []needs.Requirement{
		{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"m6i.large"}},
	}
	if same {
		reqs = append(reqs, needs.Requirement{Key: "topology.kubernetes.io/zone", Operator: needs.OperatorSame})
	}
	return needs.NewProfile(reqs, nil, priority, needs.PenaltyBucket1, needs.PenaltyBucket1)
}

func nvQty(cpu string) []needs.ResourceQty {
	return []needs.ResourceQty{{Name: "cpu", Quantity: cpu}}
}

// recordNeedViews must classify every Need's verdict from the Phase 1
// per-Need verdicts joined with Phase 2's unresolved set, and NeedViews
// must return them filtered + sorted (ADR-0061).
func TestRecordNeedViews_ClassifiesAndFilters(t *testing.T) {
	t.Parallel()
	s := &Shard{}

	nA := needs.Need{ClusterID: "cluster-a", Profile: nvProfile(1000, false), AggregateResources: nvQty("8"), MinUnit: nvQty("1")}
	nB := needs.Need{ClusterID: "cluster-b", Profile: nvProfile(1000, false), AggregateResources: nvQty("8"), MinUnit: nvQty("1")}
	nC := needs.Need{ClusterID: "cluster-c", Profile: nvProfile(2000, false), AggregateResources: nvQty("8"), MinUnit: nvQty("1")}
	nD := needs.Need{ClusterID: "cluster-d", Profile: nvProfile(500, false), AggregateResources: nvQty("8"), MinUnit: nvQty("1")}
	nE := needs.Need{ClusterID: "cluster-e", Profile: nvProfile(1500, true), AggregateResources: nvQty("8"), MinUnit: nvQty("1")}

	p1 := decision.Phase1Result{Verdicts: []decision.NeedVerdict{
		{Need: &nA, Satisfied: true, ClaimedCount: 2, BootstrapCount: 2},
		{Need: &nB, Satisfied: false, Deficit: nvQty("8"), MatchingSupplyExists: false},
		{Need: &nC, Satisfied: false, Deficit: nvQty("8"), MatchingSupplyExists: true},
		{Need: &nD, Satisfied: false, Deficit: nvQty("8"), MatchingSupplyExists: true},
		{Need: &nE, Satisfied: false, Deficit: nvQty("8"), MatchingSupplyExists: true, SameSatisfiable: false},
	}}
	p2 := decision.Phase2Result{Unresolved: []decision.UnsatisfiedNeed{
		{Need: nC, Deficit: nvQty("8"), Preempted: true},  // freed some, fell short
		{Need: nD, Deficit: nvQty("8"), Preempted: false}, // no displaceable victim
		{Need: nE, Deficit: nvQty("8")},                   // topology — caught first
	}}

	s.recordNeedViews(42, p1, p2)

	all := s.NeedViews("", 0)
	if all.Cycle != 42 {
		t.Fatalf("cycle = %d, want 42", all.Cycle)
	}
	if all.Total != 5 || len(all.Views) != 5 {
		t.Fatalf("views = %d/%d, want 5/5", all.Total, len(all.Views))
	}
	// Sorted priority desc: nC(2000), nE(1500), nA(1000), nB(1000), nD(500).
	if all.Views[0].ClusterID != "cluster-c" || all.Views[len(all.Views)-1].ClusterID != "cluster-d" {
		t.Errorf("sort order wrong: first=%s last=%s", all.Views[0].ClusterID, all.Views[len(all.Views)-1].ClusterID)
	}

	want := map[machine.ClusterID]struct {
		satisfied bool
		reason    NeedUnmetReason
	}{
		"cluster-a": {true, NeedReasonUnspecified},
		"cluster-b": {false, NeedReasonNoMatchingSupply},
		"cluster-c": {false, NeedReasonPreemptionExhausted},
		"cluster-d": {false, NeedReasonPriorityStarved},
		"cluster-e": {false, NeedReasonTopologyUnsatisfiable},
	}
	for _, v := range all.Views {
		w, ok := want[v.ClusterID]
		if !ok {
			t.Errorf("unexpected cluster %s", v.ClusterID)
			continue
		}
		if v.Satisfied != w.satisfied {
			t.Errorf("%s satisfied = %v, want %v", v.ClusterID, v.Satisfied, w.satisfied)
		}
		if v.UnmetReason != w.reason {
			t.Errorf("%s reason = %d, want %d", v.ClusterID, v.UnmetReason, w.reason)
		}
	}

	// Per-cluster filter returns only that cluster.
	one := s.NeedViews("cluster-c", 0)
	if one.Total != 1 || len(one.Views) != 1 || one.Views[0].ClusterID != "cluster-c" {
		t.Errorf("cluster filter wrong: total=%d views=%d", one.Total, len(one.Views))
	}
	if one.Views[0].UnmetReason != NeedReasonPreemptionExhausted {
		t.Errorf("cluster-c reason = %d, want PreemptionExhausted", one.Views[0].UnmetReason)
	}

	// Limit caps the rows but Total reports the pre-cap count.
	capped := s.NeedViews("", 2)
	if capped.Total != 5 || len(capped.Views) != 2 {
		t.Errorf("limit: total=%d views=%d, want 5/2", capped.Total, len(capped.Views))
	}
}

// A shard that has never run a cycle reports an empty ledger with cycle 0
// ("rebuilding"), distinct from "no demand".
func TestNeedViews_EmptyBeforeFirstCycle(t *testing.T) {
	t.Parallel()
	s := &Shard{}
	r := s.NeedViews("", 0)
	if r.Cycle != 0 || len(r.Views) != 0 {
		t.Errorf("empty ledger: cycle=%d views=%d, want 0/0", r.Cycle, len(r.Views))
	}
}
