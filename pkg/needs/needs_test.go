package needs_test

import (
	"sort"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

func TestBucketForDollars(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   float64
		want needs.PenaltyBucket
	}{
		{0, needs.PenaltyBucketZero},
		{0.25, needs.PenaltyBucketHalfDollar},
		{0.50, needs.PenaltyBucketHalfDollar},
		{0.51, needs.PenaltyBucket1},
		{1.00, needs.PenaltyBucket1},
		{1.01, needs.PenaltyBucket2},
		{8000, needs.PenaltyBucket8192},
		{1_048_576, needs.PenaltyBucket1048576},
		{20_000_000, needs.PenaltyBucketPinned},
	}
	for _, tc := range cases {
		got := needs.BucketForDollars(tc.in)
		if got != tc.want {
			t.Errorf("BucketForDollars(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestProfile_Fingerprint_StableAcrossEqualInputs(t *testing.T) {
	t.Parallel()
	// ADR-0027: Profile is pure identity — requirements, spread, priority,
	// and the two penalty buckets. No resource shape participates.
	a := needs.NewProfile(
		[]needs.Requirement{
			{Key: "z", Operator: needs.OperatorIn, Values: []string{"b", "a"}},
			{Key: "a", Operator: needs.OperatorIn, Values: []string{"x"}},
		},
		nil,
		1_000_000,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
	b := needs.NewProfile(
		[]needs.Requirement{
			{Key: "a", Operator: needs.OperatorIn, Values: []string{"x"}},
			{Key: "z", Operator: needs.OperatorIn, Values: []string{"a", "b"}},
		},
		nil,
		1_000_000,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatalf("equal profiles produced different fingerprints:\n  a: %s\n  b: %s", a.Fingerprint(), b.Fingerprint())
	}
}

func TestProfile_Fingerprint_DiffersOnAnyField(t *testing.T) {
	t.Parallel()
	base := needs.NewProfile(nil, nil, 100, needs.PenaltyBucket1, needs.PenaltyBucket1)
	variants := []needs.Profile{
		needs.NewProfile(nil, nil, 200, needs.PenaltyBucket1, needs.PenaltyBucket1),
		needs.NewProfile(nil, nil, 100, needs.PenaltyBucket2, needs.PenaltyBucket1),
		needs.NewProfile(nil, nil, 100, needs.PenaltyBucket1, needs.PenaltyBucket2),
		needs.NewProfile(
			[]needs.Requirement{{Key: "x", Operator: needs.OperatorExists}},
			nil, 100, needs.PenaltyBucket1, needs.PenaltyBucket1),
		needs.NewProfile(
			nil,
			[]needs.TopologySpread{{TopologyKey: "zone", MaxSkew: 1}},
			100, needs.PenaltyBucket1, needs.PenaltyBucket1),
	}
	for i, v := range variants {
		if base.Fingerprint() == v.Fingerprint() {
			t.Errorf("variant %d collided with base fingerprint", i)
		}
	}
}

func TestAggregate_SumsResourcesAndMaxesMinUnit(t *testing.T) {
	t.Parallel()
	// ADR-0027: aggregating Needs that share (cluster, profile, group)
	// sums their AggregateResources and takes the per-dimension max of
	// MinUnit.
	pf := func(p int32) needs.Profile {
		return needs.NewProfile(nil, nil, p, needs.PenaltyBucket1, needs.PenaltyBucket1)
	}
	rq := func(cpu string) []needs.ResourceQty {
		return []needs.ResourceQty{{Name: "cpu", Quantity: cpu}}
	}
	in := []needs.Need{
		{ClusterID: "a", Profile: pf(100), AggregateResources: rq("1"), MinUnit: rq("1")},
		{ClusterID: "a", Profile: pf(100), AggregateResources: rq("1"), MinUnit: rq("1")},
		{ClusterID: "a", Profile: pf(100), AggregateResources: rq("3"), MinUnit: rq("3")},
		{ClusterID: "b", Profile: pf(100), AggregateResources: rq("2"), MinUnit: rq("2")},
		{ClusterID: "a", Profile: pf(200), AggregateResources: rq("4"), MinUnit: rq("4")},
	}
	agg := needs.Aggregate(in)

	type want struct{ aggCPU, minUnitCPU string }
	wantBy := map[string]want{
		"a|p=100": {"5", "3"}, // 1+1+3 summed; max(1,1,3) folded into min-unit
		"b|p=100": {"2", "2"},
		"a|p=200": {"4", "4"},
	}
	if len(agg) != len(wantBy) {
		t.Fatalf("aggregate produced %d entries, want %d", len(agg), len(wantBy))
	}
	for _, n := range agg {
		key := string(n.ClusterID) + "|p=" + intToString(int(n.Profile.Priority()))
		w, ok := wantBy[key]
		if !ok {
			t.Errorf("unexpected aggregate entry %s", key)
			continue
		}
		if got := cpuOf(n.AggregateResources); got != w.aggCPU {
			t.Errorf("%s: aggregate cpu = %q, want %q", key, got, w.aggCPU)
		}
		if got := cpuOf(n.MinUnit); got != w.minUnitCPU {
			t.Errorf("%s: min-unit cpu = %q, want %q", key, got, w.minUnitCPU)
		}
	}
}

func cpuOf(rs []needs.ResourceQty) string {
	for _, r := range rs {
		if r.Name == "cpu" {
			return r.Quantity
		}
	}
	return ""
}

func TestTable_Replace_FullReplacementSemantics(t *testing.T) {
	t.Parallel()
	tbl := needs.NewTable()
	pf1 := needs.NewProfile(nil, nil, 100, needs.PenaltyBucket1, needs.PenaltyBucket1)
	pf2 := needs.NewProfile(nil, nil, 200, needs.PenaltyBucket1, needs.PenaltyBucket1)

	tbl.Replace("cluster-a", []needs.Need{
		{ClusterID: "cluster-a", Profile: pf1},
		{ClusterID: "cluster-a", Profile: pf2},
	})
	if got := tbl.Stats().Needs; got != 2 {
		t.Fatalf("after first replace: needs = %d, want 2", got)
	}

	// Replace with a smaller set — the prior 2 must be gone.
	tbl.Replace("cluster-a", []needs.Need{
		{ClusterID: "cluster-a", Profile: pf1},
	})
	if got := tbl.Stats().Needs; got != 1 {
		t.Fatalf("after second replace: needs = %d, want 1", got)
	}

	// Replace with empty — withdrawal.
	tbl.Replace("cluster-a", nil)
	if got := tbl.Stats().Needs; got != 0 {
		t.Fatalf("after empty replace: needs = %d, want 0", got)
	}
	if got := tbl.Stats().Clusters; got != 0 {
		t.Fatalf("after empty replace: clusters = %d, want 0", got)
	}
}

func TestTable_Snapshot_PrioritySortedThenArrival(t *testing.T) {
	t.Parallel()
	tbl := needs.NewTable()
	hi := needs.NewProfile(nil, nil, 1_000_000, needs.PenaltyBucket1, needs.PenaltyBucket1)
	mid := needs.NewProfile(nil, nil, 500_000, needs.PenaltyBucket1, needs.PenaltyBucket1)
	lo := needs.NewProfile(nil, nil, 0, needs.PenaltyBucket1, needs.PenaltyBucket1)

	tbl.Replace("cluster-a", []needs.Need{
		{ClusterID: "cluster-a", Profile: lo, ArrivalUnixNanos: 30},
		{ClusterID: "cluster-a", Profile: hi, ArrivalUnixNanos: 20},
	})
	tbl.Replace("cluster-b", []needs.Need{
		{ClusterID: "cluster-b", Profile: mid, ArrivalUnixNanos: 10},
		{ClusterID: "cluster-b", Profile: hi, ArrivalUnixNanos: 5},
	})

	snap := tbl.Snapshot()
	wantOrder := []struct {
		cluster  machine.ClusterID
		priority int32
	}{
		{"cluster-b", 1_000_000}, // earliest arrival within priority
		{"cluster-a", 1_000_000},
		{"cluster-b", 500_000},
		{"cluster-a", 0},
	}
	if len(snap) != len(wantOrder) {
		t.Fatalf("snapshot len = %d, want %d", len(snap), len(wantOrder))
	}
	for i, w := range wantOrder {
		if snap[i].ClusterID != w.cluster || snap[i].Profile.Priority() != w.priority {
			t.Errorf("snap[%d] = (%s, %d), want (%s, %d)",
				i, snap[i].ClusterID, snap[i].Profile.Priority(), w.cluster, w.priority)
		}
	}
}

func TestTable_Snapshot_CachedAcrossCleanReads(t *testing.T) {
	t.Parallel()
	tbl := needs.NewTable()
	pf := needs.NewProfile(nil, nil, 100, needs.PenaltyBucket1, needs.PenaltyBucket1)
	tbl.Replace("cluster-a", []needs.Need{
		{ClusterID: "cluster-a", Profile: pf},
	})
	first := tbl.Snapshot()
	second := tbl.Snapshot()
	// Both snapshots returned independent slices but with equal contents.
	if len(first) != len(second) {
		t.Fatalf("len mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].ClusterID != second[i].ClusterID {
			t.Errorf("cluster mismatch at %d", i)
		}
	}
	// Mutate first; second must not change.
	first[0].Group = "mutated"
	if second[0].Group == "mutated" {
		t.Fatalf("snapshots not isolated: mutating first changed second")
	}
}

func TestTable_Concurrent_NoRace(t *testing.T) {
	t.Parallel()
	tbl := needs.NewTable()
	pf := needs.NewProfile(nil, nil, 100, needs.PenaltyBucket1, needs.PenaltyBucket1)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			tbl.Replace(machine.ClusterID(intToString(i%10)), []needs.Need{
				{ClusterID: machine.ClusterID(intToString(i % 10)), Profile: pf},
			})
		}
		close(done)
	}()
	for i := 0; i < 1000; i++ {
		_ = tbl.Snapshot()
		_ = tbl.Stats()
	}
	<-done
}

// Sanity: the Table's iteration order is deterministic across runs given
// the same Replace history. Sort and compare.
func TestTable_Snapshot_DeterministicWithinPriorityAndArrival(t *testing.T) {
	t.Parallel()
	tbl := needs.NewTable()
	pf := needs.NewProfile(nil, nil, 100, needs.PenaltyBucket1, needs.PenaltyBucket1)
	tbl.Replace("cluster-c", []needs.Need{{ClusterID: "cluster-c", Profile: pf, ArrivalUnixNanos: 0}})
	tbl.Replace("cluster-a", []needs.Need{{ClusterID: "cluster-a", Profile: pf, ArrivalUnixNanos: 0}})
	tbl.Replace("cluster-b", []needs.Need{{ClusterID: "cluster-b", Profile: pf, ArrivalUnixNanos: 0}})

	snap := tbl.Snapshot()
	got := make([]string, len(snap))
	for i, n := range snap {
		got[i] = string(n.ClusterID)
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("expected cluster IDs to be sorted within tied priority/arrival, got %v", got)
	}
}

func intToString(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
