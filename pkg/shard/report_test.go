package shard

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// TestPriorityClass pins the bounded priority→class mapping the
// per-priority shortfall metric uses. The realistic scale catalog's
// tiers (100 / 1000 / 1000000) must land on batch / service / critical;
// anything outside the catalog buckets to "other" so cardinality stays
// fixed.
func TestPriorityClass(t *testing.T) {
	t.Parallel()
	cases := []struct {
		priority int32
		want     string
	}{
		{100, priorityClassBatch},      // realistic catalog floor
		{500, priorityClassBatch},      // between batch and service
		{1000, priorityClassService},   // realistic catalog default
		{999999, priorityClassService}, // just below critical
		{1000000, priorityClassCritical},
		{5000000, priorityClassCritical},
		{0, priorityClassOther}, // out of catalog
		{-5, priorityClassOther},
		{99, priorityClassOther}, // below the batch floor
	}
	for _, c := range cases {
		if got := priorityClass(c.priority); got != c.want {
			t.Errorf("priorityClass(%d) = %q, want %q", c.priority, got, c.want)
		}
	}
}

// TestEmitShortfallsByPriorityClass_SetAndReset verifies the per-class
// gauge is populated from each Shortfall's Profile.Priority(), that
// same-class shortfalls sum, and — the load-bearing reset behaviour —
// that a class which clears in a later cycle reads 0 rather than holding
// a stale value (same discipline as the aged-bucket gauge).
func TestEmitShortfallsByPriorityClass_SetAndReset(t *testing.T) {
	// Not parallel: mutates a process-global GaugeVec.
	profile := func(priority int32) needs.Profile {
		return needs.NewProfile([]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"m6i.large"}},
		}, nil, priority, needs.PenaltyBucket1, needs.PenaltyBucket1)
	}
	classGauge := func(class string) float64 {
		return testutil.ToFloat64(metrics.ShardShortfallsByPriority.WithLabelValues(class))
	}

	// Cycle 1: two batch, one service, one critical shortfall.
	emitShortfallsByPriorityClass([]Shortfall{
		{Profile: profile(100)},
		{Profile: profile(100)},
		{Profile: profile(1000)},
		{Profile: profile(1000000)},
	})
	if got := classGauge(priorityClassBatch); got != 2 {
		t.Errorf("cycle 1 batch = %v, want 2 (summed)", got)
	}
	if got := classGauge(priorityClassService); got != 1 {
		t.Errorf("cycle 1 service = %v, want 1", got)
	}
	if got := classGauge(priorityClassCritical); got != 1 {
		t.Errorf("cycle 1 critical = %v, want 1", got)
	}
	if got := classGauge(priorityClassOther); got != 0 {
		t.Errorf("cycle 1 other = %v, want 0", got)
	}

	// Cycle 2: the confined steady state — only batch shortfalls remain.
	// service/critical must reset to 0, NOT keep their stale cycle-1 value.
	emitShortfallsByPriorityClass([]Shortfall{
		{Profile: profile(100)},
	})
	if got := classGauge(priorityClassBatch); got != 1 {
		t.Errorf("cycle 2 batch = %v, want 1", got)
	}
	if got := classGauge(priorityClassService); got != 0 {
		t.Errorf("cycle 2 service = %v, want 0 (stale value not cleared)", got)
	}
	if got := classGauge(priorityClassCritical); got != 0 {
		t.Errorf("cycle 2 critical = %v, want 0 (stale value not cleared)", got)
	}

	// Cycle 3: fully resolved — every class reads 0.
	emitShortfallsByPriorityClass(nil)
	for _, class := range shortfallPriorityClasses {
		if got := classGauge(class); got != 0 {
			t.Errorf("cycle 3 %s = %v, want 0 (all resolved)", class, got)
		}
	}
}

// TestRecordShortfalls_SameFingerprintSumsAndAgesOnce pins the M68
// ledger fix (philosophy-conformance audit, satisfaction-arithmetic
// lens): distinct unresolved Needs sharing a Profile fingerprint —
// e.g. several parked gangs of one workload shape, differing only by
// Group — must contribute their summed deficit per cycle, and the
// fingerprint must age exactly once per cycle. Pre-fix the ledger kept
// the last seed's deficit and aged once per seed.
func TestRecordShortfalls_SameFingerprintSumsAndAgesOnce(t *testing.T) {
	t.Parallel()
	s := &Shard{shortfalls: make(map[string]*shortfallEntry)}

	gangProfile := needs.NewProfile([]needs.Requirement{
		{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"a3-highgpu-8g"}},
		{Key: "topology.kubernetes.io/zone", Operator: needs.OperatorSame},
	}, nil, 2000, needs.PenaltyBucket1024, needs.PenaltyBucket1)
	plainProfile := needs.NewProfile([]needs.Requirement{
		{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"m6i.large"}},
	}, nil, 1000, needs.PenaltyBucket1, needs.PenaltyBucket1)

	gpus := func(n string) []needs.ResourceQty {
		return []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: n}}
	}
	// Two gangs share gangProfile's fingerprint; one plain seed is
	// distinct.
	seeds := []shortfallSeed{
		{Profile: gangProfile, Deficit: gpus("8")},
		{Profile: gangProfile, Deficit: gpus("16")},
		{Profile: plainProfile, Deficit: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}}},
	}

	s.recordShortfalls(seeds)
	if got := s.ShortfallCount(); got != 2 {
		t.Fatalf("entries = %d, want 2 (one per fingerprint)", got)
	}
	gang := s.shortfalls[gangProfile.Fingerprint()]
	if gang == nil {
		t.Fatal("gang fingerprint missing from ledger")
	}
	if !needs.Covers(gang.Deficit, gpus("24")) || !needs.Covers(gpus("24"), gang.Deficit) {
		t.Errorf("cycle 1 deficit = %v, want nvidia.com/gpu=24 (8+16 summed, not last-writer-wins)", gang.Deficit)
	}
	if gang.AgeCycles != 1 {
		t.Errorf("cycle 1 age = %d, want 1 (once per cycle, not once per seed)", gang.AgeCycles)
	}

	// Second cycle, same seeds: age advances by exactly one.
	s.recordShortfalls(seeds)
	gang = s.shortfalls[gangProfile.Fingerprint()]
	if gang.AgeCycles != 2 {
		t.Errorf("cycle 2 age = %d, want 2", gang.AgeCycles)
	}

	// Third cycle: one gang resolves; the survivor's deficit replaces
	// the sum, age keeps advancing.
	s.recordShortfalls(seeds[1:])
	gang = s.shortfalls[gangProfile.Fingerprint()]
	if !needs.Covers(gpus("16"), gang.Deficit) || !needs.Covers(gang.Deficit, gpus("16")) {
		t.Errorf("cycle 3 deficit = %v, want nvidia.com/gpu=16", gang.Deficit)
	}
	if gang.AgeCycles != 3 {
		t.Errorf("cycle 3 age = %d, want 3", gang.AgeCycles)
	}

	// Both resolve: entries drop.
	s.recordShortfalls(nil)
	if got := s.ShortfallCount(); got != 0 {
		t.Errorf("entries after resolution = %d, want 0", got)
	}
}
