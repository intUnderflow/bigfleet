package shard

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/needs"
)

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
