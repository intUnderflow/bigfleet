package decision_test

import (
	"math"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

func TestEffectiveCost_NoInterruptionRisk(t *testing.T) {
	t.Parallel()
	m := machine.Machine{PricePerHour: 6.0, InterruptionProbability: 0.0}
	got := decision.EffectiveCost(m, 100.0)
	if got != 6.0 {
		t.Errorf("zero-interruption: got %f, want 6.0", got)
	}
}

// Worked example from the paper: spot at $1.80/hr, 10% hourly interruption,
// $5 interruption penalty → effective $2.30. On-demand at $6.00 wins-ties
// against on-demand only when penalty pushes effective above $6.
func TestEffectiveCost_PaperWorkedExample_Spot(t *testing.T) {
	t.Parallel()
	spot := machine.Machine{PricePerHour: 1.80, InterruptionProbability: 0.10}
	got := decision.EffectiveCost(spot, 5.0)
	want := 2.30
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("paper example: got %f, want %f", got, want)
	}
}

func TestEffectiveCost_HighPenaltyFlipsDecision(t *testing.T) {
	t.Parallel()
	spot := machine.Machine{PricePerHour: 1.80, InterruptionProbability: 0.10}
	onDemand := machine.Machine{PricePerHour: 6.0, InterruptionProbability: 0.0}

	// Low-penalty workload: spot wins.
	if decision.EffectiveCost(spot, 5.0) >= decision.EffectiveCost(onDemand, 5.0) {
		t.Errorf("low penalty: spot effective should be cheaper")
	}
	// High-penalty workload: on-demand wins.
	if decision.EffectiveCost(spot, 50.0) <= decision.EffectiveCost(onDemand, 50.0) {
		t.Errorf("high penalty: on-demand effective should be cheaper")
	}
}

func TestBucketUpperBoundDollars(t *testing.T) {
	t.Parallel()
	cases := []struct {
		bucket needs.PenaltyBucket
		want   float64
	}{
		{needs.PenaltyBucketZero, 0},
		{needs.PenaltyBucketHalfDollar, 0.5},
		{needs.PenaltyBucket1, 1},
		{needs.PenaltyBucket2, 2},
		{needs.PenaltyBucket8192, 8192},
		{needs.PenaltyBucket1048576, 1_048_576},
		{needs.PenaltyBucket8388608, 8_388_608},
	}
	for _, tc := range cases {
		got := decision.BucketUpperBoundDollars(tc.bucket)
		if got != tc.want {
			t.Errorf("BucketUpperBoundDollars(%v) = %f, want %f", tc.bucket, got, tc.want)
		}
	}
	if !math.IsInf(decision.BucketUpperBoundDollars(needs.PenaltyBucketPinned), 1) {
		t.Errorf("PenaltyBucketPinned should be +Inf")
	}
}

func TestDrainGrace_LockedTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		preemptor, victim int32
		want              time.Duration
	}{
		{1_000_000, 0, 10 * time.Second},         // gap = 1M > 900K
		{1_000_000, 99_999, 10 * time.Second},    // gap = 900,001
		{1_000_000, 100_001, 30 * time.Second},   // gap = 899,999
		{1_000_000, 499_999, 30 * time.Second},   // gap > 500K
		{1_000_000, 500_001, 2 * time.Minute},    // gap < 500K but > 100K
		{1_000_000, 899_999, 2 * time.Minute},    // gap > 100K
		{1_000_000, 900_001, 10 * time.Minute},   // gap < 100K
		{1_000_000, 1_000_000, 10 * time.Minute}, // equal priority
		{0, 0, 10 * time.Minute},
	}
	for _, tc := range cases {
		got := decision.DrainGrace(tc.preemptor, tc.victim)
		if got != tc.want {
			t.Errorf("DrainGrace(%d, %d) = %v, want %v", tc.preemptor, tc.victim, got, tc.want)
		}
	}
}

func TestVictimScore_PreferLowerPriorityVictim(t *testing.T) {
	t.Parallel()
	w := decision.DefaultVictimWeights()
	preemptor := int32(1_000_000)
	low := decision.VictimCandidate{
		Machine:                machine.Machine{},
		AssignedPriority:       0,
		InterruptionPenalty:    1.0,
		ReclamationPenalty:     1.0,
		EstimatedDrainDuration: 30 * time.Second,
	}
	high := decision.VictimCandidate{
		Machine:                machine.Machine{},
		AssignedPriority:       900_000,
		InterruptionPenalty:    1.0,
		ReclamationPenalty:     1.0,
		EstimatedDrainDuration: 30 * time.Second,
	}
	if decision.VictimScore(preemptor, low, w) <= decision.VictimScore(preemptor, high, w) {
		t.Errorf("low-priority victim should score higher than high-priority")
	}
}

func TestVictimScore_PreferFasterDrain(t *testing.T) {
	t.Parallel()
	w := decision.DefaultVictimWeights()
	preemptor := int32(1_000_000)
	fast := decision.VictimCandidate{
		AssignedPriority:       0,
		InterruptionPenalty:    1.0,
		ReclamationPenalty:     1.0,
		EstimatedDrainDuration: 5 * time.Second,
	}
	slow := decision.VictimCandidate{
		AssignedPriority:       0,
		InterruptionPenalty:    1.0,
		ReclamationPenalty:     1.0,
		EstimatedDrainDuration: 10 * time.Minute,
	}
	if decision.VictimScore(preemptor, fast, w) <= decision.VictimScore(preemptor, slow, w) {
		t.Errorf("fast-drain victim should score higher")
	}
}

func TestVictimScore_PreferLowPenalty(t *testing.T) {
	t.Parallel()
	w := decision.DefaultVictimWeights()
	preemptor := int32(1_000_000)
	cheap := decision.VictimCandidate{
		AssignedPriority:       0,
		InterruptionPenalty:    1.0,
		ReclamationPenalty:     1.0,
		EstimatedDrainDuration: 30 * time.Second,
	}
	expensive := decision.VictimCandidate{
		AssignedPriority:       0,
		InterruptionPenalty:    10_000,
		ReclamationPenalty:     10_000,
		EstimatedDrainDuration: 30 * time.Second,
	}
	if decision.VictimScore(preemptor, cheap, w) <= decision.VictimScore(preemptor, expensive, w) {
		t.Errorf("low-penalty victim should score higher than high-penalty")
	}
}

func TestVictimScore_FloorsAvoidDivByZero(t *testing.T) {
	t.Parallel()
	w := decision.DefaultVictimWeights()
	c := decision.VictimCandidate{
		AssignedPriority:       0,
		InterruptionPenalty:    0,
		ReclamationPenalty:     0,
		EstimatedDrainDuration: 0,
	}
	got := decision.VictimScore(1_000_000, c, w)
	if math.IsInf(got, 0) || math.IsNaN(got) {
		t.Errorf("score with zero denominators must be finite, got %f", got)
	}
}
