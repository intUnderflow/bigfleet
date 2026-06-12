package operator

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// M68b item 4: penalties bucket through AsApproximateFloat64, not
// AsInt64 — AsInt64 reports ok=false for fractional quantities and the
// discarded error flattened '500m' to the $0 bucket, erasing exactly
// the sub-dollar distinction the $0.50 bucket boundary exists for.
func TestProfileFromCapacityRequest_FractionalPenaltyBuckets(t *testing.T) {
	t.Parallel()
	cases := []struct {
		quantity string
		want     needs.PenaltyBucket
	}{
		{"500m", needs.PenaltyBucketHalfDollar},
		{"250m", needs.PenaltyBucketHalfDollar},
		{"0.5", needs.PenaltyBucketHalfDollar},
		{"1500m", needs.PenaltyBucket2},
		{"0", needs.PenaltyBucketZero},
		{"8000", needs.PenaltyBucket8192},
	}
	for _, tc := range cases {
		t.Run(tc.quantity, func(t *testing.T) {
			t.Parallel()
			q := resource.MustParse(tc.quantity)
			cr := &bfv1alpha1.CapacityRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "cr-frac", Namespace: "default"},
				Spec: bfv1alpha1.CapacityRequestSpec{
					Resources:           corev1.ResourceList{"cpu": resource.MustParse("1")},
					InterruptionPenalty: &q,
					ReclamationPenalty:  &q,
				},
			}
			profile, _, err := profileFromCapacityRequest(cr)
			if err != nil {
				t.Fatalf("profileFromCapacityRequest: %v", err)
			}
			if got := profile.InterruptionPenaltyBucket(); got != tc.want {
				t.Errorf("InterruptionPenaltyBucket(%s) = %v, want %v", tc.quantity, got, tc.want)
			}
			if got := profile.ReclamationPenaltyBucket(); got != tc.want {
				t.Errorf("ReclamationPenaltyBucket(%s) = %v, want %v", tc.quantity, got, tc.want)
			}
		})
	}
}

// M68b item 10: ScheduleAnyway spread terms are consumed by nothing
// engine-side, so the roll-up edge drops them from the Profile — for
// CRs from any source, not just the UPC. DoNotSchedule terms survive.
func TestProfileFromCapacityRequest_DropsScheduleAnywaySpread(t *testing.T) {
	t.Parallel()
	cr := &bfv1alpha1.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "cr-spread", Namespace: "default"},
		Spec: bfv1alpha1.CapacityRequestSpec{
			Resources: corev1.ResourceList{"cpu": resource.MustParse("1")},
			TopologySpread: []bfv1alpha1.TopologySpreadConstraint{
				{
					TopologyKey:       "topology.kubernetes.io/zone",
					MaxSkew:           1,
					WhenUnsatisfiable: corev1.DoNotSchedule,
				},
				{
					TopologyKey:       "kubernetes.io/hostname",
					MaxSkew:           3,
					WhenUnsatisfiable: corev1.ScheduleAnyway,
				},
			},
		},
	}
	profile, _, err := profileFromCapacityRequest(cr)
	if err != nil {
		t.Fatalf("profileFromCapacityRequest: %v", err)
	}
	spread := profile.Spread()
	if len(spread) != 1 {
		t.Fatalf("Spread = %+v, want only the DoNotSchedule term", spread)
	}
	if spread[0].TopologyKey != "topology.kubernetes.io/zone" ||
		spread[0].WhenUnsatisfiable != needs.WhenUnsatisfiableDoNotSchedule {
		t.Errorf("kept term = %+v, want the DoNotSchedule zone term", spread[0])
	}
}
