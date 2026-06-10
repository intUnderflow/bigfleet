package operator

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

// BenchmarkBuildRollup_CoLocated25K is the post-ADR-0039 per-cluster
// rollup shape: one CR per Pod (25K CRs — the whole Pod population,
// not just the unmet fraction), ~11 % of them in co-location groups of
// ~4 whose group-unique selectors each aggregate into their own
// Same-carrying Need (~700 group Needs + a handful of plain
// fingerprints). BenchmarkBuildRollup's homogeneous 10K shape predates
// both the CR-count growth and the group-cardinality fold; the cloud
// runs after ADR-0039 measured rollup p99 roughly doubling, so this is
// the shape that has to stay cheap.
//
//	go test -bench=BuildRollup_CoLocated -benchtime=5x ./pkg/operator/
func BenchmarkBuildRollup_CoLocated25K(b *testing.B) {
	crs := makeCoLocatedCapacityRequests(25000)
	op := makeOperator(b, &bfv1alpha1.CapacityRequestList{Items: crs})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = op.buildRollup(crs)
	}
}

// makeCoLocatedCapacityRequests mirrors what the unschedulable-pod
// controller produces for the realistic catalog: a tiny-stateless
// majority, an instance-typed service tier, and ~11 % co-located pods
// whose CRs carry a CoLocationTerm with a group-unique selector
// (groups of 4) — each group folds into its own Need at aggregation.
func makeCoLocatedCapacityRequests(n int) []bfv1alpha1.CapacityRequest {
	intr := resource.MustParse("1024")
	recl := resource.MustParse("8192")
	out := make([]bfv1alpha1.CapacityRequest, n)
	for i := 0; i < n; i++ {
		cr := bfv1alpha1.CapacityRequest{
			ObjectMeta: metav1.ObjectMeta{Name: crName(i), Namespace: "default"},
			Spec: bfv1alpha1.CapacityRequestSpec{
				Priority:            100,
				InterruptionPenalty: &intr,
				ReclamationPenalty:  &recl,
			},
		}
		switch {
		case i%9 == 0: // ~11 %: co-located gang member (groups of 4)
			gid := fmt.Sprintf("grp-%d", i/(9*4))
			cr.Spec.Requirements = []corev1.NodeSelectorRequirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"r6i.2xlarge"},
			}}
			cr.Spec.Resources = corev1.ResourceList{
				"cpu":    resource.MustParse("4"),
				"memory": resource.MustParse("32Gi"),
			}
			cr.Spec.Priority = 1000
			cr.Spec.CoLocation = &bfv1alpha1.CoLocationTerm{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"scaletest.bigfleet/co-location-group": gid},
				},
				TopologyKey: "topology.bigfleet/rack",
			}
		case i%9 == 1: // ~11 %: instance-typed service tier
			cr.Spec.Requirements = []corev1.NodeSelectorRequirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"m6i.xlarge"},
			}}
			cr.Spec.Resources = corev1.ResourceList{
				"cpu":    resource.MustParse("2200m"),
				"memory": resource.MustParse("8500Mi"),
			}
			cr.Spec.Priority = 1000
		default: // the tiny-stateless majority
			cr.Spec.Requirements = []corev1.NodeSelectorRequirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"m6i.large", "m6i.xlarge"},
			}}
			cr.Spec.Resources = corev1.ResourceList{
				"cpu":    resource.MustParse("400m"),
				"memory": resource.MustParse("500Mi"),
			}
		}
		out[i] = cr
	}
	return out
}
