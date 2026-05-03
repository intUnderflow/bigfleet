package operator

import (
	"io"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// Tests that buildRollup honours owner-grouping (paper §8 co-location):
// CRs sharing an OwnerReference UID become a single CapacityNeed with
// a Same(CoLocationKey) requirement; CRs from different owners stay in
// separate Needs even when their profiles are otherwise identical.

func makeOperatorT(t *testing.T) *Operator {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	op, err := New(Config{
		ClusterID:    "test-cluster",
		ShardAddress: "127.0.0.1:7780",
		KubeClient:   c,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("operator.New: %v", err)
	}
	return op
}

func gpuCR(name string, ownerUID types.UID) bfv1alpha1.CapacityRequest {
	intr := resource.MustParse("8192")
	cr := bfv1alpha1.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: bfv1alpha1.CapacityRequestSpec{
			Requirements: []corev1.NodeSelectorRequirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"a3-highgpu-8g"},
			}},
			Resources: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("8"),
			},
			Priority:            1_000_000,
			InterruptionPenalty: &intr,
		},
	}
	if ownerUID != "" {
		cr.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
			Name:       "trainer",
			UID:        ownerUID,
		}}
	}
	return cr
}

// hasSameRequirement returns the value of a Same requirement on key, if
// any (Values list).
func sameRequirementOnKey(reqs []*pb.NodeSelectorRequirement, key string) (*pb.NodeSelectorRequirement, bool) {
	for _, r := range reqs {
		if r.GetKey() == key && r.GetOperator() == pb.NodeSelectorRequirement_OPERATOR_SAME {
			return r, true
		}
	}
	return nil, false
}

func TestBuildRollup_AppendsSameWhenOwnerRefPresent(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("cr-1", "owner-A"),
		gpuCR("cr-2", "owner-A"),
		gpuCR("cr-3", "owner-A"),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 1 {
		t.Fatalf("needs len = %d, want 1 (single owner)", got)
	}
	n := rollup.GetNeeds()[0]
	if n.GetCount() != 3 {
		t.Errorf("count = %d, want 3", n.GetCount())
	}
	if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.kubernetes.io/zone"); !ok {
		t.Errorf("Same requirement on zone missing; got requirements: %+v", n.GetRequirements())
	}
}

func TestBuildRollup_DifferentOwnersStaySeparate(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("cr-a-1", "owner-A"),
		gpuCR("cr-a-2", "owner-A"),
		gpuCR("cr-b-1", "owner-B"),
		gpuCR("cr-b-2", "owner-B"),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 2 {
		t.Fatalf("needs len = %d, want 2 (two owners)", got)
	}
	for _, n := range rollup.GetNeeds() {
		if n.GetCount() != 2 {
			t.Errorf("count = %d, want 2 per owner", n.GetCount())
		}
		if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.kubernetes.io/zone"); !ok {
			t.Errorf("Same requirement on zone missing on a per-owner Need")
		}
	}
}

func TestBuildRollup_OwnerlessCRsAggregateNormally(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("cr-1", ""),
		gpuCR("cr-2", ""),
		gpuCR("cr-3", ""),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 1 {
		t.Fatalf("needs len = %d, want 1 (no owners → single aggregated need)", got)
	}
	n := rollup.GetNeeds()[0]
	if n.GetCount() != 3 {
		t.Errorf("count = %d, want 3", n.GetCount())
	}
	if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.kubernetes.io/zone"); ok {
		t.Errorf("Same requirement should NOT be present on ownerless CRs")
	}
}

func TestBuildRollup_CoLocationKeyConfigurable(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	op, err := New(Config{
		ClusterID:     "test-cluster",
		ShardAddress:  "127.0.0.1:7780",
		KubeClient:    c,
		CoLocationKey: "topology.example.com/rack",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("operator.New: %v", err)
	}

	rollup, _ := op.buildRollup([]bfv1alpha1.CapacityRequest{gpuCR("cr-1", "owner-X")})
	n := rollup.GetNeeds()[0]
	if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.example.com/rack"); !ok {
		t.Errorf("Same requirement should be on configured CoLocationKey; got: %+v", n.GetRequirements())
	}
}
