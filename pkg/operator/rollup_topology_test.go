package operator

import (
	"io"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// Tests that buildRollup honours co-location (paper §8, ADR-0024): CRs
// carrying an equal Spec.CoLocation term become a single CapacityNeed
// with a Same requirement on the term's TopologyKey; CRs with
// different terms — independent workloads — stay separate even when
// their profiles are otherwise identical; CRs with no CoLocation
// aggregate purely by profile fingerprint and carry no Same.

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

// coLocTerm builds a CoLocationTerm of the shape the UPC projects from
// a pod's required podAffinity (and the load-driver sets directly in
// Mode=cr): a label selector on a group-unique label, plus the
// topology key the pods share.
func coLocTerm(group, topologyKey string) *bfv1alpha1.CoLocationTerm {
	return &bfv1alpha1.CoLocationTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"scaletest.bigfleet/co-location-group": group},
		},
		TopologyKey: topologyKey,
	}
}

func gpuCR(name string, coloc *bfv1alpha1.CoLocationTerm) bfv1alpha1.CapacityRequest {
	intr := resource.MustParse("8192")
	return bfv1alpha1.CapacityRequest{
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
			CoLocation:          coloc,
			InterruptionPenalty: &intr,
		},
	}
}

// sameRequirementOnKey returns the Same requirement on key, if any.
func sameRequirementOnKey(reqs []*pb.NodeSelectorRequirement, key string) (*pb.NodeSelectorRequirement, bool) {
	for _, r := range reqs {
		if r.GetKey() == key && r.GetOperator() == pb.NodeSelectorRequirement_OPERATOR_SAME {
			return r, true
		}
	}
	return nil, false
}

func TestBuildRollup_AppendsSameWhenCoLocationPresent(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	term := coLocTerm("group-A", "topology.bigfleet/rack")
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("cr-1", term),
		gpuCR("cr-2", term),
		gpuCR("cr-3", term),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 1 {
		t.Fatalf("needs len = %d, want 1 (single co-location group)", got)
	}
	n := rollup.GetNeeds()[0]
	if n.GetCount() != 3 {
		t.Errorf("count = %d, want 3", n.GetCount())
	}
	if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.bigfleet/rack"); !ok {
		t.Errorf("Same requirement on the term's topology key missing; got: %+v", n.GetRequirements())
	}
}

func TestBuildRollup_DifferentCoLocationStaySeparate(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	// Two independent workloads, identical profile, different group
	// selectors — each must keep its own Need and its own Same domain.
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("cr-a-1", coLocTerm("group-A", "topology.bigfleet/rack")),
		gpuCR("cr-a-2", coLocTerm("group-A", "topology.bigfleet/rack")),
		gpuCR("cr-b-1", coLocTerm("group-B", "topology.bigfleet/rack")),
		gpuCR("cr-b-2", coLocTerm("group-B", "topology.bigfleet/rack")),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 2 {
		t.Fatalf("needs len = %d, want 2 (two independent workloads)", got)
	}
	for _, n := range rollup.GetNeeds() {
		if n.GetCount() != 2 {
			t.Errorf("count = %d, want 2 per workload", n.GetCount())
		}
		if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.bigfleet/rack"); !ok {
			t.Errorf("Same requirement missing on a per-workload Need")
		}
	}
}

func TestBuildRollup_NoCoLocationAggregateNormally(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("cr-1", nil),
		gpuCR("cr-2", nil),
		gpuCR("cr-3", nil),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 1 {
		t.Fatalf("needs len = %d, want 1 (no co-location → single aggregated need)", got)
	}
	n := rollup.GetNeeds()[0]
	if n.GetCount() != 3 {
		t.Errorf("count = %d, want 3", n.GetCount())
	}
	for _, r := range n.GetRequirements() {
		if r.GetOperator() == pb.NodeSelectorRequirement_OPERATOR_SAME {
			t.Errorf("no Same requirement should be present on CRs without CoLocation; got: %v", r)
		}
	}
}

// TestBuildRollup_CoLocatedAndPlainStaySeparate: a co-located CR and a
// plain CR of the *same* profile shape must not merge — one carries a
// Same requirement, the other must not.
func TestBuildRollup_CoLocatedAndPlainStaySeparate(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("plain-1", nil),
		gpuCR("plain-2", nil),
		gpuCR("coloc-1", coLocTerm("group-A", "topology.bigfleet/rack")),
		gpuCR("coloc-2", coLocTerm("group-A", "topology.bigfleet/rack")),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 2 {
		t.Fatalf("needs len = %d, want 2 (plain vs co-located)", got)
	}
	var sawSame, sawPlain bool
	for _, n := range rollup.GetNeeds() {
		if n.GetCount() != 2 {
			t.Errorf("count = %d, want 2", n.GetCount())
		}
		if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.bigfleet/rack"); ok {
			sawSame = true
		} else {
			sawPlain = true
		}
	}
	if !sawSame || !sawPlain {
		t.Errorf("expected one Need with Same and one without; sawSame=%v sawPlain=%v", sawSame, sawPlain)
	}
}

// TestBuildRollup_SameKeyFromTerm: the Same requirement's topology key
// is the term's own TopologyKey — there is no operator-wide key
// (ADR-0024 retired Config.CoLocationKey). Two CRs that share a
// selector but declare different topology keys are different
// workloads and must not merge.
func TestBuildRollup_SameKeyFromTerm(t *testing.T) {
	t.Parallel()
	op := makeOperatorT(t)
	crs := []bfv1alpha1.CapacityRequest{
		gpuCR("rack-1", coLocTerm("group-A", "topology.bigfleet/rack")),
		gpuCR("zone-1", coLocTerm("group-A", "topology.kubernetes.io/zone")),
	}
	rollup, _ := op.buildRollup(crs)
	if got := len(rollup.GetNeeds()); got != 2 {
		t.Fatalf("needs len = %d, want 2 (different topology keys → different workloads)", got)
	}
	var sawRack, sawZone bool
	for _, n := range rollup.GetNeeds() {
		if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.bigfleet/rack"); ok {
			sawRack = true
		}
		if _, ok := sameRequirementOnKey(n.GetRequirements(), "topology.kubernetes.io/zone"); ok {
			sawZone = true
		}
	}
	if !sawRack || !sawZone {
		t.Errorf("each Need's Same key should come from its own term; sawRack=%v sawZone=%v", sawRack, sawZone)
	}
}
