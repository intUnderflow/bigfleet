package v1alpha1_test

import (
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

// JSON / YAML round-trip tests for the three CRD Go types. A full envtest
// dance (install CRDs into a real apiserver, create-then-get) is deferred
// to M4 when the operator's informers actually need it; for M1 we verify
// that the kubebuilder field tags accurately describe the Go types and
// that DeepCopy is wired up.

func TestRoundTrip_CapacityRequest_JSON(t *testing.T) {
	t.Parallel()
	want := newCapacityRequest()
	roundTripJSON(t, want, &v1alpha1.CapacityRequest{})
}

func TestRoundTrip_CapacityRequest_YAML(t *testing.T) {
	t.Parallel()
	want := newCapacityRequest()
	roundTripYAML(t, want, &v1alpha1.CapacityRequest{})
}

func TestRoundTrip_UpcomingNode_JSON(t *testing.T) {
	t.Parallel()
	want := newUpcomingNode()
	roundTripJSON(t, want, &v1alpha1.UpcomingNode{})
}

func TestRoundTrip_AvailableCapacity_JSON(t *testing.T) {
	t.Parallel()
	want := newAvailableCapacity()
	roundTripJSON(t, want, &v1alpha1.AvailableCapacity{})
}

func TestDeepCopy_CapacityRequest(t *testing.T) {
	t.Parallel()
	original := newCapacityRequest()
	copy, ok := original.DeepCopyObject().(*v1alpha1.CapacityRequest)
	if !ok {
		t.Fatalf("DeepCopyObject returned wrong type")
	}
	// Mutate the copy; the original must be unaffected.
	copy.Spec.Priority = 0
	copy.Spec.Resources["cpu"] = resource.MustParse("0")
	if original.Spec.Priority == 0 {
		t.Fatalf("DeepCopy did not isolate Spec.Priority")
	}
	cpu := original.Spec.Resources["cpu"]
	if cpu.String() == "0" {
		t.Fatalf("DeepCopy did not isolate Spec.Resources map")
	}
}

func TestSchemeRegistration(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	want := []runtime.Object{
		&v1alpha1.CapacityRequest{},
		&v1alpha1.CapacityRequestList{},
		&v1alpha1.UpcomingNode{},
		&v1alpha1.UpcomingNodeList{},
		&v1alpha1.AvailableCapacity{},
		&v1alpha1.AvailableCapacityList{},
	}
	for _, obj := range want {
		if !scheme.Recognizes(v1alpha1.GroupVersion.WithKind(typeName(obj))) {
			t.Errorf("scheme did not recognise %T", obj)
		}
	}
}

func typeName(obj runtime.Object) string {
	switch obj.(type) {
	case *v1alpha1.CapacityRequest:
		return "CapacityRequest"
	case *v1alpha1.CapacityRequestList:
		return "CapacityRequestList"
	case *v1alpha1.UpcomingNode:
		return "UpcomingNode"
	case *v1alpha1.UpcomingNodeList:
		return "UpcomingNodeList"
	case *v1alpha1.AvailableCapacity:
		return "AvailableCapacity"
	case *v1alpha1.AvailableCapacityList:
		return "AvailableCapacityList"
	}
	return ""
}

func newCapacityRequest() *v1alpha1.CapacityRequest {
	pen := resource.MustParse("8000")
	rec := resource.MustParse("0")
	return &v1alpha1.CapacityRequest{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "CapacityRequest",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cr-trainer-worker-42",
			Namespace: "training",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       "trainer-worker-42",
				UID:        types.UID("e8d3a7e1-0b3c-4f15-9c44-9d6f0d2b1c00"),
			}},
		},
		Spec: v1alpha1.CapacityRequestSpec{
			Requirements: []corev1.NodeSelectorRequirement{
				{
					Key:      "node.kubernetes.io/instance-type",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"a3-highgpu-8g", "p5.48xlarge"},
				},
			},
			Resources: corev1.ResourceList{
				"cpu":            resource.MustParse("96"),
				"memory":         resource.MustParse("768Gi"),
				"nvidia.com/gpu": resource.MustParse("8"),
			},
			Priority: 1_000_000,
			TopologySpread: []v1alpha1.TopologySpreadConstraint{
				{
					TopologyKey:       "topology.kubernetes.io/zone",
					MaxSkew:           1,
					WhenUnsatisfiable: corev1.DoNotSchedule,
				},
			},
			CoLocation: &v1alpha1.CoLocationTerm{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"job": "trainer-worker-42"},
				},
				TopologyKey: "topology.bigfleet/rack",
			},
			InterruptionPenalty: &pen,
			ReclamationPenalty:  &rec,
		},
		Status: v1alpha1.CapacityRequestStatus{
			Phase: v1alpha1.CapacityRequestAcknowledged,
			AcknowledgedAt: &metav1.Time{
				Time: time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			},
		},
	}
}

func newUpcomingNode() *v1alpha1.UpcomingNode {
	return &v1alpha1.UpcomingNode{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "UpcomingNode",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "un-research-049",
		},
		Spec: v1alpha1.UpcomingNodeSpec{
			Labels: map[string]string{
				"accelerator-type": "nvidia-h100-80gb",
			},
			Resources: corev1.ResourceList{
				"cpu":            resource.MustParse("208"),
				"memory":         resource.MustParse("1872Gi"),
				"nvidia.com/gpu": resource.MustParse("8"),
			},
			Taints: []corev1.Taint{{
				Key:    "nvidia.com/gpu",
				Effect: corev1.TaintEffectNoSchedule,
			}},
		},
		Status: v1alpha1.UpcomingNodeStatus{
			Phase:      v1alpha1.UpcomingNodeRegistered,
			NodeRef:    &corev1.ObjectReference{Name: "node-gpu-east-0291"},
			ProviderID: "aws:///us-east-1a/i-0abc123def456",
			ProvisioningStartTime: &metav1.Time{
				Time: time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			},
		},
	}
}

func newAvailableCapacity() *v1alpha1.AvailableCapacity {
	return &v1alpha1.AvailableCapacity{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "AvailableCapacity",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "gpu-h100-us-east",
		},
		Spec: v1alpha1.AvailableCapacitySpec{
			Requirements: []corev1.NodeSelectorRequirement{
				{
					Key:      "node.kubernetes.io/instance-type",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"a3-highgpu-8g"},
				},
			},
			Resources: corev1.ResourceList{
				"cpu":            resource.MustParse("208"),
				"memory":         resource.MustParse("1872Gi"),
				"nvidia.com/gpu": resource.MustParse("8"),
			},
			AvailableCount: 200,
			Availability:   v1alpha1.ConfidenceHigh,
			Cost:           resource.MustParse("31220m"),
		},
	}
}

func roundTripJSON(t *testing.T, in, out runtime.Object) {
	t.Helper()
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	again, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("json.Marshal (round 2): %v", err)
	}
	if string(data) != string(again) {
		t.Fatalf("json round-trip not byte-identical:\n  first:  %s\n  second: %s", data, again)
	}
}

func roundTripYAML(t *testing.T, in, out runtime.Object) {
	t.Helper()
	data, err := yaml.Marshal(in)
	if err != nil {
		t.Fatalf("yaml.Marshal: %v", err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	again, err := yaml.Marshal(out)
	if err != nil {
		t.Fatalf("yaml.Marshal (round 2): %v", err)
	}
	if string(data) != string(again) {
		t.Fatalf("yaml round-trip not byte-identical:\n  first:\n%s\n  second:\n%s", data, again)
	}
}
