package cr_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/controller/cr"
)

func newFakeClient(t *testing.T, objs ...client.Object) (client.Client, *runtime.Scheme) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := bfv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("bfv1alpha1.AddToScheme: %v", err)
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&bfv1alpha1.CapacityRequest{}).
		WithObjects(objs...).
		Build()
	return c, scheme
}

func unschedulablePod(name string, withAffinity bool, gpu int64) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
			UID:       types.UID("uid-" + name),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "busybox",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						"cpu":            resource.MustParse("1"),
						"nvidia.com/gpu": *resource.NewQuantity(gpu, resource.DecimalSI),
					},
				},
			}},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodScheduled,
				Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable,
			}},
		},
	}
	if withAffinity {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      "node.kubernetes.io/instance-type",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{"a3-highgpu-8g"},
						}},
					}},
				},
			},
		}
	}
	return pod
}

func reconcile(t *testing.T, r *cr.Reconciler, pod *corev1.Pod) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func TestReconciler_CreatesCRForUnschedulablePod(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("trainer-0", true, 8)
	c, scheme := newFakeClient(t, pod)
	r := &cr.Reconciler{Client: c, Scheme: scheme}

	reconcile(t, r, pod)

	var list bfv1alpha1.CapacityRequestList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("CRs created = %d, want 1", len(list.Items))
	}
	got := list.Items[0]
	if got.OwnerReferences[0].UID != pod.UID {
		t.Errorf("ownerRef UID mismatch")
	}
	gpuQ := got.Spec.Resources["nvidia.com/gpu"]
	if want := resource.NewQuantity(8, resource.DecimalSI); gpuQ.Cmp(*want) != 0 {
		t.Errorf("GPU resource not propagated: got %v, want %v", gpuQ.String(), want.String())
	}
	if len(got.Spec.Requirements) != 1 || got.Spec.Requirements[0].Operator != corev1.NodeSelectorOpIn {
		t.Errorf("requirements not propagated: %+v", got.Spec.Requirements)
	}
}

func TestReconciler_Idempotent(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("trainer-0", true, 8)
	c, scheme := newFakeClient(t, pod)
	r := &cr.Reconciler{Client: c, Scheme: scheme}

	reconcile(t, r, pod)
	reconcile(t, r, pod)
	reconcile(t, r, pod)

	var list bfv1alpha1.CapacityRequestList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("CRs after 3 reconciles = %d, want 1", len(list.Items))
	}
}

func TestReconciler_SkipsScheduledPods(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("scheduled-0", true, 8)
	pod.Status.Conditions[0].Status = corev1.ConditionTrue
	pod.Status.Conditions[0].Reason = ""
	c, scheme := newFakeClient(t, pod)
	r := &cr.Reconciler{Client: c, Scheme: scheme}

	reconcile(t, r, pod)

	var list bfv1alpha1.CapacityRequestList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 0 {
		t.Errorf("CRs created for scheduled pod = %d, want 0", len(list.Items))
	}
}

func TestReconciler_SyntheticRequirementWhenNoAffinity(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("trainer-0", false, 8)
	c, scheme := newFakeClient(t, pod)
	r := &cr.Reconciler{Client: c, Scheme: scheme}

	reconcile(t, r, pod)

	var list bfv1alpha1.CapacityRequestList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("CRs created = %d, want 1", len(list.Items))
	}
	reqs := list.Items[0].Spec.Requirements
	if len(reqs) != 1 || reqs[0].Operator != corev1.NodeSelectorOpExists ||
		reqs[0].Key != "node.kubernetes.io/instance-type" {
		t.Errorf("synthetic requirement mismatch: %+v", reqs)
	}
}

func TestReconciler_PenaltyAnnotations(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("trainer-0", true, 8)
	pod.Annotations = map[string]string{
		cr.AnnotationInterruptionPenalty: "8000",
		cr.AnnotationReclamationPenalty:  "5000",
	}
	c, scheme := newFakeClient(t, pod)
	r := &cr.Reconciler{Client: c, Scheme: scheme}

	reconcile(t, r, pod)

	var list bfv1alpha1.CapacityRequestList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("CRs created = %d, want 1", len(list.Items))
	}
	got := list.Items[0]
	if got.Spec.InterruptionPenalty == nil || got.Spec.InterruptionPenalty.String() != "8k" {
		t.Errorf("interruption penalty: got %v, want 8000", got.Spec.InterruptionPenalty)
	}
	if got.Spec.ReclamationPenalty == nil {
		t.Errorf("reclamation penalty not propagated")
	}
}
