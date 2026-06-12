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

// TestReconciler_TranslatesPodAffinityToCoLocation locks in the
// ADR-0024 contract: a pod's required podAffinity becomes the CR's
// structured Spec.CoLocation, which the operator later turns into a
// Same requirement at roll-up.
func TestReconciler_TranslatesPodAffinityToCoLocation(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("trainer-0", true, 8)
	pod.Spec.Affinity.PodAffinity = &corev1.PodAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"job": "trainer-7"},
			},
			TopologyKey: "topology.bigfleet/rack",
		}},
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
	coloc := list.Items[0].Spec.CoLocation
	if coloc == nil {
		t.Fatalf("Spec.CoLocation not populated from podAffinity")
	}
	if coloc.TopologyKey != "topology.bigfleet/rack" {
		t.Errorf("TopologyKey = %q, want topology.bigfleet/rack", coloc.TopologyKey)
	}
	if coloc.LabelSelector == nil || coloc.LabelSelector.MatchLabels["job"] != "trainer-7" {
		t.Errorf("LabelSelector not propagated: %+v", coloc.LabelSelector)
	}
}

// TestReconciler_NoCoLocationWithoutPodAffinity: a pod with no required
// podAffinity produces a CR with nil Spec.CoLocation, so it aggregates
// freely by profile fingerprint at roll-up.
func TestReconciler_NoCoLocationWithoutPodAffinity(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("plain-0", true, 8)
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
	if list.Items[0].Spec.CoLocation != nil {
		t.Errorf("Spec.CoLocation should be nil for a pod without podAffinity, got %+v", list.Items[0].Spec.CoLocation)
	}
}

// TestReconciler_StampsInitialPendingPhase locks in the M19 contract
// that newly-created CRs land with status.phase=Pending so observers
// running `kubectl get capacityrequest` see the lifecycle walk
// instead of a blank phase column.
func TestReconciler_StampsInitialPendingPhase(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("trainer-pending", true, 8)
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
	if got, want := list.Items[0].Status.Phase, bfv1alpha1.CapacityRequestPending; got != want {
		t.Errorf("status.phase = %q, want %q", got, want)
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

// TestReconciler_CreatesCRForScheduledPod locks in ADR-0039: a CR is
// created for every Pod, including already-scheduled ones. The roll-up
// must carry the cluster's total desired capacity, not only its unmet
// demand — otherwise Phase 3 sees a phantom surplus and thrashes.
func TestReconciler_CreatesCRForScheduledPod(t *testing.T) {
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
	if len(list.Items) != 1 {
		t.Errorf("CRs created for scheduled pod = %d, want 1 (ADR-0039)", len(list.Items))
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

// TestReconciler_PriorityClassDefaults_Fallback: M16 contract — when
// a pod has no penalty annotation, the controller falls back to the
// PriorityClass-defaults map. Pod annotation still wins when set.
func TestReconciler_PriorityClassDefaults_Fallback(t *testing.T) {
	t.Parallel()
	defaults := map[string]cr.PriorityClassDefaults{
		"ml-research": {
			InterruptionPenalty: ptrQuantity("8192"),
			ReclamationPenalty:  ptrQuantity("65536"),
		},
		"batch-best-effort": {
			InterruptionPenalty: ptrQuantity("16"),
		},
	}

	cases := []struct {
		name              string
		priorityClassName string
		annotations       map[string]string
		wantInterruption  string // "" = nil
		wantReclamation   string
	}{
		{
			name:              "PriorityClass default applies when no annotation",
			priorityClassName: "ml-research",
			wantInterruption:  "8192",
			wantReclamation:   "65536",
		},
		{
			name:              "annotation overrides PriorityClass default",
			priorityClassName: "ml-research",
			annotations: map[string]string{
				cr.AnnotationInterruptionPenalty: "1",
			},
			wantInterruption: "1",
			wantReclamation:  "65536",
		},
		{
			name:              "PriorityClass default with only one penalty leaves the other nil",
			priorityClassName: "batch-best-effort",
			wantInterruption:  "16",
			wantReclamation:   "", // not in defaults map for this PC
		},
		{
			name:              "unknown PriorityClass falls all the way through to nil",
			priorityClassName: "no-such-class",
			wantInterruption:  "",
			wantReclamation:   "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := unschedulablePod("trainer-"+tc.name, true, 8)
			pod.Spec.PriorityClassName = tc.priorityClassName
			if tc.annotations != nil {
				pod.Annotations = tc.annotations
			}
			c, scheme := newFakeClient(t, pod)
			r := &cr.Reconciler{Client: c, Scheme: scheme, PriorityClassDefaults: defaults}

			reconcile(t, r, pod)

			var list bfv1alpha1.CapacityRequestList
			if err := c.List(context.Background(), &list); err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(list.Items) != 1 {
				t.Fatalf("CRs created = %d, want 1", len(list.Items))
			}
			got := list.Items[0]
			if tc.wantInterruption == "" {
				if got.Spec.InterruptionPenalty != nil {
					t.Errorf("InterruptionPenalty = %v, want nil", got.Spec.InterruptionPenalty)
				}
			} else if got.Spec.InterruptionPenalty == nil || got.Spec.InterruptionPenalty.String() != tc.wantInterruption {
				t.Errorf("InterruptionPenalty = %v, want %q", got.Spec.InterruptionPenalty, tc.wantInterruption)
			}
			if tc.wantReclamation == "" {
				if got.Spec.ReclamationPenalty != nil {
					t.Errorf("ReclamationPenalty = %v, want nil", got.Spec.ReclamationPenalty)
				}
			} else if got.Spec.ReclamationPenalty == nil || got.Spec.ReclamationPenalty.String() != tc.wantReclamation {
				t.Errorf("ReclamationPenalty = %v, want %q", got.Spec.ReclamationPenalty, tc.wantReclamation)
			}
		})
	}
}

func ptrQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

// TestReconciler_TerminalPodDeletesCR pins M68b / ADR-0045: ownerRef GC
// only fires when the Pod object is deleted, but Succeeded/Failed pods
// can linger in the apiserver indefinitely — so the controller must
// delete the CR itself on the terminal transition. With the CR gone the
// operator's next roll-up (which Lists CRs) shrinks by construction.
func TestReconciler_TerminalPodDeletesCR(t *testing.T) {
	t.Parallel()
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		phase := phase
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			pod := unschedulablePod("batch-0", true, 8)
			c, scheme := newFakeClient(t, pod)
			r := &cr.Reconciler{Client: c, Scheme: scheme}

			// Running pod → CR exists.
			reconcile(t, r, pod)
			var list bfv1alpha1.CapacityRequestList
			if err := c.List(context.Background(), &list); err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(list.Items) != 1 {
				t.Fatalf("CRs before terminal = %d, want 1", len(list.Items))
			}

			// Pod transitions terminal → reconcile deletes the CR.
			pod.Status.Phase = phase
			if err := c.Status().Update(context.Background(), pod); err != nil {
				t.Fatalf("update pod status: %v", err)
			}
			reconcile(t, r, pod)
			if err := c.List(context.Background(), &list); err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(list.Items) != 0 {
				t.Fatalf("CRs after terminal = %d, want 0 (next roll-up must shrink)", len(list.Items))
			}

			// Re-reconciling the terminal pod must not resurrect the CR.
			reconcile(t, r, pod)
			if err := c.List(context.Background(), &list); err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(list.Items) != 0 {
				t.Fatalf("CRs after re-reconcile = %d, want 0", len(list.Items))
			}
		})
	}
}

// TestReconciler_NoCRForAlreadyTerminalPod: a pod first seen at a
// terminal phase never synthesizes demand.
func TestReconciler_NoCRForAlreadyTerminalPod(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("done-0", true, 8)
	pod.Status.Phase = corev1.PodSucceeded
	c, scheme := newFakeClient(t, pod)
	r := &cr.Reconciler{Client: c, Scheme: scheme}

	reconcile(t, r, pod)

	var list bfv1alpha1.CapacityRequestList
	if err := c.List(context.Background(), &list); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("CRs created for already-terminal pod = %d, want 0", len(list.Items))
	}
}

// TestReconciler_EffectiveRequest_InitHeavy pins the kube-scheduler
// effective-request arithmetic (M68b): init containers run serially
// before the main containers, so an init-heavy pod is sized at the
// peak init stage, not the sum of everything.
func TestReconciler_EffectiveRequest_InitHeavy(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("init-heavy", false, 0)
	pod.Spec.Containers = []corev1.Container{{
		Name: "main",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}}
	pod.Spec.InitContainers = []corev1.Container{{
		Name: "loader",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{"cpu": resource.MustParse("4")},
		},
	}}
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
	res := list.Items[0].Spec.Resources
	// cpu: max(sum(containers)=1, init peak=4) = 4 — NOT the old 1+4=5.
	if got := res["cpu"]; got.Cmp(resource.MustParse("4")) != 0 {
		t.Errorf("cpu = %s, want 4 (max(containers, init peak))", got.String())
	}
	// memory: only the main container names it → 1Gi.
	if got := res["memory"]; got.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("memory = %s, want 1Gi", got.String())
	}
}

// TestReconciler_EffectiveRequest_Sidecar: a restartPolicy=Always init
// container (sidecar) keeps running alongside the main containers, so
// it joins the running sum AND every later init stage's footprint.
func TestReconciler_EffectiveRequest_Sidecar(t *testing.T) {
	t.Parallel()
	always := corev1.ContainerRestartPolicyAlways
	pod := unschedulablePod("sidecar-0", false, 0)
	pod.Spec.Containers = []corev1.Container{{
		Name: "main",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{"cpu": resource.MustParse("1")},
		},
	}}
	pod.Spec.InitContainers = []corev1.Container{
		{
			Name:          "log-shipper",
			RestartPolicy: &always,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{"cpu": resource.MustParse("2")},
			},
		},
		{
			Name: "migrator",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{"cpu": resource.MustParse("2500m")},
			},
		},
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
	// running sum = main(1) + sidecar(2) = 3
	// init peak   = max(sidecar(2), sidecar(2)+migrator(2.5)=4.5) = 4.5
	// effective   = max(3, 4.5) = 4.5
	got := list.Items[0].Spec.Resources["cpu"]
	if got.Cmp(resource.MustParse("4500m")) != 0 {
		t.Errorf("cpu = %s, want 4500m (init stage incl. sidecar dominates)", got.String())
	}
}

// TestReconciler_NodeSelectorMergedIntoRequirements pins M68b item 3:
// pod.Spec.NodeSelector entries become In requirements on the CR, just
// as required nodeAffinity does — pre-fix they were silently dropped
// and a pinned pod produced a CR any machine could satisfy.
func TestReconciler_NodeSelectorMergedIntoRequirements(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("pinned-0", true, 8)
	pod.Spec.NodeSelector = map[string]string{
		"topology.kubernetes.io/zone": "eu-west-1a",
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
	reqs := list.Items[0].Spec.Requirements
	if len(reqs) != 2 {
		t.Fatalf("requirements = %+v, want affinity expression + nodeSelector entry", reqs)
	}
	found := false
	for _, req := range reqs {
		if req.Key == "topology.kubernetes.io/zone" &&
			req.Operator == corev1.NodeSelectorOpIn &&
			len(req.Values) == 1 && req.Values[0] == "eu-west-1a" {
			found = true
		}
	}
	if !found {
		t.Errorf("nodeSelector entry not carried as In requirement: %+v", reqs)
	}
}

// A nodeSelector-only pod (no nodeAffinity) carries its real
// constraint instead of the synthetic instance-type Exists fallback.
func TestReconciler_NodeSelectorOnly_NoSyntheticRequirement(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("pinned-1", false, 8)
	pod.Spec.NodeSelector = map[string]string{
		"disktype":                    "ssd",
		"topology.kubernetes.io/zone": "eu-west-1b",
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
	reqs := list.Items[0].Spec.Requirements
	// Sorted by key for determinism: disktype, then zone.
	if len(reqs) != 2 || reqs[0].Key != "disktype" || reqs[1].Key != "topology.kubernetes.io/zone" {
		t.Fatalf("requirements = %+v, want sorted nodeSelector entries only", reqs)
	}
	for _, req := range reqs {
		if req.Operator != corev1.NodeSelectorOpIn || len(req.Values) != 1 {
			t.Errorf("requirement %q not translated as In [value]: %+v", req.Key, req)
		}
	}
}

// TestReconciler_DropsScheduleAnywaySpread pins M68b item 10: nothing
// engine-side consumes ScheduleAnyway spread terms (Phase 1 enforces
// DoNotSchedule only), so the UPC drops them at the demand edge rather
// than letting them fragment roll-up aggregation.
func TestReconciler_DropsScheduleAnywaySpread(t *testing.T) {
	t.Parallel()
	pod := unschedulablePod("spread-0", true, 8)
	pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
		{
			TopologyKey:       "topology.kubernetes.io/zone",
			MaxSkew:           1,
			WhenUnsatisfiable: corev1.DoNotSchedule,
		},
		{
			TopologyKey:       "kubernetes.io/hostname",
			MaxSkew:           2,
			WhenUnsatisfiable: corev1.ScheduleAnyway,
		},
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
	spread := list.Items[0].Spec.TopologySpread
	if len(spread) != 1 {
		t.Fatalf("TopologySpread = %+v, want only the DoNotSchedule term", spread)
	}
	if spread[0].TopologyKey != "topology.kubernetes.io/zone" ||
		spread[0].WhenUnsatisfiable != corev1.DoNotSchedule {
		t.Errorf("kept term = %+v, want the DoNotSchedule zone term", spread[0])
	}
}
