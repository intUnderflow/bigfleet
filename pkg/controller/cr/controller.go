// Package cr implements the optional per-pod CapacityRequest controller
// — the agent users opt into when they want unschedulable pods to
// trigger BigFleet capacity provisioning automatically.
//
// The controller watches Pods. When a Pod has PodScheduled=False with
// reason=Unschedulable, the controller creates a CapacityRequest that
// owns the pod via ownerRef (so deleting the pod garbage-collects the
// CR — the paper's implicit-withdrawal contract).
//
// This package is *optional*. Operators that drive CRs from their own
// admission controller, from Kueue, or from any other source are
// welcome to skip this controller entirely.
package cr

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

const (
	// AnnotationInterruptionPenalty is the pod annotation users set to
	// declare an interruption-penalty dollar value. Empty / missing → 0.
	AnnotationInterruptionPenalty = "bigfleet.lucy.sh/interruption-penalty"
	// AnnotationReclamationPenalty is the pod annotation for the
	// reclamation-penalty dollar value.
	AnnotationReclamationPenalty = "bigfleet.lucy.sh/reclamation-penalty"

	// LabelOwnedByPod is set on CRs the controller creates so users can
	// query them (kubectl get cr -l bigfleet.lucy.sh/owned-by=pod).
	LabelOwnedByPod = "bigfleet.lucy.sh/owned-by-pod"

	// crNamePrefix is the leading string on CR names this controller
	// creates. Combined with the pod's UID for uniqueness.
	crNamePrefix = "cr-pod-"
)

// PriorityClassDefaults is the per-PriorityClass penalty fallback used
// when a pod doesn't carry an explicit annotation. Resolution order
// in buildCRForPod is:
//  1. pod annotation (bigfleet.lucy.sh/{interruption,reclamation}-penalty)
//  2. matching PriorityClass default (this map, keyed by Pod.Spec.PriorityClassName)
//  3. nil (the autoscaler treats absent penalty as 0)
//
// M16: the platform team configures these defaults centrally via the
// helm chart so workload owners don't need to set every annotation.
type PriorityClassDefaults struct {
	InterruptionPenalty *resource.Quantity
	ReclamationPenalty  *resource.Quantity
}

// Reconciler watches Pods and creates CapacityRequests for unschedulable
// ones. Add it to a manager via SetupWithManager.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// PriorityClassDefaults maps PriorityClass name → penalty fallback.
	// Nil / absent → no defaults applied; controller behaves as before
	// M16 (annotation-only).
	PriorityClassDefaults map[string]PriorityClassDefaults
}

// SetupWithManager wires the Reconciler into a controller-runtime
// manager. The default controller name is
// "bigfleet-unschedulable-pod-controller"; tests that instantiate
// multiple managers in one process should pass a unique name.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts ...SetupOption) error {
	cfg := setupConfig{name: "bigfleet-unschedulable-pod-controller"}
	for _, opt := range opts {
		opt(&cfg)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Named(cfg.name).
		Complete(r)
}

// SetupOption customises SetupWithManager.
type SetupOption func(*setupConfig)

type setupConfig struct {
	name string
}

// WithControllerName overrides the default controller name. Useful in
// tests that bring up multiple managers in the same process — the
// controller-runtime metrics registry rejects duplicate names.
func WithControllerName(name string) SetupOption {
	return func(c *setupConfig) { c.name = name }
}

// WithPriorityClassDefaults supplies the M16 PriorityClass → penalty
// defaults map. The cmd binary loads it from a YAML config file at
// startup; tests pass it directly. Empty / nil → no defaults applied.
func WithPriorityClassDefaults(m map[string]PriorityClassDefaults) AddOption {
	return func(c *addOptions) { c.defaults = m }
}

// AddOption customises AddToManager. Distinct from SetupOption so the
// Reconciler can be configured before SetupWithManager runs.
type AddOption func(*addOptions)

type addOptions struct {
	setup    []SetupOption
	defaults map[string]PriorityClassDefaults
}

// WithSetupOption forwards a SetupOption through AddToManager.
func WithSetupOption(o SetupOption) AddOption {
	return func(c *addOptions) { c.setup = append(c.setup, o) }
}

// Reconcile is called by controller-runtime for each pod event. We
// only care about pods where PodScheduled=False, reason=Unschedulable.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pod", req.NamespacedName)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			// Pod gone — its CR is GC'd via ownerRef.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !isUnschedulable(&pod) {
		return ctrl.Result{}, nil
	}
	// Idempotent: skip if a CR already exists for this pod.
	exists, err := r.crExistsForPod(ctx, &pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if exists {
		return ctrl.Result{}, nil
	}

	newCR := r.buildCRForPod(&pod)
	if err := r.Create(ctx, newCR); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("create CR: %w", err)
	}
	logger.Info("created CapacityRequest", "cr", newCR.Name)
	// M19: stamp the initial Pending phase. Status subresource — needs a
	// separate write since Create cannot set status. The operator's
	// markAcknowledged path treats both "" and "Pending" as "needs
	// acknowledging", so a transient Pending → Acknowledged → Pending
	// → Acknowledged flap (if the operator's first rollup races this
	// patch) is harmless: the next rollup re-acks. Failure here is
	// non-fatal — the lifecycle still works without it, just less
	// observable from kubectl.
	if err := r.patchInitialPending(ctx, newCR); err != nil {
		logger.Info("patch initial Pending status failed (non-fatal)", "err", err)
	}
	return ctrl.Result{}, nil
}

// patchInitialPending stamps status.phase=Pending on the freshly-
// created CR via a JSON merge-patch. Mirrors the operator's
// markAcknowledged pattern (single apiserver call, no resource-version
// precondition). Idempotent if Phase is already Pending; on
// already-Acknowledged CRs the operator's next rollup will re-ack.
func (r *Reconciler) patchInitialPending(ctx context.Context, cr *bfv1alpha1.CapacityRequest) error {
	patch, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"phase": string(bfv1alpha1.CapacityRequestPending),
		},
	})
	if err != nil {
		return err
	}
	target := &bfv1alpha1.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{Name: cr.Name, Namespace: cr.Namespace},
	}
	if err := r.Status().Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// AddToManager is the convenience wiring used by cmd/bigfleet-unschedulable-pod-controller.
func AddToManager(mgr manager.Manager, opts ...AddOption) error {
	cfg := addOptions{}
	for _, o := range opts {
		o(&cfg)
	}
	r := &Reconciler{
		Client:                mgr.GetClient(),
		Scheme:                mgr.GetScheme(),
		PriorityClassDefaults: cfg.defaults,
	}
	return r.SetupWithManager(mgr, cfg.setup...)
}

// isUnschedulable reports whether the pod has PodScheduled=False with
// reason=Unschedulable.
func isUnschedulable(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse &&
			c.Reason == corev1.PodReasonUnschedulable {
			return true
		}
	}
	return false
}

// crExistsForPod returns whether the controller has already produced a
// CR owned by this pod. Looks up by label.
func (r *Reconciler) crExistsForPod(ctx context.Context, pod *corev1.Pod) (bool, error) {
	var list bfv1alpha1.CapacityRequestList
	if err := r.List(ctx, &list,
		client.InNamespace(pod.Namespace),
		client.MatchingLabels{LabelOwnedByPod: string(pod.UID)},
	); err != nil {
		return false, err
	}
	return len(list.Items) > 0, nil
}

// buildCRForPod constructs the CapacityRequest from the pod's spec.
// Currently always succeeds; the error return is reserved for future
// validation (e.g., rejecting pods with topology terms we cannot yet
// translate).
func (r *Reconciler) buildCRForPod(pod *corev1.Pod) *bfv1alpha1.CapacityRequest {
	requirements := requirementsFromPod(pod)
	resources := resourcesFromPod(pod)

	cr := &bfv1alpha1.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    pod.Namespace,
			GenerateName: crNamePrefix + pod.Name + "-",
			Labels: map[string]string{
				LabelOwnedByPod: string(pod.UID),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       pod.Name,
				UID:        pod.UID,
				Controller: ptrBool(true),
			}},
		},
		Spec: bfv1alpha1.CapacityRequestSpec{
			Requirements:        requirements,
			Resources:           resources,
			Priority:            podPriority(pod),
			TopologySpread:      topologySpreadFromPod(pod),
			InterruptionPenalty: r.resolveInterruptionPenalty(pod),
			ReclamationPenalty:  r.resolveReclamationPenalty(pod),
		},
	}
	return cr
}

// resolveInterruptionPenalty applies the M16 fallback chain:
//  1. pod annotation
//  2. PriorityClass default for pod.Spec.PriorityClassName
//  3. nil (autoscaler treats as 0)
func (r *Reconciler) resolveInterruptionPenalty(pod *corev1.Pod) *resource.Quantity {
	if v := penaltyFromAnnotation(pod.Annotations[AnnotationInterruptionPenalty]); v != nil {
		return v
	}
	if def, ok := r.PriorityClassDefaults[pod.Spec.PriorityClassName]; ok {
		return def.InterruptionPenalty
	}
	return nil
}

func (r *Reconciler) resolveReclamationPenalty(pod *corev1.Pod) *resource.Quantity {
	if v := penaltyFromAnnotation(pod.Annotations[AnnotationReclamationPenalty]); v != nil {
		return v
	}
	if def, ok := r.PriorityClassDefaults[pod.Spec.PriorityClassName]; ok {
		return def.ReclamationPenalty
	}
	return nil
}

func requirementsFromPod(pod *corev1.Pod) []corev1.NodeSelectorRequirement {
	if pod.Spec.Affinity == nil ||
		pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		// No node affinity — synthesize an Exists requirement on the
		// instance-type label so the autoscaler still has something to
		// match against. The shard's MatchProfile will satisfy this with
		// any machine that has an instance-type set.
		return []corev1.NodeSelectorRequirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: corev1.NodeSelectorOpExists,
		}}
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return nil
	}
	// Take the union of MatchExpressions across the first term — pods
	// with multiple terms / OR semantics are out of scope for v1.
	out := make([]corev1.NodeSelectorRequirement, 0, len(terms[0].MatchExpressions))
	out = append(out, terms[0].MatchExpressions...)
	return out
}

func resourcesFromPod(pod *corev1.Pod) corev1.ResourceList {
	out := corev1.ResourceList{}
	add := func(rl corev1.ResourceList) {
		for k, v := range rl {
			cur := out[k]
			cur.Add(v)
			out[k] = cur
		}
	}
	for _, c := range pod.Spec.InitContainers {
		add(c.Resources.Requests)
	}
	for _, c := range pod.Spec.Containers {
		add(c.Resources.Requests)
	}
	return out
}

func podPriority(pod *corev1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	return 0
}

func topologySpreadFromPod(pod *corev1.Pod) []bfv1alpha1.TopologySpreadConstraint {
	out := make([]bfv1alpha1.TopologySpreadConstraint, 0, len(pod.Spec.TopologySpreadConstraints))
	for _, c := range pod.Spec.TopologySpreadConstraints {
		out = append(out, bfv1alpha1.TopologySpreadConstraint{
			TopologyKey:       c.TopologyKey,
			MaxSkew:           c.MaxSkew,
			WhenUnsatisfiable: c.WhenUnsatisfiable,
		})
	}
	return out
}

// penaltyFromAnnotation parses a dollar value from an annotation. Empty
// or unparseable → nil (defaults to zero penalty).
func penaltyFromAnnotation(value string) *resource.Quantity {
	if value == "" {
		return nil
	}
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return nil
	}
	return &q
}

func ptrBool(b bool) *bool { return &b }
