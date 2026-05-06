// Command bigfleet-scaletest-pod-shim stands in for kube-scheduler
// inside a kwok-backed scaletest pod (M43b / Item 10).
//
// Real production:
//
//	Pod created → kube-scheduler tries to bind → no fit
//	            → PodScheduled=False, reason=Unschedulable
//	            → bigfleet-unschedulable-pod-controller creates a CR
//
// The kwok harness runs no kube-scheduler and no Nodes. Without a
// shim, Pods sit in the apiserver indefinitely with no PodScheduled
// condition; the unschedulable-pod-controller watches for
// reason=Unschedulable specifically and never sees one.
//
// This shim is the cheap, faithful substitute. It watches Pods; for
// every Pod without a nodeName and without an existing PodScheduled
// condition (or one carrying any other reason), it patches the
// status to PodScheduled=False, reason=Unschedulable. From there
// the unschedulable-pod-controller takes over with its real
// production logic.
//
// M43c will extend this binary with a CR-watcher that, when the
// shard binds a CR, creates a fake Node and binds the Pod to it —
// closing the user-facing latency loop.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bigfleet-scaletest-pod-shim:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("pod-shim", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	kubeconfig := fs.String("kubeconfig", "", "path to kubeconfig (default: in-cluster or $KUBECONFIG)")
	metricsAddr := fs.String("metrics-addr", ":8772", "Prometheus metrics listen address (\"0\" disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if *kubeconfig != "" {
		rules.ExplicitPath = *kubeconfig
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	restCfg, err := cc.ClientConfig()
	if err != nil {
		return err
	}
	// Match the operator's QPS budget — the shim's writes are bursty
	// during ramp (one per new Pod).
	restCfg.QPS = 50
	restCfg.Burst = 100

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: *metricsAddr},
	})
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	r := &podSchedulerShim{Client: mgr.GetClient()}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Named("bigfleet-scaletest-pod-shim").
		Complete(r); err != nil {
		return fmt.Errorf("controller: %w", err)
	}

	// M43c: UpcomingNode → Node + Pod-binding loop. Watches the
	// UpcomingNode CRDs the operator publishes; on Phase=Ready,
	// creates a matching k8s Node (idempotent) and binds one
	// pending Pod whose nodeAffinity matches the new Node's
	// labels.
	un := &upcomingNodeBinder{Client: mgr.GetClient()}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&bfv1alpha1.UpcomingNode{}).
		Named("bigfleet-scaletest-upcoming-node-binder").
		Complete(un); err != nil {
		return fmt.Errorf("upcoming-node controller: %w", err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil && !errors.Is(err, ctrl.SetupSignalHandler().Err()) {
		return err
	}
	return nil
}

// podSchedulerShim sets PodScheduled=False, reason=Unschedulable on
// any Pod that has no .spec.nodeName and no existing condition with
// the same reason. Idempotent — re-reconciles are no-ops.
type podSchedulerShim struct {
	client.Client
}

func (r *podSchedulerShim) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	if pod.Spec.NodeName != "" {
		// Pod is already bound (M43c will be the path that did the
		// binding). Nothing to do.
		return reconcile.Result{}, nil
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
			return reconcile.Result{}, nil
		}
	}
	patch := client.MergeFrom(pod.DeepCopy())
	cond := corev1.PodCondition{
		Type:    corev1.PodScheduled,
		Status:  corev1.ConditionFalse,
		Reason:  corev1.PodReasonUnschedulable,
		Message: "bigfleet-scaletest-pod-shim: no Node available; bigfleet-unschedulable-pod-controller will create a CR",
	}
	// Replace any existing PodScheduled condition; otherwise append.
	replaced := false
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodScheduled {
			pod.Status.Conditions[i] = cond
			replaced = true
			break
		}
	}
	if !replaced {
		pod.Status.Conditions = append(pod.Status.Conditions, cond)
	}
	if err := r.Status().Patch(ctx, &pod, patch); err != nil {
		return reconcile.Result{}, fmt.Errorf("patch status: %w", err)
	}
	return reconcile.Result{}, nil
}

// upcomingNodeBinder reconciles UpcomingNode → (k8s Node, Pod binding).
// When an UpcomingNode reaches Phase=Ready, the binder:
//
//  1. Reads UpcomingNode.Spec.{Labels, Resources, Taints} populated
//     by the operator from the shard's NodeStateUpdate (ADR-0016).
//  2. Creates a k8s Node named "fake-{machineID}" carrying those.
//  3. Walks pending Pods in the cluster; binds the first whose
//     nodeAffinity matches the new Node's labels.
//  4. Sets PodScheduled=True on the bound Pod.
//
// Single-Pod-per-Node assumed (matches the test's 1:1 CR→machine
// pattern); multi-Pod packing is a future enhancement.
type upcomingNodeBinder struct {
	client.Client
}

func (r *upcomingNodeBinder) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var upn bfv1alpha1.UpcomingNode
	if err := r.Get(ctx, req.NamespacedName, &upn); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if upn.Status.Phase != bfv1alpha1.UpcomingNodeReady {
		return reconcile.Result{}, nil
	}

	// 1. Ensure the k8s Node exists (idempotent — if a previous
	// reconcile created it, skip Create + status patch but still
	// attempt to bind a Pod below).
	nodeName := nodeNameFromUpcoming(upn.Name)
	var existing corev1.Node
	getErr := r.Get(ctx, client.ObjectKey{Name: nodeName}, &existing)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return reconcile.Result{}, fmt.Errorf("get node: %w", getErr)
	}
	if apierrors.IsNotFound(getErr) {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: cloneLabels(upn.Spec.Labels)},
			Spec: corev1.NodeSpec{
				Taints: append([]corev1.Taint(nil), upn.Spec.Taints...),
			},
			Status: corev1.NodeStatus{
				Capacity:    upn.Spec.Resources,
				Allocatable: upn.Spec.Resources,
				Conditions: []corev1.NodeCondition{{
					Type:    corev1.NodeReady,
					Status:  corev1.ConditionTrue,
					Reason:  "KubeletReady",
					Message: "bigfleet-scaletest-pod-shim: fake Node provisioned by BigFleet",
				}},
			},
		}
		// Best-effort status fill — apiserver typically strips Status
		// on Create, but we try a follow-up Status().Update on the
		// returned object's resourceVersion. Cache lag means a Get
		// here often misses the object, so we build the patch off
		// `node` directly which Create populated. Failure here is
		// non-fatal for binding (binding only requires the Node to
		// exist; allocatable/Ready advertisement is a kwok-stage
		// hint that the harness can live without).
		if err := r.Create(ctx, node); err != nil && !apierrors.IsAlreadyExists(err) {
			return reconcile.Result{}, fmt.Errorf("create node: %w", err)
		}
		if node.ResourceVersion != "" {
			statusPatch := node.DeepCopy()
			statusPatch.Status = corev1.NodeStatus{
				Capacity:    upn.Spec.Resources,
				Allocatable: upn.Spec.Resources,
				Conditions: []corev1.NodeCondition{{
					Type:    corev1.NodeReady,
					Status:  corev1.ConditionTrue,
					Reason:  "KubeletReady",
					Message: "bigfleet-scaletest-pod-shim: fake Node provisioned by BigFleet",
				}},
			}
			_ = r.Status().Update(ctx, statusPatch)
		}
	}

	// 2. Bind one pending Pod that matches the Node's labels. Runs
	// every reconcile regardless of whether the Node was just created
	// or already existed.
	var pods corev1.PodList
	if err := r.List(ctx, &pods); err != nil {
		return reconcile.Result{}, fmt.Errorf("list pods: %w", err)
	}
	bound := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName != "" {
			continue
		}
		if !podMatchesNodeLabels(pod, upn.Spec.Labels) {
			continue
		}
		// Pod.spec.nodeName is immutable via PATCH; bindings are
		// created via the /binding subresource. controller-runtime's
		// SubResource("binding").Create routes through that endpoint.
		binding := &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			Target: corev1.ObjectReference{
				Kind: "Node",
				Name: nodeName,
			},
		}
		if err := r.SubResource("binding").Create(ctx, pod, binding); err != nil {
			continue
		}
		bound = true
		break
	}
	if !bound {
		// No matching Pod was Pending yet — requeue. This handles
		// the race where the UpcomingNode races ahead of the Pod that
		// triggered the CR creation. RequeueAfter is short because
		// the Pod usually arrives within a single rollup interval.
		return reconcile.Result{RequeueAfter: 2 * time.Second}, nil
	}
	return reconcile.Result{}, nil
}

func cloneLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// podMatchesNodeLabels checks the Pod's nodeAffinity (required terms
// only) + nodeSelector against the Node's labels. Standard
// In/NotIn/Exists/DoesNotExist semantics; multiple terms ORed,
// matchExpressions within a term ANDed.
func podMatchesNodeLabels(pod *corev1.Pod, nodeLabels map[string]string) bool {
	for k, v := range pod.Spec.NodeSelector {
		if nodeLabels[k] != v {
			return false
		}
	}
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return true
	}
	req := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) == 0 {
		return true
	}
	for _, term := range req.NodeSelectorTerms {
		if termMatches(term, nodeLabels) {
			return true
		}
	}
	return false
}

func termMatches(term corev1.NodeSelectorTerm, nodeLabels map[string]string) bool {
	for _, expr := range term.MatchExpressions {
		v, has := nodeLabels[expr.Key]
		switch expr.Operator {
		case corev1.NodeSelectorOpIn:
			ok := false
			for _, want := range expr.Values {
				if has && v == want {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		case corev1.NodeSelectorOpNotIn:
			for _, want := range expr.Values {
				if has && v == want {
					return false
				}
			}
		case corev1.NodeSelectorOpExists:
			if !has {
				return false
			}
		case corev1.NodeSelectorOpDoesNotExist:
			if has {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// nodeNameFromUpcoming maps "un-{machineID}" → "fake-{machineID}". The
// "fake-" prefix flags this as a test artefact in `kubectl get nodes`.
func nodeNameFromUpcoming(upcomingName string) string {
	return "fake-" + strings.TrimPrefix(upcomingName, "un-")
}

// (M43c earlier had setPodScheduledTrue; the /binding subresource
// flips PodScheduled=True automatically — kube-scheduler's actual
// behaviour. Helper removed in favour of the apiserver's built-in
// transition.)
