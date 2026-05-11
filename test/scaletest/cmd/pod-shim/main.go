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

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

// labelClaimedByPod is set on a fake-Node when the binder atomically
// claims it for a Pod via apiserver Patch — the apiserver-side lock
// that prevents two concurrent binders racing for the same Node and
// double-binding it. The label's presence is the lock; its value
// (the Pod name) is convenient for debugging but not load-bearing.
const labelClaimedByPod = "scaletest.bigfleet/claimed-by-pod"

// podBindLatencySeconds is ADR-0017's per-Pod binding-latency
// histogram — wall-clock from Pod.metadata.creationTimestamp to the
// moment this shim issues clientset.CoreV1().Pods.Bind. This is
// what users feel from "I asked for capacity" to "my Pod is bound";
// the runner gates on it directly. Per-Pod granularity, sub-second
// to ~100 s buckets covering the plausible range from in-process
// fake provider (sub-second) to real cloud provisioning (~minutes).
var podBindLatencySeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "bigfleet_scaletest_pod_bind_latency_seconds",
	Help:    "BigFleet-internal binding latency: wall-clock from Pod.metadata.creationTimestamp to the bigfleet-scaletest-pod-shim issuing the binding subresource Create on a fake Node. Per-Pod granularity. Records every Pod, including initial-fill thundering-herd ramps. ADR-0018: the harness fake provider returns instantly, so this measures BigFleet's contribution only.",
	Buckets: prometheus.ExponentialBuckets(0.05, 2, 12), // 0.05s, 0.1s, 0.2s, ... 102.4s
})

// podBindLatencySteadySeconds is the SLO-bearing histogram. Records
// only Pods carrying bigfleet.lucy.sh/steady-state=true (set by the
// load-driver after the cluster reaches its target Pod count). The
// initial fill of the cluster from cold start is a synthetic
// thundering-herd that doesn't reflect production binding behaviour
// (production has existing inventory + steady-state churn + occasional
// bursts, not a 50K-Pod cold start). Excluding the fill keeps the SLO
// honest about what users actually feel.
var podBindLatencySteadySeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "bigfleet_scaletest_pod_bind_latency_steady_seconds",
	Help:    "BigFleet-internal binding latency for STEADY-STATE Pods only — those carrying bigfleet.lucy.sh/steady-state=true (created by the load-driver after the cluster reached its target Pod count). The runner's SLO gates on this rather than the all-Pods histogram so the cluster's initial-fill thundering herd doesn't dominate the p99.",
	Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
})

// M44.4 chain-drop diagnostic counters. The 50K-Pod-mode cloud run
// surfaced ~30× drop between Pods created (148 K) and Pods bound
// (4 K). Per-stage counters localise where the chain throttles.
var (
	podsMarkedUnschedulable = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_pod_shim_pods_marked_unschedulable_total",
		Help: "Count of Pods this shim has patched to PodScheduled=False (Unschedulable). Compare to load-driver's scaletest_loadgen_cr_created_total to localise the first hop drop.",
	}, []string{"outcome"}) // outcome: patched, already_marked, error
	upcomingNodesObserved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_pod_shim_upcoming_nodes_observed_total",
		Help: "Count of UpcomingNode reconcile events seen by the fake-Node reconciler. Compare to shard's bigfleet_shard_actions_total{kind=Bootstrap} to find shard→cluster propagation gaps.",
	}, []string{"outcome"}) // outcome: created, exists, not_ready
	fakeNodesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_pod_shim_fake_nodes_created_total",
		Help: "Count of fake Node objects this shim has created from UpcomingNodes.",
	})
	podBindAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_pod_shim_pod_bind_attempts_total",
		Help: "Count of Pod-binding subresource attempts by outcome. The success count should match podBindLatencySeconds_count. claim_lost = another binder beat us to the Node label patch (apiserver-side lock); bind_error = Bind subresource itself failed (Pod gone or apiserver issue) — see pod_bind_errors_total for the reason breakdown.",
	}, []string{"outcome"}) // outcome: success, claim_lost, bind_error
	// Drop N: classify bind_error by apiserver status so the long-tail
	// p99 has a named cause. Ramp-phase runs at ~50% error rate; this
	// counter tells us which apiserver response codes drive that.
	podBindErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_pod_shim_pod_bind_errors_total",
		Help: "Per-reason breakdown of Pod /binding subresource failures (outcome=bind_error). reason classifies the apierror class: not_found, conflict, forbidden, throttled, timeout, server, other.",
	}, []string{"reason"})
)

// classifyBindError maps an apiserver error returned from the /binding
// subresource to a short, low-cardinality reason. The /binding subresource
// is the canonical scheduler path; failures here are interesting in
// proportion to how often they appear, so the labels stay coarse on
// purpose. Anything we can't recognise lands in "other" — we'd rather
// see a small "other" bucket and add a case than blow up cardinality.
func classifyBindError(err error) string {
	switch {
	case err == nil:
		return "none"
	case apierrors.IsNotFound(err):
		return "not_found"
	case apierrors.IsConflict(err):
		return "conflict"
	case apierrors.IsForbidden(err):
		return "forbidden"
	case apierrors.IsTooManyRequests(err):
		return "throttled"
	case apierrors.IsServerTimeout(err), apierrors.IsTimeout(err):
		return "timeout"
	case apierrors.IsInternalError(err), apierrors.IsServiceUnavailable(err):
		return "server"
	default:
		return "other"
	}
}

func init() {
	// Register on controller-runtime's metrics registry so all
	// histograms + counters are served on the same :8772 endpoint.
	ctrlmetrics.Registry.MustRegister(
		podBindLatencySeconds,
		podBindLatencySteadySeconds,
		podsMarkedUnschedulable,
		upcomingNodesObserved,
		fakeNodesCreated,
		podBindAttempts,
		podBindErrors,
	)
}

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
	qps := fs.Float64("qps", 50, "client-go QPS budget for apiserver requests; raise for scale-test profiles whose apiserver can absorb more")
	burst := fs.Int("burst", 100, "client-go burst budget for apiserver requests")
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
	// during ramp (one per new Pod). The default 50/100 is production-
	// safe; scale-test profiles override (M44.4 — the binder does 3
	// apiserver writes per UpcomingNode (Create Node + Status Update +
	// Bind), so 1000 Pods/cluster × 3 / 50 QPS ≈ 60 s p99 binding
	// latency unless the budget is raised).
	restCfg.QPS = float32(*qps)
	restCfg.Burst = *burst

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

	// Typed clientset for the /binding subresource — controller-
	// runtime's SubResource("binding") path has subtle issues with
	// the cache layer; the typed client.CoreV1().Pods(ns).Bind() is
	// well-tested and goes straight to the apiserver.
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("clientset: %w", err)
	}

	// M44.4 Drop G: pod-driven binding. The Pod controller is now the
	// binder — it owns the "find a fake-Node and Bind to it" path.
	// The UpcomingNode controller is reduced to "create the fake-Node";
	// it no longer races against itself across 64 reconcilers for the
	// single matching Pod (M44.4 Drop F surfaced 64 % bind-attempt
	// errors — concurrent reconcilers all picked the same pending Pod
	// then collided on the Bind subresource).
	//
	// The binder also watches Nodes via Watches(): when a new fake-Node
	// is created, all currently-pending Pods get re-enqueued (filtered
	// by label compatibility). This is what unblocks the queue when a
	// fresh fake-Node arrives — without it, a Pod that reconciled
	// before any matching Node existed would only retry on its own
	// status changes (rare for a still-unschedulable Pod).
	pb := &podBinder{Client: mgr.GetClient(), clientset: clientset}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return enqueueMatchingPendingPods(ctx, mgr.GetClient(), obj)
			}),
		).
		Named("bigfleet-scaletest-pod-binder").
		WithOptions(controller.Options{MaxConcurrentReconciles: 64}).
		Complete(pb); err != nil {
		return fmt.Errorf("pod-binder controller: %w", err)
	}

	// UpcomingNode → fake-Node only. No Pod binding here anymore.
	// MaxConcurrentReconciles low because each reconcile is now O(1):
	// Get the Node, Create-if-missing, done. Most events are
	// already-exists no-ops (operator status updates fire many events
	// per machine; only the first matters for Node creation).
	fn := &upcomingNodeFakeNodeReconciler{Client: mgr.GetClient()}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&bfv1alpha1.UpcomingNode{}).
		Named("bigfleet-scaletest-upcoming-node-fake-node").
		WithOptions(controller.Options{MaxConcurrentReconciles: 8}).
		Complete(fn); err != nil {
		return fmt.Errorf("upcoming-node controller: %w", err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil && !errors.Is(err, ctrl.SetupSignalHandler().Err()) {
		return err
	}
	return nil
}

// podBinder is the M44.4 Drop G refactor: the Pod controller owns the
// "find a fake-Node and Bind to it" path. Per Pod reconcile:
//
//  1. Skip if already bound (Spec.NodeName != "").
//  2. List unclaimed fake-Nodes (label !claimed-by-pod).
//  3. Find one whose labels satisfy the Pod's nodeSelector + nodeAffinity.
//  4. Atomically claim the Node by Patch-adding the claimed-by-pod
//     label (apiserver-side optimistic-concurrency lock — only one
//     binder wins). Lost claim ⇒ try the next candidate.
//  5. Bind the Pod. On failure, the claim leaks but the Pod retries
//     on the next reconcile against a different Node.
//  6. Record bind latency.
//
// If no candidate Node matches, mark the Pod Unschedulable so the
// unschedulable-pod-controller creates a CR; the chain wakes up and
// Bootstraps a fresh fake-Node, the Watches(Node) hook re-enqueues
// this Pod, and the next reconcile completes the bind.
//
// Why this beats the old UpcomingNode-driven binder: that binder
// reconciled per-UpcomingNode-event and tried to claim the matching
// pending Pod. With M35 unique fingerprints each fake-Node matches
// one Pod, but with 64 concurrent reconcilers picking from a shared
// pending-Pod set, ~64 % of Bind subresource calls collided and
// errored — observed in scaleway-50k Drop F (19/sec error vs
// 11/sec success). The Pod-driven shape inverts ownership: each
// pending Pod is responsible for finding its own Node, and the
// claimed-by-pod label is the apiserver-side lock that serialises
// concurrent claims atomically.
type podBinder struct {
	client.Client
	clientset kubernetes.Interface
}

func (b *podBinder) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var pod corev1.Pod
	if err := b.Get(ctx, req.NamespacedName, &pod); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if pod.Spec.NodeName != "" {
		return reconcile.Result{}, nil
	}

	// 1. Try to find + claim + bind a matching fake-Node.
	bound, err := b.tryBind(ctx, &pod)
	if err != nil {
		return reconcile.Result{}, err
	}
	if bound {
		return reconcile.Result{}, nil
	}

	// 2. No fake-Node fits → mark Unschedulable so UPC creates a CR
	// and the chain provisions one. Idempotent — re-reconciles are
	// no-ops.
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
			podsMarkedUnschedulable.WithLabelValues("already_marked").Inc()
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
	if err := b.Status().Patch(ctx, &pod, patch); err != nil {
		podsMarkedUnschedulable.WithLabelValues("error").Inc()
		return reconcile.Result{}, fmt.Errorf("patch status: %w", err)
	}
	podsMarkedUnschedulable.WithLabelValues("patched").Inc()
	return reconcile.Result{}, nil
}

// tryBind iterates unclaimed fake-Nodes, atomically claims the first
// label-compatible one, and binds the Pod. Returns (true, nil) on
// success, (false, nil) when no Node matches, (false, err) on a hard
// apiserver failure that should retry.
func (b *podBinder) tryBind(ctx context.Context, pod *corev1.Pod) (bool, error) {
	sel, err := labels.Parse("!" + labelClaimedByPod)
	if err != nil {
		return false, err
	}
	var nodes corev1.NodeList
	if err := b.List(ctx, &nodes, &client.ListOptions{LabelSelector: sel}); err != nil {
		return false, fmt.Errorf("list nodes: %w", err)
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if !strings.HasPrefix(n.Name, fakeNodePrefix) {
			continue
		}
		if !podMatchesNodeLabels(pod, n.Labels) {
			continue
		}
		// Atomic claim via Patch with optimistic concurrency.
		// MergeFrom captures the resourceVersion at the snapshot
		// we read, so a concurrent claim by another reconciler
		// (for the same Node) results in a Conflict error here.
		patch := client.MergeFrom(n.DeepCopy())
		if n.Labels == nil {
			n.Labels = map[string]string{}
		}
		n.Labels[labelClaimedByPod] = pod.Name
		if err := b.Patch(ctx, n, patch); err != nil {
			// Conflict (race-lost) or NotFound (Node deleted under
			// us). Either way, try the next candidate.
			podBindAttempts.WithLabelValues("claim_lost").Inc()
			continue
		}
		// Claim won — Bind the Pod. The /binding subresource is the
		// scheduler-canonical path; the typed client goes straight
		// at the apiserver, no controller-runtime cache between us
		// and the truth.
		binding := &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			Target:     corev1.ObjectReference{Kind: "Node", Name: n.Name},
		}
		if err := b.clientset.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{}); err != nil {
			// Bind failed after we claimed the Node. The claim
			// label leaks (Node is now permanently un-bindable),
			// but the Pod will pick a different Node next reconcile.
			// Log but don't error — apiserver will retry the Pod.
			podBindAttempts.WithLabelValues("bind_error").Inc()
			podBindErrors.WithLabelValues(classifyBindError(err)).Inc()
			continue
		}
		podBindAttempts.WithLabelValues("success").Inc()
		if !pod.CreationTimestamp.IsZero() {
			latency := time.Since(pod.CreationTimestamp.Time).Seconds()
			podBindLatencySeconds.Observe(latency)
			if pod.Labels["scaletest.bigfleet/state"] == "steady" {
				podBindLatencySteadySeconds.Observe(latency)
			}
		}
		return true, nil
	}
	return false, nil
}

// upcomingNodeFakeNodeReconciler is the M44.4 Drop G slimmed-down
// successor to upcomingNodeBinder: it creates the fake-Node and
// nothing else. Pod binding moved to podBinder, where the natural
// Pod-driven cardinality avoids the 64-reconcilers-fighting-over-one-
// Pod collision the old shape produced.
type upcomingNodeFakeNodeReconciler struct {
	client.Client
}

func (r *upcomingNodeFakeNodeReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var upn bfv1alpha1.UpcomingNode
	if err := r.Get(ctx, req.NamespacedName, &upn); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if upn.Status.Phase != bfv1alpha1.UpcomingNodeReady {
		upcomingNodesObserved.WithLabelValues("not_ready").Inc()
		return reconcile.Result{}, nil
	}

	nodeName := nodeNameFromUpcoming(upn.Name)
	var existing corev1.Node
	getErr := r.Get(ctx, client.ObjectKey{Name: nodeName}, &existing)
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return reconcile.Result{}, fmt.Errorf("get node: %w", getErr)
	}
	if getErr == nil {
		upcomingNodesObserved.WithLabelValues("exists").Inc()
		return reconcile.Result{}, nil
	}

	fakeNodesCreated.Inc()
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
	upcomingNodesObserved.WithLabelValues("created").Inc()
	return reconcile.Result{}, nil
}

// enqueueMatchingPendingPods is the Watches(Node) handler that wakes
// up pending Pods when a fresh fake-Node arrives. Filtered by label
// compatibility so a typical fake-Node enqueues exactly one Pod
// reconcile (the M35 unique-fingerprint case) — controller-runtime
// dedup handles the rest.
func enqueueMatchingPendingPods(ctx context.Context, c client.Client, obj client.Object) []reconcile.Request {
	if !strings.HasPrefix(obj.GetName(), fakeNodePrefix) {
		return nil
	}
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	// If the Node is already claimed, no point waking pending Pods —
	// they couldn't bind to it anyway.
	if node.Labels[labelClaimedByPod] != "" {
		return nil
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, 4)
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName != "" {
			continue
		}
		if !podMatchesNodeLabels(p, node.Labels) {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: p.Namespace, Name: p.Name}})
	}
	return out
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

// fakeNodePrefix flags a Node as a harness-created stand-in for a
// BigFleet-provisioned machine. Used for filtering in the binder's
// Watches(Node) mapper and tryBind candidate scan.
const fakeNodePrefix = "fake-"

// nodeNameFromUpcoming maps "un-{machineID}" → "fake-{machineID}". The
// "fake-" prefix flags this as a test artefact in `kubectl get nodes`.
func nodeNameFromUpcoming(upcomingName string) string {
	return fakeNodePrefix + strings.TrimPrefix(upcomingName, "un-")
}

// (M43c earlier had setPodScheduledTrue; the /binding subresource
// flips PodScheduled=True automatically — kube-scheduler's actual
// behaviour. Helper removed in favour of the apiserver's built-in
// transition.)
