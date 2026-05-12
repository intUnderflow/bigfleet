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

// fieldIndexPodNodeName is the controller-runtime field-indexer key on
// Pod.spec.nodeName. ADR-0022 / M45.4: tryBind uses it to look up
// already-bound Pods on a candidate Node via a cache-served List rather
// than a full apiserver scan, so per-bind cost stays O(1) in the size
// of the Pod population even at density>1.
const fieldIndexPodNodeName = "spec.nodeName"

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
		Help: "Count of Pod-binding subresource attempts by outcome. The success count should match podBindLatencySeconds_count. claim_lost = another binder beat us to the Node label patch (apiserver-side lock on the Node). bound_by_other = Bind returned IsConflict, i.e. apiserver-side lock on the Pod fired — another reconcile already bound it, so the chain's goal is met (Drop T treats this as success). bind_error = Bind subresource failed for any other reason — see pod_bind_errors_total for the apierror class.",
	}, []string{"outcome"}) // outcome: success, bound_by_other, claim_lost, bind_error
	// Drop N: classify bind_error by apiserver status so the long-tail
	// p99 has a named cause. Ramp-phase runs at ~50% error rate; this
	// counter tells us which apiserver response codes drive that.
	podBindErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_pod_shim_pod_bind_errors_total",
		Help: "Per-reason breakdown of Pod /binding subresource failures (outcome=bind_error). reason classifies the apierror class: not_found, conflict, forbidden, throttled, timeout, server, other.",
	}, []string{"reason"})
	// Drop Q: per-stage residence histograms to localise the chain's
	// 50 s+ steady-state p99 tail. The runner already measures end-to-end
	// (pod creation → pod bound) and shardProvisioningLatency (Phase 1
	// emit → Bootstrap complete). These two close the remaining gap:
	// from when the operator surfaces an UpcomingNode through when the
	// Pod is bound to the fake-Node we built from it. Buckets match the
	// pod_bind_latency_steady_seconds histogram so the cumulative
	// distribution lines up visually in Grafana.
	upcomingToNodeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bigfleet_scaletest_pod_shim_upcoming_to_node_latency_seconds",
		Help:    "Wall-clock from UpcomingNode CR creation to fake-Node Create succeeding in this pod-shim. Captures the reconciler's queue + per-event handler time; a flat distribution here means the fake-Node controller is keeping up, a long tail means controller-runtime queueing.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
	})
	nodeToBoundLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bigfleet_scaletest_pod_shim_node_to_bound_latency_seconds",
		Help:    "Wall-clock from fake-Node creation to successful Bind for the Pod that ends up on it. Includes Watches(Node) re-enqueue, podBinder Reconcile queueing, candidate-Node scan, claim Patch, and the /binding RPC. The 'how long does pod-shim sit on a Ready Node before getting a Pod onto it?' question.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
	})
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
		upcomingToNodeLatency,
		nodeToBoundLatency,
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

	// ADR-0022 / M45.4: index Pods by spec.nodeName so tryBind's per-
	// candidate "what's already bound here" lookup is a cache hit
	// instead of an apiserver List. Real Kubernetes Nodes host
	// multiple Pods via Allocatable; the harness now matches that
	// shape rather than the M44 1-Pod-per-fake-Node simplification.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, fieldIndexPodNodeName, func(o client.Object) []string {
		pod, ok := o.(*corev1.Pod)
		if !ok || pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}); err != nil {
		return fmt.Errorf("index Pod.spec.nodeName: %w", err)
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

// podBinder is the M44.4 Drop G refactor with the M45.4 multi-Pod-per-
// Node rework. The Pod controller owns the "find a fake-Node and Bind"
// path. Per Pod reconcile:
//
//  1. Skip if already bound (Spec.NodeName != "").
//  2. List fake-Nodes (no claim filter — bin-pack, don't lock).
//  3. For each label-compatible candidate, compute
//     `remaining = Node.Allocatable - Σ(bound Pods on this Node).Requests`
//     via the spec.nodeName field index, and bind iff remaining covers
//     this Pod's Requests.
//  4. Bind the Pod via the /binding subresource. Apiserver-side IsConflict
//     on Pod.spec.nodeName already-set is the only authoritative race
//     guard — it prevents double-binding the same Pod (Drop T) but
//     does not enforce per-Node capacity. Capacity is best-effort:
//     within one pod-shim process the cache index is consistent;
//     across the brief window before Watch updates land, concurrent
//     reconciles may over-pack a Node by a handful of Pods. Real K8s
//     scheduler accepts the same shape — the apiserver doesn't gate
//     Bind on capacity, the scheduler's own bookkeeping does.
//
// If no candidate Node fits, mark the Pod Unschedulable so the
// unschedulable-pod-controller creates a CR; the chain wakes up and
// Bootstraps a fresh fake-Node, the Watches(Node) hook re-enqueues
// pending Pods, and the next reconcile completes the bind.
//
// ADR-0022 / M45.4: dropped the `scaletest.bigfleet/claimed-by-pod`
// label entirely. The old shape was 1 Pod = 1 Node by construction,
// which made density>1 unworkable end-to-end — a density-10 seed
// emitted ceil(totalPods/10) Bootstraps, so only that many Pods
// could ever claim a Node, and the rest sat Pending. Real fleets
// host many Pods per Node; the harness now matches that shape.
// The Drop O thundering-herd lesson (unclaim-on-error re-enqueues
// every pending Pod via Watches(Node)) still applies — we no
// longer Patch Nodes on the bind path, so the Watches re-trigger
// only fires on legitimate Node Add events (a fresh fake-Node
// landing), which is exactly what we want for waking pending Pods.
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

// tryBind iterates fake-Nodes and binds the Pod to the first that's
// label-compatible AND has enough remaining Allocatable to host this
// Pod's Requests. Returns (true, nil) on success, (false, nil) when no
// Node fits, (false, err) on a hard apiserver failure.
//
// ADR-0022 / M45.4: bin-packing model. No claim label; the /binding
// subresource is the only authoritative race guard, and only against
// double-binding the same Pod (it doesn't enforce Node capacity).
// Concurrent reconciles may briefly over-pack a Node by a handful of
// Pods before the cache catches up; this is the same window real K8s
// scheduler accepts (it does its own bookkeeping in-process — we use
// the controller-runtime cache as the equivalent).
func (b *podBinder) tryBind(ctx context.Context, pod *corev1.Pod) (bool, error) {
	var nodes corev1.NodeList
	if err := b.List(ctx, &nodes); err != nil {
		return false, fmt.Errorf("list nodes: %w", err)
	}
	podReq := sumPodRequests(pod)
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if !strings.HasPrefix(n.Name, fakeNodePrefix) {
			continue
		}
		if !podMatchesNodeLabels(pod, n.Labels) {
			continue
		}
		if !nodeFits(ctx, b.Client, n, podReq) {
			continue
		}
		// /binding is the scheduler-canonical path; the typed client
		// goes straight at the apiserver, no controller-runtime cache
		// between us and the truth.
		binding := &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			Target:     corev1.ObjectReference{Kind: "Node", Name: n.Name},
		}
		if err := b.clientset.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{}); err != nil {
			// Drop T: IsConflict means the Pod was already bound by
			// another reconcile (typically of the same Pod, fired by
			// a stale-cache Watches(Node) event). From the chain's
			// perspective the Pod IS bound — treat as success-by-other.
			if apierrors.IsConflict(err) {
				podBindAttempts.WithLabelValues("bound_by_other").Inc()
				return true, nil
			}
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
		if !n.CreationTimestamp.IsZero() {
			nodeToBoundLatency.Observe(time.Since(n.CreationTimestamp.Time).Seconds())
		}
		return true, nil
	}
	return false, nil
}

// sumPodRequests sums Requests across all containers in a Pod. Init
// containers are treated as "must fit" but not summed (matches the
// kube-scheduler resource model where init runs sequentially).
func sumPodRequests(pod *corev1.Pod) corev1.ResourceList {
	out := corev1.ResourceList{}
	for _, c := range pod.Spec.Containers {
		for k, v := range c.Resources.Requests {
			cur := out[k]
			cur.Add(v)
			out[k] = cur
		}
	}
	return out
}

// nodeFits returns true if Node.Status.Allocatable - Σ(bound Pods'
// Requests) covers `podReq`. Bound-Pod set is read from the cache via
// the spec.nodeName field index — O(Pods on this Node), not O(all Pods).
//
// Returns false on cache miss / list error: better to skip and let
// another candidate try than to over-pack a Node we can't measure.
func nodeFits(ctx context.Context, c client.Client, node *corev1.Node, podReq corev1.ResourceList) bool {
	alloc := node.Status.Allocatable
	if len(alloc) == 0 {
		// Pre-status-update window: a fake-Node may briefly have empty
		// Allocatable between Create and the Status.Update in the
		// upcomingNodeFakeNodeReconciler. Treat as not-yet-fittable.
		return false
	}
	var bound corev1.PodList
	if err := c.List(ctx, &bound, client.MatchingFields{fieldIndexPodNodeName: node.Name}); err != nil {
		return false
	}
	used := corev1.ResourceList{}
	for i := range bound.Items {
		for k, v := range sumPodRequests(&bound.Items[i]) {
			cur := used[k]
			cur.Add(v)
			used[k] = cur
		}
	}
	for k, want := range podReq {
		have := alloc[k]
		if u, ok := used[k]; ok {
			have = have.DeepCopy()
			have.Sub(u)
		}
		if have.Cmp(want) < 0 {
			return false
		}
	}
	return true
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
		// Drop AA: NotFound = UpcomingNode was deleted (operator deletes
		// on Drained terminus). Cascade-delete the fake-Node we created
		// from it. Without this path fake-Nodes accumulate across the
		// soak: the previous Reconcile bailed on NotFound via
		// IgnoreNotFound, leaving the Node permanent. At 30/sec churn
		// over 30 min × 50 clusters that's ~54 K stale fake-Nodes per
		// cluster, every one of which pod-binder's tryBind List/iterate
		// has to consider. Every claim Patch ages the cache further;
		// every Bind RPC scales with the apiserver's working set.
		if apierrors.IsNotFound(err) {
			nodeName := nodeNameFromUpcoming(req.Name)
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
			if dErr := r.Delete(ctx, node); dErr != nil && !apierrors.IsNotFound(dErr) {
				upcomingNodesObserved.WithLabelValues("cleanup_error").Inc()
				return reconcile.Result{}, fmt.Errorf("delete fake-Node on cascade: %w", dErr)
			}
			upcomingNodesObserved.WithLabelValues("cleanup_deleted").Inc()
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
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
	if !upn.CreationTimestamp.IsZero() {
		upcomingToNodeLatency.Observe(time.Since(upn.CreationTimestamp.Time).Seconds())
	}
	upcomingNodesObserved.WithLabelValues("created").Inc()
	return reconcile.Result{}, nil
}

// enqueueMatchingPendingPods is the Watches(Node) handler that wakes
// up pending Pods when a fresh fake-Node arrives. Filtered by label
// compatibility so each fresh fake-Node only enqueues Pods that can
// actually consider it — controller-runtime dedup handles overlap.
//
// ADR-0022 / M45.4: there's no more "claimed" filter. A density>1 Node
// hosts many Pods, so a Node Add event should wake every pending Pod
// whose nodeAffinity matches its labels, not just one. The per-Pod
// reconcile then bin-packs against actual Allocatable (see tryBind).
func enqueueMatchingPendingPods(ctx context.Context, c client.Client, obj client.Object) []reconcile.Request {
	if !strings.HasPrefix(obj.GetName(), fakeNodePrefix) {
		return nil
	}
	node, ok := obj.(*corev1.Node)
	if !ok {
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
