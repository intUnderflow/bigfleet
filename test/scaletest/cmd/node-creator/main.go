// Command bigfleet-scaletest-node-creator is the scale-test harness
// counterpart to a cloud provisioner: it watches `UpcomingNode` CRs
// published by the bigfleet operator and creates corresponding fake
// Kubernetes Nodes (with the Allocatable derived per ADR-0022) so a
// real kube-scheduler can place Pods on them. It is the harness side
// of "a Node now exists in the cluster" — nothing more.
//
// Per ADR-0023, this binary replaces the UpcomingNode→fake-Node half
// of bigfleet-scaletest-pod-shim. The Pod-marking-Unschedulable and
// Pod-binding halves of pod-shim are taken over by a real kube-
// scheduler running against the same apiserver. The two are deployed
// in parallel during the transition; once the new flow is validated,
// pod-shim is retired entirely.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

// fakeNodePrefix is the substring `kubectl get nodes` shows to flag
// scaletest-created Nodes. Kept identical to pod-shim's prefix so the
// retired-vs-new transition is invisible to tooling that filters on it.
const fakeNodePrefix = "fake-"

var (
	upcomingNodesObserved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_node_creator_upcoming_nodes_observed_total",
		Help: "Count of UpcomingNode reconcile events seen by node-creator, labelled by outcome. Compare to shard's bigfleet_shard_actions_total{kind=Bootstrap} to find shard→cluster propagation gaps.",
	}, []string{"outcome"})
	fakeNodesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_scaletest_node_creator_fake_nodes_created_total",
		Help: "Count of fake Node objects this node-creator has created from UpcomingNodes.",
	})
	// upcomingToNodeLatency measures the harness-side propagation
	// from operator-published UpcomingNode to Kubernetes-visible Node.
	// In the ADR-0023 split this is the only chain stage the
	// node-creator owns; everything downstream is kube-scheduler.
	upcomingToNodeLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "bigfleet_scaletest_node_creator_upcoming_to_node_latency_seconds",
		Help:    "Wall-clock from UpcomingNode CR creation to fake-Node Create succeeding. A flat distribution means node-creator is keeping up; a long tail means controller-runtime queueing or apiserver write contention.",
		Buckets: []float64{0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6, 51.2, 102.4},
	})
)

func init() {
	ctrlmetrics.Registry.MustRegister(upcomingNodesObserved, fakeNodesCreated, upcomingToNodeLatency)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bigfleet-scaletest-node-creator:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("bigfleet-scaletest-node-creator", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	kubeconfig := fs.String("kubeconfig", "", "path to kubeconfig (default: in-cluster or $KUBECONFIG)")
	metricsAddr := fs.String("metrics-addr", ":8775", "Prometheus metrics listen address (\"0\" disables)")
	qps := fs.Float64("qps", 200, "client-go QPS budget. Node creation is one Create + one Status Update per UpcomingNode; bursts size with churn rate.")
	burst := fs.Int("burst", 400, "client-go burst budget.")
	concurrency := fs.Int("concurrency", 8, "MaxConcurrentReconciles for the UpcomingNode→fake-Node controller. Each reconcile is O(1) (one Get + one Create). 8 matches pod-shim's default; raise for profiles emitting machines faster than ~50/sec/cluster.")
	pprofAddr := fs.String("pprof-addr", "", "net/http/pprof listen address (e.g. \":8776\"). Empty disables.")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))

	// Pprof on a separate listener for live profiling during scale
	// tests. Disabled by default; enabled via --pprof-addr.
	if *pprofAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		srv := &http.Server{Addr: *pprofAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.ListenAndServe() }()
	}

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if *kubeconfig != "" {
		rules.ExplicitPath = *kubeconfig
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	restCfg, err := cc.ClientConfig()
	if err != nil {
		return err
	}
	restCfg.QPS = float32(*qps)
	restCfg.Burst = *burst

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: *metricsAddr},
		// 10m CacheSyncTimeout (vs controller-runtime's 2m default) for
		// the same reason pod-shim and UPC use it (see ADR-0023 / M45.5
		// lessons): at large per-cluster object counts, initial
		// List/Watch pagination can run past the default.
		Controller: config.Controller{
			CacheSyncTimeout: 10 * time.Minute,
		},
	})
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}

	fn := &upcomingNodeReconciler{Client: mgr.GetClient()}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&bfv1alpha1.UpcomingNode{}).
		Named("bigfleet-scaletest-node-creator").
		WithOptions(controller.Options{MaxConcurrentReconciles: *concurrency}).
		Complete(fn); err != nil {
		return fmt.Errorf("upcoming-node controller: %w", err)
	}

	// ctrl.SetupSignalHandler installs a process-wide handler and
	// panics with "close of closed channel" if invoked twice. Cache
	// the context (M45.5 lesson; ADR-0023 brings this forward).
	ctx := ctrl.SetupSignalHandler()
	if err := mgr.Start(ctx); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

// upcomingNodeReconciler is the ADR-0023 node-creator. It mirrors
// pod-shim's `upcomingNodeFakeNodeReconciler` exactly, less anything
// to do with Pods. Kept structurally identical so behavioural
// equivalence between the two flows is easy to reason about during
// the transition.
type upcomingNodeReconciler struct {
	client.Client
}

func (r *upcomingNodeReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var upn bfv1alpha1.UpcomingNode
	if err := r.Get(ctx, req.NamespacedName, &upn); err != nil {
		// NotFound = UpcomingNode was deleted (operator deletes on the
		// Drained terminus). Cascade-delete the fake-Node we created
		// from it; without this, fake-Nodes accumulate across the soak
		// and weigh down kube-scheduler's Node informer. Same rationale
		// as pod-shim's Drop AA.
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

	// Inject `pods` capacity. UpcomingNode.Spec.Resources carries
	// cpu+memory+gpu but does not include the `pods` resource (that's
	// a kubelet-set field, not a provisioning concept). Real
	// kube-scheduler's NodeResourcesFit plugin treats absent
	// `allocatable.pods` as zero, which means 100% of fits fail with
	// "Too many pods". Pod-shim's old /binding path bypassed the
	// fit check; the new real-scheduler path doesn't, so we must
	// surface a pods capacity here.
	//
	// 1100 chosen to leave headroom over ADR-0022's density-100 model
	// (10× slack) without going absurdly high. Production-realistic
	// Nodes usually carry `pods: "110"`; the bigger value here is
	// purely so the harness doesn't gate before BigFleet does.
	resources := corev1.ResourceList{}
	for k, v := range upn.Spec.Resources {
		resources[k] = v
	}
	if _, ok := resources[corev1.ResourcePods]; !ok {
		resources[corev1.ResourcePods] = resource.MustParse("1100")
	}
	fakeNodesCreated.Inc()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: cloneLabels(upn.Spec.Labels)},
		Spec: corev1.NodeSpec{
			Taints: append([]corev1.Taint(nil), upn.Spec.Taints...),
		},
		Status: corev1.NodeStatus{
			Capacity:    resources,
			Allocatable: resources,
			Conditions: []corev1.NodeCondition{{
				Type:    corev1.NodeReady,
				Status:  corev1.ConditionTrue,
				Reason:  "KubeletReady",
				Message: "bigfleet-scaletest-node-creator: fake Node provisioned by BigFleet",
			}},
		},
	}
	if err := r.Create(ctx, node); err != nil && !apierrors.IsAlreadyExists(err) {
		return reconcile.Result{}, fmt.Errorf("create node: %w", err)
	}
	if node.ResourceVersion != "" {
		statusPatch := node.DeepCopy()
		statusPatch.Status = corev1.NodeStatus{
			Capacity:    resources,
			Allocatable: resources,
			Conditions: []corev1.NodeCondition{{
				Type:    corev1.NodeReady,
				Status:  corev1.ConditionTrue,
				Reason:  "KubeletReady",
				Message: "bigfleet-scaletest-node-creator: fake Node provisioned by BigFleet",
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

// nodeNameFromUpcoming maps "un-{machineID}" → "fake-{machineID}". The
// "fake-" prefix flags this as a test artefact in `kubectl get nodes`.
// Kept identical to pod-shim's mapping so a mixed run with both
// binaries deployed produces the same Node names from the same
// UpcomingNodes.
func nodeNameFromUpcoming(upcomingName string) string {
	return fakeNodePrefix + strings.TrimPrefix(upcomingName, "un-")
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
