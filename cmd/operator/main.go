// Command operator is the BigFleet per-cluster agent. Runs inside (or
// alongside) a Kubernetes cluster, dials a BigFleet shard, and holds a
// long-lived bidi Session stream that carries roll-ups, bootstrap
// blobs, reclaim instructions, and node-state updates.
//
// The agent never opens an inbound listener — outbound-only by design.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/operator"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "operator:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("operator", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	clusterID := fs.String("cluster-id", "", "stable identifier for this cluster (required)")
	shardAddr := fs.String("shard-addr", "", "host:port of the BigFleet shard's gRPC endpoint (required)")
	kubeconfig := fs.String("kubeconfig", "", "path to kubeconfig (default: in-cluster config or $KUBECONFIG)")
	metricsAddr := fs.String("metrics-addr", ":8770", "address for the Prometheus /metrics endpoint (\"0\" disables)")
	qps := fs.Float64("qps", 50, "client-go QPS budget for apiserver requests; raise for scale-test profiles whose apiserver can absorb more")
	burst := fs.Int("burst", 100, "client-go burst budget for apiserver requests")
	ackConcurrency := fs.Int("ack-concurrency", 16, "max parallel CR Pending→Acknowledged status writes per rollup")
	rollupInterval := fs.Duration("rollup-interval", 0, "interval between rollups; 0 means use the operator default (10s)")
	bootstrapTemplateFile := fs.String("bootstrap-template-file", "", "path to a Go text/template file for kubelet userdata. Context: .ClusterID, .Requirements ([]{Key, Operator, Values}). Empty → use the built-in stub renderer (M21).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *clusterID == "" {
		return errors.New("--cluster-id is required")
	}
	if *shardAddr == "" {
		return errors.New("--shard-addr is required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Info("signal received, shutting down")
		cancel()
	}()

	kc, err := newKubeClient(ctx, *kubeconfig, float32(*qps), *burst)
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}

	bootstrapRenderer, err := operator.NewFileTemplateRenderer(*bootstrapTemplateFile)
	if err != nil {
		return err
	}

	op, err := operator.New(operator.Config{
		ClusterID:              machine.ClusterID(*clusterID),
		ShardAddress:           *shardAddr,
		KubeClient:             kc,
		AcknowledgeConcurrency: *ackConcurrency,
		RollupInterval:         *rollupInterval,
		BootstrapTemplate:      bootstrapRenderer,
		Logger:                 logger,
	})
	if err != nil {
		return err
	}

	if *metricsAddr != "0" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		// pprof endpoints, mirroring the shard's surface
		// (cmd/bigfleet/shard.go). Used by scaletest diagnostics
		// to capture live CPU/heap/goroutine profiles when a
		// rollup-cost question can't be answered with the
		// Prometheus surface alone (bigfleet-uber #21).
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		metricsSrv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		logger.Info("metrics serving", "addr", *metricsAddr)
		go func() { _ = metricsSrv.ListenAndServe() }()
		defer func() { _ = metricsSrv.Shutdown(context.Background()) }()
	}

	if err := op.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// newKubeClient builds a cache-backed controller-runtime client wired
// to the bigfleet.lucy.sh scheme. Reads of CapacityRequest, Pod, and
// other watched resources are served from an in-process informer cache
// instead of hitting the apiserver on every list — this is the
// difference between a 2s rollup and a sub-100ms one once the cluster
// has thousands of CRs.
//
// Honours the standard kubeconfig discovery chain (--kubeconfig flag
// → $KUBECONFIG → in-cluster). The cache is started in a goroutine
// tied to ctx; the function blocks until the initial sync completes.
//
// qps/burst override the client-go rate limit. The default 50/100 is a
// safe production value; scale-test profiles raise it.
func newKubeClient(ctx context.Context, explicitPath string, qps float32, burst int) (client.Client, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if explicitPath != "" {
		rules.ExplicitPath = explicitPath
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	restCfg, err := cc.ClientConfig()
	if err != nil {
		return nil, err
	}
	// Default client-go QPS/Burst (5/10) is too low for the operator's
	// per-cycle status updates on Acknowledged transitions. Caller
	// passes a profile-aware budget; defaults are 50/100 (safe for
	// most apiservers' default flow-control).
	restCfg.QPS = qps
	restCfg.Burst = burst

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	c, err := cache.New(restCfg, cache.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("cache: %w", err)
	}
	go func() {
		if err := c.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "operator cache stopped:", err)
		}
	}()
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if !c.WaitForCacheSync(syncCtx) {
		return nil, errors.New("cache: initial sync timed out")
	}

	return client.New(restCfg, client.Options{
		Scheme: scheme,
		Cache:  &client.CacheOptions{Reader: c},
	})
}
