// Command bigfleet-unschedulable-pod-controller runs the optional
// per-pod CapacityRequest controller. Watches Pods; for each
// PodScheduled=False/reason=Unschedulable, creates a CapacityRequest
// owned by the pod (so deletion garbage-collects via ownerRef).
//
// This binary is *optional* — operators driving CRs from Kueue, an
// admission controller, or their own pipeline don't need it.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"sigs.k8s.io/yaml"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/controller/cr"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bigfleet-unschedulable-pod-controller:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("bigfleet-unschedulable-pod-controller", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	kubeconfig := fs.String("kubeconfig", "", "path to kubeconfig (default: in-cluster or $KUBECONFIG)")
	metricsAddr := fs.String("metrics-addr", ":8080", "Prometheus metrics listen address (\"0\" disables)")
	defaultsPath := fs.String("priority-class-defaults", "", "path to a YAML file mapping PriorityClass name → {interruptionPenalty, reclamationPenalty}. Used as fallback when a pod has no bigfleet.lucy.sh/{interruption,reclamation}-penalty annotation. Empty path → no defaults applied.")
	qps := fs.Float64("qps", 50, "client-go QPS budget for apiserver requests; raise for scale-test profiles whose apiserver can absorb more")
	burst := fs.Int("burst", 100, "client-go burst budget for apiserver requests")
	if err := fs.Parse(args); err != nil {
		return err
	}

	defaults, err := loadPriorityClassDefaults(*defaultsPath)
	if err != nil {
		return fmt.Errorf("priority-class-defaults: %w", err)
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
	// Bump QPS for similar reasons to the operator: status writes can
	// be bursty during large unschedulable-pod spikes. The default 50/100
	// is production-safe; scale-test profiles override (M44.4 — at 1000
	// Pods/cluster the per-cluster apiserver-write hop dominates the
	// chain's binding-latency p99 unless the budget is raised).
	restCfg.QPS = float32(*qps)
	restCfg.Burst = *burst

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	// Default cache-sync timeout (2 min) is too tight for scale-test
	// kwok apiservers under heavy ramp load — initial List/Watch
	// pagination over ~100K Pods can run past it. Manager exits, the
	// entrypoint supervisor cycles the container, the apiserver gets
	// hotter on retry: restart loop. 10 min covers the biggest profiles
	// and costs nothing when sync finishes fast.
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: *metricsAddr},
		Controller: config.Controller{
			CacheSyncTimeout: 10 * time.Minute,
		},
	})
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	if err := cr.AddToManager(mgr, cr.WithPriorityClassDefaults(defaults)); err != nil {
		return fmt.Errorf("add controller: %w", err)
	}

	// ctrl.SetupSignalHandler installs a process-wide signal handler and
	// must only be called once — a second call closes the already-closed
	// stop channel and panics. Cache the context so the error-path
	// errors.Is comparison reuses it instead of re-invoking the helper.
	ctx := ctrl.SetupSignalHandler()
	if err := mgr.Start(ctx); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

// loadPriorityClassDefaults reads the M16 PriorityClass → penalty
// fallback file. Format (YAML):
//
//	ml-research:
//	  interruptionPenalty: "8192"
//	  reclamationPenalty:  "65536"
//	batch-best-effort:
//	  interruptionPenalty: "16"
//	  reclamationPenalty:  "0"
//
// Penalties are parsed as Kubernetes resource.Quantity (decimal string).
// An empty path returns nil (no defaults applied — controller behaves
// as before M16, annotation-only).
func loadPriorityClassDefaults(path string) (map[string]cr.PriorityClassDefaults, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path) //nolint:gosec // path is operator-supplied at startup
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	type rawEntry struct {
		InterruptionPenalty string `json:"interruptionPenalty,omitempty"`
		ReclamationPenalty  string `json:"reclamationPenalty,omitempty"`
	}
	raw := map[string]rawEntry{}
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	out := make(map[string]cr.PriorityClassDefaults, len(raw))
	for name, e := range raw {
		var d cr.PriorityClassDefaults
		if e.InterruptionPenalty != "" {
			q, err := resource.ParseQuantity(e.InterruptionPenalty)
			if err != nil {
				return nil, fmt.Errorf("PriorityClass %q interruptionPenalty: %w", name, err)
			}
			d.InterruptionPenalty = &q
		}
		if e.ReclamationPenalty != "" {
			q, err := resource.ParseQuantity(e.ReclamationPenalty)
			if err != nil {
				return nil, fmt.Errorf("PriorityClass %q reclamationPenalty: %w", name, err)
			}
			d.ReclamationPenalty = &q
		}
		out[name] = d
	}
	return out, nil
}
