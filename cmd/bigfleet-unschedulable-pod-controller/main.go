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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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
	// Bump QPS for similar reasons to the operator: status writes can
	// be bursty during large unschedulable-pod spikes.
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
	if err := cr.AddToManager(mgr); err != nil {
		return fmt.Errorf("add controller: %w", err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil && !errors.Is(err, ctrl.SetupSignalHandler().Err()) {
		return err
	}
	return nil
}
