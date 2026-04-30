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
	"os"
	"os/signal"
	"syscall"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
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

	kc, err := newKubeClient(*kubeconfig)
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}

	op, err := operator.New(operator.Config{
		ClusterID:    machine.ClusterID(*clusterID),
		ShardAddress: *shardAddr,
		KubeClient:   kc,
		Logger:       logger,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Info("signal received, shutting down")
		cancel()
	}()

	if err := op.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// newKubeClient builds a controller-runtime client wired to the
// bigfleet.lucy.sh scheme. Honours the standard kubeconfig discovery
// chain (--kubeconfig flag → $KUBECONFIG → in-cluster).
func newKubeClient(explicitPath string) (client.Client, error) {
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
	// per-cycle status updates on Acknowledged transitions when a
	// cluster has thousands of CRs. Raise to a level kubebuilder uses
	// for controllers (50/100) — well within most apiservers' default
	// flow-control budget.
	restCfg.QPS = 50
	restCfg.Burst = 100

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	return client.New(restCfg, client.Options{Scheme: scheme})
}
