//go:build e2e

// Package e2e_test holds end-to-end tests that drive a real kind
// cluster + the full BigFleet pipeline (CR controller → operator →
// shard → fake provider → UpcomingNode CRs).
//
// Build tag `e2e`. Requires `kind` and `kubectl` on PATH and Docker
// Desktop running. The harness creates a fresh kind cluster per test
// run (or reuses one if BIGFLEET_E2E_KIND_REUSE=1) and tears it down
// at the end.
package e2e_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"google.golang.org/grpc"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/controller/cr"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/operator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// e2eEnv is the shared per-test environment: a kind cluster with CRDs
// installed, plus all four BigFleet components running in-process.
type e2eEnv struct {
	t           *testing.T
	ctx         context.Context
	cancel      context.CancelFunc
	scheme      *apiruntime.Scheme
	kubeconfig  string
	kc          client.Client
	kindCluster string
	provider    *providerfake.Provider
	shard       *shard.Shard
	shardSrv    *grpc.Server
	shardAddr   string
	mgr         ctrl.Manager
}

func startE2EEnv(t *testing.T) *e2eEnv {
	t.Helper()
	if !commandExists("kind") {
		t.Skip("kind binary not on PATH")
	}
	if !commandExists("kubectl") {
		t.Skip("kubectl binary not on PATH")
	}

	// Wire controller-runtime's logger so the CR controller's
	// reconcile messages surface in test output. Idempotent if called
	// multiple times across tests.
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	kubeconfig, kindCluster := provisionKindCluster(t)
	installCRDs(t, kubeconfig)

	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}
	restCfg.QPS = 200
	restCfg.Burst = 400

	kc, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"}, // disabled for tests
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	// Each e2e test runs in the same process, but the
	// controller-runtime metrics registry rejects duplicate
	// controller names. Suffix per-test so tests can run in series.
	if err := cr.AddToManager(mgr, cr.WithControllerName("bigfleet-cr-"+t.Name())); err != nil {
		t.Fatalf("add CR controller: %v", err)
	}

	prov := providerfake.New(providerfake.Options{InstantTransitions: true})
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := shard.New(shard.Config{
		ID:               "shard-e2e",
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    100 * time.Millisecond,
		BootstrapTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterShardServer(srv, sh)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	go func() { _ = sh.Run(ctx) }()

	op, err := operator.New(operator.Config{
		ClusterID:               "cluster-e2e",
		ShardAddress:            lis.Addr().String(),
		KubeClient:              kc,
		RollupInterval:          250 * time.Millisecond,
		ReconnectInitialBackoff: 100 * time.Millisecond,
		AcknowledgeConcurrency:  64,
	})
	if err != nil {
		t.Fatalf("operator.New: %v", err)
	}
	go func() { _ = op.Run(ctx) }()
	go func() { _ = mgr.Start(ctx) }()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("manager cache failed to sync")
	}

	t.Cleanup(func() { srv.Stop() })

	return &e2eEnv{
		t:           t,
		ctx:         ctx,
		cancel:      cancel,
		scheme:      scheme,
		kubeconfig:  kubeconfig,
		kc:          kc,
		kindCluster: kindCluster,
		provider:    prov,
		shard:       sh,
		shardSrv:    srv,
		shardAddr:   lis.Addr().String(),
		mgr:         mgr,
	}
}

func commandExists(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func provisionKindCluster(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("BIGFLEET_E2E_KIND_REUSE") == "1" {
		kc := os.Getenv("KUBECONFIG")
		if kc == "" {
			home, _ := os.UserHomeDir()
			kc = filepath.Join(home, ".kube", "config")
		}
		return kc, "<reused>"
	}
	name := fmt.Sprintf("bigfleet-e2e-%d", time.Now().UnixNano())
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	t.Logf("creating kind cluster %s", name)
	cmd := exec.Command("kind", "create", "cluster", "--name", name, "--kubeconfig", kubeconfig, "--wait", "2m")
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		t.Fatalf("kind create: %v", err)
	}
	t.Cleanup(func() {
		t.Logf("deleting kind cluster %s", name)
		_ = exec.Command("kind", "delete", "cluster", "--name", name).Run()
	})
	return kubeconfig, name
}

func installCRDs(t *testing.T, kubeconfig string) {
	t.Helper()
	root := repoRoot(t)
	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfig, "apply", "-f", filepath.Join(root, "api", "crd"))
	cmd.Stdout = testWriter{t}
	cmd.Stderr = testWriter{t}
	if err := cmd.Run(); err != nil {
		t.Fatalf("install CRDs: %v", err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not resolve harness file path")
	}
	// test/e2e/harness.go → ../..
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

func (e *e2eEnv) addIdleMachines(prefix string, count int, instanceType string, gpu int64) {
	e.t.Helper()
	for i := 0; i < count; i++ {
		profile := machine.Profile{
			InstanceType: instanceType,
			Zone:         "us-east-1a",
			Resources:    map[string]string{"nvidia.com/gpu": strconv.FormatInt(gpu, 10)},
		}
		e.provider.AddIdle(machine.ID(prefix+"-"+strconv.Itoa(i)),
			profile, machine.CapacityTypeBareMetal, 0, 0)
	}
}

// createUnschedulablePod creates a Pod that the kind scheduler will mark
// Unschedulable (it asks for a node label no node has). The CR
// controller picks it up and creates a CapacityRequest.
// Note: kind enables the priority admission controller, which rejects
// pods with an explicit Priority integer in the spec (you must reference
// a PriorityClass by name). For test simplicity we ignore the priority
// arg — the resulting CR has priority 0, which is fine since the tests
// don't depend on priority ordering.
func (e *e2eEnv) createUnschedulablePod(name string, gpu int64, priority int32) {
	e.t.Helper()
	_ = priority
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				"node.kubernetes.io/instance-type": "a3-highgpu-8g",
			},
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{
					// Only nvidia.com/gpu is requested. The kind cluster
					// has no node with this extended resource, so the
					// pod is marked Unschedulable, the CR controller
					// creates a CR, and the shard's MatchProfile
					// receives a need with exactly the resources the
					// fake provider seeded onto its idle machines.
					Requests: corev1.ResourceList{
						"nvidia.com/gpu": *resource.NewQuantity(gpu, resource.DecimalSI),
					},
					Limits: corev1.ResourceList{
						"nvidia.com/gpu": *resource.NewQuantity(gpu, resource.DecimalSI),
					},
				},
			}},
		},
	}
	if err := e.kc.Create(e.ctx, pod); err != nil {
		e.t.Fatalf("create pod: %v", err)
	}
}

// waitFor polls cond every 100 ms until it returns true or timeout
// elapses, then fails.
func (e *e2eEnv) waitFor(timeout time.Duration, cond func() bool, msg string) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		select {
		case <-e.ctx.Done():
			e.t.Fatalf("ctx cancelled while waiting: %s", msg)
		case <-time.After(100 * time.Millisecond):
		}
	}
	e.t.Fatalf("timeout: %s", msg)
}
