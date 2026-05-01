//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
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

// multiClusterEnv brings up N kind clusters in parallel + a single
// in-process BigFleet stack (one shard, optional coordinator) and one
// operator + CR controller per cluster. Used by the M8 e2e tests.
type multiClusterEnv struct {
	t        *testing.T
	ctx      context.Context
	cancel   context.CancelFunc
	scheme   *apiruntime.Scheme
	provider *providerfake.Provider
	shard    *shard.Shard
	shardSrv *grpc.Server
	clusters []*kindClusterRig
}

// kindClusterRig is one cluster's worth of state: kubeconfig path,
// kube client, controller-runtime manager, operator, and the cluster's
// own kind name (for cleanup).
type kindClusterRig struct {
	id          string // "cluster-train" / "cluster-batch" / etc — also used as ClusterID
	kindName    string
	kubeconfig  string
	kubeClient  client.Client
	mgr         ctrl.Manager
	operator    *operator.Operator
	operatorRun chan struct{}
}

// startMultiClusterEnv brings up the M8 stack. Cluster IDs become
// the BigFleet ClusterID values. The fake provider is seeded with
// gpu-X machines all matching the same instance-type label so any
// cluster can claim them.
func startMultiClusterEnv(t *testing.T, clusterIDs []string, idleGPUMachines int) *multiClusterEnv {
	t.Helper()
	if !commandExists("kind") {
		t.Skip("kind binary not on PATH")
	}
	if !commandExists("kubectl") {
		t.Skip("kubectl binary not on PATH")
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	// Provision kind clusters in parallel — saves ~40 s on a 3-cluster run.
	clusters := make([]*kindClusterRig, len(clusterIDs))
	var wg sync.WaitGroup
	for i, id := range clusterIDs {
		i, id := i, id
		wg.Add(1)
		go func() {
			defer wg.Done()
			kubeconfig, kindName := provisionKindCluster(t)
			installCRDs(t, kubeconfig)
			clusters[i] = &kindClusterRig{id: id, kindName: kindName, kubeconfig: kubeconfig}
		}()
	}
	wg.Wait()

	// Bring up the BigFleet shard once.
	prov := providerfake.New(providerfake.Options{InstantTransitions: true})
	for i := 0; i < idleGPUMachines; i++ {
		prov.AddIdle(
			machine.ID("gpu-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: "a3-highgpu-8g",
				Zone:         "us-east-1a",
				Resources:    map[string]string{"nvidia.com/gpu": "8"},
			},
			machine.CapacityTypeBareMetal, 0, 0,
		)
	}
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := shard.New(shard.Config{
		ID:               "shard-multi",
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
	t.Cleanup(srv.Stop)
	shardAddr := lis.Addr().String()

	// Per-cluster: kube client + manager + operator. Run them in goroutines.
	for _, rig := range clusters {
		restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: rig.kubeconfig},
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			t.Fatalf("kubeconfig %s: %v", rig.id, err)
		}
		restCfg.QPS = 200
		restCfg.Burst = 400

		kc, err := client.New(restCfg, client.Options{Scheme: scheme})
		if err != nil {
			t.Fatalf("kube client %s: %v", rig.id, err)
		}
		rig.kubeClient = kc

		mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
			Scheme:  scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
		})
		if err != nil {
			t.Fatalf("manager %s: %v", rig.id, err)
		}
		// Per-cluster controller name to avoid the metrics-registry
		// duplicate-name conflict (lessons §controller-runtime).
		if err := cr.AddToManager(mgr, cr.WithControllerName("bigfleet-cr-"+t.Name()+"-"+rig.id)); err != nil {
			t.Fatalf("add CR controller %s: %v", rig.id, err)
		}
		rig.mgr = mgr

		op, err := operator.New(operator.Config{
			ClusterID:               machine.ClusterID(rig.id),
			ShardAddress:            shardAddr,
			KubeClient:              kc,
			RollupInterval:          250 * time.Millisecond,
			ReconnectInitialBackoff: 100 * time.Millisecond,
			AcknowledgeConcurrency:  64,
		})
		if err != nil {
			t.Fatalf("operator %s: %v", rig.id, err)
		}
		rig.operator = op
		rig.operatorRun = make(chan struct{})
		go func(r *kindClusterRig) {
			defer close(r.operatorRun)
			_ = r.operator.Run(ctx)
		}(rig)
		go func(r *kindClusterRig) { _ = r.mgr.Start(ctx) }(rig)
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			t.Fatalf("cache sync %s failed", rig.id)
		}
	}

	return &multiClusterEnv{
		t:        t,
		ctx:      ctx,
		cancel:   cancel,
		scheme:   scheme,
		provider: prov,
		shard:    sh,
		shardSrv: srv,
		clusters: clusters,
	}
}

// rig returns the rig for the given cluster id.
func (e *multiClusterEnv) rig(id string) *kindClusterRig {
	for _, c := range e.clusters {
		if c.id == id {
			return c
		}
	}
	e.t.Fatalf("unknown cluster id %s", id)
	return nil
}

// createUnschedulablePodIn creates an unschedulable pod in the named
// cluster. Limits are set on nvidia.com/gpu since kind's apiserver
// rejects extended-resource requests without matching limits
// (lessons §kind / k8s).
func (e *multiClusterEnv) createUnschedulablePodIn(clusterID, podName string, gpu int64) {
	e.t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      podName,
		},
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{
				"node.kubernetes.io/instance-type": "a3-highgpu-8g",
			},
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{
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
	r := e.rig(clusterID)
	if err := r.kubeClient.Create(e.ctx, pod); err != nil {
		e.t.Fatalf("create pod %s in %s: %v", podName, clusterID, err)
	}
}

// addPriorityClassIn pre-installs a PriorityClass in the named cluster
// so pods can carry priority via PriorityClassName (the integer
// Priority field is rejected by kind's admission controller — see
// lessons §kind/k8s).
func (e *multiClusterEnv) addPriorityClassIn(clusterID, name string, value int32) {
	e.t.Helper()
	r := e.rig(clusterID)
	pc := struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata,omitempty"`
		Value             int32 `json:"value"`
	}{}
	pc.APIVersion = "scheduling.k8s.io/v1"
	pc.Kind = "PriorityClass"
	pc.Name = name
	pc.Value = value
	// Use unstructured-style raw apply via kubectl, since we don't
	// have scheduling/v1 in our scheme.
	tmp := filepath.Join(e.t.TempDir(), "priorityclass-"+clusterID+"-"+name+".yaml")
	if err := os.WriteFile(tmp, []byte(fmt.Sprintf(
		"apiVersion: scheduling.k8s.io/v1\nkind: PriorityClass\nmetadata:\n  name: %s\nvalue: %d\nglobalDefault: false\n",
		name, value)), 0o644); err != nil {
		e.t.Fatalf("write pc yaml: %v", err)
	}
	cmd := exec.Command("kubectl", "--kubeconfig", r.kubeconfig, "apply", "-f", tmp)
	if out, err := cmd.CombinedOutput(); err != nil {
		e.t.Fatalf("kubectl apply pc: %v\n%s", err, out)
	}
}

// createPodWithPriorityClassIn creates an unschedulable pod that
// references a PriorityClass by name (not the integer field).
func (e *multiClusterEnv) createPodWithPriorityClassIn(clusterID, podName, priorityClass string, gpu int64) {
	e.t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      podName,
		},
		Spec: corev1.PodSpec{
			PriorityClassName: priorityClass,
			NodeSelector: map[string]string{
				"node.kubernetes.io/instance-type": "a3-highgpu-8g",
			},
			Containers: []corev1.Container{{
				Name:  "main",
				Image: "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{
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
	r := e.rig(clusterID)
	if err := r.kubeClient.Create(e.ctx, pod); err != nil {
		e.t.Fatalf("create pod %s in %s: %v", podName, clusterID, err)
	}
}

// waitFor polls cond every 200 ms until it returns true or timeout
// elapses; fails the test on timeout.
func (e *multiClusterEnv) waitFor(timeout time.Duration, cond func() bool, msg string) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		select {
		case <-e.ctx.Done():
			e.t.Fatalf("ctx cancelled while waiting: %s", msg)
		case <-time.After(200 * time.Millisecond):
		}
	}
	e.t.Fatalf("timeout: %s", msg)
}
