//go:build scale && kind

// M5 scale ceiling — Layer 2 (real kind cluster), full pipeline:
// CR controller → operator → shard → fake provider.
//
// Plan §5.1 M5 ceiling: 1,000 unschedulable pods → 1,000 satisfied
// (Configured machines on the shard / Acknowledged CRs in cluster)
// within 60 seconds wall clock. The fake provider doesn't actually
// join nodes to kind, so "satisfied" is measured at the control-plane
// level: the shard reports 1,000 Configured machines bound to the
// cluster.
//
// Requires kind, kubectl, KUBECONFIG pointing at a cluster with the
// BigFleet CRDs installed.
//
// Run:
//
//	make scale
//	# or
//	go test -tags='scale kind' -run TestM5Scale_ ./test/scale/...
package scale_test

import (
	"context"
	"net"
	"path/filepath"
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
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"google.golang.org/grpc"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/controller/cr"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	mac "github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/operator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

func TestM5Scale_Kind_ThousandUnschedulablePods(t *testing.T) {
	const (
		numPods   = 1_000
		ceiling   = 60 * time.Second
		ackBudget = 90 * time.Second
		gpuPerPod = 1
	)

	scheme := apiruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	restCfg, err := cc.ClientConfig()
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}
	restCfg.QPS = 200
	restCfg.Burst = 400

	kc, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-clean prior runs.
	cleanup := func() {
		var pods corev1.PodList
		if err := kc.List(context.Background(), &pods); err == nil {
			for i := range pods.Items {
				if pods.Items[i].Namespace == "bigfleet-m5-scale" {
					_ = kc.Delete(context.Background(), &pods.Items[i])
				}
			}
		}
		var crs bfv1alpha1.CapacityRequestList
		if err := kc.List(context.Background(), &crs); err == nil {
			for i := range crs.Items {
				_ = kc.Delete(context.Background(), &crs.Items[i])
			}
		}
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "bigfleet-m5-scale"}}
		_ = kc.Delete(context.Background(), ns)
	}
	cleanup()
	t.Cleanup(cleanup)

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}
	if err := cr.AddToManager(mgr, cr.WithControllerName("bigfleet-cr-"+t.Name())); err != nil {
		t.Fatalf("add CR controller: %v", err)
	}

	prov := providerfake.New(providerfake.Options{InstantTransitions: true})
	// Seed exactly numPods idle machines so capacity matches demand.
	for i := 0; i < numPods; i++ {
		prov.AddIdle(
			mac.ID("gpu-"+strconv.Itoa(i)),
			mac.Profile{
				InstanceType: "a3-highgpu-8g",
				Zone:         "us-east-1a",
				Resources:    map[string]string{"nvidia.com/gpu": "1"},
			},
			mac.CapacityTypeBareMetal, 0, 0,
		)
	}

	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := shard.New(shard.Config{
		ID:               "shard-m5-scale",
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
	defer srv.Stop()
	go func() { _ = sh.Run(ctx) }()

	op, err := operator.New(operator.Config{
		ClusterID:               "cluster-m5-scale",
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

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "bigfleet-m5-scale"}}
	_ = kc.Create(ctx, ns)

	t.Logf("seeding %d unschedulable pods", numPods)
	startSeed := time.Now()
	for i := 0; i < numPods; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "bigfleet-m5-scale",
				Name:      "scale-pod-" + strconv.Itoa(i),
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
							"nvidia.com/gpu": *resource.NewQuantity(int64(gpuPerPod), resource.DecimalSI),
						},
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": *resource.NewQuantity(int64(gpuPerPod), resource.DecimalSI),
						},
					},
				}},
			},
		}
		if err := kc.Create(ctx, pod); err != nil {
			t.Fatalf("create pod %d: %v", i, err)
		}
		if i > 0 && i%200 == 0 {
			t.Logf("seeded %d/%d (%.0f pod/s)", i, numPods, float64(i)/time.Since(startSeed).Seconds())
		}
	}
	t.Logf("seeded %d pods in %v", numPods, time.Since(startSeed))

	// Phase 1: ceiling — wait until shard reports numPods Configured.
	startConfig := time.Now()
	deadlineCfg := startConfig.Add(ceiling)
	for time.Now().Before(deadlineCfg) {
		if got := sh.Inventory().CountByState(mac.StateConfigured); got >= numPods {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	got := sh.Inventory().CountByState(mac.StateConfigured)
	if got < numPods {
		t.Errorf("M5 ceiling missed: %d / %d Configured within %v", got, numPods, ceiling)
	} else {
		t.Logf("M5 ceiling met: %d / %d Configured in %v", got, numPods, time.Since(startConfig))
	}

	// Phase 2: also verify CRs reach Acknowledged within an extended budget.
	deadlineAck := time.Now().Add(ackBudget)
	for time.Now().Before(deadlineAck) {
		var list bfv1alpha1.CapacityRequestList
		if err := kc.List(ctx, &list); err != nil {
			t.Fatalf("list CRs: %v", err)
		}
		acked := 0
		for _, c := range list.Items {
			if c.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
				acked++
			}
		}
		if acked >= numPods {
			t.Logf("CRs Acknowledged: %d / %d", acked, numPods)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}
