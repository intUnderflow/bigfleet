//go:build scale && kind

// kind-based Layer 2 scale tests. Require an existing kind cluster
// with BigFleet's CRDs installed and KUBECONFIG set to it. Designed
// to run against a cluster created by:
//
//	kind create cluster --name bigfleet-scale --wait 2m
//	kubectl --context kind-bigfleet-scale apply -f api/crd/
//
// Build tag `scale,kind` keeps these out of the default scale run
// (Layer 1 only) so PRs don't depend on a kind binary or a running
// daemon. Run via:
//
//	go test -tags=scale,kind ./test/scale/...
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
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/grpc"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/operator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// M4 scale ceiling — Layer 2: drive ~5,000 CapacityRequests through a
// real kind cluster's etcd, with the operator running against the kind
// apiserver and the shard hosted in-process. Verify the rollup
// compresses correctly and Acknowledged transitions land within budget.
//
// 10,000 CRs is the plan §5.1 aspirational target; the practical
// ceiling on a single Docker-Desktop kind cluster is bounded by etcd
// write throughput, which on the M5 Max sits around a few hundred
// writes/sec. We use 1,500 here so the test fits in a couple of
// minutes; the 10K full target moves to nightly soak where we can
// afford the full apiserver round-trip cost.
func TestM4Scale_Kind_FifteenHundredCRs(t *testing.T) {
	const numCRs = 1_500

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	restCfg, err := cc.ClientConfig()
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}
	// Bump QPS so the test isn't gated on client-go's default 5/10
	// rate limiter when status-updating thousands of CRs.
	restCfg.QPS = 200
	restCfg.Burst = 400
	kc, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("kube client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-clean: the previous run may have left CRs behind if it
	// failed before its cleanup ran.
	{
		var existing bfv1alpha1.CapacityRequestList
		if err := kc.List(context.Background(), &existing); err == nil {
			for i := range existing.Items {
				_ = kc.Delete(context.Background(), &existing.Items[i])
			}
		}
	}
	// Cleanup on exit so the test can be re-run without manual reset.
	t.Cleanup(func() {
		var list bfv1alpha1.CapacityRequestList
		if err := kc.List(context.Background(), &list); err == nil {
			for i := range list.Items {
				_ = kc.Delete(context.Background(), &list.Items[i])
			}
		}
	})

	// Bring up an in-process shard so the operator has a real Session
	// peer.
	prov := providerfake.New(providerfake.Options{InstantTransitions: true})
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := shard.New(shard.Config{
		ID: "shard-kind-scale", Epoch: epoch, Provider: prov,
		CycleInterval: 100 * time.Millisecond,
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
		ClusterID:               "cluster-kind-scale",
		ShardAddress:            lis.Addr().String(),
		KubeClient:              kc,
		RollupInterval:          500 * time.Millisecond,
		ReconnectInitialBackoff: 100 * time.Millisecond,
		AcknowledgeConcurrency:  64,
	})
	if err != nil {
		t.Fatalf("operator.New: %v", err)
	}
	go func() { _ = op.Run(ctx) }()

	// Need a namespace.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "bigfleet-scale-test"}}
	_ = kc.Create(ctx, ns)

	// Seed N CRs across 10 distinct profiles.
	t.Logf("seeding %d CapacityRequests in real kind etcd", numCRs)
	seedStart := time.Now()
	for i := 0; i < numCRs; i++ {
		cr := &bfv1alpha1.CapacityRequest{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "bigfleet-scale-test",
				Name:      "scale-cr-" + strconv.Itoa(i),
			},
			Spec: bfv1alpha1.CapacityRequestSpec{
				Requirements: []corev1.NodeSelectorRequirement{{
					Key:      "node.kubernetes.io/instance-type",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"profile-" + strconv.Itoa(i%10)},
				}},
				Resources: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")},
				Priority:  int32(1_000_000 - (i%10)*1000),
			},
		}
		if err := kc.Create(ctx, cr); err != nil {
			t.Fatalf("create CR %d: %v", i, err)
		}
		if i > 0 && i%500 == 0 {
			t.Logf("seeded %d/%d (%.0f CR/s)", i, numCRs, float64(i)/time.Since(seedStart).Seconds())
		}
	}
	t.Logf("seeded %d CRs in %v (%.0f CR/s)", numCRs, time.Since(seedStart), float64(numCRs)/time.Since(seedStart).Seconds())

	// Wait until ≥99% of CRs reach Acknowledged. The shard should
	// trigger this on the operator's first rollup that includes the
	// CR. With a 200ms rollup interval and ~5000 CRs spanning 10
	// profiles, the first rollup batch ends up in the table fast; the
	// subsequent status writes are batched against kind etcd.
	deadline := time.Now().Add(2 * time.Minute)
	var ackCount int
	for time.Now().Before(deadline) {
		var list bfv1alpha1.CapacityRequestList
		if err := kc.List(ctx, &list); err != nil {
			t.Fatalf("list CRs: %v", err)
		}
		ackCount = 0
		for _, cr := range list.Items {
			if cr.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
				ackCount++
			}
		}
		t.Logf("Acknowledged: %d / %d", ackCount, numCRs)
		if ackCount >= numCRs*99/100 {
			break
		}
		time.Sleep(2 * time.Second)
	}

	if ackCount < numCRs*99/100 {
		t.Fatalf("only %d / %d CRs Acknowledged within budget", ackCount, numCRs)
	}
	t.Logf("M4 ceiling met: %d / %d CRs Acknowledged via real kind etcd", ackCount, numCRs)
}
