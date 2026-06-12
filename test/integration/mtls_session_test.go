//go:build integration

package integration_test

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
	"sigs.k8s.io/controller-runtime/pkg/client"
	kubefake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil/tlstest"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/operator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// TestMTLS_OperatorShardSessionEndToEnd: the full operator → shard
// Session flow — rollup, BootstrapRequest pull, Configure — over real
// mTLS (ADR-0048). Certificates are minted in-test from a throwaway
// CA (no checked-in keys); the operator's client cert carries
// bigfleet://cluster/<cluster_id> and the shard binds Hello.cluster_id
// to it. A second client with a different cluster's certificate is
// rejected with PermissionDenied — the impersonation vector M74
// closes.
func TestMTLS_OperatorShardSessionEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const clusterID = "mtls-cluster"
	ca := tlstest.NewCA(t)
	certDir := t.TempDir()

	// Shard with mTLS from CA-issued files (exercising the file-based
	// flag path, not just in-memory tls.Config).
	prov := providerfake.New(providerfake.Options{InstantTransitions: true})
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := shard.New(shard.Config{
		ID:               "shard-mtls",
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    50 * time.Millisecond,
		BootstrapTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	shardCert, shardKey, shardCA := ca.WriteFiles(t, filepath.Join(certDir, "shard"), tlstest.LeafOpts{
		URIs: []string{grpcutil.ShardURI("shard-mtls")},
		IPs:  []net.IP{net.ParseIP("127.0.0.1")},
	})
	serverOpts, err := grpcutil.TLSConfig{CertFile: shardCert, KeyFile: shardKey, CAFile: shardCA}.ServerOptions()
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}
	srv := grpc.NewServer(serverOpts...)
	pb.RegisterShardServer(srv, sh)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	go func() { _ = sh.Run(ctx) }()

	// Idle inventory for the bind.
	for i := 0; i < 2; i++ {
		prov.AddIdle(machine.ID("gpu-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: "a3-highgpu-8g",
				Zone:         "us-east-1a",
				Resources:    map[string]string{"nvidia.com/gpu": "8"},
			},
			machine.CapacityTypeBareMetal, 0, 0)
	}

	// Operator against a controller-runtime fake client, dialing with
	// its cluster certificate.
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	if err := bfv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("bfv1alpha1.AddToScheme: %v", err)
	}
	kc := kubefake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&bfv1alpha1.CapacityRequest{}, &bfv1alpha1.UpcomingNode{}, &bfv1alpha1.AvailableCapacity{}).
		Build()
	seedCapacityRequests(t, ctx, kc, 2)

	opCert, opKey, opCA := ca.WriteFiles(t, filepath.Join(certDir, "operator"), tlstest.LeafOpts{
		URIs: []string{grpcutil.ClusterURI(clusterID)},
	})
	op, err := operator.New(operator.Config{
		ClusterID:               machine.ClusterID(clusterID),
		ShardAddress:            lis.Addr().String(),
		TLS:                     grpcutil.TLSConfig{CertFile: opCert, KeyFile: opKey, CAFile: opCA},
		KubeClient:              kc,
		RollupInterval:          50 * time.Millisecond,
		ReconnectInitialBackoff: 20 * time.Millisecond,
		ReconnectMaxBackoff:     200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("operator.New: %v", err)
	}
	go func() { _ = op.Run(ctx) }()

	// The whole loop closes over mTLS: rollup in, Phase 1 decision,
	// BootstrapRequest pulled down the stream, blob returned,
	// provider Configure — 2 machines reach Configured.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if sh.Inventory().Snapshot().CountByState(machine.StateConfigured) == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := sh.Inventory().Snapshot().CountByState(machine.StateConfigured); got != 2 {
		t.Fatalf("Configured machines = %d; want 2 (mTLS session flow did not complete)", got)
	}

	// Impersonation: a certificate for another cluster asserting this
	// cluster's ID is terminated with PermissionDenied.
	rogueCert, rogueKey, rogueCA := ca.WriteFiles(t, filepath.Join(certDir, "rogue"), tlstest.LeafOpts{
		URIs: []string{grpcutil.ClusterURI("other-cluster")},
	})
	dialOpts, err := grpcutil.TLSConfig{CertFile: rogueCert, KeyFile: rogueKey, CAFile: rogueCA}.DialOptions()
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	conn, err := grpc.NewClient(lis.Addr().String(), dialOpts...)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()
	rogueCtx, rogueCancel := context.WithTimeout(ctx, 10*time.Second)
	defer rogueCancel()
	stream, err := pb.NewShardClient(conn).Session(rogueCtx)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if err := stream.Send(&pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Hello{Hello: &pb.Hello{ClusterId: clusterID}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("impersonating Recv = %v; want PermissionDenied", err)
	}
}

func seedCapacityRequests(t *testing.T, ctx context.Context, kc client.Client, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		cr := &bfv1alpha1.CapacityRequest{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "training",
				Name:      "trainer-" + strconv.Itoa(i),
			},
			Spec: bfv1alpha1.CapacityRequestSpec{
				Requirements: []corev1.NodeSelectorRequirement{{
					Key:      "node.kubernetes.io/instance-type",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"a3-highgpu-8g"},
				}},
				Resources: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("8"),
				},
				Priority: 1_000_000,
			},
		}
		if err := kc.Create(ctx, cr); err != nil {
			t.Fatalf("create CR: %v", err)
		}
	}
}
