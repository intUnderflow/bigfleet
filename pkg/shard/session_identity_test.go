package shard_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil/tlstest"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// ADR-0048 Session identity binding: under mTLS the shard verifies
// that the operator's client certificate carries the URI SAN
// bigfleet://cluster/<Hello.cluster_id>. Match = session proceeds;
// mismatch = PermissionDenied + counter. Plaintext (every other test
// in this package) skips the check.

// startMTLSShard brings up a real Shard.Session gRPC server with mTLS
// from CA-issued files and returns its address.
func startMTLSShard(t *testing.T, ca *tlstest.CA) string {
	t.Helper()
	prov := fake.New(fake.Options{InstantTransitions: true})
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := shard.New(shard.Config{ID: "shard-mtls", Epoch: epoch, Provider: prov})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}

	cert, key, caPath := ca.WriteFiles(t, filepath.Join(t.TempDir(), "shard"), tlstest.LeafOpts{
		URIs: []string{grpcutil.ShardURI("shard-mtls")},
		IPs:  []net.IP{net.ParseIP("127.0.0.1")},
	})
	serverOpts, err := grpcutil.TLSConfig{CertFile: cert, KeyFile: key, CAFile: caPath}.ServerOptions()
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
	return lis.Addr().String()
}

// openSession dials with the given client cert URI SAN, sends a Hello
// asserting helloCluster, and returns the result of the first Recv.
func openSession(t *testing.T, ca *tlstest.CA, addr, certClusterURI, helloCluster string) (*pb.ShardMessage, error) {
	t.Helper()
	cert, key, caPath := ca.WriteFiles(t, t.TempDir(), tlstest.LeafOpts{URIs: []string{certClusterURI}})
	dialOpts, err := grpcutil.TLSConfig{CertFile: cert, KeyFile: key, CAFile: caPath}.DialOptions()
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	stream, err := pb.NewShardClient(conn).Session(ctx)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if err := stream.Send(&pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Hello{Hello: &pb.Hello{
			ClusterId: helloCluster, ProtocolVersion: "v1alpha1",
		}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	return stream.Recv()
}

func TestSessionIdentity_MatchingSANAccepted(t *testing.T) {
	ca := tlstest.NewCA(t)
	addr := startMTLSShard(t, ca)

	msg, err := openSession(t, ca, addr, grpcutil.ClusterURI("cluster-a"), "cluster-a")
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if msg.GetAck().GetEcho() != "hello" {
		t.Fatalf("expected hello ack, got %v", msg)
	}
}

func TestSessionIdentity_MismatchedClusterRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	addr := startMTLSShard(t, ca)

	before := testutil.ToFloat64(metrics.ShardSessionIdentityRejected)
	// The certificate says cluster-a; the Hello claims cluster-b —
	// the forged-roll-up / stolen-reclaim impersonation vector.
	_, err := openSession(t, ca, addr, grpcutil.ClusterURI("cluster-a"), "cluster-b")
	if err == nil {
		t.Fatal("session with mismatched cluster identity: want PermissionDenied, got ack")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("status = %v; want PermissionDenied", err)
	}
	after := testutil.ToFloat64(metrics.ShardSessionIdentityRejected)
	if after <= before {
		t.Fatalf("identity-rejected counter did not increase (before=%v after=%v)", before, after)
	}
}

func TestSessionIdentity_NoURISANRejected(t *testing.T) {
	ca := tlstest.NewCA(t)
	addr := startMTLSShard(t, ca)

	// Valid certificate, but no bigfleet:// URI SAN at all: the
	// caller authenticates yet asserts no cluster identity. Exactly
	// one is the rule.
	cert, key, caPath := ca.WriteFiles(t, t.TempDir(), tlstest.LeafOpts{})
	dialOpts, err := grpcutil.TLSConfig{CertFile: cert, KeyFile: key, CAFile: caPath}.DialOptions()
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := pb.NewShardClient(conn).Session(ctx)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if err := stream.Send(&pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Hello{Hello: &pb.Hello{ClusterId: "cluster-a"}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Recv = %v; want PermissionDenied", err)
	}
}
