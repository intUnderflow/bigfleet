package coordinator_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil/tlstest"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// ADR-0048 + ADR-0060 coordinator SAN binding: under mTLS, ReportShard binds
// the asserted shard_id to bigfleet://shard/<shard_id>; the MUTATING surface
// (AssignDomain / UnassignDomain / RemoveShard / JoinRaftCluster /
// SnapshotSave) requires bigfleet://admin; and the READ surface (ListShards /
// ListDomainAssignments / ListShardReports) accepts bigfleet://readonly OR
// bigfleet://admin (the read-only role can read but never mutate the fleet).

// startMTLSCoordinator brings up a single-node leader with an mTLS
// gRPC front and returns a client-builder keyed by certificate URI SAN.
func startMTLSCoordinator(t *testing.T) (dial func(sanURIs ...string) pb.CoordinatorClient) {
	t.Helper()
	c, err := coordinator.New(coordinator.Config{
		NodeID:          "node-1",
		DataDir:         filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress: freePort(t),
		Bootstrap:       true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Close)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := c.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	ca := tlstest.NewCA(t)
	cert, key, caPath := ca.WriteFiles(t, filepath.Join(t.TempDir(), "server"), tlstest.LeafOpts{
		URIs: []string{grpcutil.AdminURI},
		IPs:  []net.IP{net.ParseIP("127.0.0.1")},
	})
	serverOpts, err := grpcutil.TLSConfig{CertFile: cert, KeyFile: key, CAFile: caPath}.ServerOptions()
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}
	gsrv := grpc.NewServer(serverOpts...)
	pb.RegisterCoordinatorServer(gsrv, coordinator.NewGRPCServer(c))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.Stop)

	return func(sanURIs ...string) pb.CoordinatorClient {
		cliCert, cliKey, cliCA := ca.WriteFiles(t, t.TempDir(), tlstest.LeafOpts{URIs: sanURIs})
		dialOpts, err := grpcutil.TLSConfig{CertFile: cliCert, KeyFile: cliKey, CAFile: cliCA}.DialOptions()
		if err != nil {
			t.Fatalf("DialOptions: %v", err)
		}
		conn, err := grpc.NewClient(lis.Addr().String(), dialOpts...)
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return pb.NewCoordinatorClient(conn)
	}
}

func TestCoordinatorIdentity_ReportShardBindsShardID(t *testing.T) {
	dial := startMTLSCoordinator(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Matching shard SAN: accepted.
	asShardA := dial(grpcutil.ShardURI("shard-a"))
	if _, err := asShardA.ReportShard(ctx, &pb.ShardReport{ShardId: "shard-a"}); err != nil {
		t.Fatalf("ReportShard with matching SAN: %v", err)
	}

	// Same certificate asserting a different shard_id: rejected.
	if _, err := asShardA.ReportShard(ctx, &pb.ShardReport{ShardId: "shard-b"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ReportShard with mismatched shard_id = %v; want PermissionDenied", err)
	}

	// Admin SAN does not authorize ReportShard either — the binding
	// is strict, not hierarchical.
	asAdmin := dial(grpcutil.AdminURI)
	if _, err := asAdmin.ReportShard(ctx, &pb.ShardReport{ShardId: "shard-a"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ReportShard with admin SAN = %v; want PermissionDenied", err)
	}
}

func TestCoordinatorIdentity_AdminSurfaceRequiresAdminSAN(t *testing.T) {
	dial := startMTLSCoordinator(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A shard certificate must not reach the admin surface.
	asShard := dial(grpcutil.ShardURI("shard-a"))
	if _, err := asShard.ListShards(ctx, &pb.ListShardsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ListShards with shard SAN = %v; want PermissionDenied", err)
	}
	if _, err := asShard.JoinRaftCluster(ctx, &pb.JoinRaftClusterRequest{
		NodeId: "node-2", RaftAddress: "127.0.0.1:1",
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("JoinRaftCluster with shard SAN = %v; want PermissionDenied", err)
	}
	if _, err := asShard.AssignDomain(ctx, &pb.AssignDomainRequest{
		TopologyKey: "rack", TopologyValue: "r-1", ShardId: "shard-a",
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("AssignDomain with shard SAN = %v; want PermissionDenied", err)
	}

	// The admin SAN passes authz (ListShards then succeeds outright).
	asAdmin := dial(grpcutil.AdminURI)
	if _, err := asAdmin.ListShards(ctx, &pb.ListShardsRequest{}); err != nil {
		t.Fatalf("ListShards with admin SAN: %v", err)
	}
}

// TestCoordinatorIdentity_ReadonlyRole pins ADR-0060: the bigfleet://readonly
// SAN may call EVERY read RPC but NO mutating RPC, and admin remains a
// superset (it passes the reads too). The whole point is that a read-only
// dashboard/CLI cert cannot change the fleet — the auth check fires before the
// request is even examined, so empty requests suffice for the rejection cases.
func TestCoordinatorIdentity_ReadonlyRole(t *testing.T) {
	dial := startMTLSCoordinator(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	asReadonly := dial(grpcutil.ReadonlyURI)

	// Every READ RPC is allowed.
	if _, err := asReadonly.ListShards(ctx, &pb.ListShardsRequest{}); err != nil {
		t.Errorf("ListShards with readonly SAN: %v", err)
	}
	if _, err := asReadonly.ListDomainAssignments(ctx, &pb.ListDomainAssignmentsRequest{}); err != nil {
		t.Errorf("ListDomainAssignments with readonly SAN: %v", err)
	}
	if _, err := asReadonly.ListShardReports(ctx, &pb.ListShardReportsRequest{}); err != nil {
		t.Errorf("ListShardReports with readonly SAN: %v", err)
	}

	// Every MUTATING RPC is rejected — the read-only cert cannot change the fleet.
	if _, err := asReadonly.AssignDomain(ctx, &pb.AssignDomainRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("AssignDomain with readonly SAN = %v; want PermissionDenied", err)
	}
	if _, err := asReadonly.UnassignDomain(ctx, &pb.UnassignDomainRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("UnassignDomain with readonly SAN = %v; want PermissionDenied", err)
	}
	if _, err := asReadonly.RemoveShard(ctx, &pb.RemoveShardRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("RemoveShard with readonly SAN = %v; want PermissionDenied", err)
	}
	if _, err := asReadonly.JoinRaftCluster(ctx, &pb.JoinRaftClusterRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("JoinRaftCluster with readonly SAN = %v; want PermissionDenied", err)
	}

	// Admin is a superset: it passes the new reads too.
	asAdmin := dial(grpcutil.AdminURI)
	if _, err := asAdmin.ListShardReports(ctx, &pb.ListShardReportsRequest{}); err != nil {
		t.Errorf("ListShardReports with admin SAN: %v", err)
	}

	// A shard SAN is neither a read nor an admin role — rejected on reads too.
	asShard := dial(grpcutil.ShardURI("shard-a"))
	if _, err := asShard.ListShardReports(ctx, &pb.ListShardReportsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("ListShardReports with shard SAN = %v; want PermissionDenied", err)
	}
}
