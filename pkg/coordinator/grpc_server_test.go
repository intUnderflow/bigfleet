package coordinator_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// startCoordinatorWithGRPC brings up a single-node coordinator + a
// real grpc server, returns a connected client for the test.
func startCoordinatorWithGRPC(t *testing.T) (*coordinator.Coordinator, *coordinator.GRPCServer, pb.CoordinatorClient, context.Context) {
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

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	if err := c.WaitForLeader(waitCtx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	srv := coordinator.NewGRPCServer(c)
	gsrv := grpc.NewServer()
	pb.RegisterCoordinatorServer(gsrv, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := pb.NewCoordinatorClient(conn)
	return c, srv, cli, ctx
}

func TestGRPCServer_ReportShard_DeliversPending(t *testing.T) {
	c, srv, cli, ctx := startCoordinatorWithGRPC(t)

	// Pre-register the shard via Raft so the heartbeat lands.
	if err := c.Apply(ctx, coordinator.MakeAddShardCommand(coordinator.ShardEntry{
		ID: "shard-a", Address: "host:1",
	})); err != nil {
		t.Fatalf("AddShard: %v", err)
	}

	// Enqueue an instruction for shard-a.
	instr := &pb.CoordinatorInstruction{
		CoordinatorTerm: c.RaftTerm(),
		SequenceNumber:  1,
		InstructionId:   "instr-1",
		Payload: &pb.CoordinatorInstruction_AssignDomain{
			AssignDomain: &pb.AssignDomain{
				TopologyKey: "rack", TopologyValue: "r-1",
			},
		},
	}
	if err := srv.EnqueueInstruction("shard-a", instr); err != nil {
		t.Fatalf("EnqueueInstruction: %v", err)
	}

	// First report: receives the pending instruction.
	resp, err := cli.ReportShard(ctx, &pb.ShardReport{
		ShardId: "shard-a", Cycle: 1, ShardEpoch: 1,
		Summary: &pb.ShardSummary{TotalMachines: 100, FreeMachines: 20},
	})
	if err != nil {
		t.Fatalf("ReportShard: %v", err)
	}
	if !resp.GetAcknowledged() {
		t.Errorf("expected acknowledged")
	}
	if got := len(resp.GetInstructions()); got != 1 {
		t.Fatalf("instructions = %d, want 1", got)
	}
	if resp.GetInstructions()[0].GetInstructionId() != "instr-1" {
		t.Errorf("instruction_id mismatch")
	}

	// Soft state captured.
	if s, ok := srv.LatestSummary("shard-a"); !ok || s.TotalMachines != 100 {
		t.Errorf("LatestSummary: %+v ok=%v", s, ok)
	}

	// Second report acks the instruction; pending queue should drain.
	if _, err := cli.ReportShard(ctx, &pb.ShardReport{
		ShardId: "shard-a", Cycle: 2,
		InstructionAcks: []*pb.InstructAck{{
			InstructionId: "instr-1", Outcome: pb.InstructAck_OUTCOME_ACCEPTED,
		}},
	}); err != nil {
		t.Fatalf("second ReportShard: %v", err)
	}
	if got := srv.PendingForShard("shard-a"); got != 0 {
		t.Errorf("PendingForShard after ack = %d, want 0", got)
	}
}

// TestGRPCServer_ReportShard_SelfRegistersUnknownShard locks in the
// M12 contract that an unknown shard's first ReportShard auto-registers
// the shard in coordinator state (Raft-applied AddShard with the
// dial address from the report) so multi-shard deployments don't need
// an out-of-band registration step.
func TestGRPCServer_ReportShard_SelfRegistersUnknownShard(t *testing.T) {
	c, _, cli, ctx := startCoordinatorWithGRPC(t)

	// No AddShard call. The shard is unknown.
	if _, ok := c.State().Shard("shard-new"); ok {
		t.Fatalf("precondition: shard-new must be unknown")
	}

	if _, err := cli.ReportShard(ctx, &pb.ShardReport{
		ShardId:      "shard-new",
		ShardAddress: "bigfleet-shard-2.bigfleet-shard-headless:7780",
		Cycle:        1,
	}); err != nil {
		t.Fatalf("ReportShard: %v", err)
	}

	entry, ok := c.State().Shard("shard-new")
	if !ok {
		t.Fatalf("expected shard-new to be registered after first ReportShard")
	}
	if entry.Address != "bigfleet-shard-2.bigfleet-shard-headless:7780" {
		t.Errorf("Address = %q, want the dial address from the report", entry.Address)
	}
	if entry.LastHeartbeat.IsZero() {
		t.Errorf("LastHeartbeat must be set after registration")
	}

	// Second report from the same shard is a no-op on registration —
	// we keep the original RegisteredAt and only refresh LastHeartbeat.
	registeredAt := entry.RegisteredAt
	time.Sleep(5 * time.Millisecond)
	if _, err := cli.ReportShard(ctx, &pb.ShardReport{
		ShardId: "shard-new", Cycle: 2,
	}); err != nil {
		t.Fatalf("second ReportShard: %v", err)
	}
	entry2, _ := c.State().Shard("shard-new")
	if !entry2.RegisteredAt.Equal(registeredAt) {
		t.Errorf("RegisteredAt drifted across heartbeats: %v -> %v", registeredAt, entry2.RegisteredAt)
	}
	if !entry2.LastHeartbeat.After(entry.LastHeartbeat) {
		t.Errorf("LastHeartbeat did not advance: %v -> %v", entry.LastHeartbeat, entry2.LastHeartbeat)
	}
}

// TestGRPCServer_AdminRPCs_RoundTrip exercises the M15 admin surface
// end-to-end: register a shard via ReportShard, list it, assign a
// domain to it via the new AssignDomain RPC, list assignments, then
// unassign + remove. Locks in the user-stories Mode-2 contract that
// a platform engineer can drive coordinator state changes via gRPC
// without restarting the coordinator with new bootstrap state.
func TestGRPCServer_AdminRPCs_RoundTrip(t *testing.T) {
	_, _, cli, ctx := startCoordinatorWithGRPC(t)

	// 1. Self-register a shard via ReportShard so the registry is
	// non-empty; AssignDomain refuses unknown shard ids.
	if _, err := cli.ReportShard(ctx, &pb.ShardReport{
		ShardId:      "shard-a",
		ShardAddress: "shard-a.bigfleet-shard-headless:7780",
		Cycle:        1,
	}); err != nil {
		t.Fatalf("ReportShard: %v", err)
	}

	// 2. ListShards sees it.
	shards, err := cli.ListShards(ctx, &pb.ListShardsRequest{})
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	if len(shards.GetShards()) != 1 || shards.GetShards()[0].GetShardId() != "shard-a" {
		t.Fatalf("ListShards = %v, want [shard-a]", shards.GetShards())
	}

	// 3. AssignDomain.
	if _, err := cli.AssignDomain(ctx, &pb.AssignDomainRequest{
		TopologyKey: "rack", TopologyValue: "r-1", ShardId: "shard-a",
	}); err != nil {
		t.Fatalf("AssignDomain: %v", err)
	}

	// 4. AssignDomain again to same shard is idempotent.
	if _, err := cli.AssignDomain(ctx, &pb.AssignDomainRequest{
		TopologyKey: "rack", TopologyValue: "r-1", ShardId: "shard-a",
	}); err != nil {
		t.Fatalf("AssignDomain (idempotent re-assign): %v", err)
	}

	// 5. AssignDomain to different shard fails AlreadyExists.
	if _, err := cli.ReportShard(ctx, &pb.ShardReport{
		ShardId: "shard-b", ShardAddress: "shard-b:7780", Cycle: 1,
	}); err != nil {
		t.Fatalf("ReportShard shard-b: %v", err)
	}
	_, err = cli.AssignDomain(ctx, &pb.AssignDomainRequest{
		TopologyKey: "rack", TopologyValue: "r-1", ShardId: "shard-b",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("re-assign to different shard: code = %v, want AlreadyExists", status.Code(err))
	}

	// 6. ListDomainAssignments sees the binding.
	asn, err := cli.ListDomainAssignments(ctx, &pb.ListDomainAssignmentsRequest{})
	if err != nil {
		t.Fatalf("ListDomainAssignments: %v", err)
	}
	if len(asn.GetAssignments()) != 1 ||
		asn.GetAssignments()[0].GetTopologyKey() != "rack" ||
		asn.GetAssignments()[0].GetShardId() != "shard-a" {
		t.Fatalf("ListDomainAssignments = %v, want one rack=r-1→shard-a", asn.GetAssignments())
	}

	// 7. UnassignDomain idempotent on absent.
	if _, err := cli.UnassignDomain(ctx, &pb.UnassignDomainRequest{
		TopologyKey: "zone", TopologyValue: "absent",
	}); err != nil {
		t.Errorf("UnassignDomain (absent) should be idempotent: %v", err)
	}
	if _, err := cli.UnassignDomain(ctx, &pb.UnassignDomainRequest{
		TopologyKey: "rack", TopologyValue: "r-1",
	}); err != nil {
		t.Fatalf("UnassignDomain: %v", err)
	}

	// 8. RemoveShard cascades cleanup.
	if _, err := cli.RemoveShard(ctx, &pb.RemoveShardRequest{ShardId: "shard-a"}); err != nil {
		t.Fatalf("RemoveShard: %v", err)
	}
	_, err = cli.RemoveShard(ctx, &pb.RemoveShardRequest{ShardId: "shard-a"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("re-remove: code = %v, want NotFound", status.Code(err))
	}
}

// TestGRPCServer_ListQuotas: M24 quota inspection surface. Pre-seed a
// quota allocation via direct State.SetQuota (the FSM path is covered
// by state_test.go); ListQuotas should round-trip the entries with
// stable provider/region ordering.
func TestGRPCServer_ListQuotas(t *testing.T) {
	c, _, cli, ctx := startCoordinatorWithGRPC(t)

	c.State().SetQuota(coordinator.QuotaKey{Provider: "aws-prov", Region: "us-east-1"},
		map[coordinator.ShardID]int32{"shard-a": 100, "shard-b": 50})
	c.State().SetQuota(coordinator.QuotaKey{Provider: "gcp-prov", Region: "europe-west4"},
		map[coordinator.ShardID]int32{"shard-a": 25})

	resp, err := cli.ListQuotas(ctx, &pb.ListQuotasRequest{})
	if err != nil {
		t.Fatalf("ListQuotas: %v", err)
	}
	if got := len(resp.GetAllocations()); got != 2 {
		t.Fatalf("allocations = %d, want 2", got)
	}
	// Stable provider sort: aws-prov before gcp-prov.
	if resp.GetAllocations()[0].GetProvider() != "aws-prov" {
		t.Errorf("first allocation provider = %q, want aws-prov", resp.GetAllocations()[0].GetProvider())
	}
	awsPerShard := resp.GetAllocations()[0].GetPerShard()
	if awsPerShard["shard-a"] != 100 || awsPerShard["shard-b"] != 50 {
		t.Errorf("aws per_shard = %v, want shard-a=100 shard-b=50", awsPerShard)
	}
}

func TestGRPCServer_ReportShard_RejectsNonLeader(t *testing.T) {
	// Build a coordinator that never becomes leader (no Bootstrap).
	c, err := coordinator.New(coordinator.Config{
		NodeID:          "node-1",
		DataDir:         filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress: freePort(t),
		Bootstrap:       false,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Close)

	srv := coordinator.NewGRPCServer(c)
	gsrv := grpc.NewServer()
	pb.RegisterCoordinatorServer(gsrv, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cli := pb.NewCoordinatorClient(conn)

	_, err = cli.ReportShard(context.Background(), &pb.ShardReport{ShardId: "shard-x"})
	if err == nil {
		t.Fatal("expected non-leader error")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", err)
	}
}
