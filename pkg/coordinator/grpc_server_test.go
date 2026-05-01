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
