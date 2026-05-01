package coordclient_test

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/shard/coordclient"
)

// freePort returns an available local TCP port.
func freePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// stubView is a minimal in-process ShardView for tests.
type stubView struct {
	id    string
	epoch int64

	mu           sync.Mutex
	assigned     []string // domains as "key=value"
	unassigned   []string
	reassigned   [][]string
	xshardDrains [][]string
	transfers    []string
}

func (v *stubView) ID() string   { return v.id }
func (v *stubView) Epoch() int64 { return v.epoch }
func (v *stubView) Summary() coordclient.ShardSummary {
	return coordclient.ShardSummary{TotalMachines: 100, FreeMachines: 20}
}
func (v *stubView) Shortfalls() []coordclient.ShardShortfall { return nil }
func (v *stubView) OnAssignDomain(k, val string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.assigned = append(v.assigned, k+"="+val)
	return nil
}
func (v *stubView) OnUnassignDomain(k, val string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.unassigned = append(v.unassigned, k+"="+val)
	return nil
}
func (v *stubView) OnReassignSpeculative(ids []string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.reassigned = append(v.reassigned, ids)
	return nil
}
func (v *stubView) OnCrossShardDrain(ids []string, _ int32) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.xshardDrains = append(v.xshardDrains, ids)
	return nil
}
func (v *stubView) OnTransferOwnership(_ []string, from, to string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.transfers = append(v.transfers, from+"→"+to)
	return nil
}

// snapshotAssigned returns a copy of v.assigned for safe reads.
func (v *stubView) snapshotAssigned() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]string, len(v.assigned))
	copy(out, v.assigned)
	return out
}

// startTestCoord brings up a single-node coordinator + grpc server.
// Returns the GRPCServer (for queueing instructions), the listener
// address, and the coordinator (for term lookup).
func startTestCoord(t *testing.T) (*coordinator.GRPCServer, string, *coordinator.Coordinator) {
	t.Helper()
	c, err := coordinator.New(coordinator.Config{
		NodeID:          "node-1",
		DataDir:         filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress: freePort(t),
		Bootstrap:       true,
	})
	if err != nil {
		t.Fatalf("coord New: %v", err)
	}
	t.Cleanup(c.Close)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	return srv, lis.Addr().String(), c
}

func TestCoordClient_DispatchesAssignDomain(t *testing.T) {
	srv, addr, c := startTestCoord(t)
	view := &stubView{id: "shard-1", epoch: 1}

	cli, err := coordclient.New(coordclient.Config{
		CoordinatorAddress: addr,
		View:               view,
		CoordinatorTerm:    fencing.NewCoordinatorTerm(),
		ReportInterval:     50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect insecure for queue setup; we only need the server running.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	go func() { _ = cli.Run(ctx) }()

	// Wait for the first ReportShard so the soft-state side knows
	// shard-1 exists, then queue an AssignDomain instruction.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := srv.LatestSummary("shard-1"); ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := srv.EnqueueInstruction("shard-1", &pb.CoordinatorInstruction{
		CoordinatorTerm: c.RaftTerm(),
		SequenceNumber:  1,
		InstructionId:   "instr-1",
		Payload: &pb.CoordinatorInstruction_AssignDomain{
			AssignDomain: &pb.AssignDomain{
				TopologyKey: "rack", TopologyValue: "r-1",
			},
		},
	}); err != nil {
		t.Fatalf("EnqueueInstruction: %v", err)
	}

	// Wait until the stub records the AssignDomain call and the
	// coordinator's pending queue drains (i.e., we've ack'd it).
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(view.snapshotAssigned()) > 0 && srv.PendingForShard("shard-1") == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(view.snapshotAssigned()) == 0 || view.snapshotAssigned()[0] != "rack=r-1" {
		t.Errorf("assigned domains = %v, want [rack=r-1]", view.snapshotAssigned())
	}
	if got := srv.PendingForShard("shard-1"); got != 0 {
		t.Errorf("PendingForShard = %d, want 0", got)
	}
}

func TestCoordClient_RejectsStaleTermInstruction(t *testing.T) {
	srv, addr, _ := startTestCoord(t)
	view := &stubView{id: "shard-1", epoch: 1}

	// Don't pre-bump the high-water mark; the first ReportShard
	// response carries the coordinator's real term and the client
	// adopts it. Then we send an instruction with term=0 — older
	// than whatever Raft elected — which the instruction-level
	// fence rejects.
	term := fencing.NewCoordinatorTerm()

	cli, err := coordclient.New(coordclient.Config{
		CoordinatorAddress: addr,
		View:               view,
		CoordinatorTerm:    term,
		ReportInterval:     50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = cli.Run(ctx) }()

	// Wait for the first ReportShard so the high-water mark catches up
	// to the coordinator's real term.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if term.HighWaterMark() > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if term.HighWaterMark() == 0 {
		t.Fatal("term high-water mark never advanced from initial report")
	}

	// Enqueue an instruction with a clearly-stale term (0).
	if err := srv.EnqueueInstruction("shard-1", &pb.CoordinatorInstruction{
		CoordinatorTerm: 0, SequenceNumber: 1, InstructionId: "instr-stale",
		Payload: &pb.CoordinatorInstruction_AssignDomain{
			AssignDomain: &pb.AssignDomain{TopologyKey: "rack", TopologyValue: "r-1"},
		},
	}); err != nil {
		t.Fatalf("EnqueueInstruction: %v", err)
	}

	// The client should ack with REJECTED_STALE; the coordinator
	// drains it on the next report.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.PendingForShard("shard-1") == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(view.snapshotAssigned()) != 0 {
		t.Errorf("assigned = %v, want empty (instruction should have been term-rejected)", view.snapshotAssigned())
	}
	if got := srv.PendingForShard("shard-1"); got != 0 {
		t.Errorf("PendingForShard = %d, want 0 (REJECTED_STALE ack should drain it)", got)
	}
}
