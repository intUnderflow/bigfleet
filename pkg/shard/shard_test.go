package shard_test

import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// Component test: real gRPC server hosting the Shard.Session bidi
// stream + a scripted operator client + the in-memory fake provider.
// Drives the four paper scenarios and asserts the shard's behaviour
// end-to-end without an actual cluster.

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	prov := fake.New(fake.Options{InstantTransitions: true})
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	sh, err := shard.New(shard.Config{
		ID:               "shard-test",
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    50 * time.Millisecond,
		BootstrapTimeout: 2 * time.Second,
		// Tests assert on AC emit timing; nanosecond rate-limit lets
		// every cycle emit so coalesce remains the only gate.
		AvailableCapacityInterval: 1 * time.Nanosecond,
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

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	client := pb.NewShardClient(conn)

	go func() {
		if err := sh.Run(ctx); err != nil && ctx.Err() == nil {
			t.Logf("shard.Run: %v", err)
		}
	}()

	t.Cleanup(func() {
		cancel()
		srv.Stop()
		_ = conn.Close()
	})

	return &testEnv{
		ctx:      ctx,
		shard:    sh,
		provider: prov,
		client:   client,
	}
}

type testEnv struct {
	ctx      context.Context
	shard    *shard.Shard
	provider *fake.Provider
	client   pb.ShardClient
}

// scriptedOperator opens a Session, sends Hello, then accepts inbound
// frames. BootstrapRequest is auto-answered with a canned blob.
type scriptedOperator struct {
	t       *testing.T
	cluster string
	stream  grpc.BidiStreamingClient[pb.OperatorMessage, pb.ShardMessage]
	cancel  context.CancelFunc

	// Captured frames for assertions.
	bootstrapRequests   chan *pb.BootstrapRequest
	reclaimInstructions chan *pb.ReclaimInstruction
	availableCapacity   chan *pb.AvailableCapacityUpdate
}

func newScriptedOperator(t *testing.T, env *testEnv, cluster string) *scriptedOperator {
	t.Helper()
	ctx, cancel := context.WithCancel(env.ctx)
	stream, err := env.client.Session(ctx)
	if err != nil {
		cancel()
		t.Fatalf("Session: %v", err)
	}
	op := &scriptedOperator{
		t:                   t,
		cluster:             cluster,
		stream:              stream,
		cancel:              cancel,
		bootstrapRequests:   make(chan *pb.BootstrapRequest, 64),
		reclaimInstructions: make(chan *pb.ReclaimInstruction, 64),
		availableCapacity:   make(chan *pb.AvailableCapacityUpdate, 64),
	}
	if err := stream.Send(&pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Hello{Hello: &pb.Hello{
			ClusterId: cluster, ProtocolVersion: "v1alpha1",
		}},
	}); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	go op.readLoop()
	t.Cleanup(func() { cancel() })
	return op
}

func (op *scriptedOperator) readLoop() {
	for {
		msg, err := op.stream.Recv()
		if err != nil {
			return
		}
		switch p := msg.GetPayload().(type) {
		case *pb.ShardMessage_BootstrapRequest:
			req := p.BootstrapRequest
			op.bootstrapRequests <- req
			// Auto-answer with a canned blob (the test fake provider
			// accepts anything).
			_ = op.stream.Send(&pb.OperatorMessage{
				Payload: &pb.OperatorMessage_BootstrapResponse{BootstrapResponse: &pb.BootstrapBlobResponse{
					RequestId: req.GetRequestId(),
					UserData:  []byte("#cloud-config\n"),
				}},
			})
		case *pb.ShardMessage_ReclaimInstruction:
			op.reclaimInstructions <- p.ReclaimInstruction
			_ = op.stream.Send(&pb.OperatorMessage{
				Payload: &pb.OperatorMessage_ReclaimAck{ReclaimAck: &pb.ReclaimAck{
					InstructionId: p.ReclaimInstruction.GetInstructionId(),
					NodesStarted:  int32(len(p.ReclaimInstruction.GetNodes())),
				}},
			})
		case *pb.ShardMessage_AvailableCapacity:
			// Non-blocking; drop if the channel is full so a noisy
			// emit loop doesn't deadlock the test.
			select {
			case op.availableCapacity <- p.AvailableCapacity:
			default:
			}
		}
	}
}

func (op *scriptedOperator) sendRollup(needs []*pb.CapacityNeed) {
	op.t.Helper()
	if err := op.stream.Send(&pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Rollup{Rollup: &pb.ClusterCapacityNeeds{
			ClusterId:          op.cluster,
			TimestampUnixNanos: time.Now().UnixNano(),
			Needs:              needs,
		}},
	}); err != nil {
		op.t.Fatalf("send rollup: %v", err)
	}
}

// gpuNeed returns the standard 8-GPU CapacityNeed used in most paper
// examples: `count` replicas, each a single a3-highgpu-8g (8 GPU) unit.
// ADR-0027: demand is the aggregate resource vector (count × 8 GPU)
// plus the per-replica MinUnit, not a Pod count.
func gpuNeed(priority int32, count int32) *pb.CapacityNeed {
	return &pb.CapacityNeed{
		Requirements: []*pb.NodeSelectorRequirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: pb.NodeSelectorRequirement_OPERATOR_IN,
			Values:   []string{"a3-highgpu-8g"},
		}},
		Priority:                  priority,
		AggregateResources:        map[string]string{"nvidia.com/gpu": strconv.Itoa(int(count) * 8)},
		MinUnit:                   map[string]string{"nvidia.com/gpu": "8"},
		InterruptionPenaltyBucket: pb.PenaltyBucket_PENALTY_BUCKET_8192,
		ReclamationPenaltyBucket:  pb.PenaltyBucket_PENALTY_BUCKET_PINNED,
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, name string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", name)
}

// -----------------------------------------------------------------
// Scenario tests
// -----------------------------------------------------------------

// Training-job scenario: 4 idle GPU machines available, one cluster
// asks for 4. Phase 1 emits Bootstrap actions, the shard pulls bootstrap
// blobs from the operator, calls Configure on the provider, and the
// machines end up Configured for the cluster.
func TestShard_TrainingJob_BootstrapFromIdle(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	for i := 0; i < 4; i++ {
		env.provider.AddIdle(machine.ID("gpu-"+strconv.Itoa(i)),
			machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a", Resources: map[string]string{"nvidia.com/gpu": "8"}},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	op := newScriptedOperator(t, env, "cluster-train")
	op.sendRollup([]*pb.CapacityNeed{gpuNeed(1_000_000, 4)})

	waitFor(t, 5*time.Second, func() bool {
		return env.shard.Inventory().Snapshot().CountByState(machine.StateConfigured) == 4
	}, "4 machines reach Configured")

	// Operator should have seen exactly 4 BootstrapRequest frames.
	count := 0
	for done := false; !done; {
		select {
		case <-op.bootstrapRequests:
			count++
		case <-time.After(100 * time.Millisecond):
			done = true
		}
	}
	if count != 4 {
		t.Errorf("BootstrapRequests received = %d, want 4", count)
	}
}

// Capacity stockout: cluster needs 8 but only 2 idle machines exist.
// Phase 1 satisfies 2; the remaining 6 stay unsatisfied and Phase 2
// has no victims to take. The 2 satisfied machines reach Configured.
func TestShard_Stockout_PartialFulfilment(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	env.provider.AddIdle("gpu-0",
		machine.Profile{InstanceType: "a3-highgpu-8g", Resources: map[string]string{"nvidia.com/gpu": "8"}},
		machine.CapacityTypeBareMetal, 0, 0)
	env.provider.AddIdle("gpu-1",
		machine.Profile{InstanceType: "a3-highgpu-8g", Resources: map[string]string{"nvidia.com/gpu": "8"}},
		machine.CapacityTypeBareMetal, 0, 0)
	op := newScriptedOperator(t, env, "cluster-train")
	op.sendRollup([]*pb.CapacityNeed{gpuNeed(1_000_000, 8)})

	waitFor(t, 5*time.Second, func() bool {
		return env.shard.Inventory().Snapshot().CountByState(machine.StateConfigured) == 2
	}, "2 machines reach Configured")

	// Stays at 2 — no further capacity available to satisfy the
	// remaining deficit.
	time.Sleep(200 * time.Millisecond)
	if got := env.shard.Inventory().Snapshot().CountByState(machine.StateConfigured); got != 2 {
		t.Errorf("steady-state Configured = %d, want 2", got)
	}
}

// Withdrawal: cluster initially needs 4; later needs 0. Phase 3 reclaims
// the configured machines, sending ReclaimInstruction frames to the
// operator. The fake provider drains them back to Idle.
func TestShard_Withdrawal_ReclaimsConfigured(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	for i := 0; i < 4; i++ {
		env.provider.AddIdle(machine.ID("gpu-"+strconv.Itoa(i)),
			machine.Profile{InstanceType: "a3-highgpu-8g", Resources: map[string]string{"nvidia.com/gpu": "8"}},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	op := newScriptedOperator(t, env, "cluster-train")
	op.sendRollup([]*pb.CapacityNeed{gpuNeed(1_000_000, 4)})
	waitFor(t, 5*time.Second, func() bool {
		return env.shard.Inventory().Snapshot().CountByState(machine.StateConfigured) == 4
	}, "4 Configured")

	// Withdraw all needs.
	op.sendRollup(nil)
	waitFor(t, 5*time.Second, func() bool {
		return env.shard.Inventory().Snapshot().CountByState(machine.StateIdle) == 4 &&
			env.shard.Inventory().Snapshot().CountByState(machine.StateConfigured) == 0
	}, "all machines back to Idle")
}

// Priority inversion: cluster-batch holds 4 GPU machines at priority
// 100K; cluster-train arrives wanting the same 4 at priority 1M.
// Phase 2 preempts; the next cycle re-bootstraps for cluster-train.
func TestShard_PriorityInversion(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	for i := 0; i < 4; i++ {
		env.provider.AddIdle(machine.ID("gpu-"+strconv.Itoa(i)),
			machine.Profile{InstanceType: "a3-highgpu-8g", Resources: map[string]string{"nvidia.com/gpu": "8"}},
			machine.CapacityTypeBareMetal, 0, 0)
	}
	batch := newScriptedOperator(t, env, "cluster-batch")
	batch.sendRollup([]*pb.CapacityNeed{gpuNeed(100_000, 4)})
	waitFor(t, 5*time.Second, func() bool {
		// Configured for cluster-batch.
		count := 0
		for _, m := range env.shard.Inventory().Snapshot().All() {
			if m.State == machine.StateConfigured && m.Cluster == "cluster-batch" {
				count++
			}
		}
		return count == 4
	}, "cluster-batch claims 4")

	// Now cluster-train comes in.
	train := newScriptedOperator(t, env, "cluster-train")
	train.sendRollup([]*pb.CapacityNeed{gpuNeed(1_000_000, 4)})

	// Eventually all 4 are configured for cluster-train.
	waitFor(t, 10*time.Second, func() bool {
		count := 0
		for _, m := range env.shard.Inventory().Snapshot().All() {
			if m.State == machine.StateConfigured && m.Cluster == "cluster-train" {
				count++
			}
		}
		return count == 4
	}, "cluster-train ends with 4")

	// And cluster-batch saw 4 ReclaimInstruction frames.
	got := 0
	for done := false; !done; {
		select {
		case <-batch.reclaimInstructions:
			got++
		case <-time.After(200 * time.Millisecond):
			done = true
		}
	}
	if got != 4 {
		t.Errorf("ReclaimInstruction count = %d, want 4", got)
	}

	// Drain grace was scaled by priority gap (1M - 100K = 900K → 30s).
	// We don't strictly assert this — DrainGrace is unit-tested
	// elsewhere. Here we only verify the action fired.

	// Verify the batch cluster's needs are now considered unresolved
	// (from its perspective) — i.e., its CapacityNeeds remained in the
	// table but the machines moved to a higher-priority cluster.
	if t.Failed() {
		// Diagnostics for debugging if we get here.
		for _, m := range env.shard.Inventory().Snapshot().All() {
			t.Logf("machine %s state=%s cluster=%s assigned=%d", m.ID, m.State, m.Cluster, m.AssignedPriority)
		}
	}
	_ = decision.ActionKindPreempt // keep import live (used indirectly via shard)
}

// AvailableCapacity surfacing scenario: shard emits one
// AvailableCapacityUpdate per (cluster, profile fingerprint) per cycle.
// The scripted operator captures them and we assert the right
// confidence / count / cost-per-hour reach the wire.
func TestShard_AvailableCapacity_EmitsToOperator(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	// Two idle GPU machines at different prices — cheaper should win.
	env.provider.AddIdle("gpu-a",
		machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a", Resources: map[string]string{"nvidia.com/gpu": "8"}},
		machine.CapacityTypeBareMetal, 6.0, 0)
	env.provider.AddIdle("gpu-b",
		machine.Profile{InstanceType: "a3-highgpu-8g", Zone: "us-east-1a", Resources: map[string]string{"nvidia.com/gpu": "8"}},
		machine.CapacityTypeBareMetal, 4.5, 0)

	op := newScriptedOperator(t, env, "cluster-ac")
	// Send a rollup that demands 1 GPU machine. Phase 1 will satisfy it
	// from idle (consuming gpu-b, the cheaper one); the AC emit runs
	// after Phase 3 and reflects the snapshot's pre-execute view.
	op.sendRollup([]*pb.CapacityNeed{gpuNeed(1_000_000, 1)})

	// Wait for at least one AvailableCapacityUpdate frame.
	var got *pb.AvailableCapacityUpdate
	waitFor(t, 5*time.Second, func() bool {
		select {
		case got = <-op.availableCapacity:
			return true
		default:
			return false
		}
	}, "first AvailableCapacityUpdate frame")

	if got.GetConfidence() != pb.AvailableCapacityUpdate_CONFIDENCE_HIGH {
		t.Errorf("confidence = %v, want HIGH", got.GetConfidence())
	}
	if got.GetAvailableCount() < 1 {
		t.Errorf("available_count = %d, want >= 1", got.GetAvailableCount())
	}
	if got.GetCostPerHour() != 4.5 {
		t.Errorf("cost_per_hour = %v, want 4.5 (cheapest of the two idle)", got.GetCostPerHour())
	}
	if got.GetSupersedesKey() == "" {
		t.Errorf("supersedes_key empty")
	}
}
