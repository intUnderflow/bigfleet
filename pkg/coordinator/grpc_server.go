package coordinator

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// GRPCServer wraps a Coordinator with the proto-generated server
// surface. Construct it after the Coordinator is up and register it
// with a grpc.Server.
type GRPCServer struct {
	pb.UnimplementedCoordinatorServer

	c *Coordinator

	// soft state (leader-local). Held here rather than on Coordinator
	// so the FSM stays focused on Raft-replicated facts.
	mu               sync.Mutex
	latestSummary    map[ShardID]ShardSummarySoft
	latestShortfalls map[ShardID][]ShortfallSoft

	// pending tracks instructions that have been queued for a shard
	// and not yet acked. Keyed by (shard_id, instruction_id).
	pendingMu sync.Mutex
	pending   map[ShardID]map[string]*pb.CoordinatorInstruction
}

// ShardSummarySoft is the leader-local copy of a shard's most-recent
// ShardSummary. Re-derived after a leader failover; not Raft state.
type ShardSummarySoft struct {
	Cycle              int64
	ReceivedAt         time.Time
	TotalMachines      int32
	FreeMachines       int32
	InstanceTypeCounts map[string]int32
	ZoneCounts         map[string]int32
	UtilCPUFraction    float64
	UtilMemoryFraction float64
}

// ShortfallSoft is the leader-local copy of one outstanding shortfall.
type ShortfallSoft struct {
	Priority                  int32
	Count                     int32
	AgeCycles                 int32
	InterruptionPenaltyBucket pb.PenaltyBucket
}

// NewGRPCServer constructs a GRPCServer over a running Coordinator.
func NewGRPCServer(c *Coordinator) *GRPCServer {
	return &GRPCServer{
		c:                c,
		latestSummary:    make(map[ShardID]ShardSummarySoft),
		latestShortfalls: make(map[ShardID][]ShortfallSoft),
		pending:          make(map[ShardID]map[string]*pb.CoordinatorInstruction),
	}
}

// ReportShard implements pb.CoordinatorServer. Accepts a shard's
// periodic report, updates the leader-local soft state, applies any
// instruction acks the shard included, and returns the coordinator's
// current term + any pending instructions.
//
// Only the current leader can serve this RPC. Followers reject with
// codes.FailedPrecondition so the shard knows to redirect.
func (g *GRPCServer) ReportShard(ctx context.Context, req *pb.ShardReport) (*pb.ReportAck, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	if req == nil || req.GetShardId() == "" {
		return nil, status.Error(codes.InvalidArgument, "shard_id required")
	}
	shardID := ShardID(req.GetShardId())

	// Heartbeat the shard. Idempotent — accepts unknown shards
	// silently so the first ReportShard from a fresh shard isn't
	// rejected before its membership has been Apply'd.
	g.c.State().MarkHeartbeat(shardID, time.Now().UTC())

	// Persist soft state.
	g.mu.Lock()
	if s := req.GetSummary(); s != nil {
		g.latestSummary[shardID] = ShardSummarySoft{
			Cycle:              req.GetCycle(),
			ReceivedAt:         time.Now().UTC(),
			TotalMachines:      s.GetTotalMachines(),
			FreeMachines:       s.GetFreeMachines(),
			InstanceTypeCounts: cloneIntMap(s.GetPerInstanceTypeCounts()),
			ZoneCounts:         cloneIntMap(s.GetPerZoneCounts()),
			UtilCPUFraction:    s.GetUtilisationCpuFraction(),
			UtilMemoryFraction: s.GetUtilisationMemoryFraction(),
		}
	}
	if len(req.GetShortfalls()) > 0 {
		copy := make([]ShortfallSoft, 0, len(req.GetShortfalls()))
		for _, sf := range req.GetShortfalls() {
			copy = append(copy, ShortfallSoft{
				Priority:                  sf.GetPriority(),
				Count:                     sf.GetCount(),
				AgeCycles:                 sf.GetAgeCycles(),
				InterruptionPenaltyBucket: sf.GetInterruptionPenaltyBucket(),
			})
		}
		g.latestShortfalls[shardID] = copy
	}
	g.mu.Unlock()

	// Process acks: clear pending instructions the shard has finished.
	g.clearAcked(shardID, req.GetInstructionAcks())

	// Build response: include any still-pending instructions.
	pending := g.snapshotPending(shardID)
	return &pb.ReportAck{
		Acknowledged:    true,
		CoordinatorTerm: g.c.RaftTerm(),
		Instructions:    pending,
	}, nil
}

// EnqueueInstruction queues a CoordinatorInstruction for delivery to
// the named shard on its next ReportShard. The instruction's term and
// sequence_number must already be set; this method does not amend
// them. Returns an error if the coordinator is not leader.
func (g *GRPCServer) EnqueueInstruction(shardID ShardID, instr *pb.CoordinatorInstruction) error {
	if !g.c.IsLeader() {
		return errors.New("coordinator: not leader")
	}
	if instr == nil || instr.GetInstructionId() == "" {
		return errors.New("coordinator: instruction must have instruction_id")
	}
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	if g.pending[shardID] == nil {
		g.pending[shardID] = make(map[string]*pb.CoordinatorInstruction)
	}
	g.pending[shardID][instr.GetInstructionId()] = instr
	return nil
}

// PendingForShard returns the count of pending instructions for the
// shard. Useful for tests / metrics.
func (g *GRPCServer) PendingForShard(shardID ShardID) int {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	return len(g.pending[shardID])
}

// LatestSummary returns the leader-local soft-state ShardSummarySoft
// the shard reported most recently.
func (g *GRPCServer) LatestSummary(shardID ShardID) (ShardSummarySoft, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	s, ok := g.latestSummary[shardID]
	return s, ok
}

// LatestShortfalls returns the leader-local soft-state shortfalls.
func (g *GRPCServer) LatestShortfalls(shardID ShardID) []ShortfallSoft {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]ShortfallSoft, len(g.latestShortfalls[shardID]))
	copy(out, g.latestShortfalls[shardID])
	return out
}

func (g *GRPCServer) snapshotPending(shardID ShardID) []*pb.CoordinatorInstruction {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	src := g.pending[shardID]
	if len(src) == 0 {
		return nil
	}
	out := make([]*pb.CoordinatorInstruction, 0, len(src))
	for _, instr := range src {
		out = append(out, instr)
	}
	// Sort by sequence_number for determinism on the wire.
	sort.Slice(out, func(i, j int) bool {
		return out[i].GetSequenceNumber() < out[j].GetSequenceNumber()
	})
	return out
}

func (g *GRPCServer) clearAcked(shardID ShardID, acks []*pb.InstructAck) {
	if len(acks) == 0 {
		return
	}
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	queue := g.pending[shardID]
	if queue == nil {
		return
	}
	for _, a := range acks {
		delete(queue, a.GetInstructionId())
	}
	if len(queue) == 0 {
		delete(g.pending, shardID)
	}
}

func cloneIntMap(in map[string]int32) map[string]int32 {
	if in == nil {
		return nil
	}
	out := make(map[string]int32, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
