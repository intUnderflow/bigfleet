package coordinator

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/metrics"
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
//
// ADR-0027: Deficit is the residual aggregate resource demand — a
// resource vector — replacing the old machine `Count`.
type ShortfallSoft struct {
	Priority                  int32
	Deficit                   map[string]string
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

	// Self-registration. If the shard isn't already in coordinator
	// state, Raft-Apply AddShard now so subsequent reports land as
	// heartbeats and so domain assignments addressed at this shard
	// have a registry entry to point at. The shard carries its dial
	// address on every report; we record it on first sight only.
	// Re-registration of an existing shard would return ErrShardExists
	// — silently swallowed because the registration is by definition
	// already done.
	if _, ok := g.c.State().Shard(shardID); !ok {
		entry := ShardEntry{
			ID:      shardID,
			Address: req.GetShardAddress(),
		}
		// AddShard goes through Raft so it survives leader failover
		// and shows up in snapshots. Failure here is logged-only —
		// we still want the heartbeat to land for soft-state.
		if err := g.c.Apply(ctx, MakeAddShardCommand(entry)); err != nil && !errors.Is(err, ErrShardExists) {
			// Don't fail the RPC: a transient Apply failure shouldn't
			// stall the shard's report loop. Next ReportShard retries.
			return nil, status.Errorf(codes.Unavailable, "coordinator: register shard: %v", err)
		}
	}

	// Heartbeat the shard. MarkHeartbeat is a no-op if the shard
	// somehow isn't registered (e.g. concurrent removal); the
	// AddShard above ensures the common path is registered first.
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
				Deficit:                   sf.GetDeficit().GetResources(),
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
	metrics.CoordinatorPendingInstructions.WithLabelValues(string(shardID)).Set(float64(len(pending)))
	metrics.CoordinatorRaftTerm.Set(float64(g.c.RaftTerm()))
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

// AssignDomain pins a topology domain to a shard. Idempotent — see
// state.AssignDomain. Re-assigning to the current shard is a no-op
// success; assigning to a different shard returns AlreadyExists.
//
// The Raft Apply path goes through the FSM, which in turn calls
// state.AssignDomain. Once committed, the next ReportShard from the
// destination shard will pick up the assignment via the existing
// CoordinatorInstruction path (not implemented here — this RPC just
// records the binding in coordinator state).
func (g *GRPCServer) AssignDomain(ctx context.Context, req *pb.AssignDomainRequest) (*pb.AssignDomainResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	if req == nil || req.GetTopologyKey() == "" || req.GetTopologyValue() == "" || req.GetShardId() == "" {
		return nil, status.Error(codes.InvalidArgument, "topology_key, topology_value, shard_id all required")
	}
	d := DomainKey{Key: req.GetTopologyKey(), Value: req.GetTopologyValue()}
	if err := g.c.Apply(ctx, MakeAssignDomainCommand(d, ShardID(req.GetShardId()))); err != nil {
		if errors.Is(err, ErrDomainAlreadyAssigned) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		if errors.Is(err, ErrShardNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Unavailable, "AssignDomain: %v", err)
	}
	return &pb.AssignDomainResponse{}, nil
}

// UnassignDomain removes the binding. Idempotent.
func (g *GRPCServer) UnassignDomain(ctx context.Context, req *pb.UnassignDomainRequest) (*pb.UnassignDomainResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	if req == nil || req.GetTopologyKey() == "" || req.GetTopologyValue() == "" {
		return nil, status.Error(codes.InvalidArgument, "topology_key and topology_value required")
	}
	d := DomainKey{Key: req.GetTopologyKey(), Value: req.GetTopologyValue()}
	if err := g.c.Apply(ctx, MakeUnassignDomainCommand(d)); err != nil {
		return nil, status.Errorf(codes.Unavailable, "UnassignDomain: %v", err)
	}
	return &pb.UnassignDomainResponse{}, nil
}

// RemoveShard deletes a shard's registry entry. Cluster + domain
// assignments pointing at it are cleaned up by state.RemoveShard.
func (g *GRPCServer) RemoveShard(ctx context.Context, req *pb.RemoveShardRequest) (*pb.RemoveShardResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	if req == nil || req.GetShardId() == "" {
		return nil, status.Error(codes.InvalidArgument, "shard_id required")
	}
	if err := g.c.Apply(ctx, MakeRemoveShardCommand(ShardID(req.GetShardId()))); err != nil {
		if errors.Is(err, ErrShardNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Unavailable, "RemoveShard: %v", err)
	}
	return &pb.RemoveShardResponse{}, nil
}

// ListShards returns the registered shards. Read path — no Raft Apply.
// The State guards itself with an RLock so this is safe on followers
// too, but we still gate to leader-only for v1 to keep the read story
// consistent (no stale-read footguns until we explicitly opt in).
func (g *GRPCServer) ListShards(_ context.Context, _ *pb.ListShardsRequest) (*pb.ListShardsResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	entries := g.c.State().Shards()
	out := make([]*pb.ShardRegistryEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &pb.ShardRegistryEntry{
			ShardId:             string(e.ID),
			Address:             e.Address,
			RegisteredAtUnixNs:  e.RegisteredAt.UnixNano(),
			LastHeartbeatUnixNs: e.LastHeartbeat.UnixNano(),
		})
	}
	return &pb.ListShardsResponse{Shards: out}, nil
}

// ListQuotas returns the speculative-pool slice for every
// (provider, region) the coordinator has assigned. Read path; same
// leader-only contract as ListShards / ListDomainAssignments. M24.
func (g *GRPCServer) ListQuotas(_ context.Context, _ *pb.ListQuotasRequest) (*pb.ListQuotasResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	all := g.c.State().Quotas()
	out := make([]*pb.QuotaAllocation, 0, len(all))
	for k, a := range all {
		perShard := make(map[string]int32, len(a.PerShard))
		for sh, n := range a.PerShard {
			perShard[string(sh)] = n
		}
		out = append(out, &pb.QuotaAllocation{
			Provider: k.Provider,
			Region:   k.Region,
			PerShard: perShard,
		})
	}
	// Stable order for tabwriter output downstream.
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetProvider() != out[j].GetProvider() {
			return out[i].GetProvider() < out[j].GetProvider()
		}
		return out[i].GetRegion() < out[j].GetRegion()
	})
	return &pb.ListQuotasResponse{Allocations: out}, nil
}

// ListDomainAssignments returns the topology-domain → shard mapping.
// Same leader-only contract as ListShards.
func (g *GRPCServer) ListDomainAssignments(_ context.Context, _ *pb.ListDomainAssignmentsRequest) (*pb.ListDomainAssignmentsResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	entries := g.c.State().Shards()
	out := make([]*pb.DomainAssignment, 0)
	for _, e := range entries {
		for _, d := range g.c.State().DomainsForShard(e.ID) {
			out = append(out, &pb.DomainAssignment{
				TopologyKey:   d.Key,
				TopologyValue: d.Value,
				ShardId:       string(e.ID),
			})
		}
	}
	return &pb.ListDomainAssignmentsResponse{Assignments: out}, nil
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
