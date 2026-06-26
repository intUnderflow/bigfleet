package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// ADR-0048 caller identity. When the transport is mTLS the verified
// client certificate's bigfleet:// URI SAN gates every RPC:
//
//   - ReportShard binds the caller-asserted shard_id to
//     bigfleet://shard/<shard_id> — the same binding Session applies
//     to Hello.cluster_id on the shard.
//   - The whole admin surface (M15: Assign/UnassignDomain,
//     RemoveShard, ListShards, ListDomainAssignments;
//     M75: JoinRaftCluster, SnapshotSave) requires bigfleet://admin.
//     Coordinator replicas carry the admin SAN themselves: they call
//     JoinRaftCluster on each other (ADR-0047) and ARE the admin
//     domain.
//
// On plaintext transports both checks are skipped — the pre-ADR-0048
// trust-the-network posture, retained as the zero-config default.

// requireShardIdentity enforces the shard_id ↔ URI SAN binding.
func (g *GRPCServer) requireShardIdentity(ctx context.Context, shardID string) error {
	uri, mtls, err := grpcutil.PeerIdentity(ctx)
	if !mtls {
		return nil
	}
	if want := grpcutil.ShardURI(shardID); err != nil || uri != want {
		g.c.log.Error("coordinator rejected shard identity",
			"shard_id", shardID, "presented_identity", uri, "err", err)
		return status.Errorf(codes.PermissionDenied,
			"coordinator: client certificate identity %q does not authorize shard_id %q", uri, shardID)
	}
	return nil
}

// requireAdminIdentity enforces the bigfleet://admin SAN on the admin
// (mutating) surface.
func (g *GRPCServer) requireAdminIdentity(ctx context.Context, rpc string) error {
	uri, mtls, err := grpcutil.PeerIdentity(ctx)
	if !mtls {
		return nil
	}
	if err != nil || uri != grpcutil.AdminURI {
		g.c.log.Error("coordinator rejected admin identity",
			"rpc", rpc, "presented_identity", uri, "err", err)
		return status.Errorf(codes.PermissionDenied,
			"coordinator: %s requires client certificate URI SAN %q (got %q)", rpc, grpcutil.AdminURI, uri)
	}
	return nil
}

// requireReadIdentity enforces the READ surface contract (ADR-0060): the
// caller must carry EITHER bigfleet://readonly (read-only operator tooling)
// OR bigfleet://admin (admin is a superset). It MUST NOT accept the write
// surface's identities for free — the mutating RPCs stay on
// requireAdminIdentity. On plaintext transports the check is skipped, like
// every other gate (the trust-the-network default).
func (g *GRPCServer) requireReadIdentity(ctx context.Context, rpc string) error {
	uri, mtls, err := grpcutil.PeerIdentity(ctx)
	if !mtls {
		return nil
	}
	if err != nil || (uri != grpcutil.ReadonlyURI && uri != grpcutil.AdminURI) {
		g.c.log.Error("coordinator rejected read identity",
			"rpc", rpc, "presented_identity", uri, "err", err)
		return status.Errorf(codes.PermissionDenied,
			"coordinator: %s requires client certificate URI SAN %q or %q (got %q)", rpc, grpcutil.ReadonlyURI, grpcutil.AdminURI, uri)
	}
	return nil
}

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
	ProviderAddress    string
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
	if err := g.requireShardIdentity(ctx, req.GetShardId()); err != nil {
		return nil, err
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
			ProviderAddress:    s.GetProviderAddress(),
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
	if err := g.requireAdminIdentity(ctx, "AssignDomain"); err != nil {
		return nil, err
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
	if err := g.requireAdminIdentity(ctx, "UnassignDomain"); err != nil {
		return nil, err
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
	if err := g.requireAdminIdentity(ctx, "RemoveShard"); err != nil {
		return nil, err
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
func (g *GRPCServer) ListShards(ctx context.Context, _ *pb.ListShardsRequest) (*pb.ListShardsResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	if err := g.requireReadIdentity(ctx, "ListShards"); err != nil {
		return nil, err
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

// JoinRaftCluster adds the calling replica as a Raft voter (M75,
// ADR-0047). Leader-only — hashicorp/raft membership changes go
// through the leader's log. AddVoter of an existing voter is
// idempotent (the configuration change updates the address in
// place), so a replica re-joining after a restart is safe.
func (g *GRPCServer) JoinRaftCluster(ctx context.Context, req *pb.JoinRaftClusterRequest) (*pb.JoinRaftClusterResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	// Joining replicas present the coordinator's own certificate,
	// which carries bigfleet://admin (ADR-0048).
	if err := g.requireAdminIdentity(ctx, "JoinRaftCluster"); err != nil {
		return nil, err
	}
	if req == nil || req.GetNodeId() == "" || req.GetRaftAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and raft_address required")
	}
	if err := g.c.AddVoter(req.GetNodeId(), req.GetRaftAddress()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "JoinRaftCluster: %v", err)
	}
	return &pb.JoinRaftClusterResponse{}, nil
}

// SnapshotSave streams the leader's freshest Raft snapshot — one
// meta_json frame, then state chunks (M75 DR, ADR-0047). Leader-only:
// a follower's snapshot can lag arbitrarily, and a backup that
// silently dropped recent writes is worse than a failed RPC.
func (g *GRPCServer) SnapshotSave(_ *pb.SnapshotSaveRequest, stream pb.Coordinator_SnapshotSaveServer) error {
	if !g.c.IsLeader() {
		return status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	// SnapshotSave hands out the full coordinator state — admin-only
	// under mTLS (ADR-0048).
	if err := g.requireAdminIdentity(stream.Context(), "SnapshotSave"); err != nil {
		return err
	}
	meta, src, err := g.c.openLatestSnapshot()
	if err != nil {
		return status.Errorf(codes.Unavailable, "SnapshotSave: %v", err)
	}
	if meta == nil {
		return status.Error(codes.NotFound, "SnapshotSave: no snapshot available yet")
	}
	defer func() { _ = src.Close() }()

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return status.Errorf(codes.Internal, "SnapshotSave: marshal meta: %v", err)
	}
	if err := stream.Send(&pb.SnapshotSaveChunk{Payload: &pb.SnapshotSaveChunk_MetaJson{MetaJson: metaJSON}}); err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.SnapshotSaveChunk{Payload: &pb.SnapshotSaveChunk_Data{Data: buf[:n]}}); err != nil {
				return err
			}
		}
		if errors.Is(rerr, io.EOF) {
			return nil
		}
		if rerr != nil {
			return status.Errorf(codes.Unavailable, "SnapshotSave: read snapshot: %v", rerr)
		}
	}
}

// ListDomainAssignments returns the topology-domain → shard mapping.
// Same leader-only contract as ListShards.
func (g *GRPCServer) ListDomainAssignments(ctx context.Context, _ *pb.ListDomainAssignmentsRequest) (*pb.ListDomainAssignmentsResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	if err := g.requireReadIdentity(ctx, "ListDomainAssignments"); err != nil {
		return nil, err
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

// ListShardReports returns the coordinator's leader-local soft-state
// snapshot per shard (ADR-0060): the latest ShardSummary + top-N Shortfall
// it accumulated from each shard's ReportShard. Read path; leader-only +
// read-gated. Leader-local soft state — empty right after a failover until
// shards re-report; each snapshot carries received_at so callers can label
// freshness. Reconstructed from the same g.latestSummary / g.latestShortfalls
// the LatestSummary / LatestShortfalls accessors expose, read here directly
// under g.mu (those accessors take g.mu, so calling them here would deadlock).
func (g *GRPCServer) ListShardReports(ctx context.Context, req *pb.ListShardReportsRequest) (*pb.ListShardReportsResponse, error) {
	if !g.c.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, "coordinator: not leader")
	}
	if err := g.requireReadIdentity(ctx, "ListShardReports"); err != nil {
		return nil, err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	want := ShardID(req.GetShardId())
	idSet := make(map[ShardID]struct{}, len(g.latestSummary))
	for id := range g.latestSummary {
		idSet[id] = struct{}{}
	}
	for id := range g.latestShortfalls {
		idSet[id] = struct{}{}
	}
	ids := make([]ShardID, 0, len(idSet))
	for id := range idSet {
		if want != "" && id != want {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	out := make([]*pb.ShardReportSnapshot, 0, len(ids))
	for _, id := range ids {
		snap := &pb.ShardReportSnapshot{ShardId: string(id)}
		if s, ok := g.latestSummary[id]; ok {
			snap.Cycle = s.Cycle
			snap.ReceivedAtUnixNs = s.ReceivedAt.UnixNano()
			snap.Summary = &pb.ShardSummary{
				TotalMachines:         s.TotalMachines,
				FreeMachines:          s.FreeMachines,
				PerInstanceTypeCounts: cloneIntMap(s.InstanceTypeCounts),
				PerZoneCounts:         cloneIntMap(s.ZoneCounts),
				ProviderAddress:       s.ProviderAddress,
			}
		}
		for _, sf := range g.latestShortfalls[id] {
			// requirements are not retained in soft state (see proto doc).
			snap.Shortfalls = append(snap.Shortfalls, &pb.Shortfall{
				Deficit:                   &pb.Resources{Resources: cloneStrMap(sf.Deficit)},
				Priority:                  sf.Priority,
				AgeCycles:                 sf.AgeCycles,
				InterruptionPenaltyBucket: sf.InterruptionPenaltyBucket,
			})
		}
		out = append(out, snap)
	}
	return &pb.ListShardReportsResponse{Reports: out}, nil
}

func cloneStrMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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
