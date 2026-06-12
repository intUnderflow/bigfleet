// Package coordclient is the shard-side adapter that talks to the
// global coordinator over gRPC. It runs in its own goroutine, lives
// in a sub-package the shard's hot path does not import, and is
// optional: the shard keeps operating against its existing
// allocations when the coordinator is unreachable.
//
// This is the *only* code on the shard side that knows about the
// coordinator. Static stability is the load-bearing safety property
// (CLAUDE.md hard rules); pkg/shard does not import pkg/coordinator,
// and a programmatic guard in pkg/shard/no_coordinator_dep_test.go
// keeps it that way.
package coordclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// Config configures the coordinator client.
type Config struct {
	// CoordinatorAddress is the gRPC endpoint of the coordinator.
	CoordinatorAddress string

	// AdvertiseAddress is the host:port the coordinator should record
	// as this shard's dial address (stamped on every ShardReport as
	// shard_address). On first sight of an unknown shard_id, the
	// coordinator Raft-Apply's AddShard with this address — that's how
	// multi-shard deploys self-register without a separate RPC.
	// Optional: when empty, the coordinator records an empty address
	// (still useful for membership / domain assignment, just less
	// helpful for admin tooling that wants to dial the shard).
	AdvertiseAddress string

	// View is the shard surface the client reports on and dispatches
	// instructions back to. Tests can substitute a stub.
	View ShardView

	// CoordinatorTerm is the shard's high-water mark of observed
	// coordinator terms. Used to reject stale instructions. Provided
	// by the shard so it can be queried by other components too.
	CoordinatorTerm *fencing.CoordinatorTerm

	// ReportInterval is how often the client calls ReportShard.
	// Default 30s per the paper.
	ReportInterval time.Duration

	// TLS configures mTLS for the coordinator dial (ADR-0048). The
	// certificate must carry the URI SAN bigfleet://shard/<View.ID()>
	// — the coordinator binds ShardReport.shard_id to it. Zero value
	// = plaintext.
	TLS grpcutil.TLSConfig

	// Logger receives structured events. nil → default.
	Logger *slog.Logger
}

// ShardView is the surface the client uses against the shard. Methods
// must be safe for concurrent use.
type ShardView interface {
	// Identity.
	ID() string
	Epoch() int64

	// Periodic state for reports.
	Summary() ShardSummary
	Shortfalls() []ShardShortfall

	// Instruction handlers. Return nil on success; the client wraps
	// the error in an InstructAck with the appropriate outcome.
	OnAssignDomain(key, value string) error
	OnUnassignDomain(key, value string) error
	OnReassignSpeculative(machineIDs []string) error
	OnCrossShardDrain(machineIDs []string, preemptorPriority int32) error
	OnTransferOwnership(machineIDs []string, fromShard, toShard string) error
}

// ShardSummary mirrors pb.ShardSummary in domain form.
type ShardSummary struct {
	TotalMachines      int32
	FreeMachines       int32
	InstanceTypeCounts map[string]int32
	ZoneCounts         map[string]int32
	UtilCPUFraction    float64
	UtilMemoryFraction float64
}

// ShardShortfall mirrors pb.Shortfall in domain form.
//
// ADR-0027: Deficit is the residual aggregate resource demand — a
// resource vector. The old (Resources, Count) pair is gone.
type ShardShortfall struct {
	Requirements              []ShortfallRequirement
	Deficit                   map[string]string
	Priority                  int32
	AgeCycles                 int32
	InterruptionPenaltyBucket pb.PenaltyBucket
}

// ShortfallRequirement is a NodeSelectorRequirement in domain form.
type ShortfallRequirement struct {
	Key      string
	Operator string
	Values   []string
}

// Client polls the coordinator and dispatches inbound instructions.
type Client struct {
	cfg Config
	log *slog.Logger

	mu               sync.Mutex
	cycle            int64
	pendingAcks      []*pb.InstructAck
	doneInstructions map[string]struct{} // dedup completed instruction_ids
}

// New constructs a Client. Validates required fields.
func New(cfg Config) (*Client, error) {
	if cfg.CoordinatorAddress == "" {
		return nil, errors.New("coordclient: Config.CoordinatorAddress required")
	}
	if cfg.View == nil {
		return nil, errors.New("coordclient: Config.View required")
	}
	if cfg.CoordinatorTerm == nil {
		return nil, errors.New("coordclient: Config.CoordinatorTerm required")
	}
	if err := cfg.TLS.Validate(); err != nil {
		return nil, fmt.Errorf("coordclient: %w", err)
	}
	if cfg.ReportInterval == 0 {
		cfg.ReportInterval = 30 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		cfg:              cfg,
		log:              log.With("component", "coordclient", "shard_id", cfg.View.ID()),
		doneInstructions: make(map[string]struct{}),
	}, nil
}

// Run dials the coordinator and runs the report loop until ctx is
// cancelled. Reconnects with backoff on transport errors. The shard
// keeps operating against its existing allocations regardless of
// whether this loop is making progress.
func (c *Client) Run(ctx context.Context) error {
	// ADR-0048: mTLS when Config.TLS is set, plaintext otherwise.
	dialOpts, err := c.cfg.TLS.DialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(c.cfg.CoordinatorAddress, dialOpts...)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	cli := pb.NewCoordinatorClient(conn)

	c.log.Info("coord client started", "coord", c.cfg.CoordinatorAddress, "interval", c.cfg.ReportInterval)
	defer c.log.Info("coord client stopped")

	// Fire one immediately on connect so the coordinator sees the
	// shard without waiting for the first tick.
	c.runOnce(ctx, cli)

	t := time.NewTicker(c.cfg.ReportInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.runOnce(ctx, cli)
		}
	}
}

// runOnce performs one ReportShard cycle: build a report, send, dispatch
// any inbound instructions.
func (c *Client) runOnce(ctx context.Context, cli pb.CoordinatorClient) {
	c.mu.Lock()
	c.cycle++
	cycle := c.cycle
	acks := c.pendingAcks
	c.pendingAcks = nil
	c.mu.Unlock()

	view := c.cfg.View
	report := &pb.ShardReport{
		ShardId:            view.ID(),
		ShardAddress:       c.cfg.AdvertiseAddress,
		Cycle:              cycle,
		TimestampUnixNanos: time.Now().UnixNano(),
		ShardEpoch:         view.Epoch(),
		Summary:            summaryToProto(view.Summary()),
		Shortfalls:         shortfallsToProto(view.Shortfalls()),
		InstructionAcks:    acks,
	}

	resp, err := cli.ReportShard(ctx, report)
	if err != nil {
		c.log.Warn("ReportShard failed", "err", err)
		// Re-queue any acks we tried to send so the next cycle picks
		// them up. Bound is fine — pendingAcks is small.
		c.mu.Lock()
		c.pendingAcks = append(acks, c.pendingAcks...)
		c.mu.Unlock()
		return
	}

	// Validate the coordinator term against the high-water mark.
	if t := resp.GetCoordinatorTerm(); t > 0 {
		if err := c.cfg.CoordinatorTerm.Validate(t); err != nil {
			c.log.Warn("coordinator term validation failed", "err", err)
			// Don't process the instructions — they'll come again
			// once the term mismatch resolves.
			return
		}
	}

	for _, instr := range resp.GetInstructions() {
		c.handleInstruction(instr)
	}
}

// handleInstruction validates fencing on a single instruction, runs
// the matching ShardView handler, and queues an InstructAck for the
// next ReportShard call. Idempotent against duplicate deliveries —
// the coordinator may send the same instruction across multiple
// reports until it sees an ack.
func (c *Client) handleInstruction(instr *pb.CoordinatorInstruction) {
	if instr == nil || instr.GetInstructionId() == "" {
		return
	}
	id := instr.GetInstructionId()

	// Dedup: if we've already executed this instruction in a prior
	// cycle, just re-ack it.
	c.mu.Lock()
	_, alreadyDone := c.doneInstructions[id]
	c.mu.Unlock()
	if alreadyDone {
		c.queueAck(id, pb.InstructAck_OUTCOME_ACCEPTED, "duplicate-redelivery")
		return
	}

	// Term fence: reject anything older than our high-water mark.
	if err := c.cfg.CoordinatorTerm.Validate(instr.GetCoordinatorTerm()); err != nil {
		c.queueAck(id, pb.InstructAck_OUTCOME_REJECTED_STALE, err.Error())
		return
	}

	view := c.cfg.View
	var execErr error
	switch p := instr.GetPayload().(type) {
	case *pb.CoordinatorInstruction_AssignDomain:
		execErr = view.OnAssignDomain(p.AssignDomain.GetTopologyKey(), p.AssignDomain.GetTopologyValue())
	case *pb.CoordinatorInstruction_UnassignDomain:
		execErr = view.OnUnassignDomain(p.UnassignDomain.GetTopologyKey(), p.UnassignDomain.GetTopologyValue())
	case *pb.CoordinatorInstruction_ReassignSpeculative:
		execErr = view.OnReassignSpeculative(p.ReassignSpeculative.GetMachineIds())
	case *pb.CoordinatorInstruction_CrossShardDrain:
		execErr = view.OnCrossShardDrain(p.CrossShardDrain.GetMachineIds(), p.CrossShardDrain.GetPreemptorPriority())
	case *pb.CoordinatorInstruction_TransferOwnership:
		execErr = view.OnTransferOwnership(p.TransferOwnership.GetMachineIds(),
			p.TransferOwnership.GetFromShardId(), p.TransferOwnership.GetToShardId())
	default:
		c.queueAck(id, pb.InstructAck_OUTCOME_REJECTED_INVALID, "unknown payload")
		return
	}

	if execErr != nil {
		c.queueAck(id, pb.InstructAck_OUTCOME_FAILED, execErr.Error())
		return
	}
	c.mu.Lock()
	c.doneInstructions[id] = struct{}{}
	c.mu.Unlock()
	c.queueAck(id, pb.InstructAck_OUTCOME_ACCEPTED, "")
}

func (c *Client) queueAck(id string, outcome pb.InstructAck_Outcome, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingAcks = append(c.pendingAcks, &pb.InstructAck{
		InstructionId: id,
		Outcome:       outcome,
		Reason:        reason,
	})
}

func summaryToProto(s ShardSummary) *pb.ShardSummary {
	return &pb.ShardSummary{
		TotalMachines:             s.TotalMachines,
		FreeMachines:              s.FreeMachines,
		PerInstanceTypeCounts:     s.InstanceTypeCounts,
		PerZoneCounts:             s.ZoneCounts,
		UtilisationCpuFraction:    s.UtilCPUFraction,
		UtilisationMemoryFraction: s.UtilMemoryFraction,
	}
}

func shortfallsToProto(in []ShardShortfall) []*pb.Shortfall {
	if len(in) == 0 {
		return nil
	}
	out := make([]*pb.Shortfall, 0, len(in))
	for _, s := range in {
		var deficit *pb.Resources
		if len(s.Deficit) > 0 {
			deficit = &pb.Resources{Resources: s.Deficit}
		}
		reqs := make([]*pb.NodeSelectorRequirement, 0, len(s.Requirements))
		for _, r := range s.Requirements {
			reqs = append(reqs, &pb.NodeSelectorRequirement{
				Key:      r.Key,
				Operator: parseOperator(r.Operator),
				Values:   r.Values,
			})
		}
		out = append(out, &pb.Shortfall{
			Requirements:              reqs,
			Deficit:                   deficit,
			Priority:                  s.Priority,
			AgeCycles:                 s.AgeCycles,
			InterruptionPenaltyBucket: s.InterruptionPenaltyBucket,
		})
	}
	return out
}

// parseOperator turns a user-facing string back into the proto enum.
// Reuses the same names the operator's bootstrap responder emits;
// keeps the wire format consistent.
func parseOperator(s string) pb.NodeSelectorRequirement_Operator {
	switch s {
	case "In":
		return pb.NodeSelectorRequirement_OPERATOR_IN
	case "NotIn":
		return pb.NodeSelectorRequirement_OPERATOR_NOT_IN
	case "Exists":
		return pb.NodeSelectorRequirement_OPERATOR_EXISTS
	case "DoesNotExist":
		return pb.NodeSelectorRequirement_OPERATOR_DOES_NOT_EXIST
	case "Same":
		return pb.NodeSelectorRequirement_OPERATOR_SAME
	}
	return pb.NodeSelectorRequirement_OPERATOR_UNSPECIFIED
}
