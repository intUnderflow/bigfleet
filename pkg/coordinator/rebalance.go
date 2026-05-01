package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sort"
	"time"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// RebalancerConfig configures the rebalance loop.
type RebalancerConfig struct {
	// Interval is how often the rebalancer runs. Default 5s.
	Interval time.Duration

	// Logger receives structured events.
	Logger *slog.Logger
}

// Rebalancer scans the coordinator's soft state for shortfalls and
// emits rebalancing instructions. M6.4 ships the simplest meaningful
// loop: for each shortfall, pick a donor shard (max FreeMachines) and
// emit a TransferOwnership instruction. The shard-side adapter
// currently no-ops the instruction; richer rebalance semantics (real
// machine moves, AssignDomain assignments, cross-shard drain under
// priority pressure) land in subsequent milestones.
//
// This loop runs only on the leader. It re-checks IsLeader on each
// tick so a stepdown stops emitting instructions.
type Rebalancer struct {
	cfg RebalancerConfig
	c   *Coordinator
	srv *GRPCServer
	log *slog.Logger
}

// NewRebalancer constructs a Rebalancer.
func NewRebalancer(c *Coordinator, srv *GRPCServer, cfg RebalancerConfig) *Rebalancer {
	if cfg.Interval == 0 {
		cfg.Interval = 5 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Rebalancer{
		cfg: cfg,
		c:   c,
		srv: srv,
		log: log.With("component", "rebalancer"),
	}
}

// Run drives the rebalance loop until ctx is cancelled.
func (r *Rebalancer) Run(ctx context.Context) error {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	r.log.Info("rebalancer started", "interval", r.cfg.Interval)
	defer r.log.Info("rebalancer stopped")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			r.runOnce()
		}
	}
}

// runOnce performs one rebalance pass. Idempotent — duplicate
// instructions are deduped by the shard via instruction_id.
func (r *Rebalancer) runOnce() {
	if !r.c.IsLeader() {
		return
	}
	term := r.c.RaftTerm()

	// Collect all shards with at least one outstanding shortfall and
	// all shards' free-capacity summaries. M6.4 uses FreeMachines as
	// the simple proxy for "has spare capacity to donate"; future
	// milestones will price by instance type, zone, and priority.
	shards := r.c.State().Shards()
	if len(shards) < 2 {
		return // no rebalance possible with a single shard.
	}

	// Build a candidate-donor list sorted by FreeMachines desc.
	type donor struct {
		ID   ShardID
		Free int32
	}
	donors := make([]donor, 0, len(shards))
	for _, s := range shards {
		if sum, ok := r.srv.LatestSummary(s.ID); ok {
			donors = append(donors, donor{ID: s.ID, Free: sum.FreeMachines})
		}
	}
	sort.Slice(donors, func(i, j int) bool { return donors[i].Free > donors[j].Free })

	// For each shard with shortfalls, look for a donor with FreeMachines.
	for _, s := range shards {
		shortfalls := r.srv.LatestShortfalls(s.ID)
		if len(shortfalls) == 0 {
			continue
		}
		// Pick the highest-shortfall priority as the trigger.
		topPriority := int32(0)
		for _, sf := range shortfalls {
			if sf.Priority > topPriority {
				topPriority = sf.Priority
			}
		}
		// Find a donor with positive FreeMachines that isn't this shard.
		var chosen donor
		for _, d := range donors {
			if d.ID == s.ID {
				continue
			}
			if d.Free > 0 {
				chosen = d
				break
			}
		}
		if chosen.ID == "" {
			continue // no donor; nothing to rebalance.
		}

		// Emit a TransferOwnership instruction. machine_ids is empty
		// in M6.4 — the shard adapter no-ops it. Future milestones
		// will populate concrete IDs after a donor-side query.
		instr := &pb.CoordinatorInstruction{
			CoordinatorTerm: term,
			SequenceNumber:  r.c.FSM().NextSequence(),
			InstructionId:   mintInstructionID(),
			Payload: &pb.CoordinatorInstruction_TransferOwnership{
				TransferOwnership: &pb.TransferOwnership{
					FromShardId: string(chosen.ID),
					ToShardId:   string(s.ID),
				},
			},
		}
		// Send to both donor (for the eventual release) and recipient
		// (for the eventual claim). M6.4 stubs both sides; subsequent
		// milestones implement real semantics.
		_ = r.srv.EnqueueInstruction(chosen.ID, instr)
		_ = r.srv.EnqueueInstruction(s.ID, instr)
		r.log.Info("rebalance: TransferOwnership emitted",
			"from", chosen.ID, "to", s.ID,
			"from_free", chosen.Free, "to_priority", topPriority,
			"instruction_id", instr.GetInstructionId(),
		)
	}
}

// mintInstructionID returns a 16-character hex id.
func mintInstructionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
