// Package shard implements the BigFleet shard controller — Tier 2 of
// the two-tier hierarchy described in the BigFleet paper §6.
//
// A shard owns a slice of the fleet's machines and a fixed set of
// clusters. Its hot path:
//
//  1. Operator opens a Shard.Session bidi stream and pushes
//     ClusterCapacityNeeds rollups (full replacement per cluster).
//  2. Each cycle (default 10s, can be event-driven on rollup arrival),
//     the shard takes a snapshot of the NeedsTable and the inventory,
//     runs Phase 1 / 2 / 3 from pkg/decision, and executes the resulting
//     actions through the configured CapacityProvider.
//  3. Outbound instructions to the operator (BootstrapRequest, Reclaim,
//     NodeStateUpdate, AvailableCapacityUpdate) flow back down the same
//     stream via a per-cluster outbox.
//
// Static stability is the load-bearing safety property: the shard keeps
// running with the global coordinator down. There is no hot-path
// dependency from this package on pkg/coordinator (see CLAUDE.md).
package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider"
)

// Config configures a Shard.
type Config struct {
	// ID is this shard's stable identifier.
	ID string

	// Epoch is the per-process epoch used in fencing tokens.
	Epoch *fencing.Epoch

	// Provider is the capacity backend.
	Provider provider.Provider

	// CycleInterval is how often the worker loop runs in the absence of
	// rollup-triggered wakeups. Default 10s.
	CycleInterval time.Duration

	// Phase2Options tunes victim scoring.
	Phase2Options decision.Phase2Options

	// BootstrapTimeout caps how long the shard waits for a
	// BootstrapBlobResponse from an operator before giving up on a
	// Configure action this cycle. Default 30s.
	BootstrapTimeout time.Duration

	// Logger receives structured events. nil → discard.
	Logger *slog.Logger

	// OnActions, if set, is called with the union of actions emitted
	// each cycle. Intended for the simulator's trace capture. Must
	// not block; the cycle path is the hot path. nil → no-op.
	OnActions func([]decision.Action)

	// LocalBootstrap, if set, is called by the executor to produce a
	// kubelet bootstrap blob in lieu of round-tripping through the
	// operator's Shard.Session stream. Used by the simulator and by
	// any test that wants to exercise the engine without an operator.
	// Production deployments leave this nil so the operator stream
	// remains the canonical source.
	LocalBootstrap func(ctx context.Context, cluster machine.ClusterID, requirements []needs.Requirement) ([]byte, error)
}

// Shard is the running controller. Construct via New, then Run.
type Shard struct {
	pb.UnimplementedShardServer

	cfg Config

	needs *needs.Table
	inv   *inventory.Inventory
	term  *fencing.CoordinatorTerm

	// sessionsByCluster maps a cluster ID to the active operator session,
	// or nil if no operator is currently connected. At most one session
	// per cluster — a new connection replaces any prior one.
	sessionsMu        sync.Mutex
	sessionsByCluster map[machine.ClusterID]*operatorSession

	// wakeup is buffered; rollup arrivals send a non-blocking wake-up
	// to trigger an immediate cycle. Multiple closely-spaced wake-ups
	// collapse into a single cycle.
	wakeup chan struct{}

	// shortfalls tracks unresolved demand across cycles for reporting
	// up to the coordinator. Keyed by profile fingerprint; aged in
	// cycles. Bounded to keep the report compact (top-N by priority).
	shortfallMu sync.Mutex
	shortfalls  map[string]*shortfallEntry

	// assignedDomains is the set of topology domains the coordinator
	// has assigned to this shard. Empty = take everything (dev mode).
	// Mutated by AssignDomain / UnassignDomain instruction handlers.
	domainsMu       sync.Mutex
	assignedDomains map[domainKey]struct{}

	log *slog.Logger
}

// domainKey is a (label-key, label-value) tuple identifying a topology
// domain assigned to this shard.
type domainKey struct {
	Key   string
	Value string
}

// shortfallEntry is one tracked unresolved need.
type shortfallEntry struct {
	Profile                   needs.Profile
	Count                     int
	AgeCycles                 int
	InterruptionPenaltyBucket needs.PenaltyBucket
}

// New constructs a Shard from cfg. Returns an error if required fields
// are missing.
func New(cfg Config) (*Shard, error) {
	if cfg.ID == "" {
		return nil, errors.New("shard: Config.ID is required")
	}
	if cfg.Provider == nil {
		return nil, errors.New("shard: Config.Provider is required")
	}
	if cfg.Epoch == nil {
		return nil, errors.New("shard: Config.Epoch is required")
	}
	if cfg.CycleInterval == 0 {
		cfg.CycleInterval = 10 * time.Second
	}
	if cfg.BootstrapTimeout == 0 {
		cfg.BootstrapTimeout = 30 * time.Second
	}
	if cfg.Phase2Options.EstimatedDrainDuration == 0 && cfg.Phase2Options.Weights == (decision.VictimWeights{}) {
		cfg.Phase2Options = decision.DefaultPhase2Options()
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	return &Shard{
		cfg:               cfg,
		needs:             needs.NewTable(),
		inv:               inventory.New(),
		term:              fencing.NewCoordinatorTerm(),
		sessionsByCluster: make(map[machine.ClusterID]*operatorSession),
		wakeup:            make(chan struct{}, 1),
		shortfalls:        make(map[string]*shortfallEntry),
		assignedDomains:   make(map[domainKey]struct{}),
		log:               log.With("component", "shard", "shard_id", cfg.ID, "epoch", cfg.Epoch.Value()),
	}, nil
}

// ID returns the shard's identifier.
func (s *Shard) ID() string { return s.cfg.ID }

// Epoch returns the shard's per-process epoch.
func (s *Shard) Epoch() int64 { return s.cfg.Epoch.Value() }

// Run drives the worker loop until ctx is cancelled. Cycles fire on the
// configured interval and on rollup-triggered wake-ups.
func (s *Shard) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.CycleInterval)
	defer ticker.Stop()

	s.log.Info("shard started")
	defer s.log.Info("shard stopped")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.runCycle(ctx)
		case <-s.wakeup:
			s.runCycle(ctx)
		}
	}
}

// triggerCycle requests an immediate cycle. Non-blocking; coalesces
// repeated calls.
func (s *Shard) triggerCycle() {
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
}

// Step runs exactly one cycle synchronously and returns the actions
// emitted across all three phases. Used by the simulator to drive a
// shard through a scenario without a real time.Ticker. The Run loop
// calls runCycle (which wraps Step) for production scheduling.
func (s *Shard) Step(ctx context.Context) []decision.Action {
	return s.runCycleCapturing(ctx)
}

// runCycle is the body of one worker cycle. Snapshot, decide, execute,
// reconcile. Errors during action execution are logged but do not abort
// the cycle — the next cycle will retry naturally.
func (s *Shard) runCycle(ctx context.Context) {
	_ = s.runCycleCapturing(ctx)
}

// runCycleCapturing runs the cycle and returns the union of actions
// emitted across phases. Shared by Run (production) and Step (simulator).
func (s *Shard) runCycleCapturing(ctx context.Context) []decision.Action {
	cycleStart := time.Now()
	defer func() {
		metrics.ShardCycleDuration.Observe(time.Since(cycleStart).Seconds())
	}()
	cycleCtx, cancel := context.WithTimeout(ctx, s.cfg.CycleInterval)
	defer cancel()

	// Reconcile inventory against the provider before deciding so we're
	// not making decisions against a stale view.
	if err := s.reconcile(cycleCtx); err != nil {
		s.log.Warn("reconcile failed", "err", err)
	}

	snap := s.inv.Snapshot()
	demand := s.needs.Snapshot()

	p1 := decision.Phase1(snap, demand)
	p2 := decision.Phase2(snap, p1.Unsatisfied, s.cfg.Phase2Options)
	p3 := decision.Phase3(snap, demand)

	for _, a := range p1.Actions {
		if err := s.execute(cycleCtx, a); err != nil {
			s.log.Warn("action failed", "kind", a.Kind, "machine", a.MachineID, "cluster", a.Cluster, "err", err)
		}
	}
	for _, a := range p2.Actions {
		if err := s.execute(cycleCtx, a); err != nil {
			s.log.Warn("action failed", "kind", a.Kind, "machine", a.MachineID, "cluster", a.Cluster, "err", err)
		}
	}
	for _, a := range p3.Actions {
		if err := s.execute(cycleCtx, a); err != nil {
			s.log.Warn("action failed", "kind", a.Kind, "machine", a.MachineID, "cluster", a.Cluster, "err", err)
		}
	}

	// Persist any unresolved needs as shortfalls for the next
	// ReportShard call. Phase 2 returns the post-preempt residual;
	// anything still here cannot be resolved within this shard.
	seeds := make([]shortfallSeed, 0, len(p2.Unresolved))
	for _, u := range p2.Unresolved {
		seeds = append(seeds, shortfallSeed{Profile: u.Need.Profile, Count: u.Deficit})
	}
	s.recordShortfalls(seeds)

	// Return the union of emitted actions for Step's caller. Cheap to
	// build — the action slices are small relative to the cycle's
	// real cost (provider RPCs, transitions).
	all := make([]decision.Action, 0, len(p1.Actions)+len(p2.Actions)+len(p3.Actions))
	all = append(all, p1.Actions...)
	all = append(all, p2.Actions...)
	all = append(all, p3.Actions...)
	for _, a := range all {
		metrics.ShardActionsTotal.WithLabelValues(a.Kind.String()).Inc()
	}
	// Inventory state gauge.
	stateCounts := map[machine.State]int{}
	for _, m := range snap.All() {
		stateCounts[m.State]++
	}
	for st := machine.StateSpeculative; st <= machine.StateFailed; st++ {
		metrics.ShardInventoryMachines.WithLabelValues(st.String()).Set(float64(stateCounts[st]))
	}
	metrics.ShardShortfalls.Set(float64(len(s.Shortfalls())))
	if s.cfg.OnActions != nil {
		s.cfg.OnActions(all)
	}
	return all
}

// applyTransition advances a machine to the given state in the inventory,
// validating against the state machine. Used by the executors to walk
// transitional states.
func (s *Shard) applyTransition(id machine.ID, target machine.State, mut func(*machine.Machine)) error {
	cur, err := s.inv.Get(id)
	if err != nil {
		// Machine not yet in inventory — insert if the target is a
		// valid initial state.
		fresh := machine.Machine{ID: id, State: target}
		if mut != nil {
			mut(&fresh)
		}
		return s.inv.Insert(fresh)
	}
	if cur.State == target {
		if mut != nil {
			mut(&cur)
			return s.inv.Apply(cur)
		}
		return nil
	}
	cur.State = target
	if mut != nil {
		mut(&cur)
	}
	return s.inv.Apply(cur)
}

// SeedInventory inserts a machine straight into the shard's inventory.
// Used during startup to import a provider's existing inventory and
// during component tests to set up scenarios without a full reconcile.
func (s *Shard) SeedInventory(m machine.Machine) error {
	return s.inv.Insert(m)
}

// NeedsTable returns the shard's NeedsTable. Exposed for tests and for
// the Session server to drive Replace from inbound rollups.
func (s *Shard) NeedsTable() *needs.Table { return s.needs }

// Inventory returns the shard's inventory. Exposed for tests and for
// future reconcilers.
func (s *Shard) Inventory() *inventory.Inventory { return s.inv }

// CoordinatorTerm exposes the term high-water mark tracker. The
// coordinator client uses this to validate inbound instructions.
func (s *Shard) CoordinatorTerm() *fencing.CoordinatorTerm { return s.term }

// discardWriter is an io.Writer used by the default no-op logger.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// formatErr is a thin helper to wrap context-cancellation errors cleanly.
func formatErr(prefix string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", prefix, err)
}
