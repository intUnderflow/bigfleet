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

	"github.com/intUnderflow/bigfleet/pkg/conv"
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

	// MaxActionsPerCycle caps the total number of decision actions
	// (Bootstrap + Provision + Reclaim + Preempt) the shard will
	// execute in one cycle. 0 = unlimited (default, preserves prior
	// behaviour). Phases run to completion regardless; only the
	// execute step honours the cap.
	//
	// When a ramp lands a large demand burst, an unlimited cycle does
	// O(actions) provider RPCs + inventory transitions, blowing past
	// the cycle SLO. With a cap, the surplus actions naturally roll
	// into the next cycle: Phase 1/2/3 are idempotent given the same
	// snapshot, so the next cycle picks up where this one left off.
	MaxActionsPerCycle int

	// ExecuteConcurrency caps the number of action executors running
	// in parallel within a cycle. 0 / 1 = serial (historical default).
	// Each Bootstrap action blocks on a per-cluster gRPC stream RTT
	// (`requestBootstrap`), so a serial loop multiplies the cycle
	// wall-clock by stream RTT × action count regardless of how cheap
	// the local compute is. The operator session, inventory, and fake
	// provider are all thread-safe; raise this for any workload where
	// a ramp burst dwarfs steady-state churn.
	ExecuteConcurrency int

	// IncrementalReconcile opts into delta-only provider.List polling
	// using the SinceRevision cursor. Off by default — reconcile then
	// performs a full List every cycle and walks the snapshot to find
	// removals (correct for any provider). On, the shard pumps the
	// cursor across cycles and only processes machines mutated since
	// the last call; the snapshot-walk for removals is skipped, so
	// providers that genuinely remove machines from inventory must
	// emit tombstones (not yet wired — defer until a real provider
	// needs it). Plan §10.6 frames this as the "above-conformance-
	// threshold" path; only enable for providers that honour
	// since_revision.
	IncrementalReconcile bool
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

	// reconcileCursor is the opaque revision returned by the most
	// recent provider.List response. Pumped back into ListFilter.
	// SinceRevision on the next cycle when Config.IncrementalReconcile
	// is set. Only the cycle goroutine reads/writes this; no lock.
	reconcileCursor []byte

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
	reconcileStart := time.Now()
	if err := s.reconcile(cycleCtx); err != nil {
		s.log.Warn("reconcile failed", "err", err)
	}
	metrics.ShardCyclePhaseDuration.WithLabelValues("reconcile").Observe(time.Since(reconcileStart).Seconds())

	// CycleSnapshot returns the cached pointer from the inventory's
	// background fold loop in O(1). Stale by at most foldDebounce +
	// buildTime; safe because applyTransition rejects re-attempts on
	// already-moved machines and Phase 1/2/3 re-derive any missed
	// actions next cycle.
	snap := s.inv.CycleSnapshot()
	demand := s.needs.Snapshot()

	p1Start := time.Now()
	p1 := decision.Phase1(snap, demand)
	metrics.ShardCyclePhaseDuration.WithLabelValues("phase1").Observe(time.Since(p1Start).Seconds())

	p2Start := time.Now()
	p2 := decision.Phase2(snap, p1.Unsatisfied, s.cfg.Phase2Options)
	metrics.ShardCyclePhaseDuration.WithLabelValues("phase2").Observe(time.Since(p2Start).Seconds())

	p3Start := time.Now()
	p3 := decision.Phase3(snap, demand)
	metrics.ShardCyclePhaseDuration.WithLabelValues("phase3").Observe(time.Since(p3Start).Seconds())

	// Collapse all phases' actions into one queue. Phase 1/2/3 compute
	// on the same snapshot, so their actions are independent; ordering
	// between them doesn't matter at execute time. MaxActionsPerCycle
	// caps total per-cycle work so a ramp burst doesn't blow the SLO;
	// surplus actions defer to the next cycle (phases re-derive them
	// since they're idempotent against an unchanged snapshot).
	all := make([]decision.Action, 0, len(p1.Actions)+len(p2.Actions)+len(p3.Actions))
	all = append(all, p1.Actions...)
	all = append(all, p2.Actions...)
	all = append(all, p3.Actions...)

	limit := s.cfg.MaxActionsPerCycle
	deferred := 0
	if limit > 0 && len(all) > limit {
		deferred = len(all) - limit
		all = all[:limit]
	}

	// Parallel execute: each Bootstrap action blocks on a per-cluster
	// gRPC stream RTT (`requestBootstrap`), so a serial loop turns the
	// cycle into N × stream RTT regardless of how cheap the local
	// compute is. The operator session supports concurrent in-flight
	// requests (sync.Map keyed by request_id), so we just need the
	// shard to fire them in parallel. Inventory.Apply, the fake
	// provider, and the operator session are all thread-safe.
	conc := s.cfg.ExecuteConcurrency
	if conc <= 0 {
		conc = 1 // serial — preserves the historical default
	}
	if conc > len(all) {
		conc = len(all)
	}
	if conc > 0 {
		executeStart := time.Now()
		jobs := make(chan decision.Action)
		var wg sync.WaitGroup
		for i := 0; i < conc; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for a := range jobs {
					if err := s.execute(cycleCtx, a); err != nil {
						s.log.Warn("action failed", "kind", a.Kind, "machine", a.MachineID, "cluster", a.Cluster, "err", err)
					}
				}
			}()
		}
		for _, a := range all {
			select {
			case <-cycleCtx.Done():
			case jobs <- a:
			}
		}
		close(jobs)
		wg.Wait()
		metrics.ShardCyclePhaseDuration.WithLabelValues("execute").Observe(time.Since(executeStart).Seconds())
	}
	if deferred > 0 {
		metrics.ShardActionsDeferred.Add(float64(deferred))
		// Schedule an immediate follow-up cycle so deferred work
		// doesn't wait for the next tick. The wakeup channel coalesces
		// repeated triggers; at most one extra cycle gets queued.
		s.triggerCycle()
	}

	// Persist any unresolved needs as shortfalls for the next
	// ReportShard call. Phase 2 returns the post-preempt residual;
	// anything still here cannot be resolved within this shard.
	seeds := make([]shortfallSeed, 0, len(p2.Unresolved))
	for _, u := range p2.Unresolved {
		seeds = append(seeds, shortfallSeed{Profile: u.Need.Profile, Count: u.Deficit})
	}
	s.recordShortfalls(seeds)

	// `all` was already populated above when we built the executor's
	// queue; reuse it for metrics. (We also need to count any deferred
	// actions not in `all` — they show up via ShardActionsDeferred.)
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
// transitional states. After a successful Apply, if the machine has a
// non-empty Cluster binding, emits a NodeStateUpdate to that cluster's
// operator session so the operator can write/refresh the matching
// UpcomingNode CR.
func (s *Shard) applyTransition(id machine.ID, target machine.State, mut func(*machine.Machine)) error {
	cur, err := s.inv.Get(id)
	if err != nil {
		// Machine not yet in inventory — insert if the target is a
		// valid initial state.
		fresh := machine.Machine{ID: id, State: target}
		if mut != nil {
			mut(&fresh)
		}
		if err := s.inv.Insert(fresh); err != nil {
			return err
		}
		s.notifyNodeState(fresh)
		return nil
	}
	if cur.State == target {
		if mut != nil {
			mut(&cur)
			if err := s.inv.Apply(cur); err != nil {
				return err
			}
			s.notifyNodeState(cur)
		}
		return nil
	}
	cur.State = target
	if mut != nil {
		mut(&cur)
	}
	if err := s.inv.Apply(cur); err != nil {
		return err
	}
	s.notifyNodeState(cur)
	return nil
}

// notifyNodeState pushes a NodeStateUpdate to the operator session
// for the machine's bound cluster, if any. Best-effort: a missing or
// disconnected session is silently skipped because the operator will
// reconcile from full state on reconnect.
func (s *Shard) notifyNodeState(m machine.Machine) {
	if m.Cluster == "" {
		return
	}
	sess := s.lookupSession(m.Cluster)
	if sess == nil {
		return
	}
	upd := &pb.NodeStateUpdate{
		MachineId: string(m.ID),
		State:     conv.MachineStateToProto(m.State),
	}
	if !m.Host.Empty() {
		upd.ProviderId = m.Host.Ref
	}
	if m.LastError != "" {
		upd.LastError = m.LastError
	}
	if err := sess.SendNodeStateUpdate(upd); err != nil {
		s.log.Debug("notifyNodeState send failed", "machine", m.ID, "err", err)
	}
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
