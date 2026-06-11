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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
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

	// ExecuteTimeout caps the duration of a single action's execution
	// in the persistent-execute-pool model (ADR-0021). Each worker
	// derives a per-action context with this deadline; cycle deadlines
	// no longer gate in-flight executes. Default 30s — same as
	// BootstrapTimeout, since most action latency is the BootstrapRequest
	// blob fetch.
	ExecuteTimeout time.Duration

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

	// MetricsWarmupCycles skips histogram observations
	// (bigfleet_shard_cycle_duration_seconds and the per-phase
	// histograms) for the first N cycles. Cycle 1 of any shard does a
	// full provider.List as it has no since_revision cursor and an
	// empty inventory; on cold start that's a one-time cost (~700 ms
	// at 500K on M5) that lands forever in the histogram and the
	// 5-min rate window keeps it visible all run. Skipping it lets
	// p99 reflect *steady state*, which is what the SLO is about.
	// Counters (actions, shortfalls, etc.) are not affected.
	// Default 0 = record every cycle (current behaviour).
	MetricsWarmupCycles int

	// AvailableCapacityInterval bounds how often the shard emits an
	// AvailableCapacityUpdate for a given (cluster, fingerprint). The
	// emit is "eventually consistent" by paper §6.2 — the operator
	// surfaces it via an AvailableCapacity CR which a human or a
	// controller polls — so a few-seconds rate limit is harmless and
	// stops the cycle (~10 Hz under load) from rewriting the
	// operator-side apiserver every cycle. Default 5 s. Set to a
	// shorter value if a workload genuinely consumes AC reactively;
	// set very large to effectively disable AC churn from steady
	// state (the coalesce layer still blocks no-change rewrites).
	AvailableCapacityInterval time.Duration

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

	// PhaseAttributionLog enables the ADR-0040 §4 diagnostic: every
	// 20th cycle, one log line with Need counts split by co-location,
	// Phase 1 unsatisfied split by co-location, Phase 3 reclaim count,
	// and the reclaim-matches-unsatisfied probe that found the
	// Same-domain crediting defect. Read-only over the cycle's own
	// snapshot and phase results; zero hot-path cost when off (the
	// default). The shard subcommand wires --phase-attribution-log and
	// the BIGFLEET_PHASEDUMP=1 env var here.
	PhaseAttributionLog bool

	// ActuationPaused is the ADR-0046 global kill switch. When set,
	// the cycle still runs in full — reconcile, phases, shortfall
	// recording, AvailableCapacity emission, metrics — but no action
	// is executed: Bootstrap/Provision/Reclaim/Preempt are counted in
	// ShardActionsSuppressed and dropped. Step/OnActions still surface
	// the decided actions so the engine's intentions stay observable
	// while paused. Static config: flipped by redeploy, never by RPC.
	ActuationPaused bool

	// ReclaimCapFraction is the ADR-0046 per-cluster reclaim
	// blast-radius cap: per cycle, at most
	// max(1, ⌊fraction × cluster's Configured count⌋) Reclaim actions
	// execute; the surplus re-derives next cycle (Phase 3 is
	// idempotent against an unchanged snapshot — roll-over, not
	// drop). Preempts are NOT capped (priority-driven allocation,
	// paper §16). 0 = cap off — the zero value keeps the sim canaries'
	// pathologies undamped; the shard binary defaults the flag to
	// DefaultReclaimCapFraction.
	ReclaimCapFraction float64

	// EmptyRollupGuard enables the ADR-0046 roll-up quarantine: a
	// full-replacement roll-up that would retain <10% of the
	// cluster's previously accepted demand (when that baseline spans
	// ≥10 Need rows) is held until 3 consecutive roll-ups confirm the
	// drop. Quarantine, not reject — genuine mass scale-down proceeds
	// after ~2 roll-up intervals. False = off (zero value, same
	// rationale as ReclaimCapFraction); the binary flag defaults on.
	EmptyRollupGuard bool
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

	// unsatSameAge counts consecutive cycles each Same-Need class
	// (cluster + group + fingerprint) has been Phase 1-unsatisfiable
	// (ADR-0042 Addendum). Aged classes get AcquisitionParked stamped
	// on their Needs before the phases, with a periodic re-probe cycle
	// so parked demand un-parks the moment supply appears. Entries not
	// re-seen in a cycle are deleted, so the map is bounded by the
	// live unsatisfied set. Only the cycle goroutine touches it.
	unsatSameAge map[string]int

	// cycleCount monotonically counts cycles since shard start. Used
	// by the warmup gate on metric observations. atomic so a shard
	// metrics-snapshot read from another goroutine is safe.
	cycleCount atomic.Int64

	// acCache dedups per-(cluster, fingerprint) AvailableCapacity
	// emits so the operator-side apiserver isn't re-written every
	// cycle with the same tuple.
	acCache *availableCapacityCache

	// demandObservedAt records the first-seen time for each (cluster,
	// profile fingerprint) currently in this shard's NeedsTable. Used
	// by the e2e provisioning-latency histogram (paper §10.7): when a
	// machine reaches Configured for a cluster + source-profile
	// fingerprint, we observe the time since the demand was first
	// observed. Entries for fingerprints no longer in a cluster's
	// rollup are pruned at rollup ingest. Reads/writes are guarded
	// by demandMu.
	demandMu         sync.Mutex
	demandObservedAt map[machine.ClusterID]map[string]time.Time

	// actionQueue feeds the persistent execute pool (ADR-0021).
	// Cycles enqueue actions and return immediately; workers drain
	// the queue at their own pace, each holding a per-action context
	// independent of any cycle deadline. Sized at 2 ×
	// ExecuteConcurrency so cycles rarely drop emissions when the
	// pool is keeping up.
	actionQueue chan decision.Action

	// pendingActions tracks machine IDs with an action enqueued or in
	// flight. ADR-0021's persistent pool decouples cycle emit from
	// worker drain, so a machine can stay Idle in the snapshot for
	// multiple cycles before the queued worker picks it up — Phase 1
	// would re-emit each cycle and burn worker capacity processing
	// duplicates that fail with "expected Idle". This set is
	// consulted at enqueue time: if a machine is already pending we
	// drop the duplicate. Workers remove the entry when execute
	// returns (success or failure).
	pendingMu      sync.Mutex
	pendingActions map[machine.ID]struct{}

	// firstRollupReceived gates Phase 3 reclaim per cluster
	// (ADR-0036). Set true the first time any rollup arrives for a
	// cluster — including a rollup with zero Needs. Phase 3 skips
	// reclaim for any cluster whose entry is false, so the install /
	// shard-restart window doesn't drain Configured supply before
	// operators have had a chance to report demand.
	firstRollupMu       sync.RWMutex
	firstRollupReceived map[machine.ClusterID]bool

	// rollupGuard is the ADR-0046 empty-roll-up quarantine state.
	// Consulted by ApplyRollup only when Config.EmptyRollupGuard is
	// set; per-cluster, in-memory (restart window is ADR-0036's).
	rollupGuard rollupGuard

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
	Deficit                   []needs.ResourceQty
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
	if cfg.ExecuteTimeout == 0 {
		cfg.ExecuteTimeout = 30 * time.Second
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
		cfg:                 cfg,
		needs:               needs.NewTable(),
		inv:                 inventory.New(),
		term:                fencing.NewCoordinatorTerm(),
		sessionsByCluster:   make(map[machine.ClusterID]*operatorSession),
		wakeup:              make(chan struct{}, 1),
		shortfalls:          make(map[string]*shortfallEntry),
		unsatSameAge:        make(map[string]int),
		assignedDomains:     make(map[domainKey]struct{}),
		acCache:             newAvailableCapacityCache(cfg.AvailableCapacityInterval),
		demandObservedAt:    make(map[machine.ClusterID]map[string]time.Time),
		pendingActions:      make(map[machine.ID]struct{}),
		firstRollupReceived: make(map[machine.ClusterID]bool),
		log:                 log.With("component", "shard", "shard_id", cfg.ID, "epoch", cfg.Epoch.Value()),
	}, nil
}

// FirstRollupReceived reports whether the cluster has sent at least
// one rollup since this shard process started. ADR-0036: Phase 3
// uses this as a per-cluster gate to skip reclaim during the
// install / shard-restart window where an empty NeedsTable would
// otherwise trigger fleet-drain (the operator hasn't yet had a
// chance to report demand, so empty Needs is "unknown demand",
// not "zero demand").
//
// Set true by the session loop when any RollupReport — including
// one with zero Needs — arrives. Stays true for the cluster's
// lifetime within this shard process; only cleared on shard
// restart (because the map is in-memory).
func (s *Shard) FirstRollupReceived(c machine.ClusterID) bool {
	s.firstRollupMu.RLock()
	defer s.firstRollupMu.RUnlock()
	return s.firstRollupReceived[c]
}

func (s *Shard) markFirstRollupReceived(c machine.ClusterID) {
	s.firstRollupMu.Lock()
	defer s.firstRollupMu.Unlock()
	s.firstRollupReceived[c] = true
}

// ApplyRollup is the shard's roll-up ingest: it replaces the
// cluster's NeedsTable slice and marks the cluster as having reported
// (ADR-0036). Both the session loop and the sim/test runners flow
// through here, so the ADR-0046 empty-roll-up guard has a single
// implementation point: when Config.EmptyRollupGuard is set, a
// full-replacement roll-up that would erase most of the cluster's
// previously accepted demand is quarantined — the previous demand
// stays active — until consecutive roll-ups confirm the drop.
//
// Returns whether the roll-up was applied. A held roll-up still
// clears the ADR-0036 Phase 3 gate: the operator DID report, and the
// demand the shard keeps acting on is real previously-reported state,
// not "unknown".
func (s *Shard) ApplyRollup(c machine.ClusterID, ns []needs.Need) bool {
	defer s.markFirstRollupReceived(c)
	if s.cfg.EmptyRollupGuard {
		v := s.rollupGuard.admit(c, len(ns))
		held := 0.0
		if !v.accepted {
			held = float64(v.held)
		}
		metrics.ShardRollupQuarantined.WithLabelValues(string(c)).Set(held)
		if !v.accepted {
			s.log.Warn("rollup quarantined: full-replacement demand drop held (ADR-0046)",
				"cluster", c,
				"previous_rows", v.prevRows,
				"new_rows", len(ns),
				"held_consecutive", v.held,
				"accepted_after", rollupGuardConsecutive)
			return false
		}
		if v.confirmed {
			s.log.Warn("quarantined rollup accepted: demand drop confirmed by consecutive reports (ADR-0046)",
				"cluster", c,
				"previous_rows", v.prevRows,
				"new_rows", len(ns),
				"consecutive_reports", rollupGuardConsecutive)
		}
	}
	s.needs.Replace(c, ns)
	return true
}

// ID returns the shard's identifier.
func (s *Shard) ID() string { return s.cfg.ID }

// Epoch returns the shard's per-process epoch.
func (s *Shard) Epoch() int64 { return s.cfg.Epoch.Value() }

// Run drives the worker loop until ctx is cancelled. Cycles fire on the
// configured interval and on rollup-triggered wake-ups. ADR-0021: a
// pool of executeWorker goroutines drains the action queue
// independently of cycle ticks.
func (s *Shard) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.CycleInterval)
	defer ticker.Stop()

	conc := s.cfg.ExecuteConcurrency
	if conc <= 0 {
		conc = 1
	}
	// Queue is sized large enough that under healthy steady-state the
	// cycle never blocks on the send. Drops are reported via
	// ShardActionsDropped (separate from ShardActionsDeferred, which
	// remains the MaxActionsPerCycle truncation counter for legacy
	// scenarios where the cycle still caps emissions).
	s.actionQueue = make(chan decision.Action, conc*2)
	for i := 0; i < conc; i++ {
		go s.executeWorker(ctx)
	}

	s.log.Info("shard started", "execute_workers", conc, "queue_capacity", cap(s.actionQueue))
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

// executeWorker drains the per-shard action queue. Each action gets a
// fresh context capped by ExecuteTimeout (default 30 s) — independent
// of the cycle that produced it. ADR-0021: cycle deadlines no longer
// gate in-flight executes, so a slow operator handler delays binding
// instead of cancelling and dropping the work.
func (s *Shard) executeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a, ok := <-s.actionQueue:
			if !ok {
				return
			}
			execCtx, cancel := context.WithTimeout(ctx, s.cfg.ExecuteTimeout)
			if err := s.execute(execCtx, a); err != nil {
				s.log.Warn("action failed", "kind", a.Kind, "machine", a.MachineID, "cluster", a.Cluster, "err", err)
			}
			cancel()
			// Clear pending regardless of success: if the action
			// failed (e.g. transient transition error), the next
			// cycle's Phase 1 will re-derive cleanly with a fresh
			// snapshot.
			s.pendingMu.Lock()
			delete(s.pendingActions, a.MachineID)
			s.pendingMu.Unlock()
			metrics.ShardActionQueueDepth.Set(float64(len(s.actionQueue)))
		}
	}
}

// isPending reports whether the machine currently has an action
// enqueued or in flight. Used by applyReconciledMachine to skip
// machines whose local state is being driven by a worker — the
// provider's List view lags any in-flight RPC and would otherwise
// overwrite the worker's transitional state with a stale read.
// bigfleet-uber #23 root-caused 17–26% Provision failures to this
// race; pendingActions was already maintained for action dedup at
// enqueue time, this is just a second reader.
func (s *Shard) isPending(id machine.ID) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	_, ok := s.pendingActions[id]
	return ok
}

// actionStillApplicable re-checks the live inventory at emit time
// to confirm the action's required starting state still holds. The
// cycle's snapshot can be milliseconds-to-cycles old; the worker
// pool runs concurrently and may have already done the work this
// action is about to dispatch (a competing Bootstrap completed,
// or a Reclaim flipped Configured back to Idle).
//
// Returns false when the action is moot — the machine has moved
// past the state the action would transition from. Returns true
// otherwise (including for inv.Get errors, since "fail open" matches
// the pre-Drop-K behaviour and the worker's own state check is the
// final authority).
//
// Drop K. The pre-existing pendingActions ledger handles "another
// worker is mid-flight on M" but not "another worker just finished
// on M". The race window is small but non-zero, and at 50K-Pod
// cloud scale produced 39/sec wasted dispatches.
func (s *Shard) actionStillApplicable(a decision.Action) bool {
	cur, err := s.inv.Get(a.MachineID)
	if err != nil {
		return true
	}
	switch a.Kind {
	case decision.ActionKindBootstrap:
		return cur.State == machine.StateIdle
	case decision.ActionKindProvision:
		return cur.State == machine.StateSpeculative
	case decision.ActionKindReclaim, decision.ActionKindPreempt:
		return cur.State == machine.StateConfigured
	}
	return true
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
	cycleNum := s.cycleCount.Add(1)
	recordMetrics := int(cycleNum) > s.cfg.MetricsWarmupCycles
	// ADR-0046: surface the kill switch every cycle so a pause nobody
	// remembers stays visible on dashboards for as long as it's on.
	if s.cfg.ActuationPaused {
		metrics.ShardActuationPaused.Set(1)
	} else {
		metrics.ShardActuationPaused.Set(0)
	}
	defer func() {
		if recordMetrics {
			metrics.ShardCycleDuration.Observe(time.Since(cycleStart).Seconds())
		}
	}()
	cycleCtx, cancel := context.WithTimeout(ctx, s.cfg.CycleInterval)
	defer cancel()

	// Reconcile inventory against the provider before deciding so we're
	// not making decisions against a stale view.
	reconcileStart := time.Now()
	if err := s.reconcile(cycleCtx); err != nil {
		s.log.Warn("reconcile failed", "err", err)
	}
	if recordMetrics {
		metrics.ShardCyclePhaseDuration.WithLabelValues("reconcile").Observe(time.Since(reconcileStart).Seconds())
	}

	// Snapshot() builds a fresh consistent view under RLock. The
	// background fold goroutine and CycleSnapshot() were removed at
	// M44.4 Drop A: stale snapshots caused ~50 % wasted Bootstraps
	// for already-Configured machines at real write rates.
	snapStart := time.Now()
	snap := s.inv.Snapshot()
	demand := s.needs.Snapshot()
	// ADR-0041: fold sub-machine Same-Needs into atomic plain
	// aggregates against this cycle's snapshot, once, so Phase 1,
	// Phase 2 (via Phase 1's residuals), Phase 3 and the attribution
	// probe all reason over the same normalized demand.
	demand = decision.NormalizeDemand(snap, demand)
	s.stampParkedNeeds(demand)
	if recordMetrics {
		metrics.ShardCyclePhaseDuration.WithLabelValues("snapread").Observe(time.Since(snapStart).Seconds())
	}

	p1Start := time.Now()
	p1 := decision.Phase1(snap, demand)
	s.recordUnsatSameAges(p1)
	if recordMetrics {
		metrics.ShardCyclePhaseDuration.WithLabelValues("phase1").Observe(time.Since(p1Start).Seconds())
	}

	p2Start := time.Now()
	p2 := decision.Phase2(snap, p1.Unsatisfied, s.cfg.Phase2Options)
	if recordMetrics {
		metrics.ShardCyclePhaseDuration.WithLabelValues("phase2").Observe(time.Since(p2Start).Seconds())
	}

	p3Start := time.Now()
	p3 := decision.Phase3(snap, demand, s.FirstRollupReceived)
	if recordMetrics {
		metrics.ShardCyclePhaseDuration.WithLabelValues("phase3").Observe(time.Since(p3Start).Seconds())
	}

	// ADR-0040 §4: flag-gated, read-only phase-attribution probe — the
	// instrument that found the Same-domain crediting defect. The guard
	// runs before any collection so the default-off path costs nothing.
	if s.cfg.PhaseAttributionLog && cycleNum%20 == 0 {
		pa := collectPhaseAttribution(snap, demand, p1, p3)
		s.log.Info("phase attribution",
			"needs_total", pa.needsTotal,
			"needs_same", pa.needsSame,
			"p1_unsatisfied", pa.p1Unsatisfied,
			"p1_unsatisfied_same", pa.p1UnsatisfiedSame,
			"p3_reclaim", pa.p3Reclaim,
			"p3_reclaim_matches_unsatisfied", pa.p3ReclaimMatchesUnsatisfied,
			"p3_reclaim_matches_and_fits", pa.p3ReclaimMatchesAndFits,
		)
		// ADR-0042 per-gang probe: one line per sampled unsatisfied
		// Same-Need. chosen_domain flipping across cycles with
		// acquired > 0 is the #56 churn loop; pinned domains with
		// acquired → 0 is parking working.
		for _, r := range pa.reclaimProbe {
			s.log.Info("reclaim attribution",
				"machine", r.machineID,
				"cluster", r.cluster,
				"instance_type", r.instanceType,
				"matches_unsatisfied", r.matches,
				"fits", r.fits,
			)
		}
		for _, g := range pa.gangProbe {
			s.log.Info("gang attribution",
				"group", g.group,
				"cluster", g.cluster,
				"chosen_domain", g.domain,
				"acquired", g.acquired,
				"residual", g.deficit,
				"parked", g.parked,
			)
		}
	}

	// Emit AvailableCapacity hints (paper §6.2). Eventually-consistent;
	// runs after the decision phases so the snapshot view reflects any
	// inventory mutations Phase 1/2/3 just made via execute (not yet
	// run, but the snapshot is already cached and we read counts from
	// the pre-built buckets — no per-cycle walk).
	emitStart := time.Now()
	s.emitAvailableCapacity(snap, demand)
	if recordMetrics {
		metrics.ShardCyclePhaseDuration.WithLabelValues("emit").Observe(time.Since(emitStart).Seconds())
	}

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

	// ADR-0046 rail 1: per-cluster reclaim blast-radius cap, applied
	// to the engine's decision before anything executes. The deferred
	// surplus re-derives next cycle (Phase 3 is idempotent against an
	// unchanged snapshot) — and unlike MaxActionsPerCycle's deferral
	// below, it does NOT trigger an immediate follow-up cycle: the
	// cap's purpose is spreading risk over wall-clock time, so the
	// surplus waits for the natural cadence. Skipped while paused
	// (rail 3 suppresses everything anyway).
	if !s.cfg.ActuationPaused && s.cfg.ReclaimCapFraction > 0 {
		var capped int
		all, capped = capReclaims(snap, all, s.cfg.ReclaimCapFraction)
		if capped > 0 {
			metrics.ShardReclaimsCapped.Add(float64(capped))
			s.log.Warn("reclaim blast-radius cap engaged (ADR-0046)",
				"deferred_reclaims", capped,
				"cap_fraction", s.cfg.ReclaimCapFraction)
		}
	}

	limit := s.cfg.MaxActionsPerCycle
	deferred := 0
	if !s.cfg.ActuationPaused && limit > 0 && len(all) > limit {
		deferred = len(all) - limit
		all = all[:limit]
	}

	// ADR-0021: enqueue actions to the persistent execute pool and
	// return immediately. Workers drain at their own pace with their
	// own per-action context — the cycle no longer waits on the
	// slowest of the batch, and a transient operator-side latency
	// spike doesn't gate cycle progress. The queue is fall-through:
	// if it's full (workers can't keep up), drop the new emission
	// and re-derive next cycle. Phase emissions are idempotent
	// against an unchanged snapshot, so re-emission is safe.
	executeStart := time.Now()
	dropped := 0
	deduped := 0
	switch {
	case s.cfg.ActuationPaused:
		// ADR-0046 rail 3: the kill switch. Everything up to here ran
		// — reconcile, phases, AvailableCapacity, the probes — so the
		// engine's view and intentions stay fully observable; only
		// execution is withheld. Suppressed actions are counted per
		// kind and kept out of ShardActionsTotal (which means
		// "emitted for execution").
		for _, a := range all {
			metrics.ShardActionsSuppressed.WithLabelValues(a.Kind.String()).Inc()
		}
		if len(all) > 0 {
			s.log.Warn("actuation paused: decided actions suppressed (ADR-0046)",
				"suppressed", len(all))
		}
	case s.actionQueue != nil:
		for _, a := range all {
			// Drop K: live-state guard at emit time. The cycle's
			// snapshot is read once at the start; phases compute on
			// it; emit runs at the end. Workers run concurrently,
			// transitioning machines as they finish actions. The
			// pendingActions ledger correctly dedups while a worker
			// is mid-flight, but a worker that *finished* between
			// the snapshot read and this emit will have cleared its
			// pendingActions entry — leaving us about to enqueue an
			// action whose target machine has already moved past
			// the action's required start state. The worker would
			// pick it up and error "expected Idle" / similar.
			//
			// Re-check live inventory here. If the machine isn't in
			// the state this action requires, the action's intent
			// either already holds (we'd be a duplicate) or has been
			// invalidated by a competing transition (Reclaim etc.);
			// either way, skip the enqueue. The next cycle's Phase
			// 1/2/3 will re-derive the correct action against fresh
			// state.
			//
			// Drop I diagnostics found 39/sec "expected Idle" at 50K-
			// Pod cloud scale (29 % of all Bootstrap dispatches).
			// Drop J added an idempotent fallback inside execute as
			// a safety net; this guard prevents the wasted worker
			// dispatch in the first place.
			if !s.actionStillApplicable(a) {
				deduped++
				continue
			}
			// Per-machine in-flight dedup: skip emit if the machine
			// already has an action queued or being processed. Without
			// this, a deep queue + slow workers means several cycles
			// re-emit the same Bootstrap before the first one
			// transitions Idle → Configuring; the duplicates fail with
			// "expected Idle" and burn worker capacity. Phase 1 will
			// re-derive next cycle if the worker fails.
			s.pendingMu.Lock()
			if _, exists := s.pendingActions[a.MachineID]; exists {
				s.pendingMu.Unlock()
				deduped++
				continue
			}
			s.pendingActions[a.MachineID] = struct{}{}
			s.pendingMu.Unlock()
			select {
			case <-ctx.Done():
				s.pendingMu.Lock()
				delete(s.pendingActions, a.MachineID)
				s.pendingMu.Unlock()
				break
			case s.actionQueue <- a:
			default:
				s.pendingMu.Lock()
				delete(s.pendingActions, a.MachineID)
				s.pendingMu.Unlock()
				dropped++
			}
		}
		if dropped > 0 {
			metrics.ShardActionsDropped.Add(float64(dropped))
		}
		if deduped > 0 {
			metrics.ShardActionsDeduped.Add(float64(deduped))
		}
		metrics.ShardActionQueueDepth.Set(float64(len(s.actionQueue)))
	default:
		// Step / runCycleCapturing called outside Run() — fall back
		// to inline serial execute so simulator and tests still work
		// without spawning a worker pool. Production never hits this.
		for _, a := range all {
			if err := s.execute(ctx, a); err != nil {
				s.log.Warn("action failed", "kind", a.Kind, "machine", a.MachineID, "cluster", a.Cluster, "err", err)
			}
		}
	}
	if recordMetrics {
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
		seeds = append(seeds, shortfallSeed{Profile: u.Need.Profile, Deficit: u.Deficit})
	}
	s.recordShortfalls(seeds)

	// `all` was already populated above when we built the executor's
	// queue; reuse it for metrics. (We also need to count any deferred
	// actions not in `all` — they show up via ShardActionsDeferred.)
	// Suppressed actions (ADR-0046 kill switch) are counted only in
	// ShardActionsSuppressed so this counter keeps meaning "emitted
	// for execution".
	if !s.cfg.ActuationPaused {
		for _, a := range all {
			metrics.ShardActionsTotal.WithLabelValues(a.Kind.String()).Inc()
		}
	}
	// Inventory state gauge with M25 FinOps labels (capacity_type,
	// interruption_penalty_bucket). Walks the snapshot once per
	// cycle to bucket. At 500K this is roughly the cost of the old
	// snap.All() walk we removed in M11 (~150 ms p99 in the cloud
	// runs); we accept it as a one-walk-per-cycle metric overhead.
	// Future work: maintain the per-(state, cap_type, bucket) counts
	// incrementally during fold/Apply if the cost shows up in
	// production telemetry.
	emitInventoryByCapacityAndPenalty(snap)
	emitDemandByPenaltyBucket(demand)
	if n := s.ShortfallCount(); n > 0 {
		// Operational signal for on-call runbooks: a non-zero count
		// every cycle should leave a "shortfall" string in the shard
		// log so `kubectl logs ... | grep -i shortfall` returns the
		// recent unresolved-demand history. Top-3 oldest shortfall
		// fingerprints are the most actionable for triage. M14.
		top := s.Shortfalls()
		if len(top) > 3 {
			top = top[:3]
		}
		fps := make([]string, 0, len(top))
		for _, sf := range top {
			fps = append(fps, sf.Profile.Fingerprint())
		}
		s.log.Warn("shortfalls detected", "count", n, "top_fingerprints", fps)
		metrics.ShardShortfalls.Set(float64(n))
		emitAgedShortfallBuckets(s.Shortfalls())
	} else {
		metrics.ShardShortfalls.Set(0)
		emitAgedShortfallBuckets(nil)
	}
	if s.cfg.OnActions != nil {
		s.cfg.OnActions(all)
	}
	return all
}

// phaseAttribution is the ADR-0040 §4 probe payload: per-cycle Need /
// unsatisfied counts split by co-location, plus the discriminator that
// found the Same-domain crediting defect — Phase 3 reclaiming a
// machine that MatchProfiles (and fits) a Need Phase 1 declared
// unsatisfied in the same cycle. Under correct attribution the
// matches/fits counters read zero in steady state.
type phaseAttribution struct {
	needsTotal                  int
	needsSame                   int
	p1Unsatisfied               int
	p1UnsatisfiedSame           int
	p3Reclaim                   int
	p3ReclaimMatchesUnsatisfied int
	p3ReclaimMatchesAndFits     int
	// gangProbe carries up to gangProbeN unsatisfied Same-Needs'
	// per-cycle attribution (ADR-0042): the joint choice's domain, the
	// machines acquired before exhausting, and the residual. A
	// chosen_domain that flips across consecutive probe lines while
	// acquired stays non-zero is the #56 assemble↔reclaim churn
	// signature; post-ADR-0042 it should pin.
	gangProbe []gangProbeEntry
	// reclaimProbe samples Phase 3's reclaim set (probe v3, the #58
	// follow-up): which machines Phase 3 sheds and whether any
	// unsatisfied Need matches/fits them. Sustained reclaims with
	// matches=false alongside equal-rate bootstraps is the
	// excess-inventory oscillation signature #58 surfaced — churn that
	// the unsatisfied-only gang probe is blind to by construction.
	reclaimProbe []reclaimProbeEntry
}

// gangProbeN bounds the per-cycle gang probe so the log line stays
// readable at uber-5k's ~190 unsatisfied gangs. Deterministic
// selection (the first N in Phase 1's result order) keeps the same
// gangs comparable across cycles.
const gangProbeN = 5

type reclaimProbeEntry struct {
	machineID    string
	cluster      string
	instanceType string
	matches      bool // some unsatisfied Need matches the machine's profile
	fits         bool // ...and the machine covers that Need's MinUnit
}

type gangProbeEntry struct {
	group    string
	cluster  string
	domain   string
	acquired int
	deficit  string
	parked   bool
}

// Acquisition parking (ADR-0042 Addendum). parkAfterCycles is the
// debounce: a Same-Need class must be Phase 1-unsatisfiable this many
// consecutive cycles before its members park — long enough that claim
// races and transient supply dips don't park live demand, short enough
// that a structurally-unsatisfiable gang stops churning within
// seconds at the shard's cycle rate. reprobeEveryCycles periodically
// un-parks for a single cycle so parked demand re-attempts acquisition
// and naturally un-parks (the age resets) when supply has appeared —
// without it, a parked Need's creditable-only choice could never see
// new supply and would park forever.
const (
	parkAfterCycles    = 8
	reprobeEveryCycles = 32
)

// unsatSameKey identifies a Same-Need class across cycles: group is
// the wire-level co-location identity (ADR-0042 Addendum); the
// fingerprint disambiguates groups that collide across workloads; the
// cluster scopes it.
func unsatSameKey(n *needs.Need) string {
	return string(n.ClusterID) + "\x00" + n.Group + "\x00" + n.Profile.Fingerprint()
}

// recordUnsatSameAges updates the per-class stalled-cycle counter from
// this cycle's Phase 1 result. Faithful to concentrate-THEN-park: a
// cycle only ages a class when it was unsatisfied AND acquired nothing
// — an unsatisfied gang that is still successfully concentrating
// (Acquired > 0) is making progress and must keep going at full speed,
// so any progress resets its age. Classes that resolved (or vanished)
// are forgotten.
func (s *Shard) recordUnsatSameAges(p1 decision.Phase1Result) {
	seen := make(map[string]struct{}, len(p1.Unsatisfied))
	for i := range p1.Unsatisfied {
		u := &p1.Unsatisfied[i]
		if _, ok := occ.SameRequirementKey(u.Need.Profile); !ok {
			continue
		}
		k := unsatSameKey(&u.Need)
		if u.Acquired > 0 || u.SameSatisfiable {
			// Progress, or a satisfiable bucket exists (the stall is a
			// lost claim race, not structural): reset.
			delete(s.unsatSameAge, k)
			continue
		}
		seen[k] = struct{}{}
		s.unsatSameAge[k]++
	}
	for k := range s.unsatSameAge {
		if _, ok := seen[k]; !ok {
			delete(s.unsatSameAge, k)
		}
	}
}

// stampParkedNeeds marks this cycle's Same-Needs whose class has aged
// past the parking threshold, skipping the periodic re-probe cycle.
// Both phases honour the stamp identically (creditable-only domain
// choice, no acquisition), so a parked gang keeps its concentrated
// partial assembly and stops driving Bootstrap/Reclaim.
func (s *Shard) stampParkedNeeds(demand []needs.Need) {
	if len(s.unsatSameAge) == 0 {
		return
	}
	for i := range demand {
		if _, ok := occ.SameRequirementKey(demand[i].Profile); !ok {
			continue
		}
		age := s.unsatSameAge[unsatSameKey(&demand[i])]
		if age >= parkAfterCycles && age%reprobeEveryCycles != 0 {
			demand[i].AcquisitionParked = true
		}
	}
}

// formatQtys renders a residual vector compactly for the gang probe
// line ("cpu=12,nvidia.com/gpu=96").
func formatQtys(qs []needs.ResourceQty) string {
	var b strings.Builder
	for i, q := range qs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(q.Name)
		b.WriteByte('=')
		b.WriteString(q.Quantity)
	}
	return b.String()
}

// collectPhaseAttribution computes the probe over the cycle's own
// snapshot and phase results. Read-only; only runs on flag-gated
// cycles, so the O(reclaims × unsatisfied) match walk is acceptable.
func collectPhaseAttribution(snap *inventory.Snapshot, demand []needs.Need, p1 decision.Phase1Result, p3 decision.Phase3Result) phaseAttribution {
	pa := phaseAttribution{
		needsTotal: len(demand),
		// Phase 3 emits Reclaim actions only.
		p3Reclaim:     len(p3.Actions),
		p1Unsatisfied: len(p1.Unsatisfied),
	}
	for i := range demand {
		if _, ok := occ.SameRequirementKey(demand[i].Profile); ok {
			pa.needsSame++
		}
	}
	for i := range p1.Unsatisfied {
		u := &p1.Unsatisfied[i]
		if _, ok := occ.SameRequirementKey(u.Need.Profile); !ok {
			continue
		}
		pa.p1UnsatisfiedSame++
		if len(pa.gangProbe) < gangProbeN {
			group := u.Need.Group
			if group == "" {
				group = u.Need.Profile.Fingerprint()[:12]
			}
			pa.gangProbe = append(pa.gangProbe, gangProbeEntry{
				group:    group,
				cluster:  string(u.Need.ClusterID),
				domain:   u.SameDomain,
				acquired: u.Acquired,
				deficit:  formatQtys(u.Deficit),
				parked:   u.Need.AcquisitionParked,
			})
		}
	}
	for _, a := range p3.Actions {
		m, ok := snap.Get(a.MachineID)
		if !ok {
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		matched, fits := false, false
		for i := range p1.Unsatisfied {
			u := &p1.Unsatisfied[i]
			if !decision.MatchProfile(u.Need.Profile, m) {
				continue
			}
			matched = true
			if needs.Covers(alloc, u.Need.MinUnit) {
				fits = true
				break
			}
		}
		if matched {
			pa.p3ReclaimMatchesUnsatisfied++
		}
		if fits {
			pa.p3ReclaimMatchesAndFits++
		}
		if len(pa.reclaimProbe) < gangProbeN {
			pa.reclaimProbe = append(pa.reclaimProbe, reclaimProbeEntry{
				machineID:    string(a.MachineID),
				cluster:      string(a.Cluster),
				instanceType: m.Profile.InstanceType,
				matches:      matched,
				fits:         fits,
			})
		}
	}
	return pa
}

// emitInventoryByCapacityAndPenalty walks the snapshot once and
// publishes per-(state, capacity_type, interruption_penalty_bucket)
// gauge values. M25 FinOps surface — supersedes the old per-state-only
// emit. Aggregation queries that don't care about the new labels still
// work via `sum by (state) (bigfleet_shard_inventory_machines)`.
//
// Stale label combinations are not actively pruned: if a shard has had
// machines in (Idle, OnDemand, $1024) historically but currently has
// none, the gauge series persists at 0 because Set(0) was called
// during the cycle that drained the bucket. The "set zeros first"
// pattern below ensures a bucket that drains to zero reports 0, not
// a stale non-zero from the previous cycle.
func emitInventoryByCapacityAndPenalty(snap *inventory.Snapshot) {
	if snap == nil {
		return
	}
	type key struct {
		state   machine.State
		capType machine.CapacityType
		bucket  needs.PenaltyBucket
	}
	counts := make(map[key]int)
	for _, m := range snap.All() {
		k := key{
			state:   m.State,
			capType: m.Profile.CapacityType,
			bucket:  needs.BucketForDollars(m.AssignedInterruptionPenaltyDollars),
		}
		counts[k]++
	}
	// Set zeros for every (state, capType, bucket) combination that
	// has a label child anywhere on this shard's metric tree, so a
	// drained bucket falls to 0 instead of carrying stale state.
	// Iterating counts also publishes the non-zero values.
	for k, n := range counts {
		metrics.ShardInventoryMachines.WithLabelValues(
			k.state.String(),
			k.capType.String(),
			k.bucket.Label(),
		).Set(float64(n))
	}
}

// emitDemandByPenaltyBucket walks the NeedsTable snapshot and publishes
// the per-bucket count of demand rows. Bounded cardinality (28 buckets
// max). M25; ADR-0027: demand is a resource vector, so this reports the
// number of Need rows in each penalty bucket, not a Pod count.
func emitDemandByPenaltyBucket(rows []needs.Need) {
	counts := make(map[needs.PenaltyBucket]int, 8)
	for _, r := range rows {
		counts[r.Profile.InterruptionPenaltyBucket()]++
	}
	// Reset every bucket label to 0 first so empty NeedsTable rows
	// don't keep stale values around.
	for b := needs.PenaltyBucketUnspecified; b <= needs.PenaltyBucketPinned; b++ {
		metrics.ShardDemandMachines.WithLabelValues(b.Label()).Set(float64(counts[b]))
	}
}

// emitAgedShortfallBuckets resets the per-bucket gauge and re-publishes
// the current age distribution. Called every cycle so absent buckets
// fall back to 0 — alerts that fire on `> 0` clear cleanly when the
// shortfall resolves.
func emitAgedShortfallBuckets(sfs []Shortfall) {
	buckets := map[string]float64{
		"1-9": 0, "10-59": 0, "60-299": 0, "300+": 0,
	}
	for _, sf := range sfs {
		buckets[ageBucket(sf.AgeCycles)]++
	}
	for k, v := range buckets {
		metrics.ShardShortfallsAged.WithLabelValues(k).Set(v)
	}
}

func ageBucket(age int) string {
	switch {
	case age >= 300:
		return "300+"
	case age >= 60:
		return "60-299"
	case age >= 10:
		return "10-59"
	default:
		return "1-9"
	}
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
	// ADR-0016: NodeStateUpdate carries node identity so the operator
	// can populate UpcomingNode.Spec with the labels / resources /
	// taints the kubelet will register with. Empty values are valid
	// for transitional states where no host has been bound yet (e.g.
	// SPECULATIVE) — the operator writes them through as-is.
	if m.Profile.InstanceType != "" || m.Profile.Zone != "" || len(m.Profile.Labels) > 0 {
		labels := make(map[string]string, len(m.Profile.Labels)+2)
		for k, v := range m.Profile.Labels {
			labels[k] = v
		}
		if m.Profile.InstanceType != "" {
			labels["node.kubernetes.io/instance-type"] = m.Profile.InstanceType
		}
		if m.Profile.Zone != "" {
			labels["topology.kubernetes.io/zone"] = m.Profile.Zone
		}
		upd.Labels = labels
	}
	// ADR-0022 / M45.5: send the machine's *Allocatable* (per-machine
	// capacity), not Profile.Resources (per-replica request shape).
	// EffectiveAllocatable falls back to Profile.Resources when
	// Allocatable is unset, so pre-M45.0 inventory keeps the historical
	// 1 Pod = 1 machine math. With density>1 the operator now
	// publishes UpcomingNode.Spec.Resources at the actual fake-Node
	// Allocatable (e.g. 100 × {cpu:4, memory:8Gi} = {cpu:400, memory:800Gi})
	// so pod-shim's nodeFits bin-packing fills each fake-Node up to
	// its real capacity instead of capping at one Pod per Node.
	alloc := m.EffectiveAllocatable()
	if len(alloc) > 0 {
		upd.Resources = &pb.Resources{Resources: alloc}
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
