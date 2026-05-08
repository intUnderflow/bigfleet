// Package metrics is the BigFleet Prometheus instrumentation. The
// counters / histograms / gauges defined here are owned by their
// respective subsystems (shard, coordinator, operator) and registered
// once on a shared default registry. Each binary exposes /metrics on
// a configurable address.
//
// Naming follows the Prometheus best-practice "<subsystem>_<unit>" with
// the bigfleet_ prefix:
//
//	bigfleet_shard_cycle_duration_seconds          histogram
//	bigfleet_shard_actions_total{kind}             counter
//	bigfleet_shard_inventory_machines{state}       gauge
//	bigfleet_shard_shortfalls                      gauge
//	bigfleet_coordinator_raft_term                 gauge
//	bigfleet_coordinator_apply_total{outcome}      counter
//	bigfleet_coordinator_pending_instructions{shard} gauge
//	bigfleet_operator_rollup_duration_seconds      histogram
//	bigfleet_operator_acknowledged_total           counter
//	bigfleet_operator_session_reconnects_total     counter
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Shard metrics. Set on every cycle.
var (
	ShardCycleDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bigfleet_shard_cycle_duration_seconds",
		Help:    "Wall-clock duration of one shard.runCycle (decision + execute + reconcile).",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms → 16s
	})

	// ShardProvisioningLatency is the wall-clock from the first
	// rollup observing demand of a given (cluster, profile fingerprint)
	// to the shard transitioning a matching machine to Configured.
	// Plan §10.7: end-to-end provisioning latency was previously
	// uninstrumented. Buckets bias toward sub-second since most
	// transitions complete inside a single cycle in the harness.
	ShardProvisioningLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bigfleet_shard_provisioning_latency_seconds",
		Help:    "Wall-clock from first rollup observing a (cluster, profile fingerprint) to a matching machine reaching Configured. Per-CR granularity is not preserved; this measures fingerprint-level fan-out latency.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})

	// ShardCyclePhaseDuration decomposes the cycle into its constituent
	// phases so we can identify which one dominates p99 without
	// re-running. Labelled phase ∈ {reconcile, phase1, phase2, phase3,
	// execute}. Sum of per-cycle phase samples ≈ ShardCycleDuration; the
	// small uncovered overhead is the snapshot/needs reads, action
	// collation, and the deferred-actions follow-up trigger.
	ShardCyclePhaseDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bigfleet_shard_cycle_phase_duration_seconds",
		Help:    "Wall-clock duration of each phase within shard.runCycle.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms → 16s
	}, []string{"phase"})

	ShardActionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_shard_actions_total",
		Help: "Count of decision-engine actions emitted, by kind (Bootstrap / Provision / Reclaim / Preempt).",
	}, []string{"kind"})

	// ShardInventoryMachines now carries capacity_type and
	// interruption_penalty_bucket labels alongside state (M25). Lets
	// FinOps roll up:
	//   - spot/on-demand ratio per shard
	//   - penalty-bucket distribution of held capacity
	//   - which penalty buckets are over- or under-supplied
	// Cardinality bound: 9 states × 4 capacity types × 28 penalty
	// buckets = 1008 series per shard. Acceptable for prom; alerts
	// that don't care about the new labels keep working via
	// `sum by (state) (bigfleet_shard_inventory_machines)`.
	ShardInventoryMachines = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bigfleet_shard_inventory_machines",
		Help: "Machines currently in inventory, by state, capacity type, and interruption-penalty bucket.",
	}, []string{"state", "capacity_type", "interruption_penalty_bucket"})

	// ShardDemandMachines is the M25 NeedsTable-side counterpart to
	// ShardInventoryMachines. Per (interruption_penalty_bucket)
	// aggregate count of demanded machines — surfaces the
	// "penalty-bucket distribution of demand" view from the FinOps
	// runbook in user-stories.md.
	ShardDemandMachines = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bigfleet_shard_demand_machines",
		Help: "Machines currently demanded by the NeedsTable, bucketed by interruption-penalty bucket.",
	}, []string{"interruption_penalty_bucket"})

	ShardShortfalls = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bigfleet_shard_shortfalls",
		Help: "Number of unresolved shortfalls the shard is reporting up.",
	})

	// ShardShortfallsAged is the per-age-bucket count of currently
	// unresolved shortfalls. Buckets are decimal cycle-counts:
	//   "1-9"     fresh, normal-noise
	//   "10-59"   sustained, worth a runbook check
	//   "60-299"  long-lived, almost certainly a topology / quota problem
	//   "300+"    pages-louder territory
	// Alerts wired against `bigfleet_shard_shortfalls_aged{bucket="60-299"} > 0`
	// or higher buckets surface the user-stories "max age before it
	// pages louder" runbook entry without bolting an alerting policy
	// into the shard binary itself.
	ShardShortfallsAged = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bigfleet_shard_shortfalls_aged",
		Help: "Count of unresolved shortfalls bucketed by AgeCycles. Used by alert rules to escalate long-aged unresolved demand.",
	}, []string{"bucket"})

	ShardActionsDeferred = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_shard_actions_deferred_total",
		Help: "Count of decision actions deferred to a later cycle by MaxActionsPerCycle. Phase 1/2/3 are idempotent so deferred work re-derives next cycle.",
	})

	// M44.4 Drop A diagnostic: per-execute-outcome counters. The
	// scaleway-50k pipeline run found 71% of Bootstrap actions emitted
	// by Phase 1 don't translate to Configured machines — execute
	// returned errors silently. This counter localises which return
	// path each error takes; combined with ShardSessionLifecycle and
	// ShardActiveSessions below, it pinpoints whether nil-session
	// is the dominant failure mode.
	ShardActionExecuteOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_shard_action_execute_outcomes_total",
		Help: "Count of action-execute outcomes by kind + outcome. Sums to (or close to) ShardActionsTotal — gaps point at unaccounted return paths. Outcomes: success, no_session (operator stream gone), transition_error (state-machine refused), blob_error (operator/local bootstrap blob fetch failed), configure_error (provider Configure rejected), ctx_canceled (cycleCtx timeout).",
	}, []string{"kind", "outcome"})

	ShardSessionLifecycle = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_shard_session_lifecycle_total",
		Help: "Count of operator-session lifecycle events. installed = new operator dialed, removed = stream ended, replaced = same cluster's prior session was kicked. High replaced rate = grpc keepalive churn under load.",
	}, []string{"event"})

	ShardActiveSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bigfleet_shard_active_sessions",
		Help: "Number of currently-installed operator sessions on this shard. Should equal the count of clusters bound to this shard's domain assignment; lower means at least one cluster's operator hasn't dialed (or got disconnected).",
	})

	// M44.4 Drop B: gauge of currently-running execute() goroutines.
	// Compare against the configured executeConcurrency cap to see if
	// the shard is parallelism-bound during burst — a sustained gauge
	// at the cap with low per-execute latency means we're shipping
	// less than the cap allows; sustained at cap with high per-execute
	// latency means downstream (operator stream RTT, blob fetch) is
	// the gate.
	ShardExecuteInflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bigfleet_shard_execute_inflight",
		Help: "Currently-running execute() goroutines on this shard. Compare against the configured executeConcurrency to see whether burst processing is parallelism-bound.",
	})

	// ADR-0021 (persistent execute pool): action queue depth. Sustained
	// climb means the persistent pool can't keep up with cycle
	// emissions; drops will follow. Healthy steady-state stays well
	// below the queue cap (ExecuteConcurrency × 2).
	ShardActionQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bigfleet_shard_action_queue_depth",
		Help: "Current depth of the persistent execute pool's action queue (ADR-0021). Cap = ExecuteConcurrency × 2. Climbing toward cap = workers can't drain as fast as cycles emit; drops next.",
	})

	// ADR-0021: actions dropped because the action queue was full. Phase
	// emissions are idempotent so the next cycle re-derives — but a
	// non-zero rate here signals sustained worker-pool back-pressure
	// (operator handler latency × queue cap < cycle's emit rate).
	ShardActionsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_shard_actions_dropped_total",
		Help: "Cumulative actions dropped at cycle-emit time because the persistent execute pool's queue was full (ADR-0021). Distinct from ShardActionsDeferred (MaxActionsPerCycle truncation) — same intent, different mechanism.",
	})

	// ADR-0021: actions deduped at cycle-emit time because the same
	// machine already has an action queued or in flight from a prior
	// cycle. Without this counter we'd silently waste worker capacity
	// on "expected Idle" failures from duplicate Bootstrap actions
	// emitted across cycles before the first one completed its
	// transition.
	ShardActionsDeduped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_shard_actions_deduped_total",
		Help: "Cumulative actions skipped at enqueue time because the target machine already has an action queued or in flight (ADR-0021 in-flight set). High rates relative to ShardActionsTotal mean the cycle interval is firing faster than the worker pool drains — Phase keeps re-emitting for the same machines. Healthy steady state has near-zero dedup.",
	})

	// ADR-0019: per-sub-path Phase 1 instrumentation. The cloud-vs-
	// bench discrepancy in M44.4 required attribution of where Phase 1
	// wall-clock actually goes — pool build (merge+sort across multi-
	// type profiles) vs take (head-cursor walk + MatchProfile) vs
	// takeCoLocated (whole-pool bucket walk for sameRack profiles).
	// Buckets match the cycle/phase histograms so the views align.
	ShardPhase1PoolBuildDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bigfleet_shard_phase1_pool_build_duration_seconds",
		Help:    "Wall-clock duration of phase1Allocator.poolFor's buildPoolSource. One observation per unique (state, fingerprint) visited per cycle.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	})

	ShardPhase1TakeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bigfleet_shard_phase1_take_duration_seconds",
		Help:    "Wall-clock duration of phase1Allocator.take and its sub-paths. One observation per Need that called the allocator.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"path"}) // path ∈ {take, takeCoLocated, takeSpread}

	ShardPhase1Calls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_shard_phase1_calls_total",
		Help: "Counter of phase1Allocator sub-path invocations, so mean cost = sum(duration) / count emerges from the same data.",
	}, []string{"path"})
)

// Coordinator metrics.
var (
	CoordinatorRaftTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bigfleet_coordinator_raft_term",
		Help: "Current Raft term observed by this coordinator replica.",
	})

	CoordinatorApplyTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_coordinator_apply_total",
		Help: "Count of FSM Apply outcomes (success / error) on the leader.",
	}, []string{"outcome"})

	CoordinatorPendingInstructions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bigfleet_coordinator_pending_instructions",
		Help: "Pending coordinator-issued instructions per shard, awaiting ack.",
	}, []string{"shard"})
)

// Operator metrics.
var (
	// OperatorRollupDuration measures only the customer-facing path:
	// list CRs, aggregate by Profile, enqueue the stream message. It
	// deliberately excludes the per-CR Acknowledged status-write batch,
	// which scales with the number of newly-Pending CRs and would
	// otherwise dominate the histogram on the first rollup after a
	// large ramp. Status-write latency is exposed separately as
	// OperatorAcknowledgeDuration.
	OperatorRollupDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "bigfleet_operator_rollup_duration_seconds",
		Help: "Wall-clock duration of one operator rollup: list CRs, aggregate by Profile, enqueue the stream message. Excludes the post-rollup status-write batch.",
		// 1ms → ~32s. Top buckets give headroom to measure tail
		// latency at large CR counts before the histogram saturates.
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
	})

	// OperatorAcknowledgeDuration is the time spent transitioning a
	// rollup's batch of Pending CRs to Acknowledged. Bounded by
	// AcknowledgeConcurrency × per-status-write latency, so this can
	// run for several seconds on the first rollup after a large CR
	// burst. Operationally interesting; not on the rollup hot path.
	OperatorAcknowledgeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "bigfleet_operator_acknowledge_duration_seconds",
		Help: "Wall-clock duration of one acknowledgement batch (Pending → Acknowledged status writes for the rollup's newly-included CRs).",
		// 10ms → ~5min. Status writes against a slow apiserver
		// (kine + sqlite, throttled flow control) can take minutes
		// for thousand-CR batches; we want measurement, not a cap.
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})

	// OperatorOutboxDropped counts non-rollup messages dropped because
	// the bounded session outbox was full (paper §10.5). Drops are
	// recoverable: the shard re-issues on RPC timeout. A non-zero rate
	// signals the operator's send pipeline is sustainedly behind its
	// stream — investigate per-cluster apiserver throughput or stream
	// RTT before raising outboxCap.
	OperatorOutboxDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_operator_outbox_dropped_total",
		Help: "Operator session-outbox messages dropped (BootstrapBlobResponse / ReclaimAck) because the bounded queue was full.",
	})

	OperatorAcknowledgedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_operator_acknowledged_total",
		Help: "Count of CapacityRequests transitioned from Pending to Acknowledged.",
	})

	OperatorSessionReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_operator_session_reconnects_total",
		Help: "Count of Shard.Session reconnect attempts (transport closed and re-dialled).",
	})

	// M44.4 Drop B observability: localise where the shard → operator →
	// UpcomingNode → binder chain bleeds throughput when binding-latency
	// p99 exceeds expectations. Each NodeStateUpdate the operator
	// receives translates to 2-3 apiserver writes (Get cache; Create on
	// new; re-fetch; Status().Update); this histogram captures the full
	// end-to-end handler cost so we can attribute slow stages.
	OperatorNodeStateUpdateDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "bigfleet_operator_node_state_update_duration_seconds",
		Help: "Wall-clock duration of handleNodeStateUpdate per inbound NodeStateUpdate frame, by resulting UpcomingNode phase. p99 above ~100 ms means apiserver-write back-pressure is bleeding into chain throughput.",
		// 0.001s … 65.536s — buckets reach 65 s so p99 doesn't saturate
		// at the top under burst conditions where back-pressure stretches
		// individual handlers into the 10-30 s range.
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 17),
	}, []string{"phase"})

	// Per-op outcome counter for UpcomingNode CRD writes. Splits the
	// handler's 2-3 apiserver hops by op so we can see which one's
	// erroring or being retried.
	OperatorUpcomingNodeWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bigfleet_operator_upcoming_node_writes_total",
		Help: "UpcomingNode CRD apiserver write attempts, by op (create, spec_update, status_update) and outcome (success, conflict, error). Sum / NodeStateUpdate-rate ≈ apiserver round-trips per binding.",
	}, []string{"op", "outcome"})

	// recvLoop spawns one goroutine per inbound frame with no semaphore.
	// At high inbound rates this can balloon into thousands of in-flight
	// handlers all queuing on the apiserver-write rate-limiter. Gauge so
	// we can correlate inflight depth with handler-duration p99.
	OperatorDispatchInflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bigfleet_operator_dispatch_inflight",
		Help: "Currently-running stream-dispatch goroutines on this operator. Sustained high values point at apiserver-side back-pressure (per-cluster QPS limiter draining slower than the inbound stream).",
	})
)
