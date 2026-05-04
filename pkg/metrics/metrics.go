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

	ShardInventoryMachines = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bigfleet_shard_inventory_machines",
		Help: "Machines currently in inventory, by state.",
	}, []string{"state"})

	ShardShortfalls = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bigfleet_shard_shortfalls",
		Help: "Number of unresolved shortfalls the shard is reporting up.",
	})

	ShardActionsDeferred = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bigfleet_shard_actions_deferred_total",
		Help: "Count of decision actions deferred to a later cycle by MaxActionsPerCycle. Phase 1/2/3 are idempotent so deferred work re-derives next cycle.",
	})
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
)
