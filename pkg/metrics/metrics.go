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
		Name:    "bigfleet_operator_rollup_duration_seconds",
		Help:    "Wall-clock duration of one operator rollup: list CRs, aggregate by Profile, enqueue the stream message. Excludes the post-rollup status-write batch.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms → 4s
	})

	// OperatorAcknowledgeDuration is the time spent transitioning a
	// rollup's batch of Pending CRs to Acknowledged. Bounded by
	// AcknowledgeConcurrency × per-status-write latency, so this can
	// run for several seconds on the first rollup after a large CR
	// burst. Operationally interesting; not on the rollup hot path.
	OperatorAcknowledgeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bigfleet_operator_acknowledge_duration_seconds",
		Help:    "Wall-clock duration of one acknowledgement batch (Pending → Acknowledged status writes for the rollup's newly-included CRs).",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms → 40s
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
