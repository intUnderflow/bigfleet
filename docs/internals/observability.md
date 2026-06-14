# Metrics and observability catalog

This is the exhaustive, code-grounded register of every Prometheus series BigFleet emits, where it is set, and what it means — the catalog the [operator guide](../operator-guide.md#day-2-observability) draws its curated "key metrics" subset from. The operator guide tells an on-call which four numbers to put on a dashboard and how to react; this doc is the index of *all* of them, with the emit site for each so you can read the surrounding code when a series misbehaves. The framing throughout is "what does a non-zero / climbing / saturating value mean", because most of these counters were born from a specific diagnostic drop and carry that provenance in their Help text. The SLO-bearing subset gets its own section at the end, because *which* latency histogram gates a release is itself a multi-ADR decision.

## How metrics are wired

Every production series is a package-level `promauto`-registered variable in `pkg/metrics/metrics.go`, owned by the subsystem that sets it but defined centrally so the help text stays in one place (`pkg/metrics/metrics.go:1`). `promauto` registers on the process-global default registry at package-init time, so importing `pkg/metrics` is enough to make a series visible. Each binary serves that registry over plain HTTP `promhttp.Handler()` on a configurable address, `"0"` to disable:

| Binary | Default `/metrics` | Flag |
|---|---|---|
| Shard | `:8780` | `--metrics-addr` (`cmd/bigfleet/shard.go:599`, handler `:805`) |
| Coordinator | `:8790` | `--metrics-addr` (`cmd/bigfleet/coordinator.go:34`, handler `:143`) |
| Operator | `:8770` | `--metrics-addr` (`cmd/operator/main.go:51`, handler `:110`) |
| `all-in-one` | `:8780` shard / `:8790` coord | `--shard-metrics-addr` / `--coordinator-metrics-addr` (`cmd/bigfleet/all_in_one.go:42`) |
| pod-controller | `:8080` | controller-runtime registry (`pkg/controller/cr/controller.go:56`) |

The one exception to the central-registry rule is `bigfleet_unschedulable_pod_controller_reconciles_total`, which lives in the optional CR controller and registers on `sigs.k8s.io/controller-runtime`'s `ctrlmetrics.Registry` (`pkg/controller/cr/controller.go:49`), because that binary is a controller-runtime manager and exposes its registry, not the prometheus default. Endpoints are plaintext HTTP even under mTLS; the deployment keeps them cluster-internal (operator guide, mTLS section).

The harness metrics under `test/scaletest/` (`scaletest_*`, `bigfleet_scaletest_*`) are a separate surface — they live in the load-driver, pod-shim, and node-creator binaries, never in BigFleet itself. They are cataloged in a dedicated section below because the binding-latency SLO gate reads one of them, not a BigFleet series.

---

## Shard series

The shard is the hot path; it carries the densest instrumentation. All shard series are owned by `pkg/shard` (and its `pkg/decision/occ` sub-broker), defined in `pkg/metrics/metrics.go:27-374`.

### Cycle timing

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_cycle_duration_seconds` | histogram | — | Wall-clock of one `runCycle` (decision + execute + reconcile), buckets 1 ms→16 s (`metrics.go:29`). Set once per cycle at `pkg/shard/shard.go:640`. The throughput-envelope metric of ADR-0014 — tracked and alerted, **not** a release gate. |
| `bigfleet_shard_cycle_phase_duration_seconds` | histogram | `phase` (see below) | Per-phase decomposition so you can see which phase dominates p99 without a re-run (`metrics.go:89`). Emitted across `pkg/shard/shard.go:655-768` and `:945`. |

The emitted `phase` label values are `{reconcile, snapread, phase1, phase2, phase3, emit, execute}` (`pkg/shard/shard.go:655`, `:673`, `:680`, `:686`, `:702`, `:768`, `:945`) — note the Help text at `metrics.go:90` lists only five; `snapread` (snapshot read) and `emit` (action collation) were added later and the Help string is stale relative to the emitters. Sum of phase samples ≈ cycle duration; the residue is the deferred-actions follow-up trigger.

The phase histogram is the first thing to read when `cycle_duration` p99 breaches: per ADR-0028 the cycle envelope scales linearly with NeedsTable size, so a cycle-p99 alert under the realistic catalog is often a workload-cardinality fact, not a regression — the per-phase split tells you whether it is Phase 1 cost (real) or reconcile/execute (suspicious).

### Per-machine transition timing

These four histograms split the host-binding gap into stream-RPC vs local-work, on both the acquire and release paths. They were added across diagnostic Drops R/W (`metrics.go:47-81`) precisely because the operator's `upcoming_to_node` tail was being blamed on pod-shim when the latency actually lived inside `executeBootstrap`.

| Series | Type | Meaning · emit site |
|---|---|---|
| `bigfleet_shard_provisioning_latency_seconds` | histogram | First rollup observing a `(cluster, fingerprint)` → a matching machine reaching Configured (`metrics.go:41`). **Fingerprint fan-out latency, not per-CR** (ADR-0017). Emitted at `pkg/shard/provisioning_latency.go:65` with observe-once-and-delete semantics — the tracking entry is dropped after each sample so a 30-min soak doesn't resample the soak duration and saturate +Inf (the bug that pinned the histogram at 327.68 s; see the function comment at `provisioning_latency.go:48`). First-seen times are recorded per rollup at `pkg/shard/session.go:281`. |
| `bigfleet_shard_configure_phase_seconds` | histogram | Per-machine wall-clock from after Idle→Configuring to after Configuring→Configured inside `executeBootstrap` (`metrics.go:59`). A high p99 here is what makes the downstream `UpcomingNode` observation old. Emitted at `pkg/shard/execute.go:389`. |
| `bigfleet_shard_request_bootstrap_seconds` | histogram | Per-machine `sess.requestBootstrap` — the synchronous BootstrapRequest→BootstrapBlobResponse round-trip over the operator stream (`metrics.go:64`). `configure_phase − request_bootstrap` = local work (`Provider.Configure` + transition). Emitted at `pkg/shard/execute.go:328`. |
| `bigfleet_shard_drain_phase_seconds` | histogram | Symmetric to configure_phase: per-machine Configured→Draining→Idle inside `executeDrain` (`metrics.go:77`). High p99 ⇒ Reclaim slow per action; low p99 with low Reclaim throughput ⇒ Phase 3 isn't emitting enough. Emitted at `pkg/shard/execute.go:461`. Drop W found Bootstrap outrunning Reclaim by ~7/s, which tracked the e2e bind p99 climbing 6 s→25 s. |

### Inventory and demand

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_inventory_machines` | gauge | `state`, `capacity_type`, `interruption_penalty_bucket` | Machines in inventory by state × capacity type × penalty bucket (`metrics.go:110`, M25). Cardinality bound: 9 states × 4 capacity types × 28 buckets = 1008 series/shard. Legacy alerts survive via `sum by (state) (...)`. Set at `pkg/shard/shard.go:1340`. The `state` label spans all 8 machine states (the 3 stable + 4 transitional + Failed) plus Unspecified. |
| `bigfleet_shard_demand_machines` | gauge | `interruption_penalty_bucket` | NeedsTable-side counterpart: demanded machines by penalty bucket (`metrics.go:120`). The FinOps "penalty-bucket distribution of demand" view. Set at `pkg/shard/shard.go:1360`. |

The penalty bucket here is `interruption_penalty` (the workload-interruption cost in `effective_cost`), bucketed powers-of-2 per the §0.1 decision — distinct from `reclamation_penalty`, which has no metric label because it is a machine-specific tiebreak input, not a fleet-aggregatable dimension.

### Shortfall

There is no shortfall *package*; the buffer and aging live in `pkg/shard` and the deficit is derived in `pkg/decision`. The two shortfall series reflect the shard-side buffer.

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_shortfalls` | gauge | — | Unresolved shortfalls the shard reports up (`metrics.go:125`). Set at `pkg/shard/shard.go:1000` and `:1003`. Persistent non-zero = under-provisioned slice *or* over-aggressive priorities (operator-guide runbook). Topology `Same` requests that can't be met within the shard become shortfalls here — they are never resolved cross-shard. |
| `bigfleet_shard_shortfalls_aged` | gauge | `bucket` ∈ {"1-9","10-59","60-299","300+"} (cycle-counts) | Unresolved shortfalls by `AgeCycles` (`metrics.go:140`). Alert on `{bucket="60-299"} > 0` for the "long-lived, almost certainly topology/quota" escalation without baking an alerting policy into the binary. Set at `pkg/shard/shard.go:1376`. |

### Action accounting

`bigfleet_shard_actions_total{kind}` is the spine. Its label values come directly from `ActionKind.String()` — `Bootstrap`, `Provision`, `Reclaim`, `Preempt`, `Delete`, `Unspecified` (`pkg/decision/action.go:42`). Everything else here is a deliberate *sibling* counter that is **not** folded into `actions_total`, so that counter keeps meaning "emitted for execution".

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_actions_total` | counter | `kind` | Decision actions emitted (`metrics.go:95`). Set at `pkg/shard/shard.go:972`. |
| `bigfleet_shard_action_execute_outcomes_total` | counter | `kind`, `outcome` | Per-execute-outcome: success / no_session / transition_error / blob_error / configure_error / ctx_canceled / **fenced** (`metrics.go:157`, Drop A). Sums ≈ actions_total; gaps point at unaccounted return paths. `fenced` is a zombie-shard incident (paper §11 fencing token rejected) — alert, never retry. Set at `pkg/shard/execute.go:55`. |
| `bigfleet_shard_actions_deferred_total` | counter | — | Actions deferred by `MaxActionsPerCycle` truncation; idempotent, re-derived next cycle (`metrics.go:145`). Set at `pkg/shard/shard.go:948`. |
| `bigfleet_shard_actions_dropped_total` | counter | — | Actions dropped at emit because the persistent execute pool's queue was full (ADR-0021, cap = `ExecuteConcurrency×2`) (`metrics.go:205`). Distinct mechanism from deferred. Set at `pkg/shard/shard.go:928`. |
| `bigfleet_shard_actions_deduped_total` | counter | — | Actions skipped at enqueue because the target machine already has an action queued/in-flight (ADR-0021 in-flight set) (`metrics.go:216`). High vs actions_total ⇒ cycle interval firing faster than the pool drains. Set at `pkg/shard/shard.go:931`. |
| `bigfleet_shard_action_queue_depth` | gauge | — | Persistent execute pool queue depth (`metrics.go:196`). Climbing toward cap ⇒ drops next. Set at `pkg/shard/shard.go:546` and `:933`. |
| `bigfleet_shard_execute_inflight` | gauge | — | Currently-running `execute()` goroutines (`metrics.go:187`, Drop B). Compare against `executeConcurrency`: at-cap + low per-execute latency ⇒ under-shipping; at-cap + high latency ⇒ downstream-bound. Set/decremented at `pkg/shard/execute.go:51-52`. |

### Actuation safety rails (ADR-0046)

One metric per rail so each engaging is independently alertable. The kill-switch and dry-run counters are kept out of `actions_total` so a paused shard's *intentions* are observable without polluting the executed-action count.

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_reclaims_capped_total` | counter | — | Reclaims deferred by the per-cluster blast-radius cap (rail 1, 5%/cycle default) (`metrics.go:293`). Roll-over, not drop. Sustained non-zero = a mass drain being rate-limited — investigate before it finishes. Set at `pkg/shard/shard.go:797`. |
| `bigfleet_shard_rollup_quarantined` | gauge | `cluster` | Consecutive roll-ups held per cluster by the empty-roll-up guard (rail 2, 0 = clear) (`metrics.go:316`). While non-zero the cluster's prior accepted demand stays active. Set at `pkg/shard/shard.go:450`. |
| `bigfleet_shard_actions_suppressed_total` | counter | `kind` | Actions dropped at execute by the kill switch (rail 3, `--actuation-paused`) (`metrics.go:325`). The engine's intentions while paused. Set at `pkg/shard/shard.go:837`. |
| `bigfleet_shard_actuation_paused` | gauge | — | 1 while `--actuation-paused` (`metrics.go:333`). A pause nobody remembers is its own incident — alert on it staying non-zero. Set at `pkg/shard/shard.go:634`/`:636`. |
| `bigfleet_shard_actions_dryrun_total` | counter | `kind` | Actions reported-not-executed under `--dry-run` shadow mode (ADR-0046 addendum) (`metrics.go:346`). Deliberately distinct from suppressed so dashboards tell "shadowing by design" from "paused in anger". Set at `pkg/shard/shard.go:858`. |
| `bigfleet_shard_idle_releases_total` | counter | — | Idle→Speculative releases via `provider.Delete` after the per-CapacityType idle hold expired (paper §8, M73 / ADR-0049) (`metrics.go:307`). `rate()` ≈ releases/cycle. The Create↔Delete money loop is impossible by construction; this climbing in lockstep with Provision rates would mean construction broke — alert on the **pair**. Set at `pkg/shard/execute.go:514`. |

### Ingest validation

Both are the "garbage at the boundary" mirrors — the inventory/cluster keeps its last-known-good record and these increment instead of silently aliasing.

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_machines_rejected_total` | counter | `reason` ∈ {price, interruption_probability, structural} | Provider machine records refused at ingest by `machine.Invariant` — negative/NaN price, `interruption_probability` outside [0,1], or a state violation (`metrics.go:359`, M70). Set at `pkg/shard/safety.go:209`. |
| `bigfleet_shard_rollups_rejected_total` | counter | `cluster` | Demand-side mirror: roll-ups refused for out-of-range penalty bucket or unparseable resource quantity (`metrics.go:370`, M68b). Set at `pkg/shard/session.go:268`. |

### Session lifecycle and identity

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_active_sessions` | gauge | — | Currently-installed operator sessions (`metrics.go:167`). Should equal clusters bound to this shard's domain assignment; lower = an operator hasn't dialed. Set at `pkg/shard/session.go:156`/`:168`. |
| `bigfleet_shard_session_lifecycle_total` | counter | `event` ∈ {installed, removed, replaced} | Operator-session lifecycle (`metrics.go:162`). High `replaced` = grpc keepalive churn under load. Set at `pkg/shard/session.go:151-166`. |
| `bigfleet_shard_session_identity_rejected_total` | counter | — | Sessions terminated because the mTLS client cert's `bigfleet://` URI SAN didn't match `Hello.cluster_id` (ADR-0048) (`metrics.go:175`). **Any non-zero rate is a security event** — misissued cert or impersonation. Set at `pkg/shard/session.go:56`. |

### Phase 1 / OCC broker (ADR-0019, ADR-0029)

The OCC counters live in the `pkg/decision/occ` broker and answer the M46.3 cutover's primary diagnostic axis: "is OCC over-conflicting or under-emitting?"

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_shard_phase1_occ_proposals_total` | counter | `outcome` ∈ {committed, conflict} | Every `broker.Propose` by outcome (`metrics.go:256`). `conflict/committed` is the cycle's effective conflict rate; ADR-0029 targets ≤ 0.15 steady, ≤ 0.3 cold-start. Emitted at `pkg/decision/occ/broker.go:60`, `:99`, `:104`, `:158`. |
| `bigfleet_shard_phase1_occ_displacements_total` | counter | — | Incumbent Needs evicted by higher-precedence proposals, one per evicted Need (machine-level dedupes per Need) (`metrics.go:268`). Vs committed-proposals = "fraction of commits requiring displacement" → genuine priority asymmetry vs unclaimed-pool work. Emitted at `pkg/decision/occ/broker.go:160`. |
| `bigfleet_shard_phase1_occ_retries_exhausted_total` | counter | — | Needs that hit their retry budget without committing (`metrics.go:279`). Differentiates contention-bound Unsatisfied from catalog-bound Unsatisfied. Emitted at `pkg/decision/occ/cycle.go:211`. |

**Pre-OCC, now dead:** `bigfleet_shard_phase1_pool_build_duration_seconds` (`metrics.go:227`), `bigfleet_shard_phase1_take_duration_seconds{path}` (`metrics.go:233`), and `bigfleet_shard_phase1_calls_total{path}` (`metrics.go:239`) are still *defined* (so they appear on `/metrics` with zero samples) but have **no production emitter** since the OCC cutover replaced the linear `phase1Allocator.take`/`poolFor` path the ADR-0019 instrumentation measured. They predate the broker; treat them as deprecated until removed. Don't build alerts on them.

---

## Coordinator series

Owned by `pkg/coordinator`, defined at `pkg/metrics/metrics.go:377-392`. The coordinator is off the hot path; the shard plane runs autonomously through coordinator failover (static stability), so these are health/control-plane signals, never something a binding waits on.

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_coordinator_raft_term` | gauge | — | Current Raft term this replica observes (`metrics.go:378`). Rapidly increasing = partition or stepdown loop. Set at `pkg/coordinator/grpc_server.go:198`. |
| `bigfleet_coordinator_apply_total` | counter | `outcome` ∈ {success, error, fsm_error} | FSM `Apply` outcomes on the leader (`metrics.go:383`). `error` = apply pipeline error (`coordinator.go:295`); `fsm_error` = the FSM returned an error result (`:300`); `success` (`:304`). |
| `bigfleet_coordinator_pending_instructions` | gauge | `shard` | Coordinator-issued instructions per shard awaiting ack (`metrics.go:388`). Should drain to zero between rebalance cycles — rebalance instructions ride on the shard-pulled `ReportShard`, not pushed. Set at `pkg/coordinator/grpc_server.go:197`. |

---

## Operator series

Owned by `pkg/operator`, defined at `pkg/metrics/metrics.go:395-499`. The operator is per-cluster, dials out, holds one bidi `Shard.Session`, and is outbound-only; everything here measures its two jobs — rolling demand up, and applying the shard's node-state updates down to CRDs.

### Roll-up path

The roll-up histogram deliberately excludes the per-CR acknowledge batch, which scales with newly-Pending CR count and would otherwise dominate the first post-ramp rollup; ack latency is its own series.

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_operator_rollup_duration_seconds` | histogram | — | One rollup: list CRs, aggregate by Profile, enqueue the stream message (`metrics.go:403`). Excludes the status-write batch. Set at `pkg/operator/rollup.go:57`/`:75`. |
| `bigfleet_operator_rollup_phase_duration_seconds` | histogram | `phase` ∈ {list, build, enqueue} | Breaks rollup wall-clock into the three phases; sums to rollup_duration (`metrics.go:427`). Added to localise the uber-5k realistic-catalog rollup-p99 breach (~10× gap between bench and prod, bigfleet-uber #20). Set at `pkg/operator/rollup.go:55`, `:62`, `:72`. |
| `bigfleet_operator_acknowledge_duration_seconds` | histogram | — | One ack batch (Pending→Acknowledged status writes), buckets 10 ms→~5 min (`metrics.go:438`). Bounded by `AcknowledgeConcurrency × per-status-write`; slow apiserver (kine+sqlite throttled) can take minutes on thousand-CR batches — we want measurement, not a cap. Set at `pkg/operator/rollup.go:84`. |
| `bigfleet_operator_acknowledged_total` | counter | — | CRs transitioned Pending→Acknowledged (`metrics.go:458`). Should track unschedulable-pod arrival rate. Set at `pkg/operator/rollup.go:436`. |

Roll-ups are full-replacement: each `ClusterCapacityNeeds` is the cluster's complete desired state, so these series measure a fixed-cost-per-rollup operation, not a delta stream.

### Stream and node-state-down path

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_operator_session_reconnects_total` | counter | — | `Shard.Session` reconnect attempts (transport closed, re-dialed) (`metrics.go:463`). Near-zero in steady state. Set at `pkg/operator/operator.go:180`. |
| `bigfleet_operator_outbox_dropped_total` | counter | — | Non-rollup messages (BootstrapBlobResponse / ReclaimAck) dropped because the bounded session outbox was full (paper §10.5) (`metrics.go:453`). Recoverable — the shard re-issues on RPC timeout — but a sustained rate = the send pipeline is behind the stream. Set at `pkg/operator/stream.go:202`. |
| `bigfleet_operator_node_state_update_duration_seconds` | histogram | `phase` (resulting UpcomingNode phase) | `handleNodeStateUpdate` per inbound NodeStateUpdate, buckets to 65 s so p99 doesn't saturate under back-pressure (`metrics.go:474`, Drop B). p99 above ~100 ms ⇒ apiserver-write back-pressure bleeding into chain throughput. Set at `pkg/operator/upcoming.go:54`. |
| `bigfleet_operator_upcoming_node_writes_total` | counter | `op` ∈ {create, spec_update, status_update}, `outcome` ∈ {success, conflict, error} | UpcomingNode CRD write attempts (`metrics.go:486`). `sum / NodeStateUpdate-rate` ≈ apiserver round-trips per binding. Set at `pkg/operator/upcoming.go:86`, `:130`, `:159`, `:191`, `:209`. |
| `bigfleet_operator_dispatch_inflight` | gauge | — | Currently-running stream-dispatch goroutines (`metrics.go:495`). `recvLoop` spawns one goroutine per inbound frame with no semaphore; sustained high values = apiserver-side back-pressure (per-cluster QPS limiter draining slower than the inbound stream). Set/decremented at `pkg/operator/stream.go:277-305`. |

---

## CR controller series

The optional `bigfleet-unschedulable-pod-controller` registers on the controller-runtime registry, not the prometheus default (`pkg/controller/cr/controller.go:55`).

| Series | Type | Labels | Meaning · emit site |
|---|---|---|---|
| `bigfleet_unschedulable_pod_controller_reconciles_total` | counter | `outcome` ∈ {cr_created, cr_exists, pod_gone, pod_terminal, error} | Reconcile invocations by outcome (`controller.go:49`). Compare `cr_created` against the harness's `scaletest_loadgen_cr_created_total` to find the Pod→CR drop. |

---

## SLO-bearing metrics

Which series gates a release is a layered decision recorded across five ADRs. The short version: **binding-latency p99 is the release gate; cycle wall-clock is a tracked envelope, never a gate.** The longer version is below, because the "binding latency" you measure depends on which harness mode is running and is BigFleet-internal-only.

### Binding latency is the gate; cycle wall-clock is not (ADR-0014)

ADR-0014 reorganised the SLO surface into one release gate (`bindingLatencyP99` — CR creation → bound machine Configured), one throughput envelope (`shardCycleDurationP99 ≤ rollupInterval / 2`, default ≤ 5 s), and tracked-but-not-gated cycle/phase wall-clocks. The intent of the envelope is backlog-prevention: the shard must consume one full snapshot before the next rollup lands. A run with cycle p99 = 4.8 s **passes** if binding latency holds; cycle p99 = 6 s **fails the envelope** even if binding latency is currently fine, because the next rollup compounds the lag. `bigfleet_shard_cycle_duration_seconds` and the phase histogram feed alerts (cycle p99 > 1 s warns, > rollupInterval/2 pages, phase regression > 2× baseline warns) but a regression here wakes someone, it does not block a release.

### The binding latency we measure is internal-only (ADR-0018)

Under the harness fake provider, `Configure` returns in under a second, so the measured binding latency is **BigFleet's contribution only** — `internal_binding_latency`, not the user-facing `internal + provider_capacity_create_latency`. The runner's profile key is `internalBindingLatencyP99Seconds`; ADR-0014's tiered targets (5 s/60 s/90 s/5 min by priority tier) are user-facing ceilings that real-provider validation (conformance suite, out-of-tree provider scaletests, production canaries) owns. The harness gate is a regression detector for BigFleet's slice, not a user-experience SLO.

### Per-CR binding latency vs fingerprint fan-out (ADR-0017)

`bigfleet_shard_provisioning_latency_seconds` was the *only* latency histogram when the M32 runner first wired the gate, and it measures the wrong granularity: **per-(cluster, fingerprint) fan-out**, not per-CR. The scaleway-500k run pinned it at 327.68 s (the +Inf bucket) with every algorithmic SLO green — at 50 clusters × 1 fingerprint it took only 50 samples, each measuring "first observation of a brand-new fingerprint → first machine of it Configured", which is a cold-pool capacity-planning number, not what a user feels per CR. ADR-0017 split the two: the per-Pod histogram below became the gate; `provisioning_latency` keeps its name but its Help text now reads "fingerprint fan-out diagnostic" (`metrics.go:43`). CR-mode profiles with no pod-shim fall back to it with an explicit profile-level SLO override that admits the fan-out shape.

### The gate metric, and why it respects the rollup interval (ADR-0020)

The actual release gate reads a **harness** series, not a BigFleet one: `bigfleet_scaletest_pod_bind_latency_steady_seconds`, emitted by either the pod-shim (`test/scaletest/cmd/pod-shim/main.go:89`) or, in kube-scheduler mode, the load-driver (`test/scaletest/cmd/load-driver/main.go:263`) — exactly one source per run, selected by `HARNESS_SCHEDULER`. It records Pod `creationTimestamp` → bound, for **steady-state Pods only** (created after the cluster reached its target count), so a 50K-Pod cold-start thundering herd doesn't dominate p99. The all-Pods twin `bigfleet_scaletest_pod_bind_latency_seconds` (`pod-shim/main.go:75`) is informational. ADR-0020 sets the gate to **15 s** = `rollupInterval (10 s) + 5 s` chain headroom — the 10 s rollup is a deliberate production posture (10× fewer stream messages than 1 s rollups without meaningfully degrading user-facing latency, since real-provider create time dwarfs rollup batching), and lowering it to make a 5 s SLO pass would mask regressions in non-rollup chain stages.

### Cycle p99 is regime-parametric (ADR-0028)

The 100 ms cycle-p99 bar applies only to the **aggregated regime** (per-cluster Need count bounded by distinct fingerprints, no co-location inflation). Under the realistic catalog the cycle envelope scales linearly with NeedsTable size, so it is graded on **per-Need Phase 1 p99 ≤ 200 µs** instead (≈1.5× the empirical ~130 µs/Need at uber-5k), read from `bigfleet_shard_cycle_phase_duration_seconds{phase=phase1}` divided by the cycle's Need count, alongside rollup p99 ≤ 1 s and ack p99 ≤ 12 s. This is why a cycle-p99 alert is not automatically a regression: the per-phase split plus the regime tells you whether the cost is Need-cardinality (workload) or genuine slowdown (BigFleet).

---

## Harness metrics (test/scaletest)

These never ship in a BigFleet binary; they live in the scaletest load-driver, pod-shim, and node-creator and exist to localise where the synthetic Pod→CR→Bootstrap→Node→bind chain throttles. The gate metric above is one of them; the rest are diagnostic. Catalogued briefly because dashboards mix them with BigFleet series and the provenance (which Drop, which ADR) is otherwise opaque:

| Series | Source · meaning |
|---|---|
| `scaletest_loadgen_cr_created_total` / `_deleted_total` / `_active` / `_target` / `_errors_total{kind}` | load-driver (`load-driver/main.go:214`+). CR/Pod throughput and the runner's sustained-load denominator. |
| `scaletest_loadgen_steady_state` / `..._anchors_bound_total` | load-driver. Test-phase indicator (sum = clusterCount ⇒ fleet in steady state); ADR-0025 co-location-gang anchor binds. |
| `bigfleet_scaletest_pod_bind_latency_seconds` / `_steady_seconds` | pod-shim (`pod-shim/main.go:75`/`:89`) or load-driver (kube-scheduler mode). All-Pods vs steady-state binding latency; the `_steady_` twin is the SLO gate (ADR-0017/0018/0020). |
| `bigfleet_scaletest_pod_shim_*` | pod-shim chain-drop counters (`pod-shim/main.go:99`+): pods_marked_unschedulable, upcoming_nodes_observed, fake_nodes_created, pod_bind_attempts, pod_bind_errors{reason}, upcoming_to_node_latency, node_to_bound_latency (Drops N/Q/T). |
| `bigfleet_scaletest_node_creator_*` | node-creator (`node-creator/main.go:68`+): fake_nodes_created, upcoming_to_node_latency, bound_pods — the kube-scheduler-path equivalents (ADR-0023 split). |

When attributing a binding-latency tail, the cross-component chain is: `bigfleet_shard_actions_total{kind=Bootstrap}` (shard decided) → `bigfleet_shard_configure_phase_seconds` (shard executed) → `bigfleet_operator_node_state_update_duration_seconds` (operator wrote the UpcomingNode) → `bigfleet_scaletest_pod_shim_upcoming_to_node_latency_seconds` (harness built the Node) → `bigfleet_scaletest_pod_shim_node_to_bound_latency_seconds` (harness bound the Pod). A gap between two adjacent stages localises the bottleneck; that decomposition is the whole reason the per-machine and per-phase histograms exist.

---

## See also

- [Operator guide — Day-2 observability](../operator-guide.md#day-2-observability): the curated key-metrics subset, suggested dashboard layout, and the alert→action runbook.
- [Shard hot path](shard-hot-path.md), [Decision engine](decision-engine.md), [Phase 1 / OCC](phase1-occ.md): the code each shard series instruments.
- [Static stability](static-stability.md): why coordinator series are control-plane signals a binding never waits on.
