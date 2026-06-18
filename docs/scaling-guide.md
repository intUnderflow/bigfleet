# BigFleet scaling guide

How to size BigFleet for your fleet, and the knobs to reach for at each scale.

## Demand-to-inventory regimes (ADR-0013)

BigFleet's `shardCycleDurationP99` SLO is **regime-aware and size-dependent** (ADR-0028): the bar is set per regime and scales with per-shard inventory — it is *not* a single fixed figure. "Demand" is unsatisfied CapacityRequests not yet bound to a configured machine; the demand-to-inventory ratio is the fraction of inventory that has demand pending against it at any moment.

| Regime | Pending demand vs inventory | When it happens | Cycle SLO |
|---|---|---|---|
| **Steady state** | ≤ 2 % (1:50) | normal day-to-day | tightest bar for the size |
| **Burst** | ≤ 10 % (1:10) | deploy ramps, AZ rebalance, batch-job submission | the *graded* bar — what the release gate measures |
| **Reprovisioning** | up to 100 % (1:1) | cluster migration, full DR, mass spot eviction | **no per-cycle SLO** — convergence-rate guarantee: `≥5 000 bindings/cycle until drain` |

The concrete bar lives in each profile's `slo:` block and is justified in [SLOs](slos.md) — at fleet scale it is seconds, not the millisecond figures an early single-shard revision of this guide quoted (the shard walks the whole inventory snapshot each cycle, so the floor rises with size). For example the published `uber-50k` clears its burst bar at shard-cycle p99 4.08 s on a 50,000-machine single shard.

**Worked example.** A real production fleet of 500 000 nodes running ~5 000 deployments has roughly 5 000 binding events per minute (Borg / Twine churn rate). With a ~30 s bind-to-running latency, ~2 500 of those bindings are pending at any moment — 0.5 % of inventory. **The fleet sits in the steady-state row 99 % of the time.** A platform-wide deployment ramp (every team pushing on a Friday afternoon) might briefly push pending demand to 5-8 % — still inside the burst regime. A 1:1 reprovisioning event is rare enough to be worth its own runbook.

The scaletest profiles model this realistically (M29). For burst-regime profiles the per-shard inventory is ~90 % Configured (already-bound, AssignedPriority pinned to 1000000 so it's never preempted, instance-type matching the load-driver's CRs so Phase 3 keeps them) and ~10 % Idle headroom. The load-driver creates 10× as many CRs as the unsatisfied burst — 9 of every 10 CRs are pre-matched by Configured (no Phase 1 work, no Phase 3 reclaim), the remaining 1 of every 10 is the burst that drives Phase 1 work. Earlier revisions of these profiles pre-seeded the entire fleet as Idle, which is not what real fleets look like and meant Phase 2/3 were never exercised at-scale against a Configured-heavy snapshot.

Per ADR-0013, the cycle-p99 SLO is *measured* under burst conditions (the worst case real fleets actually hit during normal operation). Reprovisioning is its own regime gated on throughput rather than per-cycle latency. The scaletest profiles follow the regime split:

| profile (file) | machines | regime | status |
|---|---|:--|:--|
| `uber-5k` (`5k.yaml`) | 5,000 | burst, single shard | ✅ published |
| `uber-50k` (`50k.yaml`) | 50,000 | burst, single shard | ✅ published |
| `uber-500k` (`500k.yaml`) | 500,000 | burst, single shard | planned (test-fleet-capacity gated) |
| `uber-1m` (`1m.yaml`) | 1,000,000 | burst, 2 shards | planned |
| `uber-5m` (`5m.yaml`) | 5,000,000 | burst, 10 shards | planned |

Profiles are graded on the **ADR-0054 capacity-delivery gate set** — configure-phase, bootstrap-success, node-state-publish, roll-up, shard-cycle, ack, `shortfalls == 0`, and a loose bind-p50 — measured at steady state under churn through a real, *uncapped* kube-scheduler. End-to-end pod-bind p99 is **not** gated (it is scheduler-retry + reprovision-bound, not BigFleet's deliverable). The exact bars live in each profile's `slo:` block and are justified per-metric in [SLOs](slos.md); achieved numbers are on the [scale-test results](./scaletest-results.md) page. (`uber-*` is the published *label* for the substrate-agnostic `Nk.yaml` profiles; there is no `uber-5k.yaml`.)

The 1:1 reprovisioning regime (gated on convergence rate, not cycle p99) is exercised by scaling a profile's demand × 10; convergence-rate gating in the runner is manual (interpret `bigfleet_shard_actions_total{kind="Bootstrap"}` from the snapshot).

## Per-shard machine ceiling

The shard's hot path runs over the inventory snapshot every cycle, so per-cycle cost scales with per-shard inventory. The cycle-p99 SLO is **regime-parametric** (ADR-0028) — graded in the burst regime under the ADR-0054 release gate — **not** a single 100 ms bar (an earlier revision of this guide quoted one; that framing was retired by ADR-0028/0054).

**Proven single-shard operating point:** `uber-50k` runs **50,000 machines on one shard** at shard-cycle p99 **4.08 s**, inside the burst bar, with every capacity-delivery gate green — published and reproduced 4× (see the [scale-test results](./scaletest-results.md)). The next single-shard target is `uber-500k` (500,000 machines — the M11 per-shard ceiling); it is gated on **test-fleet capacity, not the engine**.

Local microbenchmarks (`pkg/shard/cycle_bench_test.go`) bound the per-cycle **compute** cost on the dev box (substrate-independent — useful as a relative scaling curve, not an SLO):

| Inventory size | Cycle p99 (M5 Max bench) |
|---|---|
| 50K | 245 ms |
| 100K | 412 ms |
| 500K | 2.15 s |

The path to higher per-shard numbers is M11.18 (parallel/batched execute) plus incremental snapshots. In practice per-shard count is rarely the binding limit — operational blast radius is (see the sizing table below).

## Sizing table

The table below maps fleet size to BigFleet's recommended deployment shape. Numbers come from the BigFleet paper §11 and the measured ceilings in `docs/plan.md` §5.1 + §10.

| Fleet machines | Clusters | Shards | Shard CPU/RAM | Coordinator | Notes |
|---|---|---|---|---|---|
| ≤ 10K | ≤ 2 | **1** | 0.5 / 1 GB | 1 replica (no HA) | Single binary works. Skip the coordinator entirely if you're OK with no cross-shard rebalance. |
| ≤ 100K | ≤ 20 | **1** | 1 / 2 GB | 3 replicas | First scale where HA matters. Single shard is still fine. |
| ≤ 1M | ≤ 200 | **2–5** | 2 / 4 GB each | 3 replicas | First shard split. The coordinator becomes load-bearing for cross-shard rebalance. |
| ≤ 10M | ≤ 2,000 | **20** | 2 / 4 GB each | 3 replicas | Multiple cloud accounts per region for provider throughput. Provider must support `since_revision` cursor. |
| ≤ 100M | ≤ 20,000 | **200** | 4 / 8 GB each | 3 replicas | Full two-tier hierarchy. Coordinator-side machine-id assignments at topology-domain granularity (plan §0.1 A) are mandatory. |

The general rule: shard count grows roughly linearly with fleet size. Per-shard limits aren't the bottleneck — operational blast radius is. Start small (1 shard) and split when:

- Per-shard machine count is approaching ~500K (the M11 single-shard ceiling), *or*
- A shard's hot-path cycle-p99 is approaching its regime SLO (ADR-0028/0054), *or*
- A single shard's failure would impact too many clusters at once for your incident-response budget.

Don't push a shard toward the 500K ceiling without first exercising a soak at that size (the soak / failover / scale-down depth campaign is the dimension that proves it, separate from the throughput ladder).

## Per-cluster CR ceiling

Each operator holds an informer cache of every CapacityRequest in its cluster. At 250K CRs per cluster (roughly the paper's worst case), the operator's RAM is ~500 MB. **etcd is the binding constraint**: 250K objects × ~1 KB each = ~250 MB, ~3% of an 8 GB etcd budget. Co-tenancy with everything else in the cluster's etcd may push past sensible limits in some environments.

Mitigations, in order of preference:

1. **Use the unschedulable-only mode** of `bigfleet-unschedulable-pod-controller` (the default — it only creates a CR when a Pod is `PodScheduled=False`/`Unschedulable`). This caps CR count at the unschedulable-pod count, which is bounded by your provisioning latency. At 30s provisioning latency and 1K pod/sec arrivals, that's ~30K CRs in flight.
2. **Drive CRs from Kueue** instead of the per-pod controller. Kueue creates one CR per workload (PodGroup), not per pod. Same demand, ~10× fewer CRs.
3. **If neither helps**, partition the cluster's workloads across multiple Kubernetes namespaces and watch only the relevant ones — though this requires changes to the controller's watch filter.

## Knobs by tier

### Coordinator

- `coordinator.replicas`: 1 for dev, 3 for production. Per ADR-0002, single Raft group / single region for v1.
- `coordinator.storage.size`: 10 Gi default is generous. Raft log + snapshots are ~500 MB at 100M nodes; 10 Gi fits ~20 snapshot-retention cycles plus headroom.

### Shard

- `shard.replicas`: see the sizing table above.
- `shard.resources.requests.cpu`: 500m is enough at ≤100K machines/shard. Per the paper's L3-cache-resident-state argument, shards are CPU-and-memory-bound, not I/O-bound; raise CPU before RAM.
- `Phase2Options` (in code): victim-scoring weights. Defaults are fine; tune only if you have a workload mix that changes the priority-vs-drain-speed-vs-penalty tradeoff.
- `BootstrapTimeout` (in code): default 30 s. Operators that take longer to mint kubelet bootstrap blobs (e.g., a slow CA roundtrip) should raise this; the shard waits for this duration before failing the bootstrap action.

### Operator

- `RollupInterval`: 10 s default per the paper. Faster pickup of demand at the cost of more apiserver load. Don't go below 1 s.
- `AcknowledgeConcurrency`: 16 default. **Raise to 64 if your cluster has > 1K pending CRs at any time** — without this the per-rollup status writes serialise behind client-go's QPS limit. Not raising this caused our M4 1500-CR scale run to time out before the fix.
- `restCfg.QPS` / `restCfg.Burst`: 50/100 default in `cmd/operator`. Bump to 200/400 for clusters with > 5K CRs at any time. Coordinate with whoever owns your apiserver flow-control.

### `bigfleet-unschedulable-pod-controller`

- Default mode is unschedulable-only (one CR per `PodScheduled=False/reason=Unschedulable` pod, owned by the pod, GC'd via ownerRef). This is the right default — see "Per-cluster CR ceiling" above.
- `restCfg.QPS` / `restCfg.Burst`: same advice as the operator.

## When to enable the `since_revision` cursor

The provider's `List` RPC carries an opaque `since_revision` cursor (per plan §0.1 C). Below ~10K machines per shard, full-list reconciliation every cycle is fine. Above that, the provider should support cursor-based incremental list — failing this, the shard's per-cycle reconciliation traffic balloons quadratically with provider size.

The conformance suite (`make conformance TARGET=...`) reports whether your provider advances the revision; it's currently informational below the threshold and should become required above it. Provider authors: see `docs/provider-author-guide.md` for implementation guidance.

## Scaletest ramp budget

The scaletest-runner gates each profile with a "ramp-to-steady-state" deadline. If the fleet hasn't reached ≈ 99.9 % of the profile's target Pod count by the deadline, the runner aborts pre-soak and reports failure.

A profile may set an explicit `rampBudget`; otherwise the runner falls back to the M22 formula `max(15 min, totalCRs / 750 CR/sec, durationSeconds × 0.5)`. The uber-* ladder profiles set explicit overrides, because a realistic fleet ramps for longer than the formula floor as it scales:

| profile (file) | `rampBudget` |
|---|---|
| `uber-5k` (`5k.yaml`) | 60m |
| `uber-50k` (`50k.yaml`) | 120m |
| `uber-500k` (`500k.yaml`) | 240m |
| `uber-1m` (`1m.yaml`) | 240m |
| `uber-5m` (`5m.yaml`) | 240m |

Override per profile with the `rampBudget:` field; the 750 CR/sec floor in the fallback formula is empirical (the 1M de-risk sustained ~1110 CR/sec, so 750 leaves ~1.5× headroom).

## CI vs local hardware

Scale ceilings (the numbers under `test/scale/`, build tag `scale`) are **measured on the M5 Max dev box** (18-core / 64 GB / Docker Desktop). They are not run on CI — `ubuntu-latest` is roughly 3× slower CPU and ¼ the RAM, and we don't want CI flakes from machine-speed-dependent timing.

The simulator (`make sim`) and soak (`make soak`) are deterministic — same trace bytes regardless of machine speed — so they're safe to run anywhere; nightly CI runs both.

## Cross-references

- Architecture: [BigFleet paper](https://lucy.sh/bigfleet) (vendored at `docs/papers/bigfleet.md`)
- Operating model: [Fleet-Scale Kubernetes paper](https://lucy.sh/fleet-scale-kubernetes) (vendored at `docs/papers/fleet-scale-kubernetes.md`)
- Implementation plan + scalability concerns: `docs/plan.md` §10
- Operator runbook: `docs/operator-guide.md`
- Provider authoring: `docs/provider-author-guide.md`
