# BigFleet scaling guide

How to size BigFleet for your fleet, and the knobs to reach for at each scale.

## Per-shard machine ceiling (revised by scale tests)

The plan §5.1 aspirational ceiling of 500K machines per shard does not hold at the runner's `shardCycleDurationP99 ≤ 100 ms` SLO. Empirical numbers from `pkg/shard/cycle_bench_test.go` and the Scaleway 500K real-cluster run:

| Inventory size | Cycle p99 (M5 Max bench) | Cycle p99 (PRO2 cloud) |
|---|---|---|
| 50K | 245 ms | ~480 ms |
| 100K | 412 ms | ~820 ms |
| 500K | 2.15 s | 4.07 s |

**Effective per-shard ceiling at the 100 ms SLO is ~25K–50K machines** depending on hardware single-thread speed. To reach plan §5.1's 100M-machine fleet target, the deployment needs 2–4K shards, not the 200 the original sizing implied. See M11.18 (parallel/batched execute) and any future M11.19+ work on incremental snapshots for the path to a higher per-shard number.

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

- Per-shard machine count is approaching ~500K, *or*
- A shard's hot-path cycle duration p99 exceeds 100 ms, *or*
- A single shard's failure would impact too many clusters at once for your incident-response budget.

## Per-shard machine ceiling

The shard's hot path runs over the inventory snapshot every cycle. Plan §10 estimates ~500K machines per shard fits comfortably in a few hundred MB and stays under the 50 ms cycle budget. We've measured the M6 ceiling — 100K machines across 3 shards — at 400 ms rebalance latency, 12.5× under the 5s plan target. Headroom is generous.

Don't push past 500K per shard without first exercising the soak test (`make soak`) at that size.

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
- `coordinator.rebalanceInterval`: 5 s default. Lowering increases responsiveness but burns more apply-cycles. Raise to 30s+ once your fleet is mostly stable (rebalance is rare in steady state).

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

The scaletest-runner gates each profile with a "ramp-to-steady-state" deadline. If the load-driver fleet hasn't reached ≥ 99.9 % of `totalCRs` (= `kwok.clusterCount × loadProfile.target`) by the deadline, the runner aborts pre-soak and the run reports failure.

Default formula (M22):

```
rampBudget = max(15 min, totalCRs / 750 CR/sec, durationSeconds × 0.5)
```

Resolved budgets for the in-tree profiles:

| profile | totalCRs | resolved budget | which clause |
|---|---|---|---|
| `dev-5k` | 5K | 15 min | floor |
| `local-50k` | 50K | 15 min | floor |
| `scaleway-500k` | 50K | 15 min | floor |
| `scaleway-1m` | 1M | ~22 min | 750 CR/sec |
| `scaleway-5m` | 5M | ~112 min | 750 CR/sec |
| `failover-soak` | 50K (60 min soak) | 30 min | durationSeconds × 0.5 |

The 750 CR/sec floor is empirical: the 1M de-risk run sustained ~1110 CR/sec aggregate; sizing budget at 750 gives ~1.5× headroom over the observed throughput. If a real run is missing the gate by a small margin (the 1M de-risk landed at 998 975 / 999 000), the resolved budget value gets logged at runner startup so the failure mode is obvious from `runner.log`.

Profile authors can override the formula directly:

```yaml
rampBudget: 45m
```

## CI vs local hardware

Scale ceilings (the numbers under `test/scale/`, build tag `scale`) are **measured on the M5 Max dev box** (18-core / 64 GB / Docker Desktop). They are not run on CI — `ubuntu-latest` is roughly 3× slower CPU and ¼ the RAM, and we don't want CI flakes from machine-speed-dependent timing.

The simulator (`make sim`) and soak (`make soak`) are deterministic — same trace bytes regardless of machine speed — so they're safe to run anywhere; nightly CI runs both.

## Cross-references

- Architecture: [BigFleet paper](https://lucy.sh/bigfleet) (vendored at `docs/papers/bigfleet.md`)
- Operating model: [Fleet-Scale Kubernetes paper](https://lucy.sh/fleet-scale-kubernetes) (vendored at `docs/papers/fleet-scale-kubernetes.md`)
- Implementation plan + scalability concerns: `docs/plan.md` §10
- Operator runbook: `docs/operator-guide.md`
- Provider authoring: `docs/provider-author-guide.md`
