# ADR-0015: Realistic archetype improvements — fingerprint multiplicity, bimodal lifetimes, concentrated bursts, `Same`-rack workloads, cluster-size skew

**Status**: Accepted

**Date**: 2026-05-06

## Context

M31 introduced a six-archetype workload catalog (`test/scaletest/profiles/archetypes/realistic.yaml`) and re-shaped the load-driver + Configured seed to read the same file. That's an honest improvement on M29's single-shape harness, but an audit against what production fleets actually look like (Borg, Twine, observed Kubernetes-on-everything) surfaced significant remaining gaps:

| Axis | M31 catalog | Production reality | Gap |
|---|---|---|---|
| Profile fingerprints per cluster | ~6 archetypes × ~3 zones × ~2 instance types per pool ≈ 36 | 50-200+ per large cluster | 3-5× too few |
| Fingerprint cardinality fleet-wide | ~36 (every cluster identical) | thousands (per-team labels, app-version labels, etc.) | very large |
| CR lifetime distribution | uniform 5%/min churn on a fixed Target | bimodal: long-lived services (days) + short-lived batch (minutes-hours) | large — biggest realism gap after fingerprint cardinality |
| Demand burstiness | uniform churn across all clusters | concentrated in time AND in cluster (Friday deploy hits 5 clusters simultaneously) | large — the "1:10 burst regime" tests aren't actually bursty |
| Topology constraints | none — no `Same`, no zone-spread | `Same`-rack pinning + pod-topology-spread are common in production | critical — the protobuf-only `Same` operator is paper-load-bearing and untested under load |
| Resource-quantity diversity | one fixed value per archetype | each archetype is itself a distribution (cpu-service: 1c-32c) | moderate — multiplies fingerprint cardinality |
| Cluster size skew | uniform: every cluster identical Target | heavy-tailed: 5 huge + 50 medium + 500 small | moderate — affects per-cluster Phase 3 cost distribution |

The takeaway: the M31 catalog is "more honest than M29 was, less honest than it claims to be." Conclusions from runs against the M31 catalog about Phase 2/3 algorithmic fitness, cycle p99 budget, or CPU efficiency may not generalise to production. Before any further optimisation work or SLO claims rest on these tests, the harness needs to close the largest realism gaps.

## Decision

Five extensions land together, each closing one of the named gaps. They share one design principle: every dimension that production fleets distribute *over*, the harness must also distribute *over* — not just "vary," but "match production-shaped distributions."

### 1. Per-archetype fingerprint multiplicity

Each archetype expands to N concrete profile fingerprints by combining its instance-type pool with size buckets and zones. A `cpu-service` archetype with 3 instance types × 3 zones × 4 size buckets generates 36 distinct profiles, drawn weighted-uniformly per CR.

Schema addition:

```yaml
archetypes:
  - name: cpu-service
    sizeBuckets:
      - { weight: 40, cpu: "2",  memory: "8Gi"  }   # most services
      - { weight: 35, cpu: "4",  memory: "16Gi" }
      - { weight: 20, cpu: "8",  memory: "32Gi" }
      - { weight: 5,  cpu: "16", memory: "64Gi" }   # rare big services
```

When `sizeBuckets` is non-empty, the archetype's top-level `resources` is ignored and per-CR resources are picked weighted-random from the bucket list. Catalog-wide this produces 100-500 distinct fingerprints per cluster — production-shaped without making the load-driver unmanageable.

### 2. Bimodal CR lifetimes

The current `churnPerMinute: 0.05` model assumes every CR has the same expected lifetime. Real fleets are bimodal: long-running services live days (effectively immortal at scaletest timescales), short-running batch lives minutes.

Schema addition (per-archetype):

```yaml
- name: cpu-batch
  meanLifetimeSeconds: 600   # 10-minute mean batch job
- name: cpu-service
  meanLifetimeSeconds: 0     # 0 = effectively immortal, no aging
```

The load-driver tracks per-CR creation timestamps. On each tick, every CR with `meanLifetimeSeconds > 0` is independently aged (exponential distribution) and replaced when sampled. Population stays at Target — replacements are created as fast as deletions. Long-lived archetypes contribute to baseline population; short-lived archetypes drive the bulk of the churn. The legacy `churnPerMinute` field stays as a fallback when no archetype has lifetimes set.

### 3. Concentrated bursts

A new `burst` runner action injects K extra CRs of a chosen archetype across N selected clusters at offset T, optionally reverses M seconds later. Models a Friday-afternoon deploy ramp: most clusters quiet, a few ramping hard simultaneously.

Schema addition:

```yaml
runnerActions:
  - atSeconds: 300
    action: burst-archetype
    burst:
      archetype: cpu-service
      clusters: 5            # how many clusters get the spike
      crsPerCluster: 2000    # extra CRs each
      durationSeconds: 60    # auto-reverse after this many seconds
```

The runner picks `clusters` random clusters at fire time, creates the extra CRs through their respective load-drivers, and (if `durationSeconds > 0`) deletes them after the window. Phase 1's per-cluster hot-spot behaviour is finally exercised.

### 4. `Same`-rack-operator workloads

Per the BigFleet paper, the `Same` operator constrains a *group* of machines to share an attribute value (rack, AZ, blast domain). Cluster operators emit it during roll-up; it never appears in the user's CRD (the CRD uses `In/NotIn/Exists/DoesNotExist`). Phase 1 / Phase 2's spread-aware paths handle it. This is currently un-exercised in any scaletest profile.

Implementation requires two pieces:

**Synthetic rack labels in the seed.** Each Configured machine gets a `topology.bigfleet/rack` label drawn from a pool of N racks (e.g., 10 racks per zone). The seed knows the pool and emits labels deterministically. The load-driver-side seed mirror keeps the same rack pool.

**A `same-rack` archetype attribute.** When `sameRack: true`, the load-driver emits a `Same` requirement on `topology.bigfleet/rack` for the CR's group. Group sizes are drawn from a small distribution (2-8 nodes typical for tightly-coupled training jobs).

```yaml
- name: gpu-training
  sameRack: true
  groupSizeRange: [2, 8]
```

Phase 3's reclaim path and Phase 2's preemption path now have a non-trivial `Same`-aware load to validate against.

### 5. Cluster-size skew

The harness currently runs uniform clusters. Real fleets are heavy-tailed: a few huge clusters (running batch in bulk), many medium, a long tail of small.

Schema addition (top-level):

```yaml
kwok:
  clusterCount: 100
  clusterSizeDistribution:
    - { fraction: 0.05, targetMultiplier: 5.0 }   # 5% are 5× the base target
    - { fraction: 0.20, targetMultiplier: 2.0 }   # 20% are 2× the base
    - { fraction: 0.50, targetMultiplier: 1.0 }   # 50% baseline
    - { fraction: 0.25, targetMultiplier: 0.3 }   # 25% small
```

The harness picks each kwok pod's effective Target from this distribution at deploy time. Per-cluster Phase 1 / Phase 3 work is no longer uniform; the per-cluster Configured count varies 5×-15× across the fleet, exercising the per-(cluster, fingerprint) cost distribution honestly.

## Consequences

- **The realistic.yaml catalog is rewritten** to use the new schema (sizeBuckets, meanLifetimeSeconds, sameRack where applicable). Backward compatibility with the M31 inline schema is retained: an archetype with neither `sizeBuckets` nor `resources` falls back to "no resources required."
- **The load-driver, the shard's seed, and the runner all read the catalog.** Adding sizeBuckets / sameRack / meanLifetimeSeconds touches `pkg/scaletest/archetype` (shared types), `test/scaletest/cmd/load-driver/main.go` (CR build path + lifetime aging), `cmd/bigfleet/shard.go` (seed expansion), and `test/scaletest/cmd/scaletest-runner/main.go` (burst runner action).
- **A new `scaleway-1m-realistic.yaml` shape lands** that uses every dimension. Existing scaleway-1m.yaml and scaleway-5m.yaml stay on the M29 single-shape catalog as compatibility baselines; the realistic profile is the new release-gate target.
- **Cycle p99 will likely rise** vs. the M31 catalog. M30.{1,2}'s fast paths fire less often — pin-only is gated on archetypes without resources (almost none), min-priority short-circuit is gated on no preemptable victims (rarely true with real priority diversity). Under ADR-0014's posture this is fine: cycle p99 is a tracked metric, not a release gate; the binding-latency gate is what releases pass / fail on.
- **`Same`-operator code paths get their first under-load exposure.** Existing unit and conformance tests cover correctness; the realistic harness validates throughput. Expect surfacing of inefficiencies in the Phase 1/Phase 2 group-aware code paths — those will be tracked as new milestones if found.
- **Bimodal lifetimes change the rollup-traffic profile.** The operator's per-CR ack QPS now sees bursts of deletes-and-creates from short-lived archetypes; the steady-state ack p99 SLO (12s) may need re-examination. Add to the open-question list for the first cloud run.
- **Cluster-size skew interacts with multi-shard load balance.** ADR-0007 binds clusters to shards at deploy time via the harness's `c % shardReplicas` mapping. With heavy-tailed sizes, shard load can drift up to ~3× across replicas (one shard owns the big cluster). The shard.replicas count is still per-deploy operator-chosen (ADR-0007); the harness records the per-shard load distribution in summary.json so this is measured, not papered over.
- **Backward compatibility for existing profiles.** Profiles without the new schema fields keep behaving as before. M31's scaleway-1m-realistic.yaml continues to work; the new profile lands alongside it as `scaleway-1m-realistic-v2.yaml` (or replaces it after one release of overlap; final naming chosen at implementation time).
