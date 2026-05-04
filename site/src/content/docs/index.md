---
title: BigFleet
description: A fleet-level infrastructure autoscaler. Many clusters, one fleet, one decision engine.
template: splash
hero:
  tagline: Many Kubernetes clusters. One fleet. One decision engine.
  actions:
    - text: Quickstart
      link: /quickstart/
      icon: right-arrow
      variant: primary
    - text: Architecture
      link: /architecture/
      icon: external
      variant: secondary
    - text: GitHub
      link: https://github.com/intUnderflow/bigfleet
      icon: external
      variant: minimal
---

## What BigFleet is

BigFleet is a **fleet-level infrastructure autoscaler**. It receives capacity needs from many Kubernetes clusters and provisions or reclaims machines through pluggable, **out-of-tree** `CapacityProvider` backends. It's the reference implementation of the design described in two papers — [BigFleet](https://lucy.sh/bigfleet) (architecture) and [Fleet-Scale Kubernetes](https://lucy.sh/fleet-scale-kubernetes) (operating model). Both are also vendored in this site under [/papers/bigfleet/](/papers/bigfleet/) and [/papers/fleet-scale-kubernetes/](/papers/fleet-scale-kubernetes/).

BigFleet is **not** a scheduler. It does not place pods, simulate kube-scheduler, manage cluster lifecycle, or run quota / admission. It sits one layer below the cluster autoscaler.

## Proven at scale

The reference implementation passes the runner's SLO checks at the production-shape benchmark: **50 simulated clusters, 50 000 demand CRs, 500 000 pre-seeded inventory machines** on a 5-node Scaleway Kapsule (PRO2-M, nl-ams).

| metric | result |
|---|---|
| **Shard cycle p99** | **55.5 ms** (SLO 100 ms) |
| Operator rollup p99 | 15.7 ms |
| Operator ack p99 | 156 ms |
| Sustained active CRs | 50 000 (target) |
| Shortfalls | 0 |

Cumulative trajectory across the M11 milestone series: cycle p99 fell from 4.08 s to 55.5 ms — a **98.6 % reduction** — at the same load. Per-run details, prometheus snapshots, and a log-scale progression chart live on the [scale-test results page](/scaletest-results/); every number is reproducible from a committed `summary.json`.

The shard's per-shard ceiling is intentionally generous because the design is horizontally sharded. Per the paper, the architecture is sized for a **100 M-node fleet across roughly 200 shards**, with topology constraints contained inside a single shard.

## Where to go next

- **[Quickstart](/quickstart/)** — bring up BigFleet on a kind cluster in five minutes.
- **[Concepts](/concepts/)** — Need, Profile, Penalty, Cost, the three Phases, victim score.
- **[Architecture](/architecture/)** — two-tier design, decision engine phases, static stability.
- **[API reference](/api-reference/)** — CRDs and gRPC services.
- **[Operator guide](/operator-guide/)** — install, metrics, runbook.
- **[Scaling guide](/scaling-guide/)** — sizing for 10K to 100M machines.
- **[Provider author guide](/provider-author-guide/)** — implementing a `CapacityProvider`.

## Status

**v1 feature-complete.** Tested via race-detector unit tests, multi-cluster e2e on kind, deterministic simulator with golden traces, long-running soak, provider conformance suite, and Helm chart render smoke tests. See the [implementation plan](/plan/) for milestone history.

Real provider implementations (AWS, GCP, Azure, bare-metal) live in separate repos by design — see the [provider author guide](/provider-author-guide/).
