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

## Built for fleet scale

BigFleet is designed for fleets of **up to 100 million machines** across thousands of Kubernetes clusters. A single shard handles a half-million-machine workload comfortably under its latency budget; the architecture is horizontally sharded from the start, so adding more clusters means adding more shards, not running into ceilings.

Every result on the [scale-test results page](/scaletest-results/) is reproducible from a committed run summary — no fudged numbers, no asterisks.

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
