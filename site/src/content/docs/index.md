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

## Capacity is trapped inside clusters

Kubernetes was designed for a single cluster. Most organisations now run tens, hundreds, or thousands of clusters — and the capacity inside them is fragmented. One team's GPU rack sits idle while another team's training job is GPU-starved. There's no mechanism to rebalance.

Datadog's State of Cloud Costs report puts average enterprise CPU utilisation at around **18 %**, with overprovisioning factors of **2× to 5×** and waste estimates between **$50 000 and $500 000 per cluster per year**. The capacity is there. It's just stuck.

The hyperscalers have fleet-level control planes (Borg, Twine). Most organisations manage their fleets with per-cluster GitOps and ad-hoc rebalancing.

## What BigFleet does

BigFleet sits one tier above the cluster autoscaler. Every cluster reports its capacity needs in a small standard contract — three CRDs and one protobuf message. BigFleet provisions, reclaims, and rebalances machines across the entire fleet, treating bare-metal, reserved, on-demand, and spot as a single fungible pool priced by `price + interruption_probability × interruption_penalty`.

Three properties carry the design:

- **Capacity is decoupled from cluster identity.** A GPU rack can serve any cluster that asks for one. "The GPU cluster" stops being a fixed thing; it's wherever GPUs are right now.
- **Every cluster looks the same.** No snowflake configurations. The cluster is reduced to a scheduling domain — kube-scheduler still places pods, BigFleet just makes sure the right machines are there.
- **Static stability.** Clusters keep running with BigFleet entirely down. Running pods, kubelet, kube-scheduler are unaffected; only new provisioning pauses. The autoscaler is not on the data-plane critical path.

BigFleet is the reference implementation of two papers: the [operating model](/papers/fleet-scale-kubernetes/) (how cluster fleets should work) and the [architecture](/papers/bigfleet/) (how this autoscaler is built). It is **not** a scheduler — it does not place pods, simulate kube-scheduler, manage cluster lifecycle, or run quota / admission.

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
