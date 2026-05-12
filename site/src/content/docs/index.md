---
title: BigFleet
description: A fleet-level infrastructure autoscaler. Many clusters, one fleet, one decision engine.
template: splash
hero:
  tagline: Many Kubernetes clusters. One fleet. One decision engine.
  actions:
    - text: Read the operating model
      link: https://lucy.sh/fleet-scale-kubernetes
      icon: right-arrow
      variant: primary
      attrs:
        target: _blank
        rel: noopener
    - text: Quickstart
      link: /quickstart/
      icon: external
      variant: secondary
    - text: GitHub
      link: https://github.com/intUnderflow/bigfleet
      icon: external
      variant: minimal
---

<div style="margin: 2rem 0;">
  <p style="font-size: 0.85rem; opacity: 0.7; margin: 0 0 0.5rem;">Scale testing supported by</p>
  <a href="https://uber.com" target="_blank" rel="noopener">
    <img
      class="supported-by-logo"
      src="/uber-logo.svg"
      alt="Uber"
      style="height: 32px; width: auto; max-width: none; opacity: 0.85; display: inline-block;"
    />
  </a>
</div>
<style>
  :root[data-theme='dark'] .supported-by-logo { filter: invert(1); }
</style>

## What it is

BigFleet is the piece of software the cluster autoscaler stops being when you outgrow one cluster. It runs once for an entire Kubernetes fleet. Each cluster reports the capacity it needs in a small standard contract — three CRDs and one protobuf message. BigFleet decides which physical or virtual machines should serve which cluster, and provisions, reclaims, and rebalances them across the fleet.

Inside each cluster, kube-scheduler still places pods. BigFleet doesn't touch that. It's the tier above: it makes sure the right machines exist to be scheduled on, anywhere they're needed.

![BigFleet sits one tier above per-cluster autoscalers: each Kubernetes cluster runs an operator that reports capacity needs to BigFleet, which provisions from a fleet-wide pool of bare metal, cloud, and spot machines.](/architecture-diagram.svg)

It's the reference implementation of two papers — the [operating model](/papers/fleet-scale-kubernetes/) (how cluster fleets should work) and the [architecture](/papers/bigfleet/) (how this autoscaler is built).

## Tested at scale

Designed for fleets of **up to 100 million machines** across thousands of clusters, horizontally sharded. The most recent passing benchmark — 50 simulated clusters, 50 000 demand requests, 500 000 inventory machines on a 5-node cloud cluster — held shard cycle p99 at **55 ms** under the runner's 100 ms SLO.

Every number on the [scale-test results page](/scaletest-results/) is reproducible from a committed run summary.

## Why this exists

The hyperscalers built fleet-level capacity control planes in-house — Google has [Borg](https://research.google/pubs/pub43438/), Meta has [Twine](https://www.usenix.org/conference/osdi20/presentation/tang). They did because per-cluster capacity management collapses at fleet scale.

Most organisations now run tens to thousands of Kubernetes clusters and manage capacity per-cluster. The result is well documented: industry surveys put average enterprise CPU utilisation around 18 %, with overprovisioning factors of 2× to 5× and waste estimates between $50 000 and $500 000 per cluster per year. A team's GPUs sitting idle in cluster A can't help that team's training job in cluster B. Multi-node training, gang scheduling, and priority-based preemption — all the things AI/ML workloads actually need — require fleet-wide capacity decisions. Per-cluster autoscalers can't make them.

## What changes for you

- **GPUs serve the team that needs them, not the team that owns the cluster.** Capacity is a fleet-wide pool, not a per-cluster allocation.
- **Costs are comparable across capacity classes.** Bare metal, reserved, on-demand, and spot are scored apples-to-apples by `price + interruption_probability × interruption_penalty`. The autoscaler picks the cheapest option that fits the workload's interruption tolerance.
- **Clusters become disposable.** No more "GPU cluster" / "batch cluster" snowflakes; every cluster looks identical to BigFleet, and to your platform team's runbooks.
- **The autoscaler is not on the critical path.** Static stability is a hard rule: clusters keep running with BigFleet entirely down. Running pods, kubelet, kube-scheduler are unaffected. Only new provisioning pauses.
- **Integration is small.** An operator pod runs alongside each cluster. Three CRDs (`CapacityRequest`, `UpcomingNode`, `AvailableCapacity`) and one protobuf message. Nothing touches kube-scheduler or kubelet.

## Compared to the cluster autoscalers

BigFleet replaces the **cluster-level capacity autoscaler** — [Cluster Autoscaler](https://github.com/kubernetes/autoscaler) or [Karpenter](https://karpenter.sh/). Both consume the same input signal (`PodScheduled=False, reason=Unschedulable`) and produce the same kind of output (a new node). Running them alongside BigFleet would have two deciders racing for the same trigger; one would always be undoing what the other just did.

The difference is *where* the decision happens. Cluster Autoscaler and Karpenter run inside each cluster and provision per-cluster, in isolation. BigFleet runs once across the fleet and makes the same kind of decision with full cross-cluster context — so a GPU rack idle in cluster A can satisfy a training job's unschedulable pods in cluster B without the rack having to be moved or re-provisioned.

What BigFleet does **not** replace: kube-scheduler. Pod placement stays cluster-local, the same as it is today.

If your fleet is one cluster, you don't need BigFleet. If it's a hundred, you start to.

## Where to go next

- If you're **evaluating** whether this fits your fleet → start with the [operating-model paper](/papers/fleet-scale-kubernetes/) (~15 minutes), then the [architecture paper](/papers/bigfleet/).
- If you're an **engineer** wanting to try it → [quickstart on kind in 5 minutes](/quickstart/), then the [concepts page](/concepts/).
- If you build **infrastructure providers** (AWS / GCP / bare-metal) → [provider author guide](/provider-author-guide/) and the conformance suite.
- If you want the **source** → [GitHub](https://github.com/intUnderflow/bigfleet).

## Status

v1 feature-complete. Designed and implemented by [Lucy Sweet](https://lucy.sh). Coverage: race-detector unit tests, deterministic simulator with golden traces, long-running soak, multi-cluster end-to-end on kind, [provider conformance suite](/provider-author-guide/), and the [scale-test results](/scaletest-results/) on a real cloud cluster. Real provider implementations live in separate repositories by design — see the [provider author guide](/provider-author-guide/).

<hr style="margin-top: 3rem; opacity: 0.3;" />

<p style="font-size: 0.75rem; opacity: 0.6; margin-top: 1rem;">
  Uber's support is limited to providing compute for scale-test runs.
  BigFleet is otherwise independent of Uber and is not sponsored, endorsed, or
  maintained by Uber. All trademarks belong to their respective owners.
</p>
