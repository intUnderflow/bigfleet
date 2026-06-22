---
title: BigFleet and networking
description: How BigFleet handles networking — by expressing networking needs as capacity requirements rather than modelling the network as a separate subsystem. What that covers, what it deliberately does not, and where the responsibilities sit.
---

A recurring question about a fleet-level capacity autoscaler is networking: *not all capacity is equal, Kubernetes networking is already complex — doesn't moving capacity between clusters founder on that?*

The short answer:

> **BigFleet doesn't model the network, because a workload's networking needs — locality, latency, residency, fabric-adjacency — *are* requirements on the capacity it requests. The unit of fungibility is not "any node" but "any node satisfying the same requirements." A requirement BigFleet can't satisfy within a shard becomes a shortfall, never a placement that violates an expressed requirement. "Not all capacity is equal" is the premise of the matching model, not a gap in it.**

This page is the long answer: what that covers, what BigFleet deliberately leaves to other layers, and where the responsibilities sit.

## Networking needs are capacity requirements

BigFleet is not a scheduler. It provisions whole nodes; kube-scheduler places pods. So BigFleet never has to reason about pod-to-pod connectivity. What it reasons about is which *machines* should exist — and a machine's networking properties are expressed the same way every other property is: as requirements on the request.

- **Zone** is a first-class field the shard reads directly to satisfy `topology.kubernetes.io/zone` selectors — not a label afterthought.
- **Region, instance type, and arbitrary provider labels** are matched as `nodeSelector`-style requirements.
- **Co-location** (gang/rack adjacency) is expressed with the `Same` operator and `TopologySpread`, over whatever `(key, value)` topology domains the provider exposes.

Matching is **requirement-gated before cost is ever consulted**: the cost function only breaks ties among machines that *already* satisfy every requirement. A workload that pins its zone, names its `Same` key, or selects its region cannot be served by capacity that violates *that requirement* — if nothing in the shard satisfies it, the demand becomes a **shortfall** (surfaced, aged, escalated), never a wrong-zone, wrong-region, or wrong-fabric provision.

The corollary matters: BigFleet enforces only the requirements a workload **expresses**. A `Need` that omits its region can be served by capacity in another region of the same shard — expressing locality is the operator's job, not something BigFleet infers (see [Default locality](#default-locality)).

## The fungibility boundary

Capacity is fungible **within a shard**, not across the whole fleet. Two clusters share one capacity pool only if they are bound to the same shard, and **topology constraints never cross shard boundaries** — a `Same`-rack request that can't be met inside a shard is a shortfall, not a cross-shard search.

This makes the shard the natural unit to align with a network domain. The paper makes the coupling explicit: the shard count is bounded by the shallowest scarce resource pool, *and topology-constrained requests must remain satisfiable within a single shard*. The practical guidance that follows: **align shard membership with the network domain you expect capacity to move within** (AZ / VPC / region as appropriate). BigFleet does not *enforce* that today — see [Default locality](#default-locality).

## Default locality

BigFleet matches the requirements a workload **expresses**, and only those. The cluster roll-up does not stamp a default region or zone onto a `Need`, so a workload that does not pin its locality can be served by capacity anywhere in its shard — including a different region or VPC, where it may not actually reach the cluster's network. Safe-by-default is, today, the operator's responsibility:

- **Express the locality you require** (region / zone / `Same` domain) on locality-sensitive workloads.
- **Align each shard with one network domain** (AZ / VPC / region), so the shard's whole pool is reachable rather than relying on every workload to self-describe.

Whether BigFleet should inject an ambient locality bound at roll-up, or warn when a region-spanning shard binds a locality-free `Need`, is an open design question.

## What BigFleet deliberately does not model

These are scope-outs by design, not oversights. In each case the need is expressed at a layer better suited to it:

- **Network / egress cost is not in the cost formula.** `effective_cost = price + interruption_probability × interruption_penalty` is fixed and has no transfer-cost term. Network *economics* are handled as *constraints*, not price: a workload whose egress cost dominates pins its locality as a hard requirement, and BigFleet will shortfall rather than place it far from its data. BigFleet is not a cloud-cost optimiser; it does not make a soft "cheaper compute vs. higher egress" trade. If you need that trade, express the locality you require.
- **Physical fabric is not modelled.** `Same` is label-equality over opaque domains; BigFleet has no bandwidth/oversubscription/bisection model. Fabric adjacency (RDMA / InfiniBand leaf / NVLink rail) is expressed by the granularity of the provider's topology labels and enforced by the provider's label taxonomy plus kube-scheduler — BigFleet matches over whatever proximity tiers the provider exposes.
- **Service-mesh enrollment is the bootstrap's job.** A node joins its destination cluster through that cluster's operator-supplied `BootstrapTemplate`; mesh/SPIFFE enrollment belongs there. BigFleet uses priority and `interruption_penalty` (a term in `effective_cost` and victim scoring) as the dial that keeps stable workloads from being churned.

## Two responsibilities at the provider boundary

Because providers are out-of-tree, two networking-adjacent responsibilities live at the provider boundary rather than in the engine — one the contract now requires, one still an open question. We call them out so adopters wire them correctly:

- **A node counts as capacity only once it has actually joined.** A node's value is realised when kubelet has registered and the node is `Ready` on its target cluster's network — not when its VM boots. The provider contract requires it (ADR-0056): a provider must not report a machine `Configured` until it has observed the node `Ready`. Because the six provider RPCs carry no node-readiness ground truth, enforcement is **layered, not a single black-box check** — `providerkit` centralises the wait-for-`Ready` so every kit-based provider inherits it; the conformance suite certifies the gate against the reference provider (behaviour `B708`); and a provider that hand-rolls the contract is trusted to honour the obligation and proves it in its own against-a-cluster integration test. The point is the same either way: reporting `Configured` before the node joins would credit capacity that isn't schedulable, and the contract forbids it.
- **Host hygiene on reuse is an open responsibility.** When a node is reclaimed from one cluster and re-provisioned into another, it is the same physical hardware. For multi-tenant or regulated fleets that draw a security boundary between clusters, host sanitisation between uses (disk / memory / accelerator-memory scrub) must be enforced at the provider or platform layer — BigFleet's drain is a pod-eviction, not a wipe. Whether BigFleet should require a sanitisation step (or a trust-tier fence) on the reuse edge is an open design question.

## In one line

BigFleet treats networking the way Kubernetes itself does: as constraints the workload expresses, satisfied by matching — not as a subsystem the capacity layer models. That makes most of the "networking" objection a question of demand expressiveness (which the contract handles) and provider-boundary correctness — node-join readiness the contract now **requires** and the reference and kit paths **certify**, host hygiene on reuse an open responsibility it **delegates** — not a reason the model can't work.
