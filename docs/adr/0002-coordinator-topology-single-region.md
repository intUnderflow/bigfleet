# ADR-0002: Coordinator topology — single Raft group, single region (v1)

**Status**: Accepted

**Date**: 2026-04-30

## Context

BigFleet's global coordinator is a small, low-write-rate state machine: shard membership, cluster→shard mapping, topology-domain→shard assignments, quota allocations, provider registry. At 100M nodes the coordinator's state is on the order of a few megabytes (after the per-domain assignment fix in `docs/plan.md` §10.1) with steady-state writes around 10/sec.

The plan calls for the coordinator to be replicated for availability. Three replicas with a consensus protocol is the canonical answer. The open question is the *topology* of those replicas.

Three candidate shapes existed when this decision was made:

1. **Single Raft group, single region.** Three replicas in three AZs of one operator-chosen region.
2. **Single Raft group, multi-region.** Replicas spread across regions.
3. **Hierarchical / federated.** A top-level coordinator with regional sub-coordinators.

Tradeoffs:

- Option 1: simplest. `hashicorp/raft` is well-understood for this shape. Median write latency is millisecond-class. The cost is a SPOF at the chosen region for *cross-shard rebalancing* — but not for the data plane, because BigFleet is statically stable and shards continue to provision from their existing allocations during a coordinator outage. A regional outage is therefore degraded service, not lost service.
- Option 2: cross-region Raft writes pay inter-region RTTs (tens to hundreds of ms) on every quorum. At 10 writes/sec this is fine arithmetically, but burst events (mass spot reclamation, AZ failure) push write rates higher and the latency floor matters then. Operationally it also doubles the failure surface: WAN flapping causes leader churn that single-region deployments don't see.
- Option 3: real engineering. Hierarchical means a new tier of code, a new failure mode (regional coordinator divergence), and a substantially larger v1 surface area. The papers don't require it; the design memory doesn't require it; nothing about 100M nodes intrinsically requires it given the per-domain assignment model.

Static stability — the property that the data plane keeps running with the coordinator down — is the load-bearing safety net here. It is what makes a single-region coordinator a defensible choice for v1.

## Decision

For v1, the coordinator is **a single Raft group of three replicas, deployed in a single region of the operator's choice**. The operator places the three replicas across three AZs of that region for AZ-level fault tolerance.

Backing store: `hashicorp/raft` + BoltDB on local disk per replica. Snapshots written periodically to local disk; off-host DR copies to durable object storage are tracked as a separate concern (`docs/plan.md` §10.8) and do not change this topology decision.

Cross-region fleets are explicitly supported as a *deployment* — i.e., shards and clusters can live in any region — but the coordinator itself is not. A regional outage in the coordinator's region halts cross-shard rebalancing and cross-shard preemption fleet-wide for the duration of the outage. The data plane (shards) continues to operate against its existing allocations.

## Consequences

- **Simpler implementation.** One Raft group, one well-trodden library, one set of operational runbooks.
- **Documented SPOF.** Operators running cross-region fleets must understand that the coordinator's region is the SPOF for fleet-wide rebalancing. The scaling guide will state this explicitly.
- **Static stability does the heavy lifting.** Any future regression that introduces a hot-path coordinator dependency from the data plane breaks the safety net that makes this decision defensible. `pkg/shard` must not import `pkg/coordinator`. CI should enforce this with an import-graph check.
- **Multi-region coordinator is post-v1.** When we revisit, candidates are: (a) a Raft group with witness replicas in remote regions, (b) a hierarchical scheme with regional coordinators, (c) accepting commercial-grade dependencies (Spanner, FoundationDB) for organisations that already have them. The decision is not pre-made; a future ADR will supersede this one.
- **Operational levers when the coordinator is down**: shards continue using their assigned allocations. Cross-shard preemption pauses. The operator should be alerted; runbook is "wait for coordinator recovery" rather than "fail over to another region", because we have not built that path.
