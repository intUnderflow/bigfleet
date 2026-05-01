# BigFleet architecture

This is the synthesis. The full design is in the [BigFleet paper](https://lucy.sh/bigfleet) (vendored at [`papers/bigfleet.md`](papers/bigfleet.md)); read this first if you want a tour, the paper if you want the rationale.

## The shape

BigFleet is two tiers and three component types:

```
┌─────────────────────────────────────────────────────────────┐
│  COORDINATOR (Tier 1)                                       │
│  - Raft-replicated (3 replicas, single region)              │
│  - Owns: shard membership, cluster→shard map,               │
│    topology-domain→shard assignments, quotas, providers     │
│  - Does not make provisioning decisions                     │
└──────────────────────────┬──────────────────────────────────┘
                           │ unary RPCs (rebalance instructions
                           │ ride on shard-pulled ReportShard)
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  SHARD       │   │  SHARD       │   │  SHARD       │
│  (Tier 2)    │   │              │   │              │   ...
│  - Hot path  │   │              │   │              │
│  - Inventory │   │              │   │              │
│  - Decision  │   │              │   │              │
│    engine    │   │              │   │              │
└──┬─────┬─────┘   └──┬─────┬─────┘   └──┬─────┬─────┘
   │     │            │     │            │     │
   │     ▼            │     ▼            │     ▼
   │  PROVIDER        │  PROVIDER        │  PROVIDER
   │  (out-of-tree)   │  (out-of-tree)   │  (out-of-tree)
   │                  │                  │
   ▼                  ▼                  ▼
OPERATOR           OPERATOR           OPERATOR
(per cluster)      (per cluster)      (per cluster)
   │                  │                  │
   ▼                  ▼                  ▼
[ Kubernetes ]     [ Kubernetes ]     [ Kubernetes ]
```

### Coordinator (Tier 1)

Three replicas, hashicorp/raft over BoltDB, single region. Holds the slow-changing fleet state every shard needs but no shard owns:

- **Shard membership** — which shards exist, with what addresses.
- **Cluster→shard binding** — clusters are bound to a shard on first contact and never move (no cluster-lifecycle API).
- **Topology-domain→shard assignment** — at scale, machine IDs aren't tracked here; topology domains (rack, zone, AZ) are. ~100K entries instead of 100M.
- **Quotas** — cluster-level entitlement counters, allocated to shards on demand.
- **Provider registry** — addresses of registered `CapacityProvider` services.

The coordinator is **not** in the hot path. Shards run autonomously; the coordinator only intervenes for cross-shard rebalance and quota allocation.

See [ADR-0002](adr/0002-coordinator-topology-single-region.md) for why single-region.

### Shard (Tier 2)

The shard is BigFleet's heart. One process, one decision engine, one inventory snapshot. Per-shard scale ceiling is ~500K machines and ~5K cluster sessions; deployments grow by adding shards, not by growing one shard.

Each shard holds:

- **Inventory** — every machine the shard owns, in one of seven states (see `pkg/machine`). In-memory, refilled from the provider's `List` on startup and reconciled every cycle.
- **NeedsTable** — every cluster's last-known full demand, priority-sorted. Each rollup is a full replacement; never partial.
- **Sessions** — one bidi gRPC stream per managed cluster. Operator-initiated; the shard never opens an outbound connection to a cluster.
- **Decision engine** — three-phase synchronous loop. Runs every cycle (default 1 s).

The hot path **never** depends on the coordinator. This is enforced by `pkg/shard/no_coordinator_dep_test.go`, which parses `pkg/shard`'s import graph and fails CI if `pkg/coordinator` shows up.

### Operator

One per managed Kubernetes cluster. Outbound-only — no inbound listener, no public API surface. Dials the shard, holds one bidi `Shard.Session` stream, and multiplexes:

- **Outbound**: `ClusterCapacityNeeds` rollups (every 10 s, full replacement), `BootstrapBlobResponse` to shard pulls, `ReclaimAck` to drain instructions.
- **Inbound**: `BootstrapRequest`, `ReclaimInstruction`, `NodeStateUpdate`, `AvailableCapacityUpdate`.

The operator reads `CapacityRequest` CRs from the cluster, aggregates them into a NeedsTable, and emits the rollup. It also watches `Pod` (optionally; via the separate `bigfleet-unschedulable-pod-controller`) and writes `AvailableCapacity` / `UpcomingNode` CRs back so users can `kubectl describe` them.

### Provider

One process per backend (AWS, GCP, bare-metal, MAAS, …). Lives in a **separate repo**. Implements six gRPC RPCs: `Create`, `Configure`, `Drain`, `Delete`, `Get`, `List`. No `Watch` — reconciliation is `List + Get`.

Why out-of-tree: Kubernetes spent years undoing in-tree CCM and CSI providers; we don't repeat that. The repo ships only the proto contract, a conformance suite, and a test-fixture fake.

## The decision engine: Phase 1 / 2 / 3

Every shard cycle (default 1 s) runs three phases sequentially. Order matters and is fixed:

### Phase 1 — Assign

Walk the NeedsTable in priority order (highest first). For each `Need(cluster, profile, count)`:

1. Find idle inventory matching the profile. Pick the cheapest by **effective cost**.
2. If insufficient idle, ask the provider to provision more (Speculative → Creating → Idle, then Idle → Configuring → Configured).
3. If still insufficient, record a shortfall.

Effective cost is fixed:

```
effective_cost = price + (interruption_probability × interruption_penalty)
```

`price` is per-hour. `interruption_probability` is provider-declared only — no cluster-side override. `interruption_penalty` is the cluster's stated cost of having this workload interrupted, in dollars.

### Phase 2 — Preempt inversions

A "priority inversion" is a configured-but-low-priority machine occupying inventory that a higher-priority shortfall could use. Phase 2 walks shortfalls and looks for victims.

A victim's score is:

```
victim_score = victim's interruption_penalty + reclamation_penalty
              + drain_grace_remaining × per-hour-cost
```

Lowest score wins. Two penalties, deliberately distinct:

- **`interruption_penalty`** — cost of interrupting the workload. Used in `effective_cost` and victim scoring.
- **`reclamation_penalty`** — operational value tied to the specific machine (long-running stateful work, in-flight training, etc.). Used in idle tiebreak, victim scoring, and Phase 3 release.

### Phase 3 — Reclaim excess

Walk configured machines whose cluster no longer needs them. Drain to Idle. The release order is governed by `reclamation_penalty` (cheapest first).

### Why phases, not a single optimiser

A single LP-style optimiser could in principle replace all three phases. We don't, for two reasons in the paper: (a) deterministic phase ordering makes the engine debuggable; (b) victim selection's "drain grace" interaction with effective-cost arithmetic doesn't fit a single objective without knobs. See the [BigFleet paper](https://lucy.sh/bigfleet) §8 (or [`papers/bigfleet.md`](papers/bigfleet.md)).

## Static stability

The load-bearing safety property: **clusters keep running with BigFleet entirely down**.

Implications:

- Configured machines stay configured if BigFleet is unreachable.
- Phase 3 reclaims only run when a cluster's NeedsTable actually drops below the configured count.
- The shard's hot path has no dependency on the coordinator. (Programmatic guard: `pkg/shard/no_coordinator_dep_test.go`.)
- The data plane (shards) operates autonomously during coordinator failover.
- Operators reconnect with exponential backoff; they do not panic or alter cluster state during a disconnect.

This rules out a class of designs that would otherwise be tempting (e.g., putting cluster→shard mapping resolution on the per-rollup hot path, or making Phase 3 depend on quota recheck).

## Wire formats and protocol invariants

- **Roll-ups are full replacement.** Every `ClusterCapacityNeeds` is the cluster's complete desired state. Never partial.
- **`Same` topology operator is protobuf-only.** CRDs use `In`, `NotIn`, `Exists`, `DoesNotExist`. The operator translates co-location signals to `Same` during rollup.
- **Topology constraints do not cross shard boundaries.** A `Same`-rack request that can't be satisfied within a shard becomes a shortfall, never resolved cross-shard.
- **Stream coalescing uses `supersedes_key`** — explicit field on coalescing message types so reconnect ordering is safe.
- **Provider `List` is reconcilable, optionally incremental.** `since_revision` is opaque bytes; conformance-gated above a documented threshold.
- **Penalty buckets are powers of 2** ($0.50 to $8.4M) so cross-cluster aggregation has stable boundaries.

## Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| Shard crash | Cluster operators reconnect; in-flight transitions resume on `List` reconcile | Auto |
| Coordinator quorum loss | No cross-shard rebalance, no new quota allocations; existing decisions continue | Restore quorum |
| Provider unreachable | Affected machines mark Failed with `last_error`; remaining demand satisfied from rest of inventory | Provider returns; manual cleanup of Failed machines |
| Operator → shard partition | Cluster runs on last-known configured machines; rollups queue locally | Reconnect; backoff |
| Region-wide BigFleet down | All clusters keep running on existing inventory | Restore BigFleet |

The two M10 fault-injection scenarios under [`../sim/scenario/provider_failure.go`](../sim/scenario/provider_failure.go) exercise the third row.

## What BigFleet deliberately is not

- Not a scheduler. Does not place pods. Does not simulate kube-scheduler.
- Not a cluster-lifecycle manager. Clusters are bound to shards on first contact; there is no register/deregister API.
- Not a quota / admission / entitlement engine. Priority is the sole throttling mechanism.
- Not a cloud-cost optimiser. The cost formula is fixed and not pluggable.
- Not in-tree for providers. See [`provider-author-guide.md`](provider-author-guide.md).
