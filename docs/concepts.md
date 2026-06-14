# BigFleet concepts and glossary

The vocabulary you'll see in code, protos, CRDs, and dashboards.

## Capacity model

### Need

One row of demand from one cluster: `(ClusterID, Profile, Count)`. The atomic unit of the wire format. Created by the operator from the cluster's CRs and emitted in the rollup. `pkg/needs.Need`.

### Profile

The shape of a machine the cluster wants. Carries:

- **Requirements**: label selectors (`In`, `NotIn`, `Exists`, `DoesNotExist`, plus protobuf-only `Same`).
- **Resources**: extended-resource quantities (`nvidia.com/gpu`, `cpu`, `memory`, …).
- **TopologySpread**: anti-affinity / spread constraints across topology domains.
- **Priority**: int32. Higher wins in Phase 1 and Phase 2.
- **InterruptionPenaltyBucket**: bucketed dollars (see Penalty bucket).
- **ReclamationPenaltyBucket**: bucketed dollars (see Penalty bucket).

Profiles are content-addressed by `Fingerprint()` — identical profiles share storage and aggregate cleanly. `pkg/needs.Profile`.

### NeedsTable

The shard's full picture of every cluster's demand. One entry per `(cluster, profile fingerprint)`. Each rollup is a `Replace(cluster, []Need)` — complete replacement, never partial. `pkg/needs.NeedsTable`.

### Penalty bucket

Penalties (interruption and reclamation) are quantised to powers of 2 from $0.50 to $8,388,608, plus a `Pinned` sentinel. The bucketing makes cross-cluster aggregation stable: two clusters at $1.05 and $1.10 don't ratchet aggregation differently when one of them rounds up to $2 and the other doesn't. `pkg/needs.PenaltyBucket`.

### Shortfall

Unsatisfied demand. When Phase 1 cannot satisfy a Need (no idle inventory, provider out of capacity, topology unsatisfiable within a shard), the residual count becomes a shortfall. Shortfalls age and escalate; they're surfaced as a metric (`bigfleet_shard_shortfalls`) and as `CapacityRequest.status.phase=Shortfall`. The buffer and aging live in `pkg/shard`; the deficit that records a shortfall is derived in `pkg/decision` (Phase 1/2).

## Cost and victim selection

### Effective cost

Fixed formula, not pluggable, not configurable:

```
effective_cost = price + (interruption_probability × interruption_penalty)
```

Used in Phase 1 to pick the cheapest matching machine. `price` is per-hour; `interruption_probability` is provider-declared (no cluster-side override). `pkg/decision`.

### Interruption penalty

Dollars representing what the *cluster* says it costs to interrupt this workload. Used in `effective_cost` (so cheap-but-flaky spot vs. expensive-but-stable on-demand reflects the workload's tolerance) and in victim scoring.

### Reclamation penalty

Dollars representing the *operational* value tied to a specific machine — long-running training state, accumulated cache, in-flight stateful work. Distinct from interruption penalty: a machine with high `reclamation_penalty` is preferred to be left alone in idle tiebreaks and Phase 3 release ordering, even if its workload's `interruption_penalty` is low.

### Victim score

In Phase 2, the candidate to preempt is the configured-machine-of-lower-priority-than-shortfall with the lowest score:

```
victim_score = interruption_penalty + reclamation_penalty
              + drain_grace_remaining × per-hour-cost
```

`drain_grace_remaining × per-hour-cost` makes mid-drain machines harder to re-preempt — already paying the drain time has value.

## Machine state machine

`pkg/machine` defines eight states (three stable, four transitional, one terminal):

| State | Meaning | Stable? |
|---|---|---|
| `Speculative` | Quota slot, not yet provisioned | Yes |
| `Creating` | Provider working on Create | No (transitional) |
| `Idle` | Provisioned, unassigned to a cluster | Yes |
| `Configuring` | Provider working on Configure (joining a cluster) | No (transitional) |
| `Configured` | Joined a cluster, serving workloads | Yes |
| `Draining` | Provider working on Drain (leaving cluster) | No (transitional) |
| `Deleting` | Provider working on Delete (returning to Speculative or gone) | No (transitional) |
| `Failed` | Terminal — a transition failed; `last_error` is populated | Yes |

Transitions are validated; an invalid transition is a hard error. The fake provider's `InstantTransitions` mode collapses transitionals for tests, but the inventory still walks every state.

## Components

### Cluster

A Kubernetes cluster managed by BigFleet. Identified by `ClusterID` — a stable string, supplied to the operator on startup. **Permanently bound to a shard on first contact.** No registration / deregistration API.

### Operator

The per-cluster agent. Lives inside (or alongside) a Kubernetes cluster. Outbound-only — never opens an inbound listener. Holds one `Shard.Session` bidi gRPC stream. `cmd/operator`, `pkg/operator`.

### Shard

The decision engine. One process, one Raft of `Need`s, one inventory. Per-shard ceiling ~500K machines, ~5K cluster sessions. `cmd/bigfleet shard`, `pkg/shard`.

### Coordinator

The fleet-state replicator. Three replicas, hashicorp/raft, single region. Out of the hot path. `cmd/bigfleet coordinator`, `pkg/coordinator`.

### Provider

A `CapacityProvider` implementation — the thing that actually creates / configures / drains / deletes machines. Out-of-tree. The repo ships a test-fixture fake at `pkg/provider/fake` (never deployed). `provider.proto`, `pkg/provider`.

### CapacityRequest (CR)

A Kubernetes CR, created by users (or the optional `bigfleet-unschedulable-pod-controller`), describing a workload's capacity needs. The operator aggregates CRs into the rollup. CRD: `bigfleet.lucy.sh/v1alpha1.CapacityRequest`.

### AvailableCapacity

A read-back CR the operator writes to inform users which Profiles BigFleet currently has idle inventory for. CRD: `bigfleet.lucy.sh/v1alpha1.AvailableCapacity`.

### UpcomingNode

A read-back CR the operator writes when BigFleet is about to bring a node into the cluster. Lets `kubectl describe pod` show "BigFleet is provisioning a node". CRD: `bigfleet.lucy.sh/v1alpha1.UpcomingNode`.

### Topology domain

A unit of co-location (rack, zone, AZ, …). Identified by a `(key, value)` pair. The coordinator assigns topology domains to shards, not individual machines (~100K entries instead of 100M).

### Bootstrap blob

The cluster-specific data needed to join a node to a Kubernetes cluster — kubelet config, CA bundle, join token, cloud-init / userdata. The shard pulls it from the operator on demand and forwards it to the provider's `Configure` RPC. The operator's `BootstrapTemplate` generates it.

## Protocol invariants

### Full replacement

Every rollup is the cluster's complete desired state. The operator never sends a delta. This makes reconnect logic trivial and the protocol stateless above the stream layer.

### Supersedes key

Coalescing message types on the bidi stream carry an explicit `supersedes_key` so the receiver can drop superseded messages on reconnect without ordering subtleties.

### `Same` is protobuf-only

The CRD vocabulary is the standard `In / NotIn / Exists / DoesNotExist`. The operator translates co-location signals from CRs into `Same` during rollup. CR authors never write `Same` directly.

### Reconciliation, not Watch

Provider does not stream events. Shards reconcile via `List + Get`. Optional `since_revision` cursor on `List` makes it incremental for large fleets.

### Six provider RPCs

`Create`, `Configure`, `Drain`, `Delete`, `Get`, `List`. That's the entire surface a provider implements.

## What's not in the model

- **No `Watch` on `CapacityProvider`** — reconciliation only.
- **No `operational_value` field** — use `reclamation_penalty`.
- **No cluster-supplied interruption probability** — provider-declared only.
- **No pluggable cost function** — the formula is fixed.
- **No cross-shard topology resolution** — within-shard or shortfall.
- **No quota / admission API** — priority is the sole throttle.
- **No cluster-lifecycle / registration RPCs** — clusters bind on first contact.
