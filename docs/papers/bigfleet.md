# BigFleet: A Fleet-Level Infrastructure Autoscaler

A design for a fleet-level infrastructure autoscaler that manages 100 million nodes across 20,000 Kubernetes clusters, accepting pluggable capacity providers and treating all capacity — bare metal, cloud, reserved, spot — as a single fungible pool priced by cost.

Lucy Sweet — April 2026
Disclosure: Assisted by AI tools (Claude Opus 4.6 1M Context)

## 1. What BigFleet is

BigFleet is an implementation of the Infrastructure Autoscaler described in *Fleet-Scale Kubernetes*. It receives `ClusterCapacityNeeds` protobuf messages from per-cluster operators, diffs them against its own provisioned inventory, and provisions or reclaims nodes through pluggable `CapacityProvider` backends.

BigFleet is not a scheduler. It does not decide where pods run. It does not simulate kube-scheduler.

BigFleet is not a fleet manager. It does not deploy workloads, manage cluster lifecycle, or handle upgrades.

## 4. The capacity model

Two kinds of capacity, one interface:

- **Fixed capacity** — bare metal, reserved instances. Marginal cost per assignment is zero; the decision is allocation.
- **Elastic capacity** — cloud on-demand, spot. The decision is procurement.

`effective_cost = price + (interruption_probability × interruption_penalty)`

`price` is per-hour. `penalty` is dollars. The math works out because `interruption_probability × penalty` is dollars-per-hour expected interruption cost, comparable to price.

## 5. Shard architecture

Shards are horizontal slices across all infrastructure (homogeneous), not specialised. The shard count is bounded by the shallowest scarce resource pool (e.g., GPU rack count) — topology-constrained requests must remain satisfiable within a single shard.

### Machine model

Stable states:

| State | Host | Cluster | Meaning |
|-------|------|---------|---------|
| Speculative | nil | 0 | Quota slot |
| Idle | set | 0 | Real hardware, no cluster binding |
| Configured | set | set | Real hardware, joined to cluster |

Invariant: `host=nil, cluster≠0` is impossible.

Transitional states: Creating, Configuring, Draining, Deleting. Plus Failed (timeout).

Idle is the hub. Cross-cluster transfer always: `Configured → Drain → Idle → Bootstrap → Configured(new cluster)`.

All four lifecycle RPCs (Create, Configure, Drain, Delete) are async (return immediately) and idempotent (keyed by `(machine_id, target_state)`).

## 6. The two-tier hierarchy

**Tier 1: Global Coordinator** (3 replicas, Raft) — does NOT make provisioning decisions. Owns: shard membership, cluster→shard map, machine→shard assignments, quota allocations, provider registry. Latency budget: seconds.

**Tier 2: Shard Controllers** (~200 at 100M nodes) — hot path. Latency budget: <500µs per cluster, <50ms full evaluation per cycle.

## 7. CapacityProvider interface (gRPC)

```protobuf
service CapacityProvider {
  rpc Create(CreateRequest) returns (TransitionAck);
  rpc Configure(ConfigureRequest) returns (TransitionAck);
  rpc Drain(DrainRequest) returns (TransitionAck);
  rpc Delete(MachineRef) returns (TransitionAck);
  rpc Get(MachineRef) returns (Machine);
  rpc List(ListFilter) returns (MachineList);
}
```

Six methods. All transitions async, all idempotent. `List` returns machines in any state subset.

## 8. Decision engine (per shard, declarative)

**Phase 1**: walk needs top-down by priority. Prefer Idle (one bootstrap). Fall back to Speculative (Create + bootstrap). Within idle: tiebreak by reclamation_penalty. Within speculative: tiebreak by effective_cost.

**Phase 2**: resolve priority inversions via victim scoring:
```
score = priorityGap*wp + (1/drainTime)*ws + (1/interruptionPenalty)*wpen + (1/reclamationPenalty)*wrec
```
Drain grace scales with priority gap: gap >900K → 10s, >500K → 30s, >100K → 2m, else 10m.

**Phase 3**: reclaim excess. Reclaimed → Idle. Idle → Speculative lazily per provider (bare metal: forever; on-demand: minutes; spot: ~1m).

## 9. Inventory

Per-shard in-memory: ~30–55 bytes per machine. 500K machines = ~20MB (fits L3).

Global coordinator: ~240 bytes per shard summary. 200 shards = 48KB (fits L1).

Shortfall protocol: `Shortfall{Profile, Priority, Count, Age, InterruptionPenalty}`. Bounded to 100 per shard. Age >5 cycles → escalate.

> **Affected by [ADR-0027](../adr/0027-rollup-demand-is-a-constrained-resource-request.md) (2026-05-14):** under the resource-shaped demand model a Shortfall's unmet demand is a resource deficit vector, not a pod `Count`, and `Profile` is the constraint-set. The Phase 2 / Phase 3 rework ADR-0027 entails carries this change; the struct shape is finalised at implementation.

Coordinator response: (1) reassign idle from other shards, (2) reassign speculative quota, (3) cross-shard preemption (drain-first, expensive, rare).

## 11. Reliability

**Static stability**: clusters keep running when BigFleet is down. Running pods, kubelet, kube-scheduler all unaffected. Only new provisioning pauses.

**Coordinator failover**: 3 replicas, Raft. Election in <1s. Data plane (shards) operates autonomously during election — in-shard preemption continues.

**Fencing**:
- Coordinator → shard: `(coordinator_term, sequence_number)`.
- Shard → provider: `(shard_id, shard_epoch, sequence_number)`.

Recommended backing: **embedded Raft (etcd/raft or hashicorp/raft) + BoltDB**.

## 13. Operational concerns

**Node join**: BigFleet calls `ClusterOperator.GenerateBootstrap(cluster, requirements) → BootstrapBlob`. Push-based, on-demand. Cluster operators generate kubelet config, join token, CA cert. BigFleet treats blob as opaque bytes.

**Scale-down**: BigFleet does NOT watch pod events. CR garbage-collected via ownerRef → next roll-up has fewer needs → Phase 3 reclaims.

## 16. Design decisions

- Shards own capacity. No distributed locking on hot path.
- Topology constraints do not cross shard boundaries.
- Two penalties: interruption_penalty (cost-of-interruption, in `effective_cost` and victim scoring), reclamation_penalty (machine-tied operational value, in idle tiebreak / victim scoring / Phase 3 release).
- BigFleet does not manage cloud commitments.
- No quota, no admission, no entitlement. Priority is the sole throttling mechanism.
- Clusters are permanently assigned to shards on first contact. No cluster-lifecycle API.

(See source paper for full content; this file is a working summary for design reference.)
