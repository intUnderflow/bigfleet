# ADR-0016: NodeStateUpdate carries node identity (labels, resources, taints)

**Status**: Accepted

**Date**: 2026-05-06

## Context

`NodeStateUpdate` is the shard → operator coalescing message that drives `UpcomingNode` CR phase transitions. Today it carries only:

- `machine_id`, `cluster_id`, `state`, `node_name`
- `provider_id`, `estimated_ready_unix_nanos`, `last_error`

It does **not** carry the shape of the node being brought online — its labels, allocatable resources, or taints. The operator's `handleNodeStateUpdate` therefore writes `UpcomingNode.Spec` with `Resources: corev1.ResourceList{}` and no `Labels` or `Taints`. Anyone reading the CR sees only a phase + name, not what the node will *be*.

This is a real production gap, surfaced during the M43d Pod-mode loopback work:

- The bigfleet-scaletest-pod-shim wants to fake a Kubernetes Node when an UpcomingNode reaches `Phase=Ready`. To create the Node it needs labels (so a pending Pod's nodeAffinity can match it) and resources (so a pending Pod's resource requests can fit). It can't get them.
- Real production has the same shape of dependency: any controller that wants to pre-allocate a Pod against an upcoming node (Karpenter-style consolidation, custom packers, the BigFleet plan §6 pre-binding optimisation) needs to know the node's labels + resources before kubelet joins.
- The `kubectl get upcomingnodes` UX is currently degraded: users see a phase but no shape, so they can't tell *which* upcoming node is for *their* workload.

The shard already has all three — labels and resources are part of the machine's `provider.Profile`, taints come from `Profile.Taints`. The only reason they don't reach the operator is that NodeStateUpdate doesn't have the fields.

## Decision

`NodeStateUpdate` gains three fields:

```proto
message NodeStateUpdate {
  ...
  map<string, string> labels = 9;
  Resources resources        = 10;
  repeated Taint taints      = 11;
}

message Taint {
  string key    = 1;
  string value  = 2;
  string effect = 3;
}
```

Semantics:

- **The shard populates them on every NodeStateUpdate**, derived from the machine's `Profile` at emit time. Empty values are valid for transitional states where the shard hasn't yet bound a host (SPECULATIVE, CREATING-pre-host) — the operator writes them through to UpcomingNode.Spec as-is.
- **The operator copies them into UpcomingNode.Spec** on every state transition. Existing fields (Resources, Labels, Taints) on the UpcomingNode CRD are populated for the first time.
- **Coalescing semantics are unchanged.** The supersedes_key still keys on `node:<machine_id>`; consumers may still drop older queued frames. A frame that arrives later overwrites the earlier one's labels/resources/taints — which is correct, since "later state of the same machine" is always the truth.

A new `Taint` message lives in `shard.proto` (rather than `provider.proto`) because the operator's UpcomingNode write path is the only consumer and `provider.proto`'s Taint shape (if it adds one) might evolve differently. The `Resources` message is reused from `provider.proto` since both shard and provider sides already share that vocabulary.

## Consequences

- **The operator's `handleNodeStateUpdate` populates `UpcomingNode.Spec.{Labels, Resources, Taints}`.** Existing UpcomingNode CRDs in flight at deploy time keep their empty Spec until the next state transition; this is harmless because UpcomingNodes are short-lived.
- **The shard's `emitNodeStateUpdate` reads from the machine's Profile** (instance-type, zone, capacity-type already exist as fields; resources + labels + taints are added). The shard already holds the Profile in inventory, so no new persistence is required.
- **`kubectl get upcomingnodes -o yaml` becomes informative.** Workload owners debugging "why is my pod still pending" can read the labels of upcoming nodes and see whether one of them will satisfy their nodeAffinity.
- **The bigfleet-scaletest-pod-shim's M43c upcomingNodeBinder reads `UpcomingNode.Spec.Labels` directly** instead of inferring labels from a pending Pod. Cleaner, deterministic across multiple Pods (the harness no longer needs the "single Pod per Node" assumption).
- **Real-provider deployments that want to pre-bind workloads to upcoming nodes have the data.** Karpenter-style consolidation, BigFleet plan §6's "bind-to-upcoming" optimisation — both unblocked. (We don't ship those optimisations as part of this ADR; we just stop the proto from blocking them.)
- **Backward compatibility.** Older operators reading a NodeStateUpdate with the new fields ignore them silently (proto3 unknown-field handling). Older shards omitting the new fields produce empty maps/lists on the operator side — same as the pre-ADR behaviour. Rolling-upgrade-safe.
- **Scaletest harness binary built against the new proto cannot dial an old shard binary** because the operator-side decoding is forward-compatible but the *shard's emitter* is what produces the data. In a coordinated rollout this is a non-issue; users running mixed versions for a window will see empty Spec.Labels just as today.

## Future work

- A future ADR may extend AvailableCapacityUpdate similarly to carry the same shape, so AvailableCapacity hints reflect what would actually land. Today AvailableCapacity already has a `NodeTemplate` that includes labels + resources, populated separately from NodeStateUpdate; the two paths could converge.
- Real production may want to extend this to carry node-system info (kubelet version, container-runtime version) for tooling that wants to plan around upgrade windows. Out of scope for this ADR.
