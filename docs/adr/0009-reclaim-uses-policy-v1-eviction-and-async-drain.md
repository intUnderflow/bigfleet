# ADR-0009: ReclaimInstruction uses policy/v1 Eviction and acks before drain completes

**Status**: Accepted

**Date**: 2026-05-05

## Context

Pre-M20, `pkg/operator/reclaim.go` was a "log + ack" stub from M4: receive ReclaimInstruction, log it, ack immediately, do nothing. The user-stories pre-M14 already described the operator as "respecting PodDisruptionBudgets when handling ReclaimInstruction" — false until M20.

M20 had to make two design choices that shape the operator's contract:

1. **Eviction API.** Direct pod-delete bypasses PDBs. The standard PDB-respecting path is the `policy/v1` Eviction subresource (GA in 1.22). Using it ties BigFleet to that minimum Kubernetes version (no problem — see ADR-0010) and means the operator has to handle 429-too-many-requests as a transient retryable signal, not a hard failure.

2. **Ack timing.** Two viable contracts:
   - **Ack on cordon.** The operator cordons the node, sends ReclaimAck, and drains async in the background. The shard's reclamation accounting is "started", not "finished".
   - **Ack on drain completion.** The operator blocks the recv-loop on drain. Better correctness, worse session ergonomics — drain can take minutes per pod and the recv-loop must not stall on it.

The static-stability contract makes ack-on-cordon more honest: cordon is the post-condition the shard cares about (no new pods will land here), drain is the workload's eviction journey. A 5-min PDB-blocked eviction shouldn't hold the shard's session loop hostage.

## Decision

The operator's ReclaimInstruction handler:

1. Cordons each named node synchronously via JSON merge-patch on `Spec.Unschedulable=true`.
2. Patches the matching UpcomingNode CR to `phase=Draining` (M14's enum addition; located by `Status.NodeRef.Name`).
3. Sends the ReclaimAck — at this point the shard sees the reclaim as "started".
4. Spawns a background goroutine that:
   - Lists pods on each cordoned node.
   - Skips DaemonSet-owned pods (pinned by design; not evictable in any normal sense).
   - Posts a `policy/v1` Eviction for each remaining pod via `client.SubResource("eviction").Create`.
   - Loops with 2-second backoff on 429 (PDB-blocked); the `grace_period_seconds` from the instruction bounds the total drain time via `context.WithDeadline`.
   - On per-node completion: patches UpcomingNode to `phase=Drained`.
   - On per-node grace timeout: patches `phase=Failed` with the underlying error in `Status.LastError`.

The recv-loop returns immediately after step 3. Drain runs to completion (or grace timeout) on its own clock.

## Consequences

- **PDBs are respected at the API layer, not the application layer.** No re-implementation of the eviction contract; the apiserver enforces it.
- **The shard's reclamation contract is "started" semantics.** `ReclaimAck` says "I have accepted the instruction and the node is no longer scheduling new work." Subsequent NodeStateUpdate frames (already wired) tell the shard when the machine returns to Idle. Shard-side accounting of "drain still in progress" relies on observing the next state transition, not a deferred ack.
- **Grace timeout produces a Failed UpcomingNode.** Per the protocol, `Failed` machines need operator intervention; v1 doesn't auto-uncordon or auto-retry. The runbook is to investigate why drain stalled (PDB too strict, finalizer hung, etc.) and either bump the PDB or force-delete the offending pod.
- **K8s version floor.** Eviction `policy/v1` is GA in 1.22; combined with the rest of BigFleet's stack the floor is 1.31 (ADR-0010). This decision doesn't drive the floor on its own.
- **No in-process drain timeout.** The drain goroutine's only deadline is `grace_period_seconds`. If the operator pod itself dies before drain completes, the next operator startup observes the cordon-but-not-drained state via the cluster's own NodeStateUpdate flow; the goroutine doesn't survive a pod restart, but the cordon does, and the shard re-evaluates next cycle.
