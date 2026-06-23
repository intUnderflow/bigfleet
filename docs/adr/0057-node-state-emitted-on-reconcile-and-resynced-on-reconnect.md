# ADR-0057: The shard emits NodeStateUpdate on reconcile-observed transitions and resyncs node state on operator (re)connect

## Status

**Accepted**, 2026-06-23 (author decision, Lucy Sweet). P0 fix.

## Context

The shard's only channel for telling an operator that a node exists — `notifyNodeState` (`pkg/shard/shard.go`) — had **exactly three call sites, all inside `applyTransition`** (the decision-worker / execute path). It was never called from the reconcile path or on session establish. An independent adversarial verification (four facets, each trying to *refute* the gap, all failing) plus a direct read confirmed this.

That is a P0 for **every async out-of-tree provider — i.e. the entire `providerkit` ecosystem (the blessed path)**:

- The provider contract is **asynchronous by design** (`pkg/provider/provider.go`): every mutating RPC returns a `TransitionAck` with the machine in the *transitional* state, and the terminal state is observed only via subsequent `Get`/`List`. So an async provider's `Configure` returns `Configuring`; the machine reaches terminal `Configured` **out-of-band, ingested via `applyReconciledMachine`** (`pkg/shard/reconcile.go`).
- `applyReconciledMachine` updated inventory via `s.inv.Apply`/`Insert` and **never called `notifyNodeState`**. So the operator never heard the terminal `Configured` → never wrote the `UpcomingNode`/Node → **the workload never scheduled onto the capacity the shard just provisioned.** The system's whole purpose failed silently.
- The in-process scaletest fake **masked it**: its `Configure` ack synchronously carries `State=Configured`, so the *worker's* `applyTransition` fires the notify. Every scale test and `sim` use that fake, so the async-provider → shard → operator → Node path had **never been integration-tested**. (Surfaced via the `bigfleet-demo` hand-off — the first thing to exercise the real async path.)
- The smoking gun: `notifyNodeState`'s own doc comment justified skipping a missing-session notify *"because the operator will reconcile from full state on reconnect."* **That reconnect resync was never implemented** — `Session` sent only a Hello-Ack, no node-state snapshot. So both the live async path *and* the assumed reconnect safety net were broken.

This is the operator-facing counterpart to [ADR-0056]: ADR-0056 governs provider→shard (*when* a machine is `Configured`); this governs shard→operator (*does the operator ever hear it*). Both are required for the real async provider path to work end to end.

## Decision

Make the shard tell the operator about a machine's state whenever the shard *learns* it, from either direction, and on (re)connect:

1. **Reconcile-side emit.** `applyReconciledMachine` now calls `notifyNodeState` on both slow paths — after `Apply` (state diverged; `prevCluster` captured before `Apply` so a binding-clearing terminal transition still routes to the owning cluster, exactly as `applyTransition` does) and after `Insert` (a machine first seen already bound). It is reached **only on a real state change** (the state-match fast path returns first), so it never floods, and the frame coalesces by `supersedes_key=node:<id>` regardless. This is the symmetric counterpart to the worker-path notify.

2. **Reconnect resync.** On every operator session establish, after the Hello-Ack, the shard replays the current state of every machine **bound to that cluster** (`resyncNodeState` → `Snapshot().ListByCluster`). This makes the doc comment's promise real and closes both the reconnect window and any live-miss. Bounded by the cluster's own population; coalescing dedups against concurrent live updates.

A new `inventory.Snapshot.ListByCluster(cluster)` returns a cluster's bound machines (O(K) in the cluster's population), used by the resync.

## Consequences

- **Async out-of-tree providers now work end to end** — the operator learns of `Configured` nodes reached via reconcile, and a (re)connecting operator catches up. This unblocks `providerkit`-based providers and the `bigfleet-demo` provider path.
- **Static stability preserved.** Both emits are shard→operator (data plane); they introduce **no `pkg/shard` → `pkg/coordinator` dependency**, and they follow `applyTransition`'s existing pattern (notify after the inventory mutation, no `s.mu` held — `notifyNodeState` takes only `sessionsMu` briefly + the session's `sendMu`).
- **Cost.** The resync builds a snapshot per session establish (already the per-cycle reconcile cost); a fleet-wide operator-reconnect storm after a shard restart pays one snapshot per reconnecting cluster. Acceptable for a rare event; if it ever bites, a live per-cluster index on `Inventory` (rather than a snapshot build) is the optimization — deferred (YAGNI).
- **Test-coverage gap that hid this remains at the harness layer.** The in-process fake is synchronous, so unit/scaletest can't exercise the async reconcile→operator path by default. Closed here with direct unit tests driving `applyReconciledMachine` and `resyncNodeState`; a fuller async-provider integration test (e.g. via the conformance/`providerkit` path) is worthwhile follow-up so this class can't regress.

[ADR-0056]: ./0056-node-join-readiness-gates-coverage-credit.md
