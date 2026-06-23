# ADR-0059: Async-provider drain finalizes via reconcile — the post-Drain clear lands on terminal Idle, not the transitional Draining ack

## Status

**Accepted**, 2026-06-23 (author decision, Lucy Sweet). P0 fix.

## Context

Against a real async (`providerkit`) provider on `--provider-addr`, **every Phase-3 Reclaim and Phase-2 Preempt drain failed**, and cloud capacity could never be released — after demand dropped, the fleet stayed stuck holding nodes (`action_execute_outcomes_total{kind="Reclaim",outcome="transition_error"}` climbing, machines stuck `Draining`, `Idle`/`Speculative` never refilling). The shard log repeated `drain: post-Drain transition: ... Draining state must have a cluster`.

This is the **third async-provider gap** the `bigfleet-demo` real out-of-tree provider exposed (after [ADR-0057]'s reconcile→operator notify and [ADR-0058]'s concurrency↔fencing), and the same root blind spot each time: the in-process scaletest fake is **synchronous**, so the real `providerkit` path is the first to hit it.

The mechanism (`pkg/shard/execute.go` `executeDrain`):

1. `applyTransition(StateDraining, nil)` — `Configured → Draining`, cluster preserved (valid: `Draining` must have a cluster, `pkg/machine/machine.go`).
2. `Provider.Drain(...)` — the async kit returns the `TransitionAck` in its **transitional** state (`Draining`) immediately and runs teardown out-of-band; `ack.Machine.State == Draining`, not terminal `Idle`.
3. `applyTransition(drained.State, func(m){ m.Cluster=""; m.Assigned*=0 })` — with `drained.State == Draining`, this set **`Draining`-without-a-cluster**, tripping the invariant → the transition errored → the drain never completed; the machine was left `Draining`.

The terminal binding-clear is correct only for the **terminal `Idle`** state. The synchronous fake masked it: its `Drain` ack carries `State=Idle`, so step 3 ran `applyTransition(Idle, clear)`, which is valid (`Idle` needs no cluster). `executeDelete` had **already** got this right — it gates its clear on the terminal `Speculative` state — so `executeDrain` was the lone unported site. Both Reclaim and Preempt route through `executeDrain`, so both failed.

A second, quieter half: `applyReconciledMachine` (`pkg/shard/reconcile.go`) **unconditionally preserves `Assigned*`** from the existing record across a state change (the [ADR-0057] / restart-rebuild path — the provider view carries assignment only as the `shard_metadata` echo, which reconcile nils). So a drain reaching `Idle` *via reconcile* would keep stale priority/penalty on the now-unbound machine.

## Decision

Make the drain finalize on the **terminal** state, exactly as `executeDelete` and the [ADR-0057] async-configure path do:

1. **`executeDrain` clears only on `Idle`.** Gate the binding-clear on `drained.State == machine.StateIdle`. A synchronous provider lands directly at `Idle` and clears as before. An async provider returns a `Draining` ack — the machine is already `Draining`-with-cluster (step 1), so we leave it and let the terminal `Idle`, observed via `applyReconciledMachine`, finalize the drain (the same reconcile path [ADR-0057] taught to emit the node-state update, so the operator learns the node drained).

2. **Reconcile clears assignment on a transition to an unbound state.** In `applyReconciledMachine`, preserve `Assigned*` only while the machine stays bound (`Configured`/`Configuring`/`Draining`); on a transition to `Idle`/`Speculative` (an async drain or delete completion) leave the incoming zero values, so the binding and its assignment clear together. Bound→bound transitions (the async-configure `Configuring → Configured`) still preserve, unchanged.

The fake gains a `DrainStaged` option (the teardown twin of `ConfigureStaged`/`CreateStaged`): `Drain` returns `Draining` and the binding clears only at the `CompleteStaged`-driven terminal `Idle`. This lets the in-process fake model the async-drain path the synchronous default masked, so the class is now unit-testable in-process.

## Consequences

- **Phase 2/3 can release capacity against async out-of-tree providers.** Reclaim and Preempt drains complete (`Configured → Draining → [reconcile] → Idle`); `Idle`/`Speculative` refill; the autoscaler can shrink, not just grow.
- **Symmetric with the async lifecycle.** Drain now mirrors async Configure (`Idle → Configuring → [reconcile] → Configured`) and Delete (terminal-gated clear). The worker dispatches; reconcile finalizes the out-of-band terminal transition and notifies the operator.
- **No stale assignment on drained slots.** A drained `Idle` machine carries zeroed `Assigned*`, so the per-penalty-bucket inventory metric and any `Assigned*`-keyed logic see it as the unbound slot it is.
- **Static stability preserved.** Both changes are shard-local (execute path + reconcile ingest); no `pkg/shard` → `pkg/coordinator` dependency. The synchronous path is byte-identical (instant `Drain` → `Idle` → clears, exactly as before), so every existing scaletest/sim is unaffected.
- **Test-coverage gap closed at the harness layer.** `DrainStaged` + the new `executeDrain` async integration test and the reconcile-finalize unit tests exercise the async-drain path the fake previously couldn't. Combined with [ADR-0057]/[ADR-0058], this is the third instance of the same lesson: a real async provider in CI would have caught all three; that integration test remains the worthwhile standing follow-up.

[ADR-0057]: ./0057-node-state-emitted-on-reconcile-and-resynced-on-reconnect.md
[ADR-0058]: ./0058-fence-high-water-mark-per-shard-machine.md
