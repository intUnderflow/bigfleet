# ADR-0005: The provider boundary is the validation point; reconcile trusts domain types

**Status**: Accepted

**Date**: 2026-05-02

## Context

The shard's `provider.Provider` interface returns domain types (`pkg/machine.Machine`), not proto messages. Two implementations live behind the interface today: the in-tree `pkg/provider/fake` (constructs `machine.Machine` values directly in-process) and `pkg/provider/grpcadapter` (wraps a real gRPC `CapacityProvider` client and converts proto → domain via `pkg/conv` on every call).

Pre-M11.24a, `pkg/shard/reconcile.go` re-routed every machine returned by `provider.List` through `conv.MachineToProto + conv.MachineFromProto` before applying it to inventory. The comment justifying this read:

> MachineFromProto round-trips through the proto-shaped Machine
> from the provider; here the provider is already returning
> domain types, so the conversion is a no-op clone (we still
> route through MachineToProto + MachineFromProto to get the
> validation paths exercised on every reconcile).

In other words: belt-and-braces validation. The comment was honest about it being redundant.

The cost: at 500K inventory and a post-execute cycle pulling 1000 deltas, that's 1000 × {proto allocation, label-map clone, resource-map clone, MachineFromProto allocation, second label/resource clone, validation walk}. Per-machine wall-clock is small but per-cycle aggregate showed up as ~70 ms even after the M11.22 incremental reconcile reduced the working set to deltas.

The validation re-run was load-bearing for two scenarios that no longer apply:

1. **A future in-tree provider that constructed invalid `machine.Machine` values directly.** Defensive. We have no such provider and the `provider.Provider` interface contract is "return valid domain types." Adding a misbehaving in-tree provider is a code-review concern, not a runtime concern.
2. **Catching `grpcadapter` regressions where the proto→domain conversion produced an invalid machine.** Defensive. The grpcadapter validates exactly once at the conversion boundary; that's the right place. Re-validating in reconcile catches the same bugs the unit tests for `pkg/conv` already catch.

## Decision

**The provider boundary is the validation point.** `reconcile` trusts that `provider.Provider.List` returns valid `machine.Machine` values and applies them directly to inventory without round-tripping through proto.

Validation responsibilities are explicit:

- **`pkg/provider/fake`** validates by construction (the in-process fake creates `machine.Machine` values from typed inputs; invalid combinations are caught at write time, not at read time).
- **`pkg/provider/grpcadapter`** validates at the proto→domain conversion via `pkg/conv` (`MachineFromProto` performs state validation and shape checks). This conversion happens once per RPC response, not once per reconciled machine on the shard side.
- **`pkg/shard/reconcile`** assumes the input is valid and applies it to the inventory. State-machine correctness on the apply path is enforced by `inventory.Apply` (which validates state transitions through `machine.CanTransition`); that's the safety net for "bad data slipped through anyway."

The early state-match short-circuit in `applyReconciledMachine` (M11.24a) is the other half of this change: when the local inventory already has the machine in the same state as the provider returned, do nothing. This is the common case after `executeBootstrap` / `executeProvision` / `executeDrain` apply the post-RPC `ack.Machine` state to inventory locally — the next reconcile sees state-match and returns immediately, paying only an `inv.Get` per delta machine.

## Consequences

- **Reconcile per-delta cost dropped from "two map clones + struct copies" to "one `inv.Get` + state compare".** Phase-dump on M5 Max showed the total cycle mean drop 153 ms → 116 ms (−24 %) at 500K, dominated by the round-trip elimination.
- **Single source of truth for machine validation.** New providers register their validation at the boundary they own. The shard does not double-check.
- **Adding an in-tree provider that returns malformed machines is now a real bug, not a no-op.** The defence-in-depth was implicit; removing it makes the interface contract explicit. New in-tree providers (we expect zero — real providers live in separate repos) must validate inputs themselves.
- **`pkg/conv` is unchanged and still used in the execute path.** Reconcile is the only caller that dropped the validation round-trip; execute (`pkg/shard/execute.go`) still routes through `conv.MachineFromProto(conv.MachineToProto(ack.Machine))` to materialise the post-RPC machine snapshot. That call site is rare (one per executed action, not one per inventory entry) and the per-RPC clone is a useful boundary for the assigned-* field merge that follows. Future cleanup may consolidate that as well; this ADR does not pre-empt it.
- **Conformance suite implication.** The `provider.Provider` Go-interface implementations that satisfy `test/conformance/` must validate at the boundary; the conformance suite should grow a positive test that an out-of-tree provider's invalid output is rejected at the gRPC adapter, not silently propagated.
