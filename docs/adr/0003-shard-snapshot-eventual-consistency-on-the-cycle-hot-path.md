# ADR-0003: Shard inventory snapshots are eventually consistent on the cycle hot path

**Status**: Superseded by M44.4 Drop A — the shard cycle switched to synchronous `Snapshot()`, making the background fold goroutine redundant; fold goroutine and live triple-indexes removed at M66.1.

**Date**: 2026-05-02

## Context

The shard's `runCycle` reads an `*inventory.Snapshot` once per cycle and feeds it to reconcile, Phase 1, Phase 2, and Phase 3. Pre-M11.19, every cycle synthesised a fresh snapshot under the inventory's read lock by walking `byID` and rebuilding the per-state and per-(state, instance-type) index slices. At 500K machines that walk dominated the per-cycle compute (`BenchmarkShardCycle_Steady` showed ≈700 ms of the cycle was the snapshot build, on M5 Max).

The snapshot is read-mostly. The cycle reads it; writes happen on the hot path through `Insert` / `Apply` / `Remove` (driven by reconcile, execute, and the post-RPC `applyTransition`). The build cost is O(N) regardless of how few machines actually changed since the previous snapshot.

Three candidate shapes were considered when this decision was made:

1. **Synchronous fold on every read.** The pre-M11.19 status quo: `Snapshot()` builds a fresh O(N) view on every call. Always fresh; always pays full cost.
2. **Lazy fold with threshold.** Track a "dirty count" of writes since the last fold; rebuild only when it crosses a threshold; otherwise return the cached snapshot. Bounded staleness; threshold has to be tuned.
3. **Background fold with debounce.** A goroutine watches a signal channel that writes ping; it folds on debounce intervals and publishes the result through `atomic.Pointer[Snapshot]`. The cycle reads the pointer in O(1). Bounded staleness by `foldDebounce + buildTime`; no tuning required beyond the debounce.

We chose Option 3.

The correctness question is whether eventual consistency on the cycle is safe. The safety net is `inv.Apply`'s state-machine validation: if the cycle decides on a stale snapshot and emits an action against a machine that has already moved, `applyTransition` rejects the re-attempt. Phase 1, 2, and 3 are all idempotent against an unchanged snapshot — re-deriving them on the next cycle costs nothing if the prior cycle's actions already landed. So the cycle tolerates *any* finite-bounded staleness, not just small staleness.

## Decision

The inventory exposes **two snapshot APIs with different freshness contracts**:

- **`Inventory.Snapshot()`** — synchronous, fresh, O(N). Builds a new snapshot under the read lock and returns it. Updates the cached pointer as a side effect. *Tests* and *any caller that needs strict consistency with the most recent write* use this.
- **`Inventory.CycleSnapshot()`** — atomic.Pointer load, O(1). Returns the most-recently-folded cached snapshot. May be stale by up to `foldDebounce + buildTime` (default `250ms + buildTime`). *The shard's cycle hot path is the only intended caller.*

A background goroutine is started in `inventory.New` (`foldLoop`). Writes through `Insert` / `Apply` / `Remove` send a non-blocking signal on a buffered `foldChan` after releasing the inventory lock. The fold goroutine drains the channel with a debounce window, builds a fresh snapshot under the read lock, and publishes it through `atomic.Pointer`.

`inventory.Stop()` shuts the goroutine down deterministically; the test setup that wants strict consistency in the same cycle as a write calls `Snapshot()` (synchronous) instead.

The shard's `runCycleCapturing` uses `CycleSnapshot()`. No other production caller currently uses it, and adding new callers should be a documented decision.

## Consequences

- **Per-cycle snapshot read is O(1).** The `~700 ms` snapshot build at 500K is no longer paid per cycle on the hot path.
- **The cycle is eventually consistent against inventory writes.** Acceptable because every action emitted by Phase 1/2/3 is idempotent and `applyTransition` rejects illegal re-attempts. The next cycle re-derives anything missed.
- **Two APIs, deliberate split.** Tests and code that reads-after-write keep `Snapshot()`. Mixing the APIs is a footgun (a test that writes then `CycleSnapshot()`s will see stale data and look broken); the split is documented and the test convention is to call `Snapshot()` once before any cycle invocation.
- **Foreground responsibilities are minimal.** Writers send a non-blocking signal; the goroutine does the work. No write path can be blocked by the fold.
- **`foldDebounce` is a knob.** 250 ms is the default; under sustained churn it bounds peak fold-CPU at 4 folds/sec at the cost of up to 250 ms staleness. If a future workload needs tighter freshness, lower the debounce; if fold CPU becomes a concern, raise it. The knob is not exposed via Config today — production callers don't tune it.
- **Restart behaviour is the obvious thing.** A fresh `Inventory` starts with an empty cached snapshot; the first `Snapshot()` call seeds the cache. Tests rely on this for warm-up.
- **The synchronous API is *not* deprecated.** It is the freshness-strict path and remains a first-class API. This ADR does not make it less canonical — it adds a second API for a different consumer.
