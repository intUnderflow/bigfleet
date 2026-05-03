# ADR-0004: Incremental reconcile via `since_revision` — opt-in, deltas only, removal-via-tombstone deferred

**Status**: Accepted

**Date**: 2026-05-02

## Context

The shard's `reconcile` pulls the provider's view of every machine and updates the in-memory inventory to match. Pre-M11.22, `reconcile` always issued an unfiltered `provider.List()` and processed the full result set every cycle. At 500K machines this dominated the cycle: per-phase histograms (M11.21) showed reconcile at **87 % of the per-cycle wall-clock** on M5 Max (~696 ms mean, ~500 ms p99 on Scaleway PRO2-M).

The plan already calls out the fix: `docs/plan.md` §10.6 prescribes cursor-based incremental list. The wire field exists — `ListFilter.SinceRevision` and `MachineList.revision` are on the proto. What didn't exist was:

1. A provider implementation that *honoured* the cursor (the in-tree fake ignored it before M11.22).
2. A path on the shard that *used* it.
3. A semantics for **removed machines** in the delta path. The proto has no tombstone field; a delta response only contains machines that *exist*. Without a separate signal, a shard that switches to delta mode has no way to learn that a previously-known machine was removed.

The §0.1 framing ("conformance threshold") sets the boundary: providers above the threshold support `since_revision`; providers below the threshold may ignore it. The shard cannot assume the provider honours the cursor without configuration.

Three candidate shapes for the shard side were considered:

1. **Auto-detect.** Compare the response's `Revision` to the cursor passed in; if they're equal, treat the response as "no changes" and skip removal walks. Doesn't tell us whether the provider *honoured* `since_revision` or returned full state with a stale revision; misclassifying produces stale removal-state.
2. **Always full-list, always walk.** Status quo. Always correct, always slow.
3. **Opt-in flag.** A `Config.IncrementalReconcile` boolean. Off = full-list. On = delta-only and trust the provider. Simple, explicit, gives operators a single point of control.

Tombstones in the delta path are the harder design question. Three candidates:

A. **Add a tombstone field to `MachineList`** — `repeated string deleted_ids = 3` or similar. Clean wire shape; requires proto extension and provider-side bookkeeping (tombstones must outlive the cursor that "covers" them).
B. **Sentinel-state machines.** Encode tombstones as `Machine{id, state=UNSPECIFIED}` rows in the delta response. No proto extension; reuses an existing sentinel.
C. **Periodic full re-sync.** Run incremental for N cycles, then a full list to GC anything the deltas missed. Simple operationally; bounded staleness but unbounded inventory drift between full lists.

The fake provider — the only in-tree provider — never *removes* machines. `Delete` walks Idle → Speculative; the machine stays in the provider's `machines` map. So we have **no driving requirement for tombstones today**, and pre-designing one risks over-engineering.

## Decision

The shard exposes `Config.IncrementalReconcile bool`, default **false**. Off is always-correct fallback (every cycle: full `provider.List()`, walk inventory snapshot to find removals). On uses the delta path:

1. Pass `s.reconcileCursor` (initially empty) as `ListFilter.SinceRevision` on each cycle's `provider.List()`.
2. Process every returned machine through `applyReconciledMachine` (the merge that preserves shard-side `Assigned*` fields and short-circuits on state match).
3. Update `s.reconcileCursor = resp.Revision` after the loop.
4. **Skip the inventory-snapshot walk for removals.** Document the gap explicitly.

`s.reconcileCursor` is process-state, not persisted. Shard restart loses the cursor; the next reconcile is therefore a cold-start full list (empty cursor). This is the static-stability story for the data plane: a restarted shard re-bootstraps from scratch and converges, the same way it would after a crash or rolling deploy.

The fake provider honours `since_revision`. Each machine carries a `lastModRev` set to the provider's monotonically-increasing `rev` at its most recent mutation; `List` with a non-empty cursor returns only machines whose `lastModRev` is strictly greater than the cursor.

**Tombstone semantics in the delta path are deliberately deferred.** The wire field exists for a future ADR; until a real provider drives a removal use-case, we do not pre-design the encoding (option A vs B vs C). Operators running with `IncrementalReconcile: true` against a provider that genuinely removes machines must understand this gap until that ADR lands.

## Consequences

- **Massive wall-clock win at scale.** M11.22 cloud validation: shard cycle p99 dropped 3.69 s → 0.71 s (−81 %) at 500K. Reconcile dropped 696 ms → 73 ms mean on the M5 phase-dump and proportionally on cloud.
- **Two reconcile paths, opt-in.** `Config.IncrementalReconcile = false` is the default and always correct against any provider. Operators must set the flag explicitly to enable the optimisation and must verify their provider honours `since_revision`. The conformance test `TestConformance_ListRevisionAdvances` covers basic cursor advancement; a stricter "honours since_revision" test should be added when a real provider claims above-threshold conformance.
- **Static stability is preserved.** Cursor is process-state; restart re-cold-starts; the data plane stays correct through restart and rolling deploys. The hard rule from `bigfleet.md` §11 is unviolated.
- **The removal gap is real.** Operators running `IncrementalReconcile: true` against a provider that removes machines (rather than transitioning them through the state machine) will see stale entries in the shard's inventory until the next full reconcile (which only happens on shard restart today). This is fine for the fake provider, fine for any cloud provider whose Drain/Delete cycle returns machines to Speculative rather than removing them, and unsafe for an arbitrary provider. The flag should *not* be enabled by default for that reason.
- **Tombstone ADR is owed.** When the first real provider has a removal use-case, write a follow-up ADR choosing between `MachineList.deleted_ids` (extend the wire), `state=UNSPECIFIED` sentinel rows, or periodic full re-sync. Don't pick one now; the shape of the use-case will inform the right answer.
- **Cycle-1 cold-start cost is unchanged.** The first reconcile after process start (or after a restart) still does a full list. At 500K that's ~700 ms on M5 / ~1.4 s on PRO2. This is one cycle per shard lifetime; acceptable.
