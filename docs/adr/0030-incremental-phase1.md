# ADR-0030: Incremental Phase 1 — delta-only processing as a layered optimization

## Status

Proposed, 2026-05-17. Conditional follow-on to [ADR-0029]. Implemented
only if post-OCC measurements show steady-state cycle cost dominated
by re-processing unchanged Needs (rather than churn).

## Context

[ADR-0029] redesigns Phase 1 as a single-pass Omega-style OCC: every
cycle, a worker pool walks the **entire** NeedsTable, claims
inventory through a commit broker, and produces a coherent claimed-
set at the barrier. Concurrency reduces wall-clock by ~10–14× but
the per-cycle work is still **O(NeedsTable)**.

In steady state most Needs don't change cycle-to-cycle. A long-
lived `cpu-service` Deployment emits the same Need for its 4
replicas every cycle indefinitely; its CapacityRequest hash is
identical; its placement against inventory is stable. Re-processing
it 60 times an hour is wasted work — the answer hasn't changed.

Incremental Phase 1 is the lever that makes per-cycle work
proportional to **churn**, not to total NeedsTable size. The
mechanism is straightforward: detect which Needs (and which
inventory machines) changed since the previous cycle, process only
the delta, carry the rest forward.

This ADR is deliberately Proposed rather than Accepted: it's
conditional on measured evidence that the steady-state cycle cost
under ADR-0029 is large enough to justify the implementation
complexity. If post-OCC re-baselines show steady-state cycles
already well under target (cold start being the dominant cost
instead), this ADR stays on the shelf.

## Goals

1. **Steady-state cycle p99 ∝ O(churn), not O(NeedsTable).** A
   1M-Need shard with 100 changed Needs/cycle should do roughly
   the work of a 100-Need shard.
2. **No regression on burst / cold-start** behavior. Burst arrivals
   are inherently delta — incremental should handle them as well
   as or better than full-pass OCC.
3. **Preserve every invariant from [ADR-0029]**: priority on
   conflict, partial fills via shortfall, per-bucket sequence-
   number CAS, attribution mirroring with Phase 3.
4. **Bounded memory cost.** Carry-forward state per Need is
   bounded by NeedsTable size × a small per-Need state struct
   (~few hundred bytes); same order of magnitude as the existing
   NeedsTable footprint.

## Non-goals

- **Skip Phase 2 / Phase 3 changes.** Those phases continue to run
  on the post-barrier claimed-set. Whether their inputs were
  produced by a delta pass or a full pass is invisible to them.
- **Replace OCC.** Incremental layers on top of OCC. A delta cycle
  is still an OCC cycle; the broker, conflict detection, and retry
  budgets are unchanged.
- **Replace Phase 3 reclaim.** Reclaim still runs unconditionally
  per cycle; only the Phase 1 claim path benefits from delta
  processing.

## Design overview

```
┌──────────────────────────────────────────────────────────────────┐
│              Cycle N (incremental)                               │
│                                                                  │
│   ┌─────────────────────┐                                        │
│   │  Cycle (N-1) state  │ ──── carry forward ─────┐              │
│   │  • Claimed-set      │                         │              │
│   │  • Per-Need digest  │                         │              │
│   └─────────────────────┘                         ▼              │
│                                          ┌─────────────────┐     │
│                                          │   Delta scan    │     │
│   ┌─────────────────────┐ ──── input ──▶ │                 │     │
│   │  NeedsTable (now)   │                │   added /       │     │
│   └─────────────────────┘                │   removed /     │     │
│                                          │   modified      │     │
│   ┌─────────────────────┐ ──── input ──▶ │                 │     │
│   │  Inventory snapshot │                │   inventory     │     │
│   │  (now)              │                │   churn         │     │
│   └─────────────────────┘                └────────┬────────┘     │
│                                                   │              │
│                                                   ▼              │
│                                          ┌─────────────────┐     │
│                                          │  Delta queue    │     │
│                                          │  (changed Needs │     │
│                                          │   only)         │     │
│                                          └────────┬────────┘     │
│                                                   │              │
│                                       ┌───────────┴────────────┐ │
│                                       │  OCC workers (ADR-0029)│ │
│                                       │  process delta only    │ │
│                                       └───────────┬────────────┘ │
│                                                   │              │
│              ──── COMMIT BARRIER ────             │              │
│                                                   ▼              │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │  Cycle N state = (Cycle N-1 carry-forward) ∪ (delta)    │    │
│   └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│   Phase 2 / Phase 3 read this as before                          │
└──────────────────────────────────────────────────────────────────┘
```

The cycle's claimed-set is the **union** of unchanged carry-forward
claims plus newly-processed delta claims. Phase 2/3 see the same
coherent claimed-set they always do.

## Detailed design

### Change detection

Per-Need digest: a stable hash of the Need's `(Profile fingerprint,
AggregateResources, MinUnit, CoLocation, TopologySpread)` tuple.
Computed once at receipt; carried in the operator's roll-up; stable
across cycles for unchanged Needs.

A Need is **changed** in cycle N if:
- It's new (no entry in cycle N-1's digest map), or
- It's removed (entry in cycle N-1 but not in current NeedsTable), or
- Its digest differs from cycle N-1's entry.

An inventory **machine state transition** is also a delta:
- Idle → Configuring → Configured (a claim materialised)
- Configured → Drain → Idle (a reclamation finished)
- → Failed (provider-reported failure)

These come from the existing shard reconciliation loop; the
incremental processor subscribes to them.

### Delta processing

For each delta entry:

| Delta kind | Processing |
|---|---|
| **Added Need** | Standard ADR-0029 OCC claim path |
| **Removed Need** | Release the Need's claimed machines back to Idle (Phase 3 reclaim will pick them up) |
| **Modified Need** | Release previous claim, re-claim with new shape (treat as remove-then-add) |
| **Inventory transition** | Update carry-forward state; re-claim any Need whose machines were affected |

The OCC machinery from [ADR-0029] is unchanged: the same commit
broker, same per-bucket sequence numbers, same retry budget, same
priority-on-conflict displacement.

### Drift detection

Incremental state can drift from ground truth if a bug, a missed
delta event, or a state-machine race causes the carry-forward to
diverge from what the inventory snapshot actually shows. Two
mechanisms catch this:

1. **Per-cycle digest check.** At the end of each cycle, hash the
   claimed-set and compare with a hash computed by walking the
   inventory snapshot's Configured machines. Mismatch ⇒ drift
   detected; the next cycle is forced to full-pass mode.

2. **Periodic full re-sync.** Every K cycles (default K=100;
   roughly every 100 seconds at 1 Hz cycle rate) the cycle runs as
   a full pass regardless. Trades a small steady-state cost for a
   bounded blast radius if drift accumulates silently.

### Full-pass fallback

The cycle falls back to full-pass (ADR-0029's behaviour, walking
the entire NeedsTable) when:
- It's the first cycle after shard restart (no carry-forward state)
- Drift was detected last cycle
- It's a scheduled periodic re-sync
- More than `fullPassThreshold` fraction of Needs changed
  (default 50%) — beyond that, the delta machinery is more
  overhead than it saves

Full-pass cycles are exactly ADR-0029's design; incremental is
purely a fast-path overlay.

## Performance projections

Empirical churn rates from existing production benchmarks suggest:
- **Steady state**: ~1–2% of Needs change per cycle (services
  scaling up/down, CR GC, occasional re-configuration).
- **Deploy burst**: 5–20% of Needs change in a burst window; the
  burst itself may be a single delta or a few-cycle smear.
- **Cold start / drift re-sync**: full pass — same cost as
  ADR-0029.

Projected per-cycle cycle p99 under incremental, taking ADR-0029's
~10 s uber-500k baseline:

| Scenario | Delta fraction | Projected cycle p99 |
|---|---|---|
| Steady state | ~1% | **~100 ms** |
| Deploy-burst smear | ~10% | **~1 s** |
| Full pass (cold start, drift re-sync) | 100% | ~10 s (= ADR-0029 baseline) |

The cold-start / drift cost is the ceiling; everything else is
better. The 100× steady-state reduction is the lever this ADR
buys.

Whether it's worth the implementation complexity depends on what
fraction of operational time is spent in steady state vs cold-
start equivalents. Production fleets are mostly steady state (most
of the day, every day) so the headline number is meaningful — but
ADR-0029's per-cycle cost may already be small enough that ~10 s
matters less than the design simplicity it costs to avoid the
overlay.

## Invariants

**Preserved from [ADR-0029]:**
- All OCC mechanics: commit broker, per-bucket seqnos, priority-
  on-conflict displacement, bounded retries → shortfall.
- Attribution mirroring with Phase 3 ([ADR-0027] stage 5.1): the
  post-barrier claimed-set is what Phase 3 reads, regardless of
  whether it was produced by full-pass or delta.

**Newly introduced:**
- **Carry-forward correctness**: a cycle's claimed-set equals
  (cycle N-1 ∩ unchanged Needs) ∪ (delta-processed claims). The
  invariant is checked at the cycle barrier via the per-cycle
  digest.
- **Drift bound**: at most K cycles between full-pass re-syncs,
  bounding the worst-case duration of undetected drift.
- **Determinism modulo OCC**: incremental cycles are deterministic
  to the same precision as full-pass cycles (modulo commit
  ordering); the delta path doesn't introduce additional non-
  determinism.

## Open risks

- **Per-Need digest cost**: computing a stable hash over the
  Need's tuple every cycle is O(1) per Need but the constant
  factor matters at scale (775K Needs at uber-500k). Profile-
  ahead: confirm the hash compute fits in the delta-scan budget.
- **Inventory transition events**: today's shard reconciliation
  loop emits state-machine transitions via callbacks; subscribing
  to these reliably is straightforward, but the firehose of
  transitions at uber-500k+ may itself be a hot path. Measure.
- **PDB gap**: same caveat as [ADR-0029]'s. Incremental doesn't
  introduce new PDB-violation surfaces but doesn't fix the
  existing one either.
- **Drift recovery latency**: a full-pass re-sync at the worst
  moment (during a deploy-burst) compounds with the burst's own
  cost. Periodic re-syncs should be scheduled during low-churn
  windows where possible.

## Future work

- **Multi-cycle reasoning**: a Need that's been satisfied for
  many cycles could be "frozen" in carry-forward state with even
  cheaper invariant checks. Premature; revisit if delta-scan
  cost itself becomes a hotspot.
- **Subscription to operator roll-up deltas**: today the operator
  emits a full roll-up every cycle. If the operator can emit
  per-Need add/remove/modify events instead, the shard skips its
  own change-detection step. Requires operator protocol change.

## Alternatives considered

**Compute the full NeedsTable diff on the shard every cycle.**
This is what a naive "incremental" might do — but the diff itself
is O(NeedsTable), so we'd save nothing. The per-Need digest
approach saves O(unchanged-Needs) work because we only re-run
OCC for changed Needs, not because we avoid the comparison.

**Operator emits explicit deltas instead of full roll-ups.**
Cleaner: the operator knows which CRs changed; it tells us. But
this breaks `fleet-scale-kubernetes.md` §7's "roll-ups are full
replacement" invariant and would require a protocol change.
Possible future evolution; out of scope here.

**Skip incremental entirely; rely on OCC alone.** Possible if
post-ADR-0029 measurements show steady-state cost is already
well under target. The cost of incremental is real (drift
detection, carry-forward state, full-pass fallback complexity);
if the benefit is marginal, don't take the cost. **This is the
explicit gating decision before promoting this ADR from Proposed
to Accepted.**

[ADR-0027]: 0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0029]: 0029-phase1-omega-style-occ.md
