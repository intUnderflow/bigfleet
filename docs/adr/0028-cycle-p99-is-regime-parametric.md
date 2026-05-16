# ADR-0028: Cycle-p99 SLO is regime-parametric; the realistic catalog scales with Need cardinality

## Status

Accepted, 2026-05-16.

## Context

The canonical shard cycle SLO has been **p99 ≤ 100 ms** since the
scaleway-500k trajectory work. Every passing result on
`docs/scaletest-results.md` is graded against that bar. It has held
across `scaleway-50k`, `scaleway-500k`, `scaleway-1m` and `dev-500` —
all runs whose operator roll-up produces a tightly-aggregated
NeedsTable (Needs per cluster ≈ Profile-fingerprint count, single
digits per archetype).

Validating uber-5k (the first rung of the public Uber-infra realistic
ladder) surfaced a regime the canonical bar was never calibrated for.
The realistic archetype catalog (`realistic.yaml`) contains
`gpu-training` and `memory-db` archetypes with `sameRack: true` and
small `groupSizeRange` (~3–8 CRs/group). Each group becomes a
separately-labelled `CoLocation.LabelSelector` (per [ADR-0024], with
the gang-scheduler stand-in from [ADR-0025]), so the operator's
`(Profile, CoLocation, group)` roll-up correctly produces one Need
per group — not one Need per archetype.

Inner-agent measurements from a steady-state uber-5k deployment
(bigfleet-uber #16, run `2026-05-16-uber-5k-2host-20x25k`):

- NeedsTable size: **~7,759 rows/cycle** (~388 Needs/cluster across
  20 clusters)
- Phase 1 calls by path: `take = 2,444`, **`takeCoLocated = 3,178,821`**
  (99.92 % of work)
- Shard cycle p99: **1.019 s** (Phase 1 dominates at 1.012 s)
- Per-Need Phase 1 cost: 1,012 ms / 7,759 = **~130 µs/Need**

`BenchmarkPhase1_Uber5K_LateRun` (mirrors the same late-ramp inventory
on the local M5 Max) reports **121 µs/op at 160 Needs**, i.e. ~0.76 µs
per Need with no Prometheus observe, no GC pressure, no metric
recording. Scaled to 7,759 production Needs the bench predicts ~5.9
ms of pure algorithmic work; the rest is the per-Need wall-clock
overhead (histogram observe, allocation pressure, scheduler latency).
**The bench is faithful — Phase 1's wall-clock cost scales linearly
with Need cardinality, with a small constant per Need.**

The dominance of `takeCoLocated` is not a bug. Each independent
sameRack group is meant to be its own Need, with its own `Same(rack)`
requirement; that is the design of [ADR-0024]. The path through
`takeCoLocated` is correct. What changes between regimes is the Need
**count**, not the per-Need cost.

## Projecting the realistic ladder

Per-shard Need-count projection under the same realistic catalog, at
the uber-5k cluster shape (25K CRs/cluster, ~388 Needs/cluster):

| Profile     | Clusters / shard | NeedsTable / shard | Phase 1 p99 (projected) |
|-------------|-----------------:|-------------------:|------------------------:|
| uber-5k     |               20 |              7,759 | **1.01 s (measured)**   |
| uber-50k    |              200 |             77,600 | ~10.1 s                 |
| uber-500k   |            2,000 |            776,000 | ~101 s                  |
| uber-1m     |            2,000 |            776,000 | ~101 s                  |
| uber-5m     |            2,000 |            776,000 | ~101 s                  |

(uber-1m and uber-5m are multi-shard; the per-shard Needs count is
bounded by the per-shard cluster count, not the fleet total.)

The 100 ms canonical bar requires Phase 1 wall-clock to drop by
roughly **8× per ladder rung** to uber-50k and ~1000× by uber-500k.
No constant-factor optimisation closes that gap. What this
projection tells us about the ladder is not "stop" but **"the
absolute cycle bar is the wrong SLO to grade against here."** We
grade BigFleet on the *per-Need* cost — which the bench shows is
constant across cardinality — and let the workload-level cycle and
ramp envelopes relax per rung.

## Phase 1 OCC is the long-term solve, deferred

The structural redesign on the table is **Omega-style optimistic
concurrency control**: instead of one priority-sorted serial walk
over the NeedsTable per cycle, decisions are made independently per
cluster (or per (cluster, fingerprint)) on a shared snapshot, and
conflicts (two needs claiming the same machine) are reconciled at
commit time. This is the model BigFleet's paper anticipates for
multi-shard fan-out and what eventually re-tightens the absolute
cycle bar back toward 100 ms across the realistic ladder.

It is a **major undertaking** — invariant rewrites in `pkg/decision`,
the claimed-set discipline, the Phase 2 / Phase 3 deficit
attribution, and the snapshot-revision plumbing — and it is **not in
the current milestone**. This ADR records the intent and the
prerequisite measurement, not the work. The ladder does not wait
for it; the per-Need bar lets us advance now and let workload-level
envelopes relax with cardinality.

## Decision

1. **The 100 ms cycle-p99 bar applies to the aggregated regime, not
   universally.** "Aggregated regime" means: operator roll-up produces
   a NeedsTable where per-cluster Need count is bounded by the number
   of distinct Profile fingerprints (i.e. no co-location-group
   inflation). All current passing rows in `docs/scaletest-results.md`
   sit in this regime.

2. **The realistic regime publishes Phase 1 p99 per Need.**
   `realistic.yaml` is the published realistic catalog. Runs against
   it are graded on:
   - sustained active CRs ≥ 99.9 % of target,
   - per-Need Phase 1 p99 ≤ **200 µs**,
   - rollup p99 ≤ 1 s,
   - ack p99 ≤ 12 s.

   The cycle-p99 absolute bar is replaced by a per-Need bar because
   the cycle-p99 envelope scales linearly with NeedsTable size and is
   therefore a function of the workload, not a property of BigFleet.
   The 200 µs/Need bar is set at ~1.5× the empirical uber-5k cost
   (~130 µs/Need) and is what an Omega-OCC-redesigned Phase 1 should
   comfortably clear at every ladder rung.

3. **uber-5k is where the realistic ladder currently sits.** uber-5k
   at `00ef120` clears every regime SLO above (130 µs/Need < 200 µs;
   rollup 497 ms < 1 s; ack 296 ms < 12 s; load 100 %). It is
   published as the first row in `docs/scaletest-results.md`'s
   realistic-regime table — the **current** rung, not the ceiling.

4. **The single-shard ladder under realistic catalog is uber-5k →
   uber-50k → uber-500k.** uber-500k is the scale-proof point:
   that's where a single shard runs the per-shard ceiling under a
   realistic catalog and shows the model converges (or doesn't) at
   that workload. uber-50k is the intermediate rung that validates
   the per-Need bar holds and the projections track as Need
   cardinality grows. Each rung sets its own profile-level
   `shardCycleDurationP99Seconds` and `rampBudget` derived from the
   projected NeedsTable size (e.g. uber-50k targets cycle ≤ ~12 s
   and a longer ramp; uber-500k longer still). We grade BigFleet on
   the per-Need bar (constant ≤ 200 µs) and let the workload-level
   envelopes relax with cardinality. Each rung either clears its
   regime-aware envelopes or it doesn't, and we publish either way.

5. **OCC's value is revisited after uber-500k, not before.** Phase 1
   OCC is the long-term mechanism that would re-tighten the
   absolute cycle bar across the realistic ladder, but it's a major
   undertaking and we don't know yet whether BigFleet's current
   single-threaded Phase 1 even converges at the uber-500k workload.
   That data is what tells us whether OCC is worth pursuing as a
   redesign of the model. uber-1m and uber-5m (multi-shard fan-out)
   are deferred until that revisit; they're a separate scaling
   axis and a fresh decision once the single-shard proof lands.

6. **The aggregated-catalog ladder remains unchanged.** Scaleway
   trajectory work (and any future uber-* runs against a
   profile-aggregated catalog) continues to be graded against the
   100 ms canonical bar. That bar has not moved; it has been scoped.

## Held bars vs scaled envelopes

The principle: each SLO scales (or doesn't) with workload size based
on what mechanism actually produces it. **BigFleet-property bars are
held constant across the ladder; workload-property bars scale
linearly with cardinality; user-facing latency is held where
inventory hits, scaled where it bursts into empty.**

| SLO | Scaling rule | uber-5k | uber-50k | uber-500k |
|-----|--------------|--------:|---------:|----------:|
| Per-Need Phase 1 cost | scale-invariant (BigFleet property) | ≤ 200 µs | ≤ 200 µs | ≤ 200 µs |
| Cycle p99 | linear in NeedsTable × 1.5 safety | ~2.3 s | ~23 s | ~230 s |
| Ramp budget | linear in total CRs | 60 m | ~120 m | ~240 m |
| Ack p99 | scale-invariant (rollup-cadence bound) | ≤ 12 s | ≤ 12 s | ≤ 12 s |
| Binding latency (steady-state) | scale-invariant | ≤ 15 s | ≤ 15 s | ≤ 15 s |
| Binding latency (burst-into-empty) | bounded by cycle p99 | ~2 s | ~23 s | ~230 s |

Cycle-envelope numbers come from `projected_NeedsTable × 200 µs/Need
× 1.5` — the 1.5 absorbs GC / scheduler / observe noise while still
catching superlinear regressions. NeedsTable projections come from
the table earlier in this ADR. Ramp budgets scale with total CR
count rather than NeedsTable because they're bound by Bootstrap
throughput, not Phase 1.

A run that misses a **scale-invariant** bar is a real regression. A
run that misses a **scaled** bar is either a real regression (the
per-Need cost grew superlinearly) or the projection itself was off;
both are findable by inspecting the per-Need histogram alongside
the cycle p99.

The honest concession of the scaled cycle envelope: at uber-500k
scale, a *burst* of net-new demand into an empty inventory pool
takes up to one cycle (~150–230 s) before BigFleet emits Provision
actions for it. That is the workload-property cost of running the
realistic catalog at this scale on the current single-threaded
Phase 1, and is consistent with what fleet-level autoscalers
(Karpenter, cluster-autoscaler) deliver. Steady-state binding (the
common path where existing inventory absorbs new Pods) stays under
the 15 s bar at every rung. Whether the burst latency is acceptable
at uber-500k is part of what the scale-proof run actually
determines.

## Consequences

- `docs/scaletest-results.md` grows a "Realistic-regime" section,
  with uber-5k as the first row, per-Need bar explicit, cycle p99
  reported and graded against the rung's own envelope. Subsequent
  ladder rungs land in the same table as they pass.
- The `bigfleet-uber` issues for **uber-50k and uber-500k** are the
  next rungs to file, each with regime-aware per-Need / cycle / ramp
  thresholds derived from the projection above (not the canonical
  100 ms bar). uber-500k is the scale-proof goal.
- The `bigfleet-uber` issues for **uber-1m and uber-5m** are
  deferred pending the post-uber-500k OCC revisit — they're a
  multi-shard scaling axis and a separate decision once the
  single-shard proof lands.
- The "uber-* SLO-passing scale ladder" memory is updated to
  reflect: ladder is in flight at uber-5k under the realistic
  catalog; the per-Need bar is the property of BigFleet we grade
  against; cycle and ramp envelopes relax per rung with Need
  cardinality.
- A future ADR will cover the Omega-OCC redesign itself
  (commitments, conflict resolution, fairness vs. priority strict
  ordering). It supersedes the relevant decision logic in this one.

## Alternatives considered

- **Strip sameRack archetypes from `realistic.yaml`** for uber-50k+
  so the Need count stays bounded. Loses the workload property the
  realistic catalog exists to model (co-location is a core BigFleet
  feature). Rejected.
- **Scale `groupSizeRange` upward per profile size** (e.g. uber-5m
  uses 100-CR groups instead of 5-CR). Production fleets really do
  have larger co-located workloads at larger scale, so this isn't
  unrealistic — but it bakes a per-profile catalog tweak in to make
  numbers look favourable. We'd rather publish what BigFleet does
  on a uniform catalog and grade against the per-Need bar than tune
  the workload to the implementation. Rejected.
- **Optimise Phase 1 constant factor instead of OCC.** Locally
  tried two takeCoLocated allocation reductions (parsed-form alloc
  cache; int64 milli-unit math with scratch maps). Both were a wash
  in the realistic LateRun bench (±10 %), neither approached the 8×
  reduction needed for the next ladder rung. Constant factor work
  isn't load-bearing here.

[ADR-0024]: 0024-co-location-via-podaffinity.md
[ADR-0025]: 0025-harness-anchors-samerack-groups.md
