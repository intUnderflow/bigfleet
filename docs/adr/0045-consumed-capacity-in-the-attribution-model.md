# ADR-0045: consumed capacity enters the attribution model

## Status

**Proposed — author decision required** (plan §12 queue item 2).
This is engine-semantics and wire-contract territory; per precedent
(ADR-0027) it does not ship without sign-off. M67 implements the
accepted option; M68 (single attribution) builds on it.

## Context

The engine has no representation of capacity consumed by bound Pods.
Phase 1 credits demand against machines' gross
`EffectiveAllocatable()` (thirteen credit sites across
`pkg/decision/occ/{seed,cycle,candidates,samesupply}.go` and
`normalize.go`; `machine.Machine` has no consumed/free field), and
Phase 3 independently re-derives a keep-set with the same gross
arithmetic (`phase3_reclaim.go:158-186,216-319`) — keeping the
*perfect-bin-packing* machine count.

The failure is pinned deterministically by
`sim/m67_repro_test.go` (`TestClosedLoop_ConsumedCapacityInvisible`)
and was observed live on kind and cloud (2026-06-11): at the tail of
a fill, open demand fits the gross aggregate but no per-machine
residual, so Phase 1 reports `p1_unsatisfied=0` while the scheduler
holds unplaceable Pods, acquisition stops with idle supply standing
by, and Phase 3 reclaims "excess" machines that are hosting bound
Pods — then the loop converges *quietly* in the defective state. In
the repro, 10 % gross headroom suffices: 4 reclaims evict 32 bound
Pods under pending demand.

Three facts constrain the fix:

1. **The roll-up already carries total desired state.** Every CR,
   no phase filter (`rollup.go:121-127`); ADR-0039 makes the CR live
   for the Pod's entire lifetime precisely so Phase 3's surplus
   arithmetic sees total demand. Open-CRs-only is not an option.
2. **The gross diff is the papers' own arithmetic.**
   `fleet-scale-kubernetes.md:89` (ADR-0027 revision): "diffs
   `aggregate_resources` against `Σ machine.Allocatable`". The
   arithmetic is exact only under perfect packing; nothing in the
   papers models the assumption failing. This ADR chooses new model
   territory, not a restoration.
3. **"BigFleet does NOT watch pod events"** (`bigfleet.md:114`).
   Per-pod placement must not enter the shard. The cluster side,
   however, already observes binding state (the operator consults
   Pod status to acknowledge CRs, `rollup.go:175-177`).

### Demand realism (ADR-0043)

The trigger is harness-observed but the shape is production-universal:
any fleet operating at realistic utilisation has fragmentation —
residuals scattered across machines that sum to more than the largest
placeable unit. The repro needs only 90 % per-machine utilisation and
one open workload larger than a residual. No catalog artifact is
involved; production fleets emit this shape continuously.

## Options

**A — Demand-side split only** (roll-up marks each Need's bound vs
open portion). Insufficient alone: the engine still credits gross
supply, so satisfied-by-arithmetic recurs; rejected as a complete fix.

**B — Operator-reported consumption (recommended).** The operator —
which already watches the cluster — reports per-machine consumed
vectors on the existing session stream (additive message with
`supersedes_key` coalescing, like every other coalescing type), and
the roll-up splits each Need into bound and open portions (it knows
CR↔Pod binding state today). The shard stores
`Consumed` per machine; the attribution model becomes:

- Phase 1 satisfies **open** demand against **residual** supply
  (`EffectiveAllocatable − Consumed`) plus acquirable machines.
- Phase 3's keep-set becomes direct rather than re-derived:
  keep machines with non-zero consumption (they host bound Pods)
  plus machines claimed for open demand; reclaim only empty-and-
  unclaimed. The perfect-packing assumption disappears from both
  phases, and "excess" finally means what the paper intends.

Consistency note: total demand vs gross supply double-counts nothing
today only because consumption is invisible; B's split (open demand
vs residual supply) is the consistent pairing once it isn't.
Paper alignment: roll-ups stay full-replacement total state
(ADR-0039 intact); the shard still receives only machine-level
aggregates, never pod events (bigfleet.md:114 intact); the ownerRef
GC reclaim flow is preserved (Pod dies → consumption drops → machine
becomes empty-and-unclaimed → Phase 3 reclaims). Wire impact:
additive fields only. Cluster-side cost: per-node aggregation by the
operator (a pod informer with field-stripping, the same pattern the
load-driver's bind watcher uses).

**C — Engine-side inference** (shard treats its own assignment
history / Phase 1 claims as consumption; no wire change). Cheap but
unsound: the kube-scheduler, not BigFleet, decides placement, so
intended and actual placement drift — re-creating the same wrongness
with a different cause. Useful only as M68's claimed-set persistence,
not as the consumption source of truth.

## Decision

Proposed: **Option B**, with M68 building Phase 3's keep-set from
consumption + Phase 1's claimed-set (one attribution, two inputs).
Papers get a companion diff noting the §ADR-0027 arithmetic revision
("diffs open demand against residual supply") — same mechanism as the
ADR-0027 paper-diff precedent.

## Consequences

- The `sim/m67_repro_test.go` characterization test inverts: its five
  defect pins become the fix's acceptance criteria (shortfall
  reported, acquisition fires, demand places, zero reclaims under
  pending demand).
- dev-50-v2 (the catalog gate, parked on this defect) goes green and
  replaces the legacy gate (M66.5 unblocks, then M77).
- The bootstrap≈reclaim oscillation evidence (#59/#60) is expected to
  collapse; the M78 re-baseline measures the ADR-0042 parking layer
  on a sound engine for the first time.
- Roll-up and session-stream size grow by the consumption messages;
  coalescing bounds steady-state traffic to changed machines only.
- The fleet-scale paper's ADR-0027 section needs the companion diff —
  flagging explicitly: this revises paper-documented arithmetic.
