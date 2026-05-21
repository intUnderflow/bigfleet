# ADR-0039: One CapacityRequest per Pod — not per *unschedulable* Pod

## Status

Accepted, 2026-05-21.

## Context

BigFleet's demand signal is the operator roll-up, aggregated from
`CapacityRequest` objects. The reference per-pod controller
(`pkg/controller/cr`, the `bigfleet-unschedulable-pod-controller`)
creates a CR only for Pods it observes in `PodScheduled=False,
reason=Unschedulable`.

The diagnostic chain bigfleet-uber #45 → #48 traced the sustained
~32 machine/sec Bootstrap+Reclaim cascade on stable demand to this.
#48 measured it directly: **~84 % of bound Pods carry no CR**. A traced
Pod was scheduled 13 s after creation having never been marked
`Unschedulable`, so the controller never created a CR for it. Two paths
bypass `Unschedulable` entirely:

- the scale-test pre-bind fast-path binds Pods via the Bind API before
  the scheduler ever marks them `Unschedulable`;
- after ADR-0038, a controller-recreated Pod binds straight onto
  capacity Phase 1 has just Bootstrapped, again skipping `Unschedulable`.

The consequence is asymmetric. **Phase 1 (Bootstrap)** is correct on an
unmet-demand signal — "what have I not yet satisfied?" *is* unmet
demand. **Phase 3 (Reclaim)** is structurally broken by it — "do I have
excess supply?" requires *total* demand. Phase 3 reads CR-count per
archetype as its demand proxy; with CRs undercounting total demand ~6×,
it sees a permanent phantom surplus and reclaims into it every cycle.
Each reclaim evicts real bound Pods; ADR-0038's controllers recreate
them; they re-acquire CRs only transiently — sustaining the cascade.

The papers are unambiguous on the intended model:

- *Fleet-Scale Kubernetes* §6.1: **"One CR per pod."**
- *BigFleet* §13 (scale-down): "CR garbage-collected via ownerRef →
  next roll-up has fewer needs → Phase 3 reclaims." This mechanism only
  works if a *running* Pod has a CR — otherwise deleting a bound Pod
  changes nothing in the roll-up.

The same papers' §12 describes the reference controller as one that
"watches `PodScheduled=False, reason=Unschedulable`." That mechanism
yields one-CR-per-pod **only under an implicit assumption**: that every
Pod is `Unschedulable` at birth — true when a BigFleet-managed cluster
runs at capacity, so a new Pod has nowhere to go until BigFleet
provisions. When that assumption breaks (a Pod binds onto spare
capacity, or is pre-bound), §12's mechanism under-produces CRs and the
§6.1 / §13 invariants are violated. §6.1 and §13 are load-bearing;
§12's `Unschedulable` filter is an implementation detail that is only
*incidentally* correct.

## Decision

The reference per-pod capacity-request controller creates a CR for
**every Pod**, not only those in `reason=Unschedulable`. It honours the
§6.1 contract — one CR per Pod, for the Pod's lifetime — directly,
regardless of whether the Pod ever transits `Unschedulable`.

The CR remains owner-referenced to its Pod and is garbage-collected
when the Pod is deleted — withdrawal stays implicit (§13), unchanged.

No `pkg/decision` or operator change. With the demand signal now
complete, Phase 3's existing `claimMatching` arithmetic is correct:
total demand vs Configured supply yields the true surplus, and Phase 3
reclaims it once and self-arrests.

## Consequences

- The roll-up carries the cluster's *total* desired capacity, matching
  *BigFleet* §1 / CLAUDE.md ("roll-ups are the cluster's complete
  desired state") and the author's stated rule ("total capacity, not
  the extra, or constant thrashing"). The #45–#48 reclaim cascade ends.
- CR object count rises to ≈ one per Pod (previously only the
  unmet-demand fraction, ~16 %). The roll-up *wire* size is unaffected —
  ADR-0027 already made it a constrained aggregate, independent of Pod
  count. The cluster apiserver's object count roughly doubles
  (Pods + CRs); measured at validation.
- Paper divergence recorded: *Fleet-Scale Kubernetes* §12's "watches
  `PodScheduled=False, reason=Unschedulable`" is superseded — the
  controller watches all Pods. §6.1 ("one CR per pod") and §13
  (scale-down) are unchanged and are the invariants the controller now
  satisfies unconditionally.
- The controller's name (`bigfleet-unschedulable-pod-controller`) is now
  a historical misnomer. Renaming touches the binary, chart, metric,
  and docs; it is cosmetic and deferred — not done here.
