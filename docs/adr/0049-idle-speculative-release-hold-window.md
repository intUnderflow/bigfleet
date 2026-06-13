# ADR-0049: Idle→Speculative release — per-CapacityType idle holds inside Phase 3; the hold window is the rail, not a cap

## Status

Accepted, 2026-06-13. Engine scope — M73 (plan §12): paper §8's
release half, the reason fleet spend previously only ratcheted
upward. A separate ADR rather than an ADR-0046 addendum on purpose:
ADR-0046's rails live at the actuation boundary and "can never change
*what* the engine wants, only *how fast* it gets it" — the release
walk changes exactly what the engine wants, lives in `pkg/decision`,
and ADR-0046's own "explicitly NOT built" list already names the M73
release path as the owner of acquisition/spend, not the rails.

## Context

Paper §8, verbatim: *"Phase 3: reclaim excess. Reclaimed → Idle. Idle
→ Speculative lazily per provider (bare metal: forever; on-demand:
minutes; spot: ~1m)."* Until M73 only the first half existed —
machines entered Idle (reclaim completion, Configuring rollback,
seed/reconcile discovery) and never left. `provider.Delete` was on
the wire with M71 fencing and conformance idempotency coverage, and
had zero engine callers.

Sequencing was deliberate: plan §12 gated M73 on M67/M68. Under
ADR-0045's single attribution an Idle machine is bound to no cluster
and counts for nothing — releasing it can never take capacity from
anyone. A Delete path on the pre-ADR-0045 mis-attributing engine
(where Phase 1 could provision what Phase 3 reclaimed in the same
cycle) would have been a Create↔Delete money loop; on the unified
engine the loop is impossible by construction (below).

## Decision

### Release lives inside Phase 3

Phase 3 is the excess-disposal site: it already consumes the cycle's
claimed-set (ADR-0045) and owns every unclaimed machine. The release
walk is the same diff one lifecycle state earlier — unclaimed
Configured in excess of demand → Reclaim (shrinkage); unclaimed Idle
past its hold → Delete (`Reason: "phase3.release"`). A fourth phase
would re-plumb the same snapshot and claimed-set for no isolation
gain; the addition is ~30 lines on a ~106-line phase.

A Delete is emitted for an Idle machine iff:

1. it is **not in this cycle's claimed-set** — a claimed Idle machine
   is the one Phase 1 is about to bootstrap and must not be deleted
   out from under that commitment (same shield Phase 3's reclaim walk
   uses, same single attribution); and
2. its **CapacityType hold has expired**, measured from the
   inventory's idle-since stamp against the cycle's `now`.

The ADR-0036 first-rollup gate deliberately does **not** apply to
releases: that gate exists because an unreported cluster's empty
NeedsTable is "unknown demand", and reclaiming *bound* capacity on it
would kill workloads. An Idle machine belongs to no cluster, so there
is no per-cluster signal to gate on, and the worst case of a wrong
release is one re-buy. The restart window is instead covered by the
clock-reset semantics below: nothing releases until a full hold has
elapsed under the fresh process, which is dozens of cycles and many
roll-up intervals for demand to arrive and claim.

### Per-CapacityType holds, constants not configuration

`decision.DefaultReleasePolicy()`, paper §8 numbers as named
constants (`pkg/decision/release.go`):

| Tier | Hold | Why |
|------|------|-----|
| Bare metal | forever | §8 verbatim. Owned hardware; Delete is provider-rejected anyway (§7 `ErrNotSupported`). |
| Reserved | forever | The commitment is paid whether the machine exists or not (paper §4 — fixed capacity's marginal cost is zero); releasing saves nothing and risks the slot. |
| Unspecified | forever | Never delete capacity whose cost class is unknown. |
| On-demand | 10 minutes | §8's "minutes". The trade is asymmetric and cheap both ways: a wrong release costs one Create + bootstrap latency (no money — cloud Creates are free, the hourly bill *stops*); a wrong hold costs the hourly price for the window (~1/6 machine-hour). 10m rides out the common short demand dips (rolling restarts, redeploys — the timescale ReclaimGrace already budgets for the drain that precedes this hold) without ever holding a full billing hour of pure waste. |
| Spot | 1 minute | §8's "~1m", verbatim. Idle spot is paid-for capacity the provider can interrupt anyway, and spot-priced workloads tolerate provisioning latency by construction — the cheapest possible re-buy. |

ADR-0042-Addendum posture: tunables stay constants until evidence
demands otherwise. There is **no flag and no chart value**. The one
override surface is the library field `shard.Config.ReleasePolicy`
(nil → the defaults), which exists so the sim can pass short holds —
sim cycles ≈ time, and the paper constants would never expire inside
a milliseconds-long closed-loop run. The zero-value policy releases
nothing ("zero value = historical behaviour", the repo convention).

### Idle-since tracking: inventory sidecar, in-memory, no wire fields

M66.1 deleted per-machine transition timestamps as unread; this is
the first consumer that earns one back — and it comes back as a
sidecar map inside `pkg/inventory` (`idleSince`), not a `Machine`
field and not a proto field. Inventory is the single honest stamping
point: every entry into Idle — drain completion, Configuring
rollback, seed, reconcile discovery — flows through `Insert`/`Apply`,
so one chokepoint stamps them all; a same-state Idle→Idle `Apply`
(reconcile field merge) preserves the original stamp. The snapshot
carries a copy (O(#idle)) so the release walk reads a view consistent
with the rest of the cycle.

**Restart semantics, exactly:** the map is process-local. A restarted
shard rebuilds inventory from `provider.List` and stamps every Idle
machine at discovery time, so after a restart machines hold **longer**
than policy, never shorter. The failure mode of a lost clock is a
delayed release — a few extra machine-minutes of spend — never a
premature delete. Conservative in the safe direction; persistence
would buy precision nobody needs at the cost of a new durable state
surface (YAGNI; the audit would flag a wire field for the same
reason).

### Execute path

`executeDelete` walks Idle → Deleting → Speculative — the sanctioned
§5 transition, `StateDeleting` existed unused since M2 — via
`provider.Delete`. The M71 fencing fields ride the wire exactly as
for the other mutating RPCs (the grpcclient stamps them itself).
Idempotent on retry: a machine already past Idle is a no-op, and the
fake/conformance contract reuses the operation_id mid-Deleting. On
reaching Speculative the host ref is cleared (§7: Delete releases the
machine entirely; the ID survives as a quota slot). A provider error
— including `ErrNotSupported` — marks the machine Failed: the policy
never emits Delete for fixed tiers, so a rejection means the
provider's CapacityType declaration and its Delete support disagree,
which is a contract violation worth surfacing, not retrying.

Every disposition is observable like any other kind: audit log
records `kind=Delete` (ADR-0046 addendum),
`bigfleet_shard_actions_total{kind="Delete"}` counts emissions, and
`bigfleet_shard_idle_releases_total` counts completed releases
(rate() over the cycle cadence ≈ releases per cycle). The kill
switch, dry-run mode, and `MaxActionsPerCycle` apply unchanged —
Delete flows through the same suppression points as every kind.

## The rail question: no release cap; the hold window is the rail

ADR-0046's rail 1 caps reclaims because a wrong Reclaim kills
workloads once grace runs out. That reasoning does not transfer:

- **Zero workload blast radius by construction.** A Delete touches
  only machines that are unbound by state invariant (Idle ⇒
  `cluster=""`) *and* unclaimed by the cycle's single attribution.
  ADR-0045's model is the load-bearing fact: an Idle machine counts
  for nothing, so releasing it can never take capacity from anyone.
- **The money failure is not the Delete — it's a release/re-buy
  loop, and the loop cannot close.** Each executed Delete *stops* an
  hourly bill. For a release to cost anything, the machine must be
  re-bought, and Phase 1 acquires only on deficit (bound < demand).
  A deficit that exists at any point during the hold window claims
  the idle machine into the claimed-set and shields it — so a
  released machine was surplus to *all* demand for its entire hold.
  If demand arrives after release, the re-buy happens once and
  produces a **bound** machine, which cannot be re-released without a
  fresh demand shrink followed by another full unclaimed hold window.
  Worst-case churn under adversarial demand flapping is therefore one
  Create per machine per hold period — and that bound is exactly the
  per-tier knob §8's numbers tune (spot flaps cheap by design;
  on-demand gets ten minutes of hysteresis).
- **A cap would slow the only thing M73 exists to do** — stop paying
  for surplus after a genuine scale-down — while protecting nothing
  the hold doesn't already protect. The human backstops are already
  in place: rail 3 (kill switch) and dry-run suppress Delete like
  every other kind.

The loop impossibility is pinned in sim (`sim/m73_release_test.go`):
`TestClosedLoop_IdleRelease_SurplusReleasedOnce_LoopCannotClose` — a
steady-demand fleet with surplus elastic idle releases it exactly
once and acquires nothing over the whole run — and
`TestClosedLoop_IdleRelease_ShrinkageThenHoldExpiry_NoRebuyLoop` —
the full §8 arc: the burst absorbs bare-metal idle then buys elastic,
demand shrinks, reclaim → Idle → hold expiry → Delete for the elastic
tier only (the reclaimed bare-metal remainder idles forever), then
quiet with zero re-acquisition. The steady-state and
supply-exhaustion canaries hold unchanged (their idle tiers are bare
metal, and the paper holds never expire inside sim wall-clock
anyway).

## What is explicitly NOT built

- **A per-cycle release cap** — above.
- **Persistence or wire fields for idle-since** — restart semantics
  section; conservative direction is free.
- **Configurable holds** (flag/chart) — constants until evidence.
- **Release for fixed tiers** — the policy type cannot express it; no
  configuration mistake can delete owned hardware.
- **Operator notification on Delete** — the machine is unbound; there
  is no UpcomingNode and no cluster to notify. `NodeStateUpdate`s for
  it stopped at the Draining→Idle transition that preceded the hold.
- **Coordinator involvement** — shard-local end to end; static
  stability untouched.

## Conformance

M71 already covered Delete idempotency, fencing, and NotFound; M73
adds the lifecycle-position check `TestConformance_DeleteOnConfigured`
— Delete on a Configured machine must fail (the §5 state machine has
no Configured→Deleting edge; a bound machine must be drained first),
with any code except `FAILED_PRECONDITION` (reserved for fencing) and
`Unimplemented` allowed for bare-metal-style providers. "Delete on
Idle succeeds" was already pinned by the full-lifecycle test.

## References

- Paper §7 (Delete semantics; the M71 DeleteRequest blockquote), §8
  (the release rule, verbatim).
- [ADR-0036] — why reclaim is gated per cluster and release is not.
- [ADR-0042 Addendum] — the constants-not-config posture.
- [ADR-0045] — Idle counts for nothing; the single attribution both
  walks share.
- [ADR-0046] — the rails this ADR deliberately does not extend.
- plan §12, M73 row and its M67/M68 dependency note.

[ADR-0036]: ./0036-phase3-gated-by-first-rollup.md
[ADR-0042 Addendum]: ./0042-addendum-aged-acquisition-parking.md
[ADR-0045]: ./0045-consumed-capacity-in-the-attribution-model.md
[ADR-0046]: ./0046-actuation-safety-rails.md
