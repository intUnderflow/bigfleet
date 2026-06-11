# ADR-0042 Addendum: aged acquisition parking — the escalation path, engaged

## Status

Accepted, 2026-06-11. Extends ADR-0042 after its cloud validation
(bigfleet-uber #57) returned PARTIAL.

## Context

The equal-coverage incumbent tie-break cut the unsatisfiable-gang
churn ~3× (≈27 → ≈9/sec at uber-5k) — the acquisition half largely
stopped (`acquired=0` in 23/25 probe samples) — but domains kept
re-selecting: under 20-cluster contention for the shard-wide pool,
per-domain acquirable totals shift slightly every cycle, so coverage
between candidate domains is rarely *exactly* equal and the
strictly-greater branch keeps firing on marginal deltas. Exact-tie
pinning is too narrow to hold under perturbation; the residual
~9/sec is Phase 3 reclaiming the partial assemblies that migration
abandons. This is precisely the contingency ADR-0042's escalation
path named.

## Decision

Three pieces, shipped together:

1. **Group identity crosses the wire.** `CapacityNeed.group` (field 9)
   carries the operator's opaque co-location group ID — one value per
   gang, empty for plain needs. The shard's per-gang bookkeeping and
   the gang-attribution probe previously fell back to fingerprints
   because `Need.Group` was operator-side only; the #57 probe data was
   class-level as a result. The autoscaler derives nothing from the
   value beyond equality.
2. **Aged acquisition parking.** The shard counts consecutive cycles
   each Same-Need class (cluster + group + fingerprint) is Phase 1-
   unsatisfied with **zero acquisition AND no structurally satisfiable
   bucket** (the pre-pass's joint verdict, surfaced as
   `SameSatisfiable`) — so a gang that is still concentrating, or that
   merely lost claim races while a satisfiable bucket existed, never
   ages: parking is concentrate-THEN-park, faithfully. At
   `parkAfterCycles` (8 — debounce against snapshot flicker) the
   class's Needs are stamped `AcquisitionParked` before the phases
   run. Every supply
   site honours the stamp identically: the Phase 1 seed pass and
   Phase 3's joint fold go **creditable-only** (no acquirable bucket
   exists, so the incumbent wins trivially and there is nothing to
   flip to or migrate toward), and acquisition candidates are empty.
   A parked gang keeps its concentrated partial assembly, drives zero
   Bootstrap/Reclaim, and ages in the shortfall buffer — which is the
   paper's escalation surface for unsatisfiable demand. Priority
   remains the sole throttle: parking never reorders anything, it
   only stops futile re-attempts.
3. **Re-probe, so parking is never forever.** Every
   `reprobeEveryCycles` (32) a parked class un-parks for exactly one
   cycle. If supply has appeared it acquires, leaves the unsatisfied
   set, and the age ledger forgets it (un-parked permanently until a
   future relapse restarts the debounce); if not, one cycle of
   attempts (gangs are all-or-nothing proposals, so a futile re-probe
   acquires nothing) and it re-parks.

## Consequences

- The age ledger is the only new state: a per-class int map on the
  shard, touched solely by the cycle goroutine, pruned to the live
  unsatisfied set each cycle. No coordinator involvement; static
  stability untouched.
- Two tunables exist (8 / 32), as the escalation path predicted. Both
  are constants, not configuration — YAGNI until evidence demands
  otherwise.
- The gang-attribution probe now emits real `group` ids plus a
  `parked` bool. Expected cloud signature: `parked=true` lines with
  stable `chosen_domain` and `acquired=0`, Bootstrap/Reclaim ≈ 0
  post-fill, `p1_unsatisfied` legitimately holding at the parked-gang
  count.
- The deterministic sim still cannot discriminate the perturbation
  (ADR-0042's honesty note stands); the parking bookkeeping is pinned
  by unit tests (`TestParkingBookkeeping`) and the contention canary
  now asserts the contract order-independently — zero reclaims/
  preempts/evictions across the whole run, every acquired machine
  still serving at the end — because the OCC worker pool makes
  per-cycle claim winners timing-dependent and a fixed quiet-window
  assertion flakes. The cloud re-run decides, probe in hand.
