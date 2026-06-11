# ADR-0042: Unsatisfiable-regime domain choice is sticky at equal coverage — switch only for strictly greater

## Status

Accepted, 2026-06-11.

## Context

ADR-0041 closed the sub-machine-gang cascade; the residual
(bigfleet-uber #55) is genuinely multi-machine GPU gangs — 2–256
whole `a3-highgpu-8g` nodes, rack-coherent — that no single rack can
host. The intended behaviour for those is the ADR-0040 Addendum's
**concentrate-then-park**: assemble as much of the gang as the best
domain allows, hold the rest pending, age in the shortfall buffer
(which escalates — the paper's answer to unsatisfiable demand), and
go quiet.

The diagnostic (#56) proved they never go quiet, and named the
mechanism. Shortfall recording is a passive report — nothing consumes
it to suppress re-attempts — and every cycle Phase 1 re-derives the
gang's joint domain choice from scratch. In the **unsatisfiable
regime** that choice has no incumbent preference: ADR-0041's
sticky-domain rider was deliberately confined to satisfiable buckets
so most-covering could keep concentrating gangs toward genuinely
bigger domains. At uber-5k the GPU racks are dozens of
**identical-total** buckets, so most-covering ties constantly, and
20 clusters' sequential claim walks perturb which blocks look
acquirable to whom — the tie-break (count, then lexicographic value)
resolves differently cycle to cycle. The gang abandons its partial
assembly, acquires toward a different identical rack (scattered
supply guarantees the attempt is never empty), and Phase 3 reclaims
the stranded machines: both halves of the measured ~27/sec
Bootstrap↔Reclaim churn, sustained forever by a static set of ~190
unsatisfiable gangs.

The closed-loop sim's park tests pass because their shapes reach
zero-acquisition and stop; the cloud's contended, scattered supply
keeps acquisition narrowly non-zero. The missing piece is not a new
suppression state — it is that the domain choice has no memory
exactly where memory is what parking means.

## Decision

In `ChooseSameBucket`'s **unsatisfiable regime**, a Need switches
domains only for **strictly greater coverage**. Among buckets of
equal (capped) coverage, one containing the Need's creditable supply
— the domain where its concentrated partial assembly already lives —
wins before the count and lexicographic tie-breaks.

Stateless, like every rule in the chooser: the incumbent signal is
`CreditableCount > 0`, already present in the joint fold (the gang's
previously-concentrated machines are its cluster's Configured,
unclaimed when its priority turn arrives). Phase 3 consumes the same
chooser (ADR-0041), so keep/reclaim parity is automatic.

What this preserves, deliberately:

- **Most-covering still wins when it is strictly better.** A gang
  holding 2 machines on rack A still moves to an empty 3-machine rack
  B — concentration toward genuinely bigger domains is the Addendum's
  point, and the no-oscillation shapes depend on it.
- **Priority remains the sole throttling mechanism** (bigfleet.md
  §16). No aging thresholds, no re-probe cadence, no per-Need
  suppression state. The Need keeps participating in every cycle at
  its priority; it simply stops flip-flopping between equivalent
  domains, which makes in-domain acquirables exhaust and acquisition
  reach zero naturally.

Predicted consequence, to validate: with the domain pinned at ties,
the gang concentrates once, in-domain acquirable supply exhausts,
Phase 1's residual goes quiet (no acquisition), Phase 3 keeps the
credited assembly (parity), and the Need ages in the shortfall buffer
as designed — both churn halves die without new machinery.

## Escalation path

If the contention canary or the cloud re-run still shows residual
churn (e.g. coverage totals genuinely fluctuating rather than tying),
the author-approved fallback is explicit suppression: Needs
classified structurally unsatisfiable on the snapshot and aged ≥K
cycles stop driving acquisition, with a defined re-probe cadence.
That variant costs per-Need state and two tunables; it is not built
until the stateless rule is shown insufficient.

## Consequences

- `ChooseSameBucket`'s documented total order gains one clause:
  satisfiable > creditable-among-satisfiable (ADR-0041 rider) >
  smallest-satisfiable-total / most-covering >
  **creditable-at-equal-coverage (this ADR)** > count > value.
- Acceptance is cloud-decided, sim-pinned — an honest deviation from
  the usual simulator-first pattern. The multi-cluster contention
  canary (`TestClosedLoop_MultiClusterGangContention_ParksQuiet`)
  pins the parking contract but does NOT discriminate pre/post-rule:
  the deterministic sim resolves the count/value tie-break
  identically every cycle, so it cannot express the perturbation that
  flips the choice in the cloud (snapshot deltas from in-flight
  transitions and contending clusters). The discriminator is one
  mechanism-validation cloud run carrying the per-gang probe
  (phasedump-gated): per cycle per churning group, the chosen domain
  and residual. Expected post-fix: chosen domains stop flipping,
  Bootstrap/Reclaim → ≈0 post-fill while `p1_unsatisfied`
  legitimately stays at the unsatisfiable-gang count. If domains
  flip on strictly-fluctuating coverage rather than ties, this rule
  is insufficient and the escalation path engages.
- The per-rack-capacity question (gangs of 64–256 nodes vs racks of
  ~52) is untouched: those classes park instead of churning, and
  whether the catalog should model them at rack scope at all is the
  separate harness-realism follow-up.
