# ADR-0040: `Same`-domain attribution is unified — every supply-crediting site is domain-aware

## Status

Accepted, 2026-05-24.

## Context

The decision engine evaluates a `Same` requirement with two different
semantics depending on where it is asked:

- **Acquisition** (`occ.FindSame`, mirroring the legacy `takeCoLocated`)
  is strict: it buckets candidate machines by the `Same` key's value and
  takes from the **single best bucket only** — a co-located Need is
  served by one domain or not at all. This is the design intent;
  `MatchProfile`'s `Same` case (`pkg/decision/match.go:63`) is
  deliberately per-machine and vacuous, with the comment "group-wide
  co-location is enforced by the autoscaler when picking nodes — not at
  this match step."
- **Crediting** is vacuous: both Phase 1's pre-pass
  (`occ.SeedConfiguredSupply`, `pkg/decision/occ/seed.go`) and Phase 3's
  `claimMatching` (`pkg/decision/phase3_reclaim.go`) walk machines
  through bare `MatchProfile`, so for a `Same`-Profile they credit
  supply **across domains**. `claimMatching`'s doc comment asserts
  "identical attribution rules, so the two phases agree on which machine
  serves which Need" — for `Same`-Profiles that invariant is false. The
  earlier attribution-mirroring fix covered resource vectors and missed
  the `Same` dimension.

A scale-test diagnostic with a per-cycle attribution probe measured the
consequence directly at uber-5k. 93.5 % of the shard's Needs were
co-located (2,570 of 2,750); ~570 of them were reported unsatisfied by
Phase 1 *every cycle*; and the probe's discriminator —
"Phase 3 reclaimed a machine matching a Need Phase 1 declared
unsatisfied in the same cycle" — read **zero** throughout, proving the
two phases never even disagreed about the same machines. Instead:
Phase 1, strict, saw scattered per-domain supply (~1–3 machines/rack
against groups of 3–256) as unsatisfiable and kept bootstrapping toward
gangs it could never finish; Phase 3, vacuous, saw the same Needs as
satisfied cross-rack, kept the scatter, and reclaimed the non-co-located
over-provision Phase 1's gang-chasing created. A self-sustaining
Bootstrap≈Reclaim equilibrium (~8–21/sec across runs), Configured
inflated +77 % over seed, and a structural shortfall that never closed.

Two harness behaviours fed the manifestation (fixed alongside, below):
the pre-bind fast-path ignored `podAffinity`, binding co-located groups
scattered across racks — a placement a real scheduler can never produce,
since required affinity holds the group pending instead; and the
controller-managed-workload reshape (ADR-0038's M54.2) stopped
consulting the archetype's `GroupSizeRange`, so sameRack workloads drew
co-location gangs of up to ~400 whole-GPU machines — unsatisfiable in
any topology the harness runs.

## Decision

1. **Every supply-crediting site is domain-aware for `Same`-Profiles.**
   `SeedConfiguredSupply` and Phase 3's `claimMatching` mirror
   `FindSame`'s rule: bucket the matching machines by the `Same` key's
   value, choose the single best bucket (most-covering; atomic-
   satisfiable preferred — `FindSame`'s existing scoring), and credit /
   claim only within it. `MatchProfile` itself stays per-machine
   candidacy, exactly per its documented contract.

2. **The unsatisfiable residual is a shortfall, not a churn source.**
   No new wire field, no gang/partial mode bit — every Need remains
   partial-fill-tolerant in v1 (the mode classifier stays ADR-0029
   forward-compat). Convergence follows from (1): each cycle's
   bootstraps land in the Need's chosen domain and are credited there
   the next cycle, so the deficit shrinks monotonically until the
   domain's capacity exhausts; the stable residual then sits in the
   existing aged shortfall buffer (paper §16: a `Same` request that
   cannot be satisfied within a shard *becomes a shortfall*; §9 bounds
   and escalates it). Phase 3, now strict, claims exactly the serving
   bucket and reclaims off-domain scatter once.

3. **Harness companions.** The load-driver's pre-bind binds whole
   co-location groups rack-coherently (group → one rack, then bin-pack
   within it), and sameRack workload objects draw `replicas` from the
   archetype's `GroupSizeRange` again instead of the heavy-tailed
   service-size distribution.

4. **The shard gains a flag-gated, read-only per-cycle phase-attribution
   log** (off by default): Need counts split by co-location, Phase 1
   unsatisfied split by co-location, Phase 3 reclaim count, and the
   reclaim-matches-unsatisfied probe. It is the instrument that found
   this defect; future `Same`-domain debugging gets it for free.

## Consequences

- **One-time concentration cost at upgrade.** Phase 3 reclaims scattered
  co-located machines outside each Need's chosen bucket. In production
  that scatter should not exist (the scheduler enforces the affinity
  that defines the group); in the harness, after (3), it no longer does.
- **Need cardinality rises with realistic group sizes.** Restoring
  `GroupSizeRange` multiplies co-located Need count (~128 → several
  hundred per cluster at uber-5k shape). Cycle cost and roll-up size
  scale with co-location group count — a known tension with the paper's
  fixed-size roll-up claim, measured at validation; `FindSame`'s
  per-cycle bucket re-walk is the first perf candidate if cycle p99
  moves.
- `claimMatching`'s "identical attribution rules" comment becomes true,
  and `FindSame`'s stale reference to the pre-OCC allocator file is
  corrected with it.
