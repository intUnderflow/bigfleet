# ADR-0044: seed machine pools are sized by machine demand, not workload weight

## Status

Accepted, 2026-06-11. Harness-scope (no engine change); follows
directly from ADR-0043's "fix the harness and re-measure" applied to
the supply side. Implementation is M66.2b.

## Context

The uber-5k re-baseline on the corrected M66.2 catalog refuted the
complexity audit's prediction in the most instructive way possible:
the Same-cascade got ~6× larger, parking engaged on 136 distinct
gangs, and bind plateaued at 88.9 % — because excluding GPUs from
density (M66.2) made GPU machines *correctly scarce* while nothing
resized the seeded GPU pool to match.

The root cause is a unit error in seed composition. The seed picks
each machine's archetype weighted by `Archetype.Weight` — the
*workload-object* frequency. But machine demand per pod differs by
two orders of magnitude across archetypes: a cpu-shaped pod packs
~`density` to a machine (the seed sizes machines as density × the
pod's own resource bucket), while a pod requesting extended resources
packs exactly **one** per machine (device counts do not scale with
density, per M66.2). Weight-proportional machine pools therefore
underweight whole-machine archetypes' supply by ~`density`× relative
to their share of pod demand. On the run that surfaced this,
gpu-training-large gangs were short 120–238 GPU machines *per zone*,
every cycle, on a fleet whose aggregate capacity was ample.

Demand realism (ADR-0043) says the demand is fine: real fleets run
64–256-machine zone-scoped training gangs. A fleet that runs them
also *has* the per-zone GPU pool to place them. The supply model is
what's unrealistic — the mirror image of the demand-side artifact
M66.2 fixed.

## Decision

1. **Machine shares derive from pod demand.** A shared computation in
   `pkg/scaletest/archetype` produces per-archetype machine shares:

       podShare(a)    ∝ Weight(a) × E[replicas per workload object](a)
       machineShare(a) ∝ podShare(a) / podsPerMachine(a)

   where `podsPerMachine(a)` is `density` when the archetype's size
   buckets request only core resources, and `1` when any bucket
   requests an extended resource. `E[replicas]` comes from the
   service-size distribution for non-gang archetypes and the mean of
   `GroupSizeRange` for gangs.

2. **The service-size replica distribution moves to
   `pkg/scaletest/archetype`** (from the load-driver's package main),
   for the same reason the legacy shape tables moved to
   `pkg/scaletest/preflight`: two components must agree on it and a
   table nothing can cross-check will drift.

3. **Gang archetypes get a per-zone floor.** For every archetype with
   `sameRack`/`sameZone`, the seed places at least
   `max(GroupSizeRange)` machines of that archetype per zone (and
   rack-coherent blocks for `sameRack`, which `seedZoneRack` already
   does). Without the floor, the largest gang the catalog can draw is
   unsatisfiable by construction and every run rediscovers it as a
   parked-gang population.

4. **Supply totals derive from the catalog; `scale.machines` keeps
   defining demand.** `totalPods = scale.machines × density` is
   unchanged. The V2 renderer computes effective seed totals as
   `Σ_a totalPods × podShare(a) / podsPerMachine(a)` plus gang
   floors, split across Configured/Idle/Speculative by the existing
   fractions. Profiles with whole-machine archetypes therefore seed
   more machines than the nominal — that is the realistic fleet
   shape, and the cost note should say so.

## Consequences

- uber-5k-class profiles grow a real GPU pool (order hundreds of
  machines); the 88.9 % structural bind ceiling disappears, and the
  ADR-0042 parking engagement measured against the starved seed is
  void as a baseline. The post-M66.2b re-baseline brief is the first
  realism-clean measurement of both the canonical SLOs and parking.
- The dev-50 V2 gate's catalog (realistic-dev) satisfiability stops
  depending on weight coincidences.
- The legacy no-catalog seed (preflight tables) is unaffected — it
  dies in M66.5 regardless.
- Parking and rule 5 remain measured-but-undecided until the clean
  re-baseline: ADR-0043 applies to conclusions drawn from the starved
  seed exactly as it applied to the fabricated demand.
