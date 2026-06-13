# ADR-0051: Same-domain choice follows *this gang's* bindings (gang-granular attribution)

## Status

Accepted, 2026-06-14 — author decision. Refines ADR-0045's "the
domain choice follows the bindings" to the correct (gang) granularity;
does NOT reverse it. Implements M77g. Engine + a small additive
contract field.

## Context

M77f (ADR-0045) made a gang's Same-domain choice rank by creditable
**coverage** so it follows the bindings. But it follows the
**cluster's** bindings-in-a-domain: `SameBucket.CreditableTotal` is the
sum of the cluster's Configured/Configuring machines in the domain,
not specifically *this gang's*. So when two domains both fully cover
the gang (a tie at the capped-coverage ceiling), the engine cannot
tell "this domain holds **my gang's** machines" from "this domain
holds an equal number of **unrelated** machines," and the tie falls
through to live **acquirable slack** (rule 3, smallest joint Total).

That was stable until ADR-0050 gave GPU machines their realistic shape.
Under realistic packing the field shows (#64) ~7 machines persistently
in `Configuring` (configure-phase p99 ≈ 4.5 s > the ≈ 3.3 s cycle), so
a bootstrapped machine spans >1 cycle in flight and the acquirable
snapshot the tiebreak ranks on genuinely **moves every cycle** — the
gang flips domain, its old-domain machines become off-domain strays,
Phase 3 reclaims them, the pool shifts, it flips back: the sustained
Bootstrap≈Reclaim lockstep. Diagnosed **endogenous and
production-real** (#64): the perturbation is the engine's own bootstrap
latency, which a real cloud provider's minutes-long bootstraps make
*worse*. (It is invisible offline — instant transitions freeze the
pool and the deterministic chooser self-damps, #63 addendum — which is
why M77f's confirmation was a false green against M66.2's phantom
machines.)

The structural gap: a bound machine does not record **which gang** it
serves. `AssignedNeedFingerprint` (M72) carries only the
`Profile.Fingerprint`, not the co-location `Group`, so two same-profile
gangs are indistinguishable to the chooser — which also defeats any
profile-granular tiebreak (same-class gangs steal each other's
domains, the steal loop the M77g offline probe hit).

## Decision

Make the domain choice follow **this gang's** bindings:

1. **Record the gang on the binding.** Add an additive
   `shard_metadata` key (`bigfleet.lucy.sh/assigned-group`), populated
   at Configure-time from `Need.Group`, mirroring M72's
   store-and-echo `assigned-need-fingerprint`. The provider stores and
   echoes it verbatim — no interpretation, no new RPC, no wire message
   beyond the existing map. The machine gains `AssignedGroup`.

2. **Break capped-coverage ties on gang-own coverage.** In
   `ChooseSameBucket`, among domains tied at the creditable-coverage
   ceiling, prefer the domain holding the most of **this gang's own**
   currently-bound machines (Configured/Configuring whose
   `(profile, AssignedGroup)` match the Need), *above* the
   acquirable-slack tiebreak. `seedSameProfile` computes a per-domain
   gang-own creditable total alongside the existing cluster total.

Configuring machines carry the attribution from the moment of
Configure, so a gang's in-flight bootstraps count toward its own
domain — the choice is **stable through the bootstrap dwell** that is
the perturbation. The decision reads **current** bindings (the machine
state machine), so it is self-correcting and is **not** cross-cycle
memory.

### Why this and not the alternatives

- **Not Option B (carry the chosen domain across cycles).** That is a
  genuine second ledger — state the engine keeps about *past choices*
  — which ADR-0045 forbids and which is fragile (a remembered domain
  can be wrong). C reads where the gang's bindings *are now*.
- **Not Option A (stateless uncapped-coverage tiebreak).** Provably
  insufficient: creditable is cluster-granular, so it cannot
  distinguish the incumbent from a fresh domain holding equal
  unrelated supply. The granularity *is* the bug; A doesn't fix it.

This is ADR-0045's principle implemented correctly. ADR-0045 said
"domain follows bindings"; the cluster-granular implementation was too
coarse to distinguish gangs. "Domain follows **this gang's** bindings"
is the same principle at the granularity the bindings actually have.

## Consequences

- The sim gains **bootstrap-dwell fidelity** (machines stay
  `Configuring` for N cycles) — the missing model that let M77f's
  false-green slip through. The M77g oscillation becomes
  sim-reproducible (true red), and this fix turns it green; future
  regressions of this class are caught locally, not in the cloud.
- The dev-50 catalog gate goes green on realistic machines → unblocks
  M78. Unit pin: a gang with bound machines in domain X must not lose
  to domain Y holding equal *unrelated* supply with more acquirable
  slack.
- Additive contract change only: one `shard_metadata` key. Providers
  store-and-echo; conformance's metadata round-trip (M72) extends to
  it. No proto/RPC change, no engine behavior change outside the
  Same-domain tiebreak.
- Phase 3 / reclaim unaffected (it reads the claimed-set, not the
  domain tiebreak); ADR-0042 parking unaffected (it concerns
  unsatisfiable gangs).

## Addendum — the machine-selection layer (M77h)

Pinning the **domain** (above) was necessary but not sufficient. The
decisive field run (bigfleet-uber #65, on the M77g build) confirmed the
domain flap was gone — 0 domain flips, 4/16 gangs at a complete fixed
point — yet the gate stayed red (`reclaimActionsDuringSoak` ≈ 311). The
residual: **12/16 gangs held their domain but rotated *which machines*
they claimed within it.** "Domain follows this gang's bindings" was now
true; "the *machine set* follows this gang's bindings" was not. The
principle had one more granularity to reach.

### The driver (#65)

The credit/claim pass (`occ.seedSameProfile`) chooses the domain, then
claims that domain's machines in keep-priority order (Configured before
Configuring, then price asc / reclamation_penalty desc / ID asc) under
**stop-when-covered** — claim until the deficit is met, leave the rest
unclaimed for Phase 3 (ADR-0045: the unclaimed Configured remainder *is*
the §8 excess). With configure-phase p99 ≈ 9.8 s (~3 cycles) and ~20
machines persistently `Configuring`, a non-incumbent machine maturing
`Configuring → Configured` in a gang's domain jumps from the back of the
walk (the Configuring section) to its sorted position in the Configured
section. If that position falls inside the first-N-covering subset, it
**bumps an already-serving incumbent out** of the claimed set → the
bumped incumbent is unclaimed → Phase 3 reclaims it (correctly, by its
own contract) → it re-bootstraps → the residual Bootstrap≈Reclaim
lockstep, at the *machine* granularity rather than the domain
granularity M77g closed.

### Decision (M77h)

When the claim loop selects which of a gang's coverable machines to
claim under stop-when-covered, **prefer this gang's own incumbents** —
the Configured/Configuring machines whose `(AssignedNeedFingerprint,
AssignedGroup)` match the Need's `(fingerprint, Group)`, the *same*
attribution this ADR added for the domain tiebreak — before the
keep-priority sort decides among non-incumbents. `seedSameProfile`
marks each candidate `own` (it already computes the predicate for
`CreditableOwnTotal`) and the claim loop applies a **stable** partition
(`incumbentFirst`) that moves incumbents ahead while preserving, within
each group, the keep-priority order the bucket walk built. So a gang
keeps its current machines and only the marginal (deficit) selection
draws from the sorted fresh pool; a maturing equivalent can no longer
bump a serving incumbent.

This preserves the §8 release order for genuine excess: when a gang's
own incumbents themselves exceed the deficit, the stable partition keeps
them in keep-priority order, so the excess it sheds is still the §8
release-order tail (the invariant ADR-0045 ties to the unclaimed
remainder). It reads **current** bindings (`AssignedGroup`) only — no
memory of past claims — so ADR-0045's no-second-ledger rule holds, the
same way the domain tiebreak does. No proto/wire change; Phase 3,
parking, and static stability are untouched (the change is purely the
claim-pass ordering, and Phase 3 still reclaims exactly the unclaimed
Configured remainder).

### Reproduction and scope

The SUSTAINED actuation does not reproduce in the offline closed loop —
it sheds over-coverage once and converges, the same self-damping that
kept M77f/M77g's offline probes quiet (the deterministic cluster model
re-anchors and re-binds to a fixed point even when "wrong but stable").
But the engine-granularity defect the actuation rides on reproduces
**deterministically, fail-pre / pass-post**: a gang whose domain holds
its incumbents plus a non-incumbent equivalent that matures across one
cycle has its claimed set churn pre-fix and stay stable post-fix
(`occ.TestSeedSameProfile_ClaimedSetStableAcrossMaturation` and the
single-cycle `_IncumbentKeptOverMaturedEquivalent`). The
`sim.TestClosedLoop_IncumbencyConverged` guard keeps the converged-state
property (quiescent under dwell + over-coverage + mixed-attribution
racks).

This is the final binding-granularity the claim pass has: the engine
attributes supply at **domain** (M77g) and **machine** (M77h); within a
machine there is nothing finer to follow. The claimed set is now a fixed
point through the bootstrap dwell — stable domain *and* stable machine
set — for a gang served by its own bindings. One residual is known and
deliberately left: a gang over-covered by *its own* machines (all the
same attribution) still re-picks which N to keep as an own machine
matures, because attribution cannot disambiguate equally-attributed
machines; this is genuine over-coverage that resolves in a single §8
shed (not perpetual churn) and is not a realistic steady-state demand
shape (the engine does not bootstrap more machines for an already-covered
gang). Per ADR-0043 it is not worth a cross-cycle mechanism.
