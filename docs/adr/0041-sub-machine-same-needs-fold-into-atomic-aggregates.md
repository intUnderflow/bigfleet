# ADR-0041: Sub-machine `Same`-Needs fold into atomic aggregates — `Same` is for cross-machine topology

## Status

Accepted, 2026-06-11.

## Context

The claim ledger is machine-granular and exclusive: crediting walks
matching machines, claims each whole machine for one Need, subtracts
its full Allocatable, and stops when the Need is covered. That was
truthful for the demand shape it was built against — a few large
aggregated Needs per (cluster, fingerprint), each consuming many
machines (ADR-0027), arbitrated per-machine by the OCC broker
(ADR-0029).

Two correct decisions changed the demand shape underneath it. ADR-0024
made each co-location group its own Need — necessarily, since each gang
needs its own single-domain coherence. ADR-0039 made every Pod carry a
CR, so *every* group is visible, not only the pending ones. At uber-5k
that is ~2,400 gang Needs — and at density 10–100, a gang of 3–8 Pods
is a **fraction of one machine**. Under exclusive claiming each
sub-machine gang up-rounds to a whole machine: ~2,400 gangs demand
~2,400 exclusive machines where ~540 gang-archetype machines exist,
while kube-scheduler happily packs many gangs per machine. The measured
cloud signature: bind 97.5 % (Pods placed and packed) with 78 % of
gangs reported unsatisfiable (the ledger starved), Configured inflated
+48 % toward one-machine-per-gang, and Phase 3 churning the claim-race
leftovers. The closed-loop simulator reproduces the same signature in
0.4 s (`TestClosedLoop_SubMachineGangsLedgerMatchesReality`).

The insight resolving it: **a gang whose entire aggregate fits on one
machine does not need the `Same` machinery at all.** Any single machine
with room hosts the whole gang, so co-residency is automatic. And the
wire contract already has the atomicity primitive — *Fleet-Scale
Kubernetes* §7 defines `min_unit` as "the largest atomic schedulable
unit — a per-machine floor that preserves indivisibility". A
one-machine gang *is* an atomic unit. `Same` exists for the genuinely
cross-machine case: a gang too large for any machine, whose machines
must share a topology domain.

## Decision

1. **Demand normalization at the shard, per cycle.** Before the
   decision phases run, the shard normalizes the cycle's demand
   (`decision.NormalizeDemand(snap, needs)`): a `Same`-carrying Need
   whose `AggregateResources` fit on at least one matching machine in
   the current snapshot (any serving tier: the cluster's
   Configured/Configuring, or shard-wide Idle/Speculative) is
   **foldable**. Foldable Needs of the same (cluster, Profile,
   aggregate size) fold into one plain Need: the `Same` requirement is
   stripped, `AggregateResources` are summed, and **`min_unit` = one
   gang's aggregate** — the §7 atomicity floor, so the vector math only
   counts machines that can host a whole gang. Needs that fit no
   machine keep their per-gang `Same` Need — the cross-machine topology
   case, where machine-exclusive claiming is genuinely correct.
   Every phase consumes the same normalized demand.
2. **Rider — Phase 3's acquirable fold consumes, like Phase 1's.**
   Phase 3's joint ranking folded acquirable supply with a nil
   claimed-view while Phase 1's pre-pass folds claimed-aware, so the
   moment idle `Same`-capacity appeared, every gang ranked the same
   fresh domain best and Phase 3 mass-reclaimed healthy bound gangs
   (20 of 24 in the simulator's trace). Phase 3 now virtually consumes
   acquirable members as its sequential walk assigns them, mirroring
   Phase 1's sequencing — restoring the ADR-0040 Addendum's "identical
   joint potential" promise.
3. **Rider — prefer-creditable, ahead of size scoring (satisfiable
   regime only).** Among *satisfiable* buckets, `ChooseSameBucket`
   prefers one containing creditable supply over an acquirable-only one
   **before** the smallest-total comparison — sticky-domain semantics:
   a Need's currently-serving domain must not lose to a fresh idle
   domain that merely scores smaller or sorts lower and relocate a
   healthy gang. Staying put costs nothing — excess machines *within*
   the serving domain are still reclaimed individually by the claim
   loop's stop-when-covered. The preference is deliberately confined to
   the satisfiable regime: among unsatisfiable buckets the Addendum's
   most-covering rule must keep winning (concentrate-then-park), or a
   2-machine serving domain would pin a Need away from a 3-machine idle
   domain it genuinely needs.

## Consequences

- The exclusivity assumption of the OCC claim model becomes **true
  again** for every Need that reaches the phases: folded aggregates
  consume many whole machines; surviving `Same`-Needs genuinely consume
  whole machines. No broker/displacement change (ADR-0029 untouched).
- Need cardinality collapses from O(co-location groups) to
  O(fingerprints × gang sizes) — deflating the cycle-cost pressure and
  most of the roll-up-size tension recorded against the paper's §11
  claim.
- Conservatism under fragmentation: the folded Need counts only
  machines that fit a whole gang, even though the scheduler could place
  a gang across two half-free machines on one rack. BigFleet may
  slightly over-provision in fragmented states; capacity feasibility is
  never overstated.
- Classification is snapshot-dependent: if the fleet's largest matching
  machines disappear, a previously foldable gang class reverts to
  per-gang `Same`-Needs next cycle. Deterministic, recomputed per
  cycle.
- Acceptance is simulator-first: un-skip
  `TestClosedLoop_SubMachineGangsLedgerMatchesReality` (this decision's
  acceptance criterion), all closed-loop scenarios green, `make
  bench-hot` flat — then one mechanism-validation cloud run.
