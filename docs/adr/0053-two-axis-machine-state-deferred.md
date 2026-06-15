# ADR-0053: two-axis machine-state model (provisioned × bound) — DEFERRED

## Status

Proposed / **Deferred**, 2026-06-15 — author decision. A possible future
simplification of the machine state machine, **explicitly decoupled from
the #66/#74 over-acquire** (which ADR-0052 fixes). Recorded so it is not
rediscovered and so a future "the state machine is over-complicated"
impulse re-reads the costs first. NOT to be implemented until the uber-*
scale ladder completes.

## Context

While fixing the #66/#74 pre-Configuring runway over-acquire, the author
asked whether the 8-state machine (3 stable {Speculative, Idle,
Configured} + 4 transitional {Creating, Configuring, Draining, Deleting} +
Failed, pkg/machine) is over-complicated — whether to collapse it to two
orthogonal axes:

- **host**: provisioned? (the Create/Delete axis)
- **binding**: bound to a cluster? (the Configure/Drain axis)

with the 4 transitional states replaced by a single operation-in-flight
annotation `(op, since)` on the stable state (Creating = Speculative +
Create-in-flight, etc.). The valid stable combinations are exactly the 3
existing stable states; "bound without a host" is the one invalid corner
(already an Invariant, machine.go:317-335; paper bigfleet.md:41).

An Opus-4.8 multi-agent scout (ground → design → stress → verdict;
bigfleet-uber/this session) evaluated it against the status quo and the
ADR-0052 amendment.

## Decision

**DEFER.** The two-axis model is the more elegant data model and is
*safe*, but it is the **wrong instrument for the over-acquire** and is not
worth a foundational refactor mid-ladder.

To its credit (why "defer," not "reject"):

- **Wire-safe / conformance-safe.** The only viable form **freezes** the
  MachineState wire enum (api/proto/.../provider.proto:59-69) and confines
  the decomposition to pkg/machine, re-expanding via a `DerivedState()` at
  the conv boundary. No flag-day, no stranded out-of-tree providers, no
  conformance edit (the 12 state-literal tests pass unchanged), fencing
  fields untouched.
- **Paper-truer.** bigfleet.md:35-41 already presents the model as a
  (Host, Cluster) table with one impossible corner, and treats the
  transitionals as an annotation. No paper diff needed.
- **Static-stability-clean.** No pkg/coordinator import; reconciliation
  stays List+Get.

Why it is **worse for this bug** (and why ADR-0052 is the fix instead):

1. **It does not fix the over-acquire.** That is the ADR-0045/ADR-0052
   *accounting* decision ("count the shard's in-flight commitment"), not a
   state-shape defect. The re-arch's credit predicate `IsBound ||
   Phase==Configure` still excludes `PhaseCreate`, so the bug persists
   verbatim unless the identical accounting change is made. The re-arch
   cannot be justified *by* the bug that prompted it.
2. **Blast radius.** `machine.State` threads ~149 non-test references
   across 9 packages; the re-arch rewrites pkg/machine, the conv map plus
   two further wire→domain maps (grpcadapter, operator/upcoming), ~15-19
   engine switch sites, and forks the L3-resident inventory index
   (inventory.go, five maps keyed on machine.State) — with no winning
   branch (relabel-for-nothing, or re-index under the §9 byte budget for
   nothing). ADR-0052 is ~3 precedented sites.
3. **It increases the engine's correctness surface.** Today {Idle,
   Speculative} and {Configured, Configuring} are disjoint *for free* by
   single-bucket inventory construction. An orthogonal `Phase`
   reintroduces double-match, requiring a hand-encoded `Phase==Stable`
   exclusion at every engine site — in pkg/decision (the extra-scrutiny
   package) — and a Phase-1 double-count is itself an over-acquire.
4. **It relocates complexity, not subtracts it.** The wire keeps all 8
   states; the in-memory model *gains* a Phase enum + a `DerivedState()`.
   Honest model = (HasHost, IsBound, Phase, LastError) — more moving parts
   than one `uint8` State + a `LastError` string.

## If ever pursued (the scout's hard constraints)

- A **standalone** model-clarity/ergonomics ADR, decoupled from any bug,
  **after the uber-* ladder completes** (project scale arc) — never drop a
  foundational rewrite mid-ladder.
- **Wire-frozen:** MachineState verbatim on provider.proto; `DerivedState()`
  the sole boundary translation; conformance untouched.
- Update **all three** wire→domain maps (conv, grpcadapter,
  operator/upcoming), not just conv.
- Default the inventory index to **enum-keyed** (a pure relabel, hot path
  provably unchanged); require a measured benchmark before any re-index.
- Do **NOT** store `since` in the Machine struct — keep transitional-
  timeout tracking in the existing pendingActions ledger; storing `since`
  bloats the §9 footprint and poisons the reconcile fast-path equality
  (reconcile.go:133).
- Add explicit tests for the **derived** Drain-vs-Delete distinction
  (operator-mediation + grace vs none) and the per-site `Phase==Stable`
  disjointness, before merge.

## Consequences

The over-acquire is fixed independently by ADR-0052. This ADR is a roadmap
record and a guardrail: the two-axis model is a *future ergonomics*
candidate with a known, bounded cost, not a remedy for any current defect.
