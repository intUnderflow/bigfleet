# ADR-0045: capacity counts for a cluster iff it is bound — BigFleet never models packing

## Status

Accepted, 2026-06-12 — author decision, reached in design dialogue.
Supersedes this ADR's own first draft (which proposed
operator-reported per-machine consumption; withdrawn as
scheduler-shadowing). Implemented by M67; M68 dissolves into it.

## Context

The original investigation: at the tail of a catalog fill, the shard
reported `p1_unsatisfied=0` while the cluster's scheduler held
unplaceable pods on fragmented residuals, and Phase 3 reclaimed
machines hosting bound pods under that unchanged demand
(`sim/m67_repro_test.go`, observed on kind and cloud 2026-06-11).
The first draft of this ADR read that as "consumed capacity is
invisible to the engine" and proposed feeding consumption in.

The author's correction reframes it: **clusters demand capacity;
BigFleet decides whether to fulfill; that is the whole contract.**
BigFleet is not a scheduler (hard rule), and any arithmetic that
tries to anticipate whether the cluster's scheduler can *use* bound
capacity — gross-aggregate satisfaction checks, residual math,
consumption vectors, perfect-packing keep-sets — is a shadow of the
scheduler and out of scope by design.

## Decision

One accounting rule: **capacity counts for a cluster iff it is bound
to that cluster.** Binding (Configure) is the atomic act of
fulfillment; the machine state machine is the only supply ledger.

1. **Demand** is the roll-up's total desired capacity (ADR-0039,
   unchanged — no wire changes anywhere in this ADR).
2. **Phase 1**: bound capacity < demand → fulfill the difference.
   Bound ≥ demand → BigFleet's job is done. Because a binding counts
   from the moment it is made — before the node exists — double-
   supply is impossible by construction; no grace windows, no
   in-flight discounting, no second ledger.
3. **Phase 3**: reclaim is triggered by **demand shrinkage** only:
   excess = bound − demand, taken in the paper §8 release order
   through the ADR-0009/M69 operator-mediated drain path (cordon,
   PDB-respecting eviction, real grace). Phase 3 does not re-derive
   per-cycle keep-sets; at steady demand it does nothing. This
   deletion *is* the former M68 ("single attribution") — there is no
   longer a second satisfaction arithmetic to unify.
4. **Satisfied-but-stuck is the cluster's problem.** If bound
   capacity covers demand but pods cannot place (fragmentation at
   equal priority), the cluster holds the levers: kube-scheduler
   preemption resolves it whenever priorities differ; a descheduler
   or revised capacity demands handle the rest. BigFleet carries no
   unmet-demand signal, no telemetry for this state, and takes no
   action — per YAGNI, a signal nothing acts on is speculative
   plumbing. (Genuine undersupply — e.g. Same gangs short of
   per-zone machines — still surfaces exactly as today: bound <
   demand in the constraint-scoped buckets, shortfall buffer,
   escalation.)

Explicitly NOT built, and rejected by name: per-machine consumed
vectors; bound/open demand splits; residual-fit arithmetic in any
phase; a per-machine busy bit for victim selection (PDBs and the
M69 eviction flow already protect drains; if validation shows
needless disruption, that evidence may justify one later); any
persistence/aging rule that overrides bound-vs-demand arithmetic.

## Consequences

- Phase 1 needs little more than doc-comment honesty — its
  bound-counts arithmetic was always this model. The implementation
  weight falls on Phase 3: replace keep-set re-derivation with the
  shrinkage diff. Net code should go DOWN.
- The bootstrap≈reclaim oscillation class (#59/#60 reclaim
  attribution, the dev-50-v2 plateau's reclaim-under-demand half)
  is removed by construction: at steady demand Phase 3 is inert.
- `sim/m67_repro_test.go` inverts: shortfalls=0 and acquisitions=0
  become correct-behavior pins; the surviving defect assertion is
  zero reclaims at steady demand; the pending pods are asserted to
  be exactly the cluster's residue, untouched by BigFleet.
- The dev-50-v2 catalog gate redefines around BigFleet's contract:
  demand covered by bound capacity and zero reclaim churn — not
  cluster bind-percentage, which asserts a promise BigFleet does not
  make. Validation SLOs that gate on bind% of catalog profiles need
  the same review (M78).
- ADR-0042 parking and the shortfall path are untouched: they handle
  bound < demand (genuine unsatisfiability), which this ADR does not
  change.
- The papers need no diff: this restores §8's "reclaim follows the
  next roll-up having fewer needs" and §16's division of labour,
  rather than revising them.
- **Future work (author, 2026-06-12): the sanctioned home for
  stuck-pod smartness is the operator, not the core.** The per-
  cluster operator already translates cluster reality into demand;
  a smarter reference operator — or a user's own specialized one —
  can observe satisfied-but-stuck conditions and reshape what it
  asks BigFleet for, with zero core changes. This mirrors the
  out-of-tree provider model on the demand side: BigFleet ships the
  wire contract and a reference operator; specialization lives at
  the edges, where cluster-specific knowledge lives. Not scheduled;
  recorded so the extension point is designed-for, not rediscovered.
