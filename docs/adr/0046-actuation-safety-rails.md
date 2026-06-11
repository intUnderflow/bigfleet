# ADR-0046: Actuation safety rails — reclaim blast-radius cap, empty-roll-up quarantine, global kill switch

## Status

Accepted, 2026-06-12. Ops scope — first half of M70 (plan §12). The
remaining M70 items (dry-run/shadow mode, `machine.Invariant` at
provider ingest, decision audit log) landed as the Addendum at the
bottom of this file.

## Context

The production-readiness audit (docs/production-readiness-2026-06.md,
arc 3) sustained the claim that nothing bounds the damage when the
engine is wrong:

- **A roll-up can drain a fleet in one cycle.** Roll-ups are full
  replacement (hard rule); a `ClusterCapacityNeeds` reporting zero
  demand is legal and means "reclaim everything I hold". ADR-0036's
  first-rollup gate closes the *restart window* variant — Phase 3
  holds until the operator has reported once — but once a cluster has
  reported, any subsequent wipe is accepted at face value. The wipe
  class is real: an operator-side bug returning an empty CR `List`, a
  wiped CR store behind a healthy operator, or (audit arc 4) a forged
  roll-up over the unauthenticated session.
- **No per-cycle bound on reclaim volume exists.**
  `MaxActionsPerCycle` is a scaletest cycle-SLO knob: it caps ALL
  action kinds, defaults to unlimited, and cannot distinguish "ramp
  burst of Bootstraps" (healthy, want throughput) from "the engine is
  draining a fleet" (want a brake).
- **Pausing actuation requires killing the shard**, which also kills
  reconciliation, shortfall reporting, AvailableCapacity hints, and
  the metrics an operator needs to diagnose the very incident that
  made them want to pause.

These are one arc: bound the blast radius of a wrong decision, refuse
to swallow the most dangerous wrong input without confirmation, and
give the human a stop button that doesn't blind them. The three rails
are deliberately independent layers — the guard catches the wipe
before it becomes demand, the cap bounds the drain rate of anything
that gets through (including engine defects the guard can't see), and
the kill switch is the human backstop for everything else.

## The §16 tension

Paper §16: "No quota, no admission, no entitlement. **Priority is the
sole throttling mechanism.**" Mechanisms that hold actions back look
like throttles, so the line must be drawn explicitly.

§16 governs *capacity allocation among competing tenants*: when
demand exceeds supply, who gets the machine is decided by priority
and nothing else. The rails decide no such question — they are safety
bounds on **actuation volume**, not allocation among claimants:

- The blast-radius cap applies **only to Phase 3 voluntary reclaims**
  — the release of machines that *no demand claims*. There is no
  competing claimant for a reclaimed machine, hence no allocation
  decision, hence nothing for priority to arbitrate. Phase 2
  **Preempts are deliberately exempt**: preemption IS priority-driven
  capacity allocation, and capping it would make a high-priority
  workload's access to capacity depend on how many other preemptions
  fired this cycle — exactly the throttling §16 forbids.
- The empty-roll-up guard does not throttle demand. It delays
  *believing* a demand signal whose shape matches the known
  catastrophic-input class until repetition confirms intent. Once
  accepted, the roll-up's effect is byte-identical; priority
  semantics are untouched at every point.
- The kill switch stops the entire actuator for every tenant equally.
  It expresses no preference between tenants.

Precedent: ADR-0042 Addendum faced the same tension with acquisition
parking and drew the same line — "Priority remains the sole throttle:
parking never reorders anything, it only stops futile re-attempts."
The rails never reorder anything either; they bound how much of the
priority-ordered answer is *executed* per cycle, and when.

## Decision

Three rails, all at the shard's actuation/ingest boundary.
`pkg/decision` is untouched: the engine keeps computing the
paper-faithful answer every cycle; the rails govern what crosses from
decision to execution. This placement is load-bearing — it keeps the
phases pure (testable against the papers), and it means a rail can
never change *what* the engine wants, only *how fast* it gets it.

### Rail 1 — reclaim blast-radius cap

Per cycle, per cluster, at most `max(1, ⌊fraction × C⌋)` Reclaim
actions are executed, where `C` is the cluster's Configured machine
count in the cycle's snapshot. Default `fraction = 0.05`.

- **Surplus rolls over, never drops.** Capped reclaims are simply not
  executed this cycle; Phase 3 is idempotent against an unchanged
  snapshot and re-derives them next cycle. Unlike
  `MaxActionsPerCycle`'s deferral, the cap does **not** trigger an
  immediate follow-up cycle — its purpose is to spread risk over
  wall-clock time, so the surplus waits for the natural cadence.
- **Release order is preserved.** Phase 3 emits each cluster's
  reclaims in the paper §8 release order (cheapest-per-hour first);
  the cap keeps the head of that sequence and defers the tail, so the
  machines released first under the cap are the ones §8 would release
  first anyway.
- **Only `Reclaim` is capped.** Preempts are §16 territory (above);
  Bootstraps/Provisions are acquisitions, which have their own
  natural brakes (supply, spend visibility) and whose worst case is
  cost, not workload death.

**Why 5%:** two requirements bound the choice. (a) *It must never
bind in healthy operation*: organic scale-down arrives via CR
garbage-collection through roll-ups, spread over many roll-up
intervals; steady-state Phase 3 rates observed across the validation
ladder are well under 1% of a cluster per cycle (post-ADR-0042,
near zero). 5%/cycle is an order of magnitude above healthy churn.
(b) *It must convert "fleet gone in one cycle" into "operator-
reaction time"*: at 5%/cycle a full-fleet drain signal needs ≥ 20
cycles (≈ 3–4 minutes at the 10 s default cadence) just to finish
*emitting*, while every executed Reclaim carries `ReclaimGrace` —
10 minutes of operator-side cordon + PDB-respecting eviction
(ADR-0009 / M69) — before workloads actually die. The first
irreversible harm therefore lands no earlier than ~10 minutes after
the anomaly begins, with `bigfleet_shard_reclaims_capped_total` and
the quarantine logs screaming from minute zero — inside on-call
reaction time, and the kill switch stops the remainder. Uncapped, the
whole fleet is in Draining before the first page renders.
`max(1, ·)` keeps small clusters draining (a cluster of 5 still
releases 1/cycle); since the cap re-derives against the shrinking
Configured count, a full drain is geometric then linear, which only
adds margin.

**Honesty note on units:** the cap is per *cycle*, and roll-up
arrivals can wake cycles faster than the 10 s tick. The Σ-over-k-
cycles bound (property-tested) always holds; the *wall-clock*
translation assumes the normal cadence. A wall-clock token bucket
would close that gap and is deliberately not built (see "Not built");
the 10-minute drain grace is the hard wall-clock backstop either way.

### Rail 2 — empty-roll-up quarantine

At the shard's roll-up ingest (`Shard.ApplyRollup`, shared by the
session path and the sim/test path): a full-replacement roll-up that
would retain **less than 10%** of the cluster's previously *accepted*
demand, when that previous demand spans **at least 10 Need rows**, is
quarantined — not applied, logged at WARN, surfaced on the
`bigfleet_shard_rollup_quarantined{cluster}` gauge — until
**3 consecutive** roll-ups consistently report the drop, at which
point the third is accepted and demand replacement proceeds. Any
intervening roll-up that is not a >90% drop against the held baseline
is accepted immediately and resets the quarantine. Quarantine, not
reject: the operator's intent wins; it just has to repeat it.

- **Demand magnitude proxy = Need-row count.** Rows are the post-
  aggregation workload-shape cardinality. The wipe class manifests as
  list-shaped truncation — a failed `List`, an emptied CR store, a
  forged empty message — i.e. rows vanish. Comparing aggregate
  resource vectors across heterogeneous resources (cpu vs gpu vs
  custom) has no single honest scalar; YAGNI. A bug that preserves
  rows but zeroes quantities slips this guard — that variant is far
  rarer than list truncation, and rail 1 bounds it.
- **Floor of 10 rows:** a cluster with few Needs can legitimately go
  1 → 0 rows by deleting one workload; quarantining single-workload
  clusters would tax every dev-cluster scale-to-zero while protecting
  a blast radius the cap already bounds in absolute terms.
- **N = 3:** at the operator's default 10 s roll-up interval, genuine
  mass scale-down is delayed ~2 roll-up intervals (≈ 20–30 s) —
  invisible next to the 10-minute drain grace its reclaims then
  carry. Three consecutive consistent reports are three independent
  executions of the operator's List + aggregate pipeline, which rules
  out one-shot truncation and transient apiserver failure; a
  *persistent* operator-side bug defeats the guard by construction
  (it will repeat the lie), which is why rail 1 exists.
- **Placement: `ApplyRollup`, not `needs.Table.Replace`.** The Table
  is a pure data structure (no logger, no metrics, no per-cluster
  trust state) used directly by tests and the engine as ground truth;
  the guard is an ingest-trust decision about *messages from
  operators*, so it belongs at the boundary where a message becomes
  table state. Both the production session path and the sim path flow
  through `ApplyRollup`, so the guard has one implementation and one
  test surface. Direct `NeedsTable().Replace` callers (the soak's
  synthetic churn) bypass it knowingly.
- **ADR-0036 interplay:** a quarantined roll-up still sets the
  first-rollup flag — the operator *did* report; the demand the shard
  keeps acting on is real previously-reported state, not "unknown".
  After a shard restart the guard's baseline is empty, so the first
  post-restart roll-up is always accepted whatever its size: the
  restart window belongs to ADR-0036's gate, and a wipe arriving as
  the *first* report slips this guard — rail 1 bounds that residue.

### Rail 3 — global kill switch

`--actuation-paused` on the shard binary (`Config.ActuationPaused`).
When set, the cycle runs in full — reconcile, snapshot, Phase 1/2/3,
shortfall recording, AvailableCapacity emission, all metrics and
probes — but **no action is executed**: Bootstrap, Provision, Reclaim
and Preempt are collected, counted per kind in
`bigfleet_shard_actions_suppressed_total`, logged, and dropped.
`bigfleet_shard_actuation_paused` reads 1 so dashboards and alerts
can see the pause. `Step`/`OnActions` still surface the decided
actions (the simulator's trace remains meaningful under pause).

The point is observability-preserving paralysis: during an incident
the operator can stop the bleeding *and still watch the engine's
intentions* (what would it do right now?) while diagnosing — the
exact thing killing the shard process cannot give.

Mechanism: a flag, flipped by redeploy (chart value
`shard.actuationPaused`). Durable across restarts because it is
deployment state, not process state. The coordinator's existing
instruction stream could carry a runtime toggle without a new RPC
surface — noted as a possible follow-up only; not built here, and any
future version must keep the flag as the bottom layer (static
stability: the kill switch must not depend on the coordinator being
reachable).

## Defaults, and where they live

| Rail | Binary flag (production default) | Chart value | Library `shard.Config` zero value |
|------|----------------------------------|-------------|-----------------------------------|
| Blast-radius cap | `--reclaim-cap-fraction=0.05` (`shard.DefaultReclaimCapFraction`; `0` disables) | `shard.reclaimCapFraction: 0.05` | `0` — cap off |
| Roll-up quarantine | `--empty-rollup-guard=true` | `shard.emptyRollupGuard: true` | `false` — guard off |
| Kill switch | `--actuation-paused=false` | `shard.actuationPaused: false` | `false` — running |

The guard's thresholds (10% retain / 10-row floor / 3 consecutive)
are constants, not configuration — same posture as ADR-0042
Addendum's 8/32: tunables stay constants until evidence demands
otherwise.

**Rails default ON at every deployment boundary** (the `shard`
subcommand, `all-in-one`, the chart) and OFF at the library zero
value. This follows the repo's existing split (`MaxActionsPerCycle`,
`IncrementalReconcile`: zero = historical behaviour; flags carry
production defaults) and is a deliberate honesty choice for the sim:
the closed-loop canaries (ADR-0038/0039/0040 classes) exist to
*reproduce engine pathologies* — mass-reclaim cascades and
empty-roll-up oscillations are precisely the behaviours the rails
blunt. Running the canaries rails-on would dampen the signal they
exist to detect and quietly convert "the engine is broken" into "the
rails are hiding it". So the engine's regression surface (sim,
decision tests) runs rails-off, and the rails have their own
dedicated test surface (`pkg/shard/safety_test.go`), including the
Σ-over-k-cycles cap property. Scaletest binaries inherit the flag
defaults, so cloud runs exercise rails-on — at thresholds healthy
behaviour never touches, per the 5% justification above.

## What is explicitly NOT built

- **Per-tenant quotas, admission, demand rate-limiting** — §16. The
  guard quarantines an anomalous *shape*, never a tenant's volume.
- **Wall-clock token bucket for reclaims** — per-cycle is the
  engine's decision unit and the simple bound; the drain grace is the
  wall-clock backstop. Revisit only with evidence that wakeup-driven
  cycle acceleration matters in practice.
- **Caps on Bootstrap/Provision/Preempt** — Preempt per §16;
  acquisitions' worst case is spend, owned by the M73 release path
  and provider quota, not by this ADR.
- **Per-cluster or per-kind kill switches, runtime toggle RPCs** —
  one global flag. The coordinator-instruction toggle is a noted
  follow-up, not scope.
- **Automatic engagement** (e.g. auto-pause when capped-reclaims
  spikes) — engaging a fleet-wide pause is a human/runbook decision;
  an auto-trigger is a new outage mode.
- **Persistence of guard/cap state across restart** — the guard
  baseline is in-memory by design; the restart window is ADR-0036's,
  and rail 1 bounds what slips both.
- **Configurable guard thresholds** — constants until evidence.
- **Dry-run/shadow mode** — second half of M70; built in the Addendum
  below.

## Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `bigfleet_shard_reclaims_capped_total` | counter | Reclaims deferred by rail 1. Healthy steady state ≈ 0; sustained rate = something is mass-draining and the cap is what's slowing it. |
| `bigfleet_shard_rollup_quarantined{cluster}` | gauge | Consecutive roll-ups currently held for the cluster (0 = clear). Non-zero alongside an unchanged fleet = rail 2 is holding a wipe. |
| `bigfleet_shard_actions_suppressed_total{kind}` | counter | Actions the kill switch suppressed, by kind. The engine's "intentions" while paused. |
| `bigfleet_shard_actuation_paused` | gauge | 1 while paused. Alert if non-zero longer than the incident that justified it. |

Suppressed actions are *not* double-counted into
`bigfleet_shard_actions_total` (that counter keeps meaning "emitted
for execution").

## Hard rules touched

- **Priority is the sole throttling mechanism** — see "The §16
  tension". Capacity allocation among tenants remains priority-only;
  Preempts are uncapped by construction.
- **Roll-ups are full replacement** — unchanged. A quarantined
  roll-up is not merged or edited; it is either applied whole (after
  confirmation) or not yet applied, with the previous full-replacement
  state staying authoritative.
- **Static stability** — strengthened: all three rails are shard-
  local, no coordinator involvement, and the kill switch specifically
  preserves the reporting half of the data plane.

## References

- docs/production-readiness-2026-06.md, arc 3 — the audit evidence.
- Paper §8 (Phase 3 / release order, drain grace), §16 (priority is
  the sole throttling mechanism).
- [ADR-0009] Reclaim uses policy/v1 eviction and async drain — the
  grace the cap's timing argument leans on (with M69).
- [ADR-0036] Phase 3 gated by first rollup — the restart-window
  defense this ADR's guard complements.
- [ADR-0042 Addendum] Aged acquisition parking — the §16-tension
  precedent and the constants-not-config posture.

## Addendum (2026-06-12): second half of M70 — shadow mode, ingest validation, decision audit log

Same arc, same boundary, three remaining audit items. All shard-local;
`pkg/decision` stays pure (one doc-comment change, below); no proto or
RPC changes.

### Dry-run / shadow mode (`--dry-run`, `shard.dryRun`)

The day-one adoption posture: an operator runs BigFleet in shadow
against a live fleet to see what it WOULD have done before trusting
it. Cycles run in full; every decided action is reported — one Info
log line with kind/machine/cluster/reason/grace, one increment of
`bigfleet_shard_actions_dryrun_total{kind}` — and nothing executes:
no provider RPC, no BootstrapRequest or ReclaimInstruction reaches an
operator.

Mechanically this shares the kill switch's suppression point, but the
flag and the metric are deliberately distinct from
`--actuation-paused` / `bigfleet_shard_actions_suppressed_total`:
dashboards must be able to tell "shadowing by design" (expected,
possibly weeks-long) from "paused in anger" (an incident, alert if it
lingers). If both flags are set, the pause wins the counting — an
emergency stop during a shadow run should read as a pause. Rail 1 and
`MaxActionsPerCycle` are skipped in shadow: nothing executes, so there
is no drain rate or execute cost to bound, and the report should be
the engine's whole decision, not a rail-metered schedule. (Skipping
the limit also avoids its deferral wakeup busy-looping against a
never-changing snapshot.)

**Honest limitation:** shadow mode cannot observe what would have
*bound*. Nothing executes, so no fake Node materializes, the operator
never writes an UpcomingNode, and no Pod schedules — and because the
engine's actions never apply, its view never converges and the same
intentions re-report every cycle. Shadow validates decision volume
and shape (would it mass-drain? does the acquisition mix look sane?),
not outcomes. Validating outcomes requires the sim/scaletest ladder,
which executes against a fake provider.

### `machine.Invariant` at provider ingest

The audit (arc 3) found provider-declared `price` and
`interruption_probability` entering the locked cost formula
unvalidated, while the validator (`machine.Invariant` — the audit
called it `machine.Validate`) ran only inside `inventory.Insert/Apply`
with its errors discarded at the reconcile call sites, and a reconcile
doc comment claimed the records arrived "pre-validated" (false:
grpcadapter checks only the state enum). Changes:

- `machine.Invariant` now also bounds the cost inputs:
  `price_per_hour` ≥ 0 and not NaN; the probability check catches NaN.
- The shard screens records at its provider-ingest boundary — the
  reconcile slow paths (the state-match fast path ingests no fields)
  and the Create ack, the one ack that carries cost fields. Policy:
  **reject, loudly** — log + `bigfleet_shard_machines_rejected_total
  {reason}` (`price` / `interruption_probability` / `structural`) —
  never crash, never silently accept. The inventory keeps its
  last-known-good record; a rejected record is never treated as a
  removal. A rejected Create ack marks the machine Failed, same as a
  provider error.
- Conformance: `TestConformance_CostFieldBounds` makes the bounds a
  mechanical provider contract. The suite's system-under-test is the
  provider (the shard isn't in that harness), so *survivability* of
  violations is asserted by `pkg/shard`'s own tests; conformance
  asserts providers don't emit the garbage in the first place.

### Decision audit log (`--audit-log`, `shard.auditLog`)

A structured, durable record of every action disposition: a dedicated
`slog` logger writing JSONL to a configurable file path. One record
per executed action — timestamp, cycle, kind, machine, cluster,
reason, grace, outcome (the classified execute result) — and one per
suppressed / dry-run action, marked `outcome=suppressed` /
`outcome=dryrun`. Empty path disables; the chart value points it at
the shard's existing data PVC. This is deliberately NOT a storage
system and NOT a metrics replacement: it is the simplest thing an
operator can ship to their log pipeline and replay after an incident
("what exactly did the shard do to cluster X between 14:02 and
14:09, and why").

- **Rotation / size are explicitly out of scope.** The shard only
  appends; logrotate-style handling (sidecar, pipeline retention) is
  the operator's.
- `cycle` is the shard's cycle counter at record time; under the
  ADR-0021 async execute pool, an executed action's record carries
  the executing cycle, which can trail the deciding cycle.
- Enqueue-time drops/dedups are not audited — they execute nothing
  and re-derive next cycle; their rates stay metrics
  (`bigfleet_shard_actions_dropped/deduped_total`).
- `decision.Action.Reason` was documented as "safe to drop"; that
  stopped being true — the audit trail depends on it surviving to the
  actuation boundary. Comment fixed; still unused for decision logic.

### Addendum metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `bigfleet_shard_actions_dryrun_total{kind}` | counter | Actions reported-not-executed in shadow mode. A per-cycle intention rate, not a count of distinct actions. |
| `bigfleet_shard_machines_rejected_total{reason}` | counter | Provider records refused at ingest by `machine.Invariant`. Sustained non-zero = the provider is emitting garbage. |

### Defaults

| Surface | Binary flag | Chart value | Library zero value |
|---------|-------------|-------------|--------------------|
| Shadow mode | `--dry-run=false` | `shard.dryRun: false` | off |
| Audit log | `--audit-log=""` (off) | `shard.auditLog: ""` | `Config.AuditLogger = nil` |

Ingest validation has no knob: it is not a rail an operator tunes,
it is the contract being enforced.

[ADR-0009]: ./0009-reclaim-uses-policy-v1-eviction-and-async-drain.md
[ADR-0036]: ./0036-phase3-gated-by-first-rollup.md
[ADR-0042 Addendum]: ./0042-addendum-aged-acquisition-parking.md
