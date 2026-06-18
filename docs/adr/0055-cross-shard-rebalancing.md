# ADR-0055: coordinator-driven cross-shard rebalancing (§9 idle→quota→preempt)

## Status

Proposed, 2026-06-19 — author decision (2026-06-19) to BUILD cross-shard
rebalancing rather than remove the stub handlers. Realises bigfleet.md §9
("Coordinator response: (1) reassign idle from other shards, (2) reassign
speculative quota, (3) cross-shard preemption — drain-first, expensive,
rare"). Builds on ADR-0002 (single-region single-Raft, the cross-shard
rebalancing SPOF), and is the cross-shard counterpart to the in-shard
anti-oscillation work of ADR-0045 (Phase-3 = pure diff over Phase-1's
claimed set) and ADR-0052 (shard counts its own in-flight commitment).
Reuses the M20/M69 drain path. No new instruction types; one soft-state
field and one author-gated wire selector field.

## Author decisions (resolved 2026-06-19)

The author approved this design as **Proposed** — to revisit before staged
implementation begins — and resolved the two architectural forks:

- **Machine-id sourcing: donor-resolves.** The coordinator addresses the
  three instructions by a `count + (instance_type, zone)` selector; the
  donor shard picks the concrete idle ids (honouring its
  `reclamation_penalty` idle tiebreak) and echoes them. Selector fields are
  added to `ReassignSpeculative` / `CrossShardDrain` / `TransferOwnership`;
  the §9 ~240-byte/shard summary budget and "shards own capacity" hold.
- **Ownership partition: shard-local persisted owned-set** (NOT the draft's
  provider-side ownership tracking). Each shard persists the set of machine
  ids it owns; its `reconcileFull` consults that set so a transferred
  machine survives the next reconcile **without any change to the provider
  contract**. Trade-off: a shard restart rebuilds the owned-set from
  `provider.List` and re-establishes the partition as the coordinator
  re-asserts assignments — acceptable, and it keeps the six-RPC provider
  surface untouched (this realises the "ownership-partition mechanism"
  stage, superseding the draft's option (a)).

Remaining tunables (cooldown ≥ 8 cycles, age > 5 cheap / > 12 preempt,
~5-cycle cadence, soft leader-local cooldown, donor drain grace via the
existing Phase-2 grace function) stand at the recommended defaults pending
implementation. **Status stays Proposed** until the author returns to
greenlight the staged build.

## Context

The wire and delivery for cross-shard rebalancing already exist and are
sound. `CoordinatorInstruction` (coordinator.proto:251-312) carries the
`(coordinator_term, sequence_number, instruction_id)` fence and a oneof
with `reassign_speculative(6)` / `cross_shard_drain(7)` /
`transfer_ownership(8)`. Delivery is pull-based:
`GRPCServer.EnqueueInstruction(shardID, instr)` →
`g.pending[shard][instruction_id]` → drained into `ReportAck.instructions`
on the shard's next `ReportShard`, sorted by sequence_number; the shard
acks via `ShardReport.instruction_acks`; the coordinator resends until
acked; the shard dedups by instruction_id and term-fences against its
high-water mark. This path needs **no changes**.

Two halves are missing:

- **Coordinator emission does not exist.** `EnqueueInstruction` has zero
  non-test callers. There is no leader-side loop that constructs these
  instructions. The only periodic leader loops are `snapshotExportLoop`
  and `joinLoop` (coordinator.go:164,167), both started from `Run` and
  self-gated on `raft.Leader`. A rebalancer is the natural third loop.
- **Shard execution is stubbed by design.** `shardAdapter`'s
  `OnReassignSpeculative` / `OnCrossShardDrain` / `OnTransferOwnership`
  (coordclient/adapter.go:72-74) are literal `return nil` no-ops; the
  dispatch, dedup, term-fence and ack plumbing around them
  (coordclient.go) is complete.

The coordinator already holds every input except two gaps:

- Per-shard `ShardSummarySoft` (TotalMachines, FreeMachines,
  InstanceTypeCounts, ZoneCounts) and `[]ShortfallSoft` (Priority,
  Deficit vector, AgeCycles, InterruptionPenaltyBucket) are collected on
  every `ReportShard` and exposed via `LatestSummary` / `LatestShortfalls`.
  domain→shard, cluster→shard and quota allocations are in Raft state.
- **Gap 1 (soft-state):** `ShortfallSoft` DROPS the requirements list
  (grpc_server.go:181-186 copies only four fields). The shard *sends*
  requirements incl. the `Same` operator (adapter.go:38-45), but the
  coordinator cannot today tell a topology-constrained shortfall from a
  plain one — which the first hard-rule gate requires.
- **Gap 2 (wire-vs-state):** all three instructions are addressed by
  `repeated machine_ids`, but `ShardSummary` is COUNTS only. The
  coordinator can decide "shard B has 40 free type-X/zone-Z and shard A
  has a priority-700 deficit for type-X/zone-Z" but cannot *name* the
  ids. Inflating the summary with per-machine ids breaks the §9
  ~240-byte/shard budget.

The known danger is **oscillation**. ADR-0045 makes Phase 3 a memoryless
diff (excess = bound − demand at machine granularity); a machine
transferred into shard A arrives Idle/unbound and, if A has no matching
open deficit, is surplus that A's Phase 3 reclaims next cycle — the
cross-shard analog of the M77a ping-pong. The donor, having lost the
machine, may re-open its deficit and re-request. ADR-0052 adds a
one-cycle window where a transferred-in Idle machine is uncredited until
the receiver's Phase 1 claims it. The coordinator has no hysteresis state
today; it must be built in as a first-class requirement.

## Decision

Build a **leader-only periodic `rebalanceLoop`** in pkg/coordinator that
emits the three existing instructions under a strict §9 cost-ordered
escalation, and make the three shard stub handlers real. Six elements:

### 1. Loop placement and trigger

A third goroutine started from `Coordinator.Run`, ticker-driven on
`--rebalance-interval` (default a small multiple of the shard report
cycle, ~5×), self-gated `if c.raft.State() != raft.Leader { continue }`.
**Periodic sweep, not per-report**: donor selection needs a global view
across all shards' soft state, which one shard's report cannot give, and
a periodic pass damps oscillation. The flag MUST be wired into
deploy/helm/ in the same change (lessons_learned:586 — a phantom
`--rebalance-interval` once crashlooped the published chart).

### 2. Eligibility gate (hard rules first)

Per pass, build the eligible-shortfall set across all shards. A shortfall
is cross-shard-eligible iff:

- `AgeCycles > 5` (the §9 escalation gate — primary trigger and primary
  hysteresis), AND
- it is NOT topology-constrained. **Soft-state fix:** add a precomputed
  `TopologyConstrained bool` to the wire `Shortfall` (shard sets it true
  iff any requirement uses `OperatorSame`) and copy it into
  `ShortfallSoft`. We ship the bool, not the full requirements list
  (YAGNI: the coordinator would only re-scan it for `Same`). Skip any
  shortfall flagged `TopologyConstrained` — `Same`-rack/`Same`-zone
  deficits remain within-shard shortfalls forever. `In(zone)`/`In(type)`
  still qualify (they pin attributes, not co-location).

Sort eligible shortfalls by (Priority desc, AgeCycles desc).

### 3. Cost-ordered escalation (strictly ascending; stop at first tier that covers)

- **TIER 1 — reassign idle (cheapest, non-destructive).** At age>5. Find
  a donor whose Idle subset (FreeMachines, per-type/per-zone counts)
  matches the deficit with a retained surplus margin. Emit
  `TransferOwnership` of Idle machines donor→requester. No cluster moves,
  no Configure interrupted.
- **TIER 2 — reassign speculative quota.** At age>5 when Tier 1 finds no
  idle donor. Find a donor with spare speculative quota for the matching
  `QuotaKey{Provider,Region}`. Emit `ReassignSpeculative` **and** commit
  `CmdSetQuota` through Raft so the moved slice survives failover (quota
  is authoritative Raft state; the soft instruction is the shard-side
  materialization, the Raft commit is the durable ledger).
- **TIER 3 — cross-shard preempt (expensive, rare).** ONLY at a HIGHER
  sustained-age threshold (~age>12) AND after Tiers 1–2 are exhausted
  fleet-wide for that shortfall AND with a strict cost gate: preemptor
  priority > donor victim priority AND victim
  `InterruptionPenaltyBucket` < requester's (interrupt cheap work for
  expensive demand, never the reverse). Emit `CrossShardDrain{selector,
  preemptor_priority}` to the donor; the donor reuses `executeDrain`
  (execute.go:413) via a `decision.Action{Kind: ActionKindPreempt}`,
  scoring its own victims with the existing Phase-2 victim score and
  deriving grace from preemptor_priority. When the donor's next summary
  shows the machine Idle, a subsequent pass emits the Tier-1
  `TransferOwnership`. **Drain-first is mandatory**; CrossShardDrain
  never moves ownership itself.

### 4. Machine-id sourcing — donor-resolves (author wire call)

The coordinator addresses a count + (instance-type, zone) selector to the
donor; the donor picks concrete Idle machine_ids honoring its own
reclamation_penalty idle-tiebreak (give up lowest-penalty Idle first) and
echoes them in its InstructAck / next report; the coordinator then issues
`TransferOwnership` naming those ids to both donor (from_shard_id) and
recipient (to_shard_id), each self-selecting its role by comparing
`s.ID()`. This keeps the §9 summary budget intact and preserves "shards
own capacity". It requires adding count+type+zone selector fields to the
instruction messages (or a two-phase offer/resolve handshake) — **author
must rule** (§Author decisions).

### 5. Shard handlers (pkg/shard, no coordinator import)

- `OnCrossShardDrain` → per machine_id, route through `executeDrain` as a
  `Preempt` action; mark the resulting Idle machine with a **Go-only
  `reservedForTransfer`** flag (TTL'd) so the donor's own Phase 3 cannot
  reclaim it before TransferOwnership lands.
- `OnTransferOwnership` → role-self-select; verify `State==Idle &&
  Cluster==""` first (else OUTCOME_FAILED — never force-drain inside a
  transfer); donor `inv.Remove(id)` + suppress re-adoption, recipient
  `inv.Insert(idle, unbound)`.
- `OnReassignSpeculative` → pure inv bookkeeping (Remove/Insert
  Speculative rows) mirroring the seed path.

All handlers idempotent against redelivery (the
executeBootstrap/executeDelete pattern); any error becomes OUTCOME_FAILED
and the coordinator resends.

### 6. Anti-oscillation (four layered, leader-local soft defenses)

1. **Age gate** (free hysteresis): age>5 (cheap tiers) / age>12 (preempt).
2. **Post-move cooldown**: per-(machine | (type,zone)-flow, shard-pair)
   timestamp set on every move; that unit is ineligible to move again
   (either direction) for N≥8 cycles. Kills B→A→B and quota ping-pong.
3. **Min-benefit + donor-surplus margin**: a move fires only if the donor
   retains margin after donating AND the move closes a meaningful
   fraction of the deficit. No oscillation around break-even.
4. **Demand-pull invariant**: instructions only ever go to a shard that
   reported a matching eligible shortfall, sized ≤ its deficit. Idle is
   never PUSHED. The arriving machine lands against real demand so the
   receiver's Phase 1 (before Phase 3) claims it the same cycle — it is
   never surplus. The donor-side `reservedForTransfer` flag closes the
   symmetric drain-then-reclaim hazard.

Cooldown/age state is soft (lost on failover); acceptable because a new
leader re-derives shortfalls from fresh reports and the age gate re-arms.
The cooldown duration must exceed the report cadence so within-leader
thrash is impossible.

### Fencing & failover

Each instruction is stamped `instr.CoordinatorTerm = c.RaftTerm()` +
`instr.SequenceNumber = FSM.NextSequence()` + a stable instruction_id
before `EnqueueInstruction`. The soft pending queue is leader-local and
lost on failover; the new leader re-derives from fresh reports under a
new term, and stale-leader instructions are rejected by the shard's term
high-water mark. Quota moves (Tier 2) survive via `CmdSetQuota`. The one
sharp edge — leader dies between CrossShardDrain and its follow-up
TransferOwnership — is bounded by the donor's `reservedForTransfer` TTL:
the marker self-clears and the machine returns to the donor's normal Idle
pool. Worst case is a wasted drain, never an orphan or double-adopt
(provider fencing fences the loser at Configure).

## Consequences

- **§9 realised** with no new instruction types and no new delivery
  machinery — the new code is the leader-side loop, three handler bodies,
  the `TopologyConstrained` propagation, and the selector wire field.
- **Preemption stays "expensive, rare" by construction** via the
  graduated age gate + strict priority/penalty-bucket cost gate; cheap
  non-destructive tiers carry the common case.
- **Static stability preserved by construction** — emission in
  coordinator, mechanical execution in shard, no new import;
  TestStaticStability_ShardDoesNotImportCoordinator stays green; a
  coordinator outage pauses rebalancing while the data plane runs.
- **Anti-oscillation is first-class**, attacking each enumerated thrash
  mode (B→A→B, quota ping-pong, drain-then-reclaim, escalation churn) and
  the ADR-0045/ADR-0052 cross-shard analog independently.
- **Cheap at 200 shards**: soft state unchanged (~240 B/shard summary +
  ≤100 shortfalls/shard); per-pass O(shards × shortfalls × donor-scan)
  ≈ low-millions of integer/map ops, sub-millisecond, every ~5 cycles,
  leader-only; no per-pass Raft except rare Tier-2 quota commits.
- **New tunable surface** (rebalance-interval, age>5/age>12, cooldown N,
  donor margin, min-benefit) must be helm-wired and validated against
  demand realism (ADR-0043) on the uber-* ladder before the thresholds
  are trusted.
- **Two preconditions gate end-to-end function** (not the loop): the
  donor-resolves wire field, and the ownership-partition source of truth
  (provider.List today returns all machines with no shard scoping and
  reconcileFull re-adopts everything, so `inv.Remove` on the donor is
  undone next reconcile). Until ownership-partition is chosen, the
  handlers are correct-but-inert and the loop + gates are testable in
  isolation.

## Alternatives considered

- **greedy-shortfall (per-shortfall, locally-greedy).** Serves the
  oldest-highest-priority shortfall first through the same idle→quota→
  preempt escalation, but with a weaker (non-global) donor view and a
  single age gate. Rejected: no global donor selection (can strand a
  donor that would better serve a younger shortfall) and it does not make
  preemption rare as strongly as a graduated gate.
- **periodic-global-pass (single age gate, all tiers).** The closest
  alternative and mechanically identical; rejected only because one age
  gate for all three tiers does not enforce "preemption expensive, rare"
  — the graduated age>12 gate + strict cost gate is the decisive
  difference the paper's language demands. Its global-best-donor framing
  is adopted into the chosen design.
- **Per-report inline emission inside ReportShard.** Rejected: a single
  shard's report gives no global donor view and races other shards' soft
  state, amplifying oscillation.
- **Coordinator names machine_ids directly** (shard reports per-machine
  Idle ids). Rejected: inflates the summary past the §9 ~240-byte/shard
  budget and breaks "shards own capacity"; donor-resolves is the chosen
  path.
- **Durable Raft "transfer intent"** for in-flight cross-shard moves.
  Rejected for v1: couples the loop to Apply latency for a window the
  `reservedForTransfer` TTL + re-derivation already bound to a wasted
  drain; revisit if wasted drains prove material at scale.
- **Remove the stubs** (don't build §9 rebalancing). Rejected by author
  2026-06-19.