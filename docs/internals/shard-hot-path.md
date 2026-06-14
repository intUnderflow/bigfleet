# The shard controller hot path

This is the code-level deep-dive on `pkg/shard` — Tier 2, the part of BigFleet that
actually decides and actuates. Read [`docs/architecture.md`](../architecture.md) first for
the two-tier shape and the Phase 1/2/3 summary, and [`docs/concepts.md`](../concepts.md) for
the vocabulary (Need, Profile, Shortfall, effective cost). This page goes underneath both:
how one cycle runs end to end, how actions execute *off* the cycle barrier, how the
operator-facing session multiplexes everything, how the shard reports to the coordinator
*without depending on it*, and exactly which invariants the code enforces and why. The
decision engine itself (`pkg/decision`, the Phase 1/2/3 algorithms) is covered in
[`decision-engine.md`](decision-engine.md) and is in scope here only as the cycle calls it;
this is about the controller that *drives* the engine. The papers are the final authority
(`docs/papers/bigfleet.md` §6–§11); where the code diverges — notably ADR-0003, now
superseded — this page flags it rather than papering over it.

---

## 1. The central invariant: `pkg/shard` must not import `pkg/coordinator`

Static stability is the load-bearing safety property of the whole system: **clusters keep
running with BigFleet entirely down, and shards run autonomously while the coordinator is
unreachable or mid-failover** (paper §6). The architectural rule that
makes that defensible is a single import constraint — the shard's hot path has no
compile-time path to the coordinator. There is then *nothing to block on* when the
coordinator is down, because there is no call from the cycle into coordinator code.

This is verified mechanically, not by convention. `pkg/shard/no_coordinator_dep_test.go`
imports `pkg/shard` via `go/build` and fails CI if any import — production *or* test — is
prefixed `github.com/intUnderflow/bigfleet/pkg/coordinator`
(`pkg/shard/no_coordinator_dep_test.go:24-33`). Confirmed against the live graph
(`go list`): `pkg/shard`'s only intra-repo imports are `conv`, `decision`, `decision/occ`,
`fencing`, `grpcutil`, `inventory`, `machine`, `metrics`, `needs`, `proto`, and `provider`
— no `coordinator`, no `coordclient`.

The dependency arrow points the *other* way, which is how the shard still gets coordinator
service without importing it:

```
                       imports
   pkg/shard/coordclient ───────▶ pkg/shard   (adapter.go: ViewFromShard(*shard.Shard))
            │                          ▲
            │ imports                   │ never imports coordclient or coordinator
            ▼                          │
   pkg/proto (Coordinator gRPC)    cmd/bigfleet wires both, in separate goroutines
```

`coordclient` lives in a *sub-package the hot path does not import*
(`pkg/shard/coordclient/coordclient.go:1-12`). It depends on `pkg/shard` (to read the
shard's summary and dispatch instructions back), never the reverse. The two are stitched
together at the binary boundary: `cmd/bigfleet/shard.go:788-802` constructs the coordclient
with `coordclient.ViewFromShard(sh)` and runs it in its own goroutine
(`go func() { errCh <- cc.Run(ctx) }()`), entirely separate from `sh.Run(ctx)` at
`shard.go:770`. An empty `--coordinator-addr` skips the coordclient goroutine altogether
(`cmd/bigfleet/shard.go:788`) and the shard runs cycles unchanged — single-shard / dev runs
need no coordinator at all.

The two readiness probes reinforce this. `/healthz` is "the HTTP server answers" and
`/readyz` is `sh.FirstReconcileDone()` — the first *provider* reconcile completing
(`cmd/bigfleet/shard.go:812-821`). Neither touches the coordinator, by explicit design: a
shard must come up alive and ready with zero coordinators reachable. `firstReconcileDone` is
latched (`atomic.Bool`, `shard.go:283`) so readiness never regresses on a later transient
provider error.

---

## 2. The cycle hot path (`shard.go`)

A shard is one process: one `needs.Table`, one `inventory.Inventory`, one
`fencing.CoordinatorTerm`, a map of operator sessions, and (under `Run`) a persistent pool
of execute workers (`Shard` struct, `shard.go:224-341`). `Run` (`shard.go:487-518`) is the
whole controller loop:

```
Run:
  spawn max(1, ExecuteConcurrency) executeWorker goroutines, draining actionQueue (ADR-0021)
  loop select:
      ctx.Done   → return
      ticker.C   → runCycle    (CycleInterval, default 10s)
      wakeup     → runCycle    (a rollup arrived; coalesced, buffer-1)
```

Cycles fire on a fixed interval *and* on a rollup-triggered wakeup. `triggerCycle`
(`shard.go:603-608`) is a non-blocking send into a buffer-1 channel, so a burst of rollups
collapses into at most one extra queued cycle — the shard reacts to new demand fast without
self-DoSing.

`runCycleCapturing` (`shard.go:627-1010`) is the body, shared by production (`runCycle`,
`shard.go:621`) and the simulator (`Step`, `shard.go:614-616`). One cycle, in order:

1. **Reconcile** inventory against the provider so the engine decides against a fresh view
   (`shard.go:648-656`; §3). Success latches `firstReconcileDone`.
2. **Snapshot** the inventory and NeedsTable (`shard.go:662-674`). `decision.NormalizeDemand`
   folds sub-machine `Same`-Needs into atomic plain aggregates *once*, against this cycle's
   snapshot, so Phase 1, Phase 2 (via Phase 1's residuals) and the attribution probe all
   reason over identical normalized demand (ADR-0041). `stampParkedNeeds` then marks aged-out
   structurally-unsatisfiable `Same`-gangs (ADR-0042 Addendum; §2.2).
3. **Phase 1 / 2 / 3** (`shard.go:676-703`). All three run on the *same* snapshot, so their
   actions are independent and execute-order between phases is irrelevant. Phase 3 consumes
   `p1.Claimed` — the single attribution — and is shrinkage-only (ADR-0045): it reclaims the
   Configured remainder demand left unclaimed, plus, since M73, emits Delete for Idle
   machines past their `CapacityType` hold (paper §8 release order). It runs no demand walk
   of its own, and is gated per-cluster on `FirstRollupReceived` (ADR-0036; §2.1).
4. **Emit AvailableCapacity** hints (`shard.go:766`; §7).
5. **Collect, rail-check, and enqueue** all actions (`shard.go:777-953`; §2.1 rails, §4 pool).
6. **Record shortfalls** from Phase 2's residual (`shard.go:958-962`; §6) and **emit metrics**.

The cycle never blocks on action execution. Phases compute and enqueue; the worker pool
drains. A slow operator handler delays a binding; it does not stall the cycle (ADR-0021, §4).

### 2.1 The three actuation safety rails (ADR-0046)

The rails live at the actuation/ingest boundary — `pkg/decision` stays pure, computing the
paper-faithful answer every cycle; the rails only govern what crosses from decision to
*execution*, and what an inbound rollup may do to the NeedsTable
(`safety.go:1-12`). None ever reorders priority. This placement is load-bearing: the phases
stay testable against the papers, and a rail can change *how fast* the engine's answer
executes, never *what* it is. The §16 tension ("priority is the sole throttling mechanism")
is resolved in the ADR: the rails bound *actuation volume*, not *allocation among claimants*.

| Rail | Where | What it does |
|------|-------|--------------|
| **1 — reclaim blast-radius cap** | `capReclaims`, `safety.go:150-193`, called at `shard.go:793-802` | Per cluster per cycle, at most `max(1, ⌊fraction × Configured⌋)` Reclaims execute (`reclaimCap`, `safety.go:133-139`); production default fraction `0.05` (`DefaultReclaimCapFraction`, `safety.go:37`). The kept reclaims are the head of the emission sequence — Phase 3's paper-§8 release order — so the cap defers the tail, never reorders. Surplus rolls over (Phase 3 re-derives next cycle) and deliberately does **not** trigger an immediate follow-up cycle, because the cap's job is to spread risk over wall-clock time. **Only `Reclaim` is capped**: Preempts are priority-driven allocation (§16-exempt), Bootstraps/Provisions are acquisitions. |
| **2 — empty-rollup quarantine** | `rollupGuard.admit`, `safety.go:100-126`, called from `ApplyRollup`, `shard.go:442-470` | A full-replacement rollup retaining `< 10%` of the cluster's previously *accepted* Need rows, when that baseline spanned `≥ 10` rows, is held until `3` consecutive rollups confirm the drop (constants `safety.go:42-59`). Quarantine, not reject — genuine mass scale-down proceeds after ~2 rollup intervals. The held rollup still clears the ADR-0036 first-rollup gate: the operator *did* report (`ApplyRollup` defers `markFirstRollupReceived` unconditionally, `shard.go:443`). |
| **3 — global kill switch** | `Config.ActuationPaused`, branch at `shard.go:827-843` | The cycle runs in full — reconcile, phases, shortfalls, AvailableCapacity, metrics, probes — but no action executes. Suppressed actions are counted per kind in `ShardActionsSuppressed` (kept out of `ShardActionsTotal`), logged, audited. Static config, flipped by redeploy, never by RPC — so the kill switch never depends on the coordinator being reachable. |

A fourth disposition, **dry-run / shadow mode** (`Config.DryRun`, branch at
`shard.go:844-866`), shares the kill switch's suppression point with the opposite posture:
the day-one adoption mode that *reports* every decided action (`ShardActionsDryRun`, Info
log, audit) without executing. When both are set, `ActuationPaused` wins the counting
(`shard.go:826-827` checks it first) — an emergency stop during a shadow run reads as a
pause, not a shadow. Rail 1 and `MaxActionsPerCycle` are skipped under pause/shadow
(`shard.go:793, 810`): nothing executes, so there is no drain rate or execute cost to bound,
and `MaxActionsPerCycle`'s deferral would otherwise busy-loop the wakeup channel re-deriving
a surplus that never drains.

Every executed, suppressed, or dry-run action lands in the decision audit log if configured
(`auditAction`, `safety.go:239-252`) — one JSONL record per disposition, carrying the
classified outcome. Note the audit record stamps `s.cycleCount.Load()`, which for an executed
action is the *executing* cycle and under the async pool (§4) can trail the deciding cycle.

### 2.2 Acquisition parking (ADR-0042 Addendum)

A structurally-unsatisfiable `Same`-gang (e.g. a Same-rack request larger than any rack the
shard owns) would otherwise churn Bootstrap/Reclaim forever — partially assemble, fail to
complete, reclaim the scatter, repeat. The shard tracks per-class consecutive-unsatisfiable
cycles in `unsatSameAge` (`recordUnsatSameAges`, `shard.go:1121-1143`), keyed by
`(cluster, group, fingerprint)` (`unsatSameKey`, `shard.go:1110-1112`). Faithful to
*concentrate-then-park*: a class only ages when it is unsatisfied **and acquired nothing this
cycle** — any forward progress (`Acquired > 0`) or the existence of a satisfiable bucket
(`SameSatisfiable`, a lost claim race, not structural) resets it. Past `parkAfterCycles` (8),
`stampParkedNeeds` (`shard.go:1150-1163`) marks the Need `AcquisitionParked` so both phases
make a creditable-only domain choice and stop driving actions; every `reprobeEveryCycles`
(32) the stamp lifts for one cycle so parked demand re-attempts and naturally un-parks the
moment supply appears (`shard.go:1101-1104`). The map is bounded by the live unsatisfied
set — entries not re-seen in a cycle are deleted, and only the cycle goroutine touches it
(no lock).

> The flag-gated phase-attribution probe (`PhaseAttributionLog`, `shard.go:705-758`) is the
> diagnostic instrument that found and now watches the `Same`-domain crediting defects
> across ADR-0040 → ADR-0051. It is read-only over the cycle's own snapshot + phase results,
> default-off, zero hot-path cost when off. See [`domain-attribution.md`](domain-attribution.md)
> for the saga it instruments.

---

## 3. Reconciliation: `List + Get`, no `Watch` (`reconcile.go`)

There is no `Watch` provider RPC by design — only the six (`Create, Configure, Drain, Delete,
Get, List`). Reconciliation is a `List` per cycle, run *before* the decision phases so the
engine never decides against a stale inventory (`reconcile`, `reconcile.go:29-34`, called at
`shard.go:649`). Two modes, gated on `Config.IncrementalReconcile`:

- **Full (default, correct for any provider)** — `reconcileFull` (`reconcile.go:39-62`):
  `provider.List` with an empty filter, apply each returned machine, then walk the inventory
  snapshot to find machines the provider no longer reports and `Remove` them. Linear in
  inventory size.
- **Incremental (above the §10.6 conformance threshold)** — `reconcileIncremental`
  (`reconcile.go:69-81`): pumps the opaque `SinceRevision` cursor (`reconcileCursor`,
  read/written only by the cycle goroutine, so no lock — `shard.go:261`) and processes only
  deltas. O(k changes) instead of O(N). The removal snapshot-walk is skipped — deltas don't
  yet communicate "this id is gone" (tombstones are deferred until a real provider needs
  them). A fresh cursor (cold start, shard restart) returns the full inventory, same as the
  full path. This matters because the M11.21 phase dump showed reconcile is ~85% of the cycle
  at 500K (`reconcile.go:64-68`).

`applyReconciledMachine` (`reconcile.go:116-165`) is where a provider machine merges into
inventory, and it carries three load-bearing subtleties, checked **in this order**:

1. **Skip in-flight machines** (`reconcile.go:129-131`). If `isPending(id)` — a worker is
   mid-RPC on this machine (§4) — the provider's `List` view *lags* the in-flight RPC and
   would overwrite the worker's authoritative transitional state (e.g. flip
   `Configuring → Idle` under a running `Configure`, causing the post-Configure transition to
   fail with "invalid transition Idle → Configured"). `bigfleet-uber #23` root-caused 17–26%
   of Provision failures to exactly this race.
2. **State-match fast path** (`reconcile.go:132-135`). If local state already equals the
   provider's, do nothing — no proto round-trip, no `Apply`, no allocation, no Invariant
   screen. This is the common case at steady state and *especially* the cycle right after
   execute fans out (execute already locally `applyTransition`-ed each machine to its ack
   state).
3. **Shard memory is authoritative for tracked machines** (`reconcile.go:139-147`). On a
   genuine state divergence the locally-known `Assigned*` fields (priority, the two penalties,
   the Need fingerprint, the gang group) are preserved — the provider doesn't own assignment
   state. For a machine with *no* local record (genuinely new, or being rebuilt after a
   process restart wiped memory), the provider-echoed `shard_metadata` is decoded to restore
   the assignment state (`DecodeShardMetadata`, `reconcile.go:159-164`; M72 — see §4/§5).

Slow-path records are screened by `validateProviderMachine` (`safety.go:204-213`) before they
touch inventory — `machine.Invariant` bounds the provider-declared cost-formula inputs
(`price ≥ 0`, `interruption_probability ∈ [0,1]`; the locked formula is
`effective_cost = price + interruption_probability × interruption_penalty`). A rejected record
is logged, counted (`ShardMachinesRejected`, with a `price` / `interruption_probability` /
`structural` label from `machineRejectReason`), and the inventory keeps its last-known-good
state. Because `reconcileFull` marks the id *seen* first (`reconcile.go:47`), a rejection
never masquerades as a removal (ADR-0046 addendum / M70). The state-match fast path is *not*
screened: it ingests no fields.

### Divergence: ADR-0003 is superseded

ADR-0003 ("shard inventory snapshots are eventually consistent on the cycle hot path")
describes a background fold goroutine and a `CycleSnapshot()` API returning a stale, O(1)
cached snapshot — chosen for its O(N) build cost (≈700 ms at 500K on M5). **The code no longer
works this way.** The cycle calls the synchronous, fresh `s.inv.Snapshot()` (`shard.go:663`),
and the ADR's own Status line records it: *"Superseded by M44.4 Drop A — the shard cycle
switched to synchronous `Snapshot()` … fold goroutine and live triple-indexes removed at
M66.1."* The reason: at real write rates, stale snapshots caused ~50% wasted Bootstraps for
already-Configured machines (`shard.go:658-661`). Per the source-of-truth order, treat the
ADR as historical context for *why eventual consistency was considered safe* (every phase
action is idempotent against an unchanged snapshot, and `applyTransition` rejects illegal
re-attempts), not as a description of current behaviour. The cost that motivated it was
instead reclaimed by the incremental-reconcile path above and by inventory's own pre-built
per-bucket indexes.

---

## 4. The persistent execute pool and actuation (`execute.go`, ADR-0021)

The cycle does not execute actions inline and does not `wg.Wait` on them. `Run` spawns
`max(1, ExecuteConcurrency)` `executeWorker` goroutines once at start (`shard.go:500-503`);
each drains the shared `actionQueue` (sized `2 × concurrency`, `shard.go:500`) and derives
its **own per-action context** capped by `ExecuteTimeout` (default 30s), independent of any
cycle deadline (`executeWorker`, `shard.go:525-549`). This is the ADR-0021 decoupling:
pre-change the cycle wall-clock was `max(action_latency)` because the cycle ctx was the
parent of every action and the cycle waited for the slowest worker — a slow operator handler
dragged the whole cycle and, worse, cycle-ctx cancellation cascaded into machine state.
Post-change, throughput is `concurrency / mean(latency)`, and a slow handler delays one
binding. (The scaleway-50k chain in the ADR sat at a ~25 binds/sec ceiling — below the
~41 CRs/sec churn rate — purely from the per-cycle batch barrier.)

The decoupling forces three correctness guards against stale/duplicate dispatch, because an
action emitted in cycle T may run in cycle T+5:

- **`pendingActions` ledger** (`pendingMu`, `shard.go:318-319`). A per-machine in-flight set,
  consulted at enqueue (`shard.go:905-912`): a machine already pending drops the duplicate.
  Workers clear the entry when execute returns, success *or* failure (`shard.go:543-545`) — a
  failed action re-derives cleanly next cycle. Also read by `isPending` during reconcile (§3).
- **`actionStillApplicable`** (`shard.go:583-599`), the "Drop K" live-state guard at *emit*
  time. The `pendingActions` ledger dedups while a worker is *mid-flight* but not when one
  just *finished* between the cycle's snapshot read and this emit. So at enqueue, re-check
  live inventory: a Bootstrap needs `Idle`, Provision needs `Speculative`, Reclaim/Preempt
  need `Configured`, Delete needs `Idle`; otherwise the action is moot (already done, or
  invalidated by a competing transition) and is skipped (`ShardActionsDeduped`). It fails
  *open* on an `inv.Get` error, matching pre-Drop-K behaviour and leaving the worker's own
  state check as the final authority. At 50K-Pod cloud scale this prevented 39/sec wasted
  dispatches.
- **Idempotent re-application inside executors** ("Drop J", `executeBootstrap`,
  `execute.go:268-272`): a machine already `Configured` for the same cluster + fingerprint the
  action targets is a no-op success — the desired end state already holds. Only a *different*
  cluster/fingerprint is a real conflict the state machine refuses.

If the queue is full (workers can't keep up), the new emission is dropped and re-derives next
cycle (`shard.go:919-925`, `ShardActionsDropped`) — fall-through, not blocking. `Step`/cycles
called outside `Run` (simulator, tests) have a nil `actionQueue`, so they fall back to inline
serial execute (`shard.go:934-942`); production never hits this.

### The state machine each executor walks

Machines have eight states — three **stable** (Speculative, Idle, Configured), four
**transitional** (Creating, Configuring, Draining, Deleting), and **Failed**
(`pkg/machine/machine.go:32-41`; only stable states are allocatable, `IsStable`,
`machine.go:69-71`). `execute` (`execute.go:45-83`) dispatches by `Action.Kind`; each handler
walks transitional states and calls the matching provider RPC, using `applyTransition`
(`shard.go:1399-1440`) which validates against the legal-transition table
(`validTransitions`, `machine.go:235-247`) and emits a `NodeStateUpdate` (§5) on every change.

```
Provision (Speculative→Configured):  Speculative ─Create─▶ Creating ─▶ Idle ─(hand off to Bootstrap)
Bootstrap (Idle→Configured):         Idle ─▶ Configuring ─[blob fetch]─Configure─▶ Configured
Reclaim/Preempt (Configured→Idle):   Configured ─[notify operator]─▶ Draining ─Drain─▶ Idle
Delete  (Idle→Speculative, M73 §7):  Idle ─▶ Deleting ─Delete─▶ Speculative
```

- **`executeProvision`** (`execute.go:181-235`): `Speculative → Creating`, `provider.Create`,
  reflect the ack's host/profile/price/interruption into inventory. The Create ack is where
  provider-declared cost-formula inputs enter; it is `validateProviderMachine`-screened, and
  garbage drives the machine to Failed (`execute.go:216-221`). Hand off to Bootstrap if Create
  reached Idle.
- **`executeBootstrap`** (`execute.go:242-399`): `Idle → Configuring`, stamping the
  destination cluster + Need fingerprint + gang group **early** (`execute.go:286-294`) so
  Phase 1 counts the in-flight machine as supply for its own Need — without this, Phase 1
  over-emits Bootstraps on fresh Idle machines for the same fingerprint ("Drop G": 318K of
  522K emits were dedup-skipped). Then it pulls a kubelet bootstrap blob — via the operator's
  `Shard.Session` stream (`sess.requestBootstrap`, production) or the `LocalBootstrap` hook
  (simulator/test) — calls `provider.Configure`, walks `Configuring → Configured`, and stamps
  priority + both penalties for Phase 2 victim scoring (`execute.go:363-372`). The `Configure`
  request also carries `EncodeShardMetadata(priority, intPen, recPen, fingerprint, group)`
  (`execute.go:352`): Configure is the *only* writer of the provider-side echo, so a restart
  can rebuild protection state from `List` (§3). A blob-fetch *timeout* rolls back to Idle for
  retry; a real failure goes Failed (`handleBootstrapBlobErr`, `execute.go:94-105`) — the
  distinction matters because the blob fetch happens *before* `provider.Configure`, so
  rollback leaks no provider state.
- **`executeDrain`** (`execute.go:405-463`) handles both Reclaim (Phase 3) and Preempt
  (Phase 2): notify the operator first (`sendReclaimInstruction`) so the cordon +
  PDB-respecting eviction runs ahead of the provider drain (ADR-0009), then
  `Configured → Draining`, `provider.Drain`, `→ Idle`, clearing all `Assigned*` fields. On a
  voluntary Reclaim `PreemptorPriority` is 0 — there is no preemptor (`execute.go:415-427`);
  if no session is connected the provider drain still runs (kubelet default grace) but the
  skipped PDB pass gets its own alertable log line.
- **`executeDelete`** (`execute.go:475-516`): the M73 paper-§7 idle release — `Idle → Deleting`,
  `provider.Delete`, `→ Speculative` (the host is released entirely; only the quota slot
  survives, `ShardIdleReleases`). A `Delete` rejection (incl. `ErrNotSupported`) means the
  provider's `CapacityType` declaration and its Delete support disagree — a contract
  violation, so the machine goes Failed, not retried.

`classifyExecuteError` (`execute.go:114-176`) maps every return path to a bounded outcome
label for `ShardActionExecuteOutcomes`. Two are worth knowing: `transition_raced_to_idle`
(`errProvisionRacedToIdle`, `execute.go:38`) is the benign Configuring→Idle race that recovers
next cycle, kept distinct so alerting on real `transition_error` stays meaningful; and
`fenced` (`outcomeFenced`, `execute.go:110`, matched via `provider.ErrFenced` *before* the
message-string buckets so it can't be miscounted as `provider_error`) is a provider rejecting
our fencing token (paper §11) — logged at Error as a zombie-shard incident, *do not retry*.

---

## 5. The operator-facing session (`session.go`)

Operators are **outbound-only**: each dials the shard and holds one bidirectional
`Shard.Session` stream; the shard has no inbound listener on the operator and never opens an
outbound connection to a cluster. *All* cluster ↔ shard traffic multiplexes on that one
stream. `Session` (`session.go:36-108`) is the server side.

- **First frame is `Hello`** (`session.go:38-46`), carrying the `cluster_id`. On an mTLS
  transport the client certificate's URI SAN must match `ClusterURI(cluster_id)` (ADR-0048,
  `session.go:54-63`) — otherwise `Hello.cluster_id` is a free-text impersonation vector
  (receive another cluster's reclaim instructions, or zero its capacity with a forged
  full-replacement rollup; `ShardSessionIdentityRejected` counts failures). Plaintext
  transports skip the check; identity is only as strong as the transport.
- **At most one session per cluster.** A new connection replaces and closes the prior one
  (`installSession`, `session.go:147-158`); `removeSession` only deletes the map entry if it
  still points at *this* session (`session.go:162-178`), so a replaced session's deferred
  cleanup can't evict its successor.
- **Split recv pump from handling** (`session.go:94-108`; `routeOperatorMessage`,
  `session.go:119-143`). The recv loop only routes; *fast* paths (BootstrapResponse,
  ReclaimAck, Hello-ack — channel sends / trivial Sends) run inline; the *slow* path (Rollup
  — decode + `needs.Replace` over full demand + cycle trigger) is handed to a per-session
  goroutine via `enqueueRollup`. This is M44.4 Drop B: a slow rollup running inline used to
  block delivery of the very BootstrapBlobResponses an in-flight `executeBootstrap` was
  waiting on, pushing machines to Failed for an orchestration timeout unrelated to them.
  Per-session ordering is preserved within each lane (one rollup goroutine); cross-lane
  ordering is intentionally relaxed.

**Rollup ingest** (`rollupWorker`, `session.go:250-291`). The rollup channel is buffer-1
because rollups are *full replacement* (paper §3.1): only the latest matters, so a newer
arrival supersedes a queued one (`enqueueRollup` drains-and-replaces, `session.go:226-244`).
The worker decodes (`conv.NeedsFromRollup`; a malformed rollup is rejected loudly,
`ShardRollupsRejected`, and the NeedsTable keeps last-known-good demand — M68b,
`session.go:262-272`), then `sh.ApplyRollup` (`shard.go:442-470`) replaces the cluster's
NeedsTable slice and marks the ADR-0036 first-rollup gate. `ApplyRollup` is the *single*
ingest point — both the production session path and the sim/test runners flow through it, so
rail 2 (empty-rollup quarantine) has one implementation and one test surface. A held rollup
still marks first-rollup but returns `false`, so the cycle isn't triggered and
`demandObservedAt` keeps tracking the *accepted* fingerprints, not the held ones
(`session.go:280-283`).

**Outbound frames** ride the same stream:

- `requestBootstrap` (`session.go:317-343`): sends a `BootstrapRequest` with a minted
  crypto-random `request_id`, registers a buffer-1 response channel, and blocks on it (or
  ctx) until the matching `BootstrapBlobResponse` arrives (`deliverBootstrapResponse` routes
  by id; late responses drop silently, `session.go:348-361`).
- `sendReclaimInstruction` (`session.go:367-381`): fire-and-forget the drain instruction; ack
  tracking is best-effort telemetry only.
- `SendNodeStateUpdate` / `SendAvailableCapacityUpdate` (`session.go:403-417`): coalescing
  frames whose `supersedes_key` (`node:<id>` / `available:<fingerprint>`) lets the operator
  drop stale queued frames safely on reconnect.

### NodeStateUpdate carries node identity (ADR-0016)

`notifyNodeState` (`shard.go:1449-1505`) fires from `applyTransition` on every state change of
a cluster-bound machine. Beyond `(machine_id, state, provider_id, last_error)`, it carries the
node's *shape* — labels (including the synthesised `node.kubernetes.io/instance-type` and
`topology.kubernetes.io/zone`) and per-machine **Allocatable** resources — so the operator can
populate `UpcomingNode.Spec` with what the kubelet will register (ADR-0016,
`shard.go:1471-1501`). Empty values are valid for transitional states with no bound host. Two
details earn comments in the code:

- It sends `EffectiveAllocatable()` (per-machine capacity), not `Profile.Resources`
  (per-replica request shape), so density>1 bin-packing works (ADR-0022/M45.5,
  `shard.go:1489-1501`). `EffectiveAllocatable` falls back to `Profile.Resources` for
  pre-M45 inventory, keeping the historical 1-Pod-per-machine math.
- The bound cluster is captured *before* the mutating callback runs (`prevCluster`,
  `shard.go:1420`), because a terminal `Draining→Idle` transition clears `Machine.Cluster`
  and the update must still route to the cluster whose `UpcomingNode` GC keys on it (M68b);
  otherwise every drained machine leaked a stale Draining-phase `UpcomingNode`.

Delivery is best-effort — a missing or disconnected session is silently skipped; the operator
reconciles from full state on reconnect.

---

## 6. Reporting to the coordinator (`report.go`) and the off-hot-path client (`coordclient/`)

The shard's *contribution* to coordinator state is built by pure read methods in `report.go`,
which import nothing from coordclient or coordinator — they are plain accessors over the
shard's own state:

- **`Summary`** (`report.go:23-43`): a coarse inventory snapshot (total / free machines,
  per-instance-type and per-zone counts). Bounded; not the per-machine inventory.
- **`Shortfalls`** (`report.go:70-94`): the unresolved demand this shard couldn't satisfy
  from its own inventory *and* couldn't resolve via in-shard preemption — sorted by priority
  desc (tiebreak: older first) and bounded to the top 100 (paper §3.5 shortfall protocol
  bound). Shortfalls are a *resource vector* (`Deficit`), not a machine count (ADR-0027).
  `recordShortfalls` (`report.go:114-148`) ages entries per cycle and drops those no longer
  present (eventually-consistent). Distinct unresolved Needs sharing a Profile fingerprint —
  several parked gangs of one shape differing only by co-location Group — are summed per
  fingerprint (resource-vector add) and aged *once* per fingerprint per cycle (M68); the
  ledger stays fingerprint-keyed because its only consumer, the coordinator, seeks supply by
  Profile and can't act on per-gang identity.

Topology constraints never cross shard boundaries: a `Same`-rack request unsatisfiable
in-shard becomes a shortfall and is reported up; it is *never* resolved cross-shard (a BigFleet
hard rule). The coordinator may rebalance *supply* between shards, but it does not resolve one
shard's topology constraint using another's machines.

**`coordclient`** (`coordclient/coordclient.go`) is the only shard-side code that knows the
coordinator exists, and it runs in its own goroutine off the hot path. `Client.Run`
(`coordclient.go:157-187`) dials the coordinator (mTLS per ADR-0048, with a shard URI SAN),
fires one `ReportShard` immediately, then ticks every `ReportInterval` (default 30s, per
paper). Each `runOnce` (`coordclient.go:191-235`) builds a `ShardReport` from the `ShardView`
(the `coordclient.ViewFromShard` adapter delegating to `*shard.Shard`, `adapter.go:14-59`),
sends it, validates the returned `coordinator_term`, and dispatches any returned instructions
— **rebalance instructions ride back on the shard-pulled report**; there is no inbound RPC to
the shard. Static stability is explicit in the failure handling: a `ReportShard` error
re-queues the pending acks and returns; the shard's data plane continues against existing
allocations regardless (`coordclient.go:212-220`).

### Shard self-registration via heartbeat (ADR-0006)

There is **no registration RPC**. The `ShardReport` carries `ShardAddress` (= the
configured `AdvertiseAddress`, `coordclient.go:200-209`); on the first report from an unknown
`shard_id` the coordinator Raft-Applies `AddShard{ID, Address}` itself, then subsequent
reports take the cheap heartbeat path. One coordinator RPC for the whole shard lifecycle —
the shard already pulls instructions via `ReportShard`, so a separate `Register` would mean
two coordinator interactions where one suffices (ADR-0006). Registration succeeding or not is
bookkeeping that lets domain assignments and admin tooling see the shard; the shard runs
cycles either way.

Inbound instructions are fenced and idempotent (`handleInstruction`, `coordclient.go:242-291`):
each is deduped against `doneInstructions` (a redelivery just re-acks, `ACCEPTED`), validated
against the `CoordinatorTerm` high-water mark (stale → `REJECTED_STALE`), then dispatched to a
`ShardView` handler. `AssignDomain`/`UnassignDomain` mutate the shard's `assignedDomains` set
(`report.go:158-190`; empty = "take everything", dev/single-shard mode);
`ReassignSpeculative`/`CrossShardDrain`/`TransferOwnership` are M6.3 stubs awaiting the
rebalancing logic (`adapter.go:70-74`). The coordinator may redeliver the same instruction
across reports until it sees an ack — hence the dedup ledger and the re-queue-on-error path.

---

## 7. AvailableCapacity reporting (`available_capacity.go`)

Once per cycle, after Phase 1/2/3, `emitAvailableCapacity` (`available_capacity.go:130-164`)
pushes one `AvailableCapacityUpdate` per `(cluster, distinct demanded profile fingerprint)`
down the matching operator session — a paper §6.2 *eventually-consistent* hint the operator
surfaces as an `AvailableCapacity` CR. Emission is keyed on the cluster's *demanded* shapes
only (not every profile inventory could match), so CR cardinality on the cluster apiserver is
bounded by the cluster's distinct workload shapes, not by inventory size.

The hint is cheap to compute — `buildAvailableCapacityUpdate` (`available_capacity.go:170-210`)
reads pre-built per-`(state, instance-type)` snapshot buckets (each `Count` is O(1)), no
per-machine walk — and reports a confidence ladder: **HIGH** if an Idle machine matches (no
wait on Create), **MEDIUM** if only a Speculative slot matches, **NONE** otherwise. Per
ADR-0027 a Profile carries no single resource shape, so the hint carries
requirements + count + cost + confidence, not a `Resources` value.

The churn control is `availableCapacityCache` (`available_capacity.go:36-104`), two layers:
**coalesce** (skip if the `(count, confidence, cost)` tuple matches the last emit) and
**rate-limit** (at most one emit per `minInterval`, default 5s, per `(cluster, fingerprint)`).
Without it, a 50-cluster harness at ~10 Hz cycles would emit ~500 AC writes/sec into the
operator-side kine sqlite — dwarfing the load AC itself describes. The cache survives
reconnects per-cluster: `removeSession` calls `acCache.forget(cluster)` (`session.go:175-177`)
so a reconnecting operator that lost its CRs gets a fresh emit, while other clusters keep
their dedup state (no fleet-wide re-emit storm).

---

## 8. Provisioning latency (`provisioning_latency.go`)

Paper §10.7 end-to-end provisioning latency = time from a demand first appearing in a rollup
to a machine reaching Configured for it. `observeRolledUpDemand` (`provisioning_latency.go:15-40`)
stamps the first-seen time for each `(cluster, profile fingerprint)` at rollup ingest (called
from `rollupWorker`, `session.go:281`) and prunes fingerprints no longer demanded.
`observeProvisioningLatency` (`provisioning_latency.go:58-67`), called from the Configure
success path (`execute.go:395-397`), records `time.Since(first-observed)` into
`ShardProvisioningLatency` — then **deletes** the tracking entry (observe-once). The delete is
load-bearing: without it, every Configure during a 30-minute soak measured ~the soak duration
as latency, saturating the histogram's `+Inf` bucket and pinning the metric to a constant
327.68s. `observeRolledUpDemand` re-inserts the entry on the next rollup that still demands
the fingerprint, so the next sample measures the latency for the freshly re-observed demand.
Best-effort — silently skipped when the fingerprint isn't tracked (e.g. an action from
Phase 2/3 paths that aren't gated on rolled-up demand).

---

## 9. Where to read next

- [`decision-engine.md`](decision-engine.md) — the Phase 1/2/3 algorithms the cycle calls;
  effective cost, victim score, drain grace, the `Claimed`/`Unsatisfied`/`SatisfiedGangs`
  results this controller consumes.
- [`machine-lifecycle.md`](machine-lifecycle.md) — the eight-state machine and the legal
  transitions `applyTransition` enforces.
- [`static-stability.md`](static-stability.md) — the design class the no-coordinator-import
  rule rules out, end to end.
- [`wire-protocols.md`](wire-protocols.md) / [`provider-protocol.md`](provider-protocol.md) —
  the `Shard.Session` stream, the six provider RPCs, `supersedes_key` coalescing.
- [`coordinator-raft.md`](coordinator-raft.md) — the other end of `coordclient`: the Raft FSM,
  `AddShard`, domain assignment.
- ADRs: [0003](../adr/0003-shard-snapshot-eventual-consistency-on-the-cycle-hot-path.md)
  (superseded — read for the eventual-consistency rationale),
  [0006](../adr/0006-shard-self-registers-via-heartbeat.md),
  [0016](../adr/0016-nodestateupdate-carries-node-identity.md),
  [0021](../adr/0021-persistent-execute-pool.md),
  [0036](../adr/0036-phase3-gated-by-first-rollup.md),
  [0045](../adr/0045-consumed-capacity-in-the-attribution-model.md),
  [0046](../adr/0046-actuation-safety-rails.md).
</content>
