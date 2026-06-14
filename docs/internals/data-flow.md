# End-to-end data flow: from unschedulable pod to bound node

This is the cross-cutting trace. Every other internals page picks one subsystem and goes deep;
this one follows a single datum — one pod's worth of demand — across every component boundary
it crosses, from the moment a `Pod` lands in a managed cluster to the moment a kubelet registers
on a node BigFleet provisioned, and back around for the reclaim that releases it. It exists to
answer the onboarding question the per-subsystem pages assume you already have: *where does each
fact live, who owns it, and what is the timing budget between the hops?* Read
[`../architecture.md`](../architecture.md) for the shape first; read
[`decision-engine.md`](decision-engine.md) and [`shard-hot-path.md`](shard-hot-path.md) after this
for the function-by-function interior of the one box (the shard cycle) this page treats as a unit.
The design rationale is the two papers — *BigFleet* and *Fleet-Scale Kubernetes* (vendored under
[`../papers/`](../papers/)); the timing posture is [ADR-0014](../adr/0014-slo-posture-binding-latency-not-cycle-wall-clock.md)
/ [ADR-0018](../adr/0018-internal-vs-user-facing-binding-latency.md) / [ADR-0020](../adr/0020-internal-binding-latency-slo-respects-rollup-interval.md).

## The whole chain, at altitude

![End-to-end flow across three lanes — Kubernetes cluster, the autonomous BigFleet shard, and the out-of-tree provider: a pod becomes a CapacityRequest, the operator rolls up demand over the Shard.Session stream, the shard's cycle runs Phase 1/2/3 and drives provider Create/Configure, and node state plus reclaim instructions flow back.](./data-flow-diagram.svg)

Three ownership boundaries, never crossed the wrong way:

- **The operator dials out.** There is no inbound listener on the cluster side. Every byte of
  cluster↔shard traffic rides the one bidirectional `Shard.Session` stream the operator opens
  (`pkg/operator/stream.go:39`, `api/proto/.../shard.proto:22`). The shard *cannot* call the
  operator; it pushes frames down the stream the operator is holding open.
- **The shard is autonomous.** Nothing in this chain touches the coordinator. `pkg/shard` does not
  import `pkg/coordinator` (guarded by `pkg/shard/no_coordinator_dep_test.go`); the cycle reconciles,
  decides, and actuates against provider + operator alone. Clusters keep running with the whole
  control plane down.
- **The provider is the only actuator.** Six RPCs, all out-of-tree
  (`api/proto/.../provider.proto:47`). The shard never touches a cloud API directly.

The rest of this page walks each hop, naming the datum, the type, the package, and the timing point.

## Hop 1 — Pod → CapacityRequest CR (one per Pod, not per *unschedulable* Pod)

Demand enters BigFleet's world as a `CapacityRequest` CR in the managed cluster. The reference
producer is the optional `bigfleet-unschedulable-pod-controller` (`pkg/controller/cr/controller.go`),
which watches Pods and creates **one CR per Pod, owner-referenced to the Pod** so deleting the Pod
garbage-collects the CR (`controller.go:1`). The name is a historical misnomer:
[ADR-0039](../adr/0039-capacityrequest-per-pod-not-per-unschedulable-pod.md) changed the controller
from "CR per Pod seen `Unschedulable`" to "CR per Pod, full stop." The reason is asymmetric and
load-bearing: Phase 1 (Bootstrap) only needs the *unmet* fraction of demand and was correct on an
`Unschedulable` filter, but **Phase 3 (Reclaim) needs *total* demand** — with CRs undercounting
total demand ~6× (the #45–#48 cascade measured ~84% of bound Pods carrying no CR), Phase 3 saw a
permanent phantom surplus and reclaimed into it every cycle. One CR per running Pod is what makes
the roll-up the cluster's *complete* desired state, which is what both phases read.

This controller is *optional*. Any source — an admission webhook, Kueue, a bespoke operator — can
write the CRs; BigFleet only consumes them (`controller.go:14`). Penalties are declared here, by
Pod annotation or per-`PriorityClass` default (`controller.go:62`): `interruption-penalty` and
`reclamation-penalty`, two distinct dollar values that survive all the way to victim scoring.

**Where the datum lives:** `CapacityRequest` CRs in the cluster apiserver. One per Pod. `Spec`
carries the Pod's resource request, node-selector requirements (standard operators only — `In`,
`NotIn`, `Exists`, `DoesNotExist`), `PriorityClass` value, topology spread, an optional co-location
term, and the two raw penalties.

## Hop 2 — CRs → ClusterCapacityNeeds roll-up (full replacement, aggregated)

The operator's `rollupLoop` fires every `RollupInterval` (default **10 s**,
`pkg/operator/operator.go:137`; one immediate fire on connect, `rollup.go:35`). Each tick:

1. **List every CR** in the cluster (`rollup.go:121`).
2. **Aggregate** by `(Profile fingerprint, co-location group)` via `buildRollup` (`rollup.go:151`).
   Under [ADR-0027](../adr/0027-rollup-demand-is-a-constrained-resource-request.md) each CR
   contributes one Pod's worth of demand: its resource request is both the unit summed into
   `aggregate_resources` and the atomic `min_unit` that must fit on one machine. **No Pod count
   crosses the wire** — machine count is the autoscaler's output, never the cluster's input
   (`rollup.go:129`, `capacity.proto:60` reserves the removed `count` field). This is why the
   wire message stays ~2 KB regardless of fleet size: a million identical Pods collapse to one
   `CapacityNeed` whose `aggregate_resources` is their vector sum.
3. **Translate co-location to `Same`.** A CR's co-location term (the canonical projection of its
   source Pod's required `podAffinity`, [ADR-0024](../adr/0024-co-location-via-podaffinity.md)) is
   canonicalised into a stable group key (`coLocationGroup`, `rollup.go:199`;
   `canonicalLabelSelector`, `rollup.go:212`). CRs with an equal term aggregate into one Need and
   the operator appends a protobuf-only `Same` requirement on the term's topology key
   (`withSameRequirement`, `rollup.go:258`). **`Same` exists only on the wire** — the CRD carries
   only standard `core/v1` operators (`requirementOperatorFromCore`, `rollup.go:331`, maps exactly
   those four), and the operator is the only place a `Same` requirement is ever minted, from the
   co-location term. CRs with different terms stay separate even when their profiles are identical,
   so independent gangs each get their own domain.
4. **Bucket the penalties.** Raw dollar values become powers-of-2 `PenaltyBucket`s here — the
   operator is the canonical place this happens (`profileFromCapacityRequest`, `rollup.go:316`;
   boundaries `capacity.proto:136`). Bucketing bounds the roll-up size when penalties are
   workload-specific; the cost-function error is dwarfed by the bucket resolution.

The output is one `ClusterCapacityNeeds` (`capacity.proto:20`): `cluster_id`, a timestamp, and the
aggregated `[]CapacityNeed`. **Every roll-up is a full replacement of the cluster's desired state**
(`capacity.proto:5`, paper §3.1). Withdrawal is implicit — a CR no longer present is simply absent
from the next roll-up. This is the single most important invariant of the demand path: the receiver
never diffs partial updates; it replaces wholesale.

After building, the operator marks the included `Pending` CRs `Acknowledged` — a one-way status
transition, written once per CR ever (`markAcknowledged`, `rollup.go:413`, via JSON merge-patch to
avoid resource-version conflicts under burst).

**Timing point:** a Pod that arrives just after a tick waits up to one full `RollupInterval` before
the shard sees its CR. The 10 s default is therefore a hard floor on internal binding latency —
[ADR-0020](../adr/0020-internal-binding-latency-slo-respects-rollup-interval.md) sizes the internal
SLO at `rollupInterval + 5 s` for exactly this reason and treats 10 s as a deliberate production
posture (10× fewer messages and aggregation passes than 1 s, invisible next to real-provider
create time).

## Hop 3 — roll-up → shard NeedsTable, over the one stream

The operator enqueues the roll-up onto its send path. The send path has two queues with different
drop policies (`pkg/operator/stream.go:87`):

- **`pendingRollup`** — a single-slot `atomic.Pointer`. Because roll-ups are full replacement,
  enqueuing a fresh one atomically *replaces and drops* any older pending one (`enqueueRollup`,
  `stream.go:182`). Coalesce-by-replace: the operator never queues two roll-ups behind a slow
  stream.
- **`outbox`** — a bounded channel (cap 256) for non-coalescing RPC responses
  (`BootstrapBlobResponse`, `ReclaimAck`). Drops newest with a metric when full, because those are
  responses to a shard request the shard will re-issue on timeout (`enqueue`, `stream.go:195`).

The session was opened by `runOnce` (`stream.go:20`): dial with mTLS if configured
([ADR-0048](../adr/0048-mtls-and-uri-san-identity.md)), send `Hello` with the `cluster_id`
(`stream.go:45`), then run three goroutines — `sendLoop`, `recvLoop`, `rollupLoop` — any one of
which returning aborts the others and triggers a reconnect with exponential backoff
(`operator.go:164`, `nextBackoff`).

On the shard side, `Session` (`pkg/shard/session.go:36`) validates the `Hello`: on an mTLS
transport the client certificate's URI SAN must assert the claimed `cluster_id`, or the session is
terminated `PermissionDenied` (`session.go:54`) — otherwise `Hello.cluster_id` is a free-text
impersonation vector that could zero another cluster's capacity with a forged full-replacement
roll-up. At most one session per cluster: a new connection replaces and closes the prior one
(`installSession`, `session.go:147`).

Inbound frames are routed by `routeOperatorMessage` (`session.go:119`). The split here is the
M44.4 Drop B fix: **fast paths stay inline, slow paths go to a per-session worker.** A roll-up is
the slow path (`NeedsFromRollup` decode + `NeedsTable.Replace` + cycle trigger), so it is handed to
`rollupWorker` via a buffer-1 channel (`enqueueRollup`, `session.go:226`) — if a queued roll-up is
already pending, the newer arrival drains and replaces it (full replacement again). Without this
split a slow roll-up blocked delivery of the `BootstrapBlobResponse`s an in-flight `executeBootstrap`
was waiting on, pushing them past their deadline and marking machines `Failed` for an orchestration
timeout unrelated to the machines (`session.go:80`).

`rollupWorker` (`session.go:250`) decodes via `conv.NeedsFromRollup`. On a decode failure it
**rejects the whole roll-up loudly and keeps the last-known-good demand** (`session.go:262`) — same
posture as the provider-ingest gate. On success it calls `ApplyRollup` (`shard.go:442`), which
replaces the cluster's NeedsTable slice and marks the [ADR-0036](../adr/0036-phase3-gated-by-first-rollup.md)
first-rollup gate, then `triggerCycle()` (`session.go:280`). `ApplyRollup` may instead *quarantine*
a full-replacement roll-up that would erase most of the cluster's previously accepted demand
([ADR-0046](../adr/0046-actuation-safety-rails.md) empty-roll-up guard, `shard.go:444`) — the
previous demand stays active until consecutive reports confirm the drop. Either way the
first-rollup gate clears: the operator *did* report.

**Where the datum lives now:** the shard's in-memory NeedsTable, per cluster, priority-sorted,
full-replacement. The cluster's CRs are no longer consulted; the NeedsTable is the demand truth the
engine walks.

## Hop 4 — the shard cycle: reconcile, Phase 1/2/3, queue actions

`triggerCycle` (`shard.go:603`) coalesces onto a wakeup channel; `Run` (`shard.go:487`) fires a
cycle on that wakeup or on the `CycleInterval` ticker (default 10 s in code, but profiles run it at
**~1 s** under load; the cycle is the engine's heartbeat). One cycle is `runCycleCapturing`
(`shard.go:627`):

1. **Reconcile** inventory against the provider *before* deciding, so decisions run on a fresh view
   (`shard.go:649`). `reconcile` (`pkg/shard/reconcile.go:29`) calls `provider.List` —
   full every cycle by default, or incremental via an opaque `since_revision` cursor for providers
   above the [ADR-0004](../adr/0004-incremental-reconcile-via-since-revision.md) threshold (the
   difference between the cycle SLO being achievable at 500K and not — reconcile was ~85% of the
   cycle). `applyReconciledMachine` (`reconcile.go:116`) merges each machine: a state-match fast
   path does nothing; a diverged state applies the provider's view while preserving locally-known
   `Assigned*` attribution; a genuinely new machine is inserted, rebuilding attribution from the
   provider-echoed `shard_metadata` (the M72 restart-recovery path). In-flight machines are skipped
   (`isPending`, `shard.go:559`) — the provider's `List` lags an in-flight RPC and would overwrite
   the worker's transitional state.
2. **Snapshot** inventory and demand under one consistent read (`shard.go:663`), then
   `NormalizeDemand` folds sub-machine `Same`-Needs into atomic aggregates
   ([ADR-0041](../adr/0041-sub-machine-same-needs-fold-into-atomic-aggregates.md)) so all three
   phases reason over identical normalized demand.
3. **Phase 1 — assign** (`pkg/decision/phase1_assign.go:100`). Walks the NeedsTable; for each Need,
   credits existing bound supply, then commits Idle machines (→ `Bootstrap` actions) and Speculative
   machines (→ `Provision` actions) to fill the deficit, by **effective cost**. The accounting rule
   is [ADR-0045](../adr/0045-consumed-capacity-in-the-attribution-model.md): capacity counts for a
   cluster iff bound to it; the pre-pass credits Configured *and* Configuring machines, so a binding
   counts from the moment it is made, before the node exists — double-supply is impossible by
   construction. Runs as an Omega-style optimistic-concurrency scheduler
   ([ADR-0029](../adr/0029-phase1-omega-style-occ.md); see [`phase1-occ.md`](phase1-occ.md)). The
   output's `Claimed` set is **the engine's single supply attribution** that Phase 3 consumes.
4. **Phase 2 — preempt inversions** (`pkg/decision/phase2_inversions.go:66`). Walks Phase 1's
   `Unsatisfied` deficits, scores lower-priority Configured victims, emits `Preempt` actions. It
   does *not* move the machine to the preemptor — it drains victims to Idle; next cycle's Phase 1
   reclaims them (`phase2_inversions.go:38`).
5. **Phase 3 — reclaim excess** (`pkg/decision/phase3_reclaim.go:77`). **Shrinkage-only**: a
   Configured machine is excess iff Phase 1's `Claimed` walk left it unclaimed
   (`phase3_reclaim.go:91`). At steady demand nothing is unclaimed and Phase 3 emits nothing. It
   re-derives no keep-set of its own — sharing Phase 1's single attribution is what removed the
   Bootstrap↔Reclaim oscillator class (`phase3_reclaim.go:48`). The same pass adds §8's release
   half (M73 / [ADR-0049](../adr/0049-idle-speculative-release-hold-window.md)): unclaimed Idle
   machines past their `CapacityType` hold get `Delete` actions (`phase3_reclaim.go:108`). The
   ADR-0036 first-rollup gate applies to reclaim but **not** to Idle release — an Idle machine is
   bound to no cluster, so releasing it takes capacity from no one.

The cost and victim formulas are **fixed, not pluggable** (`pkg/decision/cost.go:1`):

```
effective_cost = price_per_hour + (interruption_probability × interruption_penalty)
victim_score   = priority_gap·w_pri + (1/drain)·w_drain + (1/interruption_penalty)·w_int + (1/reclamation_penalty)·w_rec
```

`interruption_probability` is **provider-declared only** (`provider.proto:116`, `Machine.interruption_probability`)
— no cluster-side override. The two penalties are **distinct**: `interruption_penalty` (cost of
interrupting the workload; in `effective_cost` and victim scoring) versus `reclamation_penalty`
(operational value of the specific machine; in idle tiebreak, victim scoring, §8 release). Drain
grace is a locked table keyed on the priority gap (10 s / 30 s / 2 m / 10 m, `cost.go:102`); a
voluntary Phase 3 reclaim has no preemptor and gets the most generous 10 m tier (`ReclaimGrace`,
`cost.go:122`).

All three phases' actions collapse into one queue (`shard.go:777`) — they compute on the same
snapshot so ordering between them is irrelevant. The actuation safety rails apply here, before
anything executes: the per-cluster reclaim blast-radius cap (`shard.go:793`), `MaxActionsPerCycle`
deferral (`shard.go:808`), the `ActuationPaused` kill switch and `DryRun` shadow mode
(`shard.go:827`/`844`), and a live-state re-check (`actionStillApplicable`, `shard.go:583`) plus a
per-machine in-flight dedup (`pendingActions`, `shard.go:905`) that together prevent a worker
dispatch whose target machine has already moved past the action's required start state.

## Hop 5 — execute: provider Create/Configure, blob over the stream

Actions enqueue onto a persistent worker pool ([ADR-0021](../adr/0021-persistent-execute-pool.md),
`executeWorker`, `shard.go:525`). Each worker gets a fresh context capped by `ExecuteTimeout`
(default 30 s) — **independent of the cycle that produced it**, so a slow operator handler delays a
binding instead of cancelling and dropping it. `execute` (`pkg/shard/execute.go:45`) dispatches by
kind; every outcome is classified and audited (`classifyExecuteError`, `execute.go:114`).

The hot path for the trace is `executeBootstrap` (`execute.go:242`), which turns an Idle machine
into a Configured one:

1. **Idle → Configuring**, stamping the destination cluster, the assigned Need fingerprint, and the
   gang group *early* (`execute.go:286`) so Phase 1's deficit math counts this in-flight machine as
   supply for its Need *while the Configure is still running* — without this stamp Phase 1
   over-emits a second Bootstrap on a fresh machine next cycle (Drop G).
2. **Pull a bootstrap blob.** Either via the operator's stream (production) or a `LocalBootstrap`
   hook (simulator). `requestBootstrap` (`session.go:317`) mints a request ID, sends a
   `BootstrapRequest` down the stream, and blocks on the matching `BootstrapBlobResponse` up to
   `BootstrapTimeout`. The operator renders the blob (kubelet user-data / ignition / PXE) and sends
   it back (`stream.go:340` dispatch → `handleBootstrapRequest`). The shard treats a non-empty
   response `error` as an unsatisfiable requirement (`session.go:338`, `shard.proto:99`). A
   blob-fetch timeout rolls the machine *back to Idle* for retry — not `Failed` — because the
   provider hasn't been touched yet (`handleBootstrapBlobErr`, `execute.go:94`).
3. **`provider.Configure(blob)`** (`execute.go:339`), carrying the cluster, the blob, the fencing
   token, and the assignment's protection state as opaque store-and-echo `shard_metadata` — the
   only durable copy a restarted shard can rebuild attribution from (`execute.go:352`, M72).
4. **Configuring → Configured**, stamping `AssignedPriority` + both penalty dollars + fingerprint +
   group for Phase 2 victim scoring (`execute.go:363`). The `Configuring → Idle` race (a parallel
   actor flipped the machine back) is detected and surfaced as `errProvisionRacedToIdle` with
   natural next-cycle recovery (`execute.go:378`, `errProvisionRacedToIdle` doc at `execute.go:17`).

If no Idle machine was available, `executeProvision` (`execute.go:181`) runs first: Speculative →
Creating → Idle via `provider.Create`, then hands off to `executeBootstrap`. **Create is where
provider-declared `price` / `interruption_probability` enter the inventory and the locked cost
formula** — a garbage ack is treated as a provider error: `Failed`, loud, counted, never ingested
([ADR-0046](../adr/0046-actuation-safety-rails.md), `execute.go:216`).

The six provider RPCs are the whole actuator surface (`api/proto/.../provider.proto:47`,
`pkg/provider/provider.go:34`): `Create`, `Configure`, `Drain`, `Delete`, `Get`, `List`. All four
mutating RPCs are asynchronous (return a `TransitionAck`, progress observed via `List`/`Get`),
idempotent on `(machine_id, target_state)`, and fenced by `(shard_id, shard_epoch, sequence_number)`
so a zombie shard's mutation is refused (`provider.proto:14`). **There is no `Watch`** —
reconciliation is `List + Get`, which is why hop 4 reconciles every cycle.

## Hop 6 — node state back up the stream → UpcomingNode CR → Node Ready

Every transition on a cluster-bound machine emits a `NodeStateUpdate` *up* the stream. The hook is
in `applyTransition` (`shard.go:1399`): after the inventory write it calls `notifyNodeState`
(`shard.go:1449`), which looks up the bound cluster's session and pushes a `NodeStateUpdate` carrying
the machine ID, the `MachineState`, provider ID, and — under [ADR-0016](../adr/0016-nodestateupdate-carries-node-identity.md)
— the node identity (labels, allocatable resources, taints) the kubelet will register with, so
observers know the node's shape before it joins (`shard.go:1471`, `shard.proto:133`). The binding is
captured *before* the mutation runs (`prevCluster`, `shard.go:1420`) so the terminal
`Draining → Idle` update — which clears `Machine.Cluster` — still routes to the cluster that owned
the machine.

`NodeStateUpdate` is a coalescing frame: `supersedes_key = "node:<machine_id>"` (`session.go:404`)
lets the shard's outbox drop a stale queued frame when a newer one for the same machine arrives.
On the operator side, `recvLoop` runs these through a coalescer (`stream.go:151`): rapid
`Idle → Configuring → Configured` sequences for one machine collapse to a single apiserver write of
the terminal state, with a bounded dispatch semaphore (M44.4 Drop B — unbounded fan-out contended
on controller-runtime's cache locks and blew the handler p99).

`handleNodeStateUpdate` (`pkg/operator/upcoming.go:46`) upserts an `UpcomingNode` CR, mapping each
`MachineState` to a phase (`upcomingNodePhase`, `upcoming.go:401`):

| MachineState | UpcomingNode phase |
|---|---|
| Speculative / Creating | Provisioning |
| Idle | Launched |
| Configuring | Registered |
| Configured | Ready |
| Draining | Draining |
| Failed | Failed |

(`Drained` is derived: a `Launched` update following a `Draining` phase, `upcoming.go:141`.) The
upsert uses `MergeFrom` patches rather than `Update` to dodge the ~26% resource-version conflict
rate under burst (`upcoming.go:120`), with `RetryOnConflict` as a backstop (`upcoming.go:70`) — a
dropped phase write was the dominant tail-latency driver in the scale runs because the shard does
not spontaneously re-emit. So a `kubectl describe upcomingnode` is the user's window into the
provisioning a shard is doing on their behalf; the actual kubelet join is the cluster's own
business (the blob the operator rendered in hop 5 carries the join credentials).

**Where the datum lives now:** an `UpcomingNode` CR per machine, status tracking the machine through
its states, plus — once the kubelet registers — a real `Node`. The Pod the chain started from binds
onto that Node by the cluster's own kube-scheduler. BigFleet **is not a scheduler**; it never placed
the Pod, only made a Node it could land on.

**Timing point:** the *internal* binding latency BigFleet is accountable for is roll-up-observation
→ Configured. `observeRolledUpDemand` stamps first-seen time per `(cluster, fingerprint)` on roll-up
ingest (`pkg/shard/provisioning_latency.go:15`); `observeProvisioningLatency` samples the histogram
at the `Configuring → Configured` transition and deletes the stamp (`provisioning_latency.go:58`).
The **user-facing** latency is this internal number plus provider capacity-create time, which the
fake provider contributes zero of and which is measured outside this repo
([ADR-0018](../adr/0018-internal-vs-user-facing-binding-latency.md) /
[ADR-0014](../adr/0014-slo-posture-binding-latency-not-cycle-wall-clock.md)). Cycle wall-clock is a
tracked perf envelope (`cycle p99 ≤ rollupInterval/2`), **not** the release gate — a 1 s cycle
inside a 10 s roll-up window is invisible next to a real cloud's minutes-long Create.

## Re-converge, and the reclaim half

Once the machine is Configured, the loop closes. Next roll-up still names the demand (the Pod still
has its CR), Phase 1's pre-pass credits the now-Configured machine as supply, the deficit is zero,
and the engine emits nothing for that Need. Steady state is *quiet* — Phase 3 emits nothing because
Phase 1's `Claimed` set covers every Configured machine.

When demand drops — the Pod is deleted, its owner-referenced CR is garbage-collected, the next
full-replacement roll-up simply omits it — the chain runs in reverse:

1. **Phase 3** finds a Configured machine no longer in Phase 1's `Claimed` set (bound capacity now
   exceeds demand) and emits a `Reclaim` action with the 10 m voluntary grace
   (`phase3_reclaim.go:94`).
2. **`executeDrain`** (`execute.go:405`) handles both `Reclaim` (Phase 3) and `Preempt` (Phase 2).
   It notifies the operator *first* via a `ReclaimInstruction` down the stream
   (`sendReclaimInstruction`, `session.go:367`) carrying the node names, the grace period, and the
   preemptor priority (0 for a voluntary reclaim, telemetry-only), so the cordon + PDB-respecting
   eviction runs *ahead* of the provider drain.
3. **The operator drains** (`handleReclaimInstruction`, `pkg/operator/reclaim.go:33`): cordon each
   node, walk the `UpcomingNode` to `Draining`, **ack after cordon but before drain completes** —
   the shard's reclamation accounting is the cordon, not the drain, and drain can take minutes per
   pod (`reclaim.go:52`). Then it evicts non-DaemonSet pods through the **policy/v1 eviction
   subresource** so PodDisruptionBudgets are honoured up to the grace deadline
   ([ADR-0009](../adr/0009-reclaim-uses-policy-v1-eviction-and-async-drain.md), `evictPod`,
   `reclaim.go:221`; 429-on-PDB is retried, not failed, `reclaim.go:201`). On completion the
   `UpcomingNode` walks to `Drained` and is deleted to keep the apiserver working-set from growing
   through soak (`upcoming.go:157`).
4. **`provider.Drain`** moves Configured → Draining → Idle (`execute.go:436`), clearing the cluster
   binding and all `Assigned*` attribution on the post-Drain transition (`execute.go:448`). The
   provider clears its `shard_metadata` echo with the binding (`provider.proto:179`), so a restarted
   shard cannot resurrect a dead workload's attribution onto a fresh assignment.
5. The freed Idle machine feeds the next cycle's Phase 1 (if there is residual demand) or, past its
   `CapacityType` hold, gets a Phase 3 `Delete` back to Speculative (`executeDelete`, `execute.go:475`).

`Preempt` rides the exact same `executeDrain` path; the only differences are the
priority-gap-scaled grace (`DrainGrace`, `cost.go:102`) and a non-zero `PreemptorPriority` the
operator surfaces in events. The victim becomes Idle; the higher-priority Need that triggered the
preemption claims it next cycle in Phase 1.

## What this trace does *not* cross

- **The coordinator.** Nothing above touched `pkg/coordinator`. Cluster→shard binding, domain→shard
  assignment, and quota live there, but they are not on this hot path — the shard runs the whole
  demand→bind→reclaim loop autonomously. See [`coordinator-raft.md`](coordinator-raft.md) and
  [`static-stability.md`](static-stability.md).
- **A shard boundary.** A `Same`-rack request that cannot be satisfied within one shard becomes a
  *shortfall*, never a cross-shard resolution (architecture.md "Wire formats and protocol
  invariants"). Shortfall has no standalone package: the buffer/aging live in `pkg/shard`
  (`recordShortfalls`, `shard.go`), the deficit is derived in `pkg/decision` as the residual of
  Phase 1 → Phase 2 (`phase2_inversions.go:247`).
- **A scheduler.** BigFleet never placed the Pod. It made a Node; the cluster's kube-scheduler did
  the rest. "Satisfied-but-stuck" (the cluster's scheduler can't pack its Pods onto bound capacity)
  is explicitly the cluster's problem, not the engine's (`phase1_assign.go:78`).

---

If any claim here drifts from the code, the code citation is the anchor — fix the page or the code
and note it, per the source-of-truth ordering in [`README.md`](README.md) and
[`../index.md`](../index.md). For the interior of the one box this page treated as a unit,
continue to [`decision-engine.md`](decision-engine.md); for the concurrency model that runs it,
[`shard-hot-path.md`](shard-hot-path.md).
