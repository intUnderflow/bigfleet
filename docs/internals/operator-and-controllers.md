# The operator and the unschedulable-pod controller

The operator is BigFleet's demand edge: one per-cluster agent that translates Kubernetes
intent into the engine's `ClusterCapacityNeeds` roll-up and renders the engine's decisions
back into cluster-scoped CRs. It is *outbound-only* — it dials the shard and holds exactly one
bidirectional `Shard.Session` stream, on which roll-ups, bootstrap pulls, reclaim instructions,
and node-state updates are all multiplexed (`pkg/operator/operator.go:1`). There is no inbound
listener; the operator never accepts a connection. The optional unschedulable-pod controller
(UPC) sits upstream of it, converting Pods into the `CapacityRequest` CRs the operator reads.
This doc is the wiring between those two; for the engine that consumes the roll-up see
`decision-engine.md`, for the stream's shard side see `shard-hot-path.md`, and for the CR/CRD
shapes see `wire-protocols.md`.

## The single outbound stream

`Operator.Run` is a reconnect loop around `runOnce` with exponential backoff bounded by
`ReconnectInitialBackoff`/`ReconnectMaxBackoff` (default 500ms → 30s, `operator.go:168`,
`operator.go:192`). `runOnce` dials, opens `cli.Session(streamCtx)`, sends a `Hello`
(`cluster_id` + `protocol_version="v1alpha1"`), then runs three goroutines under a tiny
`errGroup`: `sendLoop`, `recvLoop`, and `rollupLoop` (`pkg/operator/stream.go:20`,
`stream.go:63`). Any one returning an error cancels `streamCtx`, which collapses the others and
returns control to `Run` to reconnect. The whole `Shard` service is one RPC —
`rpc Session(stream OperatorMessage) returns (stream ShardMessage)`
(`api/proto/bigfleet/v1alpha1/shard.proto:23`) — so all four uplink message kinds (`Hello`,
`rollup`, `bootstrap_response`, `reclaim_ack`) and all five downlink kinds (`ack`,
`bootstrap_request`, `reclaim_instruction`, `node_state_update`, `available_capacity`) ride the
same byte stream (`shard.proto:27`, `shard.proto:37`). This is the "no inbound listener"
hard rule made concrete: the cluster owns the dial, the shard never reaches back.

**Two send queues, two drop policies.** The send path is asymmetric because the two outbound
message classes have different correctness requirements under back-pressure (paper §10.5 —
bound the per-cluster outbox so a slow stream can't grow memory without limit), encoded in
`session` (`stream.go:87`):

- **`pendingRollup`** is a single-slot `atomic.Pointer`. Roll-ups are full replacement
  (paper §3.1), so a newer roll-up *should* drop any older one still queued —
  `enqueueRollup` does `Store` + a non-blocking signal, coalesce-by-replace
  (`stream.go:182`). `sendLoop` `Swap(nil)`s the slot on each signal (`stream.go:217`). The
  operator therefore never queues two roll-ups behind a slow stream; the shard only ever sees
  the latest desired state.
- **`outbox`** is a bounded channel (`outboxCap = 256`, `stream.go:118`) for the
  non-coalescing RPC responses (`BootstrapBlobResponse`, `ReclaimAck`). When full it
  *drops the newest* with a metric (`stream.go:195`). Drop-newest is correct here precisely
  because these are responses to a shard request: the shard re-issues on its own timeout, so a
  queued-up response would only arrive after the shard had already given up
  (`stream.go:82`).

**Receive-side dispatch is split by cost.** `recvLoop` does not treat all inbound frames the
same (`stream.go:249`). Handlers that write the apiserver are bounded by a semaphore
(`dispatchConcurrency = 32`, `stream.go:131`); handlers that don't (`BootstrapRequest`, a
CPU-only blob render) bypass the bound and run free. The split exists because the shard's
`executeBootstrap` blocks on the bootstrap pull with a one-cycle deadline — if a bootstrap
response queued behind a slow `NodeStateUpdate` at the semaphore gate, the shard would cancel
and the machine would land in `Failed` (`stream.go:240`). `needsBoundedDispatch` enumerates the
slow kinds (`AvailableCapacity`, `ReclaimInstruction`); `BootstrapRequest` is deliberately
absent (`stream.go:323`). The `dispatchConcurrency = 32` value is itself an empirical scar:
M44.4 Drop B found 256 actively counterproductive — handlers contended on controller-runtime's
internal cache RWMutex and write-rate-limiter token bucket and tail-blew to 32s p99 while
apiserver throughput sat at <1% of budget (`stream.go:120`).

**NodeStateUpdate coalescing.** `NodeStateUpdate` gets its own path entirely
(`stream.go:267`). The shard emits one frame per machine transition (Idle → Configuring →
Configured for a single binding), but the operator only needs to reflect the *terminal*
state into the `UpcomingNode` CR. `coalesceNodeStateUpdate` keeps the latest pending frame per
machine ID and a per-machine in-flight flag, so at most one worker exists per machine; the
worker loops, draining any newer frame that arrived while it ran (`stream.go:145`,
`stream.go:281`). Rapid transitions collapse to a single apiserver write of the latest state —
M44.4 Drop B halved operator-side write volume during burst (`stream.go:99`).

## Building the roll-up: full replacement with `Same` translation

`rollupLoop` fires every `RollupInterval` (default 10s per the operating-model paper) and once
immediately on connect so a freshly-reconnected operator gives the shard demand without waiting
a tick (`pkg/operator/rollup.go:30`). Each cycle is `runRollup`: list every `CapacityRequest`
in the cluster from the informer cache, aggregate, enqueue, then acknowledge
(`rollup.go:51`). The roll-up is the cluster's *complete* desired state every time — never a
delta — which is what lets the shard treat it as authoritative and what makes coalesce-by-replace
safe.

**Aggregation is by Profile fingerprint and co-location group** (`buildRollup`,
`rollup.go:151`). Each CR contributes one Pod's worth of demand. Per ADR-0027, the resource
request is *not* part of the Profile identity; it is the demand vector. One CR's request is
both its `AggregateResources` (summed across same-`(Profile, Group)` Needs) and its `MinUnit`
(maxed across them) — one CR is one Pod is the atomic schedulable unit, so the wire
`CapacityNeed` carries the constraint set's total resource demand plus the largest single unit
that must fit on one machine (`rollup.go:129`). No Pod *count* crosses the wire; machine count
is the autoscaler's output, not its input. The common case — CRs with no co-location and
identical profiles — collapses to a handful of Needs, keeping the roll-up ~2KB regardless of
fleet size (paper §11, `rollup.go:148`).

**`Same` is synthesized here, never authored by the user.** The CRD speaks only standard
operators (`In`/`NotIn`/`Exists`/`DoesNotExist`); `Same` is protobuf-only. The operator emits
it when it detects a co-location signal. `coLocationGroup` derives a canonical aggregation key
from the CR's `Spec.CoLocation` term — the projection of the source Pod's required
`podAffinity`, *not* its ownerReferences (ADR-0024, `rollup.go:199`). `canonicalLabelSelector`
serialises the selector deterministically (matchLabels by sorted key, matchExpressions sorted
by key/operator/values) so two selectors that select the same set produce the same group string
regardless of map-iteration order (`rollup.go:212`). CRs with an equal term aggregate into one
Need and get a `Same` requirement appended on the term's `TopologyKey` via `withSameRequirement`
(idempotent — won't double-append, `rollup.go:258`). CRs with *different* terms stay separate
even when their profiles are otherwise identical, so each independent workload co-locates onto
its own domain. CRs with no co-location get an empty group and aggregate purely by Profile.
Note the hard rule this respects: an unsatisfiable `Same` becomes a shortfall inside one shard;
the operator never attempts cross-shard resolution — it doesn't even know about shards.

**Penalties are bucketed at the demand edge.** `profileFromCapacityRequest` is the canonical
place where raw dollar `resource.Quantity` values become `PenaltyBucket`s (`rollup.go:275`).
The two penalties stay distinct end to end: `InterruptionPenalty` (cost of interrupting the
workload) and `ReclamationPenalty` (operational value of the specific machine) bucket
independently (`rollup.go:316`). Both go through `AsApproximateFloat64`, not `AsInt64` —
M68b found `AsInt64` reports `ok=false` for fractional quantities like `500m` and the discarded
error flattened sub-dollar penalties to $0, erasing the very bucket the $0.50 boundary exists
for; float error is dwarfed by powers-of-2 bucket resolution (`rollup.go:311`). `ScheduleAnyway`
topology spread is dropped here too: Phase 1 enforces only `DoNotSchedule`, so carrying a soft
constraint would only fragment Profile aggregation for zero engine behaviour — dropped at the
edge regardless of CR source (`rollup.go:294`).

**Acknowledgement is a one-way, one-call transition.** After enqueuing the roll-up (which
never blocks — the pending slot always accepts), `runRollup` walks the CRs that were `Pending`
at observation time to `Acknowledged`, in parallel up to `AcknowledgeConcurrency` (default 16,
`rollup.go:81`, `rollup.go:89`). `markAcknowledged` is a JSON merge-patch on the status
subresource — one apiserver call per CR, no resource-version precondition, so no conflict retry
loop (`rollup.go:413`). It used to be a Get + Status().Update round trip; halving it mattered
for 1K-CR ramp bursts. JSON merge-patch (not strategic merge) because CRDs don't support
strategic merge. A `NotFound` on patch is swallowed — the CR was GC'd since the list, which is
fine.

## Handling reclaim: cordon-then-ack, drain on its own clock

`handleReclaimInstruction` is the operator's side of a Phase 3 reclaim (`pkg/operator/reclaim.go:33`).
The contract is *cordon, not drain completion*: the ack fires after the cordon patch lands but
**before** drain finishes, because draining can take minutes per Pod and the recv-loop must not
block on it (`reclaim.go:21`). The sequence:

1. **Cordon** every named node inline (`{"spec":{"unschedulable":true}}` merge-patch) and walk
   its `UpcomingNode` to `Draining` (`reclaim.go:43`, `reclaim.go:79`). Inline so the ack
   carries post-cordon truth.
2. **Ack synchronously** via the outbox — `ReclaimAck{InstructionId, NodesStarted}`. "Started"
   is the contract the shard's reclamation accounting keys on (`reclaim.go:54`). If the ack
   enqueue fails (outbox full) the operator logs and continues; the shard re-issues on timeout.
3. **Drain asynchronously** in a background goroutine bounded by the instruction's
   `grace_period_seconds` (the priority-gap-scaled window: 10s/30s/2m/10m, default 30s,
   `reclaim.go:70`). Critically the drain does *not* inherit the recv-loop ctx — that goroutine
   returns right after the ack — so a `context.Background()`-with-deadline owns the drain
   lifetime (`reclaim.go:147`).

`drainOneNode` evicts every non-DaemonSet Pod via the `policy/v1` eviction subresource so
PodDisruptionBudgets are respected — a PDB-blocked eviction returns 429, which is treated as
transient and retried with a 2s sleep up to the grace deadline rather than failing the whole
drain (`reclaim.go:178`, `reclaim.go:221`). DaemonSet Pods are skipped
(`isDaemonSetPod`, `reclaim.go:229`). On per-node completion the `UpcomingNode` walks to
`Drained`; on grace-deadline exceeded it walks to `Failed` with `lastError` populated
(`reclaim.go:147`). `UpcomingNode`s are cluster-scoped and matched to a node by scanning the
list for `Status.NodeRef.Name` — cheap at realistic in-flight-node counts (`reclaim.go:96`,
`reclaim.go:104`).

## Bootstrap: a Helm-values text template, not a CRD

When the shard pulls a kubelet bootstrap blob (`BootstrapRequest`, `shard.proto:73`),
`handleBootstrapRequest` renders via the configured `BootstrapRenderer` and enqueues a
`BootstrapBlobResponse` echoing `request_id` (`pkg/operator/bootstrap.go:14`). A renderer error
is echoed into the response's `Error` field; the shard treats a non-empty `Error` as an
*unsatisfiable requirement* and falls back to a shortfall (`shard.proto:97`,
`bootstrap.go:23`) — a bad template gates capacity instead of crashing the operator.

Per **ADR-0011**, the template is a Helm-values text/template, deliberately *not* a CRD or an
admission webhook. The rationale is the static-stability / no-coupling discipline: a CRD-based
template would re-introduce a kube-apiserver dependency on the bootstrap hot path (a path that
was a single in-process function call), and a webhook would add a second runtime BigFleet must
talk to. Helm values are static, file-mounted, parsed once. `NewFileTemplateRenderer` parses a
Go `text/template` at startup and returns a renderer; the template context mirrors
`BootstrapRendererInput` verbatim (`{{ .ClusterID }}`, `{{ range .Requirements }}…`),
decoupled from the proto so renderers don't pull in generated types
(`pkg/operator/bootstrap_template.go:32`, `bootstrap.go:34`). The chart renders the
`bootstrapTemplate` values block into a ConfigMap mounted at `/etc/bigfleet/bootstrap.tmpl`;
`cmd/operator`'s `--bootstrap-template-file` points at it (`cmd/operator/main.go:56`,
`main.go:89`). An empty path falls through to `stubBootstrapRenderer`, the placeholder that
emits a `#cloud-config` comment (`operator.go:206`) — real token minting / CA bundle / kubelet
config land when an actual kubelet must join a kind cluster.

## UpcomingNode: the engine's machine lifecycle made observable

`handleNodeStateUpdate` upserts an `UpcomingNode` CR per machine, mapping every `MachineState`
to one phase per paper §6.3 (`pkg/operator/upcoming.go:46`, `upcoming.go:401`):

| MachineState                | UpcomingNode phase |
|-----------------------------|--------------------|
| `Speculative` / `Creating`  | `Provisioning`     |
| `Idle`                      | `Launched`         |
| `Configuring`               | `Registered`       |
| `Configured`                | `Ready`            |
| `Draining`                  | `Draining`         |
| `Failed`                    | `Failed`           |

`Drained` has no MachineState — it is *inferred* from a `Draining` → `Idle` transition: an
update that maps to `Launched` whose existing phase is `Draining` is rewritten to `Drained`
(`upcoming.go:141`). At the `Drained` terminus the operator *deletes* the CR rather than leave a
stale-phase record (Drop AA, `upcoming.go:145`): a scaleway-50k soak showed bind p99 growing
linearly with run length purely from apiserver working-set growth as ~54K stale `Drained`
records accumulated per cluster; deleting on `Drained` also closes the loop with pod-shim's
fake-Node cleanup, which fires on the informer DELETE event.

Several write-path optimisations matter because this handler runs on every machine transition
fleet-wide:

- **Spec refresh** carries labels/resources/taints on every update (ADR-0016) but only writes
  when the spec actually changed (`upcomingSpecEqual`, `upcoming.go:125`, `upcoming.go:372`).
- **MergeFrom patch, not Update** (M48.4) on both spec and status paths: `Status().Update`
  requires a resourceVersion match, and controller-runtime's cached Get returns a stale version
  under burst, producing a ~26% conflict rate (bigfleet-uber #25). A `MergeFrom` JSON merge
  patch has no resourceVersion guard, eliminating the conflict at source (`upcoming.go:113`,
  `upcoming.go:195`).
- **No-op short-circuit**: if phase, nodeRef, providerID, lastError are all unchanged and
  `ProvisioningStartTime` is set, skip the status write entirely — rebroadcasts on reconnect
  and `supersedes_key` replays hit this often (`upcoming.go:185`).
- **RetryOnConflict** still wraps the whole handler as a safety net for the rare genuine race
  (`upcoming.go:70`). The reason it must be robust: the shard does *not* spontaneously re-emit
  `NodeStateUpdate`s — the coalescer only loops on a *new* frame — so a silently-dropped
  Configured write would strand the `UpcomingNode` on `Configuring` for tens of seconds, which
  is exactly the `upcoming_to_node` tail measured in earlier drops (`upcoming.go:57`).

`AvailableCapacityUpdate` follows the same upsert shape (`upcoming.go:227`). Its CR name is a
SHA-256 of the profile fingerprint (`ac-` + hex), because fingerprints contain colons, pipes,
slashes — none valid in a `metadata.name` — while the fingerprint itself round-trips through
the spec (`upcoming.go:277`). The `supersedes_key` *is* the fingerprint, so successive updates
rewrite the same object.

## The unschedulable-pod controller: one CapacityRequest per Pod

The UPC (`pkg/controller/cr/controller.go`) is *optional* — operators driving CRs from Kueue,
an admission controller, or any other pipeline skip it entirely (`controller.go:14`). When used,
it watches Pods and emits one `CapacityRequest` per Pod.

**One CR per Pod, not per *unschedulable* Pod (ADR-0039).** This is the load-bearing decision.
An earlier revision created a CR only for Pods seen `Unschedulable`; that undercounts total
demand and breaks Phase 3, because the roll-up must be the cluster's *total* desired capacity —
Phase 1 reads it as "what to provision", Phase 3 reads it as "what is genuinely surplus"
(`controller.go:1`, `controller.go:153`). A CR for every Pod is what makes the roll-up
authoritative in both directions. The CR is owner-referenced to the Pod (`controller.go:293`)
so deleting the Pod garbage-collects the CR — the paper's implicit-withdrawal contract.

**Terminal pods are reclaimed eagerly.** OwnerRef GC only fires on Pod *deletion*, but a
`Succeeded`/`Failed` Pod can linger indefinitely (completed Jobs, crashed pods kept for
debugging) while consuming no capacity. So on the terminal transition the controller
`DeleteAllOf`s the Pod's CRs itself and never creates one for an already-terminal Pod
(M68b / ADR-0045, `controller.go:170`). Creation is idempotent — guarded by a label lookup
(`LabelOwnedByPod` = Pod UID) and tolerant of `AlreadyExists` on the create
(`controller.go:189`, `controller.go:267`).

**Translation from Pod spec** (`buildCRForPod`, `controller.go:282`):

- **Requirements** come from required `nodeAffinity` (first term's `matchExpressions` only —
  multi-term OR semantics are out of v1 scope) *and* `nodeSelector`, each `nodeSelector` entry
  translated to an `In [value]` requirement, sorted for determinism (`controller.go:338`).
  Dropping `nodeSelector` was a real bug (M68b/ADR-0045): a Pod pinned by `nodeSelector`
  produced a CR any machine could satisfy, so BigFleet would "fulfill" demand the scheduler then
  couldn't place. A Pod with no constraints at all gets a synthesized `Exists` on
  `node.kubernetes.io/instance-type` so the matcher has something to bind against
  (`controller.go:382`).
- **Co-location** projects the Pod's required `podAffinity` (first term) into a
  `CoLocationTerm` — the structured signal the operator later turns into `Same` (ADR-0024,
  `controller.go:399`). Preferred (soft) podAffinity and *all* podAntiAffinity are deliberately
  ignored.
- **Resources** use kube-scheduler's own arithmetic
  (`max(sum(containers)+sum(sidecars), peak init stage) + overhead`, `controller.go:427`). The
  prior translation summed init-container requests on top of the main containers, overstating
  demand — init containers run *serially* before the main containers (M68b/ADR-0045). A sidecar
  (init container with `restartPolicy: Always`) joins the running set for the Pod's lifetime;
  pod overhead (RuntimeClass) is part of the scheduler's effective request and is included so
  sandboxed Pods aren't understated.
- **Penalties** resolve in a fallback chain: Pod annotation
  (`bigfleet.lucy.sh/{interruption,reclamation}-penalty`) → matching `PriorityClass` default
  (M16, loaded from a YAML file by the cmd binary) → nil, which the autoscaler treats as $0
  (`controller.go:79`, `controller.go:318`). The two penalties resolve independently — they are
  distinct concepts and never derived from each other or from priority.
- **Spread** drops `ScheduleAnyway` terms at the edge for the same reason the operator does
  (`controller.go:479`).

After creating the CR, the controller stamps `status.phase=Pending` via a status merge-patch —
a separate write because `Create` can't set status (`controller.go:219`, `controller.go:230`).
The operator's `markAcknowledged` treats both `""` and `Pending` as "needs acknowledging", so a
transient Pending↔Acknowledged flap if the operator's first roll-up races this patch is harmless;
the patch failing is non-fatal (lifecycle still works, just less observable from kubectl).

## Binaries

`cmd/operator` (`cmd/operator/main.go`) is the agent: it builds a *cache-backed*
controller-runtime client (an in-process informer cache — the difference between a 2s roll-up
and a sub-100ms one once there are thousands of CRs, `main.go:133`), wires
`--bootstrap-template-file` through `NewFileTemplateRenderer`, and runs `Operator.Run` until
SIGINT/SIGTERM. mTLS is opt-in (ADR-0048): the operator's cert must carry the URI SAN
`bigfleet://cluster/<cluster-id>`, which the shard binds to the asserted `cluster_id`,
terminating mismatched sessions with `PermissionDenied` (`operator.go:43`, `main.go:57`).
It also exposes `/metrics` and pprof for scaletest diagnostics.

`cmd/bigfleet-unschedulable-pod-controller` (`main.go`) runs the UPC under a controller-runtime
manager, loads the `PriorityClass` → penalty defaults YAML, and bumps client-go QPS/burst
(default 50/100) because status writes are bursty under unschedulable-Pod spikes. Its cache-sync
timeout is widened to 10 minutes — the default 2 minutes is too tight for a kwok apiserver
paginating ~100K Pods under ramp, and a manager exit there produces a restart loop that only
makes the apiserver hotter (`main.go:82`).
