# BigFleet provider author guide

If you're writing a `CapacityProvider` for BigFleet — to plug AWS, GCP, Azure, MAAS, Tinkerbell, Ironic, an internal cloud, or anything else — this is the guide.

## What you're building

Your provider is a separate process (separate repo, separate release cadence) that exposes a gRPC server implementing `bigfleet.v1alpha1.CapacityProvider`. BigFleet shards dial your address, list inventory, and walk machines through the standard lifecycle. Your provider is the thing that knows how to actually create / configure / drain / delete instances on your backend.

**BigFleet itself ships zero real providers**, on purpose. Kubernetes spent years undoing in-tree CCM/CSI providers; we don't repeat that mistake. The repo ships:

- The proto contract (`api/proto/bigfleet/v1alpha1/provider.proto`)
- The conformance test suite (`test/conformance/`) — point it at your provider to claim "BigFleet-compatible"
- A test-fixture fake (`pkg/provider/fake/`) — *not* deployable; only used internally for engine tests
- This guide

## The contract

Six RPCs, all defined on `service CapacityProvider`. No Watch — reconciliation is `List + Get`.

| RPC | Direction | What it does | Async? | Idempotent? |
|---|---|---|---|---|
| `Create(CreateRequest) → TransitionAck` | shard → provider | Speculative → Creating → Idle | yes | yes, on `(machine_id, target=Idle)` |
| `Configure(ConfigureRequest) → TransitionAck` | shard → provider | Idle → Configuring → Configured | yes | yes |
| `Drain(DrainRequest) → TransitionAck` | shard → provider | Configured → Draining → Idle | yes | yes |
| `Delete(DeleteRequest) → TransitionAck` | shard → provider | Idle → Deleting → Speculative | yes | yes |
| `Get(MachineRef) → Machine` | shard → provider | Read one machine's state | n/a | n/a |
| `List(ListFilter) → MachineList` | shard → provider | Read inventory subset | n/a | n/a |

### Async semantics

The four lifecycle RPCs return `TransitionAck` *immediately*. The actual transition is observed via subsequent `Get` / `List` calls. This is essential because real transitions take real time:

- Cloud `Create` is 30–90 s.
- Bare-metal `Create` (commissioning) can be hours.
- `Drain` of a long-running training workload with strict PDBs can be hours too.

Don't block the lifecycle RPCs waiting for completion. Accept the request, kick off the work, return.

### Idempotency

Repeated calls with the same `(machine_id, target_state)` must return the **same** `operation_id`. Use this whatever way works for your backend:

- Check whether a transition toward the target is already in flight; if so, return the existing operation_id without re-starting.
- Persist `(machine_id → in-flight transition, operation_id)` so your provider can survive restarts.

Real shards retry on transport failures. They expect retries to be safe.

### Transition timeouts → Failed

Each transitional state has a provider-defined timeout. On expiry, move the machine to `MACHINE_STATE_FAILED` with `last_error` populated. The shard takes corrective action depending on which transition failed (clean up, retry on a different slot, escalate).

### `Configured` means the node has joined and is Ready (ADR-0056)

**Do not report a machine `MACHINE_STATE_CONFIGURED` until you have observed the node `Ready` on its target cluster.** Hold it at `CONFIGURING` until then; if readiness is not observed within your Configure timeout, drive it to `FAILED` with `last_error` (see above).

A `CONFIGURED` machine is counted as *delivered capacity*: the shard credits it against demand and stops driving that demand. If you report `CONFIGURED` when the VM has merely booted — before the kubelet registered, the pod-CIDR was assigned, and CNI programmed routes — you create **phantom capacity**: the shard reads zero shortfalls while pods stay `Pending`. The bug is silent, which is why the obligation lives in the contract (ADR-0056).

How you observe readiness is your choice — BigFleet never hands you cluster credentials (`ConfigureRequest` carries only `cluster_id`, a name). Either:

- give your provider **read access to the target cluster** out-of-band (deployment-time kubeconfig / ServiceAccount) and poll the node's `Ready` condition, or
- use a **substrate signal that reliably implies kubelet registration** (e.g. a bootstrap-completion callback the node makes on join).

What the contract requires is the *guarantee* — `CONFIGURED` ⇒ joined and `Ready` — not a particular mechanism. If you build on **`providerkit`**, implement its `ReadinessChecker` hook: the kit holds the machine at `CONFIGURING` until your check passes and drives it to `FAILED` on timeout, so you don't re-implement the gate.

Conformance note: the six RPCs carry no node-readiness ground-truth signal, so the in-tree black-box suite *cannot* distinguish a provider that waits for `Ready` from one that reports `CONFIGURED` on boot — `make conformance-self` verifies the reference fake honours the gate, but **verifying your own provider against a real cluster is your integration test's job**, not the conformance suite's.

### Fencing — rejecting zombie shards

Every **mutating** RPC (`Create`, `Configure`, `Drain`, `Delete`) carries the shard's fencing token: `shard_id`, `shard_epoch`, `sequence_number` (BigFleet paper §11). The epoch is persisted shard-side and increments on every shard restart; the sequence number is a per-process monotonic counter, freshly stamped on every call attempt. The token is how your provider refuses a *zombie shard* — an old process (or a duplicate of the same shard identity) whose view of the fleet is stale and whose Drain/Delete would kill the wrong machines.

Your obligations:

- Track, per `shard_id`, the highest `(shard_epoch, sequence_number)` pair you've accepted, compared lexicographically — `(e1, s1)` is newer than `(e2, s2)` iff `e1 > e2`, or `e1 == e2 && s1 > s2`.
- Reject any mutating request whose token is **not strictly newer** than that high-water mark with `FAILED_PRECONDITION`, **without applying it** — and check the fence before your idempotent-retry short-circuit, so a zombie never gets a cached `operation_id` either.
- Accept first contact from an unknown `shard_id`; it establishes the high-water mark.
- A new epoch resets the sequence space: once the epoch advances, any `sequence_number` is acceptable.
- Advance the high-water mark whenever the fence check passes, even if the operation itself then fails — the mark records "newest shard process seen", not "operations that succeeded".
- Reserve `FAILED_PRECONDITION` for fencing rejections on this service. The shard alerts on it as a zombie-shard incident; using it for invalid state transitions creates false pages. Use a different code (the in-repo test fixture uses `INTERNAL`) for everything else.
- Don't worry about retry replays: the shard re-stamps a fresh `sequence_number` on every attempt. Idempotency is keyed on `(machine_id, target_state)`, never on the token.

`Get` and `List` carry no token — reads don't fence; a zombie reading state harms nothing. Persist the high-water marks if you can: a provider restart that forgets them re-opens the zombie window until every live shard makes contact again.

## Required label and field shape

The autoscaler's `MatchProfile` uses these fields directly. Don't bury them in labels — the shard's hot path won't go looking.

On every `Machine`:

- `id` — your stable identifier. Must survive Speculative → Idle (host attaches but the id stays the same). Treat as opaque.
- `state` — never `MACHINE_STATE_UNSPECIFIED` for a stable record.
- `instance_type` — required. The shard uses this to satisfy `node.kubernetes.io/instance-type` selectors directly without consulting `labels`.
- `zone` — required for multi-zone providers. The shard uses this to satisfy `topology.kubernetes.io/zone` selectors.
- `capacity_type` — `BARE_METAL`, `RESERVED`, `ON_DEMAND`, or `SPOT`. Drives idle-hold policy and effective-cost calculations.
- `price_per_hour` — USD. Zero for bare metal (already paid for).
- `interruption_probability` — hourly, in `[0, 1]`. **Provider-declared only**; clusters cannot override. Forecast for `SPECULATIVE` machines, observed for real ones.
- `host` — `null` when state is `SPECULATIVE` or `CREATING`; populated otherwise.
- `resources` — per-machine allocatable; the shard's `MatchProfile` does exact-string match on the resource map at v1.
- `labels` — anything else the shard / operator might want for matching beyond the well-known fields. `accelerator-type` is a common one.
- `cluster` — the binding `Configure` established, copied from `ConfigureRequest.cluster_id`. Populated while the binding exists (`CONFIGURING`, `CONFIGURED`, `DRAINING`); cleared when a `Drain` completes back to `IDLE`. Empty for `SPECULATIVE` / `CREATING` / `IDLE`. (M72)
- `shard_metadata` — see the next section. (M72)

`HostRef` is `(provider, ref)`. `provider` is your provider's name (your choice — used in logs); `ref` is whatever your backend uses to identify the host (instance ID, BMC serial, etc.).

## `shard_metadata` — store and echo, never interpret

`ConfigureRequest.shard_metadata` (M72) is an opaque `map<string,string>` the shard sends alongside the cluster binding. Your obligations are mechanical:

- **Store it verbatim** with the machine when you accept the `Configure`.
- **Echo it byte-for-byte** as `Machine.shard_metadata` on every `Get` / `List` / `TransitionAck` snapshot, for as long as the binding exists. Preserve keys you don't recognise — a newer BigFleet may write keys an older one didn't.
- **Clear the whole map together with `cluster`** when a `Drain` completes back to `IDLE`. It is per-assignment state established by `Configure`, not per-machine state; a stale echo would hand a dead workload's attribution to the machine's next assignment.
- **Never interpret it.** The contents are BigFleet-internal (assignment attribution the shard recovers after a restart). They are deliberately not first-class fields so no provider is tempted to read meaning into them; treat the map like you treat `bootstrap_blob`.

Why it matters: your store is the only persistent state the BigFleet data plane has. A shard that restarts rebuilds its entire inventory from your `List`/`Get`, and `cluster` + `shard_metadata` are what let it rebuild *which workload each machine protects* — drop them and every restart silently removes preemption protection fleet-wide.

Historical note (resolved): before M72 the wire contract could not round-trip the cluster binding at all, so a shard ingesting a gRPC provider's `CONFIGURED` records rejected every one of them — the `bigfleet_shard_machines_rejected_total{reason="structural"}` counter was added in M70 (the "M70b tripwire") precisely to make that visible. M72 closed the gap with the `cluster` and `shard_metadata` fields; the tripwire metric remains live, and a provider that fails to populate `cluster` on bound records will still trip it.

## `List`, `since_revision`, and reconciliation

The shard polls `List` every cycle for a fresh view of inventory. If your provider has more than a few thousand machines per shard, full-list responses get expensive. The wire protocol carries an optional `since_revision`:

- The provider returns a `revision` (opaque bytes) on every `MachineList`.
- The next caller passes that revision back as `since_revision`.
- The provider returns only machines whose state has changed since.

Threshold: **support `since_revision` once your provider exposes more than ~10,000 machines per shard**. Below that, full-list per cycle is fine; your conformance run will pass either way. The shard side already accepts both modes.

## Special states

- `SPECULATIVE` — quota slot. Real machine doesn't exist; `host` is null. Returned by `List` so the shard can choose to actuate one via `Create`.
- `IDLE` — real host, no cluster binding. Bare-metal providers' "free pool" is a sea of these.
- `CONFIGURED` — real host whose node has joined and reached `Ready` on its cluster. Never report it before the node is `Ready` (see "`Configured` means the node has joined and is Ready" above).
- `CREATING` / `CONFIGURING` / `DRAINING` / `DELETING` — transitional. Your `Get` should report these while work is in flight.
- `FAILED` — last transition timed out or hit an unrecoverable error. `last_error` populated. The shard intervenes.

## Bare-metal providers

`Delete` is optional for bare-metal-style providers (the machine doesn't get "terminated"; it returns to the free pool when its lifecycle ends). Return `codes.Unimplemented` from `Delete` if your backend doesn't have a meaningful semantic for it. The shard handles this case — and since M73 its idle-release path only ever emits `Delete` for machines whose `capacity_type` is `ON_DEMAND` or `SPOT` (the paper-§8 hold policy keeps fixed capacity forever), so a provider that declares its capacity types honestly never receives the call. If you do implement `Delete`, it must only succeed on `IDLE` machines: reject it on a bound (`CONFIGURED`) machine — there is no `Configured → Deleting` edge in the state machine, and the rejection must not use `FAILED_PRECONDITION` (reserved for fencing).

## Deployment shape

- One process per provider. Don't co-locate with the shard.
- Listen on its own gRPC port. mTLS for production; insecure is fine for in-cluster trust.
- Shards reach you via their `--provider-addr` flag (Helm: `shard.provider.addr`). The shard side is `pkg/provider/grpcclient`; it stamps the fencing token on every mutating call.
- One configured provider in the BigFleet coordinator's provider registry per (provider implementation × region) pair. AWS in `us-east-1` and AWS in `eu-west-1` are two separate registry entries even though the implementation is the same.

## Run the conformance suite

```sh
# Bring up your provider, listening on (e.g.) localhost:9000.
# Seed it with a handful of speculative slots so the suite has
# something to walk through the lifecycle with.

# Then, from the BigFleet repo:
make conformance TARGET=localhost:9000

# Or directly:
go test -tags=conformance -count=1 -v -target=localhost:9000 ./test/conformance/...
```

The suite's `TestConformance_*` tests pick a Speculative machine, walk it through Create → Configure → Drain → Delete (skipping Delete if you return Unimplemented), assert idempotency on all four lifecycle RPCs, reject out-of-position lifecycle calls (Drain on Speculative, Delete on Configured), enforce the fencing contract (stale epoch / stale sequence rejected, new epoch resets, unknown shard accepted, reads unaffected), enforce the `shard_metadata` echo contract (verbatim on Get and List, unknown keys preserved, cleared with the binding when Drain completes), exercise the `List` filter behaviour, and verify your label shape. A passing run is what "BigFleet-compatible" means.

## Reference example

A worked-example provider lives outside this repo (e.g. `bigfleet-provider-fake-cloud`) so authors have something concrete to read. It is *not* consumed by this repo's tests — that's what `pkg/provider/fake` (test fixture) is for.

## Common mistakes

- **Synchronous `Create`** — blocking until the instance is up. Wrong; return immediately, `Get` reports progress. The shard's reconciler polls.
- **Burying `instance_type` / `zone` in labels.** The shard's `MatchProfile` reads the top-level fields directly. If you only set them in `labels`, GPU pod placement breaks for non-obvious reasons.
- **Returning a fresh `operation_id` on every retry.** Idempotency requires the same id across retries with the same target. Persist it.
- **Using `FAILED_PRECONDITION` for anything but fencing.** The shard treats that code as "I am a zombie" and pages a human instead of retrying. Invalid transitions, bad arguments, backend hiccups — all get other codes.
- **Checking the fence after the idempotency lookup.** A zombie that gets a cached `operation_id` back believes its stale mutation succeeded. Fence first.
- **Skipping `interruption_probability`.** Spot machines whose probability is 0 will get picked for high-penalty workloads, which is a correctness issue (effective_cost = price + p × penalty). Always set the real value.
- **Interpreting `shard_metadata`, or whitelisting its keys.** The contract is store-and-echo-verbatim. Filtering to keys you recognise drops the assignment state a future BigFleet writes; reading values couples your provider to BigFleet internals that can change without notice.
- **Letting `cluster` / `shard_metadata` outlive the binding.** Both clear when a Drain completes back to Idle. A stale echo resurrects a dead workload's preemption protection onto the machine's next assignment.
- **Per-RPC timeouts that don't model your backend.** Cloud `Create` of 30–90s ≠ your provider's "request timeout" of 5s. Set transition timeouts to your backend's worst-case.
