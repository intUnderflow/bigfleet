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
| `Delete(MachineRef) → TransitionAck` | shard → provider | Idle → Deleting → Speculative | yes | yes |
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

`HostRef` is `(provider, ref)`. `provider` is your provider's name (your choice — used in logs); `ref` is whatever your backend uses to identify the host (instance ID, BMC serial, etc.).

## `List`, `since_revision`, and reconciliation

The shard polls `List` every cycle for a fresh view of inventory. If your provider has more than a few thousand machines per shard, full-list responses get expensive. The wire protocol carries an optional `since_revision`:

- The provider returns a `revision` (opaque bytes) on every `MachineList`.
- The next caller passes that revision back as `since_revision`.
- The provider returns only machines whose state has changed since.

Threshold: **support `since_revision` once your provider exposes more than ~10,000 machines per shard**. Below that, full-list per cycle is fine; your conformance run will pass either way. The shard side already accepts both modes.

## Special states

- `SPECULATIVE` — quota slot. Real machine doesn't exist; `host` is null. Returned by `List` so the shard can choose to actuate one via `Create`.
- `IDLE` — real host, no cluster binding. Bare-metal providers' "free pool" is a sea of these.
- `CONFIGURED` — real host, currently running a kubelet for a specific cluster.
- `CREATING` / `CONFIGURING` / `DRAINING` / `DELETING` — transitional. Your `Get` should report these while work is in flight.
- `FAILED` — last transition timed out or hit an unrecoverable error. `last_error` populated. The shard intervenes.

## Bare-metal providers

`Delete` is optional for bare-metal-style providers (the machine doesn't get "terminated"; it returns to the free pool when its lifecycle ends). Return `codes.Unimplemented` from `Delete` if your backend doesn't have a meaningful semantic for it. The shard handles this case.

## Deployment shape

- One process per provider. Don't co-locate with the shard.
- Listen on its own gRPC port. mTLS for production; insecure is fine for in-cluster trust.
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

The suite's `TestConformance_*` tests pick a Speculative machine, walk it through Create → Configure → Drain → Delete (skipping Delete if you return Unimplemented), assert idempotency, exercise the `List` filter behaviour, and verify your label shape. A passing run is what "BigFleet-compatible" means.

## Reference example

A worked-example provider lives outside this repo (e.g. `bigfleet-provider-fake-cloud`) so authors have something concrete to read. It is *not* consumed by this repo's tests — that's what `pkg/provider/fake` (test fixture) is for.

## Common mistakes

- **Synchronous `Create`** — blocking until the instance is up. Wrong; return immediately, `Get` reports progress. The shard's reconciler polls.
- **Burying `instance_type` / `zone` in labels.** The shard's `MatchProfile` reads the top-level fields directly. If you only set them in `labels`, GPU pod placement breaks for non-obvious reasons.
- **Returning a fresh `operation_id` on every retry.** Idempotency requires the same id across retries with the same target. Persist it.
- **Skipping `interruption_probability`.** Spot machines whose probability is 0 will get picked for high-penalty workloads, which is a correctness issue (effective_cost = price + p × penalty). Always set the real value.
- **Per-RPC timeouts that don't model your backend.** Cloud `Create` of 30–90s ≠ your provider's "request timeout" of 5s. Set transition timeouts to your backend's worst-case.
