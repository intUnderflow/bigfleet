# The `CapacityProvider` protocol and client

The provider boundary is the only place BigFleet touches real money and real machines. Everything upstream of it — the needs table, the decision engine, the shard's inventory — reasons over an in-memory model; the `CapacityProvider` is what turns a decision to provision into an actual host, and a decision to reclaim into an actual drain. This doc is about the *in-tree* half of that boundary: the Go interface the shard programs against (`pkg/provider`), the dial-out gRPC client that fences and converts (`pkg/provider/grpcclient`), the server-side adapter the conformance suite and tests use (`pkg/provider/grpcadapter`), the in-memory fake that stands in for a real provider in every test (`pkg/provider/fake`), and the engine-side reconcile loop that observes machine state through `List + Get` rather than a `Watch`. The wire-level field semantics and round-trip invariants are in [`wire-protocols.md`](wire-protocols.md) §`provider.proto`; the operator-facing how-to-build-one is in [`provider-author-guide.md`](../provider-author-guide.md). This doc covers the mechanics and the *why*, and does not repeat either.

## The two interfaces, and why there are two

There is a proto service (`bigfleet.v1alpha1.CapacityProvider`, `api/proto/bigfleet/v1alpha1/provider.proto:47-54`) and a Go interface (`provider.Provider`, `pkg/provider/provider.go:34-60`). They describe the same six RPCs but are deliberately not the same type. The Go interface uses domain types (`pkg/machine.Machine`, `machine.ID`, typed `GracePeriod`) where the proto uses generated structs and `map<string,string>`. The package doc states the reason (`pkg/provider/provider.go:5-13`): the shard's hot path runs against the Go interface and must not pay proto-runtime overhead per inventory entry, and an in-process fake must be writable without standing up gRPC. The proto↔domain conversion happens once, at the edge, in the gRPC client and adapter — never on the hot path.

This split is what makes the fake possible. The fake (`pkg/provider/fake`) implements `provider.Provider` directly, in process, with no gRPC at all — the decision engine, the shard, and the closed-loop simulator all drive it as a plain Go object. The same split is what makes the gRPC client (`pkg/provider/grpcclient`) the *only* in-tree code that talks to a real provider over the wire.

```
decision engine / shard reconcile
            │  (Go calls)
            ▼
   provider.Provider          ← domain types, no proto, no gRPC
   ┌────────┴─────────┐
   │                  │
pkg/provider/fake   pkg/provider/grpcclient.Client
(in-process,        (dials out, fences, converts proto↔domain)
 test-only)               │  gRPC + mTLS
                          ▼
              out-of-tree provider process
              (separate repo; pb.CapacityProviderServer)
```

## Six RPCs, no `Watch` — reconcile is `List + Get`

`Create`, `Configure`, `Drain`, `Delete`, `Get`, `List`. That is the entire surface (`provider.proto:47-54`, mirrored at `provider.go:34-60`). The four lifecycle RPCs are *asynchronous* — each returns a `TransitionAck` immediately and the real transition is observed later — and *idempotent* on `(machine_id, target_state)` via `operation_id` reuse. The two read RPCs are how the shard learns what actually happened.

There is no `Watch`, and the absence is a design decision, not an omission (`provider.proto:9-11`, plan §10.6). A `Watch` would make every shard hold a streaming subscription to its provider, re-introducing exactly the staleness-vs-liveness, reconnect-ordering, and missed-event problems that a poll-based `List + Get` sidesteps. The shard already runs a decision cycle on a timer; folding state observation into that same cycle as a `List` keeps the provider stateless about who is watching and keeps the shard's correctness independent of stream health. The cost is poll latency, which ADR-0004's incremental `List` makes cheap enough to ignore at scale.

The reconcile loop lives in `pkg/shard/reconcile.go`. `reconcile` (`reconcile.go:29-34`) picks one of two paths on `Config.IncrementalReconcile`:

- **Full (default, correct against any provider).** `reconcileFull` (`reconcile.go:39-62`) issues an unfiltered `Provider.List`, applies every returned machine, then walks the local inventory snapshot to find machines the provider no longer reports and removes them. Linear in inventory size.
- **Incremental (opt-in, conformance-gated).** `reconcileIncremental` (`reconcile.go:69-81`) passes the saved `SinceRevision` cursor, applies only the deltas, and advances the cursor from `resp.Revision`. O(deltas) per cycle.

`Get` (the provider RPC) is the per-machine read used off the reconcile path — e.g. confirming a single machine's state during action execution. Note that within `reconcile.go` and `execute.go` the frequent `s.inv.Get` calls are reads of the shard's *local* inventory, not provider RPCs; the provider `Get` is the targeted "what does the source of truth say about this one machine" call. The bulk observation channel is `List`.

### `since_revision`: opt-in, deltas only, and why the cursor is opaque (ADR-0004)

Pre-M11.22 `reconcile` issued a full `List` every cycle and processed the whole result. At 500K machines that was ~87% of per-cycle wall-clock (ADR-0004 Context). The fix was already on the wire — `ListFilter.since_revision` and `MachineList.revision` exist from v1alpha1 — but nothing honoured it. ADR-0004 added: a fake that honours the cursor, a shard path that uses it, and an explicit decision to **defer tombstones**.

The cursor is opaque bytes precisely so each provider chooses its own encoding without a wire change. The fake encodes it as a decimal of its internal mutation counter (`fake.go:565-617`): a cold-start (empty cursor) full-scans the map; a delta-`List` binary-searches an append-only `revLog` for the first entry past the cursor, dedups IDs, and emits current state — O(k + uniqueIDs) instead of O(N) (`fake.go:43-56`, `587-616`). A malformed cursor is treated as cold-start (`fake.go:624-633`), defensive against a real provider's encoding drift.

Two consequences worth internalising:

1. **The removal gap is real and deliberate.** A delta `List` only contains machines that *exist*; the proto has no tombstone field, so `reconcileIncremental` **skips the removal walk** (`reconcile.go:64-68`). This is safe for any provider whose `Delete` walks a machine back to `Speculative` (it stays in the inventory, state-changed) but unsafe for one that genuinely removes records. That is why `IncrementalReconcile` defaults off and is documented as something the operator must verify their provider supports. The tombstone wire extension is owed to a future ADR, to be designed against a real removal use-case rather than pre-emptively (ADR-0004 Decision/Consequences).
2. **The cursor is process-state, never persisted** (`reconcile.go:79`, ADR-0004). A shard restart loses it; the next reconcile is a cold-start full `List`. This is the data-plane static-stability story: a restarted shard re-bootstraps inventory from scratch and converges, exactly as it would after a crash or rolling deploy — no coordinator round-trip, no durable shard state to corrupt.

## The dial-out client (`pkg/provider/grpcclient`)

`grpcclient.Client` is the shard-side adapter: it implements `provider.Provider` over the proto service (`grpcclient.go:54-59`, compile-checked at `:239`). It is the only in-tree code that speaks to a real, out-of-tree provider (`grpcclient.go:5-6`). `New` dials lazily — gRPC connects on first call — and `tlsCfg` selects the transport per ADR-0048: zero value is plaintext (the legacy trust-the-network default), fully-set is mTLS with the shard presenting a cert whose URI SAN is `bigfleet://shard/<ShardID>` for the provider to authorize against (`grpcclient.go:61-89`).

Three things the client does that matter:

**It stamps the fencing token; the shard never does.** The `Identity` injected at construction (`grpcclient.go:43-52`) carries the shard's stable ID, its persisted restart `Epoch`, and a per-process `Sequence`. Every mutating call (`Create`/`Configure`/`Drain`/`Delete`) sets `shard_id`, `shard_epoch`, and a freshly-minted `sequence_number` from this identity (`grpcclient.go:98-148`). The `provider.Fence` field on the Go request structs is **server-side only** — the shard leaves it zero; the client populates the wire fields itself (`provider.go:77-90`). `Get` and `List` carry no token (`grpcclient.go:150-192`) — reads don't fence.

**A fresh sequence number per call attempt makes transport retries safe.** `c.id.Seq.Next()` is called once per RPC attempt, so a gRPC-level retry mints a new sequence and is never rejected as a replay (`grpcclient.go:9-11`, `100-102`). Idempotency — the property that a retried `Create` doesn't double-provision — is the provider's job, keyed on `(machine_id, target_state)` via `operation_id`, never on the token.

**It re-attaches the sentinel errors the shard matches on.** `mapStatusErr` (`grpcclient.go:215-226`) is the inverse of the adapter's `mapErr`: it wraps `codes.NotFound`→`ErrNotFound`, `codes.Unimplemented`→`ErrNotSupported`, and `codes.FailedPrecondition`→`ErrFenced`, while keeping the original gRPC status in the error chain (so `status.FromError` still unwraps and the shard's message-reading classifier sees the provider's verbatim text). `FAILED_PRECONDITION` is reserved on this service for fencing rejections (`provider.proto:32-34`), which is what lets the shard treat a fenced mutation as a zombie-shard *incident* (`ErrFenced`, "do not blind-retry", `provider.go:69-75`) rather than a retryable failure.

Default per-call deadlines apply only when the caller's context has none (`grpcclient.go:38-41`, `228-236`): 30s for lifecycle and `Get`, 2 minutes for `List`. The asymmetry is correct — lifecycle RPCs are async-accept so a short deadline bounds only the acknowledgement, not the (possibly hours-long) transition, while `List` scales with inventory and needs room.

## The server-side adapter (`pkg/provider/grpcadapter`)

`grpcadapter.Server` wraps a Go `provider.Provider` as a `pb.CapacityProviderServer` (`grpcadapter.go:22-34`). Production out-of-tree providers implement `pb.CapacityProviderServer` *directly* and never need this; the adapter exists so the in-tree fake can be exposed over a real gRPC port — for tests, and crucially for the conformance suite's self-test (`grpcadapter.go:1-6`). It does the proto→domain conversion on the way in and `mapErr` (the inverse of the client's `mapStatusErr`) on the way out (`grpcadapter.go:142-156`), with `FAILED_PRECONDITION` reserved for `ErrFenced` and everything unmapped — including invalid state transitions — collapsing to `Internal`.

## The validation boundary (ADR-0005)

The single most important architectural fact about this layer: **the provider boundary is the validation point, and reconcile trusts domain types.** ADR-0005 removed a belt-and-braces re-validation that round-tripped every reconciled machine through `MachineToProto + MachineFromProto` "to exercise the validation paths." At 500K inventory with 1000 deltas/cycle that round-trip was ~70ms of pure redundancy (ADR-0005 Context).

The responsibility split is now explicit (ADR-0005 Decision):

- **`pkg/provider/fake`** validates by construction — it builds `machine.Machine` values from typed inputs, so invalid combinations can't be written.
- **`pkg/provider/grpcadapter`** (and the client's `conv.MachineFromProto`) validate at the proto→domain conversion, once per RPC response, not once per machine.
- **`pkg/shard/reconcile`** assumes valid input and applies it; `inventory.Apply`'s own `machine.Invariant` + `machine.CheckTransition` checks (`pkg/inventory/inventory.go:89-103`) are the backstop for "bad data slipped through anyway."

There is one nuance the doc-as-of-ADR-0005 didn't anticipate, captured in the current code: `applyReconciledMachine` *does* screen records that take a slow path through `machine.Invariant` before they touch inventory (`reconcile.go:100-115`, `136-138`), because the production-readiness audit found nothing bounding provider-declared `price` / `interruption_probability` on this path — and `interruption_probability` feeds the cost formula directly. The state-match fast path (`reconcile.go:132-135`) is *not* screened because it ingests nothing. So "the boundary validates" is the rule; the reconcile-side `Invariant` screen is the narrow safety net for cost-formula inputs a misbehaving provider could otherwise poison.

`applyReconciledMachine` also encodes two non-obvious behaviours:
- It **skips in-flight machines** (`reconcile.go:129-131`): while a worker is driving a machine through a provider RPC, the local `applyTransition`-ed state is authoritative, because the provider's `List` view lags the in-flight RPC and would otherwise overwrite `Configuring` back to `Idle` and break the post-`Configure` transition.
- On a genuinely-new record it **decodes the provider-echoed `shard_metadata`** to recover assignment state (`reconcile.go:159-164`) — the M72 restart-rebuild path, where the echo is the only durable copy of the priority/penalty/fingerprint that Phase 2 victim scoring needs. The map itself is never carried onto the hot path; it is decoded and dropped (`reconcile.go:145`, `163`).

## `interruption_probability` is provider-declared only

`Machine.interruption_probability` is the provider's hourly forecast (for `SPECULATIVE`) or observation (for real machines), `[0,1]`, with **no cluster-side override and no max-merge** (`provider.proto:115-118`). This is a hard rule, not a default. It is one of the two inputs to the fixed cost formula `effective_cost = price + interruption_probability × interruption_penalty`; the formula is not pluggable and the probability is not configurable. The conformance suite enforces the bound (`TestConformance_CostFieldBounds`, `conformance_test.go:397-418`): `price_per_hour` must be non-negative and non-NaN, `interruption_probability` must be in `[0,1]` and non-NaN — a provider that emits garbage here fails compatibility, because garbage here corrupts every cost comparison the engine makes.

## The fake (`pkg/provider/fake`) — test-only, never deployed

The fake is an in-memory `CapacityProvider` used as a fixture by the decision engine, the shard, and the simulator (`fake.go:1-8`). It is **not a deployable artifact**: no Helm chart, no published image, no gRPC surface of its own (it gets one only when wrapped by `grpcadapter` for the self-test). Real providers live in separate repositories — the fake exists solely to exercise the engine without a real provider. The repo ships *zero* in-tree real providers by rule (next section).

What the fake models faithfully, and why it has to:

- **The full eight-state lifecycle through one `applyTransition`** (`fake.go:491-544`), which handles fencing, idempotent retry, failure injection, transition-validity (`machine.CanTransition`), and the instant-vs-staged mode. Each lifecycle method is a thin wrapper supplying a post-effect (`fake.go:414-460`): `Create` sets the host only once the record reaches `Idle`; `Configure` records the cluster and stores the `shard_metadata` verbatim; `Drain` clears the binding, the `Assigned*` fields, *and* the metadata together (it is per-assignment state, not per-machine); `Delete` clears the host only at `Speculative`.
- **The fencing high-water-mark contract** (`fake.go:462-489`). `checkFence` runs *before* the not-found check and before idempotent-retry short-circuiting (`fake.go:494-500`), per the proto contract: a zombie's request must not be applied, must not be answered with a cached `operation_id`, and must not learn whether the machine exists. A passing token advances the mark even if the operation then fails. A zero token (in-process harness construction) bypasses fencing entirely. The fake enforces this *because it is what the conformance self-test runs against* — it has to model the contract real providers are held to (`fake.go:65-71`).
- **`store-and-echo, never interpret` for `shard_metadata`.** The fake is the conformance reference, so it deliberately does **not** decode the well-known metadata keys into the `Assigned*` fields (`fake.go:422-432`): copying the verbatim map, unknown keys included, is the whole obligation. Decoding is the *shard's* job at reconcile ingest, not the provider's.

`InstantTransitions` (`Options`, `fake.go:127-139`) is the mode most tests use: a lifecycle call lands the machine in its stable target immediately rather than parking it in the transitional state. The two staging overrides (`ConfigureStaged`, `CreateStaged`) exist for the closed-loop sim's dwell models — they keep a machine in `Configuring`/`Creating` across cycles so the engine observes the in-flight runway the over-acquire investigations (ADR-0051 / M77g) turned on, with `CompleteStaged` (`fake.go:351-371`) driving the staged transition to completion when the sim's dwell budget elapses. The fake also exposes test hooks the wire contract has no analogue for: `FailNext` (inject a one-shot RPC error), `FailMachine` (force `Failed`, modelling a discovered spot reclaim), and `RemoveMachine` (hard host loss the next `List` surfaces as an absence).

## Out-of-tree by rule, and why

The repo ships the contract (`provider.proto`), the dial-out client (`grpcclient`), the adapter (`grpcadapter`), and the test fake — and no real providers. This is a hard rule (`provider.go:14-15`, `fake.go:3-7`). The reason is stated tersely in the package doc and worth stating in full: Kubernetes spent years undoing in-tree cloud providers (CCM) and in-tree storage drivers (CSI), because bundling provider code into the core binary couples every provider's release cadence, dependency tree, and security surface to the core's. BigFleet does not repeat that. A real provider is a separate process in a separate repo, implementing `pb.CapacityProviderServer`, reached only over gRPC through `grpcclient`.

The dev/laptop path is the one exception that proves the rule: with `--provider-addr` unset, `cmd/bigfleet shard` constructs the in-process fake and all the `--seed-*` / `--failure-rate-per-sec` knobs poke it directly (`cmd/bigfleet/shard.go:655-679`). Those knobs are *only* available against the fake — combining them with `--provider-addr` is a hard error (`shard.go:666-668`) — because they exist for kind/scaletest, never for a real fleet. With `--provider-addr` set, the shard dials the real provider via `grpcclient.New(addr, Identity{ShardID, Epoch}, tlsCfg)` and fences every mutating call with the same persisted epoch it advertises everywhere else (`shard.go:669-675`).

## The conformance suite is the compatibility gate

`test/conformance/` is the suite an out-of-tree provider runs to *claim* BigFleet compatibility — "a passing run = the provider is BigFleet-compatible" (`conformance_test.go:1-18`). It is built behind the `conformance` tag, takes the provider's gRPC address via `-target` or `BIGFLEET_PROVIDER_TARGET`, seeds a handful of speculative slots, and walks the contract. It asserts, among others (`conformance_test.go`, `fencing_test.go`, `metadata_test.go`):

- **Full lifecycle** `Speculative → Idle → Configured → Idle → Speculative` (`TestConformance_FullLifecycle`).
- **Idempotency** of each of `Create`/`Configure`/`Drain`/`Delete` (`*Idempotent`).
- **Read semantics**: `Get`/`Delete` on unknown machines, `List` state-filtering and `MaxResults`, label and cost-field shape, transitional-state observability, and revision advancement (`TestConformance_ListRevisionAdvances` — opt-in: a below-threshold provider may return a constant revision, but a provider that *does* advance it must change it after a mutation, `conformance_test.go:420-452`).
- **Invalid transitions** rejected (`DrainOnSpeculative`, `DeleteOnConfigured`).
- **Drain grace timeout** behaviour (`TestConformance_DrainGraceTimeout`).
- **The five fencing properties** (`fencing_test.go`): unknown shard accepted and establishes the mark; stale epoch rejected; stale/equal sequence rejected; a new epoch resets the sequence space; reads unaffected by the mutation-side high-water mark. Each test uses a run-unique `shard_id` so repeated runs against a long-lived provider don't collide (`fencing_test.go:26-29`).
- **`shard_metadata` store-and-echo** (`metadata_test.go`): echoed verbatim on `Get`/`List`, cleared with the binding on `Drain`, and unknown keys preserved byte-for-byte.

The suite keeps itself honest with a self-test (`TestConformance_SelfTest_OnFake`, `selftest_test.go`): it stands the in-tree fake up behind `grpcadapter` on a random localhost port and runs the *whole* conformance suite against it as a child `go test` process (`selftest_test.go:31-103`). This proves the suite is internally consistent and that the fake genuinely satisfies the contract real providers are graded against — the fake is the conformance reference precisely because it is the thing the conformance suite continuously validates itself with. Run it via `make conformance TARGET=host:port`.

## What an out-of-tree provider must get right (the short version)

For the full author guide see [`provider-author-guide.md`](../provider-author-guide.md). From the in-tree machinery's perspective, the load-bearing contracts are: lifecycle RPCs accept asynchronously and are idempotent on `(machine_id, target_state)`; reconciliation is `List + Get`, with `since_revision` optional and conformance-gated; `interruption_probability` and `price_per_hour` are provider-declared and must satisfy the cost-field bounds; fencing is enforced on the four mutating RPCs with `FAILED_PRECONDITION` reserved for it; and `shard_metadata` is stored, echoed, and cleared-with-the-binding but never interpreted. Get those right and the dial-out client, the reconcile loop, and the restart-rebuild path all work unchanged.
