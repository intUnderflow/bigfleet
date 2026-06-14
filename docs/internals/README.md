# BigFleet internals

Code-level deep-dives for contributors and maintainers. These pages bridge the gap between [`../architecture.md`](../architecture.md) — which sketches the two tiers and the three phases at a tour altitude — and the source itself. Each one picks a single subsystem, opens the files, and explains the *why*: the constraint, the paper section or ADR that fixed it, and the failure it prevents. Read [`../architecture.md`](../architecture.md) first for the shape, then come here for the function-by-function detail and the `*_test.go` that keeps each invariant honest. If instead you are *deploying* or *extending* BigFleet, you want the guides under [`../index.md`](../index.md) (operator-guide, provider-author-guide, api-reference); these internals pages assume you already have, and deliberately go deeper rather than repeat them.

These pages also assume you have skimmed the [BigFleet paper](https://lucy.sh/bigfleet) (vendored at [`../papers/bigfleet.md`](../papers/bigfleet.md)) and the operating-model paper (vendored at [`../papers/fleet-scale-kubernetes.md`](../papers/fleet-scale-kubernetes.md)). They link to the papers rather than re-deriving them.

## Source-of-truth ordering

Every page applies the same authority order (from [`../index.md`](../index.md)). When code and a higher source disagree, the higher source wins **and the page documents the divergence explicitly** rather than papering over it:

1. The two papers — [`../papers/bigfleet.md`](../papers/bigfleet.md), [`../papers/fleet-scale-kubernetes.md`](../papers/fleet-scale-kubernetes.md).
2. Author decisions in [`../adr/`](../adr/).
3. [`../plan.md`](../plan.md).
4. The code.

For the canonical ADR status table (Accepted / Proposed / Rejected / Superseded / Amended) see [`../adr/index.md`](../adr/index.md). For the ADR→code cross-reference — *where* each decision is realised and *which test guards it* — see [`decision-map.md`](decision-map.md); it is the maintainer's companion to these pages.

## Reading order

New to the internals? Read in this order:

1. [`data-flow.md`](data-flow.md) — the end-to-end picture, an unschedulable pod becoming a bound node.
2. [`decision-engine.md`](decision-engine.md) — the heart: the worker loop and three phases.
3. [`shard-hot-path.md`](shard-hot-path.md) — the loop that runs the engine, and the concurrency model around it.

Then drill into whichever subsystem you are changing, using the grouped table below.

## Decision & capacity

The engine that turns demand into provisioning. Start here if you are touching `pkg/decision`, `pkg/needs`, or `pkg/machine`.

| Doc | Covers | Read when |
|-----|--------|-----------|
| [`decision-engine.md`](decision-engine.md) | The decision engine: the per-cycle worker loop and the three fixed phases — Phase 1 (assign), Phase 2 (preempt inversions), Phase 3 (reclaim excess) — plus the fixed `effective_cost` and victim-score arithmetic. | Changing any provisioning behaviour, or reasoning about why a machine was acquired, preempted, or released. |
| [`phase1-occ.md`](phase1-occ.md) | Phase 1 internals: the Omega-style optimistic-concurrency assignment — how candidate idle inventory is selected by effective cost, claimed, and how conflicting claims resolve within a cycle. | Working on Phase 1 assignment, idle-tiebreak ordering, or concurrency in the assign step. |
| [`machine-lifecycle.md`](machine-lifecycle.md) | The machine state machine: three stable + four transitional + Failed states, the legal transitions, and which provider RPC drives each edge. | Touching `pkg/machine`, adding a transition, or debugging a stuck machine. |
| [`needs-table.md`](needs-table.md) | NeedsTable, profiles, powers-of-2 penalty bucketing, and `Same`-folding — how full-replacement roll-ups become the priority-sorted demand the engine walks. | Changing demand ingestion, penalty buckets, or `Same`-operator handling. |

## Shard & coordinator

The two tiers. Start here for `pkg/shard` (the hot path, autonomous) or `pkg/coordinator` (the Raft tier).

| Doc | Covers | Read when |
|-----|--------|-----------|
| [`shard-hot-path.md`](shard-hot-path.md) | The shard controller hot path: the cycle loop, inventory snapshotting, session multiplexing on the one bidi stream per cluster, and the lock-light concurrency model. Includes the no-coordinator-dependency guard (`pkg/shard/no_coordinator_dep_test.go`). | Changing `pkg/shard`. Mandatory read before any commit that touches the hot path. |
| [`coordinator-raft.md`](coordinator-raft.md) | The coordinator: `hashicorp/raft` over BoltDB, the FSM, cluster→shard and topology-domain→shard assignment, quota allocation, and ordinal join / offline-restore (ADR-0047). | Changing `pkg/coordinator`, replication, or assignment. |
| [`static-stability.md`](static-stability.md) | Static stability: how clusters keep running with BigFleet entirely down, why `pkg/shard` must not import `pkg/coordinator`, and the class of designs this rules out. | Before any change that could put a coordinator dependency on the hot path, or weaken autonomous operation. |

## Protocols & identity

The wire. Start here for anything in `api/proto`, `api/crd`, `pkg/provider`, or `pkg/fencing`.

| Doc | Covers | Read when |
|-----|--------|-----------|
| [`wire-protocols.md`](wire-protocols.md) | Wire protocols and CRDs in depth: `capacity.proto`, `shard.proto` (the operator-initiated bidi `Session`), `coordinator.proto`, `provider.proto`, the CRDs, full-replacement roll-up semantics, and `supersedes_key` stream coalescing. | Changing any proto or CRD, or reasoning about stream/reconnect ordering. |
| [`provider-protocol.md`](provider-protocol.md) | The `CapacityProvider` protocol and client: the six RPCs (Create / Configure / Drain / Delete / Get / List — **no Watch**), `List + Get` reconciliation, the dial-out client and plugin registry, and the test-only fake (`pkg/provider/fake`, never deployed). | Implementing a provider (pair with [`../provider-author-guide.md`](../provider-author-guide.md)) or changing `pkg/provider`. |
| [`fencing-and-identity.md`](fencing-and-identity.md) | Fencing and mTLS identity: the term / epoch / sequence helpers in `pkg/fencing`, and the `bigfleet://` URI-SAN identity binding (ADR-0048, superseding ADR-0008's transport posture). | Changing fencing, stale-write protection, or transport identity. |

## Operator & lifecycle

The cluster-side agent and the optional pod controller.

| Doc | Covers | Read when |
|-----|--------|-----------|
| [`operator-and-controllers.md`](operator-and-controllers.md) | The operator and the unschedulable-pod controller: outbound-only dial, the multiplexed `Shard.Session` stream, `CapacityRequest` CR → NeedsTable aggregation, write-back of `AvailableCapacity` / `UpcomingNode`, and the optional `bigfleet-unschedulable-pod-controller`. | Changing `pkg/operator` or `pkg/controller/cr`. |

## Scale & testing

How we prove it works and how it survives load.

| Doc | Covers | Read when |
|-----|--------|-----------|
| [`scaletest-harness.md`](scaletest-harness.md) | The scale-test harness architecture: the synthetic Go simulator (`make scale`), the kind rung, the `sim/` workload generators and scenarios, profiles, and how demand is generated and measured. | Working on the harness, a scale profile, or interpreting a scale run. |
| [`testing-and-validation.md`](testing-and-validation.md) | Testing taxonomy and the validation ladder: unit / property / integration / conformance / e2e, and the `prevalidate → kind → cloud` ladder from [`../scaletest.md`](../scaletest.md). | Deciding where a test belongs, or before filing a cloud brief. |

## Cross-cutting

The threads that run through every subsystem.

| Doc | Covers | Read when |
|-----|--------|-----------|
| [`data-flow.md`](data-flow.md) | End-to-end data flow: an unschedulable pod becoming a bound node, traced through CR → operator → roll-up → NeedsTable → decision engine → provider → machine state → CR write-back. | Onboarding, or tracing a request across component boundaries. |
| [`domain-attribution.md`](domain-attribution.md) | The domain-attribution saga: how `Same`-domain supply crediting evolved across ADR-0040 → ADR-0051 — unified attribution, sub-machine folding, sticky choice, aged-acquisition parking, consumed-capacity model, and gang-granular attribution. | Touching `Same`-domain supply crediting, or debugging a domain-choice flap. |
| [`observability.md`](observability.md) | Metrics and observability catalog: the emitted metrics, what each measures, and which are load-bearing SLO signals (cycle p99, phase trends). | Adding a metric, wiring a dashboard, or reading a scaletest's Grafana. |

---

If you find a divergence between any of these pages and a higher source-of-truth, fix the code or the page and note it — the ordering above is the project's source-of-truth policy. Back to the documentation landing page: [`../index.md`](../index.md).
