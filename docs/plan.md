# BigFleet Reference Implementation — Plan

This document is the comprehensive plan for the BigFleet reference implementation. It is written against the two papers in `docs/papers/` (always read those before changing design) and the design decisions in the project memory.

This plan is a target shape, not a runbook. Milestones at the bottom break it into ordered, shippable chunks.

---

## 0.1 Decisions required before implementation

These five items from §10 affect wire format or core data model. They cannot be deferred — once a proto / Raft schema ships and the first provider or cluster speaks it, changing any of them is a breaking change. Lock answers in (or accept the noted defaults) before M0 closes.

### A. Coordinator assignment granularity (from §10.1)
**Question**: does the coordinator persist `machine_id → shard` (100M entries, ~500MB) or `topology_domain → shard` (~100K entries, ~5MB)?

**Recommendation**: topology-domain. Shards rebuild their per-machine view from `provider.List()` filtered by their domains. Memory is in the design memory ("fixed machines are assigned to shards at topology-domain granularity") — the original plan diverged from that. **Default: topology-domain.**

**Affects**: coordinator Raft state machine schema, shard bootstrap path, `coordinator.proto`.

### B. Penalty bucketing in roll-up aggregation (from §10.3)
**Question**: do `CapacityNeed` aggregation keys include the raw `interruption_penalty` / `reclamation_penalty` (and risk 50K-entry roll-ups when penalties are workload-specific), or do we bucket them to a coarse log scale (e.g., powers of 2 in dollars) and accept bounded rounding error?

**Recommendation**: bucket. Document the boundaries on the `CapacityRequest` CRD so users see them; the cost-function effect of rounding to the nearest power of 2 is bounded and small relative to the spread between spot and on-demand prices. **Default: powers of 2, range $0.50 to $10M, bucket boundaries published as part of the v1alpha1 CRD.**

**Affects**: `capacity.proto` aggregation contract, `CapacityRequest` CRD documentation, operator roll-up logic.

### C. Provider `List` incremental contract (from §10.6)
**Question**: does `ListFilter` include a cursor / since-revision field in v1alpha1, or do we accept that providers re-send full state every reconcile and revisit later?

**Recommendation**: bake `since_revision` (opaque bytes) into `ListFilter` from day one. Providers below a documented threshold may ignore it and return full state; providers above the threshold must support it. The conformance suite enforces this. Adding it later is a breaking proto change; adding it now costs one optional field.

**Default: optional `since_revision` bytes field on `ListFilter`, threshold-gated requirement in conformance suite.**

**Affects**: `provider.proto`, conformance suite design.

### D. Stream coalescing semantics (from §10.5)
**Question**: when the shard sends `NodeStateUpdate` / `AvailableCapacityUpdate` frames down the operator stream, do we mark them as "supersedes prior frame for key K" explicitly in the proto, or assume operators apply last-write-wins implicitly?

**Recommendation**: explicit. Add a `supersedes_key` field on coalescing message types. The shard's outbox can then safely drop the older frame when a newer one arrives with the same key, and the operator can reason about ordering. Implicit last-write-wins works at small scale but breaks under reconnection where ordering is not guaranteed.

**Default: explicit `supersedes_key` on every coalescing message; shard outbox drops superseded frames; operator applies in arrival order.**

**Affects**: `shard.proto` (stream message types), shard outbox implementation, operator handler.

### E. Cross-region coordinator topology (from §10.9)
**Question**: is the v1 coordinator a single Raft group in one region, a Raft group across regions, or a hierarchy?

**Recommendation**: single Raft group, single region (the operator's choice of which). Cross-region fleets accept that the coordinator's region is the SPOF for fleet-wide rebalancing — static stability covers the data-plane impact. Multi-region coordinator is post-v1; document the tradeoff in the scaling guide.

**Default: single Raft group, single region. Documented tradeoff. ADR captured in M0.**

**Affects**: deployment shape, ADR list, but *not* the wire format — so this is the cheapest of the five to revisit.

---

The other five §10 items (coordinator write bursts, Phase 2 indexing, end-to-end latency event-driven path, DR snapshots, cluster etcd headroom) are **implementation concerns** — they live behind stable wire formats and stable data models and can be tackled in their natural milestones without locking us in now.

---

## 0. Goals & non-goals

### Goals
- A faithful implementation of the two-tier (coordinator + shards) architecture, even at small scale (single binary, single shard).
- The capacity contract (CRDs + protobuf) exactly as specified — implementations that match the papers should interoperate with this one.
- Pluggable `CapacityProvider` backends. We ship at least:
  - An **in-memory / fake** provider for tests and demos.
  - (No real providers — see §3.3. Providers are out-of-tree, separate repos, dialed via gRPC.)
- A reference per-cluster operator that does the roll-up, GenerateBootstrap, UpcomingNode/AvailableCapacity writing, and reclaim signalling.
- Static stability: clusters keep running with BigFleet stopped.
- A simulation harness (Borg/Twine "Fauxmaster"-style) that can replay synthetic and recorded traces against the decision engine without touching real infra.
- End-to-end demo: kind-based multi-cluster setup driven by a single BigFleet binary plus a fake provider.

### Non-goals (explicitly)
- Quota, admission, multi-tenancy, chargeback. Out of scope per the paper.
- Cluster lifecycle (creation/deletion, control-plane upgrade). BigFleet has no opinions.
- Cloud commitment management (RIs, Savings Plans).
- A web UI. `kubectl` + structured logs + Prometheus is the bar.
- Any in-tree real providers — they live in separate repos.
- "Watch" RPCs on the provider interface — six methods only, reconciliation via `List + Get`.
- Cross-shard topology resolution. A `Same` request that can't be satisfied within a shard becomes a shortfall, never a coordinator-resolved cross-shard placement.
- Cluster-supplied `interruption_probability` overrides. Provider-declared only.

---

## 1. Repo layout

```
bigfleet/
├── api/                              # all wire formats
│   ├── proto/
│   │   ├── bigfleet/v1alpha1/
│   │   │   ├── capacity.proto         # CapacityNeed, ClusterCapacityNeeds, NodeSelectorRequirement, TopologySpread
│   │   │   ├── shard.proto            # Shard service (operator-initiated bidi stream)
│   │   │   ├── provider.proto         # CapacityProvider service + Machine, MachineState, ListFilter
│   │   │   └── coordinator.proto      # internal: coordinator ↔ shard (instructions, shortfalls, fencing)
│   │   └── buf.yaml / buf.gen.yaml
│   └── crd/
│       └── bigfleet.lucy.sh_*.yaml       # CapacityRequest, AvailableCapacity, UpcomingNode
├── pkg/
│   ├── apis/bigfleet/v1alpha1/        # Go CRD types (controller-runtime / kubebuilder generated)
│   ├── machine/                       # Machine struct, state machine, transition validation
│   ├── needs/                         # NeedsTable (priority-sorted, full-replacement per cluster)
│   ├── inventory/                     # in-memory inventory per shard
│   ├── decision/                      # the worker loop: Phase 1, 2, 3
│   │   ├── phase1_assign.go
│   │   ├── phase2_inversions.go
│   │   ├── phase3_reclaim.go
│   │   └── cost.go                    # effective_cost, victim score, drain grace
│   ├── shortfall/                     # shortfall detection, aging, escalation
│   ├── shard/                         # shard controller (loop, fencing, RPC servers)
│   ├── coordinator/                   # global coordinator (Raft, state, rebalancing)
│   │   ├── raft.go                    # embedded raft + BoltDB
│   │   ├── state.go                   # cluster→shard, machine→shard, quota
│   │   ├── rebalance.go               # idle/speculative reassignment
│   │   └── crosspreempt.go            # cross-shard preemption
│   ├── provider/                      # CapacityProvider *client* + registry the shard uses to dial out-of-tree providers
│   │   └── fake/                      # in-memory test fixture only (NOT a shipped provider)
│   ├── operator/                      # the per-cluster cluster-operator agent
│   │   ├── informer.go                # CR/UpcomingNode/AvailableCapacity informers
│   │   ├── rollup.go                  # aggregate by profile, send InfrastructureAutoscaler
│   │   ├── bootstrap.go               # GenerateBootstrap server
│   │   ├── upcoming.go                # write UpcomingNode CRs from BigFleet's response
│   │   └── reclaim.go                 # receive reclaim instructions, signal kubelet
│   ├── controller/cr/                 # optional: per-pod CR controller (ships separately)
│   └── fencing/                       # term, epoch, sequence helpers
├── cmd/
│   ├── bigfleet/                                  # single binary that can run as coordinator and/or shard
│   ├── operator/                                  # the cluster-operator agent
│   ├── bigfleet-unschedulable-pod-controller/     # optional per-pod CR controller
│   └── faux/                                      # simulation harness CLI
├── sim/                               # the Fauxmaster-style simulator
│   ├── workload/                      # synthetic workload generators
│   ├── scenario/                      # YAML scenario files (training job, stockout, etc.)
│   └── replay/                        # checkpoint replay
├── test/
│   ├── e2e/                           # kind-based multi-cluster e2e tests
│   └── integration/                   # in-process multi-component tests
├── docs/
│   ├── papers/                        # source papers
│   ├── plan.md                        # this file
│   └── adr/                           # architecture decision records as we go
├── deploy/
│   ├── helm/                          # charts for bigfleet, operator, optional CR controller
│   └── kind/                          # kind cluster configs for e2e
├── go.mod
├── Makefile
└── .github/workflows/                 # CI
```

---

## 2. Wire formats

### 2.1 CRDs (cluster-side, group `bigfleet.lucy.sh`, version `v1alpha1`)

- `CapacityRequest` (namespaced). Fields per paper §6.1, including `interruptionPenalty` and `reclamationPenalty` on the spec. Status: `phase: Pending | Acknowledged`. Uses ownerRefs for GC.
- `AvailableCapacity` (cluster-scoped, namespace `fleet-system`). Hint with confidence (`High|Medium|Low|None`), price, atomic-provisioning flag, ETA, node template. Eventually consistent.
- `UpcomingNode` (cluster-scoped, namespace `fleet-system`). Status phases: `Provisioning | Launched | Registered | Ready | Failed`. Includes labels, resources, taints, providerID.

Shipped as both raw YAML in `api/crd/` and Go types under `pkg/apis/bigfleet/v1alpha1/`. Generated using `controller-gen`.

### 2.2 Protobuf services

`api/proto/bigfleet/v1alpha1/`:

- **`Shard`** (cluster operator → BigFleet shard). One RPC, operator-initiated bidirectional stream:
  - `Session(stream OperatorMessage) returns (stream ShardMessage)`.
  - The operator dials out to the shard and holds a long-lived stream. **No inbound listener is required on the operator.** All cluster ↔ shard traffic is multiplexed on this one connection.
  - **Operator → shard messages** (`OperatorMessage`):
    - `Hello{cluster_id, capabilities}` — first frame. Establishes identity and negotiates protocol features.
    - `ClusterCapacityNeeds{...}` — periodic full-replacement roll-up.
    - `BootstrapBlobResponse{request_id, user_data, ttl_seconds, error}` — replies to a prior `BootstrapRequest` from the shard, correlated by `request_id`.
    - `ReclaimAck{instruction_id}` — optional, confirms reclaim instruction was received and acted on.
  - **Shard → operator messages** (`ShardMessage`):
    - `Acknowledgement` — for roll-ups.
    - `BootstrapRequest{request_id, requirements}` — the shard wants a kubelet bootstrap blob for the given requirements. Operator must respond with a `BootstrapBlobResponse` referencing the same `request_id`. Replaces the previous push-style `GenerateBootstrap` RPC.
    - `ReclaimInstruction{instruction_id, nodes, grace_period}` — drain these nodes with this grace period.
    - `NodeStateUpdate` — feeds `UpcomingNode` CR phase transitions.
    - `AvailableCapacityUpdate` — feeds the `AvailableCapacity` CRDs.
  - Reconnection: streams are stateless. On disconnect the operator reopens; the shard re-issues any unanswered `BootstrapRequest` and any unacked `ReclaimInstruction` on the new stream. Idempotency is keyed by `request_id` / `instruction_id` so retries are safe.
- **`CapacityProvider`** (BigFleet shard → provider). Six RPCs: `Create, Configure, Drain, Delete, Get, List`. Async, idempotent.
- **`Coordinator`** (internal, shard ↔ coordinator). RPCs:
  - Shard → coord: `ReportShard(ShardReport)` (carries summary, shortfalls; idempotent per cycle).
  - Coord → shard: `Instruct(CoordinatorInstruction)` (rebalance, drain, transfer ownership). Carries `(coordinator_term, sequence_number)`.

`NodeSelectorRequirement.operator` includes `Same` (protobuf-only — translated by the cluster operator from CRD-level co-location signals).

### 2.3 Build

`buf` for proto generation. `controller-gen` for CRDs and deepcopy. A single `make generate` regenerates everything.

---

## 3. Component design

### 3.1 Cluster operator (per-cluster agent)

Runs in each cluster. Stateless, uses informers. **Outbound-only networking** — the operator dials the shard and holds a single bidirectional stream. No inbound listener, no service of type LoadBalancer/NodePort, no firewall holes punched into the cluster.

The stream (`Shard.Session`) is the one transport for everything cluster ↔ shard.

Responsibilities:
1. **Connect & hello**: dial the shard, send `Hello{cluster_id, capabilities}`, then keep the stream open. Reconnect on close with backoff.
2. **Roll-up** every 10s on the existing stream:
   - Watch all `CapacityRequest` objects (informer cache).
   - Group by `(requirements ∪ {Same translation}, resources, priority, topologySpread, interruptionPenalty, reclamationPenalty)`.
   - Emit one `CapacityNeed` per group with `count = group size`.
   - Send `ClusterCapacityNeeds` (full-replacement) up the stream.
   - On the first send that includes any `Pending` CR, mark it `Acknowledged` (single status write per CR, ever).
3. **Bootstrap responder**: when a `BootstrapRequest{request_id, requirements}` arrives down the stream, generate a blob (kubelet config + bootstrap token + CA bundle) on demand and send `BootstrapBlobResponse{request_id, user_data, ttl_seconds}` back up. If the cluster cannot satisfy the requirements (e.g., asked-for kubelet version outside skew policy), reply with `error` populated; the shard treats this as an unsatisfiable requirement.
4. **UpcomingNode writer**: `NodeStateUpdate` frames coming down the stream drive `UpcomingNode` CR phase transitions (`Provisioning → Launched → Registered → Ready → Failed`). The kubelet's node-Ready event is the local ground truth for `Ready`.
5. **AvailableCapacity writer** (optional): `AvailableCapacityUpdate` frames are written through to `AvailableCapacity` CRs.
6. **Reclaim handler**: on `ReclaimInstruction{instruction_id, nodes, grace_period}` from the shard, cordon and signal graceful node shutdown with the supplied grace. Send `ReclaimAck{instruction_id}` back up. We do not bypass PDBs; the grace period is what we pass to kubelet, not a workaround.

Authentication: the operator authenticates to the shard with mTLS (per-cluster cert) or a short-lived bearer token. Trust direction is operator → shard only, matching the network direction.

### 3.2 Optional per-pod CR controller (`bigfleet-unschedulable-pod-controller`)

Ships as a separate binary. Watches Pods with `PodScheduled=False, reason=Unschedulable`. For each, creates a `CapacityRequest` with:
- ownerRef to the pod
- requirements from pod's nodeAffinity/nodeSelector
- resources from the pod's requests
- priority from `pod.Spec.Priority`
- topologySpread from `pod.Spec.TopologySpreadConstraints`
- interruptionPenalty/reclamationPenalty from pod annotations (`bigfleet.lucy.sh/interruption-penalty`, `…/reclamation-penalty`); default to conservative non-zero values when absent.

Idempotent (keyed by pod UID). Suppresses creation when an existing UpcomingNode would satisfy the pod (best-effort check via labels).

### 3.3 CapacityProviders are out-of-tree

Kubernetes spent years untangling in-tree cloud providers (CCM, CSI). We do not repeat that mistake. **The BigFleet repo ships zero real providers.** It ships:

- The `provider.proto` contract (in `api/proto/`).
- A Go client + plugin registry under `pkg/provider/` that the shard uses to dial registered out-of-tree providers.
- A `provider/fake/` package that is *only a test fixture* — it lives in-tree because it's used by unit / component / simulator tests, never deployed.
- A **provider author guide** under `docs/` that documents the contract, idempotency rules, transition timeouts, label conventions for topology and capacity-type, and the recommended deployment shape (a separate process / pod / repo per provider).

Real providers — AWS, GCP, Azure, bare-metal frameworks like MAAS / Tinkerbell / Ironic, on-prem clouds — live in **separate repositories**, are released independently, and are dialed by BigFleet shards over gRPC like any other backend. They can be written in any language, vendored separately, and version-skewed independently of BigFleet.

Common contract (enforced by the proto + the conformance test suite, which is shipped from this repo):
- All four lifecycle RPCs return immediately with a `TransitionAck{operation_id, machine}`.
- Idempotent on `(machine_id, target_state)`.
- Each transitional state has a configured timeout; on timeout the machine flips to `Failed` with `last_error`.
- `List` returns machines in any state subset (caller filters).

We will publish a **provider conformance suite** (`test/conformance/`) that any provider can run against itself: spin up your provider process, point the suite at it, and it exercises the lifecycle, idempotency, timeout, and label-shape requirements end-to-end. Passing the suite is the bar for being called a BigFleet-compatible provider.

#### `provider/fake` (test fixture, in-tree)
A purely in-memory provider used by tests and the simulator. Configurable:
- Initial speculative quota (instance type × zone × count).
- Synthetic provisioning latency (per-state).
- Synthetic interruption probability (per offering).
- Failure injection (rate, deterministic seed).

This is the only in-tree provider, and it is explicitly not a deployable artifact — it has no Helm chart, no published image, and exists solely so the engine can be tested without a real backend.

### 3.4 Shard controller

Runs as part of the `bigfleet` binary (the same binary can also be the coordinator).

Owns:
- An in-memory `NeedsTable` (priority-sorted, replaced per-cluster on each roll-up).
- An in-memory `Inventory` of machines (state machine, stable + transitional + failed).
- A bounded shortfall buffer (top 100 by priority).
- A per-cluster **outbox** of pending instructions to send down the operator stream: open `BootstrapRequest`s (keyed by `request_id`), open `ReclaimInstruction`s, queued `NodeStateUpdate` and `AvailableCapacityUpdate` frames. The outbox is in-memory and rebuilt on shard restart from inventory state — instructions are reissued on stream reconnect.
- Fencing: tracks coordinator term high-water mark, increments its own shard epoch on restart.

Loop:
1. **Ingest**: stream reader handles inbound `OperatorMessage` frames — updates `NeedsTable` from roll-ups, resolves outstanding `BootstrapRequest`s by `request_id` from `BootstrapBlobResponse`, clears `ReclaimAck`s.
2. **Decide** (every cycle, default 10s, configurable, also event-driven on roll-up arrival but rate-limited):
   - Phase 1 (assign): per [ADR-0029](adr/0029-phase1-omega-style-occ.md), an Omega-style OCC dispatcher races GOMAXPROCS workers over a shared Need queue, each submitting proposals to a single commit broker that arbitrates conflicts on `(priority, interruption_penalty, reclamation_penalty)` precedence. The outcome ("walk by priority") is preserved at commit time, not enforced via a priority-sorted outer loop. Idle first (one bootstrap), then speculative (Create + bootstrap). When a bootstrap blob is needed, enqueue a `BootstrapRequest` on the cluster's outbox; the actual `Configure` provider call is held until the operator's `BootstrapBlobResponse` arrives.
   - Phase 2 (inversions): for each unsatisfied high-priority need, score victims; enqueue `ReclaimInstruction` frames on the donor cluster's outbox with grace period scaled by priority gap.
   - Phase 3 (reclaim excess): per-cluster diff against roll-up; reclaim cheapest-per-hour first, breaking ties by lowest reclamation_penalty.
3. **Execute**: send async RPCs to providers; track ops; reconcile state on next cycle. Drain `outbox`es to the corresponding open operator streams (drop / reorder safely if a stream is currently disconnected — they'll be reissued on reconnect).
4. **Report** (every 30s): send summary + shortfalls to coordinator. Stateless; coordinator pulls latest.
5. **Reconcile**: re-read provider state via `List(states={...})` to catch transitions started elsewhere or that have been stuck.

Hot path is in-memory and lock-light: a single goroutine owns the inventory and needs table. RPC handlers post deltas via channels.

### 3.5 Global coordinator

Single logical instance. 3 replicas, Raft consensus.

Backing: **embedded Raft (`hashicorp/raft` is the working choice; `etcd/raft` is the alternative) + BoltDB**. State machine is the small set of maps in memory; durable log + snapshots on local disk per replica.

State (reconstructable from Raft log):
- `shards`: shard membership, addresses, last heartbeat.
- `cluster_to_shard`: map (set on first contact, permanent).
- `machine_to_shard`: map (the big one — ~500MB at 100M nodes; mostly id pairs).
- `quota_allocations`: per `(provider, region, shard)` slice.
- `provider_registry`: configured backends.

Behaviour:
- Accepts `ReportShard` from any shard.
- Sends `Instruct` to shards with `(term, seq)` for fencing.
- Periodically (e.g., every 30s) computes rebalancing actions from aggregated shortfalls:
  1. Move idle machines from over-supplied shards to shortfall shards.
  2. Move speculative quota slices.
  3. Cross-shard preemption: pick lowest-priority configured machines on donor shards with a profile match; instruct donor shard to drain → idle, then transfer ownership.
- On startup of a new shard, assigns initial machine slices (topology-domain granularity).
- Adds a cluster to the `cluster_to_shard` map on first roll-up to a previously unknown cluster, picking the least-loaded shard.
- Shard split/merge: triggered by thresholds, slow operation. Out of v1 scope as automated; ship the manual primitives.

Followers don't serve writes. Clients (shards) talk to any replica; followers redirect or forward. Standard Raft-aware client pattern.

### 3.6 Static stability

Demonstrated by tests:
- Kill all coordinator replicas → shards keep servicing roll-ups, keep provisioning from their existing inventory and speculative quota, keep doing in-shard preemption. Only cross-shard rebalancing pauses.
- Kill a shard → its clusters' running pods stay running. New CRs go unsatisfied until the shard restarts. On restart, shard reads its assignments from the coordinator and rebuilds inventory by `List`-ing each provider for its shard slice.
- Kill BigFleet entirely → clusters keep operating at current capacity.

---

## 4. The simulation harness (`cmd/fauxctl`)

Modelled on Borg's Fauxmaster. Drives the same `decision`, `inventory`, `needs`, `shortfall` packages used in production, with synthetic providers and synthetic clusters.

Capabilities:
- Replay scenario YAMLs (`sim/scenario/`): training job with topology, capacity stockout, withdrawal, priority inversion, full-fleet preemption.
- Generate synthetic workloads: Poisson arrivals with profile mix, training-job bursts, batch tails.
- Inject failure: provider rate-limit storms, leader election, shard restart.
- Output: structured event log, per-cycle metrics (decision time, queue depth, shortfall age), end-of-run report (utilization, cost, interruption count).
- Determinism: seedable RNG; given a scenario + seed, results are reproducible.

This is also the first integration test — the decision engine should be fully exercisable through the simulator before any real provider is wired.

---

## 5. Test strategy

| Layer | Tooling | Coverage |
|-------|---------|----------|
| Unit | `go test`, table-driven | State machine transitions, cost math, victim score, drain grace, full-replacement roll-up semantics, fencing token comparisons, NeedsTable ordering |
| Component | `go test` w/ in-process gRPC | Shard ↔ fake provider, shard ↔ fake operator, coordinator ↔ shard, Raft replicated state |
| Simulation | `cmd/fauxctl` scenario suite | Decision-engine behaviour at scale, Phase 1/2/3 interactions, shortfall propagation, idle-vs-speculative tiebreaking |
| Integration | `test/integration/` | End-to-end pipeline in one process: per-pod controller → operator → shard → fake provider → UpcomingNode CRs |
| E2E | `kind` | Multi-cluster (3 kind clusters), real BigFleet binary, fake provider, real operator, real CRDs. Validates static stability by killing the coordinator and observing data-plane continuity. |
| Soak | nightly CI | 24h simulator run with churn, asserting bounded resource usage, no leaked machines, no stuck transitions |
| Race | `go test -race` everywhere | The shard's hot path is intentionally lock-light; the race detector is the safety net |

Property-based tests (`testing/quick` or `gopter`) for:
- Aggregation correctness: any set of CRs aggregated then disaggregated back equals the original set on `(profile, count)`.
- Idempotency: replaying provider transition RPCs never produces duplicate machines.
- Phase 3 conservation: total inventory before reclaim - reclaimed == inventory after reclaim.

### 5.1 Scale ceilings — what we actually test, on what hardware

Scale testing is a first-class part of E2E, not a one-time exercise. Every milestone that ships real code is exercised at the highest scale the local M5 Max running Docker Desktop can sustain, and the achieved numbers are recorded as the milestone's scale-ceiling baseline. Regressions against the prior ceiling are a release blocker.

The local development hardware is one Apple M5 Max 16" MacBook Pro: **18-core CPU, 40-core GPU, 64 GB unified memory, 2 TB SSD**, running Docker Desktop. With macOS overhead and Docker Desktop sized to ~40 GB, the realistic budget for testing is:

| Knob | M5 Max ceiling (with headroom) |
|------|--------------------------------|
| Resident kind clusters running concurrently | ~3 with 5–8 worker nodes each (each kind node ≈ 1–2 GB) |
| Lightweight container "operators" against one shard | ~2,000 (one process per container, ~15 MB RSS each ≈ 30 GB) |
| Operator gRPC streams against one shard process (in-process or container-light) | ~10,000 (goroutines + sockets — bottleneck is fd limit, not memory) |
| Synthetic machines in shard inventory (in-memory) | ~5,000,000 (≈250 MB at the per-machine record size from BigFleet paper §9 — well within 64 GB) |
| Rollups/sec the shard can ingest aggregated across streams | target 5,000 rollups/sec sustained (18-core CPU is the lever, not memory) |
| Decision cycles/sec at full inventory | target 10 Hz (100 ms/cycle) at the largest inventory above |

The split worth noting: CPU is generous (18 cores is plenty for the engine's hot path), so throughput targets are aggressive; memory is the binding constraint, so kind-cluster and container-operator counts are deliberately moderate.

These numbers map onto two distinct test layers:

**Layer 1 — `cmd/fauxctl` synthetic scale**: pure Go simulation, no Docker. Drives the `decision`, `inventory`, `needs`, `shortfall` packages with synthetic providers and synthetic operators. Exercises the engine at the *machines* and *rollups/sec* ceilings — orders of magnitude beyond what kind can host. This is where we prove the decision engine is fast enough at fleet scale.

**Layer 2 — Docker / kind end-to-end scale**: real binaries, real gRPC, real Kubernetes. Smaller numbers, but real network paths, real CRDs, real kubelet behaviour. This is where we prove the system survives realistic protocol roundtrips and that no bug hides behind in-process shortcuts.

Each milestone (M3 onwards) defines its own scale ceilings:

- **M3 (shard)**: 1 shard process, 1,000 simulated streams via `fauxctl-style` test driver, 10,000 machines in inventory, 1,000 rollups/sec sustained for 10 minutes. Full Phase 1/2/3 cycle within 100 ms at peak.
- **M4 (operator)**: 1 real kind cluster with 5 nodes. Operator runs inside, shard runs on host. Drive 10,000 fake CRs through the cluster's etcd; verify the rollup compresses to ~5–20 entries and the cycle still runs at 10 Hz.
- **M5 (single-cluster e2e)**: 1 kind cluster, full pipeline (CR controller → operator → shard → fake provider → UpcomingNode). 1,000 unschedulable pods → 1,000 satisfied pods within 60 seconds wall clock.
- **M6 (multi-shard)**: 3 shards, 10 simulated clusters, 100K total machines. Cross-shard rebalance latency under 5 seconds. Coordinator failover under 2 seconds.
- **M8 (multi-cluster e2e)**: 3 kind clusters, 1 BigFleet control plane in Docker. Cross-cluster preemption via real ReclaimInstruction frames. Static stability test: kill BigFleet, verify all three clusters keep running.

Scale tests live under `test/scale/` with build tag `scale` so they don't slow PR CI. Run via `make scale`. CI runs them nightly; PRs that change `pkg/decision`, `pkg/shard`, `pkg/inventory`, or `pkg/needs` opt-in to running the relevant subset on the PR.

The numbers above are starting targets. Each milestone is allowed to adjust *up* (we discovered we can do more) but a downward revision needs an ADR.

---

## 6. Observability

- **Metrics** (Prometheus): per-cycle decision time histogram; per-phase action counts; per-shard inventory by state; shortfall age histogram; provider RPC latency / error rate; coordinator Raft term, leader, log lag.
- **Logs**: structured JSON. Include cluster id, shard id, machine id, operation id on every line. No PII, no secrets.
- **Traces**: OpenTelemetry on the hot path RPCs. Optional (off by default; cardinality concern at fleet scale).
- **kubectl experience**: per the paper — `kubectl get capacityrequests -A`, `kubectl get availablecapacity -n fleet-system`, `kubectl get upcomingnodes -n fleet-system` all just work via the CRDs.

---

## 7. Build, ship, run

- Single `make` for everything common: `generate`, `build`, `test`, `lint`, `e2e`, `sim`.
- Single `bigfleet` binary with subcommands: `bigfleet coordinator`, `bigfleet shard`, `bigfleet all-in-one`.
- Helm charts for `bigfleet` (control plane), `bigfleet-operator` (per-cluster), and `bigfleet-unschedulable-pod-controller` (optional per-pod controller).
- CI: PR checks (lint, unit, race, integration, simulator scenarios). Nightly: e2e + soak.
- Versioning: SemVer on the Go module; CRD/proto evolution via `v1alpha1 → v1beta1 → v1`.

---

## 8. Open questions & ADRs to write

Each becomes an ADR in `docs/adr/`:

1. **Raft library choice**: `hashicorp/raft` vs `etcd/raft`. Default to `hashicorp/raft` for operational simplicity; revisit if we hit lag.
2. **Shard ↔ coordinator transport**: gRPC streaming vs request/response. Default request/response with periodic poll; streaming if shortfall propagation latency ever matters.
3. **Bootstrap blob caching**: do we cache responses in the shard between `BootstrapRequest`s with identical requirements? Probably no — TTL is short, and stream round-trip is cheap. Revisit if the operator turns out to be slow to respond at scale.
4. **CR aggregation key**: do penalties partition the aggregation? Yes — two CRs with different interruption_penalty cannot share a `CapacityNeed` because the autoscaler must price each independently. This widens the NeedsTable but keeps semantics correct.
5. **Phase 2 weight tuning**: `wp, ws, wpen, wrec`. Start with sensible defaults, expose as shard config, document as tuning knobs.
6. **Idle hold timeouts** per capacity type: bare-metal/reserved = forever; on-demand = 5m default; spot = 60s default. Configurable per provider.
7. **Topology granularity for fixed-machine assignment**: rack-level for GPUs, zone-slice for CPU. How does the coordinator know the topology tree? The provider declares it via labels on List.
8. **Cluster operator authn to shard**: mTLS with per-cluster certs, or short-lived tokens. mTLS is the cleaner default.

---

## 9. Milestones

Each milestone is shippable and demoable; the next milestone subsumes the previous.

### M0. Repo bootstrap (small)
- Repo layout, `go.mod`, `Makefile`, lint config, CI skeleton.
- `docs/papers/` populated. `docs/plan.md` (this file). `docs/adr/0001-record-architecture-decisions.md`.
- Empty proto files compile via `buf`.

### M1. Wire formats land
- Full `capacity.proto`, `shard.proto`, `provider.proto`, `operator.proto`, `coordinator.proto`.
- CRDs (`CapacityRequest`, `AvailableCapacity`, `UpcomingNode`) generated and round-trip-tested.
- Generated Go types live under `pkg/apis/bigfleet/v1alpha1/` and `pkg/proto/`.

### M2. Core engine in isolation
- `pkg/machine`, `pkg/needs`, `pkg/inventory`, `pkg/decision`.
- Phase 1, 2, 3 implemented and unit-tested.
- Cost / victim-score / drain-grace functions with locked-in formulas matching the design memory.
- `provider/fake` exists and is used by decision-engine tests.

### M3. Shard controller, single-process, single shard
- `cmd/bigfleet shard` runs.
- gRPC server for `Shard.Session` (bidirectional stream).
- Per-cluster outbox; correlation of `BootstrapRequest`/`BootstrapBlobResponse` by `request_id`; reissue on reconnect.
- gRPC client for `CapacityProvider`.
- Per-cycle worker; reconciliation via `List`.
- Fencing tokens (epoch on restart).
- Component tests: shard + fake provider + scripted operator stream.

### M4. Cluster operator
- `cmd/operator` runs. **Outbound-only** — dials the shard, holds one stream, no inbound listener.
- Informers for the three CRDs.
- Roll-up loop (10s, full-replacement, profile aggregation, Pending → Acknowledged) sending up the stream.
- Bootstrap responder: handles `BootstrapRequest` frames, generates blobs from a template, replies with `BootstrapBlobResponse`.
- `UpcomingNode` writer driven by `NodeStateUpdate` frames.
- Reclaim handler driven by `ReclaimInstruction` frames; sends `ReclaimAck` back up.
- Stream reconnect with backoff; idempotent handling of reissued requests/instructions.
- Component tests with envtest.

### M5. End-to-end on kind, single cluster
- One kind cluster, BigFleet shard + operator + fake provider, all in one process group.
- `bigfleet-unschedulable-pod-controller` creates CRs from unschedulable pods; we observe roll-up → fake provisioning → UpcomingNode → kubelet (simulated) join → schedule.
- Static-stability test: kill BigFleet, observe pods continue.

### Deferred to post-v1: real cross-shard machine reassignment

The M6 rebalancer emits `TransferOwnership` instructions, the M6.3 shard adapter stubs the corresponding handlers (no-op + ack), and M8 demonstrates that the *protocol loop* closes correctly. What is **not** in v1 is real machine movement across shard boundaries — i.e., a donor shard actually picking specific `machine_ids` of a requested profile, draining them through the provider, and the recipient shard claiming them.

Doing it properly requires:

1. **Coordinator-side machine-id discovery**: the coordinator only sees per-shard summaries, not per-machine inventory. A donor-side query (new RPC or a piggyback on `ReportShard`) is needed to ask "give me N specific machine_ids of profile X you can spare."
2. **Donor adapter** in `pkg/shard/coordclient` for `OnCrossShardDrain` that scores victims via the existing Phase 2 logic, drains them through the provider, and returns the freed machine_ids.
3. **Recipient adapter** for `OnTransferOwnership` that claims the listed machine_ids and drives Configure through the provider.
4. **Provider ownership semantics**: either machines are unowned at the provider layer (BigFleet bookkeeping is the only source of truth — current model) or the provider gains a Transfer RPC. The current model is fine; the conformance suite (M9) just needs to spell it out.

The paper §6 acknowledges cross-shard preemption is "intentionally expensive… intentionally rare." V1 ships in-shard preemption (M5 + M8) which covers the common case, plus the protocol scaffolding for cross-shard work to land cleanly later. A post-v1 milestone — call it `Mx. Cross-shard machine reassignment` — picks this up. Implementation is roughly the size of M6.

### M6. Coordinator + multi-shard
- `cmd/bigfleet coordinator` (Raft + BoltDB).
- Shard registers, sends reports.
- Coordinator owns `cluster_to_shard` and `machine_to_shard` (cluster-scoped persistence).
- Cross-shard rebalance for idle and speculative.
- Cross-shard preemption (drain-first).
- Static-stability test: kill all coordinator replicas, observe shards continue.
- Property test: no machine ends up owned by two shards.

### M7. Simulator hardening
- `cmd/fauxctl` runs the published scenarios end-to-end.
- Soak job in CI.
- Outputs match-against-expected golden traces for regressions.

### M8. Multi-cluster e2e on kind
- 3 kind clusters, 1 BigFleet (coordinator + 2 shards in-process), 1 operator per cluster, fake provider with topology labels.
- Demonstrates: cross-cluster homogeneous capacity, training-job topology, reclaim of one cluster's batch for another's training, static stability.

### M9. Provider conformance suite + author guide
- `test/conformance/` — runnable test binary + harness that any provider can target.
- Covers: lifecycle RPC semantics, idempotency, transition timeouts, `List` filter behaviour, required label shape (topology, capacity-type, instance-type, zone), `Failed` handling.
- `docs/provider-author-guide.md` documents the contract, deployment shape, and how to run the conformance suite against a candidate provider.
- A reference out-of-tree provider lives in a *separate repo* (e.g. `bigfleet-provider-fake-cloud`) purely as a worked example for authors. It is not consumed by this repo's tests; the in-tree `provider/fake` covers that.

### M10. Production readiness pass
- Helm charts.
- Metrics + dashboards.
- Documentation: operator guide, provider author guide, scaling guide.
- Fault-injection CI (kill leader / shard / provider during scenarios).

### M11. Single-shard scale-test harness + algorithmic ceiling
Already shipped (see `project_lessons_learned.md` §M11.x). The end state: the harness runs a 50-cluster, 50K-CR, 500K-inventory production-shape scenario against a single shard on a 2-node Scaleway Kapsule and holds shard cycle p99 ≤100 ms with full sustained load, reproducible across six concurrent runs. The runner emits a snapshot + summary.json the site syncs into a public progression chart.

The cap surfaced by M11: a single shard runs out of cycle headroom around 500K machines because Phase 3 walks the full inventory each cycle. Beyond that we can't improve a single shard's number — we have to scale the architecture, not the algorithm. That is M12.

### M12. Multi-shard wiring (prerequisite for fleet-scale tests)
**Goal:** make the helm chart, the shard binary, the operator binary, and the scaletest harness multi-shard-correct, so that a fleet test with N shards × ~500K each is a configuration question, not a code question.

**Why this is its own milestone, not part of the next scale test:** today the chart's `shard.replicas: 1` is the only configuration we've ever exercised in the cloud. Bumping it produces silently-wrong behaviour: anonymous pods behind one round-robin Service, no per-shard DNS, no PVC, no coordinator registration on shard startup, and operators that get bound to whichever pod the Service routed them to first. Reconnect after a pod restart can land the operator on a different shard, which violates the "clusters are permanently bound to shards on first contact" hard rule. This is wrong enough that it can't be a side-quest of a scale test.

**Scope:**
1. Chart change: `shard-deployment.yaml` → StatefulSet with a headless Service for stable per-pod DNS (`bigfleet-shard-N.bigfleet-shard-headless.svc:7780`); plus a `volumeClaimTemplate` for the data dir so epoch + state survive pod restart.
2. Shard binary: `--coordinator-addr` flag; on startup, the shard registers itself with the coordinator (idempotent — second registration of the same `shard_id` is a no-op heartbeat).
3. Operator binary: documented support for being told its shard endpoint at deploy time; the harness's kwok chart wires `cluster-N` → `bigfleet-shard-(N % shardCount).bigfleet-shard-headless:7780` deterministically. No coordinator-driven routing in v1 — the operator's first connection establishes the binding.
4. Coordinator domain assignment: `domainToShard` is already there; verify the existing Heartbeat + AssignDomain flow does the right thing once shards report what they own. Add an integration test that runs 3 shards + 3 operators and confirms each operator's domain stays on one shard across pod restarts.
5. Harness: scaletest profiles gain a `shard.replicas` knob that propagates through both bigfleet and kwok charts. The runner reads it back to determine which shard endpoints to scrape and which to expect for each kwok cluster.

**Validation:**
- 2-shard cloud run: same per-shard SLO as M11's single-shard 500K. Cycle p99 ≤100 ms on each shard. Operator-to-shard binding survives a `kubectl rollout restart` of the shard StatefulSet.
- Static stability still holds: kill the coordinator mid-run; both shards continue operating; existing AssignDomain entries persist via Raft snapshots.

**Out of scope (deferred to M13+):**
- Cross-shard reclaim or rebalance (per the hard rules, topology constraints don't cross shard boundaries).
- Cross-region. ADR-0002 keeps single-region.
- Coordinator-driven shard re-routing on the operator. Today's "first connection wins" is enough for v1; ADR if/when re-routing is needed.

### M14. User-stories reconciliation (cheap)
**Goal:** close the doc-vs-code gaps surfaced by the audit of `docs/user-stories.md`. Each is small enough that grouping makes sense.

**Scope:**
1. **UpcomingNode drain phases.** `UpcomingNodePhase` enum is currently `Provisioning / Launched / Registered / Ready / Failed`. The user story (and the `bigfleet-shard` lifecycle docs) reference `Draining / Drained`. Add both to the enum and to the operator's `upcomingNodePhase` mapper so reclaim progress is observable via `kubectl get upcomingnodes`.
2. **Shard shortfall log line.** On-call runbook tells humans to `kubectl logs ... | grep -i shortfall` but no log line in `pkg/shard/` contains that word. Add a structured log on each cycle that emits a non-zero shortfall count (`logger.Warn("shortfalls detected", "count", n, "top", topProfileFingerprints)`).
3. **CR phase field-selector.** `kubectl --field-selector=status.phase=Pending` for `CapacityRequest` doesn't work — CRDs don't support arbitrary field-selectors without the `selectableFields` declaration. Either add the declaration (Kubernetes ≥1.30 only — bumps minimum supported version) or update the runbook to use jq. Default to the runbook update unless we want to commit to k8s 1.30 as floor.

**Validation:** existing CRD round-trip tests cover (1); a new on-call runbook excerpt in `docs/operator-guide.md` covers (3) once the command is rewritten; (2) is one new log line.

### M15. Coordinator admin RPC surface
**Goal:** make Mode 2 of `docs/user-stories.md` actually doable. Today's `service Coordinator { rpc ReportShard ... }` has no admin surface; the FSM commands `AssignDomain`, `UnassignDomain`, `RemoveShard`, and the provider-registry operations exist but are only reachable from in-process tests. A platform engineer running BigFleet cannot push a topology-domain assignment through Raft without restarting the coordinator with new bootstrap state.

**Scope:**
1. New RPCs in `coordinator.proto`: `AssignDomain`, `UnassignDomain`, `RemoveShard`, `ListShards`, `ListDomainAssignments`. Each gated to leader-only with the same `FailedPrecondition` pattern as `ReportShard`.
2. Authorisation: v1 ships unauthenticated since the coordinator is an internal-only service. Note in the manifest that exposing the coordinator outside the cluster requires a sidecar (mTLS/OIDC). Don't ship in-tree auth.
3. CLI helper: `cmd/bigfleetctl` (small new binary) wraps the new RPCs so the runbook commands look like `bigfleetctl assign-domain --topology-key=rack --topology-value=r-1 --shard=shard-2`.
4. Integration test: 3-shard process group; CLI assigns domain → next ReportShard delivers the assignment via existing `CoordinatorInstruction.AssignDomain`.

**Out of scope:** authn / authz beyond "trust the cluster boundary"; cross-region admin commands; CRD-style declarative shard topology (an admin command is fine for v1, declarative is post-v1).

### M16. PriorityClass → penalty defaults
**Goal:** `interruptionPenalty` and `reclamationPenalty` should have sensible cluster-wide defaults so workloads don't all need annotations. Today the controller (`pkg/controller/cr/controller.go`) reads only `bigfleet.lucy.sh/interruption-penalty` / `bigfleet.lucy.sh/reclamation-penalty` pod annotations; absent annotations → 0 penalties → workloads are arbitrarily preemptable.

**Scope:**
1. New CRD or Helm values block: `PriorityClassDefaults` mapping PriorityClass name → `{interruptionPenalty, reclamationPenalty}`.
2. Controller resolves penalty for a Pod as: pod annotation > matching PriorityClass default > 0.
3. Operator-guide entry showing how a platform team configures the defaults.

**Out of scope:** more complex policy (per-namespace penalty caps, per-team budgets). Those land in M-anything-after.

### M17. Runner failover automation
**Goal:** make `failover-soak.yaml`'s `runnerActions` block actually do something. Today the runner ignores it; the on-call ritual in `docs/user-stories.md` walks through manually issuing `kubectl delete pod` on the leader during a soak. That's fine pre-release but doesn't catch regressions automatically.

**Scope:**
1. Runner reads `runnerActions: [{ atSeconds: N, action: K }]` from the profile.
2. Implement actions: `kill-coordinator-leader` (delete the leader pod), `kill-shard-N` (delete a specific shard pod), `partition-coordinator-from-shard-N` (NetworkPolicy injection / removal). Each with a corresponding "expected outcome" the runner asserts (e.g. session_reconnects ≤ 1 per cluster after kill-leader).
3. Single dedicated profile per scenario rather than overloading `failover-soak.yaml` (`failover-leader-kill.yaml`, `failover-shard-kill.yaml`, `failover-partition.yaml`).
4. Runner emits `failures: [...]` in summary.json when an asserted outcome is violated, so the static-stability invariant is regression-checked automatically on every release run.

**Out of scope:** chaos engineering beyond pod kills (no kernel-level fault injection in v1).

### M18. user-stories doc-vs-code follow-up (cheap)
**Goal:** close the two cheap gaps from the post-M17 user-stories re-audit. Each is a doc rewrite or a Makefile-target add.

**Scope:**
1. **`docs/user-stories.md` Pre-release validation section.** Line 250 still says "automating it inside the runner is not yet shipped". M17 (commit `12f0407`) and M17.x (`79703d7`) shipped the asserted-outcome runner. Rewrite the section: drop the manual-kill walkthrough, point at `failover-leader-kill.yaml` / `failover-shard-kill.yaml` / `failover-partition.yaml` / `failover-soak.yaml`, and explain that the runner's `failures: []` in summary.json is the regression signal.
2. **`make conformance-build` / `./bin/conformance`.** Story line 197-198 documents these but neither exists. Either add a Makefile target that compiles a binary (the conformance suite is a Go test today), or rewrite the runbook to match the existing `make conformance TARGET=…` flow. Going with the rewrite is cleaner — keeping the suite as a `go test` invocation is the right ergonomic for `go test ./...` integration anyway.

### M19. CapacityRequest `.status.phase` write path
**Goal:** make the CR lifecycle visible. Today the CRD declares `status.phase` (Pending / Acknowledged) but no code anywhere writes it; the user story claims a Pending → Acknowledged walk that doesn't actually render. Workload owners watching `kubectl get capacityrequest` see no phase progression, on-call's runbook query returns nothing useful, and the lifecycle pillar is bookkeeping-only.

**Scope:**
1. Operator-side: when `buildRollup` includes a CR for the first time (its fingerprint enters the rollup), patch `status.phase = Acknowledged`. Idempotent on re-rollup.
2. Pod-controller / external creator: CR objects start with `status.phase = Pending` at create time (the controller already returns immediately after Create — add a status patch right after).
3. Operator's existing post-rollup batch-status-write path already handles `bigfleet_operator_acknowledge_duration_seconds` measurement. Rename / extend so the new write is observable as `bigfleet_operator_acknowledged_total{phase="Acknowledged"}`.
4. Update test/conformance and integration tests to assert the phase walk on a synthetic CR.

**Out of scope:** Failed / Satisfied phases — the v1 lifecycle is intentionally a two-step (Pending / Acknowledged) per the user story. Adding more phases is post-v1.

### M20. ReclaimInstruction end-to-end
**Goal:** lift `pkg/operator/reclaim.go` out of its M4 "log + ack" stub and implement the full Reclaim path: cordon, graceful-shutdown grace period, PDB respect, UpcomingNode `Draining → Drained` walk completion. The user-stories Mode-2 / preempt path claims the operator does this work; today it doesn't.

**Scope:**
1. On ReclaimInstruction: cordon each named node (`Spec.Unschedulable = true`).
2. Drain workloads with `eviction` API calls that respect PodDisruptionBudgets — the evictor takes the instruction's `grace_period_seconds` as the timeout per pod.
3. UpcomingNode CR phase walks through `Draining` (during eviction) → `Drained` (when the node is empty) → CR garbage-collected after a retention window.
4. ReclaimAck only fires once the cordon has taken effect — so the shard's reclamation-progress accounting is real, not an immediate ack.
5. Drain-grace timeout: a Drain that hits `grace_period_seconds` without succeeding flips the machine to `Failed` per the protocol; the operator surfaces `last_error` on the UpcomingNode.

**Out of scope:** node-drain ergonomics beyond the eviction API (custom drainers, in-pod hooks, etc.). The standard Kubernetes eviction path is enough for v1.

**Validation:** the existing `failover-soak` and a new `reclaim-cycle.yaml` profile drive synthetic preempt actions and assert the UpcomingNode CR walks through both Draining and Drained before deletion.

### M21. BootstrapTemplate as user-facing config
**Goal:** let cluster owners configure their bootstrap blob via the operator's helm values, not by forking the Go code. Today `Operator.Config.BootstrapTemplate` is a Go callback — only embedders can set it, which contradicts the per-cluster operator-install story where a cluster owner runs `helm install bigfleet-operator …` and expects to be able to specify how userdata is rendered for their cluster.

**Scope:**
1. Helm values block: `bootstrapTemplate` accepts a Go-template-style string with `.ClusterID`, `.MachineID`, `.NodeSelector`, etc. context. Default is a small no-op template (matches the in-process fake provider's behaviour).
2. Operator binary: `--bootstrap-template-file` flag pointing at a mounted ConfigMap-backed file; loaded at startup and parsed via `text/template`.
3. Operator's existing `BootstrapTemplate` Go callback retained for embedders + tests; the new flag's behaviour is implemented as a default callback that reads the parsed template.
4. Operator-guide entry walking through "I have nodes that need a custom kubelet config — here's how to render it via the bootstrap template".

**Out of scope:** template helpers beyond what `text/template` ships with (no Sprig). Per-cluster bootstrap-template overrides via CRD (operator chart values are per-deployment; cross-cluster overrides are a different design).

### M22. Runner ramp budget that scales with profile size
**Goal:** stop the 15-min hard-coded ramp budget from being the gate that fails fleet-scale runs. The 1M de-risk landed at 998,975 / 999,000 active CRs (99.898 % vs the 99.9 % bar) when the 15-min budget hit and the runner aborted; active count was still climbing. Empirically the 1M ramp needed ~17 min, the 5M ramp will need ~110 min at the same per-cluster pace.

**Today's formula** (`test/scaletest/cmd/scaletest-runner/main.go`):

```go
rampBudget := 15 * time.Minute
if t := time.Duration(prof.LoadProfile.DurationSeconds) * time.Second / 2; t > rampBudget {
    rampBudget = t
}
```

`max(15 min, durationSeconds×0.5) = 15 min` for every profile with the standard 30-min soak — the formula is a no-op as soon as you're over 30-min soak.

**Scope:**
1. New `rampBudget` field on the profile YAML (Duration string). Profile authors override directly.
2. Default formula when profile doesn't set `rampBudget`:
   ```
   rampBudget = max(15 min, totalCRs / 750 CR/sec, durationSeconds × 0.5)
   ```
   Empirical: the 1M de-risk's 998975 CRs in 900s = ~1110 CR/sec aggregate; sizing budget at 750 CR/sec gives ~1.5× headroom over observed throughput.
   - dev-5k:        15 min  (floor)
   - scaleway-50k:  15 min  (floor)
   - scaleway-500k: 15 min  (floor; 50K demand against 500K inventory)
   - scaleway-1m:   15 min  (100K demand at 1:10 burst against 1M)
   - scaleway-5m:   ~12 min (500K demand at 1:10 burst against 5M)
3. Runner logs the resolved budget at startup so the failure mode "ramp aborted at exactly the budget" is obvious from `runner.log`.
4. `docs/scaling-guide.md` entry documenting the formula and the empirical CR/sec floor it's based on.

**Validation:** re-run the 1M de-risk on the new defaults; confirm ramp completes inside the resolved budget with margin. Then proceed to M13 (5M) with confidence.

**Out of scope:** dynamic per-cluster pacing (the load-driver's own QPS knob — already configurable). This milestone is just the runner's gate-time, not the load-driver's behaviour.

### M23. Conformance: transitional-state observability + drain-grace handling
**Goal:** close two categories the user-stories doc claims the conformance suite covers but that didn't actually exist as tests.

**Scope:**
1. `TestConformance_TransitionalStateObservability`: poll Get aggressively after Create on a Speculative machine and assert at least one valid post-Create state ({Speculative, Creating, Idle}) was observed. The full kill-and-restart contract from user-stories ("kill the provider mid-Configure, restart, observe in-progress state preserved") needs process control we don't have over an external gRPC endpoint; the underlying property — that transitional states are reportable via Get — is what's testable from outside, and is what kill-and-restart depends on.
2. `TestConformance_DrainGraceTimeout`: Drain a Configured machine with `grace_period_seconds = 0`. Final state must be Idle (drain succeeded immediately) or Failed-with-non-empty-last_error. Stuck-in-Draining or silently-reverted-to-Configured fails the test.

**Out of scope:** killing the provider process from inside the suite (can't generically; provider authors add their own integration tests for restart-survival).

### M27. Phase 2 + Phase 3 optimisation at high demand-to-inventory ratio
**Surfaced by:** the M13.gate scaleway-1m cloud run (1M demand × 1M aggregate inventory, 2 shards × 500K each). Cycle p99 = 967 ms vs the 100 ms SLO. Per-phase breakdown:

  reconcile        1 ms
  phase1          58 ms   (allocator — proportional to M11 results, fine)
  phase2         471 ms   ← biggest
  phase3         416 ms   ← second biggest
  execute         —      (no execute work in this profile)

The wall is **algorithmic at high demand-to-inventory ratio**. M11's 500K-inventory validation drove 50K demand against 500K inventory (10:1); M13.gate drives 500K demand against 500K inventory (1:1) and pushes Phase 2's victim-scoring + Phase 3's reclaim into territory neither was ever optimised for. M13 (5M) has the same per-shard ratio (500K × 500K), so it would land in the same place at 5× the cost.

**Goal:** drop per-shard cycle p99 below the 100 ms SLO at 500K × 500K demand-inventory load. Re-validate via scaleway-1m; once that passes, M13 (5M) follows.

**Scope (provisional, sized after a profiler run on the M13.gate snapshot):**
1. **Phase 2 victim scoring** is currently O(N×M) — for every Need in the unsatisfied set, walk every Configured machine to score it. At 500K × 500K that's ~250B comparisons. Realistic optimisations: per-fingerprint candidate-pool index (analogous to M11.16 for Phase 1); short-circuit when the preemptor's priority gap collapses to zero against high-priority workloads; skip pinned-penalty victims early.
2. **Phase 3 reclaim** at high demand walks idle inventory looking for fingerprint-match opportunities. Same per-fingerprint index pattern. The reclaim heuristic is also priority-coupled, so the same pinned-penalty short-circuit applies.
3. Microbench in `pkg/decision/` covering the (500K demand, 500K inventory) shape so regressions catch in `make bench` rather than on a paid cloud run.

**Validation:** re-run scaleway-1m on the optimised binary; cycle p99 ≤ 100 ms with the same 0.5×churn / 30-min soak shape. If 1M passes, M13 (5M) is unblocked. If 1M still fails, the next milestone re-profiles and iterates.

**Out of scope:** changes to the algorithmic semantics (cost formula, victim-score reciprocal weights — both are author-locked) or to the proto contracts. M27 is pure data-structures-and-loops work inside `pkg/decision/`.

### M28. Demand-to-inventory regimes — reshape profiles + scaling guide
**Codifies:** ADR-0013 (demand-to-inventory regimes and SLOs).

**Goal:** make the SLO landscape honest. M13.gate showed that 1:1 demand-to-inventory ratio (M13.gate's scaleway-1m profile) breaks the 100 ms cycle SLO even with M27's optimisations. ADR-0013 names the three regimes (steady ≤2 %, burst ≤10 %, reprovisioning up to 100 %) and assigns each its own SLO. M28 ports the regime split into the test profiles and the scaling guide so the cycle-p99 SLO is measured at the density it actually promises.

**Scope:**
1. **Reshape `scaleway-1m`** from 1M demand × 1M inventory (1:1) to 100K demand × 1M inventory (1:10). Burst-density validation; gates on the standard `cycle p99 ≤ 100 ms` SLO. The 1M-demand shape moves to `scaleway-1m-reprovision`.
2. **Reshape `scaleway-5m`** from 5M × 5M to 500K × 5M (1:10). Same shape rationale. The 5M-demand shape moves to `scaleway-5m-reprovision`.
3. **New `scaleway-{1m,5m}-reprovision` profiles**: same total inventory, demand at 1:1. Documented as gating on convergence rate (≥5K bindings/cycle until drain), not cycle p99. v1 of these profiles ships without runner-side automation of the convergence gate — the operator interprets the snapshot's `bigfleet_shard_actions_total{kind="Bootstrap"}` rate manually. Future work to add the gate to the runner.
4. **`docs/scaling-guide.md`** new section: "Regime-aware SLOs" — the regime / pending-demand / SLO table from ADR-0013, plus a worked example showing how a real production fleet's load lands in the steady-state row 99 % of the time.

**Validation:** re-run `scaleway-1m` (the reshaped 1:10 version) on the M27-optimised binary. Expect cycle p99 well under 100 ms — Phase 2 + 3 caches were specifically tuned for this density. If 1:10 1M passes, M13 (5M at the 1:10 ratio) follows directly.

**Out of scope:** runner-side convergence-rate gating for the *-reprovision profiles (deferred to a follow-up); steady-state-shape profiles at 1:50 (deferred — burst is the worst-case the SLO has to honour, so validating burst implies validating steady-state).

### M13. Fleet-scale realism test (5M, hundreds of clusters)
**Gated on M27 + a passing scaleway-1m run.** The 5M test is end-of-line: it's the most expensive run we ship (~$2.60-$8 depending on duration after M22's ramp budget bump) and the most likely to surface previously-unseen bottlenecks. Originally drafted right after M12; deferred behind a chain of cheaper milestones because (a) M14-M21 close real user-stories gaps, (b) M22 fixes the ramp-budget cliff that failed the original 1M de-risk by 25 CRs, (c) re-running 1M cleanly is a much cheaper way to validate the per-cluster 10K-CR path before paying for 5M, and (d) M13.gate (commit / rundir 20260505-203638-scaleway-1m) found that Phase 2 + Phase 3 break the 100 ms cycle SLO at the 500K × 500K demand-to-inventory ratio per shard — the same ratio M13 has, so paying for 5M before fixing the algorithm just buys an expensive confirmation of the same wall.

**Pre-gate:** a passing `scaleway-1m` run on the M27 algorithmic optimisations. If 1M misses the SLO bar, fix it before paying for 5M — same per-shard work, one-fifth the cost (~$0.42/run).

**Target shape:**
- 10 shards × 500K inventory each = **5M total machines under management**.
- ~500 simulated clusters via KWOK (10× M11). Production fleets at 5M nodes have hundreds-to-thousands of clusters; the prior "stay at 50 fat clusters" sketch is rejected because it doesn't exercise per-cluster operator overhead, per-cluster gRPC stream lifecycle, the operator-to-shard fan-out, or the rollup-aggregation path's behaviour under realistic cluster counts. The whole point of testing on real Kapsule rather than `cmd/fauxctl` is realism; collapsing the cluster axis gives that up.
- ~10K demand CRs per cluster × 500 clusters = 5M demand. Steady-state churn matches M11's 0.05 / minute.

**Per-shard expectation:** each shard sees its M11-validated 500K inventory + ~50K demand. Cycle p99 ≤100 ms per shard. No shard sees cross-shard topology resolution; if a `Same`-rack request can't be served within a shard, it shortfalls (per the hard rule).

**Coordinator expectation:** ~500 domain assignments × ~10 changes/min sustained — well within the Raft FSM's measured throughput. Coordinator apply ops/sec is now scraped (M12.x chart fix) so the gate sees real data.

**Cost / capacity (rough):**
- KWOK pod budget: 500 pods × ~370 MiB observed = ~185 GiB. Plus shards (10 × 3 GiB) + coordinator (3 × 256 MiB) + prom (4 GiB) ≈ 220 GiB total memory.
- CPU budget: 500 KWOK pods × 300m req = 150 vCPU; shards 10 × 2 vCPU = 20; total ~175 vCPU req.
- Cloud sizing: 7× Scaleway PRO2-L (32 vCPU / 128 GiB) ≈ 224 vCPU / 896 GiB. Estimated ~€2.94 / hr; under the M22 ramp budget the run is ~110 min ramp + 30 min soak ≈ 2.5 hr ≈ €7.40 (~$7.85) end-to-end. Original 8× PRO2-XXL sketch was ~22 % more expensive at the same scale.
- PRO2-L is a separate per-zone quota counter from PRO2-M; default 10/zone, we need 7. No quota raise required.

**Validation gates:** the same SLO bar as M11, applied per shard. Sustained CRs ≥99.9 % of target. All metrics non-negative in summary.json. Per-shard runner queries (`max(by pod)`, M13.preflight 1ff1d27) catch a single overshooting shard that aggregate quantiles would dilute.

**Pre-flight work** (small, mostly already done):
- ✅ Coordinator scrape config in the harness chart (M12.x, commit 039bb62).
- ✅ Per-shard runner queries (M13.preflight, commit 1ff1d27).
- ✅ Provisioning-latency histogram fix + new gates (e886cbc).
- ✅ Helm install timeout 10m → 20m for 500-pod ramp (32db605).
- ✅ Profile `scaleway-5m.yaml` (1ff1d27, sized down 7077b6d).
- ⏳ M22: ramp budget that scales with totalCRs, not hard-coded 15 min.
- ⏳ Passing 1M run on the M22 budget (cheap re-validation of the de-risk).

### M45. ADR-0022 alignment: `Need.Count` is Pod count, BigFleet diffs aggregates

**Driven by** [ADR-0022](adr/0022-need-count-semantics-pod-vs-machine.md). The M44.4 Drop M–AA iteration loop closed every per-stage chain bug we could find and still landed scaleway-50k bind p99 at ~23 s after a 30 min soak. The remaining gap is architectural, not a chain bug: `pkg/decision/phase1_assign.go:103-114` treats `Need.Count` as machine count and emits one Bootstrap per unit, so a Profile that aggregates 100 CRs becomes 100 machines provisioned — when the paper-correct answer is "however many machines fit 100 Pods' worth of `Profile.Resources`." The harness's M35 unique-per-Pod label-axis multiplier has been hiding this drift by keeping `Count = 1` everywhere.

**Out of scope here:** speculative-quota allocation refactor (coordinator-side), Path B Profile split (separate `WorkloadClass` / `MachineShape` types), provider-contract changes. All deferred to future milestones if they turn out to be needed.

**M45.0 — types + proto: `Machine.Allocatable`, doc-comments**
- Add `Allocatable corev1.ResourceList` to `pkg/machine/machine.Machine` and to the `Machine` message in `api/proto/.../shard.proto` + `provider.proto`.
- Every code path that constructs a `Machine` defaults `Allocatable = Profile.Resources` (preserves all current 1:1 behaviour).
- Doc-comment `Need.Count` in `pkg/needs/needs.go` and `CapacityNeed.count` in the proto as "post-aggregation Pod count for this Profile, not machine count."
- Doc-comment `Profile` to spell out "Resources is the per-replica request shape."
- Commit observable change: none. Tests still all pass. Sets the table.

**M45.1 — Phase 1 vector math**
- New helper `decision.MachinesForAggregate(profileResources, count, machineAllocatable) int`: bottleneck-dimension `ceil(N × per-replica / per-machine)` math, fully unit-testable.
- Replace `fromSupply := n.Count` / `deficit := n.Count - fromSupply` at `pkg/decision/phase1_assign.go:103-114` with: aggregate demand = `Profile.Resources × Count`, supply = `Σ machine.Allocatable for matching machines`, deficit = `max(0, demand − supply)` per dimension, emit `MachinesForAggregate(...)` Bootstraps.
- Existing 1 Pod = 1 machine tests (Count=1, homogeneous shapes) keep passing trivially.
- New tests: density > 1, memory-bottleneck, CPU-bottleneck, GPU dimension, mixed.

**M45.2 — Phase 3 vector math (reclaim)**
- Mirror M45.1's changes on `pkg/decision/phase3_reclaim.go:144-148`: slack budget in aggregate space, reclaim candidates picked by the same vector-comparison logic, sign flipped.
- Symmetric tests.

**M45.3 — Operator rollup audit + doc**
- Audit the operator's rollup path (`pkg/operator/rollup.go` or wherever the aggregation lives) to confirm same-Profile CRs sum into one `CapacityNeed{count = N}`.
- I'm fairly sure this already works; this milestone is mostly a verification + doc-comment update on the rollup function.
- No behaviour change expected; if behaviour changes, file the bug here.

**M45.4 — Harness re-shape: bounded Profile cardinality**
- Update M35 label-axis multipliers in `test/scaletest/profiles/archetypes/realistic.yaml` so axes multiply to ~10 distinct Profiles per cluster (target across the fleet: ~500 Profiles × ~100 CRs each at scaleway-50k).
- Update seed catalog correspondingly — seed Configured machines must still match demand-side Profiles.
- Set seed `Allocatable` to be a multiple of the per-replica `Profile.Resources` (e.g. 100× — one machine fits 100 replicas of its Profile).
- Kind regression: `dev-5k` still passes.

**M45.5 — Scaleway-50k cloud validation**

Original framing: "50K" is the *machine* count, with ~5M aggregated Pods coalescing onto them at density ≈ 100 — the real production shape Lucy described in the §0.1 alignment discussion. The first attempt at this milestone re-shaped the profile at density=10 (50K Pods → 5K machines), which validated the algorithm cloud-side cleanly (gate cleared at 17.6× density ratio, 100% sustained load, all chain p99s under SLO) but doesn't honor the original sizing intent.

Prep work needed before the real 5M-Pod run:

- **Load-driver `reconcilePerTickCap`**: `reconcileTarget` in `test/scaletest/cmd/load-driver/main.go:640-649` caps creates+deletes at 20 per tick (1s tick → 20/sec/cluster). For 100K Pods/cluster over ~50 min ramp = 33/sec/cluster, which exceeds the cap. Add a profile knob (default 20, profile override e.g. 200).
- **Pod-shim throughput**: bind success ran at ~80/sec fleet-wide peak ramp + ~35/sec steady in the v2 run. For 5M Pods in any reasonable wall-clock, bind rate needs to scale ~30×. `MaxConcurrentReconciles=64` per pod-shim is a likely lever; per-cluster apiserver QPS another. Measure on a smaller density=100 dry-run first.
- **kwok pod resources**: 100K Pods per kwok-apiserver pod (1 vCPU req / 2 GiB lim today) won't fit in memory. Plan ~8 GiB lim / 4 vCPU per kwok pod. Profile-level override.
- **Node pool sizing**: 50 kwok pods × ~8 GiB ≈ 400 GiB just for kwok memory; plus shard + coordinator + prometheus. PRO2-L (the largest size in fr-par) is 128 GiB, so ≥8× PRO2-L (vs current 2×). Cost ~€3.36/hr.
- **scaleway-50k profile**: target=100000, seedDensityMultiplier=100, seedMachines=60000, rampBudget=60m, durationSeconds=3600 or longer, kwok+shard resource bumps, reconcilePerTickCap=200.
- **Run plan**: 1-2 dry runs at smaller-but-density=100 scale (e.g. 50 clusters × 10K = 500K Pods → 5K machines at density=100) first, to flush out the bottlenecks without burning a full 8-PRO2-L hour. Then the full 5M run as the lockable baseline.

Expected outcome: bind p99 lands cleanly under the 15 s SLO with 50K machines under load — the architecturally-correct shape of the test that scaleway-50k was originally meant to be.

**M45.6 — Larger-scale regression**
- `scaleway-1m` and `scaleway-500k` runs on the new shape.
- Confirm aggregation behaviour holds at higher scale.
- Update profile docs / SLO notes with the new baselines.

**Risks worth surfacing:**
- Existing unit tests in `pkg/decision/` may have hand-built Needs assuming the 1-Pod = 1-machine math. They keep passing under M45.0's default migration, but the M45.1 behaviour change needs new test coverage.
- The seed code (`pkg/scaletest/archetype/`) puts the per-machine shape into `Profile.Resources` today. After M45.0, seed-time inventory needs `Allocatable = density × Profile.Resources`.
- Run-to-run variance in the soak window was real even before this change. M45.5 may need 2–3 runs to nail down the new baseline rather than 1.

---

## 10. Scalability concerns

Found by re-reading the plan against the 100M-node target. Each of these would bite at scale; mitigations are noted, but they need to be picked up as work items, not assumed.

### 10.1 `machine_to_shard` granularity is too fine — **resolved at code-time**
Plan §3.5 was originally drafted with per-machine assignments (100M entries, ~500MB Raft state). The §0.1 A locked-in decision was per-topology-domain granularity, and the first coordinator cut implemented that directly: `pkg/coordinator/state.State.domainToShard map[DomainKey]ShardID`. ~100K entries at fleet scale.

No per-machine FSM-state has ever existed. Verified by grep over `pkg/coordinator/`: the only `MachineID` reference is in `rebalance.go`'s `TransferOwnership` instruction payload, which is deliberately empty until the donor-side query lands.

**Status**: concern resolved before the issue was tracked here. Nothing to do.

### 10.2 Coordinator write bursts during fleet events
"~10 writes/sec steady state" is the median, not the worst case. Mass spot reclamation, an AZ failure, or a shard split flips many assignments at once. A 1M-machine spot revocation under per-machine assignments would be 1M Raft entries; even per-domain it could be tens of thousands. Snapshot cadence and disk IO need to keep up.

**Fix**: per-domain assignment (10.1) shrinks the burst. In addition: coalesce burst events into a single Raft entry (e.g. "domain D5 reassigned to shard S7" rather than per-machine deltas), and size `hashicorp/raft` snapshot interval / log compaction for these bursts up front. Soak test in M7 explicitly drives a synthetic AZ-failure event and asserts coordinator latency stays bounded.

### 10.3 Aggregation key explosion from per-workload penalties
The roll-up aggregates CRs by `(requirements, resources, priority, topologySpread, interruptionPenalty, reclamationPenalty)` (§3.1, ADR open question 4). Penalties are dollars chosen by whoever creates the CR. If 50K pods in a cluster carry 50K distinct penalty values, aggregation collapses nothing — the roll-up balloons from ~2KB (15 entries) to ~5MB (50K entries).

**Fix**: bucket penalties to a coarse log scale (e.g., powers of 2 in dollars: $1, $2, $4, …, $1M+). Two CRs whose penalties round to the same bucket aggregate. The cost-function effect of the rounding is bounded and small; the aggregation guarantee is preserved. Document this on `CapacityRequest` so users understand the bucket boundaries. Open an ADR.

### 10.4 Phase 2 victim scoring is potentially O(needs × machines)
With 100 unsatisfied needs and 500K configured machines per shard, naive scoring is 50M pairings per cycle — well outside the 50ms budget.

**Fix**: index machines by `(profile, priority, capacity_type)` so each unsatisfied need filters to a small candidate set in O(log n). Within each candidate set, score is bounded by k victim slots (we only need the top-k cheapest victims, not all of them). Document this as a hard requirement in the inventory package.

### 10.5 Per-cluster outbox is unbounded
The plan adds a per-cluster outbox in §3.4 that holds bootstrap requests, reclaim instructions, NodeStateUpdate frames, and AvailableCapacityUpdate frames. If a cluster's stream is offline, the outbox grows unboundedly with churn.

**Fix**:
- `NodeStateUpdate` and `AvailableCapacityUpdate` are *coalescing* — only the most recent state per node / capacity profile is retained.
- `BootstrapRequest` and `ReclaimInstruction` are kept until acked; they're naturally bounded by inflight provisioning / reclamation actions, which are bounded by shard concurrency limits.
- A hard cap on outbox size per cluster, with explicit drop-policy: oldest non-essential frames first, never drop a `BootstrapRequest` or `ReclaimInstruction`.

### 10.6 Provider full-`List` reconciliation is too big
At 500K machines per shard, a full `List` response could be ~100MB. 200 shards × per-minute reconciliation × per-provider = tens of GB/min of reconciliation traffic.

**Fix**: providers expose `List` with a `since_cursor` / `revision` filter so the shard only fetches deltas. Where the provider can't support cursors, `List` accepts a bloom filter or hash chain so the shard can detect drift cheaply and only fetch state for divergent machines. Add to the provider conformance suite (M9): cursor-based incremental list is required for any provider that exceeds N machines per shard slice. Below the threshold, full-list is acceptable.

### 10.7 End-to-end provisioning latency
Worst path on a cold start: pod Pending → operator roll-up (≤10s) → shard cycle (≤10s) → provider Create (30–90s) → Configure + kubelet join (~30s). 1.5–2.5 minutes. Acceptable for batch / training; too slow for latency-critical demand.

**Fix**: the plan already says "event-driven on roll-up arrival but rate-limited". Ensure the event-driven path actually triggers an immediate Phase 1 pass for new high-priority needs rather than waiting for the next cycle, capped at one early-fire per cycle to bound CPU. Document in the operator guide that latency-sensitive workloads should pre-create CapacityRequests (paper §11 already says this; we need to surface it operationally).

### 10.8 Coordinator state DR
Plan describes Raft + BoltDB, but doesn't say what happens if all three replicas lose their disks. Coordinator state isn't recoverable from the data plane — shards know their own slice, not the global map.

**Fix**: scheduled snapshot export to durable object storage (S3-compatible). Coordinator state is small (≤500MB raw, ≤5MB after the 10.1 fix); a snapshot every 5 minutes is trivial. Add to M10.

### 10.9 Cross-region deployment shape is unspecified
The coordinator is a single Raft group. For a fleet that spans regions, where does it live? Single-region coordinator means a regional outage in *that* region halts cross-shard rebalancing globally. Static stability mitigates the data-plane impact, but operators running cross-region fleets need an answer.

**Fix**: an ADR (`docs/adr/`) specifying the supported deployment topologies for the coordinator: single-region (default), multi-region with all replicas in a meta-region, multi-region with witness replicas. Pick one for the reference impl; document the tradeoff for the others. Out-of-band concern from v1 but should not be silently ignored.

### 10.10 Cluster `etcd` headroom
Operator §3.1 holds 250K CapacityRequest objects in an informer cache for a max-sized cluster. Memory is fine (~500MB). etcd is the squeeze: 250K × ~1KB = 250MB just for CRs, ~3% of an 8GB etcd. UpcomingNode churn adds steady write load. Co-tenancy with everything else in the cluster's etcd may push this past sensible limits in some clusters.

**Fix**: document the math in the operator guide. Provide an "unschedulable-only" mode in `bigfleet-unschedulable-pod-controller` (CR per *unschedulable* pod, not per pod) — the paper notes this is the right mode at very large scale, but it should be a documented operator setting from day one, not a future-tense option.

---

## 11. Risks worth naming up front

- **Raft on the coordinator**: most likely source of operational pain. Mitigation: small state, slow write rate, plenty of upstream production users of `hashicorp/raft`. Soak tests catch lag and snapshot churn.
- **CR cardinality at scale**: 250K CRs/cluster in the paper's analysis stresses the operator's informer. Mitigation: full-replacement aggregation in proto means BigFleet never sees 250K objects; we only need the operator to handle them locally.
- **Provider rate limits**: AWS's RunInstances budget is small. Mitigation: fan-out across accounts is a provider concern, not BigFleet's; we expose the seam clearly and ship config.
- **Drain time under priority pressure**: hours of grace means a preemptor can be blocked behind a long-running drain. Mitigation: shortfall escalation surfaces this to the coordinator, which seeks alternative donors before waiting.
- **Static stability regressions**: easy to accidentally introduce a hard dependency on the coordinator from the data plane. Mitigation: keep `pkg/shard` ignorant of the coordinator package at the Go level; the coordinator client lives in a sub-package the hot path doesn't import.

---

This plan is the living target. ADRs in `docs/adr/` capture decisions as they harden; this file gets updated when something shifts at the architectural level, not for every implementation detail.

## 12. Production-readiness arc (M67–M78)

Derived from the verified audit in `docs/production-readiness-2026-06.md`
(2026-06-11: 44 blocker claims, 38 adversarially sustained, 0 refuted).
Sequencing respects two hard dependencies: the consumed-capacity /
single-attribution engine work (M67–M68) gates the Idle→Speculative
release (a Delete path on a mis-attributing engine is a Create↔Delete
money-burning loop), the M66.5–M66.7 deletion cascade, and the
validation ladder; everything else can proceed in parallel.

| M | Scope | Depends on | Author gate |
|---|-------|------------|-------------|
| M67 | **Consumed-capacity attribution.** Sim-first repro of the gross-Allocatable credit defect (engine task: `p1_unsatisfied=0` with unplaceable pods); ADR for where consumption lives (roll-up carries total desired state per the paper's full-replacement semantics, vs Phase 1 modeling consumption); implement; dev-50-v2 (catalog gate) goes green and replaces the legacy gate. | — | ADR sign-off |
| M68 | **Phase 2 joins the ADR-0045 unification** (repurposed — the original single-attribution scope dissolved into M67). Victim *eligibility* scoped by the Need's constraint scope, scoring untouched: MinUnit chunk filter; Same Needs preempt only in their Phase-1-chosen domain (skip when no domain was choosable); AcquisitionParked Needs never preempt. Plus the shortfall-ledger fix: same-fingerprint deficits sum per cycle, age once per fingerprint per cycle. From the 2026-06-12 philosophy-conformance audit. | M67 | — |
| M69 | **Reclaim safety path.** Phase 3 Reclaims route through the operator's existing cordon/PDB/evict path (as Preempts already do) with real drain-grace; fix the false PDB claims in user-stories and the phase3 comment. | — | — |
| M70 | **Safety rails.** Per-cycle reclaim blast-radius cap with a production default; empty-roll-up guard; global kill switch (pause acquisitions/reclaims); dry-run/shadow mode (recommend, don't act); wire `machine.Validate` into provider ingest; structured decision audit log. | — | — |
| M71 | **Provider edge I.** Dial-out gRPC client + `--provider-addr`; shard→provider fencing on the wire (`shard_id`, `shard_epoch`, `sequence_number` on lifecycle RPCs) with provider-side reject semantics; conformance coverage for fencing and idempotency on all six RPCs. | — | — |
| M72 | **Provider edge II.** Contract round-trips cluster binding and assigned priority/penalties so a shard restart rebuilds full protection state from List+Get. | M71 | — |
| M73 | **Idle→Speculative release.** Delete action kind + per-CapacityType idle-hold policy (bare metal: forever; on-demand: minutes; spot: ~1m); conformance. | M67, M68 | — |
| M74 | **Security I.** mTLS on all transports; Session identity binds `cluster_id` to the presented certificate; coordinator admin RPC authn/authz; chart wiring for cert material. | — | — |
| M75 | **Ops hardening.** Raft join path so the 3-replica chart forms a quorum; coordinator backup/restore tooling + DR runbook; publish operator/UPC images from CI; `buf breaking` in CI; probes/PDBs in charts; cosign/SBOM/dependabot. | — | — |
| M76 | **Reference provider** (separate repo, per the out-of-tree hard rule) against a substrate of the author's choosing; driven through the conformance suite; closes the provider-author-guide gaps found in the audit. | M71 | substrate choice |
| M77 | **M66 cascade completion.** M66.5 (dev-50-v2 becomes the gate; delete legacy demand mode + preflight package), M66.6 (M50.5 validation → M50.7 legacy profile deletion), M66.7 (scheduler default flip, delete pod-shim). | M67 | — |
| M78 | **Validation ladder campaign.** Clean uber-5k baseline on the fixed engine (also the realism-clean ADR-0042 parking measurement); uber-50k; uber-500k (needs partner approval); uber-1m; 24h soak; failover matrix (leader-kill at load, shard-kill, partition); scale-down drills. | M67–M73, M77 | uber-500k+ approval; catalog weight semantics |

**Status (2026-06-12, overnight loop):** M69 ✅ M70 ✅ (ADR-0046 +
addendum) M71 ✅ M72 ✅ M74 ✅ (ADR-0048) M75 ✅ (ADR-0047). M67
engine work ✅ per ADR-0045 (Phase 3 shrinkage-only on Phase 1's
claimed-set; the m67 repro inverted into the bound-counts contract
pin) — the original M68 scope dissolved into it as the ADR records;
M68 was repurposed from the 2026-06-12 philosophy-conformance audit
(Phase 2 constraint-scope victim eligibility + shortfall-ledger
summing) and is ✅. M73 ✅ (2026-06-13, ADR-0049): paper §8's release
half — per-CapacityType idle holds inside Phase 3, idle-since
tracking in the inventory sidecar, the Delete action kind +
executeDelete, the hold-window-is-the-rail (no release cap) argument
with its sim loop-impossibility pins, and the Delete-on-Configured
conformance check. M67's dev-50-v2
gate-redefinition tail (the runner's chain-alive bind% gate vs the
ADR's "demand covered by bound + zero reclaim churn") is the
remaining follow-up and lands with the M77 gate swap. M77/M78
now unblocked on the engine side; M76 awaits the substrate choice.
Every remaining ladder item passes through the author queue below.

**M78 ladder status (2026-06-18):** the two auto-runnable rungs are
DONE and PUBLISHED on the public scale-test results page, both graded
on the ADR-0054 reframed steady-state gate set under a default,
UNCAPPED kube-scheduler. **uber-5k** ✅ (`cee793e`, all 8 gates green).
**uber-50k** ✅ (`cee793e`, 5,000,000 pods, all 8 gates green;
reproduced 4× independently — the headline scorecard renders the four
runs as a `result 1..4` consistency table, engine numbers invariant
run-to-run). The two hard-won enablers were a GLOBAL 40-host fleet (5
regions, 1:1 distinct hosts — clearing the per-region distinct-host
wall) and a 16Gi kwok apiserver for 25K-object clusters. **uber-500k /
1m / 5m** remain approval-gated (author queue item 4) AND need a much
larger test fleet (~224 hosts + hub sharding + an in-cloud relay; the
40-host fleet does not carry forward) per
`docs/scaletest-resource-requirements.md` — not a dev-fleet task. Soak,
failover matrix, and scale-down drills stay deferred until the ladder
completes.

Residual non-blocking follow-ups recorded in their ADRs: Raft
transport TLS (ADR-0048 §scope), per-ordinal shard Certificates for
multi-shard mTLS, runtime kill-switch toggle via coordinator
instruction (ADR-0046).

**Philosophy-audit arbitrations (2026-06-12; evidence:
`docs/philosophy-audit-2026-06.md`; the loop proceeds on everything
not listed here):**
ADR-0041 foldability ruling (demand-shape vs anticipation — one
paragraph settles it); ADR-0046 roll-up quarantine vs the
full-replacement hard rule (keep-with-amended-wording vs delete);
§9 coordinator shortfall response (schedule as a milestone — papers
win — or trim the dead plumbing + paper-diff); AvailableCapacity
(document as the designated smarter-operator input, or deprecate);
two one-line paper blockquotes (§8 excess timing, §7 bound-to-
requesting-cluster); reclamation_penalty idle-tiebreak omission
(paper-diff or fix); machine_id≡node-name identity convention;
quota subsystem + coordinator instruction pipeline deletion scope.

**Author decisions queue** (the loop parks work on these and
continues elsewhere; see the production-readiness audit for context):

1. ~~Catalog weight semantics~~ — **RESOLVED 2026-06-13** by ADR-0044
   + ADR-0050 (both Accepted, after this item was written): `weight`
   is workload-OBJECT frequency; pod-count and machine-share are
   *derived* (`podShare ∝ weight × E[replicas]`; `machineShare ∝
   podShare / podsPerMachine`), and weights are back-solved from a
   target machine mix. There is no code path that reads `weight` as a
   pod-share (`sizing.go:219-228 podShare()` is the single function
   feeding both the load-driver demand draw and the seed share), so
   the "choice" this item worried about cannot be made at read time.
   `TestRealisticCatalog_MachineMix` pins the realized mix; the catalog
   + pin are byte-identical between the published baseline (`cee793e`)
   and HEAD, so no baseline impact. (The "documented as pod-count
   share" framing traced to ADR-0032's now-superseded header, since
   annotated.)
2. ~~M67/M68 ADR sign-off~~ — **RESOLVED 2026-06-12**: ADR-0045
   rewritten and Accepted per author decision (capacity counts iff
   bound; Phase 3 on demand shrinkage only; no packing model, no
   unmet telemetry — YAGNI). M68 dissolves into the M67 deletion.
3. M76 — which substrate the reference provider targets.
4. uber-500k and above — external approval, per standing policy.
5. **Steady pod-bind p99 tail (uber-5k publish gate; bigfleet-uber #77,
   2026-06-15).** On the ADR-0052-fixed engine the over-acquire is gone
   and all engine SLOs are clean, but the steady **pod-bind p99 is
   76–102s vs the ≤15s SLO** (p50/p90 pass). It is *not* the over-acquire
   (≈0 at Create-latency=0). The #77 verdict attributed it to a 327s
   "provision stage", but that is the **saturated** `shard_provisioning_
   latency` histogram (top bucket = 0.01·2¹⁵ = 327.68s; Help: "per-CR
   granularity not preserved … fingerprint-level fan-out") — a category
   error for per-pod bind. The trustworthy per-machine stages (configure
   0.56s, node-creation 1.6s, pod-shim work 0.76s) are all fast, so the
   tail lives in the unmeasured gap: demand-recognition (rollup interval)
   + cold-provision of the *specific churned shape* (the profile has warm
   headroom: speculativeMultiplier 3, idleHeadroomFraction 0.2 — so not
   gross-utilization starvation). **Decision (ADR-0043 order):** (a)
   realism — is the churn-rate / rollup-interval / shape-headroom regime
   production-real? (b) SLO posture — ≤15s p99 for a cold-provision
   rebind under churn is physically tight (Create + 3-cycle bootstrap ≈
   12–15s); reframe to p50/p90 or a regime-aware target (ADR-0028
   precedent)? (c) engine — only if (a)+(b) say regime+target are right,
   and after re-instrumenting the saturating metric. The bind-SLO-pass
   *publish claim* for uber-5k is parked on this; the engine baseline
   (over-acquire fixed, SLOs clean, 0 shortfalls) stands.
   **RESOLVED to data (bigfleet-uber #78 A/B, 2026-06-16):** BigFleet's
   per-decision engine is CLEAN in both arms (shardCycle 0.255s,
   node-materialization 1.6s, scheduler attempt *compute* 0.51s, 0
   shortfalls, no oversubscription). The #77 "76–102s" was histogram
   saturation; de-saturated, the real uncapped pod-bind p99 is
   hundreds-to-1300s. The tail decomposes into **(i) kube-scheduler
   retry/backoff amplification** — cap-mitigable 3–5× (sli_duration p99
   1310→328s with `schedulerPodMaxBackoffSeconds=1`), NOT BigFleet's, and
   **(ii) the reprovision back-edge** — cap-immune ~410s residual (a
   churn-reclaimed pod can't bind until a replacement machine reaches
   Configured; = the genuine reprovision physics). Instrumentation fixed:
   M79.4 widened the bind histograms + added a raw-max gauge; M79.5
   widened the saturating shard provisioning-latency histogram so the
   back-edge is now measurable. **Author decision (2026-06-16) + outcome:**
   (1) cap the scheduler backoff for SLO runs — **OVERRIDDEN.** The
   kube-scheduler stays UNCAPPED (production-faithful: BigFleet must not
   reconfigure the cluster's scheduler to pass its own SLO). The
   end-to-end pod-bind p99 therefore stays scheduler-retry + reprovision-
   bound by physics and is now INFORMATIONAL only.
   (2) reframe the steady release gate — **SHIPPED via ADR-0054.** The
   gate moves off the end-to-end pod-bind p99 (which BigFleet does not
   control under an uncapped scheduler) onto BigFleet's capacity-delivery
   hops: `shardConfigurePhaseP99` (held 15s, the materialization latency),
   `bootstrapSuccessRatio` (≥0.99, materialization throughput — closes the
   ADR-0052-crediting throughput-collapse hole), `operatorNodeStateUpdate
   P99` (the previously-uncovered UpcomingNode-publish hop), and
   `shortfalls==0` (the ADR-0045 coverage contract, promoted to verdict);
   a LOOSE end-to-end p50 liveness floor is kept. Posture thresholds are
   PROVISIONAL author-owned numbers, ratified after the dev-50 + uber-5k
   re-measure. Harness-only — `pkg/decision`/`pkg/shard`/`pkg/operator`
   and wire formats unchanged.
   (3) reprovision back-edge optimization (now measurable) — the
   NON-BLOCKING held-engine-bar follow-on; not a publish blocker. uber-5k
   (#258) publishes on the reframed gate.
