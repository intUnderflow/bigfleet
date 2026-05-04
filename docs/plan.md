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
   - Phase 1 (assign): walk by priority. Idle first (one bootstrap), then speculative (Create + bootstrap). When a bootstrap blob is needed, enqueue a `BootstrapRequest` on the cluster's outbox; the actual `Configure` provider call is held until the operator's `BootstrapBlobResponse` arrives.
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

### M13. Fleet-scale realism test (5M, hundreds of clusters)
**Gated on M12.** Tests the architecture's actual scale claim: many shards each at their known-good ceiling, with a realistic cluster fan-out instead of fewer fat clusters.

**Target shape:**
- 10 shards × 500K inventory each = **5M total machines under management**.
- ~500 simulated clusters via KWOK (10× M11). Production fleets at 5M nodes have hundreds-to-thousands of clusters; the prior "stay at 50 fat clusters" sketch is rejected because it doesn't exercise per-cluster operator overhead, per-cluster gRPC stream lifecycle, the operator-to-shard fan-out, or the rollup-aggregation path's behaviour under realistic cluster counts. The whole point of testing on real Kapsule rather than `cmd/fauxctl` is realism; collapsing the cluster axis gives that up.
- ~10K demand CRs per cluster × 500 clusters = 5M demand. Steady-state churn matches M11's 0.05 / minute.

**Per-shard expectation:** each shard sees its M11-validated 500K inventory + ~50K demand. Cycle p99 ≤100 ms per shard. No shard sees cross-shard topology resolution; if a `Same`-rack request can't be served within a shard, it shortfalls (per the hard rule).

**Coordinator expectation:** ~500 domain assignments × ~10 changes/min sustained — well within the Raft FSM's measured throughput. Coordinator apply ops/sec needs the harness chart updated to scrape it (currently the only metric still showing -1 in summary.json).

**Cost / capacity (rough):**
- KWOK pod budget: 500 pods × ~370 MiB observed = ~185 GiB. Plus shards (10 × 3 GiB) + coordinator (3 × 256 MiB) + prom (4 GiB) ≈ 220 GiB total memory.
- CPU budget: 500 KWOK pods × 300m req = 150 vCPU; shards 10 × 2 vCPU = 20; total ~175 vCPU req.
- Cloud sizing draft: Scaleway PRO2-XXL (32 vCPU / 128 GiB) × 8 nodes = 256 vCPU / 1024 GiB. Comfortable. Estimated ~€3.40 / hr; 50-min run ≈ €2.83 (~$3.00) per run.
- Per-zone PRO2 quota will need raising — 8 nodes is above today's per-zone cap (10 — close to the limit) so request a quota increase before the run.

**Validation gates:** the same SLO bar as M11, applied per shard. Sustained CRs ≥99.9 % of target. All metrics non-negative in summary.json (depends on coordinator scrape fix going in alongside).

**Pre-flight work** (small):
- Coordinator scrape config in the harness chart (one PodMonitor / Prometheus job, ~10 lines of yaml).
- Kapsule per-zone quota raise.
- Scale-test profile `scaleway-5m.yaml` defining the target shape above.

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
