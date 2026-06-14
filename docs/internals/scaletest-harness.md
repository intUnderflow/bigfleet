# The scale-test harness architecture

This is the code-level companion to [`docs/scaletest.md`](../scaletest.md). The runbook tells you how to *run* a scale test and how to read the results; this document explains how the machinery is *built* — the two test layers, the simulator (`cmd/fauxctl` + `sim/`), the realistic workload catalog (`pkg/scaletest/archetype`), the profile preflight (`pkg/scaletest/preflight`), and the real-protocol e2e harness under `test/scaletest/`. Read the runbook first for the operator's view and the validation ladder; read the two papers (`docs/papers/bigfleet.md` §5/§8/§16, `docs/papers/fleet-scale-kubernetes.md`) for what the demand model is trying to be faithful to. The single load-bearing principle threaded through all of it is **demand-realism before mechanism** (ADR-0043): a harness artefact that fakes a demand shape no production fleet emits will motivate engine mechanism that exists only to answer the artefact.

## The two layers, and why both exist

There are two physically distinct test stacks. They share exactly one thing — `pkg/scaletest/archetype`, the workload catalog — so demand and supply describe the same fleet on both.

```
                 ┌─────────────────────────────────────────────────────┐
   synthetic     │  Go simulator: in-process, no gRPC, no kube, no time │
   (Go, fast)    │  cmd/fauxctl + sim/  · pkg/decision/shard/needs/...  │
                 │  fake provider (pkg/provider/fake), shard.Step driven│
                 │  synchronously. Millions of machines, deterministic. │
                 └─────────────────────────────────────────────────────┘
                                       │ shares
                                       ▼
                          pkg/scaletest/archetype   (the catalog)
                                       ▲ shares
                 ┌─────────────────────────────────────────────────────┐
   real-protocol │  kind / cloud e2e: N Pods per cluster, each cluster  │
   (kube, slow)  │  a Pod bundling KWOK apiserver + real kube-scheduler │
                 │  + bigfleet operator + load-driver. Real CRD/gRPC/   │
                 │  Raft. test/scaletest/{chart,cmd,image,profiles}     │
                 └─────────────────────────────────────────────────────┘
```

The split exists because the two layers catch different bug classes and cost different amounts:

- **The synthetic layer runs the *real engine* (`pkg/decision`, `pkg/shard`, `pkg/needs`, `pkg/inventory`) against an in-memory `pkg/provider/fake`**, driven by direct `shard.Step` calls — no `time.Ticker`, no goroutines beyond `Step`'s own, no gRPC. That makes it deterministic and fast: millions of machines and tens of thousands of cycles in seconds. It is where decision-engine *logic* bugs are cheap to find.
- **The real-protocol layer runs the *real binaries* over the *real wire* against *real Kubernetes*** (KWOK-faked apiservers + a genuine `kube-scheduler`). It is where *harness wiring*, *substrate-scale*, and *protocol* effects show up — chart drift, label validity, apiserver/etcd pressure, scheduler throughput. It is slow and (in the cloud) costs money.

The "E2E as we go / scale ceilings as we go" hard rule plus the ADR-0043 discipline produce the **validation ladder** (`docs/scaletest.md` §"The validation ladder"): cheap synthetic rungs gate first, the kind/dev-50 real-protocol rung runs devpod-side as step 0 of every cloud brief, and a cloud run is the *last* confirmation, never the discovery instrument. The rest of this document is the architecture of each rung.

---

## Layer 1, the simulator: `cmd/fauxctl` + `sim/`

### Fauxmaster lineage

`cmd/fauxctl` is modelled on Borg's *Fauxmaster* (`cmd/fauxctl/main.go:1`): a CLI that drives the production decision/shard/inventory/needs packages against an in-memory provider so you can replay scenarios reproducibly. Its subcommands are deliberately small — `list`, `run`, `run-all`, `record`, `verify` — and they all route through `sim.Run` / `scenario` (`cmd/fauxctl/main.go:36-66`). The `record`/`verify` pair is the golden-trace mechanism: a scenario's `Trace` is JSON-lines (one event per line, human-diffable — `sim/runner.go:103-140`), recorded under `sim/golden/<name>.jsonl`, and `verify` byte-compares the current run against it (`cmd/fauxctl/main.go:188-225`). A trace divergence is a behavioural regression in the engine, caught without any cluster.

### Three simulation drivers, one engine

`sim/` carries three increasingly-faithful drivers, all wrapping the *same* `pkg/shard` engine:

| Driver | File | What it models | Used by |
|---|---|---|---|
| Scripted scenario | `sim/runner.go` | A fixed timeline of rollups + assertions on end state | `cmd/fauxctl`, golden traces |
| Soak | `sim/soak.go` | Long synthetic churn; invariant assertions | `make soak` (nightly, `soak` tag) |
| Closed loop | `sim/closedloop.go` | A reactive workload model that *owns Pods and reacts to BigFleet's actions* | `make prevalidate` rung 1 (`go test -run ClosedLoop ./sim/...`) |

#### Scripted scenarios (`sim/runner.go`)

A `sim.Scenario` (`sim/runner.go:34-62`) is a declarative struct: an initial fake-provider inventory (`InitialIdle`, `InitialSpeculative` — note Speculative is seeded, per ADR-0026, see below), an ordered `Events` timeline where each event applies one cluster's full-replacement rollup via `ApplyRollup` and runs `CyclesAfter` cycles of `shard.Step`, and `Assertions` evaluated against final state. `Run` constructs a real `shard.New` with a `fake.Provider{InstantTransitions: true}` and a fixed seed (`0xC0FFEE`, `sim/runner.go:168`), so every run is reproducible. The registered scenarios (`sim/scenario/`) cover the engine's interesting corners: `capacity-stockout`, `priority-inversion`, `withdrawal`, `provider-configure-failure`, `drain-failure-withdrawal`, `training-job-topology` — each a `Factory` registered by name (`sim/scenario/scenario.go:14-25`, `init()` calls in each file).

The scripted model has a structural blind spot, called out in code: **demand never reacts to BigFleet's actions** (`sim/closedloop.go:1-13`). A fixed rollup timeline cannot exhibit the feedback loop `Reclaim → drain → evict Pods → controllers recreate them → CR population churns → next rollup changes → Phase 1 reacts`. That entire ADR-0038/0039/0040 bug class is invisible by construction in the scripted runner. The closed-loop driver exists to close it.

#### The closed-loop driver (`sim/closedloop.go`) — the demand half of the loop

This is the most important and most subtle piece of the simulator, and the workhorse of `make prevalidate` rung 1. `RunClosedLoop` (`sim/closedloop.go:1088`) runs the real shard engine against a **reactive cluster model** that owns Pods, derives each cycle's rollup *from its live Pods* (mirroring `pkg/operator buildRollup`), binds Pods onto Configured machines, and evicts them when BigFleet drains a machine. The pathologies that historically cost 90-minute cloud runs to find become seconds-long `go test` failures.

Per cycle, in order (`sim/closedloop.go:1189-1295`):

1. Age in-flight provision/bootstrap dwells (see below), apply any `TargetScale` (the `kubectl scale` analogue) and `FaultEvent` (incumbent loss).
2. Each cluster derives its rollup from its live Pods (`clusterModel.rollup`, `sim/closedloop.go:647`) and applies it via `ApplyRollup`.
3. One real `sh.Step(ctx)` runs Phase 1/2/3 + OCC.
4. Machine-state changes are observed; affected Pods are evicted (`evictAndReconcile`, `sim/closedloop.go:676`).
5. Controllers recreate evicted Pods; pending Pods bind (`clusterModel.bind`, `sim/closedloop.go:741`).

The cluster model is a *faithful, integer-math* stand-in for the kube side. It mirrors the engine's own tricks so its picture and the engine's picture cannot drift:

- **Rollup derivation mirrors the operator.** `buildShapes` (`sim/closedloop.go:479`) turns each `WorkloadShape` into a `needs.Profile` exactly as the UPC→operator chain does: `In` requirements from the nodeAffinity sets, a `Same` requirement appended for gangs (`sameRack`/`sameZone`, mutually exclusive per ADR-0024), and penalties bucketed through `needs.BucketForDollars` (the operator is the canonical bucketing site). One CR = one Pod = one Need, grouped through the real `needs.Aggregate`.
- **Binding mirrors kube-scheduler.** `bind` filters by nodeAffinity, enforces gang Same-domain coherence (first member anchors the gang's rack/zone, later members bind only there or stay pending — the `planGroupOntoRack` "one rack or nowhere" rule), and among candidates picks the emptiest machine (`vecSlots`, least-allocated spreading — `sim/closedloop.go:413`). Capacity bookkeeping is interned milli-unit integer vectors (`resVec`, `sim/closedloop.go:345`), the same `occ.SameSupplyIndex` trick the engine uses, so the per-cycle bind scan never parses `resource.Quantity`.
- **Seeding mirrors `cmd/bigfleet seedFakeInventory`.** `seedClosedLoop` (`sim/closedloop.go:944`) mints the three tiers — Configured (running workloads), Idle (owned headroom, price 0), Speculative (elastic quota, priced) — with the same labels, zone/rack rotation, and contiguous-rack-block layout (ADR-0040 §4) the real seed uses.

Two booleans gate the bug classes the loop exists to pin:

- **`ControllerManaged`** (`sim/closedloop.go:174`) — true: evicted Pods are recreated by their controller, demand is conserved, Phase 3 self-arrests at the true surplus. False: bare Pods, eviction destroys demand permanently — the ADR-0038 #45 unbounded supply-thrash cascade. The canary `TestClosedLoop_BarePodsDestroyDemand_Canary` pins it.
- **`CRPerPod`** (`sim/closedloop.go:180`) — true: one CR per live Pod (papers §6.1, *total* demand). False: CRs only for pending Pods — the pre-ADR-0039 "unmet-only" signal that gave Phase 3 a phantom surplus. The canary `TestClosedLoop_UnmetOnlyCRs_PhantomSurplus_Canary` pins it.

The loop also models *engine latency* it once could not, which matters because instant transitions froze the acquirable pool at equilibrium and hid oscillation drivers:

- **`BootstrapDwellCycles`** (`sim/closedloop.go:222`) — a bootstrapped machine stays `Configuring` for N cycles before completing (ADR-0051 / M77g). Configuring *is* counted by the Phase 1 Same-domain coverage walk, so a machine in the bootstrap dwell is visible to its own gang and the dwell self-damps.
- **`ProvisionDwellCycles`** (`sim/closedloop.go:248`) — the pre-Configuring twin: a Provisioned machine stays `Creating` for N cycles (the `Speculative → Creating → Idle` runway). Creating is counted by *neither* the coverage walk *nor* the acquirable pool (`foldAcquirable` folds only Idle+Speculative) and carries *no* AssignedGroup, so a machine in the provision dwell is **invisible** — the gang re-derives the full deficit and over-acquires. The extended doc comment at `sim/closedloop.go:224-248` is the load-bearing explanation; this is the runway the over-acquire diagnostic (#66) turns on.

The dwell is driven by holding the fake provider staged (`ConfigureStaged`/`CreateStaged`, `sim/closedloop.go:1102`) and a per-machine countdown the loop ages (`completeMaturedCreateDwell` — `sim/closedloop.go:1394` — and `completeMaturedDwell` — `sim/closedloop.go:1419`). `injectFaults` (`sim/closedloop.go:1455`) uses `RemoveMachine`, not an in-place `Configured→Failed`, because the inventory FSM rejects the backward transition; a clean removal is the reconcile-ingestible incumbent-loss model.

Why the cardinality matters: `TestClosedLoop_Uber5KCardinality` (`sim/closedloop_test.go:580`) runs the closed loop at full uber-5k decision cardinality — 20 clusters × (2 plain classes + 95 memcache gangs + 32 GPU gangs) = 2,580 Needs total, 93% `Same` (`sim/closedloop_test.go:568-579`) — the convergence-failure class that historically cost a 90-minute cloud run apiece, now a `go test`.

#### Soak (`sim/soak.go`)

`Soak` drives the engine through many cycles (default 10,000, `sim/soak.go:49`) of synthetic churn — random per-cluster `Replace` rollups, periodic full withdrawals to exercise Phase 3's reclaim path — and then asserts *invariants*, not behaviour: inventory size stays exactly `IdleSeed + SpeculativeSeed` (no leaked or phantom records, `sim/soak.go:213`), no machine ends in a transitional or `Failed` state (`sim/soak.go:200-208`), bounded wall-time and action volume. It is the repetition test for use-after-reclaim and leaked transitional records; nightly CI only (`make soak`, `soak` build tag).

---

## The workload catalog: `pkg/scaletest/archetype`

This package is the **one thing both layers share**, by design (`pkg/scaletest/archetype/archetype.go:1-17`): the load-driver (which creates CRs/Pods) and the shard binary (which seeds Configured machines into the fake provider) read the *same* catalog, so demand and pre-bound inventory describe the same fleet. Drift between the two means Phase 3 reclaims the seed every cycle and the test measures reprovisioning, not steady state (`realistic.yaml` header, lines 59-63).

### What an archetype is

An `Archetype` (`pkg/scaletest/archetype/archetype.go:70`) is one recurring production workload pattern — GPU training, GPU inference, CPU batch, CPU service, memory caches, stateful DBs, critical realtime — carrying a frequency `Weight`, acceptable instance types and zones, a resource shape, priority classes, and the two distinct penalties (`InterruptionPenalty`, `ReclamationPenalty` — never `operational_value`). The catalog top-level (`Catalog`, `archetype.go:42`) can optionally split `SeedArchetypes` from `DemandArchetypes` (M34, `archetype.go:31`), because real fleets drift: the seed reflects what's been running, demand reflects what's being submitted. Both fall back to `Archetypes`.

A `Picker` (`archetype.go:293`) draws archetypes weighted-random; per draw the archetype supplies a resource shape (`PickSize` over `SizeBuckets`), label values (`PickLabels`), a gang size (`PickGroupSize`), a replica count (`PickReplicas`, the heavy-tailed service-size distribution in `sizing.go:37`), and optionally a spread constraint (`PickSpread`).

### The ADR layers that shaped it

The catalog is the most-iterated artefact in the harness because each cloud run that found a demand-shape error fed an ADR back into it:

- **ADR-0015 (M33), realistic archetype improvements.** Introduced `sizeBuckets` (per-CR resource diversity → fingerprint multiplicity), `meanLifetimeSeconds` (bimodal lifetimes — immortal services vs short-lived batch), `sameRack`/`groupSizeRange` (`Same`-rack gangs — the protobuf-only `Same` operator, paper-load-bearing and previously untested under load). The header at `realistic.yaml:65-95` is the running calibration log.
- **ADR-0032 (M44), production calibration.** Re-shaped the catalog against industry patterns after a uber-5k run measured Phase 1 at ~5 minutes: the catalog was 99% `ModeAllOrNothing` *as an artefact*, because most "sameRack" archetypes (DBs, caches) actually tolerate partial fills. Added the bottom-heavy long tail (`tiny-stateless` ≈ 70% of *pods*), the `allowPartial` flag, three GPU-training gang tiers, topology spread, a realistic priority skew (~85% default), and ~30-60 fingerprints/cluster. All profiles now run Pod-mode + this 6-archetype-family catalog by default.
- **ADR-0037 (M35 revert), node-affinity dimensions are realistic.** The earlier `labelAxes` mechanism emitted synthetic `team`/`app` axes as *required nodeAffinity*, which `kube-scheduler` also uses to place Pods: bigfleet-uber #41 measured the `NodeAffinity` filter rejecting 98.6% of placements and bind plateauing at 9.5%. The fix: node-affinity dimensions in the catalog must be things a real fleet pins Nodes on (instance type, zone), not Pod-level org labels. `LabelAxis` (`archetype.go:177`) survives as a mechanism but the realistic catalog drops the synthetic axes.
- **ADR-0038, controller-managed workloads.** The catalog's `IsStateful` set (`sizing.go:54`) classifies `stateful-db`/`memory-cache` as StatefulSets and everything else as Deployments — an intentionally small in-code set, not a profile knob (YAGNI). The replica distribution (`replicaDistribution`, `sizing.go:37`) and `StatefulReplicaCap` (`sizing.go:47`) also live in-code so demand generation and seed sizing agree on `E[replicas]`.
- **ADR-0050 (M78), machine-calibrated.** The decisive realism correction, and the cleanest ADR-0043 case. The catalog was calibrated as a realistic *pod-count* distribution, but BigFleet *allocates machines*, and for a whole-machine GPU gang **pod-share is machine-share** (1 pod/node) while commodity pods pack ~100/node. That 100× spread means a realistic ~7% GPU *pod* share mechanically implies a ~90% GPU *machine* fleet, which no production fleet resembles. The fix is `PodsPerNode` (`archetype.go:99`): a per-archetype node-packing density that supersedes M66.2's "GPU = 1 always". The realism catalog's weights are now *back-solved from a target machine mix* (`realistic.yaml:14-46`), and `TestRealisticCatalog_MachineMix` pins the realized machine mix within tolerance.

### Seed sizing — `sizing.go`, ADR-0044 / ADR-0050

`sizing.go` is the share math that keeps the shard's seed and the runner's effective-machine total from drifting. The key functions:

- **`PodsPerMachine`** (`sizing.go:167`) — `PodsPerNode` wins outright when set (ADR-0050); else falls back to M66.2's rule (global `density` for core-only/compressible shapes, `1` when any size bucket requests an extended resource, since device counts don't scale with density). `SeedScale` (`sizing.go:209`) returns `(factor, scaleExtended)`, where `scaleExtended` is true *only* when the archetype opted into ADR-0050 — that is what lets a `gpu:1` inference Pod seed a `gpu:8` 8-GPU node.
- **`MachineAllocation`** (`sizing.go:263`) — splits a machine total across archetypes by machine-demand share (largest-remainder, deterministic), then raises every gang archetype to at least its per-zone `gangFloor` (`sizing.go:239`, ADR-0044 §3 — without it the largest drawable gang is unsatisfiable by construction). The gang raises sit *on top*, so the result can exceed `totalMachines`: intended, because a fleet that runs zone-scoped gangs needs the per-zone pool whatever the nominal total says.
- **`MachinesForPods`** (`sizing.go:314`) — the inverse: the effective machine total a pod target implies, `Σ_a ceil(totalPods × podShare(a) / podsPerMachine(a)) + gang floors`. This is why a profile with whole-machine archetypes reports an *effective* machine count well above its nominal `scale.machines` (dev-50's "50 nominal ≈ 610 effective", ADR-0044). `scale.machines × density` stays the demand definition; `MachinesForPods` is the supply the demand shape actually implies.

---

## The profile preflight: `pkg/scaletest/preflight` (ladder rung 0.5)

`preflight` (`pkg/scaletest/preflight/preflight.go:1-26`) is static matching-capacity arithmetic over a profile — milliseconds, no Docker. It catches the seed-shape-vs-demand-shape mismatch that no soak duration can fix, before a ramp budget is burned. It was born from the 2026-06-11 dev-50 incident: the no-catalog legacy demand is single-shape (every Pod on one instance type) while the no-catalog seed *rotates five instance types*, so only ⅕ of the seeded pool could ever host demand — 4,800 matching Pod slots against a 4,950 bind gate, a stall knowable from the profile alone.

The package is also the **single source of truth** for the two shape tables that made the incident possible by living in two `package main`s where nothing could cross-check them: the no-catalog seed rotation (`LegacyInstanceTypes`, `preflight.go:37`, formerly `cmd/bigfleet/shard.go`) and the legacy demand shape (`LegacyDemandResources`, `preflight.go:72`, formerly the load-driver). `cmd/bigfleet` and the load-driver now both import them, so the check and the behaviour it models cannot drift.

`LegacySeed.Check` (`preflight.go:118`) computes `MatchingSlots` (`(matchingFromRotation(machines) + matchingFromRotation(speculative) + configuredPerCluster × clusters) × density`) and `BindGate` (99% of total target Pods) and fails — with a remediation suggestion — when matching capacity is provably below the gate. **Scope honesty** (`preflight.go:20-25`): it is a shortfall *detector*, not a green-guarantee. It models neither bind throughput, preemption, gang packing, nor spread; a profile can pass preflight and still fail for those reasons. It only proves the converse.

Critically, **catalog-driven (V2) profiles are out of scope and skip the check** (`test/scaletest/cmd/scaletest-runner/preflight.go:38-52`, `:59-77`): a catalog-driven seed draws machine shapes from the *same* catalog as its demand, so shape matching is by construction. The runner-side `legacyPreflight` wraps the package and refuses to install only an unreachable no-catalog profile. As `docs/scaletest.md` notes, the rung is empty of gated profiles since M77a and is slated for deletion with the legacy demand mode in M77b — its job is done once every profile is catalog-driven.

---

## Layer 2, the real-protocol e2e: `test/scaletest/`

The runbook documents the operator surface (Helm chart, runner CLI, BYO substrates). Here is the component architecture.

### One cluster = one Pod, bundling everything

Each simulated cluster is a single Kubernetes Pod that bundles its own KWOK-backed apiserver, a real `kube-scheduler`, the bigfleet operator, the node-creator, and the load-driver. `clusterCount` is derived from `ceil(totalPods / podsPerCluster)`; host count and cost are derived from the substrate (ADR-0034). The chart lives at `test/scaletest/chart/`; the orchestrator is `test/scaletest/cmd/scaletest-runner/main.go:1-25` — read profile, detect target from kubeconfig context, `helm install`, wait for steady state, soak, snapshot Prometheus TSDB, emit `summary.json`, `helm uninstall` (deferred, runs on Ctrl-C).

### BYO substrate — ADR-0034

The defining structural decision of the e2e layer: a run is **profile × substrate**, two orthogonal YAMLs (`runner main.go:124-130`). The **profile** (`test/scaletest/profiles/<scale>.yaml`) is the substrate-agnostic test definition — scale, density, catalog, ramp, soak, churn. The **substrate** (`test/scaletest/substrates/<shape>.yaml`) is the user-supplied runtime — per-host capacity, per-cluster apiserver operating point, kwok-pod resources, storage backend, cost. The runner merges them into Helm values and derives geometry. This is why profile filenames are `5k`/`50k`/`1m` (scale only) and not `scaleway-50k`/`uber-50k` (the old bundled shape ADR-0034 unbundled); see the memory note on the `devpod-`→`uber-` rename for the public-vs-internal profile distinction.

### Real kube-scheduler in the harness — ADR-0023

The harness originally shipped `pod-shim` (`test/scaletest/cmd/pod-shim/`), which did *three* jobs inside each cluster: create fake Nodes from `UpcomingNode` CRs, mark Pods Unschedulable, and bind Pods. Jobs 2 and 3 are *kube-scheduler's job in production*, and faking them made the harness's own scheduler the dominant variable in the published numbers — a 2026-05-13 uber-5k run measured 102 s p99 binding latency entirely on pod-shim's bind path, while BigFleet itself sat well inside SLO. ADR-0023 replaced jobs 2/3 with a **real `kube-scheduler`** per cluster (configured with `NodeResourcesFit` / `MostAllocated` to bin-pack to the ADR-0022 density model rather than spread), and kept only job 1 as a new ~100-line binary, `node-creator` (`test/scaletest/cmd/node-creator/main.go:1-17`): watch `UpcomingNode`, create fake Nodes, nothing else. The chain is now the real production shape:

```
Pod → kube-scheduler marks Unschedulable → unschedulable-pod-controller
    → CapacityRequest → operator roll-up → shard NeedsTable
    → Phase 1/2/3 → provider → Bootstrap → UpcomingNode
    → node-creator → fake Node → kube-scheduler binds the Pod
```

This is exactly what the dev-50 gate proves wires up end to end (`dev-50.yaml:18-29`).

### Controller-managed workloads — ADR-0038

The load-driver (`test/scaletest/cmd/load-driver/main.go:1-32`) creates **Deployments / StatefulSets**, not bare Pods. A bare Pod, once evicted by a Phase 3 drain, is gone forever — which made every reclaim permanently destroy demand and produced the unbounded ~26-machines/sec Bootstrap+Reclaim cascade of bigfleet-uber #45. Controller-managed Pods are recreated by their controller after eviction, so demand is conserved and Phase 3 self-arrests at the true surplus. This is the same conservation law the closed-loop sim's `ControllerManaged` flag models; the harness and the sim test the same property at different fidelities.

### The Speculative tier must be modelled — ADR-0026

A long-latent bug: `seedFakeInventory` originally seeded only Idle + Configured and *never* called `AddSpeculative`, so Phase 1's Speculative fallback (`Create + bootstrap` — the paper's entire elastic-procurement story, `bigfleet.md` §5/§8) was dead code in every scaletest. Any demand the fixed Idle pool couldn't directly satisfy became a permanent shortfall, which is not how BigFleet behaves. ADR-0026 made the harness seed **both** tiers, with a non-zero `--seed-speculative` default — there is no "fixed-pool-only" regime because the paper has no such regime. The simulator already did this (`InitialSpeculative` in `sim/runner.go:48`, `SpeculativeSeed` in `sim/soak.go`); the e2e harness was brought into line.

---

## Demand regimes and SLOs

### Regime-parametric cycle p99 — ADR-0013 / ADR-0028

The cycle-p99 bar is **not** a single number; it is parametric in the demand-to-inventory ratio (ADR-0013), because real fleets (Borg, Twine) live at ~1-2% pending demand in steady state, ~5-10% during ramps, and only hit 1:1 during operational events (cluster migration, DR, mass eviction):

| Regime | Pending demand vs inventory | SLO |
|---|---|---|
| Steady state | ≤ 2% (1:50) | cycle p99 ≤ 50 ms |
| Burst (deploy ramp, AZ rebalance) | ≤ 10% (1:10) | cycle p99 ≤ 100 ms |
| Reprovisioning (migration, DR, mass eviction) | up to 100% (1:1) | no per-cycle SLO; convergence-rate guarantee instead (≥5,000 bindings/cycle until drained) |

Steady-state and burst are the *production guarantees*; release-blocking profiles run at the burst ratio, the worst case the production SLO must honour. ADR-0028 adds that the realistic catalog's cycle cost scales with *Need cardinality* (the `sameRack` gangs produce per-group Needs — ~388 Needs/cluster, not the ~8 the aggregated regime suggests), which is why the bench gate (`make bench-hot`) measures Phase 1/3 at *measured* uber-5k cardinality (~2,600 Needs, 93% co-located, 25K-CR rollups) rather than the naive aggregate.

### Steady-state SLOs under churn — ADR-0035

ADR-0035 is the methodology decision that reframed the gate. The harness used to gate on **ramp behaviour** (bind ~100% of N within a budget), which conflated two things: *capacity/ramp* (how fast the system fills, dominated by downstream kube-scheduler/kine/kubelet behaviour) and the *steady-state SLO* (per-CR binding latency under churn — what the docs actually promise). A long investigation converged on the ramp ceiling being a `kube-scheduler` property under high label cardinality, not a BigFleet property — the steady-state SLOs were never failing.

The fix: **pre-seed to steady state at install** (`seed.preBind: true` + `configuredFraction: 1.0`, the load-driver sets `Spec.NodeName` at create time — no scheduler walk, no ramp) and **measure SLO histograms over a soak window driven by continuous churn** (`churnPerMinute`, each replacement Pod's CR-creation-to-bound latency is the sample). Ramp time/throughput stay captured but stop gating. The M77a/ADR-0045 refinement on V2 profiles: BigFleet's contract is *demand covered by bound capacity* — it does not promise pod placement — so `waitForSteadyStateV2` gates on `shard_shortfalls == 0` with demand at target and acquisitions quiescent, *not* on a bind percentage; pod-bind progress is reported but never gated (satisfied-but-stuck is the cluster's problem).

The **bounded-reclaim gate** (ADR-0035 amendment) is the most recent tuning: Phase 3 is shrinkage-only and should be inert at steady demand, but *zero* reclaims is unachievable on the async engine (ADR-0021) whose endogenous async-actuation reclaim floor is proven coverage-harmless (bigfleet-uber #65-69). So the gate snapshots the reclaim baseline `settleSeconds` into the soak (de-tailing the post-fill settling transient) and bounds the count by `slo.maxReclaimActionsDuringSoak` (dev-50: 150, an author-owned posture number, ~2-3× the de-tailed rate, far below a regression — `dev-50.yaml:110-125`). A regression (the M67 bootstrap≈reclaim oscillation resurfacing as sustained churn) still trips it.

### The dev-50 gate, concretely

`dev-50.yaml` is the per-milestone laptop integration gate. It is a **correctness gate, not a scale test and not a realism instrument** (`dev-50.yaml:48-50`) — no measurement taken there feeds a mechanism decision; that is the cloud profiles' job on `realistic.yaml`, per ADR-0043. It runs the `realistic-dev` *coverage* catalog (`realistic-dev.yaml:1-24`): same archetype names and shapes as `realistic.yaml` (so name-keyed logic — `isStateful`, gang anchoring — behaves identically), but gang sizes proportionate to a 50-machine fleet (`gpu-training-large` is *omitted* — its 64-256-machine gang cannot exist in dev-50's supply) and weights flattened so every demand path is exercised *every* run rather than probabilistically. Its job is to prove the chain wires up and the catalog demand paths (archetype draws, ADR-0041 sub-machine folding, `Same(rack)`/`Same(zone)` gangs, anchoring) work at gate speed.

---

## The validation ladder in code

The ladder (`docs/scaletest.md` §"The validation ladder", `Makefile:109-129`) is the operational expression of "cloud-last":

| Rung | Command | What runs | Catches |
|---|---|---|---|
| 0.5 | `prevalidate` step 1 | `TestCommittedProfiles_MatchingCapacityPreflight` (in `test/scaletest/cmd/scaletest-runner/`, driving `pkg/scaletest/preflight`) | seed/demand arithmetic on legacy profiles |
| 1 | `prevalidate` step 2 | `go test -run ClosedLoop ./sim/...` (incl. `TestClosedLoop_Uber5KCardinality`) | decision-engine feedback bugs |
| 2 | `prevalidate` step 3 → `bench-hot` | Phase 1/3 + rollup benches at uber-5k cardinality | per-cycle cost regressions |
| 3 | `prevalidate-kind` / devpod brief step 0 | dev-50 (V2 catalog) + `example-kind-laptop`, real binaries on kind | harness wiring; the ADR-0045 contract |
| 4 | a scale profile on a real substrate | full real-protocol run | substrate-scale effects only |

`make prevalidate` is rungs 0.5-2, Docker-free, ~3 min; every SHA bound for a cloud brief passes it before the brief is filed. Rung 3 runs **devpod-side** as step 0 of every cloud brief and fail-fasts the brief (verdict + gate log, no cloud profile spent) if it cannot go green — it lives where the compute is free and the images get built anyway. `make prevalidate-kind` keeps it runnable locally for working on the harness itself, but the standing guidance is *not* to run the kind rung on the laptop as a routine gate — it burns the dev box for work the devpods do free. A cloud failure a lower rung would have caught is a process bug, not just a code bug.

The synthetic scale-ceiling tests (`make scale`, the `scale` build tag — `test/scale/*.go`, `Makefile:96-98`) are a distinct axis from the ladder: they exercise the engine at millions of machines / thousands of streams on the M5 Max budget, and the achieved numbers become each milestone's baseline (plan §5.1). The kind/cloud e2e runs the *same scenarios* at realistic-but-smaller scale through real binaries.

---

## Map: where each concern lives

| Concern | Code | Authority |
|---|---|---|
| Fauxmaster CLI, golden traces | `cmd/fauxctl/main.go` | — |
| Scripted scenarios | `sim/runner.go`, `sim/scenario/` | — |
| Reactive closed loop (feedback bugs) | `sim/closedloop.go` | ADR-0038/0039/0040/0045/0051 |
| Long-run invariant soak | `sim/soak.go` | — |
| Workload catalog (shared) | `pkg/scaletest/archetype/archetype.go` | ADR-0015/0032/0037 |
| Seed/machine share math | `pkg/scaletest/archetype/sizing.go` | ADR-0044/0050 |
| Profile preflight (rung 0.5) | `pkg/scaletest/preflight/preflight.go` | M60 |
| e2e orchestrator | `test/scaletest/cmd/scaletest-runner/main.go` | ADR-0034/0035 |
| Load-driver (controller-managed demand) | `test/scaletest/cmd/load-driver/main.go` | ADR-0038 |
| Node-creator (UpcomingNode → fake Node) | `test/scaletest/cmd/node-creator/main.go` | ADR-0023 |
| Synthetic scale ceilings | `test/scale/*.go` (`scale` tag) | plan §5.1 |

When in doubt about *why* a piece of the harness is shaped the way it is, the answer is almost always a cloud run that found a demand-shape error and an ADR that corrected the *harness* rather than building engine mechanism against the artefact (ADR-0043). The ADR-0042 parking layer — rigorous mechanism built against one catalog archetype (`gpu-training-large`) demanding physically impossible rack-coherent gangs — is the cautionary tale the whole discipline exists to prevent.
