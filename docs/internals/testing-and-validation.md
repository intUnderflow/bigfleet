# Testing taxonomy and the validation ladder

BigFleet's test strategy is shaped by one economic fact: the cheapest place to find a bug is a Go unit test running in milliseconds, and the most expensive is a 30–90-minute cloud scale run that bills real hosts. Every layer below exists to push discovery of a particular bug class *down* to where it is cheap, and the validation ladder is the discipline that enforces the ordering — a cloud failure that a lower rung would have caught is a process bug, not just a code bug (`docs/scaletest.md` §"The validation ladder"). This doc is the map of which layer catches which class, why each exists, and how the `make` targets compose. It complements `scaletest-harness.md` (the harness internals) and `decision-engine.md` (what the closed-loop sims actually drive); read those for mechanism, this for the test taxonomy.

## The layers, cheapest first

| Layer | Where | Build tag | `make` target | Catches |
|---|---|---|---|---|
| Unit + property | next to code | none | `test` | logic, invariants (aggregation, idempotency, Phase 3 conservation), races (`-race`) |
| Scenario / golden | `sim/scenario/`, `sim/golden/` | none | `sim` | paper-example regressions; deterministic action traces |
| Closed-loop sim | `sim/` | none | `prevalidate` (rung 1) | decision-engine feedback bugs (the #45→#52 cascade class) |
| Hot-path bench | `pkg/decision`, `pkg/operator` | none | `bench-hot` (rung 2) | per-cycle cost regressions at measured cardinality |
| Conformance | `test/conformance/` | `conformance` | `conformance` / `conformance-self` | provider contract compliance |
| Integration | `test/integration/` | `integration` | `integration` | in-process multi-component wiring (coord↔shard, Raft, mTLS) |
| E2E | `test/e2e/` | `e2e` | `e2e` | the real Pod→CR→operator→shard→provider chain on kind |
| Scale (synthetic) | `test/scale/`, `sim/` | `scale`, `soak` | `scale`, `soak` | per-shard ceilings, leak/oscillation under millions of cycles |
| Scale (harness) | `test/scaletest/` | none (real binaries) | `scaletest` | end-to-end SLOs on a real substrate (rungs 3–4) |

## Unit tests, next to the code

Tests live in the package they test (CLAUDE.md "Working discipline"). The bias is toward **property tests for invariants** — laws the engine must satisfy under arbitrary input or arbitrary interleaving — because those are exactly the bugs a few hand-picked examples miss.

Three invariant classes carry the engine's correctness:

- **Aggregation correctness.** Penalty bucketing is powers-of-2 (CLAUDE.md "Decisions locked in"); `Same`-domain folding collapses per-machine inventory into per-domain bucket aggregates the chooser ranks. `pkg/decision/samebucket_test.go:81` (`TestChooseSameBucket_Rule`) drives `foldSameMachines` against the ADR-0041/0042 selection rules as a table; `pkg/decision/samebucket_test.go:259` (`TestSameDomainChoiceParity_Phase1VsPhase3`) asserts Phase 1 and Phase 3 fold the *same* domain set, so acquire and release can't disagree about which rack a gang lives on.
- **Idempotency.** Every mutating provider RPC is idempotent on its `(machine_id, target_state)` — replaying it returns the same `operation_id`, never a second actuation. This is asserted both at the contract layer (conformance, below) and against the in-tree fake in `pkg/provider/fake/fake_test.go`.
- **Phase 3 conservation.** A reclaim pass changes machine *state*, never the inventory *count* — excess Configured machines drain to Idle, the total is conserved. `pkg/inventory/inventory_test.go:157` (`TestPhase3_Conservation`) models a reclamation as the `Configured→Draining→Idle` Apply sequence and asserts `inv.Len()` is unchanged while `CountByState` shifts exactly `reclaim` machines. Reclaim being shrinkage-only — never re-derivation — is the property that keeps Phase 3 inert at steady demand (ADR-0045; `decision-engine.md` §"Phase 3").

The most load-bearing property test is concurrent. Phase 1 runs lock-light optimistic-concurrency claims across worker goroutines; `pkg/decision/occ/displacement_test.go:408` (`TestBroker_ConservationOfClaimedSet`) races 16 workers proposing single-machine claims and asserts the per-commit conservation law `Σ Committed − Σ Displaced = |claimedBy|` holds — the broker-side half of the ADR-0027 attribution invariant. `TestBroker_PriorityIsMonotoneUnderConcurrency` (`:333`) and `TestBroker_DisplacementMutationsAreAtomic` (`:288`) cover the other displacement laws. These are exactly the bugs the race detector alone can't see: not data races, but *accounting* races where every individual operation is correct but the aggregate ledger drifts.

### `-race` as the hot-path safety net

`make test` runs `go test -race -count=1 -timeout=30m ./...` — the race detector is **always on** for the unit suite, not an optional mode. The shard hot path is deliberately lock-light (`shard-hot-path.md`); the OCC broker substitutes atomic sequence checks for coarse locking. `-race` is the net under that design choice: it's the only automated detector for the unsynchronised-access class the broker's whole structure is engineered to avoid. The `-timeout` is raised from `go test`'s 10-minute default because the closed-loop scenarios run ~2 min without `-race` and 6–10× that with it (Makefile `test:` comment) — a default timeout reddened CI on every push from M67.1 until M75 noticed.

## Scenario and golden tests — paper examples that can't silently drift

`sim/scenario/` registers Go-defined scenarios (capacity stockout, priority inversion, training-job topology, withdrawal, provider failure) each mapping to a worked example from the paper. `sim/scenario/scenario_test.go:15` (`TestAllScenariosPass`) runs every registered scenario and fails if any assertion regresses — "any scenario that fails here means the engine's behaviour changed in a way that broke a paper example." Two of these (`sim/scenario/provider_failure.go`) are the M10 fault-injection scenarios that exercise the provider-unreachable→Failed row of `architecture.md`'s failure-modes table.

`make sim` additionally builds `fauxctl` and replays the six recorded **golden traces** under `sim/golden/*.jsonl` through `fauxctl verify <scenario>`. The golden is a frozen action stream: a behavioural diff harness. A code change that alters *which* actions the engine emits — even if every scenario assertion still passes — shows up as a golden mismatch, forcing the author to either accept the new trace or recognise an unintended behaviour change. This is the regression tripwire for the decision engine's observable output.

## Closed-loop simulation — the feedback-loop layer

The single highest-leverage layer below cloud. Ordinary sims are open-loop: fixed demand in, decisions out. The **closed-loop** sims in `sim/closedloop_test.go` model the cluster's reaction — `Reclaim → evict → recreate → rebind → rollup-change` — so the engine's actions feed back into the demand it sees next cycle. That feedback is where the expensive bugs live: supply churn, demand-signal drift, co-location attribution, convergence failures. The file header is explicit that each scenario pins one historical feedback-loop bug from the bigfleet-uber #45→#52 cascade (ADRs 0038/0039/0040) "that previously needed a 90-minute cloud run to surface."

The canaries are named for their pathology: `TestClosedLoop_BarePodsDestroyDemand_Canary` (ADR-0038), `TestClosedLoop_UnmetOnlyCRs_PhantomSurplus_Canary` (ADR-0039), `TestClosedLoop_SupplyExhaustion_StableShortfall_Canary`, plus the gang-oscillation set (`TestClosedLoop_GangScatterNoOscillation`, `TestClosedLoop_UnsatisfiableGangIsStableShortfall`, `TestClosedLoop_SubMachineGangsLedgerMatchesReality`). The keystone is `sim/closedloop_test.go:580` (`TestClosedLoop_Uber5KCardinality`), which runs the *full* uber-5k decision cardinality (2,580 Needs × 20 clusters) — "the class that historically cost a 90-minute cloud run apiece" — at `go test` speed, with `-short` trimming it to 60 cycles (~25 s). `make prevalidate` runs the `ClosedLoop` set as rung 1.

The discipline here is **demand realism before mechanism** (ADR-0043): a closed-loop bug is only worth a mechanism fix if a production fleet would emit the demand shape that triggers it. The closed-loop layer is where that question gets answered cheaply — fix the harness and re-measure before designing engine mechanism. The ADR-0042 parking layer is the cautionary tale of skipping that check.

## Hot-path benchmarks — cost regressions before they starve a shard

`make bench-hot` runs `Phase1`/`Phase3`/`AcquirableTotals`/`BuildRollup` benchmarks at measured uber-5k cardinality (~2,600 Needs, 93% co-located, 25K-CR rollups). Rationale in the Makefile is blunt: "a regression here is a starved shard in the cloud — see the #52-class `ParseQuantity` incident," where a per-element parse in a hot loop blew up cycle time only at cardinality. The bench is the pre-brief gate (rung 2) that catches per-cycle cost regressions while they're a benchmark delta, not a p99 SLO failure on a $26 cloud run. The shard cycle SLO is 100 ms with best-observed 1.8 ms (`docs/scaletest.md` §"Pass/fail SLOs"); that headroom is the budget bench-hot defends.

## Conformance — the provider contract suite

`test/conformance/` (build tag `conformance`) is what an **out-of-tree** provider runs to claim BigFleet compatibility — providers are out-of-tree by hard rule (CLAUDE.md), so the contract has to be testable against a binary the repo has never seen. The suite dials the provider's gRPC address (`-target` or `BIGFLEET_PROVIDER_TARGET`) and asserts the contract documented in `docs/provider-author-guide.md`:

- **Full lifecycle** (`conformance_test.go:84`): walks one machine `Speculative→Idle→Configured→Idle→Speculative` across all six RPCs. `Delete` returning `Unimplemented` is a *pass* — bare-metal-style providers that can't destroy hardware are conformant (`:124`).
- **Idempotency** on all four mutating RPCs (`Create`/`Configure`/`Drain`/`Delete`): back-to-back calls return the same `operation_id` (`:137` onward; M71 closed the gap where only `Create` had coverage).
- **Error codes**: `Get`/`Delete` on an unknown id return `NotFound` (`:270`, `:288`).
- **`List` semantics**: `states` filter honoured (`:309`); no `Watch` RPC exists — reconciliation is `List + Get` by design.
- **Fencing** (`fencing_test.go`, paper §11): every mutating RPC carries `(shard_id, shard_epoch, sequence_number)`; the provider keeps a per-`shard_id` high-water mark and rejects any non-strictly-newer token with `FAILED_PRECONDITION`. The suite mints a run-unique `shard_id` per test so repeated runs against a long-lived provider never collide, and `TestConformance_FencingReadsUnaffected` proves `Get`/`List` carry no token and never fence. This is the contract that stops a zombie shard actuating a stale fleet view (`fencing-and-identity.md`).
- **Metadata** (`metadata_test.go`): provider echoes metadata on `Get`/`List`, clears binding on `Drain`, preserves unknown keys verbatim.

The suite self-tests: `conformance-self` / `TestConformance_SelfTest_OnFake` (`selftest_test.go:31`) spins up `pkg/provider/fake` behind the gRPC adapter on a random port and runs the whole suite against it as a child `go test` process. This keeps the fake honest against the contract and proves the suite is self-consistent — `pkg/provider/fake` is the only in-tree provider and exists exactly to be the conformance suite's reference subject (never deployed).

## Integration — in-process multi-component wiring

`test/integration/` (build tag `integration`, `make integration`) wires two or more components together in one process with no Kubernetes:

- `coordinator_shard_test.go:42` (`TestEndToEnd_TwoShardsSelfRegister`): two shards start with **no out-of-band registration**, each appears in coordinator state after one heartbeat round, and the coordclient stamps the right `AdvertiseAddress` — the M12 self-registration contract (rebalance instructions ride on the shard-pulled report, never an inbound push to the shard).
- `raft_quorum_test.go` (`TestCoordinator_ThreeNodeQuorum_JoinAndFailover`, `:150` `TestCoordinator_RejoinAfterAddressChange_HealsConfiguration`): real three-node Raft join, leader failover, and configuration healing on address change (`coordinator-raft.md`).
- `mtls_session_test.go` (`TestMTLS_OperatorShardSessionEndToEnd`): the operator→shard bidi `Shard.Session` over mTLS — the outbound-only stream that carries all cluster↔shard traffic.

Integration is fast (~3 s) and folded into `verify` (below). The Makefile notes it "rotted invisibly for weeks when nothing compiled its build tag" — which is why `make vet` now vets every tagged package (`integration`, `scale`, `conformance`) explicitly: a `//go:build` tag hides code from the default compile, so the build can stay green while tagged test code rots.

## E2E — the real chain on kind

`test/e2e/` (build tag `e2e`, `make e2e`, ~30 min budget) runs against a real `kind` cluster's apiserver. The local dev box runs Docker Desktop so `kind create cluster` works without setup (CLAUDE.md "E2E as we go"); this is the layer that proves *behaviour*, not just code correctness, from M3 onward.

- `happy_path_test.go:22` (`TestE2E_HappyPath_PodsToConfigured`): 4 unschedulable Pods → 4 `CapacityRequest` CRs → 4 Configured machines on the fake provider → CRs `Acknowledged`, driving the full CR-controller → operator → shard → provider → status-feedback pipeline through a real apiserver. Pods stay `Pending` because the fake provider doesn't join real nodes to kind — the assertion is at the control-plane view, which is what BigFleet owns (BigFleet is **not** a scheduler; it never places the pod).
- `static_stability_test.go:27` (`TestE2E_StaticStability_ShardOutage`): bring the cluster to steady state, stop the shard's gRPC server, and assert Pods and CRs survive — the load-bearing safety property tested against a *real* kubelet/etcd, not a model. This is the e2e counterpart to the programmatic guard `pkg/shard/no_coordinator_dep_test.go` (`static-stability.md`).
- `multicluster_test.go` / `multicluster_harness_test.go`: multiple operators against one shard.

## Scale — synthetic and real-protocol

Two layers, per the "scale ceilings as we go" discipline (CLAUDE.md):

**Synthetic Go simulation.** `make scale` runs `test/scale/` under the `scale` build tag — millions of machines, thousands of streams, in-process, no Kubernetes. `m5_thousand_pods_test.go` carries an additional `kind` tag for the real-cluster variant of the M5 ceiling (1,000 unschedulable Pods → 1,000 Configured within 60 s wall clock). `make soak` (`sim/soak_test.go`, tag `soak`, nightly only) runs `DefaultSoakConfig` — 50K cycles, churn every 5 cycles, 50 rotating clusters — and asserts no leaked machines and no panics across the long run. Soak is the leak/oscillation detector that only a long horizon surfaces.

**Real-protocol harness.** `test/scaletest/` deploys the actual BigFleet binaries + N simulated clusters (KWOK apiserver + operator + load-driver per Pod) via Helm and gates on **steady-state SLO histograms over the soak window** (ADR-0035), not ramp behaviour. The contract gated is ADR-0045's — *demand covered by bound capacity* (`shortfalls == 0`), zero reclaim churn over the steady window — not a bind percentage, because BigFleet doesn't promise pod placement (satisfied-but-stuck is the cluster's problem). Full mechanics are in `scaletest-harness.md` and `docs/scaletest.md`; this doc only places it as the top two ladder rungs.

## `make verify` — the CI gate

`verify = vet lint buf-breaking test integration` (Makefile). That is exactly what CI runs on every PR, and what `.githooks/pre-push` runs locally if you `make install-hooks`:

- **`vet`** — `go vet` over the default build *and* every tagged test package (`integration`, `scale`, `conformance`), because tagged code rotted invisibly twice.
- **`lint`** — `golangci-lint` + `buf lint`. Match it before committing Go code; CI's verify gate is golangci-lint and skipping locally means red CI (memory: "Run `make lint` before commit").
- **`buf-breaking`** — `buf breaking` against the merge-base with `origin/main`. The wire formats are contracts (out-of-tree providers, persistent operators); a breaking proto change is a release blocker, not a review nit. Configured since M0, enforced since M75.
- **`test`** — the `-race` unit suite above.
- **`integration`** — the in-process suite above (~3 s).

`make verify` does **not** run e2e, scale, soak, or cloud — those are gated by build tags and the ladder, not the per-PR gate, because they need Docker/kind/real hosts and minutes-to-hours of wall clock.

## The validation ladder

The ladder is the rule that orders all of the above by cost and forbids skipping rungs (CLAUDE.md "Climb the validation ladder; never cloud-first"; `docs/scaletest.md` §"The validation ladder"). A cloud scale run is the **last** confirmation of a change, never the discovery instrument.

| Rung | Where | Command | Time | Catches |
|---|---|---|---|---|
| 0.5 Profile preflight | local | `make prevalidate` | <1 s | seed-shape-vs-demand-shape arithmetic on legacy no-catalog profiles (`pkg/scaletest/preflight`; `test/scaletest/cmd/scaletest-runner/preflight_test.go`) — a bind gate no soak can reach |
| 1 Closed-loop sim | local | `make prevalidate` | ~30 s short / ~2.5 min full | decision-engine feedback bugs, incl. `TestClosedLoop_Uber5KCardinality` at full cardinality |
| 2 Hot-path benches | local | `make prevalidate` / `bench-hot` | ~10 s | per-cycle cost regressions at measured cardinality |
| 3 Integration gate | **devpod-side, step 0 of every cloud brief** | `dev-50` + `example-kind-laptop` on kind, real binaries | ~10 min warm | harness wiring; the Pod→CR→Need→bind chain; catalog demand paths; the ADR-0045 contract |
| 4 Cloud | devpod-side | a scale profile on a real substrate | ~25–60 min | substrate-scale effects only: real apiserver/etcd pressure, kube-scheduler throughput, multi-host topology |

`make prevalidate` is rungs 0.5–2: Docker-free, ~3 min, runnable on the laptop. **Every SHA bound for a cloud run passes `make prevalidate` before the brief is filed.** Rung 3 (`make prevalidate-kind` locally, but normally run **devpod-side** as step 0 of the cloud brief) builds the images and runs `dev-50` on kind; the brief executor fail-fasts the brief — verdict with the gate log, no cloud profile run — if rung 3 can't go green. Rung 3 deliberately lives where compute is free and images get built anyway; don't burn the laptop on it as a routine gate (CLAUDE.md). The `dev-50` integration gate has its own fast-fail: a genuinely stuck engine fails in 2 minutes (the demand-side plateau detector — standing shortfall + frozen acquisitions at full demand), not at the ramp budget.

**Why the ordering is a correctness property, not a preference.** Each rung is strictly cheaper than the next and catches a *superset-disjoint* class — rung 1 catches feedback bugs a cloud run would also catch but 1,000× cheaper; rung 4 catches *only* substrate-scale effects (real etcd pressure, kube-scheduler throughput, multi-host topology) that no lower rung can. So the only legitimate reason to reach cloud is a substrate-scale bug. **A cloud run that fails on something a lower rung would have caught is therefore a process bug** — it means a rung was skipped or a gap exists in a lower rung that should be filled (`docs/scaletest.md` §"The validation ladder"). The fix for such a failure is never just the code bug; it's adding the missing closed-loop canary or bench so the class can never again cost a cloud profile. The #45→#52 closed-loop canaries are the accumulated scar tissue of exactly this loop: each one is a bug that once cost a 90-minute cloud run and now costs 25 seconds of `go test`.

## Cross-references

- Harness internals, profiles, substrates, SLOs: [`scaletest-harness.md`](scaletest-harness.md), [`../scaletest.md`](../scaletest.md)
- What the closed-loop sims drive: [`decision-engine.md`](decision-engine.md), [`phase1-occ.md`](phase1-occ.md)
- Static-stability guards: [`static-stability.md`](static-stability.md)
- Provider contract under test: [`provider-protocol.md`](provider-protocol.md), [`../provider-author-guide.md`](../provider-author-guide.md)
- Fencing contract: [`fencing-and-identity.md`](fencing-and-identity.md)
</content>
</invoke>
