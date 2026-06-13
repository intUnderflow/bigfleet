# BigFleet scale-test runbook

How to run a BigFleet scale test against any Kubernetes cluster — kind on a laptop, your homelab, GKE Autopilot, EKS spot.

The harness is **self-contained**: one Helm chart deploys the BigFleet system-under-test and N simulated clusters, each as a Pod that bundles its own apiserver (KWOK), the BigFleet operator, and a load-driver. One runner CLI orchestrates: install → wait for steady state → soak → snapshot Prometheus → emit summary → tear down.

## TL;DR

```sh
# Build the two images. (One-time, or on every BigFleet code change.)
make scaletest-images

# Side-load into kind (for local runs); push to your registry otherwise.
kind load docker-image bigfleet:dev bigfleet-scaletest:dev

# Run the integration gate on laptop (defaults: PROFILE=dev-50,
# SUBSTRATE=example-kind-laptop — the gate pairing).
make scaletest DURATION=3m
# equivalently:
go run ./test/scaletest/cmd/scaletest-runner \
    --profile=test/scaletest/profiles/dev-50.yaml \
    --substrate=test/scaletest/substrates/example-kind-laptop.yaml \
    --duration=3m \
    --output=./test/scaletest/results/$(date +%Y%m%d-%H%M%S)-dev-50/

# Run a scale test against your own substrate (ADR-0034).
go run ./test/scaletest/cmd/scaletest-runner \
    --profile=test/scaletest/profiles/5k.yaml \
    --substrate=test/scaletest/substrates/example-fat-host.yaml \
    --output=./test/scaletest/results/$(date +%Y%m%d-%H%M%S)-5k/
```

The runner prints scale, host count, and cost upfront; prompts before any paid run; and tears down on Ctrl-C.

## Bring-your-own substrate

ADR-0034 splits the scale test into two orthogonal halves:

- **Profile** (`test/scaletest/profiles/<scale>.yaml`) — the test definition: scale, density, catalog, ramp, soak, churn. Substrate-agnostic.
- **Substrate** (`test/scaletest/substrates/<your-shape>.yaml`) — your runtime: per-host capacity, per-cluster apiserver operating point, kwok-pod resources, storage backend, cost. User-supplied.

The runner takes both, derives geometry (`clusterCount = ceil(totalPods / podsPerCluster)`, host count, cost), validates ramp-feasibility against your substrate's declared bind throughput, and installs.

## Profiles

All profiles run in Pod-mode + the realistic 6-archetype catalog by default (M44, ADR-0032).

### Substrate-agnostic scale ladder

| Profile | Total Pods | Machines | Notes |
|---|---|---|---|
| `5k` | 500K | 5K | Smallest scale tier; per-shard inventory fits trivially. |
| `50k` | 5M | 50K | Mid-tier; exercises operator-rollup at meaningful Pod cardinality. |
| `500k` | 50M | 500K | Single-shard ceiling (bigfleet.md §16). |
| `1m` | 100M | 1M | 2 shards × 500K. |
| `5m` | 500M | 5M | 10 shards × 500K. |

Geometry — number of KWOK clusters, hosts needed, cost — is *derived from your substrate*, not baked into the profile.

### Laptop tier

| Profile | Geometry (on `example-kind-laptop`) | Machines | Notes |
|---|---|---|---|
| `dev-50` | 2 clusters × 2.5K = **5K Pods** | 50 nominal (≈610 effective, ADR-0044) | V2 + `realistic-dev` catalog; **the per-milestone integration gate** |
| `dev-500` | 5 × 10K = **50K Pods** | 500 | legacy bundled shape; larger rehearsal, pending M77b deletion |

`dev-50` is the fast integration gate (~10 min with churned soak) — proves the real Pod → kube-scheduler → CR → operator → shard → fake-Node → bind chain wires up, plus the catalog demand paths (archetype draws, ADR-0041 folding, `Same(rack)`/`Same(zone)` gangs). Since M77a it gates on the ADR-0045 contract — demand covered by bound capacity, zero reclaim churn — not on bind percentage (see "Pass/fail SLOs"). `dev-500` is a pre-BYO bundled profile (carries its own substrate inline); pair `5k.yaml + example-kind-laptop.yaml` for its BYO equivalent.

### Failover scenarios — static stability

50 KWOK clusters × 1K Pods = 50K Pods total, distinct purpose: exercise the **"static stability is non-negotiable"** hard rule under coordinator/shard/network disturbance mid-soak. Pre-BYO bundled shape.

| Profile | What it disturbs |
|---|---|
| `failover-leader-kill` | one coordinator-leader-pod, t=10min |
| `failover-shard-kill` | one shard-pod, t=10min |
| `failover-partition` | 60 s control-plane network partition at t=10min |
| `failover-soak` | 2 leader-kills + 1 shard-kill across a 60-min soak |

## Substrates

Three example substrates ship under `test/scaletest/substrates/`. Each is a starting point — copy one, tune to your actual hardware, and commit it to your own repo.

| Substrate | Shape | Per-cluster operating point | Best for |
|---|---|---|---|
| `example-fat-host` | 64 vCPU / 128 GiB hosts, 10 clusters/host | etcd, 25K Pods/cluster, ~30 Pods/s | AWS c6i.16xlarge, GCP n2-standard-64 — multi-cluster fat hosts |
| `example-mid-host` | 32 vCPU / 128 GiB hosts, 1 cluster/host | kine, 100K Pods/cluster, ~110 Pods/s | Scaleway PRO2-L — single-cluster mid hosts |
| `example-kind-laptop` | Laptop Docker Desktop | kine on tmpfs, 10K Pods/cluster | Local dev / failover-* rehearsals |

Substrate YAMLs document the fields. The pattern: edit `host.vCPU`, `host.memoryGiB`, `cluster.podsPerCluster`, `cluster.bindThroughputPodsPerSec` (an empirical value from a short test on your hardware), and `costEstimate.perHostUsdPerHour`.

## Picking a substrate

Resource budget rule (M44 Pod-mode floor): each kwok pod needs at least `2 × kwokPod.requests` aggregated. Pack `clustersPerHost` of them onto each host alongside the system-under-test (shard + coordinator + prometheus).

| Your situation | Try this substrate first |
|---|---|
| Laptop / kind | `example-kind-laptop` |
| Scaleway Kapsule (PRO2-L) | `example-mid-host` |
| AWS / GCP fat spot instances | `example-fat-host` |
| Anything else | Copy the closest example; tweak `host.*` |

Cost is computed as `hostsNeeded × substrate.costEstimate.perHostUsdPerHour × hours`. The runner's confirmation prompt shows the estimate based on the merged geometry; override duration with `--duration=` and skip the prompt with `--yes`.

Nothing in the harness assumes a specific distro; pure Helm + standard Kubernetes APIs. GKE Autopilot is OK because the combined image runs as non-root and declares its ports.

## Cost-model assumptions

- **Coordinator**: 500m vCPU / 1 GiB (emptyDir for stress runs; HA + persistence is a separate test).
- **Shard**: 1 vCPU / 2 GiB at ≤500K machines under management (per-shard ceiling). One shard replica per 500K of profile's `scale.machines` — derived automatically.
- **KWOK pod**: from `substrate.kwokPod.requests/limits` (per-container, applied to apiserver + workload). Per-Pod totals are 2× these values.
- **Prometheus**: scales with clusterCount — 1 vCPU / 4 GiB at small scale, 4 vCPU / 16 GiB once clusterCount ≥ 100.
- **EKS control plane**: $0.10/hr fixed (charged regardless of node count).
- **Egress** (snapshot download): TSDB tarballs are 50–500 MB; first 100 GB/month outbound free, then $0.09/GB. Effectively zero at this volume.

See each example substrate's `costEstimate.notes` for provider-specific pricing benchmarks.

## Cost guardrails

The runner will:

1. **Estimate cost up front**. `--profile=50k.yaml --substrate=example-fat-host.yaml --duration=90m → ~$26 estimated` (21 hosts × $0.85/hr × ~1.5h).
2. **Prompt for confirmation** when the target context name suggests a cloud (`eks`, `gke`, `aks`, `aws`, `gcp`, `azure` substring) and the estimated cost ≥ $5. Skipped with `--yes`.
3. **Hard-cap runtime** with `--max-duration` (default 2h). Auto-teardown if the soak hangs.
4. **Always run teardown**, even on Ctrl-C, via `defer helm uninstall`.
5. **Tag every cloud resource** the chart creates with `bigfleet-scaletest-run=<run-id>` (via Helm `runId` value). If anything escapes, AWS-side cleanup is one filtered terminate-instances call.

## Captured results

`test/scaletest/results/` is a local-only artifact directory (in `.gitignore`; M66.1 untracked it). Run outputs land there when you pass `--output`; they are never committed. Reference baseline numbers in code review or design docs by quoting the relevant `summary.json` fields inline, not by committing the file.

## What gets emitted per run

`<output>/summary.json`:

```json
{
  "runId": "20260518-130000-dev-50",
  "profile": "dev-50",
  "target":   { "context": "kind-bigfleet", "kind": "kind" },
  "cost":     { "estimatedUsd": 0, "hours": 0.08 },
  "scale":    { "kwokClusters": 2, "podsPerCluster": 2500, "totalPods": 5000, "machines": 50 },
  "metrics": {
    "shardCycleDurationP99Seconds":   0.014,
    "operatorRollupP99Seconds":       0.087,
    "coordinatorApplyOpsPerSec":      4.2,
    "shardShortfalls":                0,
    "loadgenPodsActive":              5000,
    "loadgenPodsBoundPerSec":         16.7
  },
  "passed": true
}
```

`<output>/prometheus-snapshot.tar.gz` — the full TSDB for the run. Replay with:

```sh
mkdir -p /tmp/replay
tar -xzf prometheus-snapshot.tar.gz -C /tmp/replay
docker run --rm -p 9090:9090 -v /tmp/replay:/prometheus prom/prometheus:v2.55.0 \
  --storage.tsdb.path=/prometheus --web.enable-admin-api
```

## Live dashboard during the run

The harness chart ships an in-cluster Grafana with the same panels the runner gates on (cycle p99 + per-phase, operator rollup/ack p99, provisioning latency, shortfalls, coordinator apply rate, multi-shard health). The runner prints the port-forward at startup:

```sh
kubectl -n bigfleet-scaletest port-forward svc/grafana 3000:3000
# then open http://localhost:3000/d/bigfleet-scaletest (anonymous viewer)
```

Disable with `--set grafana.enabled=false` if you don't want the deployment (e.g., very tight CPU budget). The dashboard JSON lives at `test/scaletest/chart/dashboards/scaletest.json` and is provisioned via ConfigMap; edit it like code.

## Pass/fail SLOs

Per ADR-0035, the runner gates on **steady-state SLO histograms over the soak window**, not on ramp behaviour. With `seed.preBindFraction: 1.0` (the default for the BYO scale profiles) the cluster reaches steady state at install — the load-driver pre-binds the entire target Pod count to Configured-tier fake-Nodes by setting `Spec.NodeName` at create time. The soak window then measures per-CR binding latency for churn replacements, which is what a customer feels.

**What "steady state" means on V2 profiles (M77a / ADR-0045)**: BigFleet's contract is *demand covered by bound capacity* — it does not promise pod placement, so the gate no longer asserts a bind percentage. `waitForSteadyStateV2` requires, together: every kwok pod Ready; active CRs ≥ 99.9 % of target with the shard's NeedsTable reporting demand (`sum(bigfleet_shard_demand_machines) > 0`); `sum(bigfleet_shard_shortfalls) == 0` (Phase 2 left no unresolved deficit — demand is covered); and the acquisition counter (`bigfleet_shard_actions_total{kind=~"Bootstrap|Provision"}`) flat for 30 s (fulfillment finished, not merely claimed-ahead). Pod-bind progress is printed on every waiting line for diagnosability but never gated — satisfied-but-stuck is the cluster's problem (ADR-0045 §4). Two failure modes are detected early: a standing shortfall with frozen acquisitions at full demand fails in 2 minutes (the demand-side plateau detector), and any Reclaim action emitted during the steady window fails the run (`reclaimActionsDuringSoak` in `summary.json` — Phase 3 is shrinkage-only and must be inert at steady demand; movement there is the M67 oscillation class resurfacing).

Ramp time and ramp throughput are still captured in `summary.json` for capacity exploration, but they don't gate pass/fail. The runner does still time out if steady state isn't reached at all (`waitForSteadyState` budget) — that's a sanity check that the harness installed correctly, not an SLO.

| Metric | Threshold | Best observed | Notes |
|---|---|---|---|
| `bigfleet_shard_cycle_duration_seconds` | **100 ms** | 1.8 ms (50k tier) | Decision engine; large headroom intentional. |
| `bigfleet_operator_rollup_duration_seconds` | **1 s** | 122 ms (50k tier) | One rollup pipeline turn must finish well within the 10 s rollup interval. |
| `bigfleet_operator_acknowledge_duration_seconds` | **12 s** | 9.97 s (50k tier) | Bounded by operator status-write QPS against the apiserver. Tightens when the operator gains batched status writes. |
| `bigfleet_scaletest_pod_bind_latency_steady_seconds` | per-profile (default 15 s) | tier-dependent | Per-CR binding latency under churn replacement. ADR-0014 / ADR-0017. The headline customer-facing signal. |

Edit `pass()` in `test/scaletest/cmd/scaletest-runner/main.go` to add more.

A run that doesn't reach steady state fails with `steady state: ramp budget elapsed without reaching target`. That's a harness-side or system-bring-up issue, not an SLO violation — typically meaning the substrate is under-resourced or some chart-side install step hung.

## The validation ladder

A cloud run is the **last** confirmation of a change, never the discovery
instrument. The ladder, cheapest rung first — every change climbs as far
as it needs and no further:

| Rung | Where | Command | Time | Catches |
|---|---|---|---|---|
| 0.5. Profile preflight | local (`make prevalidate`) / runner default-on | committed-profile test, `pkg/scaletest/preflight` | <1 s | seed-shape vs demand-shape arithmetic on legacy no-catalog profiles: a bind gate that no soak duration can reach (the 2026-06-11 4,800-slots-vs-4,950-gate class). Catalog-driven (V2) profiles skip it — their seed draws machine shapes from the demand catalog by construction. Empty of gated profiles since M77a; deleted with the legacy demand mode in M77b. |
| 1. Closed-loop sim | local (`make prevalidate`) | `go test -run ClosedLoop ./sim/...` (`-short` for the quick set) | ~30 s short / ~2.5 min full | decision-engine feedback bugs — supply churn, demand-signal drift, co-location attribution, convergence failures — including `TestClosedLoop_Uber5KCardinality` at full uber-5k decision cardinality (2,580 Needs × 20 clusters), the class that historically cost a 90-minute cloud run apiece. |
| 2. Hot-path benches | local (`make prevalidate`) | `make bench-hot` | ~10 s warm | per-cycle cost regressions at measured uber-5k cardinality (~2,600 Needs, 93 % co-located; 25K-CR rollups). A blow-up here is a starved shard in the cloud. |
| 3. Integration gate | **devpod-side, step 0 of every cloud brief** (`make prevalidate-kind` for on-demand local runs) | `dev-50` (V2 catalog) + `example-kind-laptop` on kind/k3s, real binaries | ~10 min warm | harness wiring bugs — chart/values drift, label validity, controller plumbing, the Pod → CR → Need → bind chain end to end — plus the catalog demand paths (gangs, folding) and the ADR-0045 contract: demand covered (shortfalls == 0), zero reclaim churn over the steady window. A genuinely stuck engine fails in 2 min (demand-side plateau detector: standing shortfall + frozen acquisitions at full demand), not at the ramp budget. |
| 4. Cloud | devpod-side | a scale profile on a real substrate | ~25–60 min | substrate-scale effects only: real apiserver/etcd pressure, kube-scheduler throughput, multi-host topology. |

**Every SHA bound for a cloud run passes `make prevalidate` (rungs
0.5–2, Docker-free, ~3 min) before the brief is filed; the brief
executor then runs rung 3 on its own substrate FIRST and fail-fasts
the brief — verdict with the gate log, no cloud profile run — if it
cannot go green.** Rung 3 lives where the compute is free and the
images get built anyway; `make prevalidate-kind` keeps it runnable
locally for working on the harness itself. A cloud run that fails on
something a lower rung would have caught is a process bug, not just a
code bug.

### Mechanism runs vs SLO runs

Cloud runs come in two intents — say which one in the run's notes,
because they need different durations:

- **Mechanism validation** ("did the fix change the behaviour?"):
  `--duration=10m`. Behavioural signatures — action-rate slopes,
  inventory drift, attribution probes — are visible within minutes of
  fill completion. Don't spend a 30-minute soak proving a slope.
- **SLO measurement** ("what are the numbers?"): the profile's full
  soak (30 m+). Only worth running once the mechanism is already green.

#### Does the run need a live fill?

The fill is 30–45 min of a cloud mechanism run's wall clock. The
migrated profiles carry `seed.preBind: true` + `configuredFraction:
1.0` (M52.B / ADR-0035), which installs the cluster near steady state
and cuts a mechanism iteration to ~15–25 min — but a pre-bound install
silently measures **nothing** for mechanism classes whose subject IS
the fill. Decide from the table; when in doubt, fill live.

| Mechanism class | Live fill? | Why |
|---|---|---|
| Bootstrap-slope / bootstraps-per-cycle (M47.2-class) | **required** | with a full Configured seed, the Bootstrap → UpcomingNode → node-creator pipeline never runs at volume — only the churn trickle |
| Machine state-machine races at fill rate (M48-class) | **required** | the race window is the fill's transition storm |
| Demand-signal shape during ramp (ADR-0041-class: needs_total collapse, fold classification) | **required** | the signature is the rollup/ledger evolving *during* the fill |
| Fragmentation-induced gang behaviour (ADR-0042-class: cascade formation, acquisition parking) | **required** | the cascade is a product of the scheduler's incremental, fragmenting placement; pre-packed installs concentrate gangs cleanly and the engine path never fires (#58) |
| kube-scheduler bulk-bind throughput / ramp exploration (ADR-0033/0035) | **required** | ramp capacity is the subject — though ramp is exploration, not an SLO |
| Steady-state attribution / churn equilibria (Phase 3 behaviour at rest, ADR-0040-class) | preBind fine | the subject starts after steady state; the fill is pure setup tax |
| Steady-state SLO measurement | preBind fine | ADR-0035's definition — the fill is excluded from the metrics anyway |

### The 10-minute abort checkpoint

Every cloud run states an explicit checkpoint up front: one observable
(e.g. "cycle time ≤ 5 s by +10 min", "fill ≥ 50 % by +15 min") and the
instruction to **abort, capture a profile, and report** if it fails.
A doomed run should cost 10 minutes, not its full budget.

## Recommended cadence

| Cadence | Profile | Substrate | Where |
|---|---|---|---|
| Per-milestone integration gate | `dev-50` | `example-kind-laptop` | M5 Max kind |
| Weekly | `5k` | `example-mid-host` | 1 host |
| Monthly | `50k` | `example-mid-host` or `example-fat-host` | 8–21 hosts |
| Quarterly | `500k` | `example-fat-host` | ~200 hosts |
| Annual / pre-release | `1m` + `failover-soak` | `example-fat-host` + (bundled) | ~400 hosts + 2 hosts |

Actual costs depend on your substrate's `perHostUsdPerHour`. The runner prints the estimate before installing.

## Adding a new profile or substrate

**New profile** (a new scale tier — uncommon):

1. Drop a `test/scaletest/profiles/<name>.yaml` with `scale`, `catalog`, `seed`, `loadProfile`, and an `slo` block. See `5k.yaml` for the shape.
2. Run it against any substrate: `scaletest-runner --profile=...<name>.yaml --substrate=...<substrate>.yaml ...`.
3. If it deserves a baseline number, capture the resulting `summary.json` under `test/scaletest/results/baseline-<name>.json` and reference it in [`scaling-guide.md`](scaling-guide.md).

**New substrate** (a different runtime — common):

1. Copy the closest example under `test/scaletest/substrates/` to a name describing your shape (e.g. `my-cloud.yaml`).
2. Adjust `host.*`, `cluster.*`, `kwokPod.*`, and `costEstimate.*` to match your hardware.
3. Measure `bindThroughputPodsPerSec` from a short test run on one cluster; update the field.
4. Commit to your own repo (or keep local) — substrates are user-side configuration.

## Troubleshooting

- **Steady state never reached** — kwok pods aren't all reporting their target CR count. Check `kubectl logs -n bigfleet-scaletest -l app.kubernetes.io/component=kwok-cluster -c harness --tail=50` for individual KWOK clusters; usually it's apiserver port collision or the in-pod sqlite running out of inotify watches.
- **Coordinator OOMKilled** — bump `coordinator.resources.limits.memory` for the profile.
- **Shard cycle p99 alarming** — the simulator is exposing a real bottleneck. Capture the snapshot, compare against the previous run's summary, and follow up with a scale-tuning ADR.

## Cross-references

- Architecture: [`architecture.md`](architecture.md)
- Sizing rationale: [`scaling-guide.md`](scaling-guide.md)
- Production install: [`operator-guide.md`](operator-guide.md)
- Plan §5.1 (scale ceilings): [`plan.md`](plan.md)
