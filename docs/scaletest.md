# BigFleet scale-test runbook

How to run a BigFleet scale test against any Kubernetes cluster — kind on a laptop, your homelab, GKE Autopilot, EKS spot.

The harness is **self-contained**: one Helm chart deploys the BigFleet system-under-test and N simulated clusters, each as a Pod that bundles its own apiserver (KWOK), the BigFleet operator, and a load-driver. One runner CLI orchestrates: install → wait for steady state → soak → snapshot Prometheus → emit summary → tear down.

## TL;DR

```sh
# Build the two images. (One-time, or on every BigFleet code change.)
make scaletest-images

# Side-load into kind (for local runs); push to your registry otherwise.
kind load docker-image bigfleet:dev bigfleet-scaletest:dev

# Run the integration gate on laptop.
go run ./test/scaletest/cmd/scaletest-runner \
    --profile=test/scaletest/profiles/dev-50.yaml \
    --duration=5m \
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

### Laptop tier (legacy bundled shape)

| Profile | KWOK clusters × Pods | Machines | Best target |
|---|---|---|---|
| `dev-50` | 2 × 2.5K = **5K Pods** | 50 | M5 Max kind, integration gate |
| `dev-500` | 5 × 10K = **50K Pods** | 500 | M5 Max kind, larger rehearsal |

`dev-50` is the fast integration gate (~5 min, no churn) — proves the real Pod → kube-scheduler → CR → operator → shard → fake-Node → bind chain converges to 100 %. Both are pre-BYO bundled profiles (carry their own substrate inline); pair `5k.yaml + example-kind-laptop.yaml` for the BYO equivalent of `dev-500`.

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

Past runs are committed under [`test/scaletest/results/`](../test/scaletest/results/). Each profile has a current baseline (most recent passing run) tracked in that directory's `README.md`. New runs add a new directory; the baseline table moves only when a passing run beats the previous one.

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

Ramp time and ramp throughput are still captured in `summary.json` for capacity exploration, but they don't gate pass/fail. The runner does still time out if steady state isn't reached at all (`waitForSteadyState` budget) — that's a sanity check that the harness installed correctly, not an SLO.

| Metric | Threshold | Best observed | Notes |
|---|---|---|---|
| `bigfleet_shard_cycle_duration_seconds` | **100 ms** | 1.8 ms (50k tier) | Decision engine; large headroom intentional. |
| `bigfleet_operator_rollup_duration_seconds` | **1 s** | 122 ms (50k tier) | One rollup pipeline turn must finish well within the 10 s rollup interval. |
| `bigfleet_operator_acknowledge_duration_seconds` | **12 s** | 9.97 s (50k tier) | Bounded by operator status-write QPS against the apiserver. Tightens when the operator gains batched status writes. |
| `bigfleet_scaletest_pod_bind_latency_steady_seconds` | per-profile (default 15 s) | tier-dependent | Per-CR binding latency under churn replacement. ADR-0014 / ADR-0017. The headline customer-facing signal. |

Edit `pass()` in `test/scaletest/cmd/scaletest-runner/main.go` to add more.

A run that doesn't reach steady state fails with `steady state: ramp budget elapsed without reaching target`. That's a harness-side or system-bring-up issue, not an SLO violation — typically meaning the substrate is under-resourced or some chart-side install step hung.

## Recommended cadence

| Cadence | Profile | Substrate | Where |
|---|---|---|---|
| Per-milestone integration gate | `dev-50` | (bundled) | M5 Max kind |
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
