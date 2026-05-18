# BigFleet scale-test runbook

How to run a BigFleet scale test against any Kubernetes cluster — kind on a laptop, your homelab, GKE Autopilot, EKS spot.

The harness is **self-contained**: one Helm chart deploys the BigFleet system-under-test and N simulated clusters, each as a Pod that bundles its own apiserver (KWOK), the BigFleet operator, and a load-driver. One runner CLI orchestrates: install → wait for steady state → soak → snapshot Prometheus → emit summary → tear down.

## TL;DR

```sh
# Build the two images. (One-time, or on every BigFleet code change.)
make scaletest-images

# Side-load into kind (for local runs); push to your registry otherwise.
kind load docker-image bigfleet:dev bigfleet-scaletest:dev

# Run the smallest profile.
go run ./test/scaletest/cmd/scaletest-runner \
    --profile=test/scaletest/profiles/dev-50.yaml \
    --duration=5m \
    --output=./test/scaletest/results/$(date +%Y%m%d-%H%M%S)-dev-50/
```

The runner prints scale and cost upfront, prompts before any paid run, and tears down on Ctrl-C.

## Profiles

All profiles run in Pod-mode + the realistic 6-archetype catalog by default (M44, ADR-0032). The profile naming convention is **`<substrate>-<N>` where `N` is the machine count** at density-100 (M45.5). The aggregated Pod count is roughly `N × 100`.

### Laptop tier — correctness + small-scale rehearsal

| Profile | KWOK clusters × Pods | Machines (@ density=100) | Cluster vCPU req | Cluster mem req | Cost |
|---|---|---|---|---|---|
| `dev-50` | 2 × 2.5K = **5K Pods** | 50 | ~5 | ~14 GiB | $0 (kind on M5 Max) |
| `dev-500` | 5 × 10K = **50K Pods** | 500 | ~8 | ~19 GiB | $0 (kind on M5 Max) |

`dev-50` is the **fast integration gate** (~5 min wall-clock, no churn) — proves the real Pod → kube-scheduler → CR → operator → shard → fake-Node → bind chain converges to 100 %. `dev-500` is the larger laptop rehearsal; runs in ~30 min but is kine-throughput-bound, not a clean BigFleet scale measurement.

### Scaleway-substrate scale ladder

| Profile | Total Pods | Machines | Cluster vCPU req | Cluster mem req | Substrate (Scaleway PRO2-L: 32 vCPU / 128 GiB) | Cost / run (full ramp+soak) |
|---|---|---|---|---|---|---|
| `scaleway-5k` | 500K | 5K | 32 | 128 GiB | 1× PRO2-L | ~$0.30 (~45 min) |
| `scaleway-50k` | 5M | 50K | 256 | 1024 GiB | 8× PRO2-L | ~$5.50 (~90 min) |
| `scaleway-500k` | 50M | 500K | 2240 | 8960 GiB | 70× PRO2-L (quota bump likely) | ~$52 (~100 min) |
| `scaleway-1m` | 100M | 1M | 4320 | 17280 GiB | 135× PRO2-L (quota bump certain) | ~$120 (~2 h) |
| `scaleway-5m` | 500M | 5M | 20480 | 81920 GiB | 640× PRO2-L (multi-region; explicit approval) | ~$1000 (~2.5 h) |

Each profile's YAML carries the exact `scw k8s cluster create` invocation (pool sizing, version pin, project ID). Always pass `project-id=3150379f-d66f-414d-8ca7-3d7d28fbeef6` — the dedicated BigFleet project.

### Private-substrate scale ladder (`uber-*`)

The `uber-*` ladder targets a different operating point than the Scaleway ladder: **fewer, larger Pod-per-cluster shape** (25K Pods × N clusters, vs Scaleway's 100K × N/10 clusters), sized for a per-cluster bind throughput of ~30 Pods/s. The names and YAMLs are public; the substrate is a private compute pool sized as below. Anyone with equivalent infrastructure (or willing to pay AWS/GCP rates for those shapes) can run them; cost-at-run is provider-specific.

| Profile | KWOK clusters × Pods | Machines | Hosts × per-host vCPU / mem | Aggregate vCPU / mem |
|---|---|---|---|---|
| `uber-5k` | 20 × 25K = 500K | 5K | 2 × (80 / 160 GiB) | 160 vCPU / 320 GiB |
| `uber-50k` | 200 × 25K = 5M | 50K | 20 × (80 / 160 GiB) | 1600 vCPU / 3200 GiB |
| `uber-500k` | 2000 × 25K = 50M | 500K | 200 × (80 / 160 GiB) | 16000 vCPU / 32000 GiB |
| `uber-1m` | 4000 × 25K = 100M | 1M | 400 × (80 / 160 GiB) | 32000 vCPU / 64000 GiB |
| `uber-5m` | 20000 × 25K = 500M | 5M | 2000 × (80 / 160 GiB) | 160000 vCPU / 320000 GiB |

`uber-500k` and above need explicit approval from the substrate provider before each run.

### Failover scenarios — static stability

50 KWOK clusters × 1K Pods = 50K Pods total, distinct purpose: exercise the **"static stability is non-negotiable"** hard rule under coordinator/shard/network disturbance mid-soak. The data plane (shards + operators) must hold its SLOs throughout.

| Profile | What it disturbs | Substrate | Aggregate vCPU / mem | Cost / run |
|---|---|---|---|---|
| `failover-leader-kill` | one coordinator-leader-pod, t=10min | 2× PRO2-L | 64 / 256 GiB | ~$0.74 (~50 min) |
| `failover-shard-kill` | one shard-pod, t=10min | 2× PRO2-L | 64 / 256 GiB | ~$0.74 (~50 min) |
| `failover-partition` | 60 s control-plane network partition at t=10min | 2× PRO2-L | 64 / 256 GiB | ~$0.74 (~50 min) |
| `failover-soak` | 2 leader-kills + 1 shard-kill across a 60-min soak | 2× PRO2-L | 64 / 256 GiB | ~$0.90 (~75 min) |

Cost numbers assume the substrate in the table column × the resources declared in each profile's `costEstimate` block. Laptop runs are free. `uber-*` runs are free at run-time on the private substrate; replicating them on a public cloud would cost roughly `aggregate-vCPU × ($0.02–0.03/vCPU-hr on spot) × duration`.

## Picking a target

Resource budget rule (M44 Pod-mode floor): **each KWOK pod needs 1 vCPU + 2 GiB combined** at the dev-50 / failover-* / dev-500 shape, scaling up to **4 vCPU + 8 GiB at the scaleway-50k+ shape** (which packs 100 K Pods per KWOK apiserver and runs against real etcd). The runner's confirmation prompt shows the estimated cost based on your selected profile's `costEstimate.awsSpotUsdPerHour × duration`; you can override duration with `--duration=` and skip the prompt with `--yes`.

| Target | What works there | What it costs |
|---|---|---|
| Laptop / M5 Max kind | `dev-50`, `dev-500` | $0 |
| 1× PRO2-L Kapsule | `scaleway-5k` | ~$0.30/run |
| 2× PRO2-L Kapsule | `failover-*` | ~$0.74–0.90/run |
| 8× PRO2-L Kapsule | `scaleway-50k` | ~$5.50/run |
| 70× PRO2-L Kapsule | `scaleway-500k` | ~$52/run (quota bump) |
| 135× PRO2-L Kapsule | `scaleway-1m` | ~$120/run (quota bump) |
| 640× PRO2-L Kapsule | `scaleway-5m` | ~$1000/run (explicit approval) |
| 2–2000 80/160 GiB hosts | `uber-5k`…`uber-5m` | substrate-dependent |

**Scaleway Kapsule** is the cheapest cloud option that's still a real Kubernetes cluster: free control plane on the Essential tier, per-second billing, ~€0.42/hr on PRO2-L (≈ $0.45/hr). See each profile YAML for the `scw` CLI commands to provision and tear down a cluster.

Nothing in the harness assumes a specific distro; pure Helm + standard Kubernetes APIs. GKE Autopilot is OK because the combined image runs as non-root and declares its ports.

## Cost-model assumptions

- **Coordinator**: 500m vCPU / 1 GiB (emptyDir for stress runs; HA + persistence is a separate test).
- **Shard**: 1 vCPU / 2 GiB at ≤500K machines under management (per-shard ceiling). Add a shard replica per additional 500K — `scaleway-1m` runs `replicas: 2`, `scaleway-5m` runs `replicas: 10`, etc.
- **KWOK pod** (kine or etcd + apiserver + kwok-controller + operator + load-driver):
  - dev-* / failover-* shape (1K Pods/cluster): 500m vCPU / 1 GiB request, 1 vCPU / 2 GiB limit (the M44 floor).
  - scaleway-* shape (100K Pods/cluster): 2 vCPU / 4 GiB request, 4 vCPU / 8 GiB limit.
  - `uber-*` shape (25K Pods/cluster, 10 clusters per host): host packs 10 KWOK pods of ~8 vCPU / 16 GiB each.
- **Prometheus**:
  - dev-* / failover-* / scaleway-{5k,50k}: 1 vCPU / 4 GiB / 20 GiB ephemeral.
  - scaleway-{500k,1m,5m}: 4 vCPU / 16 GiB / 100+ GiB ephemeral (100M+ Pod-bind histograms + 1M+ UpcomingNode series).
- **Scaleway PRO2-L**: 32 vCPU / 128 GiB at ~€0.42/hr (Kapsule, fr-par/nl-ams). Free control plane on the Essential tier. Per-second billing.
- **AWS spot c6i.4xlarge** (16 vCPU / 32 GiB): $0.02–0.03/vCPU-hr varies by region; `us-west-2` is cheapest. Use this to estimate cost of running the `uber-*` ladder on a public cloud — multiply aggregate vCPU × hours × spot rate.
- **EKS control plane**: $0.10/hr fixed (charged regardless of node count).
- **Egress** (snapshot download): TSDB tarballs are 50–500 MB; first 100 GB/month outbound free, then $0.09/GB. Effectively zero at this volume.

## Cost guardrails

The runner will:

1. **Estimate cost up front**. `--profile=scaleway-50k --duration=90m → ~$5.50 estimated`.
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

The runner marks a run failed if any of these p99 thresholds are exceeded. Each one is the best observed value from a passing baseline run plus a small variance margin — they detect regressions, they're not aspirational targets.

| Metric | Threshold | Best observed | Notes |
|---|---|---|---|
| `bigfleet_shard_cycle_duration_seconds` | **100 ms** | 1.8 ms (scaleway-50k) | Decision engine; large headroom intentional. |
| `bigfleet_operator_rollup_duration_seconds` | **1 s** | 122 ms (scaleway-50k) | One rollup pipeline turn must finish well within the 10 s rollup interval. |
| `bigfleet_operator_acknowledge_duration_seconds` | **12 s** | 9.97 s (scaleway-50k) | Bounded by operator status-write QPS against the apiserver. 1 K-CR ramp at QPS=50/Burst=100 needs ~10 s of writes; 12 s allows ~20 % run-to-run variance. Tightens when the operator gains batched status writes or higher per-profile QPS. |

Edit `pass()` in `test/scaletest/cmd/scaletest-runner/main.go` to add more.

## Recommended cadence

| Cadence | Profile | Cost/run | Where |
|---|---|---|---|
| Per-milestone integration gate | `dev-50` | $0 | M5 Max kind |
| Weekly | `scaleway-5k` | ~$0.30 | 1× PRO2-L Kapsule |
| Monthly | `scaleway-50k` | ~$5.50 | 8× PRO2-L Kapsule |
| Quarterly | `scaleway-500k` | ~$52 | 70× PRO2-L Kapsule (quota bump) |
| Annual / pre-release | `scaleway-1m` and `failover-soak` | ~$120 + ~$0.90 | 135× + 2× PRO2-L Kapsule |

Annual budget at this cadence: **~$300/yr** (52 × scaleway-5k + 12 × scaleway-50k + 4 × scaleway-500k + 1 × scaleway-1m + 4 × failover-soak). `scaleway-5m` is reserved for once-per-release-major validation given its ~$1000 substrate cost; the `uber-*` ladder is the routine path for that scale when the private substrate is available.

## Adding a new profile

1. Drop a `test/scaletest/profiles/<name>.yaml` with `kwok.clusterCount`, a `loadProfile`, and a `costEstimate` block.
2. Run it: `scaletest-runner --profile=test/scaletest/profiles/<name>.yaml ...`.
3. If it deserves a baseline number, capture the resulting `summary.json` under `test/scaletest/results/baseline-<name>.json` and reference it in [`scaling-guide.md`](scaling-guide.md).

## Troubleshooting

- **Steady state never reached** — kwok pods aren't all reporting their target CR count. Check `kubectl logs -n bigfleet-scaletest -l app.kubernetes.io/component=kwok-cluster -c harness --tail=50` for individual KWOK clusters; usually it's apiserver port collision or the in-pod sqlite running out of inotify watches.
- **Coordinator OOMKilled** — bump `coordinator.resources.limits.memory` for the profile.
- **Shard cycle p99 alarming** — the simulator is exposing a real bottleneck. Capture the snapshot, compare against the previous run's summary, and follow up with a scale-tuning ADR.

## Cross-references

- Architecture: [`architecture.md`](architecture.md)
- Sizing rationale: [`scaling-guide.md`](scaling-guide.md)
- Production install: [`operator-guide.md`](operator-guide.md)
- Plan §5.1 (scale ceilings): [`plan.md`](plan.md)
