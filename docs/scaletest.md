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
    --profile=test/scaletest/profiles/dev-5k.yaml \
    --duration=2m \
    --output=./test/scaletest/results/$(date +%Y%m%d-%H%M%S)-dev-5k/
```

The runner prints scale and cost upfront, prompts before any paid run, and tears down on Ctrl-C.

## Profiles

Size tiers (all Pod-mode + realistic 6-archetype catalog by default — M44):

| Profile | KWOK clusters | Pods/cluster | Total Pods | Best target | Cost / run |
|---|---|---|---|---|---|
| `dev-5k` | 5 | 1K | 5K | laptop kind | $0 |
| `scaleway-50k` | 50 | 1K | 50K | 2× PRO2-L Kapsule | ~$0.60 |
| `scaleway-500k` | 50 | 1K | 50K (against 500K seeded inventory) | 2× PRO2-L Kapsule | ~$0.74 |
| `scaleway-1m` | 100 | 1K | 100K (against 1M aggregate) | 4× PRO2-L Kapsule | ~$1.50 |
| `scaleway-5m` | 500 | 1K | 500K (against 5M aggregate) | 16× PRO2-L Kapsule | ~$6 |

Failover scenarios (50 clusters × 1K Pods, distinct purpose — exercise static-stability under coordinator/shard/network disturbance mid-soak):

| Profile | What it kills | Best target |
|---|---|---|
| `failover-leader-kill` | one coordinator-leader-pod | 2× PRO2-L Kapsule |
| `failover-shard-kill` | one shard-pod | 2× PRO2-L Kapsule |
| `failover-partition` | 60s control-plane network partition | 2× PRO2-L Kapsule |
| `failover-soak` | belt-and-braces (2 leader-kills + 1 shard-kill, 60-min soak) | 2× PRO2-L Kapsule |

Cost numbers assume Scaleway Kapsule pricing × the resources declared in each profile's `costEstimate` block. Laptop runs are free.

## Picking a target

Resource budget rule (M44 Pod-mode floor): **each KWOK pod needs 1 vCPU + 2 GiB combined**. A 64 GB target fits ~30 KWOK pods plus the BigFleet shard/coordinator/Prometheus/Grafana overhead. The runner's confirmation prompt shows the estimated cost based on your selected profile's `costEstimate.awsSpotUsdPerHour × duration`; you can override duration with `--duration=` and skip the prompt with `--yes`.

| Target | What works there | What it costs |
|---|---|---|
| Laptop / M5 Max kind | dev-5k | $0 |
| 2× PRO2-L Kapsule | scaleway-{50k, 500k}, failover-* | ~$0.60–0.74/run |
| 4× PRO2-L Kapsule | scaleway-1m | ~$1.50/run |
| 16× PRO2-L Kapsule | scaleway-5m | ~$6/run |

**Scaleway Kapsule** is the cheapest cloud option that's still a real Kubernetes cluster: free control plane on the Essential tier, per-second billing, ~$0.42/hr on PRO2-L. See each profile YAML for the `scw` CLI commands to provision and tear down a cluster.

Nothing in the harness assumes a specific distro; pure Helm + standard Kubernetes APIs. GKE Autopilot is OK because the combined image runs as non-root and declares its ports.

## Cost-model assumptions

- **Coordinator**: 1 vCPU / 2 GB / emptyDir for stress runs (HA + persistence is a separate test).
- **Shard**: 1 vCPU / 4 GB per ~50K simulated machines. Scales linearly.
- **KWOK pod** (kine + apiserver + kwok-controller + operator + load-driver): 0.4 vCPU / 0.5 GB sustained, 0.6 vCPU / 0.7 GB peak.
- **Prometheus**: 1 vCPU / 4 GB / 20 GB ephemeral.
- **EKS control plane**: $0.10/hr fixed (charged regardless of node count).
- **AWS spot c6i.4xlarge**: $0.20–0.30/vCPU-hr (varies by region; us-west-2 is cheapest).
- **Egress** (snapshot download): TSDB tarballs are 50–500 MB; first 100 GB/month outbound free, then $0.09/GB. Effectively zero at this volume.

## Cost guardrails

The runner will:

1. **Estimate cost up front**. `--profile=scaleway-5m --duration=30m → ~$6 estimated`.
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
  "runId": "20260501-130000-dev-5k",
  "profile": "dev-5k",
  "target":   { "context": "kind-bigfleet", "kind": "kind" },
  "cost":     { "estimatedUsd": 0.07, "hours": 0.33 },
  "scale":    { "kwokClusters": 5, "machinesPerCr": 1000, "totalCrs": 5000 },
  "metrics": {
    "shardCycleDurationP99Seconds":   0.014,
    "operatorRollupP99Seconds":       0.087,
    "coordinatorApplyOpsPerSec":      4.2,
    "shardShortfalls":                0,
    "loadgenCRsActive":               5000,
    "loadgenCRsCreatedPerSec":        4.1
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
| Every PR (optional, local) | dev-5k | $0 | M5 Max kind |
| Weekly | scaleway-50k | ~$0.60 | 2× PRO2-L Kapsule |
| Monthly | scaleway-500k | ~$0.74 | 2× PRO2-L Kapsule |
| Quarterly | scaleway-1m or scaleway-5m | $1.50–$6 | 4–16× PRO2-L Kapsule |
| Pre-release | failover-soak | ~$0.90 | 2× PRO2-L Kapsule |

Annual budget at this cadence: **~$50/yr** (12 × scaleway-500k + 4 × scaleway-1m + 4 × scaleway-5m + a handful of failover-soaks).

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
