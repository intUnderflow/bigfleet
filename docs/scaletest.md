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

| Profile | KWOK clusters | CRs/cluster | Total | Best target | Cost / run |
|---|---|---|---|---|---|
| `dev-5k` | 5 | 1K | 5K | laptop kind | $0 |
| `local-50k` | 50 | 1K | 50K | M5 Max kind | $0 |
| `homelab-500k` | 500 | 1K | 500K | homelab | $0 |
| `cloud-5m` | 5,000 | 1K | 5M | EKS spot | ~$35–40 |
| `thundering-herd` | 100 | 5K burst | 500K peak | homelab | $0 |
| `failover-soak` | 50 | 1K | 50K | M5 Max / homelab | $0 |

Cost numbers assume AWS spot `c6i.4xlarge` × the resources declared in each profile's `costEstimate` block. Homelab and laptop runs are free (amortised power not counted).

## Picking a target

Resource budget rule: **(cluster total RAM in GB) × 5 = max KWOK pod count**. A 64 GB target fits ~300 pods comfortably. The runner's confirmation prompt shows the estimated cost based on your selected profile's `costEstimate.awsSpotUsdPerHour × duration`; you can override duration with `--duration=` and skip the prompt with `--yes`.

| Target | What works there | What it costs |
|---|---|---|
| Laptop kind | dev-5k, failover-soak | $0 |
| M5 Max kind | dev-5k, local-50k, failover-soak | $0 |
| Homelab k3s/Talos | up to homelab-500k, thundering-herd | $0 |
| GKE Autopilot | up to homelab-500k (cost ~ Standard tier) | ~ $1.50/vCPU-hr |
| EKS spot | every profile incl. cloud-5m | ~ $0.20–0.30/vCPU-hr |

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

1. **Estimate cost up front**. `--profile=cloud-5m --duration=60m → ~$35 estimated`.
2. **Prompt for confirmation** when the target context name suggests a cloud (`eks`, `gke`, `aks`, `aws`, `gcp`, `azure` substring) and the estimated cost ≥ $5. Skipped with `--yes`.
3. **Hard-cap runtime** with `--max-duration` (default 2h). Auto-teardown if the soak hangs.
4. **Always run teardown**, even on Ctrl-C, via `defer helm uninstall`.
5. **Tag every cloud resource** the chart creates with `bigfleet-scaletest-run=<run-id>` (via Helm `runId` value). If anything escapes, AWS-side cleanup is one filtered terminate-instances call.

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

## Pass/fail SLOs

The runner marks a run failed if any of:

- `bigfleet_shard_cycle_duration_seconds` p99 > **100 ms**
- `bigfleet_operator_rollup_duration_seconds` p99 > **1 s**

Edit `pass()` in `test/scaletest/cmd/scaletest-runner/main.go` to add more.

## Recommended cadence

| Cadence | Profile | Cost/run | Where |
|---|---|---|---|
| Every PR (optional, local) | dev-5k | $0 | M5 Max kind |
| Weekly | homelab-500k | $0 | Homelab |
| Monthly | thundering-herd | $0 | Homelab |
| Quarterly | cloud-5m | $35 | EKS spot |
| Pre-release | failover-soak | $0 | M5 Max / homelab |

Annual budget at this cadence: **~$160/yr.**

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
