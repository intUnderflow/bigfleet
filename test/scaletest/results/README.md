# Scale-test results

Captured runs from the BigFleet scale-test harness. Each run lives in its own directory:

```
test/scaletest/results/<UTC-timestamp>-<profile>/
├── summary.json              # metrics + scale + cost + pass/fail
└── prometheus-snapshot.tar.gz # full TSDB for the run
```

Add new runs by setting `--output=test/scaletest/results/$(date +%Y%m%d-%H%M%S)-<profile>/` when running `scaletest-runner`. Empty dirs from interrupted runs are fine to delete.

## Baselines

The most recent passing run per profile is the current baseline. Update this table when a new run becomes the reference number — older runs stay in version control as history.

| Profile | Run | shard cycle p99 | operator rollup p99 | operator ack p99 | CRs sustained | CRs/sec | Pass | Notes |
|---|---|---|---|---|---|---|---|---|
| dev-5k       | [20260501-150219-m11.10](20260501-150219-m11.10/) | 1.4 ms | 0.51 s | 20.5 s | 5,000  | 2     | ✓ | M11.10 baseline (rollup metric split) |
| **scaleway-50k** | **[20260501-174325-scaleway-50k-v2](20260501-174325-scaleway-50k-v2/)** | **1.9 ms** | **79 ms** | 8.07 s | 50,000 | 118 | **✓** | Current baseline. M11.13 (per-profile QPS=200/burst=400/ack-concurrency=64) + M11.14 (patch-based ack writes, 1 RPC per CR instead of 2). Rollup p99 dropped 122 ms → 79 ms (−35 %); ack p99 dropped 9.97 s → 8.07 s (−19 %). Run cost ~$0.05 on Scaleway nl-ams PRO2-M. |
| scaleway-50k (v1, pre M11.13/14) | [20260501-165809-scaleway-50k](20260501-165809-scaleway-50k/) | 1.8 ms | 0.12 s | 10.0 s | 50,000 | 135 | ✓ | First passing 50K run. Kept as the comparison record; demonstrates the M5 Max-kind 1.47 s rollup p99 was contention, not algorithmic. |
| local-50k    | [20260501-160516-local-50k-v3](20260501-160516-local-50k-v3/) | 3.8 ms | 1.47 s | 10.0 s | 49,999 | 202   | ✗ | After M11.12 CPU-limit bump (kwok pods 800m → 2 CPU). Microbenchmarks show buildRollup at 1K CRs takes 4.2 ms in-process — the residual 1.47 s rollup p99 is environmental contention (cache reflector locks, gRPC stream backpressure, CFS throttling), not algorithmic. Real production deployments with one process per pod should see <50 ms rollup p99 at this scale. |
| local-50k (cache) | [20260501-154725-local-50k](20260501-154725-local-50k/) | 7.9 ms | 1.85 s | 10.5 s | 49,999 | 200   | ✗ | M11.11 result: informer-backed cache landed; rollup dropped from 2.05 s → 1.85 s, ack from 18.9 s → 10.5 s. Kept as a comparison point against M11.12's CPU-limit bump. |
| local-50k (pre-M11.11) | [20260501-153557-local-50k](20260501-153557-local-50k/) | 7.4 ms | ≥2.05 s* | 18.9 s | 50,000 | 194   | ✗ | Pre-cache regression record. Kept for diff-against-baseline. |
| homelab-500k | — | — | — | — | — | — | — | Equivalent profile run on Scaleway captured below. |
| **scaleway-500k** | [20260502-124541-scaleway-500k-m1123](20260502-124541-scaleway-500k-m1123/) | **0.63 s** | 16 ms | 504 ms | 50,000 demand / 500,000 inventory | 50 | ✗ | M11.23 Phase 3 fingerprint grouping. Cloud cycle p99 dropped 0.71 s → 0.63 s (−11 %). Smaller cloud delta than the M5 phase-dump implied (M5: phase3 mean 131 ms → 11 ms; total cycle 283 → 153 ms). Two reasons the p99 didn't move further: (1) cold-start cycle 1's full reconcile still sits in the histogram's right tail; (2) execute tail spikes from gRPC stream RTT under load. The mean per-cycle steady-state cost is well under the SLO now; characterising and clipping the p99 tail is the next milestone target. Demand fully satisfied (`shortfalls=0`). |
| scaleway-500k (M11.22) | [20260502-115342-scaleway-500k-m1122](20260502-115342-scaleway-500k-m1122/) | 0.71 s | 19 ms | 496 ms | 50,000 / 500,000 | 50 | ✗ | M11.22 incremental reconcile via `since_revision`. Shard cycle p99 dropped 3.69 s → 0.71 s (−81 %). Bigger than the M5 phase-dump alone projected — cloud reaped the same delta plus secondary wins (less GC pressure, less memory bandwidth pressure from per-cycle 500K proto round-trips). Phase 3 (reclaim) became the dominant cost; M11.23 addressed it. |
| scaleway-500k (M11.20) | [20260502-104945-scaleway-500k-m1120](20260502-104945-scaleway-500k-m1120/) | 3.69 s | 16 ms | 459 ms | 50,000 / 500,000 | 50 | ✗ | M11.19 (incremental snapshots) + M11.20 (cost-ordered cursor with lazy MatchProfile). 4.08 s → 3.69 s (−10 %); the cloud delta was much smaller than the M5 bench projection because phase1 wasn't the bottleneck — reconcile was. M11.21's phase histograms exposed it; M11.22 fixed it. |
| scaleway-500k (M11.18, real path) | [20260502-025246-scaleway-500k-m1118](20260502-025246-scaleway-500k-m1118/) | 4.08 s | 16 ms | 542 ms | 50,000 / 500,000 | 50 | ✗ | M11.18 + parallel operator dispatch. **Real production path** (no LocalBootstrap). Shard execute parallelised at concurrency 128; operator side dispatches each frame in its own goroutine. Demand fully satisfied (`shortfalls=0`). Same 4 s shard cycle p99 as M11.17 — establishing that the cycle is bounded by **decision-phase compute** (Phase 1/2/3 walking 500K inventory), not execute. |
| scaleway-500k (M11.17, LocalBootstrap) | [20260502-022847-scaleway-500k-m1117](20260502-022847-scaleway-500k-m1117/) | 4.07 s | 16 ms | 493 ms | 50,000 / 500,000 | 50 | ✗ | Pre-M11.18 with the harness bypassing operator-stream RTT via LocalBootstrap. Confirmed shard cycle = decision compute, not execute or stream RTT. |
| scaleway-500k (pre-M11.17) | [20260501-190456-scaleway-500k](20260501-190456-scaleway-500k/) | 4.03 s | 18 ms | 513 ms | 50,000 / 500,000 | 50 | ✗ | First 500K real-cluster run. Shard cycle p99 looked similar (4 s) but for a different reason: serial gRPC RTTs through operator streams blocked execute. CPU was idle. Kept as the comparison record. |
| cloud-5m     | — | — | — | — | — | — | — | not yet captured |
| thundering-herd | — | — | — | — | — | — | — | not yet captured |
| failover-soak | — | — | — | — | — | — | — | not yet captured |

\* hit the 2.048s histogram bucket cap; actual could be higher.

## What the harness numbers actually measure

The KWOK pods bundle `kine + kube-apiserver + kwok-controller + bigfleet-operator + load-driver` in **one** Pod. They share CPU, share the shared cgroup memory, and the operator's apiserver is a single-node sqlite-backed kine. None of those are how you'd run BigFleet in production.

Microbenchmarks at 1K CRs/cluster (`go test -bench=BuildRollup ./pkg/operator/...` on the M5 Max):

| Path | ns/op | Per CR |
|---|---|---|
| `buildRollup` (list iteration + profile fingerprint + aggregate + proto encode) | 471 µs | 471 ns |
| `listCapacityRequests` (cache deep-copy through controller-runtime fake) | 3.58 ms | 3.6 µs |
| Combined (list + build) | 4.20 ms | — |

**The algorithmic ceiling at 1K CRs/cluster is ~5 ms of in-process CPU.** The harness's observed 1.47 s rollup p99 is **300× slower**: shared-pod CFS throttling, cache reflector lock contention under heavy watch event flow, and the gRPC stream's send-buffer backpressure when 50 operators all flush rollups at the top of the cycle.

**Don't extrapolate harness latency directly into the scaling guide.** The harness is excellent for finding regressions, throughput ceilings, and protocol behavior under load. For per-rollup latency expectations in real deployments, multiply the microbench by a small factor for cache + stream overhead, not the harness measurement. We'll calibrate more accurately once a real-cluster baseline run lands.

## How to add a baseline run

1. Run the profile end-to-end with `scaletest-runner --profile=…`. Use a duration that gives at least 2 minutes of post-ramp soak so the `[5m]` rate window has stable data.
2. Confirm the run passed (exit 0) — if it failed, capture it as a regression record under `regressions/` instead.
3. Update the baseline table above. Older entries stay where they are.
4. Commit the new directory + the table edit in the same PR.

## Reproducing a previous run

The Prometheus TSDB snapshot is portable. Replay against a local `prom/prometheus`:

```sh
mkdir -p /tmp/replay
tar -xzf <run>/prometheus-snapshot.tar.gz -C /tmp/replay
docker run --rm -p 9090:9090 -v /tmp/replay:/prometheus prom/prometheus:v2.55.0 \
  --storage.tsdb.path=/prometheus --web.enable-admin-api
```

Open `http://localhost:9090` and query whatever the original `summary.json` left out. The TSDB carries every scraped series for the run.

## Regression handling

A failing run for a profile that previously passed → file an issue, archive the run under `regressions/<date>-<profile>/`, and don't update the baseline. New baselines only land when they're as good or better than the previous one on the same profile.

## See also

- [`../../docs/scaletest.md`](../../docs/scaletest.md) — how to run the harness
- [`../../docs/scaling-guide.md`](../../docs/scaling-guide.md) — sizing model the baselines validate
