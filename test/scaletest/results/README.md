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
| local-50k    | [20260501-154725-local-50k](20260501-154725-local-50k/) | 7.9 ms | 1.85 s | 10.5 s | 49,999 | 200   | ✗ | Operator now uses an informer-backed cache (M11.11): ack p99 dropped from 18.9 s → 10.5 s, rollup p99 from ≥2.05 s → 1.85 s (no longer histogram-capped). Remaining rollup latency is in-pod CPU (profile fingerprinting + proto encode for 1K CRs); next investigation is profile-the-walk. |
| local-50k (pre-M11.11) | [20260501-153557-local-50k](20260501-153557-local-50k/) | 7.4 ms | ≥2.05 s* | 18.9 s | 50,000 | 194   | ✗ | Pre-cache regression record. Kept for diff-against-baseline. |
| homelab-500k | — | — | — | — | — | — | — | not yet captured |
| cloud-5m     | — | — | — | — | — | — | — | not yet captured |
| thundering-herd | — | — | — | — | — | — | — | not yet captured |
| failover-soak | — | — | — | — | — | — | — | not yet captured |

\* hit the 2.048s histogram bucket cap; actual could be higher.

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
