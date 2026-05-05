# Scale-test results

Each run is one full pass through the scaletest harness: chart install, ramp to steady state, soak, prometheus snapshot, summary. Runs live in [`test/scaletest/results/`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results) on GitHub; this page is generated from each run's `summary.json` and refreshes whenever the site builds.

Outcomes on this page are **re-evaluated under the current SLO definition** — sustained active CRs ≥ 99.9 % of target, cycle p99 ≤ 100 ms, rollup p99 ≤ 1 s, ack p99 ≤ 12 s. Older runs that were recorded as `passed: true` by an earlier runner without a sustained-load gate appear here as ✗ when they didn't hold target load. Re-evaluation is intentional: the SLO numbers from an under-loaded run don't say anything about behaviour at the actual benchmark.

## Per-shard 500K optimisation trajectory

The paper sets 500K machines as the per-shard cycle ceiling (Phase 3 walks the inventory each cycle), so most BigFleet optimisation work happens against the scaleway-500k profile: a single shard, 50 simulated clusters, 50 000 demand CRs, 500 000 pre-seeded inventory machines on Scaleway Kapsule (PRO2-M, fr-par / nl-ams). Each milestone landed a real shard or harness change; the chart below tracks shard cycle p99 across those runs. Multi-shard tests (scaleway-1m, scaleway-5m) build on the per-shard ceiling validated here.

**4.03 s → 48 ms** (98.8 % reduction). The most recent single-shard run that meets the SLO at full sustained load is [`scaleway-500k-2node`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260505-011213-scaleway-500k-2node).

![per-shard 500K cycle p99 across milestones](./scaletest-progress.svg)

The dashed blue line is the 100 ms cycle SLO. Bars are coloured green only when the run held target load *and* hit every SLO ceiling.

## All runs

The rundir name encodes the fleet size tested (scaleway-500k = single-shard 500K, scaleway-1m = 2 × 500K, scaleway-5m = 10 × 500K — the paper's per-shard ceiling is 500K, so multi-shard runs are aggregate-named). "load" is observed sustained CRs over the target. A run only passes if it held target load *and* hit every SLO.

| run | cycle p99 | ack p99 | rollup p99 | load | pass |
|---|---|---|---|---|---|
| [`m11.9b`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-145151-m11.9b) | 1 ms | — | 2.05 s | 5000 / 5000 | ✗ |
| [`m11.10`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-150219-m11.10) | 1 ms | 20.48 s | 508 ms | 5000 / 5000 | ✗ |
| [`local-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-153557-local-50k) | 7 ms | 18.91 s | 2.05 s | 50000 / 50000 | ✗ |
| [`local-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-154725-local-50k) | 8 ms | 10.48 s | 1.85 s | 49999 / 50000 | ✗ |
| [`local-50k-v3`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-160516-local-50k-v3) | 4 ms | 9.99 s | 1.47 s | 49999 / 50000 | ✗ |
| [`scaleway-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-165809-scaleway-50k) | 2 ms | 9.97 s | 122 ms | 50000 / 50000 | ✓ |
| [`scaleway-50k-v2`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-174325-scaleway-50k-v2) | 2 ms | 8.07 s | 79 ms | 50000 / 50000 | ✓ |
| [`scaleway-500k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-190456-scaleway-500k) | 4.03 s | 513 ms | 18 ms | 50000 / 50000 | ✗ |
| [`scaleway-500k-m1117`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-022847-scaleway-500k-m1117) | 4.07 s | 493 ms | 16 ms | 49997 / 50000 | ✗ |
| [`scaleway-500k-m1118`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-025246-scaleway-500k-m1118) | 4.08 s | 542 ms | 16 ms | 50000 / 50000 | ✗ |
| [`scaleway-500k-m1120`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-104945-scaleway-500k-m1120) | 3.69 s | 459 ms | 16 ms | 48000 / 50000 | ✗ |
| [`scaleway-500k-m1122`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-115342-scaleway-500k-m1122) | 706 ms | 496 ms | 19 ms | 47999 / 50000 | ✗ |
| [`scaleway-500k-m1123`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-124541-scaleway-500k-m1123) | 634 ms | 504 ms | 16 ms | 50000 / 50000 | ✗ |
| [`scaleway-500k-m1124a`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260503-224604-scaleway-500k-m1124a) | 507 ms | 391 ms | 16 ms | 49000 / 50000 | ✗ |
| [`scaleway-500k-warmup-split`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260503-233227-scaleway-500k-warmup-split) | 512 ms | 475 ms | 780 ms | 14755 / 50000 | ✗ |
| [`scaleway-500k-fakeidx`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-001519-scaleway-500k-fakeidx) | 239 ms | 278 ms | 694 ms | 17586 / 50000 | ✗ |
| [`scaleway-500k-snapall`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-083940-scaleway-500k-snapall) | 156 ms | 4.64 s | 900 ms | 37329 / 50000 | ✗ |
| [`scaleway-500k-tmpfs`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-091949-scaleway-500k-tmpfs) | 62 ms | 380 ms | 830 ms | 13464 / 50000 | ✗ |
| [`scaleway-500k-cleanup`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-170304-scaleway-500k-cleanup) | 63 ms | 3.31 s | 878 ms | 27893 / 50000 | ✗ |
| [`scaleway-500k-strictgate`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-171932-scaleway-500k-strictgate) | 112 ms | 1.17 s | 867 ms | 35262 / 50000 | ✗ |
| [`scaleway-500k-50kfloor`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-175730-scaleway-500k-50kfloor) | 104 ms | 1.15 s | 894 ms | 30399 / 50000 | ✗ |
| [`scaleway-500k-cleangate`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-184208-scaleway-500k-cleangate) | 55 ms | 156 ms | 16 ms | 50000 / 50000 | ✓ |
| [`scaleway-50k-verify`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-202548-scaleway-50k-verify) | 16 ms | 255 ms | 15 ms | 50000 / 50000 | ✓ |
| [`scaleway-50k-repro1`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-210908-scaleway-50k-repro1) | 16 ms | 252 ms | 15 ms | 50000 / 50000 | ✓ |
| [`scaleway-50k-repro2`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-210908-scaleway-50k-repro2) | 16 ms | 236 ms | 15 ms | 49999 / 50000 | ✓ |
| [`scaleway-50k-repro3`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-210908-scaleway-50k-repro3) | 16 ms | 239 ms | 15 ms | 49999 / 50000 | ✓ |
| [`scaleway-50k-repro4`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-210908-scaleway-50k-repro4) | 16 ms | 244 ms | 15 ms | 50000 / 50000 | ✓ |
| [`scaleway-500k-z1`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-221018-scaleway-500k-z1) | 61 ms | 202 ms | 26 ms | 50000 / 50000 | ✓ |
| [`scaleway-500k-z2`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-221018-scaleway-500k-z2) | 60 ms | 156 ms | 24 ms | 50000 / 50000 | ✓ |
| [`scaleway-500k-z3`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-221018-scaleway-500k-z3) | 61 ms | 217 ms | 16 ms | 49998 / 50000 | ✓ |
| [`scaleway-500k-z4`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-221018-scaleway-500k-z4) | 61 ms | 189 ms | 21 ms | 50000 / 50000 | ✓ |
| [`scaleway-500k-z5`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-221018-scaleway-500k-z5) | 62 ms | 181 ms | 20 ms | 49999 / 50000 | ✓ |
| [`scaleway-500k-z6`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-221018-scaleway-500k-z6) | 54 ms | 146 ms | 16 ms | 49999 / 50000 | ✓ |
| [`scaleway-500k-rightsize`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260505-005134-scaleway-500k-rightsize) | 61 ms | 157 ms | 16 ms | 50000 / 50000 | ✓ |
| [`scaleway-500k-2node`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260505-011213-scaleway-500k-2node) | 48 ms | 109 ms | 15 ms | 50000 / 50000 | ✓ |
| [`scaleway-1m`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260505-020833-scaleway-1m) | 31 ms | 158 ms | 26 ms | 50000 / 50000 | ✓ |

*Generated from `test/scaletest/results/*/summary.json` by `site/scripts/sync-scaletest.mjs`. Outcomes recomputed under the current SLO bar.*
