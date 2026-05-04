# Scale-test results

Each run is one full pass through the scaletest harness: chart install, ramp to steady state, soak, prometheus snapshot, summary. Runs live in [`test/scaletest/results/`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results) on GitHub; this page is generated from each run's `summary.json` and refreshes whenever the site builds.

Outcomes on this page are **re-evaluated under the current SLO definition** — sustained active CRs ≥ 99.9 % of target, cycle p99 ≤ 100 ms, rollup p99 ≤ 1 s, ack p99 ≤ 12 s. Older runs that were recorded as `passed: true` by an earlier runner without a sustained-load gate appear here as ✗ when they didn't hold target load. Re-evaluation is intentional: the SLO numbers from an under-loaded run don't say anything about behaviour at the actual benchmark.

## Cumulative trajectory at 500K

The scaleway-500k profile is the production-shape benchmark: 50 simulated clusters, 50 000 demand CRs, 500 000 pre-seeded inventory machines on a 5-node Scaleway Kapsule (PRO2-M, nl-ams). Each milestone landed a real shard or harness change; the chart below tracks shard cycle p99 across them.

**4.03 s → 55 ms** (98.6 % reduction). The most recent run that meets the SLO at full sustained load is [`scaleway-500k-cleangate`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-184208-scaleway-500k-cleangate).

![scaleway-500k cycle p99 across milestones](./scaletest-progress.svg)

The dashed blue line is the 100 ms cycle SLO. Bars are coloured green only when the run held target load *and* hit every SLO ceiling.

## All runs

| run | profile | cycle p99 | ack p99 | rollup p99 | active CRs / target | target met | passed (current SLO) |
|---|---|---|---|---|---|---|---|
| [`m11.9b`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-145151-m11.9b) | dev-5k | 1 ms | — | 2.05 s | 5000 / 5000 | ✓ | ✗ |
| [`m11.10`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-150219-m11.10) | dev-5k | 1 ms | 20.48 s | 508 ms | 5000 / 5000 | ✓ | ✗ |
| [`local-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-153557-local-50k) | local-50k | 7 ms | 18.91 s | 2.05 s | 50000 / 50000 | ✓ | ✗ |
| [`local-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-154725-local-50k) | local-50k | 8 ms | 10.48 s | 1.85 s | 49999 / 50000 | ✓ | ✗ |
| [`local-50k-v3`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-160516-local-50k-v3) | local-50k | 4 ms | 9.99 s | 1.47 s | 49999 / 50000 | ✓ | ✗ |
| [`scaleway-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-165809-scaleway-50k) | scaleway-50k | 2 ms | 9.97 s | 122 ms | 50000 / 50000 | ✓ | ✓ |
| [`scaleway-50k-v2`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-174325-scaleway-50k-v2) | scaleway-50k | 2 ms | 8.07 s | 79 ms | 50000 / 50000 | ✓ | ✓ |
| [`scaleway-500k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-190456-scaleway-500k) | scaleway-500k | 4.03 s | 513 ms | 18 ms | 50000 / 50000 | ✓ | ✗ |
| [`scaleway-500k-m1117`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-022847-scaleway-500k-m1117) | scaleway-500k | 4.07 s | 493 ms | 16 ms | 49997 / 50000 | ✓ | ✗ |
| [`scaleway-500k-m1118`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-025246-scaleway-500k-m1118) | scaleway-500k | 4.08 s | 542 ms | 16 ms | 50000 / 50000 | ✓ | ✗ |
| [`scaleway-500k-m1120`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-104945-scaleway-500k-m1120) | scaleway-500k | 3.69 s | 459 ms | 16 ms | 48000 / 50000 | ✗ | ✗ |
| [`scaleway-500k-m1122`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-115342-scaleway-500k-m1122) | scaleway-500k | 706 ms | 496 ms | 19 ms | 47999 / 50000 | ✗ | ✗ |
| [`scaleway-500k-m1123`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-124541-scaleway-500k-m1123) | scaleway-500k | 634 ms | 504 ms | 16 ms | 50000 / 50000 | ✓ | ✗ |
| [`scaleway-500k-m1124a`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260503-224604-scaleway-500k-m1124a) | scaleway-500k | 507 ms | 391 ms | 16 ms | 49000 / 50000 | ✗ | ✗ |
| [`scaleway-500k-warmup-split`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260503-233227-scaleway-500k-warmup-split) | scaleway-500k | 512 ms | 475 ms | 780 ms | 14755 / 50000 | ✗ | ✗ |
| [`scaleway-500k-fakeidx`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-001519-scaleway-500k-fakeidx) | scaleway-500k | 239 ms | 278 ms | 694 ms | 17586 / 50000 | ✗ | ✗ |
| [`scaleway-500k-snapall`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-083940-scaleway-500k-snapall) | scaleway-500k | 156 ms | 4.64 s | 900 ms | 37329 / 50000 | ✗ | ✗ |
| [`scaleway-500k-tmpfs`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-091949-scaleway-500k-tmpfs) | scaleway-500k | 62 ms | 380 ms | 830 ms | 13464 / 50000 | ✗ | ✗ |
| [`scaleway-500k-cleanup`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-170304-scaleway-500k-cleanup) | scaleway-500k | 63 ms | 3.31 s | 878 ms | 27893 / 50000 | ✗ | ✗ |
| [`scaleway-500k-strictgate`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-171932-scaleway-500k-strictgate) | scaleway-500k | 112 ms | 1.17 s | 867 ms | 35262 / 50000 | ✗ | ✗ |
| [`scaleway-500k-50kfloor`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-175730-scaleway-500k-50kfloor) | scaleway-500k | 104 ms | 1.15 s | 894 ms | 30399 / 50000 | ✗ | ✗ |
| [`scaleway-500k-cleangate`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-184208-scaleway-500k-cleangate) | scaleway-500k | 55 ms | 156 ms | 16 ms | 50000 / 50000 | ✓ | ✓ |
| [`scaleway-50k-verify`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-202548-scaleway-50k-verify) | scaleway-50k | 16 ms | 255 ms | 15 ms | 50000 / 50000 | ✓ | ✓ |

*Generated from `test/scaletest/results/*/summary.json` by `site/scripts/sync-scaletest.mjs`. Outcomes recomputed under the current SLO bar.*
