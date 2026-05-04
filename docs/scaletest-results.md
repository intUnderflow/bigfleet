# Scale-test results

Each run is one full pass through the scaletest harness: chart install, ramp to steady state, soak, prometheus snapshot, summary. Runs live in [`test/scaletest/results/`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results) on GitHub; this page is generated from each run's `summary.json` and refreshes whenever the site builds.

## Cumulative trajectory at 500K

The scaleway-500k profile is the production-shape benchmark: 50 simulated clusters, 50 000 demand CRs, 500 000 pre-seeded inventory machines on a 5-node Scaleway Kapsule (PRO2-M, nl-ams). Each milestone landed a real shard or harness change; the line below tracks shard cycle p99 over them.

**4.03 s → 62 ms** (98.5 % reduction across 11 runs).

![scaleway-500k cycle p99 across milestones](./scaletest-progress.svg)

The 100 ms SLO line is the runner's gate. The first passing run was `20260504-091949-scaleway-500k-tmpfs`.

## All runs

| run | profile | cycle p99 | ack p99 | rollup p99 | active CRs | passed |
|---|---|---|---|---|---|---|
| [`m11.9b`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-145151-m11.9b) | dev-5k | 1 ms | — | 2.05 s | 5000 | ✗ |
| [`m11.10`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-150219-m11.10) | dev-5k | 1 ms | 20.48 s | 508 ms | 5000 | ✓ |
| [`local-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-153557-local-50k) | local-50k | 7 ms | 18.91 s | 2.05 s | 50000 | ✗ |
| [`local-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-154725-local-50k) | local-50k | 8 ms | 10.48 s | 1.85 s | 49999 | ✗ |
| [`local-50k-v3`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-160516-local-50k-v3) | local-50k | 4 ms | 9.99 s | 1.47 s | 49999 | ✗ |
| [`scaleway-50k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-165809-scaleway-50k) | scaleway-50k | 2 ms | 9.97 s | 122 ms | 50000 | ✓ |
| [`scaleway-50k-v2`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-174325-scaleway-50k-v2) | scaleway-50k | 2 ms | 8.07 s | 79 ms | 50000 | ✓ |
| [`scaleway-500k`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260501-190456-scaleway-500k) | scaleway-500k | 4.03 s | 513 ms | 18 ms | 50000 | ✗ |
| [`scaleway-500k-m1117`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-022847-scaleway-500k-m1117) | scaleway-500k | 4.07 s | 493 ms | 16 ms | 49997 | ✗ |
| [`scaleway-500k-m1118`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-025246-scaleway-500k-m1118) | scaleway-500k | 4.08 s | 542 ms | 16 ms | 50000 | ✗ |
| [`scaleway-500k-m1120`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-104945-scaleway-500k-m1120) | scaleway-500k | 3.69 s | 459 ms | 16 ms | 48000 | ✗ |
| [`scaleway-500k-m1122`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-115342-scaleway-500k-m1122) | scaleway-500k | 706 ms | 496 ms | 19 ms | 47999 | ✗ |
| [`scaleway-500k-m1123`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260502-124541-scaleway-500k-m1123) | scaleway-500k | 634 ms | 504 ms | 16 ms | 50000 | ✗ |
| [`scaleway-500k-m1124a`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260503-224604-scaleway-500k-m1124a) | scaleway-500k | 507 ms | 391 ms | 16 ms | 49000 | ✗ |
| [`scaleway-500k-warmup-split`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260503-233227-scaleway-500k-warmup-split) | scaleway-500k | 512 ms | 475 ms | 780 ms | 14755 | ✗ |
| [`scaleway-500k-fakeidx`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-001519-scaleway-500k-fakeidx) | scaleway-500k | 239 ms | 278 ms | 694 ms | 17586 | ✗ |
| [`scaleway-500k-snapall`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-083940-scaleway-500k-snapall) | scaleway-500k | 156 ms | 4.64 s | 900 ms | 37329 | ✗ |
| [`scaleway-500k-tmpfs`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/20260504-091949-scaleway-500k-tmpfs) | scaleway-500k | 62 ms | 380 ms | 830 ms | 13464 | ✓ |

*Generated from `test/scaletest/results/*/summary.json` by `site/scripts/sync-scaletest.mjs`.*
