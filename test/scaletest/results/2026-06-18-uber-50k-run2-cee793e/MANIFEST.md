# uber-50k reproducibility receipt #2 — FAITHFUL cee793e (#88)

Grafana-loadable evidence for a reproducibility sample on the standing clean BigFleet **uber-50k** fleet
(5,000,000 Pods, 200 kwok clusters / 40 satellites + dedicated single-shard hub + coordinator) at a
**stable, clean 200/200 sessions**, 2026-06-22. All-cee793e + 32Gi kwok-apiserver fix. Reproduces published
`2026-06-18-uber-50k-run2-cee793e`.

## ADR-0054 gates (50k profile) — ALL GREEN at 200/200, fleet-aggregate (summed-by-le / 40 sats)
| gate | threshold | actual | published canonical |
|---|---|---|---|
| shardConfigurePhaseP99 | <=15s | **1.18s** | 1.21s |
| shardCycleDurationP99 | <=25s (50k) | **4.08s** | 4.08s |
| bootstrapSuccessRatio | >=0.99 | **1.000** | 1.0 |
| operatorNodeStateUpdateP99 | <=1.5s | **0.90s** | 0.873s |
| operatorRollupP99 | <=1s | **0.93s** | 0.757s |
| operatorAckP99 | <=12s | **1.13s** | 1.28s |
| shardShortfalls | ==0 | **0** | 0 |
| aggregateInventory | (canonical 225,206) | **225203** | 225,206 |

Reproducibility sample on the long-running clean fleet (gates measured fresh at this capture). REQUIRED
HARNESS FIX: kwok apiserver 16Gi->32Gi. Flaky cross-region hosts were swapped for quiet spares to hold a
stable 200/200 (per #85; dead clusters identified from the shard log).

## Artifacts
`tsdb-snapshot.tar.gz` (RELEASE ASSET, see `https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-uber50k-20260621/tsdb-uber-50k-run2-cee793e.tar.gz`): hub + one-sat-per-region TSDB sample.
`tsdb-label-enumeration.txt`, `logs/` (scrubbed), `config/` `state/` `nodemetrics/`, `grafana-dashboard.json`
+ `LOAD-RECIPE.md` + `docker-compose.yml` + `prometheus.yml` + `provisioning/`.

## Sanitization
TSDB labels enumerated (no host/region/employee identifiers); logs scrubbed (deterministic same-fleet
scrub + comprehensive PII verification); neutral `sat-N` tags.
