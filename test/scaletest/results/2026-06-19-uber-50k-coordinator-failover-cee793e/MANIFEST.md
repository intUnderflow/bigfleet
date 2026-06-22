# uber-50k coordinator-failover (leader-kill) receipt — FAITHFUL cee793e (#88)

Grafana-loadable evidence for a **coordinator-failover (leader-kill) static-stability** test on a live
BigFleet **uber-50k** fleet (5,000,000 Pods, 200 kwok clusters / 40 satellites + dedicated single-shard
hub + single-replica coordinator) at a **stable, clean 200/200**, 2026-06-22. All-cee793e + 32Gi fix.
Reproduces published `2026-06-19-uber-50k-leaderkill-cee793e`.

## Test + result — STATIC STABILITY HELD (see csv/failover-timeline.csv)
The coordinator leader pod was deleted while 5M Pods run.
| signal | baseline | through kill+recovery |
|---|---|---|
| coordinator pod | 1/1 Running | deleted -> **1/1 Running in ~31s** |
| raft term | 2 | **2** (single replica recovers persisted leadership; no peer -> no election increment) |
| active sessions | 200/200 | **200/200** (held) |
| shardShortfalls | 0 | **0** |
| shardCycleDurationP99 | 4.08s | **4.08s** |
| bootstrapSuccessRatio | 1.000 | 1.000 |
| failures | - | **[]** |

The data plane was untouched through the coordinator restart at a true 200/200.

## Provenance + fix
engine/harness/chart all `cee793e`; dedicated single-node hub + 40 boot_id-distinct sats across 5 regions,
200 clusters / 5M Pods. REQUIRED HARNESS FIX: kwok apiserver 16Gi->32Gi.

## Artifacts
`tsdb-snapshot.tar.gz` (RELEASE ASSET, see `https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-uber50k-20260621/tsdb-uber-50k-coordinator-failover-cee793e.tar.gz`): hub + one-sat-per-region TSDB spanning the
kill+recovery. `csv/failover-timeline.csv`, `tsdb-label-enumeration.txt`, `logs/` (scrubbed), `config/`
`state/` `nodemetrics/`, `grafana-dashboard.json` + `LOAD-RECIPE.md` + `docker-compose.yml` + `prometheus.yml`
+ `provisioning/`.

## Sanitization
TSDB labels enumerated (no host/region/employee identifiers); logs scrubbed + comprehensive PII
verification; neutral `sat-N` tags.
