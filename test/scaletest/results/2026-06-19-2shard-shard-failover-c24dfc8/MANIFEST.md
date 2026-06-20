# 2-shard shard-kill failover receipt bundle — c24dfc8 chart / 205fb99 engine (#88)

A full, Grafana-loadable evidence bundle for a genuine BigFleet **2-shard failover-shard-kill** drill
(uber-2.5k shape: 250K pods, 10 kwok clusters split 5/5 across 2 shards, 2 satellites + a 2-shard
hub), 2026-06-20, **intra-region (Oregon)** for a clean substrate. Reproduces the published
`2026-06-19-failover-shard-kill-2shard-intraregion-c24dfc8`. The full TSDB is a **release asset**.

## The drill (the signal a single-shard leader-kill can't produce)
A genuine 2-shard deploy (c24dfc8: per-ordinal NodePorts 30780/30781; keeper `REPLICAS=2` opens one
`-L` per ordinal; sat `shard.overrideHost`/`overridePortBase` routes cluster N→shard N%2). Steady
routing is **balanced 5/5** across both shards. Then `bigfleet-shard-1` is deleted and we observe
**blast-radius containment**:

| signal | value |
|---|---|
| routing split (shard-0 / shard-1) | 5 / 5 (balanced) |
| survivor shard-0 sessions through the kill | held **5/5**, drift σ=**0** |
| killed shard-1 recreate | **fresh pod (restarts=0) 30s** after delete (StatefulSet reschedule) |
| shardShortfalls (whole window) | **0** |
| configP99 / cycleP99 max | 0.23s / 0.23s |
| bootstrapSuccessRatio min | 1.0 |

**Kill confirmed via pod `creationTimestamp`** (shard-1 deleted 17:50:28Z → recreated 17:50:58Z),
NOT the session gauge: `bigfleet_shard_active_sessions{pod="bigfleet-shard-1"}` is a Prometheus
instant value that holds the deleted pod's last reading ~5min, so a fast kill+recreate that lands
between 15s samples is invisible in sessions (the #85 session-staleness lesson). The TSDB +
`csv/shardkill-timeline.csv` carry the per-shard timeline; the pod lifecycle is the kill proof.

## Provenance
- chart rendered at git `c24dfc8` (the 2-shard NodePort routing); harness `bigfleet-scaletest:c24dfc8`;
  engine `bigfleet:205fb99` (unchanged from c24dfc8 — c24dfc8 added only chart + entrypoint, no Go).
  See `config/image-digests-and-sha.txt`.
- topology: 2-shard hub (no local kwok, coordinator off) + 2 boot_id-distinct intra-region sats ×
  5 kwok clusters = 10 clusters / 250K pods, all-preBound.

## Artifacts
Standard receipt format: `tsdb-snapshot.tar.gz` (RELEASE ASSET, ~7M), `tsdb-label-enumeration.txt`,
scrubbed `logs/` (incl. both shard logs), scrubbed `config/`/`state/`/`nodemetrics/`,
`csv/shardkill-timeline.csv` (the kill window) + `csv/gate-timeseries.csv`, `grafana-dashboard.json`
+ `LOAD-RECIPE.md` + `docker-compose.yml` + `prometheus.yml` + `provisioning/`, `deny-list.md`.

## Sanitization (clean — verified two ways)
1. **TSDB**: enumerated distinct label-values → only metric names, ephemeral pod-IPs (10.42.x),
   k8s object names. No host/region/employee identifiers.
2. **Logs + config/state/nodemetrics**: deny-list content scrub + per-node ultracode log scrub with
   an independent adversarial audit. Paths use neutral `sat-N` tags.

## How to open
See `LOAD-RECIPE.md` — unpack the TSDB release asset, `docker compose up`, open Grafana at :3000.
