# 2-shard partition + soak failover receipt bundle — c24dfc8 chart / 205fb99 engine (#88)

A full, Grafana-loadable evidence bundle for a genuine BigFleet **2-shard failover partition + soak**
drill (uber-5k shape: 375K pods, 15 kwok clusters split 8/7 across 2 shards + a coordinator,
3 satellites), 2026-06-20, **intra-region (Oregon)**. Reproduces the published
`2026-06-20-failover-partition-soak-2shard-c24dfc8` (#87 Run 2). The full TSDB is a **release asset**.

## The two drills (captured in the TSDB + `csv/`)

### Partition (`csv/partition-timeline.csv`)
A deny-all-egress NetworkPolicy on `bigfleet-shard-1` severs shard-1→coordinator (operator inbound
unaffected), held 60s, then removed. **Static stability**: shard-1 keeps advancing its decision
cycles during the partition, shard-0 unaffected, **shortfall 0** — losing the coordinator link does
not stall the shard's local decisions (the ADR-0045 steady-state contract).

### Soak (`csv/soak-timeline.csv`, compressed 30 min)
Three correlated disturbances: **kill-coordinator-leader @300s & @720s**, **kill-shard
bigfleet-shard-1 @1200s**. The load-bearing result:

| signal | result |
|---|---|
| shardShortfalls (whole soak, across all 3 kills) | **0** — data plane uninterrupted |
| survivor shard-0 sessions | held **8** |
| coordinator after both leader-kills | **raft_term=2**, Running (recovered) |
| shard-cycle p99 | 0.127s |

**Honest framing (matches the published run):** the coordinator is single-replica, so a leader-kill
is a **persisted-leadership RELOAD** (raft_term stays 2), NOT a peer-contested election — the
load-bearing check is the **data plane staying up** across all three kills, which it did (shortfall 0).
After the shard-1 kill, shard-1's operator sessions reconnect **gradually** (keeper-tunnel /
control-plane transient); the data plane is unaffected throughout.

## Provenance
- chart at git `c24dfc8` (2-shard NodePort routing + coordinator); harness `bigfleet-scaletest:c24dfc8`;
  engine `bigfleet:205fb99`. See `config/image-digests-and-sha.txt`.
- topology: 2-shard hub + coordinator (no local kwok) + 3 boot_id-distinct intra-region sats ×
  5 kwok clusters = 15 clusters / 375K pods, all-preBound.

## Artifacts
Standard receipt format: `tsdb-snapshot.tar.gz` (RELEASE ASSET, ~18M), `tsdb-label-enumeration.txt`,
scrubbed `logs/` (both shards + coordinator), scrubbed `config/`/`state/`/`nodemetrics/`,
`csv/partition-timeline.csv` + `csv/soak-timeline.csv`, `grafana-dashboard.json` + `LOAD-RECIPE.md` +
`docker-compose.yml` + `prometheus.yml` + `provisioning/`, `deny-list.md`.

## Sanitization (clean — verified two ways)
1. **TSDB**: enumerated distinct label-values → only metric names, ephemeral pod-IPs (10.42.x),
   k8s object names. No host/region/employee identifiers.
2. **Logs + config/state/nodemetrics**: deny-list content scrub + per-node ultracode log scrub with
   an independent adversarial audit. Paths use neutral `sat-N` tags.

## How to open
See `LOAD-RECIPE.md` — unpack the TSDB release asset, `docker compose up`, open Grafana at :3000.
