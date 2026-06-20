# reclaim drill receipt bundle — FAITHFUL 205fb99 (#88)

A full, Grafana-loadable evidence bundle for a genuine BigFleet **reclaim drill** at uber-5k
(500K pods, 20 kwok clusters across 4 satellites + a single-shard hub), 2026-06-20. **All-205fb99**
(chart AND images both pinned to `205fb99` — the hub and all 4 sats `git checkout 205fb99` before
rendering; 205fb99 `bigfleet-scaletest` was built on each sat for this run). Reproduces the published
`2026-06-19-uber-5k-reclaim-205fb99` drill. The full TSDB is a **release asset**.

## The drill (captured in the TSDB + `csv/gate-timeseries.csv`)
A 50% sustained demand shed (`loadProfile.scaleDowns: [{atSeconds: 600, targetMultiplier: 0.5}]`)
fires after fill; the engine reclaims the now-surplus Configured capacity:

| signal | value |
|---|---|
| Configured (peak → post-shed) | 6520 → 3568 (~45% shed) |
| Idle (post-shed) | 4101 (the reclaimed capacity — Configured→Idle, NOT deprovision) |
| Reclaim actions (total) | 12386 |
| shardShortfalls | 0 (no over-reclaim) |
| shardConfigurePhaseP99 | 0.0099s (≤15) |
| shardCycleDurationP99 | 0.21s (≤5) |
| active sessions | 20/20 |

Demand-shrink reclaim is **Configured→Idle** (the machine returns to the idle pool), so total
inventory is conserved and `Draining`/deprovision stays 0 — the ADR-0045 shrinkage-only contract.

## Provenance — all-205fb99
- engine `bigfleet:205fb99`, harness `bigfleet-scaletest:205fb99` (built per-sat for this run),
  chart rendered at git `205fb99`. See `config/image-digests-and-sha.txt`.
- topology: single-shard hub (no local kwok, coordinator off) + 4 boot_id-distinct intra-region
  satellites × 5 kwok clusters = 20 clusters / 500K pods, all-preBound.

## Artifacts
Same format as the uber-5k cee793e receipt: `tsdb-snapshot.tar.gz` (RELEASE ASSET, ~15M),
`tsdb-label-enumeration.txt`, scrubbed `logs/`, scrubbed `config/`/`state/`/`nodemetrics/`,
`csv/gate-timeseries.csv` (the shed+reclaim cycle), `grafana-dashboard.json` + `LOAD-RECIPE.md` +
`docker-compose.yml` + `prometheus.yml` + `provisioning/`, `deny-list.md`.

## Sanitization (clean — verified two ways)
1. **TSDB**: enumerated distinct label-values → only metric names, ephemeral pod-IPs (10.42.x),
   k8s object names. No host/region/employee identifiers.
2. **Logs + config/state/nodemetrics**: deny-list content scrub + per-node ultracode log scrub with
   an independent adversarial audit. Paths use neutral `sat-N` tags.

## How to open
See `LOAD-RECIPE.md` — unpack the TSDB release asset, `docker compose up`, open Grafana at :3000.
