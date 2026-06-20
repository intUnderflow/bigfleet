# uber-5k receipt bundle — FAITHFUL cee793e (#88)

A full, Grafana-loadable evidence bundle for a genuine BigFleet **uber-5k** run (500K pods,
20 kwok clusters across 4 satellites + a single-shard hub + coordinator), 2026-06-20.
**This is the faithful per-tag receipt** that replaces the version-mixed format pilot: both the
chart AND every image are pinned to the published tag **`cee793e`** (the pilot ran a 205fb99 hub
against cee793e sats — a version mix; this fixes it). The run reproduces the published canonical
exactly — **aggregateInventory 22529 matches the published `2026-06-17-uber-5k-cee793e` row**.
The full TSDB is a **release asset** (keeps the repo lean); everything else is in-tree here.

## Provenance — all-cee793e
- engine `bigfleet:cee793e`, harness `bigfleet-scaletest:cee793e`, chart rendered at git `cee793e`
  (the hub and all 4 sats `git checkout cee793e` before rendering). See `config/image-digests-and-sha.txt`.
- topology: single-shard hub + coordinator (no local kwok) + 4 boot_id-distinct intra-region
  satellites × 5 kwok clusters = 20 clusters / 500K pods, all-preBound, 20 sessions, 0 shortfalls.

## ADR-0054 gates (measured over a 20-min steady soak — all GREEN)
| gate | threshold | actual |
|---|---|---|
| shardConfigurePhaseP99 | ≤15s | 0.54s |
| shardCycleDurationP99 | ≤5s | 0.127s |
| shardShortfalls | ==0 | 0 |
| bootstrapSuccessRatio | ≥0.99 | 1.0 |
| activeSessions | 20/20 | 20 |
| aggregateInventory | (canonical 22529) | 22529 ✓ |

## Artifacts + sizes
| artifact | what |
|---|---|
| `tsdb-snapshot.tar.gz` (RELEASE ASSET) | combined hub+satellite Prometheus TSDB (full retention, native resolution) — the core receipt (~13M) |
| `tsdb-label-enumeration.txt` | complete distinct label-value set for human eyeball |
| `logs/` | 32 component pod logs, **scrubbed** (.scrubbed only), every node |
| `config/` | rendered values + resolved manifests + image digests + git SHA + geometry (scrubbed) |
| `state/` | CR / AvailableCapacity / UpcomingNode dumps + counts (scrubbed) |
| `nodemetrics/` | per-host load / mem / nproc (scrubbed) |
| `grafana-dashboard.json` + `LOAD-RECIPE.md` + `docker-compose.yml` + `prometheus.yml` + `provisioning/` | one-command "load this snapshot" recipe |
| `deny-list.md` | sanitization deny-list + result |

## Sanitization (clean — verified two ways)
1. **TSDB**: enumerated all distinct label-values → only metric names, ephemeral pod-IPs (10.42.x),
   and k8s object names. No host/region/employee identifiers. (`tsdb-label-enumeration.txt`.)
2. **Logs + config/state/nodemetrics**: deny-list content scrub (config/state/nodemetrics) +
   per-node ultracode semantic scrub of logs → `.scrubbed`, with an **independent adversarial audit**.
   Paths use neutral `sat-N` tags (no host codes).

## How to open
See `LOAD-RECIPE.md` — unpack `tsdb-snapshot.tar.gz` (release asset), `docker compose up`, open
Grafana at :3000 ("BigFleet uber-5k receipt" dashboard).
