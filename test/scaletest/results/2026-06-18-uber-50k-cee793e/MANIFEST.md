# uber-50k receipt bundle — FAITHFUL cee793e (#88)

A full, Grafana-loadable evidence bundle for a genuine BigFleet **uber-50k** run (5,000,000 Pods,
200 kwok clusters across 40 satellites + a dedicated single-shard hub + coordinator) at a **stable,
clean 200/200 sessions**, 2026-06-21. **All-cee793e** (chart + every image pinned to `cee793e`).
Reproduces the published canonical **`2026-06-18-uber-50k-cee793e`** — every ADR-0054 gate green and
matching, at a true 200/200. The full 40-satellite TSDB is ~6GB; the committed snapshot is a
**release asset** (hub + a one-satellite-per-region sample), exceeding the published hub-only evidence.

## ADR-0054 gates (50k profile) — ALL GREEN at 200/200, fleet-aggregate (summed-by-le across 40 sats)
| gate | threshold | actual | published |
|---|---|---|---|
| shardConfigurePhaseP99 | ≤15s | **1.10s** | 1.21s |
| shardCycleDurationP99 | ≤25s (50k) | **4.08s** | 4.08s |
| bootstrapSuccessRatio | ≥0.99 | **1.0** (no_session 0 at 200/200) | 1.0 |
| operatorNodeStateUpdateP99 | ≤1.5s | **0.49s** | 0.873s |
| operatorRollupP99 | ≤1s | **0.87s** | 0.757s |
| operatorAckP99 | ≤12s | **1.12s** | 1.28s |
| shardShortfalls | ==0 | **0** | 0 |
| aggregateInventory | (canonical 225,206) | **225,203** | 225,206 |

## Provenance — all-cee793e
engine `bigfleet:cee793e`, harness `bigfleet-scaletest:cee793e`, chart rendered at git `cee793e`
(hub + all 40 sats `git checkout cee793e`). See `config/image-digests-and-sha.txt`. Topology:
dedicated single-node k3s hub (shard + coordinator + prometheus, no kwok) + 40 boot_id-distinct
satellites across 5 regions, 5 kwok clusters/sat = 200 clusters / 5,000,000 Pods, density 100.
Operators dial the hub NodePort 30780 over per-host SSH keeper tunnels.

## REQUIRED HARNESS FIX (validated by this run)
kwok apiserver memory **16Gi → 32Gi**. At 25K objects/cluster under uber-50k operator load, 16Gi
**OOM-crashloops** once a cross-region watch/reload storm spikes it (collapses Configured inventory +
quiesces the shard). Host has ~280Gi free → 32Gi headroom safe; kwok then holds 40/40 Running.

## Reaching a stable 200/200
The bootstrap gate is session-completeness-sensitive: any cluster whose operator can't hold its
session churns `no_session` bootstraps that drag the raw ratio (the engine bootstrap-success-when-a-
session-exists stays ~1.0 throughout — substrate, not engine). A few flaky cross-region hosts were
swapped for quiet spares (per the #85 flaky-sat-swap procedure; dead clusters identified from the
shard log `err="no active operator"`) until the fleet held a stable **200/200** with `no_session` 0 →
clean bootstrap 1.0. The captured state is that clean, stable 200/200.

## Artifacts
| artifact | what |
|---|---|
| `tsdb-snapshot.tar.gz` (RELEASE ASSET, see `https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-uber50k-20260621/tsdb-uber-50k-cee793e.tar.gz`) | hub shard/coordinator TSDB + a one-sat-per-region sample |
| `tsdb-label-enumeration.txt` | distinct label-value set (sanitization) |
| `logs/` | per-component pod logs, **scrubbed** (.scrubbed only), every captured node |
| `config/` `state/` `nodemetrics/` | rendered values + manifests + image digests + git SHA + geometry + CR dumps + node metrics |
| `grafana-dashboard.json` + `LOAD-RECIPE.md` + `docker-compose.yml` + `prometheus.yml` + `provisioning/` | one-command load recipe |

## Sanitization
TSDB labels enumerated (only metric names, ephemeral pod-IPs, k8s object names — no host/region/
employee identifiers); logs per-node scrubbed (deterministic same-fleet scrub on hosts the flagship's
adversarial audit already cleared + comprehensive PII verification); neutral `sat-N` tags.
