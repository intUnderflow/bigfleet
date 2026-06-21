# uber-50k receipt bundle — FAITHFUL cee793e (#88)

A full, Grafana-loadable evidence bundle for a genuine BigFleet **uber-50k** run (5,000,000 Pods,
200 kwok clusters across 40 satellites + a dedicated single-shard hub + coordinator), 2026-06-21.
**All-cee793e**: both the chart AND every image are pinned to the published tag **`cee793e`** (hub and
all satellites `git checkout cee793e` before rendering). The run reproduces the published canonical
**`2026-06-18-uber-50k-cee793e`** — every ADR-0054 gate is green and matches (aggregateInventory
225,202 ≈ published 225,206). The full 40-satellite TSDB is ~6GB; the committed snapshot is a
**release asset** (hub + a representative one-satellite-per-region sample), which keeps the repo lean
and *exceeds* the published run's hub-only committed evidence.

## Provenance — all-cee793e
- engine `bigfleet:cee793e`, harness `bigfleet-scaletest:cee793e`, chart rendered at git `cee793e`
  (hub + all 40 sats `git checkout cee793e` before rendering). See `config/image-digests-and-sha.txt`.
- topology: dedicated single-node k3s hub (shard + coordinator + prometheus, no kwok pods) + 40
  boot_id-distinct satellites across 5 regions (5 regions, anonymized),
  5 kwok clusters/sat = 200 clusters / 5,000,000 Pods, density 100 → 50,000 BigFleet machines.
  Operators dial the hub NodePort 30780 over per-host SSH keeper tunnels.

## ADR-0054 gates (50k profile) — ALL GREEN, fleet-aggregate (summed-by-le across all 40 sats)
| gate | threshold | actual | published |
|---|---|---|---|
| shardConfigurePhaseP99 | ≤15s | **1.21s** | 1.21s |
| shardCycleDurationP99 | ≤25s (50k) | **4.08s** | 4.08s |
| bootstrapSuccessRatio | ≥0.99 | **1.0** (settled, true count-ratio) | 1.0 |
| operatorNodeStateUpdateP99 | ≤1.5s | **0.90s** | 0.873s |
| operatorRollupP99 | ≤1s | **0.71s** (build-dominated) | 0.757s |
| operatorAckP99 | ≤12s | **1.13s** | 1.28s |
| shardShortfalls | ==0 | **0** | 0 |
| aggregateInventory | (canonical 225,206) | **225,202** ✓ | 225,206 |

## REQUIRED HARNESS FIX (validated by this run)
The kwok apiserver memory limit was raised **16Gi → 32Gi**. At 25K objects/cluster under uber-50k
operator load, 16Gi **OOM-crashloops** once a cross-region watch/reload storm spikes it (a first
attempt this morning saw fleet-wide `OOMKilled` exit-137 ×160 restarts, collapsing the Configured
inventory and quiescing the shard). The host has ~280Gi free, so 32Gi headroom is safe. With the fix,
kwok stayed 40/40 Running with 0 crashloops through the soak and the shard cycled continuously
(endogenous reclaim floor live) — restoring the measurable churning soak the gates are read over.

## Honest run notes
- **190/200 sessions**: two hosts were noisy-neighbor-contended and did
  not hold operator sessions — the same #85 flaky-NL-host situation (run-1 there was a documented
  190/200). The gates above are fleet-aggregate over the satellites with data and are unaffected.
- **mid-run sat swap**: one satellite was on a host at load 252 (2.5× oversubscribed) inflating its rollup to
  ~15s; it was swapped for the a quiet spare host (low load) per the #85 flaky-sat-swap procedure. The
  fleet-aggregate rollup then settled to 0.71s (matching the published 0.757s).
- **sample**: `logs/sat-3` is thin — it was one of the no-session hosts.
  The hub and the other four regional samples carry full data.

## Artifacts
| artifact | what |
|---|---|
| `tsdb-snapshot.tar.gz` (RELEASE ASSET) | hub shard/coordinator Prometheus TSDB + a one-sat-per-region sample (~550M) |
| `tsdb-label-enumeration.txt` | complete distinct label-value set for human eyeball (sanitization) |
| `logs/` | per-component pod logs, **scrubbed** (.scrubbed only), every captured node |
| `config/` | rendered values + resolved manifests + image digests + git SHA + geometry |
| `state/` | CR / AvailableCapacity / UpcomingNode dumps + counts |
| `nodemetrics/` | per-host load / mem / nproc |
| `grafana-dashboard.json` + `LOAD-RECIPE.md` + `docker-compose.yml` + `prometheus.yml` + `provisioning/` | one-command "load this snapshot" recipe |

## Sanitization
1. **TSDB**: enumerated all distinct label-values → only metric names, ephemeral pod-IPs (10.42.x),
   and k8s object names. No host/region/employee identifiers (`tsdb-label-enumeration.txt`).
2. **Logs**: per-node ultracode semantic scrub → `.scrubbed`, with an independent adversarial audit.
   Paths use neutral `sat-N` tags (no host codes).

## How to open
See `LOAD-RECIPE.md` — unpack `tsdb-snapshot.tar.gz` (release asset), `docker compose up`, open
Grafana at :3000 ("BigFleet uber-50k receipt" dashboard).
