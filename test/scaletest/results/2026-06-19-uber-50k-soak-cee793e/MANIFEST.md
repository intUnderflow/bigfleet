# uber-50k multi-hour soak (leak / gate-hold) receipt — FAITHFUL cee793e (#88)

Grafana-loadable evidence for a **multi-hour soak** (~4.9h continuous shard uptime, restarts=0) on the
standing clean BigFleet **uber-50k** fleet (5,000,000 Pods, 200 kwok clusters / 40 satellites +
dedicated single-shard hub + coordinator), 2026-06-21. All-cee793e (chart + images) + 32Gi
kwok-apiserver fix. Reproduces the published **`2026-06-19-uber-50k-soak-cee793e`** leak/gate-hold finding.

## LEAK-FREE over the steady soak (see csv/soak-leak-timeline.csv)
| signal | steady-window | verdict |
|---|---|---|
| shard goroutines | 1204–1222 (Δ −6) | **DEAD-FLAT** — no goroutine leak |
| shard open fds | 196–200 (Δ −1) | **DEAD-FLAT** — no fd leak |
| shard RSS | 1452–1680 MB (Δ **−180 MB**) | **bounded GC-sawtooth, NON-MONOTONIC** — not a leak |
| inventory machines | stable 225,200 | no leaked machines |

The definitive leak indicators (goroutines, fds) are dead-flat over a 3h steady window; RSS is bounded
and decreasing (GC working), not monotonically growing.

## ADR-0054 gates — HELD throughout the soak (fleet-aggregate, summed-by-le across all 40 sats)
shardConfigurePhaseP99 **1.18s** (≤15), shardCycleDurationP99 **4.08s** (≤25), bootstrapSuccessRatio
**1.0** (≥0.99), operatorNodeStateUpdateP99 **0.90s** (≤1.5), operatorRollupP99 **0.71s** (≤1),
operatorAckP99 **1.13s** (≤12), shardShortfalls **0** (whole soak), aggregateInventory **225,202**.

Reclaim is the **bounded + constant endogenous in-flight floor** (the documented #64–70 behavior, NOT
M67 oscillation) — bounded, not accelerating, no machine accumulation.

## Provenance — all-cee793e
engine `bigfleet:cee793e`, harness `bigfleet-scaletest:cee793e`, chart at git `cee793e`. Dedicated
single-node hub + 40 boot_id-distinct satellites across 5 regions, 200 clusters / 5,000,000 Pods.
REQUIRED HARNESS FIX: kwok apiserver memory 16Gi → 32Gi.

## Honest notes
The soak spanned a coordinator-failover (leader-kill) and a noisy-neighbor sat-swap mid-run;
goroutines/fds returned to baseline after both events (no leak introduced). 190/200 baseline sessions
(two hosts in one region noisy-neighbor-contended, per #85; fleet-aggregate gates unaffected).
`logs/sat-3` (one regional sample) thin.

## Artifacts
`tsdb-snapshot.tar.gz` (RELEASE ASSET, see `https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-uber50k-20260621/tsdb-uber-50k-soak-cee793e.tar.gz`): hub + one-sat-per-region TSDB sample
spanning the ~5h soak. `csv/soak-leak-timeline.csv`, `tsdb-label-enumeration.txt`, `logs/` (scrubbed),
`config/` `state/` `nodemetrics/`, `grafana-dashboard.json` + `LOAD-RECIPE.md` + `docker-compose.yml` +
`prometheus.yml` + `provisioning/`.

## Sanitization
TSDB labels enumerated (no host/region/employee identifiers); logs scrubbed (same-fleet deterministic
scrub on identically-audited hosts + comprehensive PII verification); neutral `sat-N` tags.
