# Scale-test results

BigFleet turns each cluster's capacity demand into provisioned, configured nodes through pluggable providers — it does **not** place pods ([what BigFleet is](./papers/bigfleet.md)). This page is the canonical record of how far that is proven, against the full `realistic.yaml` workload catalog (gpu-training, memory-db, co-location gangs) and a **real, default, _uncapped_ kube-scheduler**. BigFleet is graded only on the capacity-delivery hops it *owns* — never the cluster's scheduler — and is forbidden from reconfiguring that scheduler to make its own SLO pass ([what we gate](#what-we-gate-and-why-the-bar-is-honest)).

**Ladder:** `uber-5k` ✅ · `uber-50k` ✅ · `uber-500k` ▫️ · `uber-1m` ▫️ · `uber-5m` ▫️

## Headline result — `uber-50k` (commit `cee793e`)

One shard sustaining the full realistic-catalog demand of a ~50,000-machine fleet (~5,000,000 pods) across 40 hosts in 5 regions through a real, default, uncapped kube-scheduler — every hop BigFleet owns inside SLO, **zero unmet demand**, **reproduced across 4 independent runs** (each a freshly re-surveyed fleet; engine numbers invariant run-to-run).

| gate | [result 1 ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-18-uber-50k-cee793e) | [result 2 ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-18-uber-50k-run1-cee793e) | [result 3 ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-18-uber-50k-run2-cee793e) | [result 4 ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-18-uber-50k-run3-cee793e) | SLA |
|---|---:|---:|---:|---:|---:|
| shortfalls | 0 | 0 | 0 | 0 | = 0 |
| bootstrap success | 1.00 | 1.00 | 1.00 | 1.00 | ≥ 0.99 |
| configure-phase p99 | 1.21 s | 1.15 s | 1.21 s | 1.23 s | ≤ 15 s |
| node-state-publish p99 | 873 ms | 1.02 s | 1.02 s | 1.02 s | ≤ 1.5 s |
| roll-up p99 | 757 ms | 800 ms | 800 ms | 1.00 s | ≤ 1 s |
| shard cycle p99 | 4.08 s | 4.08 s | 4.08 s | 4.08 s | ≤ 5 s |
| ack p99 | 1.28 s | 1.28 s | 1.28 s | 1.28 s | ≤ 12 s |
| pod-bind p50 | 1.60 s | 1.60 s | 1.60 s | 1.60 s | ≤ 10 s |

Each `result` column header links to that run's committed run summary. Every result clears every SLA.

> End-to-end pod-bind p99 is **not** gated and is large by design — it is dominated by the uncapped scheduler's retry/backoff and the reprovision back-edge, neither of which is BigFleet's deliverable. See [what we gate](#what-we-gate-and-why-the-bar-is-honest).


## What we gate, and why the bar is honest

The principle ([ADR-0054](./adr/0054-steady-bind-slo-reframe-for-uncapped-scheduler.md), [full justification in SLOs](./slos.md)): **gate BigFleet's deliverable, never an uncontrolled dependency.** The harness runs a real, *uncapped* kube-scheduler and a real provisioning back-edge; the latencies those impose are *reported*, never gated — and BigFleet may not cap the scheduler to make its own numbers pass (author decision). So the bar decomposes "demand observed → machine materialised → node published" into the per-hop bars BigFleet actually owns, measured at **steady state under churn** (not the cold-start ramp — ramp is capacity exploration, not pass/fail).

**Gated — BigFleet's own hops:**
- **shortfalls** = 0 — breach means demand left unmet — the one contract violation, no headroom by construction.
- **bootstrap success** ≥ 0.99 — breach means node materialisation is failing, not merely slow.
- **configure-phase p99** ≤ 15 s — breach means a machine is taking too long to become a configured node.
- **node-state-publish p99** ≤ 1.5 s — breach means the operator is slow to publish the ready node back to the cluster.
- **roll-up p99** ≤ 1 s — breach means the operator is slow to report a cluster's demand.
- **shard cycle p99** ≤ 5 s — breach means the decision loop is falling behind demand.
- **ack p99** ≤ 12 s — breach means capacity-request acknowledgement is backing up.
- **pod-bind p50** ≤ 10 s — breach means the common (median) bind path broke — a loose liveness floor.

**Informational — reported, never gated:** end-to-end pod-bind p99 + raw-max, and fingerprint fan-out latency. The pod-bind tail runs to hundreds of seconds because a churn-reclaimed pod cannot re-bind until a replacement machine is provisioned (the reprovision back-edge) and because the uncapped scheduler backs off on retry — physics outside BigFleet's contract.

Two of the gates are anti-gaming guards: **shortfalls = 0** has no percentile headroom — no reshape makes unmet demand acceptable — and **bootstrap success** catches a materialisation-throughput collapse that latency-plus-shortfall gates alone could miss. The reframe strictly *increased* coverage (the node-state-publish hop was previously ungated).

## The validated-scale ladder (uber-*)

The workload is the full `realistic.yaml` archetype catalog — gpu-training, memory-db, co-location gangs — calibrated to a realistic machine fleet (ADR-0050): the hard demand shape, not a toy. The larger rungs are sequential and gated on **test-fleet capacity, not on the engine** — what each rung costs to run, and why 500k/5m need dedicated infrastructure, is in [scale-test resource requirements](./scaletest-resource-requirements.md). Each rung's full numbers live in its run folder; the headline scorecard above carries the top rung's.

| rung | scale | status | data |
|---|---|:--|:--|
| `uber-5k` | ~5,000-machine fleet · ~500K pods · 1 shard | ✅ passed | [run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-17-uber-5k-cee793e) · [Prometheus snapshot ↗](https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-20260620/tsdb-snapshot-cee.tar.gz) · [load in Grafana ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-17-uber-5k-cee793e/LOAD-RECIPE.md) |
| `uber-50k` | ~50,000-machine fleet · ~5M pods · 1 shard | ✅ passed | [run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-18-uber-50k-cee793e) |
| `uber-500k` | planned | ▫️ planned | — |
| `uber-1m` | planned | ▫️ planned | — |
| `uber-5m` | planned | ▫️ planned | — |

_⏳ next and ▫️ planned are sequencing states, not failures — the ladder is in progress._


## Resilience & robustness

Beyond the throughput ladder, these runs stress what happens when things go wrong or change — a multi-hour soak, control-plane failover, a shard kill, a demand collapse — on the same `realistic.yaml` workload catalog. They are **not** scored against the eight capacity-delivery gates above; each has its own pass criterion (shown per result), and the verdict is read from the run's committed `summary.json`.

### `uber-50k — 5-hour soak` — ✅ passed · commit `cee793e`

*5M pods · 1 shard · 5 h*

Endurance at headline scale: 5,000,000 pods sustained for five hours under churn — does anything leak or drift?

**Pass criterion:** All eight capacity-delivery gates hold throughout, no resource leak (flat goroutines / open fds, bounded RSS), and zero leaked machines.

| metric | value |
|---|---:|
| duration | 5 h |
| goroutines (start → end) | 1,274 → 1,274 |
| open fds (start → end) | 210 → 210 |
| shard RSS (start → end) | 1,475 MB → 1,576 MB |
| leaked machines (max, transitional) | 8 |
| shortfalls | 0 |
| shard cycle p99 | 4.08 s |

_Steady reclaim held at the documented bounded floor (≈2.93/s) — the endogenous in-flight-churn rate, now codified as the bounded-reclaim gate, not drift._

[run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-uber-50k-soak-cee793e)

### `uber-50k — coordinator failover` — ✅ passed · commit `cee793e`

*5M pods · coordinator killed · 1 shard*

Static stability: kill the coordinator at 5M pods — does the data plane keep delivering capacity while the control plane is down?

**Pass criterion:** The data plane keeps cycling through the coordinator restart — sessions held, zero shortfalls. (The 'clusters keep running with BigFleet's coordinator down' hard rule.)

| metric | value |
|---|---:|
| data-plane sessions (min during kill) | 200 |
| shortfalls (max) | 0 |
| coordinator recovery | 1 min |
| shard cycle p99 | 4.08 s |
| bootstrap success | 1.00 |

_Single coordinator replica — this validates data-plane static stability while the coordinator is absent/restarting (the hard rule), not a multi-node Raft leader election._

[run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-uber-50k-coordinator-failover-cee793e)

### `2-shard shard failover` — ✅ passed · commit `c24dfc8`

*250K pods · 2 shards · intra-region*

Data-plane shard failover: on a genuine two-shard deploy, kill one shard — is the blast radius contained to its own clusters?

**Pass criterion:** The surviving shard holds all its sessions; the killed shard reschedules and recovers; zero shortfalls throughout.

| metric | value |
|---|---:|
| cluster split (shard-0 / shard-1) | 5 / 5 |
| survivor sessions held | 5 |
| killed shard recovered sessions | 5 |
| killed shard recreate time | 31 s |
| shortfalls (max) | 0 |
| configure-phase p99 (max) | 131 ms |

_Validates the per-ordinal shard routing (commit c24dfc8) end-to-end. The ~31 s kill+recreate landed between metric samples, so this captures the clean outcome (no lingering degradation) but not the drop→reconnect transition; ≤5 s sampling would capture it._

[run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-2shard-shard-failover-c24dfc8)

📦 **Receipts:** [Prometheus snapshot ↗](https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-20260620/tsdb-snapshot-2shard.tar.gz) · [scrubbed logs + config ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-2shard-shard-failover-c24dfc8) · [load in Grafana ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-2shard-shard-failover-c24dfc8/LOAD-RECIPE.md)

### `uber-5k — scale-down / reclaim` — ✅ passed · commit `205fb99`

*500K pods · 50% demand shed*

Scale-down: shed 50% of demand mid-soak — does BigFleet reclaim the surplus in a bounded, converging way (no thrash)?

**Pass criterion:** Reclaim stays under the bound, converges to a steady floor, no over-reclaim (zero shortfalls). Inverted posture: reclaim is the expected, healthy outcome here.

| metric | value |
|---|---:|
| configured (peak → converged) | 5,513 → 3,818 |
| reclaimed to idle | 1,695 |
| reclaim actions / bound | 5,127 / 6,000 |
| converged reclaim floor | 0.51/s |
| shortfalls | 0 |

_The bounded-reclaim gate this exercised is now committed on the 50k profile (settleSeconds + maxReclaimActionsDuringSoak)._

[run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-uber-5k-reclaim-205fb99)

📦 **Receipts:** [Prometheus snapshot ↗](https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-20260620/tsdb-snapshot-reclaim.tar.gz) · [scrubbed logs + config ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-uber-5k-reclaim-205fb99) · [load in Grafana ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-19-uber-5k-reclaim-205fb99/LOAD-RECIPE.md)

### `2-shard failover — partition + soak` — ✅ passed · commit `c24dfc8`

*375K pods · 2 shards · 15 clusters*

Control-plane partition then a multi-disturbance soak, on a genuine two-shard deploy: sever a shard from the coordinator, then kill leaders + a shard — does the data plane keep delivering capacity throughout?

**Pass criterion:** Both shards keep cycling through the coordinator partition (static stability); across the soak's two leader-kills + a shard-kill the data plane is unaffected, blast radius is contained, and shortfalls stay 0.

| metric | value |
|---|---:|
| partition: shard-1 cycles run during the sever | 62 |
| partition: shortfalls | 0 |
| soak: leader-kills survived | 2 |
| soak: sessions held through the kills | 15 |
| soak: survivor sessions held (shard-kill) | 8 |
| soak: killed shard recovered (of baseline) | 5 / 7 |
| soak: shortfalls | 0 |
| shard cycle p99 (max, through soak) | 127 ms |

_Single coordinator replica — the leader-kills are static-stability checks (the data plane survives the coordinator restart), not a Raft leader election. uber-5k-scale 2-shard deploy; the per-ordinal routing (c24dfc8) is the same one validated at 5M in the 2-shard-throughput run._

[run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-20-2shard-failover-partition-soak-c24dfc8)

📦 **Receipts:** [Prometheus snapshot ↗](https://github.com/intUnderflow/bigfleet/releases/download/scaletest-receipts-20260620/tsdb-snapshot-partsoak.tar.gz) · [scrubbed logs + config ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-20-2shard-failover-partition-soak-c24dfc8) · [load in Grafana ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-06-20-2shard-failover-partition-soak-c24dfc8/LOAD-RECIPE.md)


## Reproduce & trust

The profiles and substrates are committed and substrate-agnostic ([ADR-0034](./adr/0034-scaletest-byo-substrate.md)) — bring your own substrate and run the same gate:

```
make scaletest PROFILE=test/scaletest/profiles/5k.yaml SUBSTRATE=test/scaletest/substrates/example-fat-host.yaml
```

`uber-5k` is the published *label* for the `5k.yaml` profile run on Uber-donated compute — there is no `uber-5k.yaml` to hunt for. Example substrates ship for a laptop and for fatter hosts: [`example-kind-laptop`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/substrates/example-kind-laptop.yaml), [`example-mid-host`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/substrates/example-mid-host.yaml), [`example-fat-host`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/substrates/example-fat-host.yaml).

**Recreate the dashboard.** The Grafana dashboard ships in the repo ([`dashboards/scaletest.json`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/chart/dashboards/scaletest.json)); point it at any Prometheus carrying BigFleet's metrics. Published canonical runs also include a Prometheus snapshot you can load to replay the run's status over time (added per run as it is published).

**Per-run artefacts.** Raw run data (logs, full Prometheus) is dev-box-local and not committed; this page is the canonical record. The sanitised numeric results that *are* committed — each run's `summary.json` plus a `chain-numbers.csv` time-series — live in that run's folder, linked from the ladder and the configuration-variant table.

## How a result is graded

Every gate is measured at **steady state under sustained churn**, never during the cold-start ramp ([ADR-0035](./adr/0035-scaletest-slos-at-steady-state.md)): ramp is capacity exploration, not a pass/fail signal. Per-machine and per-frame bars are held identical across the whole ladder; only genuinely size-scaling quantities get size-dependent thresholds ([ADR-0028](./adr/0028-cycle-p99-is-regime-parametric.md)). Pass/fail on this page is computed from each run's committed `summary.json` against the current gate set, so a run's own recorded verdict may differ (e.g. against a since-retired saturated bind-latency metric). Separately, the shard's per-cycle decision cost was driven from seconds to tens of milliseconds over the engine-optimisation milestones — the headroom the cycle gate now runs against.

<details>
<summary><strong>uber-5k configuration runs</strong> (transparency — pre-reframe metric set)</summary>

Earlier `uber-5k` runs across host/cluster configurations, each with its sanitised numbers committed in the linked run folder. These predate the ADR-0054 reframe — older metric set, and the bind-latency p99 they recorded was the since-retired saturated metric — so the folder's numbers are on the metrics they captured, not the current gate set, and these are configuration variants, **not** ladder rungs.

| run | load | pass | data |
|---|---|:--:|:--|
| `uber-5k (single-host)` | 5,831 / 500,000 | ✗ | [run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-05-13-uber-5k) · [csv ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-05-13-uber-5k/chain-numbers.csv) |
| `uber-5k-wide` | 499,993 / 500,000 | ✗ | [run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-05-13-uber-5k-wide) · [csv ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-05-13-uber-5k-wide/chain-numbers.csv) |
| `uber-5k-2host` | 249,995 / 250,000 | ✓ | [run folder ↗](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/2026-05-16-uber-5k-2host-20x25k) |

</details>


*Generated from `test/scaletest/results/*/{summary,page}.json` by `site/scripts/sync-scaletest.mjs`.*
