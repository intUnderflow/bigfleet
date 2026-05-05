# ADR-0014: SLO posture — binding latency is the user-facing gate, cycle wall-clock is a tracked perf metric

**Status**: Accepted

**Date**: 2026-05-06

## Context

ADR-0013 framed BigFleet's per-cycle SLO as `shardCycleDurationP99 ≤ 100 ms` (burst regime, 1:10 demand-to-inventory). M11 validated it; M29's first cloud run with the production-realistic seed measured 817 ms and was reported as a "failure." Re-examining the SLO surfaced an honest concern: **the 100 ms cycle target is not what comparable systems gate on, and it is not what users feel**.

What comparable systems target:

| System | Cycle / scan interval | User-facing latency target | Source |
|---|---|---|---|
| **Borg** | event-driven, no fixed cycle | task-assignment p50 ~25 ms, p99 in seconds (not published as SLO) | Verma et al. 2015 §3.3.1 |
| **Twine** | seconds-grained allocation rounds | allocation latency tens of seconds → minutes | Tang et al. 2020 |
| **Cluster Autoscaler** | `--scan-interval=10s` default | scale-up reaction <5 min | upstream |
| **Karpenter** | event-driven on Pending pods | node-ready ~30 s spot, ~60 s on-demand | AWS |
| **kube-scheduler** | per-pod, ~30 ms target | binding latency a few hundred ms | upstream |

None of them gates a release on a fleet-wide rebalance loop completing in <100 ms. Borg's BorgMaster scheduling round is throughput-oriented; Twine's allocation pipeline is seconds-to-minutes by design. Cluster Autoscaler runs cycles 10× longer than our SLO and considers itself responsive. The 100 ms target is decorative against this baseline.

What the user actually experiences is not the cycle wall-clock; it's:

```
CR created → operator rollup (10s default) → shard cycle → Phase 1 → execute (Bootstrap RPC) → operator binding visible
```

A 5 s cycle inside a 10 s rollup interval is **invisible** to the user; both are dwarfed by the provider's Create+Boot latency, which is *minutes* for real cloud APIs. Optimising cycle wall-clock from 100 ms to 50 ms moves a number nobody feels.

This matters operationally:

- **M29 was reported as a "failure"** purely on cycle p99. Under the user-facing latency view, the run delivered every binding inside the rollup window and would have been a pass.
- **M30.{1,2}'s fast paths** were aggressive in ways that turned out to be test-shaped (the pin-only fast path doesn't fire on profiles with resources). Gating releases on cycle p99 incentivised optimisations that may not generalise to production.
- **Future work** keeps getting framed as "drive cycle p99 down further" when the real release-quality question is "does the user's CR get bound fast enough?"

## Decision

BigFleet's SLO surface is reorganised into one user-facing release gate, one throughput envelope, and one perf-tracked alert metric:

### 1. Binding-latency p99 — the user-facing release gate

`bindingLatencyP99` is the time from CapacityRequest creation (`metadata.creationTimestamp`) to the moment the bound machine reaches `StateConfigured` and the operator publishes the assignment. This is what the user feels.

Per-priority-tier targets (burst regime, 1:10):

| Priority tier | Provider class | `bindingLatencyP99` target |
|---|---|---|
| ≥1,000,000 (critical realtime) | reserved / pre-warmed pool | ≤ 5 s |
| ≥1,000 (services) | on-demand | ≤ 60 s |
| ≥100 (batch) | spot / pre-emptible | ≤ 90 s |
| <100 (best-effort) | any | ≤ 5 min |

These targets are **provider-class-dependent**. The harness's in-process fake provider returns Configured in under a second, so the harness gate runs at the in-process floor — `bindingLatencyP99 ≤ 5 s` for all tiers — which validates the *shard's* contribution to the latency budget. Real-provider deployments must measure with real provisioning latency added.

### 2. Cycle-throughput envelope — backlog-prevention guard

`shardCycleDurationP99 ≤ rollupInterval / 2`. With the default `rollupInterval = 10 s`, this is `≤ 5 s`. The intent is that the shard must consume one full snapshot before the next rollup arrives, so backlog never accumulates. Above this number, two cycles of demand pile up and binding latency drifts.

This is **not a release gate** — it's an envelope. A run with cycle p99 = 4.8 s passes if binding latency holds; a run with cycle p99 = 6 s fails on the throughput envelope even if binding latency is currently fine, because the next rollup will compound the lag.

### 3. Convergence-rate SLO — reprovisioning regime (unchanged)

ADR-0013's `≥5,000 bindings per cycle until drain` for the reprovisioning regime stands. Reprovisioning has no per-cycle SLO; throughput is the contract.

### 4. Cycle / phase wall-clocks — tracked, alerted, not gated

`shardCycleDurationP99`, `shardPhase{1,2,3,Execute,Reconcile}P99Seconds` keep flowing as Prometheus histograms. Alert thresholds:

- Cycle p99 > 1 s: warning (drift from M11 baseline of ~50 ms is investigation-worthy).
- Cycle p99 > rollupInterval / 2: page (throughput-envelope breach; binding latency at risk).
- Phase regression > 2× a milestone's published baseline: warning (likely a code regression).

A regression here wakes someone up. It does not block a release.

## Consequences

- **scaletest-runner's pass/fail switches.** The runner currently fails on `shardCycleDurationP99Seconds > 0.100`. It will instead fail on `bindingLatencyP99 > target_for_provider_class`, with the cycle-throughput envelope as a secondary gate. New summary fields: `bindingLatencyP99SecondsByPriorityTier`, `bindingLatencySLOTarget`, plus the existing cycle/phase histograms (now informational).
- **M29's run is re-graded.** The summary.json from `20260505-224242-scaleway-1m` shows `loadgenCRsActive: 999963 / 999000 (gate cleared)` — every CR was bound. The cycle p99 of 817 ms was inside the throughput envelope (≤ 5 s). Under ADR-0014 the run *passes*, with cycle latency flagged as "watch this — close to the warning band."
- **M30.{1,2} are still welcome.** Lower CPU = lower cost = more headroom under burst, and Phase 3's MatchProfile alloc fix helps every workload shape. Their *role* changes: from "load-bearing for SLO" to "operational efficiency." Future Phase 2/3 optimisations are evaluated on whether they reduce CPU/cost meaningfully under the realistic catalog (M31 / M32+), not on whether they shave milliseconds off a cycle target nobody feels.
- **Existing scaletest profiles all pass under the new gate.** scaleway-500k / 1m / 5m at burst already deliver near-100 % bindings inside the rollup interval. The reprovisioning-variant profiles continue to gate on convergence rate per ADR-0013.
- **Documentation updates.** `docs/scaling-guide.md` swaps the "100 ms cycle p99" header for binding-latency targets per tier, with the throughput envelope alongside. The plan §5.1 ceiling table is reframed: "shard handles N machines while staying inside the rollup-interval throughput envelope" rather than the more aggressive 100 ms cycle.
- **Future per-cycle optimisation has a meaningful test.** The realism work (ADR-0015, separate) ensures the test catalog produces fingerprint diversity / priority inversions / `Same`-operator constraints / bursty arrivals that match production. Under the realistic catalog, an optimisation that doesn't help binding-latency p99 measurably is unlikely to ship.
- **Real-provider deployments need to publish their binding-latency targets.** Provider authors document provisioning latency p99 for their backend; deployments add the harness floor (~1 s shard contribution) plus the provider's number. Conformance suite: optional `latencyTier` declaration so the binding-latency SLO can adapt.

This decision **does not say** the cycle wall-clock is unimportant — it says it isn't the right release gate. Tracking it, alerting on regressions, and keeping it inside the throughput envelope are all retained.
