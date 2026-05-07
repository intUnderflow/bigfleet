# ADR-0019: Phase 1 cloud-vs-bench discrepancy — instrument before optimising

**Status**: Accepted

**Date**: 2026-05-07

## Context

The scaleway-50k Pod-mode cloud run (M44 default; 50 kwok clusters × 1K Pods, 60K Idle seed across 6 realistic archetypes, 2× PRO2-L Kapsule, 30-min soak) failed pass() with `shardCycleDurationP99Seconds 8.192s > 5.0s throughput envelope`. The original interpretation was "Phase 1 needs optimisation for archetype-diverse Idle pools at scale." A local benchmark mirroring the same workload shape on M5 Max measures Phase 1 at **22 ms/op**, six thousand times faster than the cloud number.

Six thousand-times slower on cloud than on a developer laptop is not a "PRO2-L is slower than M5 Max" gap. The investigation surfaced three orthogonal findings the original interpretation missed.

### Finding 1: Phase 1 timing is bimodal — 99 % short, 1 % multi-hour

`bigfleet_shard_cycle_phase_duration_seconds{phase="phase1"}` from the prom snapshot (`test/scaletest/results/20260507-120335-scaleway-50k`):

```
mean (sum/count over 5 m)     = 134.4 s
p99 (histogram bucket-quantile) = 8.192 s   (top finite bucket; 99% of mass below)
top bucket count (≤8.192 s)   = 0
+Inf bucket count             = ~2 over 5 m
```

Reading: ~99 % of Phase 1 calls finish in 4–8 s (still over the 5 s envelope, but not catastrophic). The handful in the +Inf bucket each take long enough that, divided into 5 min, each one accounts for >100 s of accumulated time — which means individual stalls of multiple hours. This also explains M44.3's 7 h 50 min runner hang on `scaleway-500k` (same hot path; runner blocked on `snapshotPrometheus` after the apparent stall).

The bench can't reproduce the tail because the bench has no contention with anything: no concurrent operator stream traffic, no fake provider Apply path, no inventory fold goroutine, no GC pressure from anything but the test itself.

### Finding 2: Bootstrap actions emit but don't land

```
shard_actions_total{kind="Bootstrap"} cumulative = 16 202   (over 30 min)
shard_actions_deferred_total cumulative          = 689 248  (45× the emit count)
inventory_machines{state="Configured"} (final)   = 1 019    (~ one cycle's worth)
operator_acknowledge_duration_seconds_count      = 6 241
```

Phase 1 emits ~50 K Bootstraps each cycle (one per Need × deficit). `maxActionsPerCycle: 1000` caps execute; the other 49 K defer and re-emit next cycle. Despite 16 K Bootstraps actually entering execute, only 1 019 machines reach Configured. Operator acks are happening (~6 K) but most of those acks correspond to rollups, not Bootstrap responses — `bigfleet_operator_acknowledge_total{}` (the counter, not the histogram) is empty (label issue, separate followup), so we can't split rollup-ack vs action-ack from this snapshot. Inventory shows zero machines in transitional states — every machine is Idle, Configured, or Failed. That means Bootstraps either complete (Idle → Configured) or are silently dropped; nothing sits half-way.

The hypothesis is: high re-defer rate × concurrent action emit × stale snapshot view causes Phase 1 to keep emitting Bootstraps for the same Idle machines over many cycles. A few execute, the rest are deferred. The shard never converges. Phase 1 timing dominates because it emits 50 K actions even though only 1 K can ever land per cycle.

### Finding 3: 611 Failed machines were Bootstrap timeouts, not the M38 injector

The first read of the data was that M38 over-fired 380×. Tracing the code paths to StateFailed showed five distinct sources, only one of which is the M38 injector. The other four live in `pkg/shard/execute.go` (Provision/Bootstrap timeout, drain failure). With `BootstrapTimeout` defaulting to 30 s and 1000 actions/cycle racing through the operator stream against a kine-sqlite-backed kwok apiserver under heavy concurrent write load, **30-second timeouts firing on the bootstrap-blob fetch is the most likely 611-Failed cause** — the operator can't return the bootstrap blob fast enough under contention.

Subsequent investigation of the M38 injector confirmed the implementation was nonetheless broken — but in the opposite direction:

```go
// pre-ADR-0019 implementation
sampleN := 32
seen := make(map[string]struct{}, sampleN)
for i := 0; i < sampleN; i++ {
    id := prov.RandomConfiguredMachine()
    seen[string(id)] = struct{}{}
}
perCallProb := ratePerSec
for id := range seen {
    if rng.Float64() < perCallProb { ... }
}
```

Per tick, expected failures ≈ 32 × ratePerSec, capped at 32 regardless of fleet size. At 1.16e-8/sec/machine that's 6.7e-4 expected over a 30-min run — the injector would never fire, regardless of fleet size. The intended semantics ("per-second per-machine probability") would have given 1.04 expected per 30-min soak at 50 K Configured. Off by ~1500×, in the *under*-firing direction.

ADR-0019 rewrites the injector to use `prov.ConfiguredCount() × ratePerSec` as the Poisson mean per tick. The default chart `failureRatePerSec` is restored to a value (`1.16e-7`, the upper "1%/day" end of the realism band) that produces ~10 induced failures per 30-min soak at 50 K Configured — enough to exercise the recovery path, low enough not to dominate.

The 611 number is unexplained by M38 (with the bug, the injector fires <1 time over the run; with the fix, ~10 times). The remaining 600 Failed machines are an execute-path concern, listed below.

## Decision

**Add instrumentation before changing any decision-engine code.**

The cloud and bench numbers disagree by 6 000×. Optimising the algorithm based on bench numbers would optimise the wrong code. We don't have line-of-sight into where Phase 1's wall-clock actually goes in cloud. Until we do, "fix Phase 1" is guessing.

### Instrumentation plan

Three new histograms, all using the existing `prometheus.ExponentialBuckets(0.001, 2, 14)` shape:

```
bigfleet_shard_phase1_pool_build_duration_seconds  histogram
  Help: poolFor() — buildPoolSource cost for each (state, fingerprint).
        Per-call (so each unique fingerprint visited per cycle is one
        observation). Tracks how much of Phase 1's wall-clock is the
        merge+sort vs the reuse-pre-sorted path.

bigfleet_shard_phase1_take_duration_seconds        histogram
  Help: take() — the head-cursor walk + lazy MatchProfile + claimed
        check. One observation per Need that called take().

bigfleet_shard_phase1_takecolocated_duration_seconds  histogram
  Help: takeCoLocated() — the whole-pool bucket-by-Same-key walk.
        One observation per sameRack Need that called it. Same-rack
        is the suspected hot path because it doesn't honour the head
        cursor.
```

Plus one counter:

```
bigfleet_shard_phase1_calls_total                  counter labelled by sub-path
  {path="take"|"takeCoLocated"|"takeSpread"}
  Help: How many times each sub-path fired, so we can divide the
        sum by the count and see mean cost per sub-path.
```

Decision/match.go's `MatchProfile` is hot enough to warrant its own histogram if the above don't localise the issue, but adding it everywhere would generate 50 K observations per cycle — too high a cardinality cost for the marginal signal. Defer to a second instrumentation pass.

### Separate followup: Bootstrap landing rate

The Configured-stuck-at-1019 finding is an *execute-path* concern, not a Phase 1 concern. Tracking it via:

- Add `result` label to `bigfleet_shard_actions_total` (`{kind, result}` where result ∈ `{success, deferred, error, timeout}`).
- Add a `reason` label to `bigfleet_shard_actions_deferred_total` (the existing counter has a `reason` label that is rendered as `null` in the prom snapshot — fix the label assignment so it actually carries the defer reason).
- Investigate whether per-cluster operator stream RTT is the bottleneck on action ack: at 1000 actions/cycle × 50 clusters × 5 ms execute, executing through 128-concurrent workers, the bound is 5 ms × 1000/128 ≈ 40 ms — but reality says only 1019 land across 30 min, so something blocks the operator return path.

### Separate followup: Bootstrap timeout rate

Most of the 611 Failed machines came through `pkg/shard/execute.go`'s timeout path (BootstrapTimeout default 30 s). At 1000 actions/cycle × 50 clusters × 128 concurrent shard executors, the operator's bootstrap-blob fetch is the long pole; if kwok's kine-sqlite is contended under combined CR + Pod + Node write load, blob fetches stretch past 30 s. Worth tracking with a histogram on the bootstrap-blob fetch wall-clock and a counter on timeout outcomes — same instrumentation pattern as Phase 1's sub-paths.

## Consequences

- **No code change to `pkg/decision/` until instrumentation re-runs surface the actual hot path.** Resist the urge to optimise on bench results that don't reflect cloud reality.
- **A second cloud run is required** after instrumentation lands (likely scaleway-50k again, $0.60/run). The instrumentation itself is a regression artifact — we should commit it permanently rather than as a debug branch, because cloud-vs-bench discrepancies will recur and the histograms are useful generally.
- **The 5 s cycle envelope is honest**: real production fleets at this scale need this resolved, not papered over with a profile-level SLO override.
- **M38 failure-injector reimplemented + default raised**: the previous 32-sample heuristic couldn't fire at the realistic per-machine rates regardless of fleet size. New implementation uses `prov.ConfiguredCount() × ratePerSec` as the Poisson mean per tick. Chart default is now `1.16e-7` (1%/day, the upper realism bound), giving ~10 induced failures per 30-min soak at 50 K Configured.
- **Bench stays committed** (`pkg/decision/phase1_realistic_bench_test.go`) as a regression detector — it didn't reproduce the cloud bottleneck, but it's the right tool for catching algorithmic regressions in Phase 1 at the realistic shape, separate from the cloud-only path issues this ADR investigates.

## Open questions to revisit after the instrumented run

1. Is the multi-hour Phase 1 stall a Phase 1 issue or a wider stall (snapshot fold blocked, lock starvation)? The new histograms attribute time to `pool_build` / `take` / `takeCoLocated`. If none of them dominate, the stall is outside Phase 1's algorithmic path, and the next instrumentation pass goes wider (snapshot.CycleSnapshot, NeedsTable.Snapshot, lock acquisition).
2. Does setting `maxActionsPerCycle: 0` (uncapped) change the picture? With 50 K demand and async execute through 128-concurrent workers, the uncapped path might converge faster in steady state at the cost of a wider cycle-tail spike. Worth measuring once the timing question is answered.
3. Is `takeCoLocated`'s whole-pool walk a per-Need or per-cycle cost? The current code does one walk per Need that has a Same requirement; with 7 K such Needs (gpu-training + memory-db) at 50 K total demand, the same pool gets bucketed 7 K times per cycle. Cache-the-bucketing optimisation is obvious if the histogram confirms the cost is here.
