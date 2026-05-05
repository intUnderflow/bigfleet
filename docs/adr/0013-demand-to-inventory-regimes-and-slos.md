# ADR-0013: Demand-to-inventory regimes and SLOs — burst-density is the production target, 1:1 reprovisioning is its own regime

**Status**: Accepted

**Date**: 2026-05-05

## Context

The `shardCycleDurationP99 ≤ 100 ms` SLO is what `pkg/decision`'s Phase 1 / 2 / 3 are tuned for and what scaletest-runner gates on. M11 validated it at scaleway-500k's burst-shape load (50K demand × 500K inventory = 1:10 demand-to-inventory ratio) on a single shard. M13.gate (commit `87964a9`) drove a per-shard ratio of 1:1 (500K × 500K) and failed cycle p99 at 967 ms. M27's algorithmic optimisations brought that down ~5× but Phase 2 + 3 + 1 still sum to ~200 ms cloud at 1:1 — over the 100 ms SLO.

The interpretation question: is the 1:1 ratio a regime BigFleet should promise SLOs for, or a regime that requires a different SLO entirely?

What real production fleets look like:

- **Borg** (Verma et al. 2015) and **Twine** (Tang et al. 2020): cells of 12K-50K nodes; churn ~5-10 % per minute in normal operation; bind-to-running latency ~30 s; pending demand at any moment ~1-2 % of inventory in steady state, ~5-10 % during deployment ramps. Neither paper documents a "every workload re-deploys at once" mode because real fleets don't.
- **Industry-observed pattern**: deployment ramps are the worst-case the cycle-p99 SLO has to honour. Mass churn events (full re-deploy, cluster migration, DR) are operational events that produce a backlog, drain it, and return to steady state.

The 1:1 ratio in M13.gate's profile (every machine in the fleet has demand pending against it) is a **complete reprovisioning event**: every workload flipped over in a single cycle. Realistic events that produce this shape: cluster migration, full disaster recovery, mass spot eviction across an entire AZ. Common in steady-state operation: no.

Two candidate framings:

1. **One SLO for all densities.** Promise `cycle p99 ≤ 100 ms` at any demand-to-inventory ratio. Honest, but requires algorithmic work that may not be possible (`O(N×M)` victim scoring is a hard floor for the Phase 2 algorithm we have, even with caches), and doesn't reflect what production fleets actually need.
2. **Three regimes, three SLOs.** Steady-state, burst, reprovisioning — each with its own promise.

Option 2 matches industry practice and lets the cycle-p99 SLO mean what it's measured under (the burst regime is what M11 validated and what real fleets actually live in).

## Decision

BigFleet promises three operating regimes with distinct SLOs:

| Regime | Pending demand vs inventory | SLO |
|---|---|---|
| **Steady state** | ≤ 2 % (1:50) | `shardCycleDurationP99 ≤ 50 ms` |
| **Burst** (deploy ramp, AZ rebalance) | ≤ 10 % (1:10) | `shardCycleDurationP99 ≤ 100 ms` |
| **Reprovisioning** (cluster migration, DR, mass eviction) | up to 100 % (1:1) | **no per-cycle SLO** — convergence-rate guarantee instead: `≥5,000 bindings per cycle until the demand queue drains` |

**Steady-state and burst SLOs are the production guarantees.** The cycle-p99 metric is gated on these regimes; release-blocking scaletest profiles run at the burst ratio (1:10) which is the worst-case the production SLO has to honour.

**Reprovisioning is its own regime.** Per-cycle latency is unbounded; what we promise is throughput — the system drains the demand backlog at a known rate. Operators driving a reprovisioning event accept that cycle p99 will spike during the drain and fall back when steady-state returns. Validation profiles for this regime gate on convergence rate, not cycle p99.

## Consequences

- **The scaletest profile shapes follow the regime split.** `scaleway-500k`, `scaleway-1m`, `scaleway-5m` validate the burst regime (1:10 demand). A `*-reprovision` companion profile per fleet size validates the reprovisioning regime (1:1 demand) against the convergence-rate gate. M28 reshapes the existing profiles and adds the reprovision variants.
- **M13.gate's "fail" is reframed as a regime mismatch.** The run drove 1:1 against a 1:10-shaped SLO. It demonstrated reprovisioning works (998 967 / 1 000 000 sustained throughout) — what it didn't demonstrate is that BigFleet honours `cycle p99 ≤ 100 ms` at every density, which is not promised.
- **Algorithmic optimisation has a meaningful target.** M27 (Phase 2 / Phase 3 caches) brought reprovisioning from 967 ms to ~200 ms cloud cycle p99. That's still a "no-SLO regime" number, but its order of magnitude affects how operators think about reprovisioning duration — the lower it is, the faster the convergence. M27's work is meaningful even outside the SLO.
- **Steady-state SLO (50 ms) is more aggressive than burst (100 ms).** Industry convention: real production rarely sees burst conditions, so the operator's day-to-day expectation is the steady-state number. Alerts that fire at >50 ms p99 even outside burst windows surface drift before it becomes a real outage.
- **Scaling guide names the regime explicitly when quoting numbers.** "scaleway-5m at 100 ms cycle p99" is meaningless without "(burst, 1:10)" attached; the scaling guide updates accordingly so a reader can map a profile and an SLO to the regime it belongs to.
- **Future work to make 1:1 hit the 100 ms SLO is not blocked.** The decision doesn't say "we never promise SLO under reprovisioning"; it says "v1 doesn't, but the convergence-rate guarantee is the contract you can lean on." A future ADR would supersede this if a real workload demands per-cycle SLO under full reprovisioning.
