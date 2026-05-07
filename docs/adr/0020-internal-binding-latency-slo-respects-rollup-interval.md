# ADR-0020: Internal binding-latency SLO must respect the rollup interval

**Status**: Accepted

**Date**: 2026-05-08

## Context

The scaleway-50k Pod-mode cloud run (M44 default; 50 kwok clusters × 1K Pods, 30-min soak) failed `pass()` with `internalBindingLatencyP99Seconds 50.864 s > 5 s SLO`. Two rounds of optimisation followed:

- **Drop A** (`5bbae86` … `7668f50`): fresh `inv.Snapshot()` per cycle, `needs.Table.Snapshot` index-sort, pod-shim binder `MaxConcurrentReconciles 16 → 64`, operator/pod-shim/upc QPS plumbed from one knob.
- These collapsed shard cycle p99 from 8.192 s to 0.294 s and demand cleared at 49996/49950 active. **Internal binding latency stayed at 50.86 s.**

The remaining latency is dominated by a structural floor the harness baked in but the SLO never accounted for: the operator's **`rollupInterval`** defaults to **10 s** (`pkg/operator/operator.go:131`). A Pod that arrives just after a rollup tick waits up to one full interval before the shard sees its CR, so the 10 s rollup interval is a hard p99 floor on internal binding latency.

Stage-by-stage budget (post-fix, scaleway-50k shape):

| Stage | p99 contribution | Notes |
|---|---|---|
| Pod create → Unschedulable patch (pod-shim) | ~50 ms | trivial under any QPS |
| Unschedulable → CR (unschedulable-pod-controller) | ~50 ms | trivial |
| **CR → next rollup tick** | **up to 10 s** | `rollupInterval` default |
| Rollup processing (operator) | 60 ms | measured |
| Shard cycle (Phase 1 → Bootstrap) | 0.3 s | post-Drop-A |
| NodeStateUpdate → UpcomingNode | ~50 ms | apiserver write |
| Binder burst-drain tail (post-fix) | 4–5 s | M44.4 binder fix |
| Bind subresource | ~50 ms | trivial |
| **Sum** | **~15 s** | |

A 5 s SLO is therefore unachievable for *any* profile that runs the operator with the production-default 10 s rollup interval, regardless of how fast the rest of the chain becomes. The choice is binary: lower `rollupInterval` per-profile (treating it as a tunable rather than a production posture), or raise the SLO to acknowledge it as a floor.

## Decision

**The internal-binding-latency SLO is set to 15 s.** The 10 s rollup interval is treated as a deliberate production posture, and the SLO is sized as `rollupInterval + 5 s` of chain-execution headroom.

Specifically:

- The runner's default `internalBindingLatencyP99Seconds` becomes **15 s** (was 5 s).
- The four cloud profiles (`scaleway-{50k,500k,1m,5m}`) carry an explicit `internalBindingLatencyP99Seconds: 15` override rather than falling through to the default. Explicit is more self-documenting — anyone reading the profile sees the active SLO without having to know the default.
- A profile that lowers `rollupInterval` may also lower the SLO override accordingly. The relationship is documented as a guideline: `internalBindingLatencyP99 ≤ rollupInterval + 5 s` is the recommended ceiling at the cluster sizes this harness covers.
- ADR-0014's tiered targets (5 s / 60 s / 300 s for critical / services / batch) describe the **user-facing** ceiling — the floor a deployer can promise to their workloads. ADR-0018 already established that the harness's `internalBindingLatency` is *internal-only*, contributing to but not equal to the user-facing number. This ADR closes the loop: the harness's internal SLO must respect the harness's own structural floor.

## Why not lower `rollupInterval` instead?

Lowering `rollupInterval` is the obvious alternative. We considered it and chose the SLO change because:

1. **10 s is what production should default to.** The rollup carries the cluster's complete desired state to the shard; it's a fixed-cost operation independent of churn. Firing it every 10 s instead of every 1 s gives 10× fewer messages on the operator → shard stream and 10× less aggregation work in the operator without meaningfully degrading user-facing latency (real-provider capacity-create time dwarfs 10 s rollup batching). The CLAUDE.md `## Hard rules` enumerate the operator-side defaults audit (M39); 10 s rollup is one of those audited defaults.
2. **Sub-second rollups would mask regressions elsewhere.** If we lowered rollup to 1 s the SLO could land at 5 s and the test would pass — but the harness would no longer exercise the production rollup-batching shape. Future regressions in chain stages other than rollup wouldn't surface until they exceeded ~5 s individually rather than being visible against a 15 s envelope.
3. **The SLO is an internal regression detector**, not a user promise (ADR-0018). A 15 s ceiling is more than tight enough to catch the kinds of regressions this harness was built to catch (Drop A's 50 s blowup, Drop B's worker-pool saturation, M11.x-class pathologies). User-facing latency lives in provider-conformance + production canaries (ADR-0018 §Real-provider validation).

## What this ADR is *not*

- It does not raise ADR-0014's user-facing tiers. Critical workloads still need ~5 s end-to-end binding for users to feel the system as responsive — but that 5 s budget is split across `internal + provider`, with provider eating most of it for cold-starts and minimal for warm-pool attaches.
- It does not lower `rollupInterval` per profile. If a future profile's posture warrants sub-10 s rollup, that's a separate decision recorded in that profile's preamble.
- It does not weaken the regression-detector function of `pass()`. A 15 s ceiling still catches every Drop A / Drop B class issue we've encountered; what it *doesn't* catch (sub-15 s drift) was already invisible to the shard-cycle envelope.

## Consequences

- `pass()` default: `internalBindingLatencyP99Seconds: 5 → 15`.
- Profile overrides: `dev-5k.yaml` keeps its `10` (kind-local; in-process; should comfortably stay under). The four cloud profiles (`scaleway-{50k,500k,1m,5m}`) update their explicit `5` to `15`.
- ADR-0018 rendering of "harness internal-only" gains a concrete floor: 15 s = ~10 s rollup + ~5 s chain. Linked from ADR-0014.
- The runner's `pass()` comment block updates the bullet for `internalBindingLatencyP99 ≤ 5 s` to `≤ 15 s`, with the rollup-interval rationale.
- The Grafana dashboard's "Internal binding latency p99" stat panel threshold mapping updates from 5 to 15.

## Validation

The next scaleway-50k cloud run (post-Drop-A, post-binder-fix) is expected to land at 10–18 s p99 if all stages behave as modelled. Anything outside that band — significantly under, or over — is a signal worth investigating: under means the harness isn't exercising the rollup ceiling (`rollupInterval` was inadvertently shorter than 10 s), over means a downstream stage other than rollup is contributing more than its 5 s budget.
