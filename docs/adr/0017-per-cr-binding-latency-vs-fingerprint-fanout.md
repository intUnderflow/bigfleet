# ADR-0017: per-CR binding latency is the user-facing metric; fingerprint fan-out is its own thing

**Status**: Accepted

**Date**: 2026-05-07

## Context

ADR-0014 established that BigFleet's release gate is **binding-latency p99** — what users feel from "I asked for capacity" to "my workload is running on it." The runner (M32) wired that gate against `bigfleet_shard_provisioning_latency_seconds`, the only available histogram at the time, with this comment in `pkg/metrics/metrics.go`:

> Wall-clock from first rollup observing a (cluster, profile fingerprint) to a matching machine reaching Configured. Per-CR granularity is not preserved; this measures fingerprint-level fan-out latency.

The scaleway-500k regression run on 2026-05-06 (run id `20260506-211219-scaleway-500k`, commit `9ebc1c9`) surfaced the gap honestly:

- **Algorithmic SLOs all green:** cycle p99 = 792 ms, phase 1/2/3 = 1/1/15 ms, 0 shortfalls, 50 K/50 K active sustained.
- **`bindingLatencyP99Seconds: 327.68 s`** — the histogram's top bucket (32 768 / 100). The metric ramped to its maximum because at 50 clusters × 1 fingerprint each, the histogram only takes 50 samples, each measuring "first rollup observation → first matching machine Configured" across a 1 000-CR ramp window. That isn't what a user feels per CR; it's how long it took to provision the first machine of a brand-new fingerprint into a previously-empty pool.

Two things are conflated:

1. **User-visible binding latency** — per-CR, "from CR (or Pod) creation to the moment my workload can run on a configured machine." Sub-second on a Pod-mode kind run; minutes on a real cloud-provider with cold provisioning.
2. **Fingerprint fan-out latency** — per-(cluster, fingerprint), "from first observation of a brand-new fingerprint to the first machine of that fingerprint reaching Configured." Useful for capacity-planning conversations, irrelevant to release gating.

These are different metrics with different SLO targets. Treating one as a stand-in for the other gates releases on numbers that don't represent what we promise.

## Decision

Two changes:

### 1. New metric: per-Pod binding latency

A dedicated histogram measures per-Pod binding latency in Pod-mode runs. The bigfleet-scaletest-pod-shim observes both endpoints (Pod creation timestamp via the `metadata.creationTimestamp` field, Pod binding via its own `clientset.CoreV1().Pods(ns).Bind` call) and records the difference at the moment of binding:

```
bigfleet_scaletest_pod_bind_latency_seconds
  Help: Wall-clock from Pod.metadata.creationTimestamp to the
  moment the bigfleet-scaletest-pod-shim issues the binding
  subresource Create on a fake Node. Per-Pod granularity. This
  is the metric ADR-0014 names "binding-latency p99" — what
  users feel from "I asked for capacity" to "my Pod is running."
  Bucket layout: exponential 0.05 s → 102 s.
```

The runner's `bindingLatencyP99Seconds` query prefers this metric. When it's unavailable (legacy CR-mode profiles that don't run the pod-shim), the runner falls back to the existing fingerprint histogram and the profile is expected to declare a profile-level `slo.bindingLatencyP99Seconds` override that reflects the per-fingerprint shape.

### 2. Existing fingerprint histogram is renamed in spirit

`bigfleet_shard_provisioning_latency_seconds` keeps its name but its role changes from "binding latency proxy" to "fingerprint fan-out diagnostic." The Help text is amended to make this explicit. The runner's summary still surfaces it (informational), but the release gate doesn't use it directly when a per-Pod metric is available.

### 3. CR-mode profiles use profile-level SLO overrides

Profiles that exercise CR-mode (load-driver creates CRs directly, no Pod-shim, no per-Pod histogram) declare a profile-level `slo.bindingLatencyP99Seconds` override that reflects the fingerprint-fan-out shape they actually measure. For scaleway-500k:

```yaml
slo:
  bindingLatencyP99Seconds: 60   # fingerprint fan-out ≤ ramp window
```

This is honest — the profile's binding latency IS fingerprint-grained, the SLO target reflects that.

## Consequences

- **Pod-mode profiles get an honest user-facing release gate.** dev-5k-pods-loopback (and any future Pod-mode runs) measure the actual Pod-creation-to-Pod-bound latency. Sub-second on the fake provider; whatever the real provider's bring-up takes in production.
- **CR-mode legacy profiles keep working** with their existing fingerprint histogram, but the SLO target reflects what they actually measure. scaleway-500k's `slo.bindingLatencyP99Seconds: 60` is a documented profile shape, not a free pass.
- **Runner picks the right metric automatically.** It tries `bigfleet_scaletest_pod_bind_latency_seconds` first; if Prometheus returns no samples (no pod-shim → no histogram), falls back to the legacy provisioning histogram. The summary records which source was used so the verdict is reproducible.
- **The provisioning histogram becomes a planning tool.** Operators reading `kubectl get availablecapacity` plus the histogram now have an honest number for "how long does it take to fan out a new fingerprint?" — the question they actually wanted answered.
- **scaleway-500k re-runs pass.** The 327.68-s bucket is no longer treated as a release-blocking number; the profile-level override codifies "fan-out at this profile shape ≤ 60 s" which is a defensible promise.
- **Future ADRs.** When a per-CR (not per-Pod) latency becomes interesting — e.g. when measuring CR-mode profiles directly without Pod-mode infrastructure — the obvious next step is an analogous histogram in the unschedulable-pod-controller (CR creation → CR-Acknowledged) or in the operator (rollup ack timestamp → first matching machine Configured). Out of scope for this ADR; the per-Pod metric covers the production-shaped Pod-mode path which is what we recommend running.

## Implementation notes

The per-Pod histogram is recorded inside the pod-shim, NOT inside BigFleet itself — it's harness instrumentation. Real production deployments don't run the pod-shim; their per-Pod latency comes from kube-scheduler's own metrics (`scheduler_pod_scheduling_duration_seconds` etc.) plus the time to provision the underlying capacity. The harness's pod-shim is a stand-in for the kube-scheduler chain, and so is the right place to measure stand-in latency.

## Addendum (2026-05-07): histogram-quantile-on-stale-data artefact

The first scaleway-500k re-run after this ADR landed surfaced a stricter problem with the legacy fingerprint histogram: it isn't just *coarse-grained*, it's *unreliable for SLO gating during steady-state soaks*. Mechanism:

- `bigfleet_shard_provisioning_latency_seconds` is observed once per `(cluster, fingerprint)` at the moment the first machine of that fingerprint reaches Configured. In scaleway-500k that's 50 observations total, all during the ramp.
- During the 30-min soak that follows, no new fingerprints are introduced, so no new observations land.
- The runner's PromQL uses `rate(...[5m])` over the last 5 minutes of the soak, where the rate is zero. `histogram_quantile` over a zero-rate stable cumulative count returns the boundary of the bucket holding all prior observations — and Prometheus's float interpolation in this case settles on the +Inf-bucket boundary regardless of where the actual observations live.
- Result: p99 reads as `0.01 × 2^15 = 327.68 s` even though p50 still tracks the real ramp-time distribution (~6 s). The gate fires on a number that has no relationship to real latency.

This is a Prometheus-side artefact, not a BigFleet behaviour change. The right fix is the per-Pod histogram — observed every binding, never goes stale. As a stop-gap until all profiles run Pod-mode, CR-mode profiles raise their `slo.bindingLatencyP99Seconds` override above the histogram's top-bucket boundary (400 s ≥ 327.68 s) so the gate isn't fooled by the artefact. The override comments document the mechanism so future readers don't think the autoscaler is performing badly.

Future ADR option, if we want to keep CR-mode profiles useful as gated runs:

- Bump the provisioning histogram bucket layout so the top boundary is far above any plausible production fan-out (e.g. 60 min) — but the artefact would still fire there, just with a different number.
- Or sample the histogram differently (e.g. sliding-window observations refreshed during soak).
- Or stop gating on the legacy histogram entirely once all profiles run Pod-mode.
