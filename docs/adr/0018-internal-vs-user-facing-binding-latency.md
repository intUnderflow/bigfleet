# ADR-0018: "binding latency" in the scaletest harness is internal-only; the user-facing number lives elsewhere

**Status**: Accepted

**Date**: 2026-05-07

## Context

ADR-0014 framed the release gate as **binding-latency p99** — "what users feel from 'I asked for capacity' to 'my workload is running'." That's the right SLO target conceptually. The implementation, however, doesn't measure that.

The scaletest harness ships an in-process fake provider (`pkg/provider/fake`). `Create`, `Configure`, `Drain`, and `Delete` are in-memory state transitions that return in ~microseconds. The harness's `bigfleet_scaletest_pod_bind_latency_seconds` histogram measures `Pod.creationTimestamp → Pod.bind` end-to-end, but the provider component of that wall-clock is **zero**. What's left is BigFleet-internal latency: rollup → Phase 1 → ack → UpcomingNode → Pod-bind reconcile.

In production, the same metric on a real provider would include real capacity-create time:

| Provider class | Capacity-create p99 (representative) |
|---|---|
| Pre-warmed pool / fast attach | 5–15 s |
| Cloud spot, hot AMI | 30–60 s |
| Cloud on-demand, cold image | 60–180 s |
| New instance-type / region | 180–600 s |

So the harness's `bindingLatencyP99 ≤ 5 s` ceiling is achievable only because the provider returns instantly. Real users with a real cloud provider see something between 50 ms and 600 s of additional latency that we don't measure. Calling our 5 s gate "what users feel" overstates what it tells us.

## Decision

Three changes:

### 1. Rename the metric and SLO to make scope explicit

- The runner's profile-level SLO key: `bindingLatencyP99Seconds` → `internalBindingLatencyP99Seconds`.
- The runner's pass-fail failure message includes the qualifier "internal" and a forward-pointer to this ADR.
- The histogram name (`bigfleet_scaletest_pod_bind_latency_seconds`) stays — the `_scaletest_` prefix already telegraphs that it's harness-internal — but the help text is amended to point at this ADR.

The metric measures BigFleet's contribution to the user-facing latency. A regression in this number is a bug; meeting it is necessary, not sufficient.

### 2. Tier targets are framed as "internal-only" floors

ADR-0014 published a priority-tier table (5 s / 60 s / 300 s for critical / services / batch). Those numbers were intended as user-facing targets, but absent a real provider in the harness they get applied as internal floors. **The scaletest harness gates only the internal floor.** Real-provider validation lives in:

- The provider conformance suite (`test/conformance/`) — hits a real provider with controlled load and asserts behaviour. Provider repos own their conformance runs; this repo provides the spec.
- Out-of-tree provider scaletests — run by each provider implementer against their target environment.
- Production canaries — the ultimate ground truth; lives in the deploying organisation, not in this repo.

The harness's job is to make sure BigFleet's contribution doesn't regress. That's a regression detector, not a user-experience SLO.

### 3. ADR-0014's "what users feel" framing is amended

ADR-0014 is updated to clarify that the 5 s in-process tier is what BigFleet's internals contribute under a fake provider. The user-facing SLO is `internal_binding_latency + provider_capacity_create_latency`, with the second term measured outside this repo.

## Consequences

- **Honesty**: docs and metric names match what's actually measured. We stop telling users "we test the user-facing SLO" when we don't.
- **Renaming is breaking** for anyone scripting against profile YAML or the runner's failure message. The profile-level field is renamed across all in-tree profiles in this commit. External profiles need a one-line edit.
- **The harness's release gate is unchanged in behaviour** — same metric, same threshold, same -1-sentinel skip. Only the name changes.
- **Real-provider validation gap is now explicit**, which makes it easier to argue for: a provider repo running its own conformance + scale tests against its target cloud, and uploading aggregated SLO metrics to a shared dashboard. That's a follow-on conversation, not a v1 commitment.
- **ADR-0017 stays valid**: the per-Pod metric is still the right release gate, the legacy fingerprint-fan-out histogram is still a diagnostic. The per-Pod metric just measures less than ADR-0017's framing claimed.

## Implementation notes

- Profile YAML: every `slo.bindingLatencyP99Seconds` becomes `slo.internalBindingLatencyP99Seconds`. The chart's defaults YAML uses the new name.
- Runner: `sloOverrides.BindingLatencyP99Seconds` → `InternalBindingLatencyP99Seconds`. The failure message becomes `"internalBindingLatencyP99Seconds %.3fs > %.1fs SLO (ADR-0018 — internal-only; real-provider tests live elsewhere)"`.
- Histogram help text is amended to: "BigFleet-internal binding latency: Pod.creationTimestamp to Pod-bound, with the in-process fake provider contributing zero latency. ADR-0018: real-provider time is not measured here; user-facing latency = this + provider_capacity_create_latency."
- ADR-0014 gets a brief "see also ADR-0018" pointer at the top.
