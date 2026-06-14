# ADR-0035: Scaletest SLOs are measured at steady state under churn, not at ramp

## Status

Accepted, 2026-05-19.

Supersedes the relevant pass/fail logic in `test/scaletest/cmd/scaletest-runner/main.go` (M22's ramp budget as the run-gating signal). Drives the rejection of [ADR-0033].

## Context

The scaletest harness has historically gated runs on **ramp behaviour**: install at machine count N, wait for the cluster to bind ~100 % of N Pods within an M22-derived budget, then soak. Runs that didn't reach the bind-fraction target inside the ramp budget failed the test. The "bind ramp" percentage became, in practice, the headline pass/fail signal.

This conflated two distinct things:

1. **Capacity / ramp**: "how fast does the system fill from 0 to N?" — a useful operational metric, dominated by downstream-of-BigFleet behaviour (kube-scheduler binding throughput, kine WAL latency, image pull, kubelet readiness, etc.).
2. **Steady-state SLO**: "once the system is filled, how quickly does it respond to per-CR demand under churn?" — what [ADR-0014], [ADR-0017], [ADR-0028] actually define as the SLO. Per-CR binding latency, cycle p99, rollup p99.

Treating ramp percentage as the SLO produced a long investigation chain (M48 through the scale-test investigations of mid-May 2026) which converged on the diagnosis that **the bind ramp ceiling at uber-5k is a kube-scheduler property** under high label-cardinality workloads, paced by NodeAffinity rejection rate and preemption-walk overhead. Lifting that ceiling required either over-provisioning (the rejected OC3 path) or substrate rewrites (the rejected Variant B path) — none of which move steady-state SLOs, which were never failing in the first place.

The ramp gate also obscured a simpler truth: **in steady state, demand rate equals churn rate** (~2 %/min × N for the canonical profiles ≈ N/3000 Pods/sec per cluster) — well below the scheduler's per-cluster ceiling. Phase 1's aggregate-supply math is exactly correct at the churn rate; supply matches demand precisely as Pods replace each other.

`bigfleet.md` and the operator-paper define BigFleet's value proposition as "right-sized inventory under steady demand with priority-driven preemption." Nowhere does it pitch ramp-from-empty throughput as the primary characteristic. The SLOs the docs ship reflect that. The test gate should too.

## Goals

1. **Pass/fail signal is per-CR binding latency under churn**, not ramp percentage.
2. **The cluster reaches steady state at install time**, by pre-seeded inventory: Configured / Speculative / Idle tiers populated so that the demand profile is already covered when the run starts.
3. **Churn drives the SLO measurement**: a configurable replacement rate (e.g. 2 %/min) creates a continuous stream of new Pods replacing drained ones. Each replacement Pod's CR-creation-to-bound latency is the SLO sample.
4. **Ramp behaviour is a separate, optional metric** — still captured and reported, no longer pass/fail by default.
5. **No BigFleet code changes required**. The scaletest harness changes; the system-under-test stays unchanged. ([ADR-0033]'s rejection is the consequence.)
6. **The change is implementable on the existing chart** with extensions to the M29 / [ADR-0026] seed mechanism, not a rewrite.

## Non-goals

- **Removing ramp metrics altogether.** The capacity-exploration story stays — it's informative when a substrate or scale rung is new. It just stops gating the pass/fail.
- **Adopting a different SLO definition.** [ADR-0014] / [ADR-0017] / [ADR-0028] stay. We're changing *when* we measure them, not what they are.
- **Changing the realistic-catalog model** (ADR-0032). The catalog stays; the test methodology around it changes.

## Decision

**The scaletest-runner gates on steady-state SLOs measured during a soak window with continuous churn. Pre-seeded inventory means steady state is reached at install, not after a ramp. Ramp-percent and ramp-time become observational metrics, not gates.**

### Concrete shape

1. **Pre-seeded inventory at install.** The chart already supports `shard.seedMachines` for Configured machines (M29) and `shard.speculativeMultiplier` for the Speculative tier ([ADR-0026]). Extend this so the seed is **distribution-matched** to the realistic-catalog's per-Profile demand: if `tiny-stateless` has 70 % weight, ~70 % of Configured seed is `tiny-stateless`-shaped (instance-type, zone, labelAxes drawn the same way the load-driver picks per-Pod).

2. **Pre-bound Pods at install.** The load-driver, at install time, creates Pods in the inner kwok cluster with `Spec.NodeName` already populated to a matching pre-seeded fake-Node. No initial scheduler walk. No ramp. The cluster starts at 100 % bound.

3. **Churn drives SLO measurement.** The load-driver applies a continuous churn rate (default `churnPerMinute: 0.02`). For each replacement:
   - Old Pod is deleted; the fake-Node it was on transitions to Idle (or stays bound briefly until the new Pod arrives, depending on policy).
   - New Pod is created with the same Profile fingerprint as the old (so demand stays at steady state).
   - The CR-creation-to-bound latency for this new Pod is the SLO sample. Recorded in a histogram tagged with the Profile fingerprint.

4. **Runner pass/fail:**
   - **Pass**: SLO histograms over the soak window meet [ADR-0014] / [ADR-0017] / [ADR-0028]'s thresholds. Specifically `internal_binding_latency_p99 ≤ profile.slo.internalBindingLatencyP99Seconds`, `shard_cycle_duration_p99 ≤ profile.slo.shardCycleDurationP99Seconds`, etc.
   - **Fail**: any SLO histogram exceeds its threshold by ≥10 % at the soak window's tail.
   - **Informational** (not gated): ramp time (how long install + pre-seed took), peak bind-rate during pre-seed.

5. **Drop the M22 ramp budget as a pass/fail gate.** Replace its role with a steady-state-reached check (e.g. "all kwok clusters report `Spec.NodeName` populated on ≥99 % of Pods within 5 min of install"). This is a sanity check that pre-seeding worked, not an SLO gate.

### Profile schema changes

`test/scaletest/profiles/<scale>.yaml` (and the legacy uber-*.yaml / dev-*.yaml / failover-*.yaml during the migration window) gain:

- `seed.preBindFraction: 1.0` — the fraction of `target` Pods pre-bound at install. Default 1.0; reduce only for tests that intentionally exercise the ramp behaviour.
- (existing) `loadProfile.churnPerMinute` — already present; the SLO measurement now runs during the soak window driven by this rate.

The legacy fields `target` (Pods per cluster) and `durationSeconds` (soak length) stay. The legacy `rampBudget` field is parsed but emits a deprecation warning if the profile uses it for gating.

### Implementation outline

| Stage | Scope | Effort |
|---|---|---|
| **A. Distribution-matched seeding** | Extend `pkg/scaletest/archetype` + chart's pre-seed loop to draw Configured supply from the same catalog distribution the load-driver draws demand from. Bound Profile cardinality stays per M34/M35. | ~2 d |
| **B. Pre-bound Pod creation** | Load-driver at install time: enumerate target Pods, match each to a pre-seeded Node, set `Spec.NodeName`. Pod-shim / kwok-controller already faked binding; this just moves it from "after scheduler binds" to "at Pod creation." | ~1–2 d |
| **C. Runner gate change** | `pass()` in `test/scaletest/cmd/scaletest-runner/main.go` reads SLO histograms from Prometheus over the soak window; computes pass/fail per-SLO. Ramp budget logic preserved but reduced to a "steady-state-reached" sanity check. | ~1 d |
| **D. Re-validate** | uber-5k under the new test pattern. Should pass with no BigFleet code changes. | brief filing |
| **E. Migration** | Existing legacy profile YAMLs gain `seed.preBindFraction: 1.0`. Site sync regenerates runbook. | ~0.5 d |

Total ~1 week of harness work + one validation brief.

### What this means for prior work

- **OC3 is not shipped.** Not in shard binary, not as a substrate knob. The mechanism it described (over-provisioning to reduce scheduler preemption) only mattered for ramp. Steady state doesn't hit the preemption regime.
- **[ADR-0033] is rejected** as superseded by this ADR's reframe. The "Phase 1 supply-credit must respect bind readiness" question doesn't arise when the test measures steady-state SLO under matched-distribution churn, because Phase 1 emits per-Need at exactly the churn rate and supply is always ready.
- **Variant B substrate redesign** (the real-K8s-per-cluster path explored in scoping) stays rejected. The scheduler ceiling that motivated it only mattered for ramp throughput; it does not affect steady-state binding latency at the churn rates the SLO targets.
- **Per-CR binding latency** ([ADR-0017]) becomes the headline SLO signal. It was always meant to be — the realistic-catalog shift made ramp-time accidentally dominant, and this ADR restores the intended order.

## Alternatives considered

### Ship OC3 as substrate knob

Add a chart value `shard.phase1CreditFraction`. Test profiles set it to 0.5–0.8 to compensate for the kwok-scheduler ramp ceiling. Production defaults to 1.0.

**Rejected** because it builds a knob into the production codebase for a test-harness regime. Even if defaulted to no-op, the existence of the knob admits a regime where Phase 1 under-provisions — which isn't true in production, where the scheduler is fast enough that the ramp regime doesn't manifest. Carrying production-irrelevant code for harness convenience violates YAGNI.

### Replace the substrate with real K8s (Variant B)

Per the scoping done during this investigation: one substrate host can run one real-K8s control plane per BigFleet cluster at uber-5k; ~100 hosts at uber-500k. A real apiserver has higher bind throughput than the kwok-bundled apiserver.

**Rejected** because (a) the gate isn't the apiserver — it's the scheduler, which is the same code in any K8s deployment, and (b) the metric we should be gating on isn't ramp throughput. Real K8s would have the same scheduler ceiling at high label cardinality and wouldn't move steady-state SLOs (which are passing today on the kwok substrate).

### Keep ramp as a pass/fail gate; tune the ramp budget liberally

Just bump the M22 ramp budget formula's constants until uber-5k passes at the observed ~14 Pods/sec/cluster ceiling.

**Rejected** because it doesn't fix the fundamental conflation. A liberally-tuned ramp budget that passes uber-5k will fail at uber-50k or larger by exactly the same mechanism, and we'll be back here. The methodology question — what we measure — has to be addressed.

## Migration plan

1. **Stage 0**: ADR sign-off (this document).
2. **Stage A**: Distribution-matched seeding in `pkg/scaletest/archetype` + chart.
3. **Stage B**: Pre-bound Pod creation in load-driver.
4. **Stage C**: Runner `pass()` reshape.
5. **Stage D**: uber-5k re-validation brief (cloud). Verdict must show SLOs pass under churn-driven measurement on the unchanged shard.
6. **Stage E**: Legacy profile YAMLs migrate to the new schema. Runbook reshape.
7. **Stage F**: Close out the rejected scale-test investigation threads with redirect notes to this ADR.

uber-50k and beyond don't need separate validation passes for this ADR — they'll be validated as part of the normal scale ladder once Stage D confirms the methodology.

## Hard rules touched

None. This ADR changes test methodology; the BigFleet system-under-test contract is unchanged. Specifically:

- Provider RPC surface: unchanged.
- Coordinator / shard / operator wire format: unchanged.
- Cost formula: unchanged.
- Static stability: unchanged. (Failover profiles still validate it.)
- [ADR-0014] / [ADR-0017] / [ADR-0028] SLO definitions: unchanged. We're measuring them in the regime they were defined for, which is steady state.

The lesson worth landing: **ramp behaviour is not an SLO.** Capacity-exploration metrics and SLO metrics serve different purposes; conflating them produces investigation rabbit holes (M48 → OC3 → #30 → #33 → #34) that don't serve the user.

## Amendment (2026-06-14): reclaim measurement — settle window + bounded floor

M77a added a steady-window reclaim-flatness gate on top of this ADR's per-CR SLOs: snapshot the Reclaim-action counter at "steady declared", and fail the run if the post-soak delta is non-zero (the bootstrap≈reclaim oscillation class M67 / [ADR-0045] removed must not resurface). Two empirical findings from the bigfleet-uber #65-69 diagnosis chain make both the *when* and the *what* of that gate wrong as originally written, and this amendment corrects them. They are the reclaim-side analogue of this ADR's headline lesson — measure the SLO in the regime it is defined for (steady state), not in a transient.

### (a) Measure at steady state, not in the post-fill settling transient

This ADR pre-seeds inventory so the cluster reaches steady state *at install* — but that is steady **demand**, not a settled **fleet**. [ADR-0021]'s persistent execute pool decouples action execution from the cycle barrier, so after "steady declared" the fleet keeps actuating for 1–2 min as in-flight Create/Drain/Delete settle. The reclaim **rate decays through the soak**: #65-69 measured ~1.91 reclaims/s soak-average against 0.52–0.86/s at the soak's *end*. A full-soak integral is dominated by that settling tail, exactly the ramp-vs-steady-state conflation this ADR was written to kill — one level down, on the reclaim counter instead of the bind ramp.

The reclaim baseline snapshot therefore moves: `loadProfile.settleSeconds` (default 0 = unchanged) delays it to `soakStart + settleSeconds`, so the measured window is the **settled** portion of the soak. The mechanism is a one-shot timer in the runner's soak select-loop; it stays a raw absolute-counter delta (read at the settle mark, read again at end, subtract) — no `rate()`/`increase()` extrapolation, which can both invent and hide single-digit increments. Only the reclaim baseline moves; the per-CR binding-latency, cycle, rollup, and ack SLOs are unaffected. A `settleSeconds ≥` the soak duration is a misconfig that would empty the window, so it clamps to the `soakStart` snapshot and warns.

### (b) The reclaim SLO is bounded, not zero

The original gate asserted **zero** reclaims over the window. Zero is structurally unachievable on the async engine: [ADR-0021]'s async execute means the fleet self-perturbs at a non-zero rate independent of demand churn. #67 diagnosed this floor as a **coverage-harmless endogenous self-perturbation** — bind coverage stays whole; it is the engine breathing, not the oscillation defect M67 removed — and #69 measured it robust at ~340 over a 180 s soak un-de-tailed. A zero assertion against a structurally non-zero floor is a permanently-red gate that tells you nothing.

The gate becomes **bounded-reclaim**: `slo.maxReclaimActionsDuringSoak` (default 0 = the original zero assertion, every other profile unchanged) caps the count over the settled window. The bound accepts the residual steady floor while staying a real gate — a regression (the bootstrap≈reclaim oscillation resurfacing as sustained churn far above the bound) still trips it. The bound is an author-owned posture number, in the same class as `ReclaimGrace`: dev-50 sets it provisionally to 150 (~2–3× the de-tailed steady estimate of ~45–77 over its 90 s settled window), pending the validation re-run that measures the actual de-tailed value.

### Scope

Harness-only, consistent with this ADR's Goal 5 (no system-under-test change). The settle window and the bound live entirely in `test/scaletest/cmd/scaletest-runner/main.go` and the profile YAML; `pkg/decision` and `pkg/shard` are untouched. See [ADR-0045] for the attribution model the reclaim contract sits on, and [ADR-0021] for the async-actuation source of the floor.

## References

- [ADR-0014] SLO posture: binding latency, not cycle wall-clock.
- [ADR-0017] Per-CR binding latency vs fingerprint fan-out latency.
- [ADR-0026] Scaletest harness models the Speculative tier (pre-seed mechanism).
- [ADR-0028] Cycle-p99 is regime-parametric.
- [ADR-0032] Realistic catalog production-calibrated workload distribution.
- [ADR-0033] Phase 1 supply-credit must respect bind readiness (rejected; superseded by this ADR's reframe).
- [ADR-0021] Persistent execute pool — the async-actuation source of the non-zero reclaim floor (Amendment).
- [ADR-0045] Consumed capacity in the attribution model — the reclaim contract the steady-window gate sits on (Amendment).
[ADR-0014]: ./0014-slo-posture-binding-latency-not-cycle-wall-clock.md
[ADR-0017]: ./0017-per-cr-binding-latency-vs-fingerprint-fanout.md
[ADR-0021]: ./0021-persistent-execute-pool.md
[ADR-0026]: ./0026-scaletest-models-speculative-tier.md
[ADR-0028]: ./0028-cycle-p99-is-regime-parametric.md
[ADR-0032]: ./0032-realistic-catalog-production-calibration.md
[ADR-0033]: ./0033-phase1-supply-credit-respects-bind-readiness.md
[ADR-0045]: ./0045-consumed-capacity-in-the-attribution-model.md
