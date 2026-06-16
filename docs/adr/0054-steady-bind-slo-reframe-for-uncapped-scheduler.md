# ADR-0054: Steady pod-bind SLO is reframed onto BigFleet's capacity-delivery deliverable under an uncapped real scheduler

**Status**: Accepted

**Date**: 2026-06-16

## Context

The scaletest harness's headline steady-state release gate is `internalBindingLatencyP99Seconds`, defined by the load-driver as wall-clock from `Pod.metadata.creationTimestamp` to the load-driver observing `spec.nodeName` set, for Pods created after the steady phase began (`test/scaletest/cmd/load-driver/main.go:262-273`). It is gated in `pass()` (`scaletest-runner/main.go:2116-2118`) against a 15 s target (ADR-0020) and re-checked in-soak by `soakFailFastCheck` (`main.go:1966-1973`).

Three facts, established across a multi-issue diagnosis arc (bigfleet-uber #66/#74/#75/#76/#77/#78), make this the wrong release gate:

1. **It is an END-TO-END measurement, not a BigFleet-internal one.** `renderHelmValues` hardcodes `harness.scheduler: kube-scheduler` for all V2 profiles (`main.go:548-550`), so steady/churn Pods go through the real kube-scheduler. The measured interval therefore spans the cluster's own (uncapped) kube-scheduler retry/backoff WAIT and the reprovision back-edge — neither BigFleet's deliverable. The "internal" in the name is a legacy holdover from the pod-shim path (ADR-0018); under the default harness it is end-to-end. This completes the M66.3 thread: the gate was *vacuous* (pod-shim-only, read -1 on the whole uber ladder), then *measured* (kube-scheduler-mode source landed), then *trustworthy* (M79.4/M79.5 de-saturated the histograms after #77's saturation artifact). This ADR is the final stage: *honestly targeted*.

2. **The BigFleet engine is CLEAN.** #78's A/B proved BigFleet's per-decision engine is clean in both arms: shardCycle p99 0.255 s; per-machine node-materialization (`shard_configure_phase`) 0.56 s; scheduler-attempt *compute* 0.51 s; 0 shortfalls; no oversubscription. The end-to-end pod-bind p99 (hundreds-to-1300 s, de-saturated) decomposes into **(i)** the uncapped kube-scheduler retry/backoff WAIT (`sli_duration` p99 ~1310 s; cap-mitigable 3-5x but we are not capping) and **(ii)** the reprovision back-edge (~410 s; a churn-reclaimed Pod cannot bind until a *replacement* machine reaches Configured — genuine reprovision physics ADR-0018 never modeled). p50 is 1.5-6.4 s; p90 11.5-99.5 s; only the p99 is dominated by (i)+(ii).

3. **Author decision (2026-06-16): the kube-scheduler stays UNCAPPED.** Production-faithful — BigFleet must not reconfigure the cluster's scheduler (e.g. `schedulerPodMaxBackoffSeconds`) to pass its own SLO. This OVERRIDES `plan.md` §12 item 5 recommendation (1) ("cap the scheduler backoff for SLO runs"). With the scheduler uncapped, (i) and (ii) are permanently in the end-to-end p99 by physics, so a 15 s flat-p99 gate on this metric is structurally unreachable — exactly the permanently-red / vacuously-skipped gate ADR-0035's amendment warns against.

This is the one-layer-out analogue of **ADR-0020**: there the structural floor was the 10 s rollup interval and the SLO was sized to it (`rollupInterval + 5 s = 15 s`) rather than lowering the production posture; here the floor is the uncapped scheduler retry WAIT + the reprovision back-edge, and we must not reconfigure the production posture (the scheduler) to pass. It is the bind-latency analogue of **ADR-0035's reclaim amendment**: replace a structurally-impossible target against an uncontrollable floor with a regime-aware, BigFleet-scoped gate — without weakening the regression detector (ADR-0020:55) and without gaming the uncontrollable metric (ADR-0017:98).

## Decision

**The steady-state release gate moves off the end-to-end pod-bind p99 (which BigFleet does not control under an uncapped scheduler) and onto BigFleet's actual capacity-delivery deliverable plus its coverage contract. The end-to-end pod-bind p99 becomes informational regime-context.** Two halves, to avoid the reframe-to-pass trap:

### Half 1 — BigFleet-property bars (the release gate; catches real engine regressions)

Promote to hard gates in `pass()`, each measured at steady state under churn (ADR-0035), held scale-invariant per ADR-0028:

- **`shardConfigurePhaseP99Seconds` <= 15 s** — per-machine wall-clock Idle->Configuring->Configured inside `executeBootstrap` (`pkg/metrics/metrics.go:66-70`, observed `pkg/shard/execute.go:397`): operator-stream BootstrapRequest RTT + `Provider.Configure` + the post-Configure transition. The capacity-materialization *latency* BigFleet owns end to end. Per-machine, observed on every Bootstrap success path -> **continuous and non-saturating** under steady churn, unlike `bigfleet_shard_provisioning_latency_seconds` which is per-(cluster, fingerprint), observe-once-and-delete, and saturates to +Inf in steady state (ADR-0017 addendum binding: that histogram is a fingerprint-fan-out *diagnostic*, never a gate; M79.5's widening was for diagnosis only). #78 measured configure-phase 0.56 s — ~27x headroom.
- **`bootstrapSuccessRatio` >= 0.99** (materialization *throughput*, NOT latency) — from `bigfleet_shard_action_execute_outcomes_total{action="Bootstrap",outcome}` (scraped via `readBootstrapsExecuted`, `main.go:1588-1596`): `success / (success+failure)` over the soak. **This closes the coverage hole the adversarial review found:** `shardConfigurePhaseP99Seconds` times only machines that *succeed*, and `shardShortfalls==0` is **blinded by ADR-0052's in-flight crediting** (a Creating machine counts toward coverage before it materializes), so a materialization *throughput collapse* — machines repeatedly failing/retrying Configure — would slip past both a latency gate and the shortfall gate. The success-ratio gate trips on exactly that class. It is the throughput counterpart to configure-phase's latency, the same latency-plus-throughput pairing ADR-0035 Drop-W established for the reclaim path.
- **`operatorNodeStateUpdateP99Seconds` <= 1 s** — `bigfleet_operator_node_state_update_duration_seconds` (`metrics.go:481-483`, observed `pkg/operator/upcoming.go:54`): the operator publishing UpcomingNode=Ready after the shard signals Configured. This was the **one BigFleet-owned hop with zero runner coverage** — instrumented but never queried. It was empirically a real tail source (Drop S: a Conflict on the Configured-phase write stuck UpcomingNode on Configuring for tens of seconds, a >=102 s tail). Gating it makes explicit the coverage the end-to-end metric provided only implicitly.
- **`shardShortfalls == 0`** — `sum(bigfleet_shard_shortfalls)` (`main.go:1832`), today a steady-state *precondition* (`main.go:1503`) but never in the post-soak verdict. BigFleet's true ADR-0045 contract: demand covered by bound capacity. Promoting it to `pass()` is the cheapest, most direct anti-reframe-to-pass guard (and is why the success-ratio gate above is needed alongside it, not instead of it).

The existing clean engine gates are RETAINED unchanged: `shardCycleDurationP99Seconds` (throughput envelope), `operatorRollupP99Seconds`, `operatorAckP99Seconds`, `coordinatorApplyErrorRate`, `operatorOutboxDropsPerSec`, and the `maxReclaimActionsDuringSoak` bounded-reclaim gate (ADR-0035 amendment).

### Half 2 — Acknowledged regime-bound tail (informational; physics BigFleet does not control)

- **End-to-end pod-bind p99 + raw-max become INFORMATIONAL.** `internalBindingLatencyP99Seconds` and its non-saturating cross-check `internalBindingLatencyMaxSeconds` (`main.go:1805`) are still scraped and emitted in `summary.json`, but removed from `pass()` and from `soakFailFastCheck`. They are regime-context: dominated by the uncapped scheduler retry WAIT + reprovision back-edge.
- **A LOOSE end-to-end p50 liveness gate is kept**: `endToEndPodBindP50Seconds <= 10 s` (dev profiles 30 s for the kine write tail). p50 sits below the scheduler-retry tail (retries hit the minority of Pods), so a p50 blowup means the *common* bind path broke — a real liveness signal — while the p99 stays informational. Explicitly a coarse liveness floor, NOT the release gate. (Easily dropped to fully-informational if the author prefers zero end-to-end gating.)

### Threshold philosophy (ADR-0028 held-vs-scaled; ADR-0035 author-owned posture numbers)

`shardConfigurePhase` and `operatorNodeStateUpdate` are per-machine / per-frame wall-clocks independent of fleet cardinality -> **held bars**, identical across the uber ladder (5k..5m). `bootstrapSuccessRatio` is a dimensionless ratio -> also held. Targets are author-owned posture numbers in the `ReclaimGrace` / `maxReclaimActionsDuringSoak` class: **provisional in code, ratified after the dev-50 + uber-5k re-measure reports the de-tailed actuals** (exactly how `maxReclaimActionsDuringSoak=150` was filed). The held configure-phase bar inherits ADR-0020's method: size the SLO to the structural floor, preserve the regression detector.

## What this ADR amends / extends / references

- **Amends ADR-0020**: its `internalBindingLatencyP99 = rollupInterval + 5 s = 15 s` arithmetic assumed the fake-provider in-process floor with no real kube-scheduler retry and no reprovision back-edge. Under an uncapped real scheduler that end-to-end 15 s p99 is structurally unreachable. We preserve ADR-0020's *method* and move the 15 s held bar onto `shardConfigurePhase`.
- **Amends ADR-0014**: ADR-0014:41-43's "binding-latency p99 is the user-facing release gate" — the harness end-to-end pod-bind *variant* is demoted to informational under an uncapped scheduler; the release gate moves to the capacity-delivery hops (configure-phase + UpcomingNode-publish) BigFleet owns. ADR-0014's gate *identity* (a capacity-delivery latency is the gate, not cycle wall-clock) is honored.
- **Amends ADR-0035**: its steady-state pass() methodology gains the configure-phase/success-ratio/node-state-update/shortfall gates and drops the end-to-end-p99 gate; the "we change when/what we measure, not the contract" principle is the precedent.
- **Extends ADR-0028**: instantiates held-vs-scaled for the steady-pod-bind row — BigFleet-property bars held; the end-to-end pod-bind tail is the acknowledged regime-bound (now informational, the regime being an uncapped scheduler we don't control).
- **References ADR-0013** (regime root), **ADR-0017** (don't-game-the-metric + the binding ruling that `shard_provisioning_latency` stays a diagnostic), **ADR-0018** (the internal-vs-provider decomposition this updates), **ADR-0032** (the catalog the reframed SLO grades), **ADR-0045** (the `shortfalls==0` coverage contract), **ADR-0052** (the in-flight crediting that blinds shortfalls, motivating the success-ratio gate).

## Consequences

- `sloOverrides` gains `ShardConfigurePhaseP99Seconds`, `BootstrapSuccessRatio`, `OperatorNodeStateUpdateP99Seconds`, `EndToEndPodBindP50Seconds`. `InternalBindingLatencyP99Seconds` is retained for backward-compat parsing but no longer gates (documented retired-to-informational).
- `pass()` deletes the end-to-end-p99 block and adds configure-phase, success-ratio, node-state-update, shortfalls==0, and the loose p50 blocks. `soakFailFastCheck` swaps its bind branch to configure-phase so in-soak and post-soak gates stay in lockstep. `unmeasuredGated()` drops `internalBindingLatencyP99Seconds` and adds the new gated keys.
- New runner queries for `bigfleet_operator_node_state_update_duration_seconds` p99 and the Bootstrap success ratio.
- A **`pass()` unit test ships in the same commit** (table-driven: configure-phase breach, success-ratio breach, node-state-update breach, shortfalls>0, p50 breach, all-pass) — there is no existing `pass()` test, so without it the new gates would be covered only by the dev-50 rung.
- Every profile's `slo:` block swaps `internalBindingLatencyP99Seconds` for the four new fields. Held bars identical across the ladder; dev profiles loosen node-state-update + p50 for the kine write tail.
- `plan.md` §12 item 5 records: rec (1) cap-the-scheduler OVERRIDDEN; (2) reframe shipped via this ADR; (3) reprovision back-edge optimization is a non-blocking held-engine-bar follow-on.
- uber-5k (#258) publishes on the reframed gate.

## What this ADR is NOT

- **Not a reframe-to-pass.** The gate moves to DIFFERENT, BigFleet-controllable metrics — it does not loosen a threshold on the same uncontrollable metric. Coverage strictly *increases*: hop 5 (node-state-update) was a real uncovered hole, and the success-ratio gate closes the materialization-throughput hole that latency+shortfall gates miss under ADR-0052 crediting. Each new gate trips on a genuine future regression of its hop.
- **Not a definition change.** ADR-0014/0017/0018/0028/0035 definitions stand; this changes the *target shape* and *which metric the release verdict reads*.
- **Not a change to the system under test.** Harness-only (`scaletest-runner` + profile YAML). `pkg/decision`, `pkg/shard`, `pkg/operator` wire formats unchanged; static stability unchanged.
- **Not a scheduler cap.** The end-to-end p99 stays uncapped and informational, per the author decision.

## Validation

Climb the ladder (CLAUDE.md): `make prevalidate` (runner/profile compile + closed-loop sim) + the new `pass()` unit test -> `go test ./test/scaletest/...` + `go build ./...` + lint -> the devpod-side kind/dev-50 integration rung (confirm the four new gates appear GATED in `summary.json`, end-to-end p99 + raw-max appear but are NOT in the verdict, run passes on BigFleet's deliverable; ratify the provisional posture numbers against the de-tailed actuals) -> re-greenlight uber-5k (#258).
