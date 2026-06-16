# Service-level objectives (SLOs)

This page is the reference for every SLO BigFleet gates a scale-test release on:
the metric it reads, the threshold, and — at length — *why that metric and not
another*. The justifications matter more than the numbers. A threshold is a
posture knob you can re-tune after a measurement; the *choice of metric* is a
claim about what BigFleet is responsible for, and getting that wrong produces a
gate that is either permanently red against a cost BigFleet doesn't control, or
green while a real regression sails through. Each entry below argues its metric
from that standpoint.

The decisions of record are the ADRs cited per row (chiefly ADR-0014, ADR-0017,
ADR-0020, ADR-0028, ADR-0035, ADR-0045, ADR-0052, ADR-0054). The runtime source
of truth is `pass()` and the `sloOverrides` struct in
`test/scaletest/cmd/scaletest-runner/main.go`, plus the `slo:` block in each
`test/scaletest/profiles/*.yaml`.

## The five principles

1. **Gate BigFleet's deliverable, never an uncontrolled dependency.** The
   harness runs a real, uncapped Kubernetes scheduler and a real provisioning
   back-edge. Latencies that those impose — not BigFleet's decision engine —
   are reported but never gated. BigFleet must not, for example, reconfigure the
   cluster scheduler to make its own SLO pass; it gates only the hops it owns.

2. **Steady state, not ramp (ADR-0035).** Every gate is measured over the soak
   window after the fleet reaches steady state, never during the cold-start
   ramp. The ramp is a synthetic thundering herd; what a customer feels is the
   latency of a churn replacement against a warm fleet. Ramp numbers are kept
   for capacity exploration but never gate.

3. **Held bars vs scaled bars (ADR-0028).** A per-machine or per-frame quantity
   (configure-phase latency, a success ratio, one cycle's duration) is
   *scale-invariant* — it should read the same at 5k machines and 5M, so its
   threshold is *held* identical across the whole ladder. Only quantities that
   genuinely scale with fleet size get size-dependent thresholds.

4. **Thresholds are provisional, author-owned posture numbers.** Like
   `ReclaimGrace`, a threshold is set in code, then ratified against the
   de-tailed actuals from a real run and re-tuned if needed. A number here is a
   defensible starting posture, not a law of physics. Where a number has large
   headroom that is intentional and noted.

5. **A latency gate is blind to work that never finishes — pair it with a
   throughput or coverage gate.** A p99 of *completions* says nothing about
   *completions that never happened*. Every place BigFleet could fail silently
   by simply not doing the work, a throughput/coverage gate sits alongside the
   latency gate.

## Summary

Gated metrics decide pass/fail. Informational metrics are scraped into
`summary.json` for context and never gate. Thresholds shown are the cloud /
uber-ladder posture; dev profiles loosen a few for the kine write-tail (noted
per row).

| SLO | Metric | Gated? | Threshold (ladder) | ADR |
|---|---|---|---|---|
| Capacity materialization latency | `shardConfigurePhaseP99Seconds` | gate | ≤ 15 s (held) | 0054 |
| Capacity materialization throughput | `bootstrapSuccessRatio` | gate (MIN) | ≥ 0.99 (held) | 0054 |
| Node-publish latency | `operatorNodeStateUpdateP99Seconds` | gate | ≤ 1.5 s (dev 5 s) | 0054 |
| Demand coverage (the contract) | `shardShortfalls` | gate | == 0 | 0045/0054 |
| Decision-engine cycle | `shardCycleDurationP99Seconds` | gate | ≤ 5 s | 0035 |
| Roll-up pipeline turn | `operatorRollupP99Seconds` | gate | ≤ 1 s (dev 2 s) | 0020 |
| Roll-up acknowledgement | `operatorAckP99Seconds` | gate | ≤ 12 s (dev 30 s) | 0028 |
| Reclaim quiescence under churn | `maxReclaimActionsDuringSoak` | gate (bounded) | per-profile (e.g. 150) | 0035 |
| Common-path bind liveness | `endToEndPodBindP50Seconds` | gate (loose) | ≤ 10 s (dev 30 s) | 0054 |
| End-to-end pod-bind tail | `internalBindingLatencyP99Seconds` + `…MaxSeconds` | informational | — | 0054 |
| Fingerprint fan-out latency | `bigfleet_shard_provisioning_latency_seconds` | informational | — | 0017 |

---

## Capacity-delivery gates (what BigFleet promises)

BigFleet's job is to make capacity *available* — to take a cluster's demand and
have a matching, configured machine exist for it. It does **not** place pods;
the cluster's own scheduler does. The four gates in this section are BigFleet's
end-to-end deliverable, decomposed hop by hop so that a regression anywhere in
"demand observed → machine materialized → node published" trips a specific gate.
This decomposition is the substance of ADR-0054, and it is what lets us *stop*
gating on end-to-end pod-bind latency without losing regression coverage.

### `shardConfigurePhaseP99Seconds` — capacity materialization latency

**What it measures.** Per machine, the wall-clock from the shard issuing the
`Idle → Configuring` transition to the `Configuring → Configured` transition
completing, inside `executeBootstrap` (`pkg/shard/execute.go`, histogram
`bigfleet_shard_configure_phase_seconds`). Concretely that interval is the
operator-stream `BootstrapRequest` round-trip plus `Provider.Configure` plus the
local state-machine write. It is "how long does it take BigFleet to turn a
chosen machine into a ready one."

**Why this is the right gate.** This is the single hop most representative of
BigFleet's capacity-materialization cost, and it has two properties that make it
gateable where other candidates are not. It is *per machine* — observed once on
every Bootstrap, so its sample count grows with work and it never goes empty
under churn. And it is *non-saturating*: contrast `bigfleet_shard_provisioning_latency_seconds`,
which is per-(cluster, fingerprint) with observe-once-and-delete semantics and
therefore pins to its top histogram bucket in steady state (ADR-0017 keeps that
metric a diagnostic, never a gate). Configure-phase is the honest, continuous
signal of the same underlying cost.

**Why 15 s, held.** The number inherits ADR-0020's method: size the SLO to the
structural floor, don't move the production posture to pass. The bigfleet-uber
#78 A/B measured configure-phase at ~0.56 s, so 15 s is roughly 27× headroom —
deliberately loose, because this is a held bar (a per-machine wall-clock,
independent of fleet size, identical from 5k to 5m) and we would rather catch a
genuine multiple-second regression than chase false positives on cloud
write-tail variance. It can be tightened once more runs ratify the de-tailed
actual.

**What a breach means.** The stream RPC, the provider's Configure, or the
post-Configure transition got slow — a real, BigFleet-owned regression in the
materialization path. Because this gate is about *latency of successes*, it is
deliberately paired with the throughput gate below.

### `bootstrapSuccessRatio` — capacity materialization throughput

**What it measures.** Over the soak, `success / (success + failure)` for
Bootstrap actions, from `bigfleet_shard_action_execute_outcomes_total{kind="Bootstrap"}`
(`success` vs every failure outcome: `no_session`, `transition_error`,
`blob_error`, `configure_error`, `ctx_canceled`, `fenced`). It is a **MIN**
gate: it fails when the measured ratio drops *below* the target, the opposite
direction from every latency gate.

**Why this gate exists — the coverage hole it closes.** This is the
fifth-principle gate, and it was the catch of ADR-0054's adversarial review.
Configure-phase latency times only machines that *succeed*; a machine stuck in a
fail-retry loop contributes no sample, so a Configure throughput collapse is
invisible to it. You might expect the coverage gate (`shardShortfalls == 0`) to
catch that instead — but it cannot, because ADR-0052's in-flight crediting
counts a `Creating` machine toward coverage *before* it materializes, so demand
reads as covered even while materialization is failing downstream. A throughput
collapse would therefore slip *both* the latency gate and the shortfall gate.
The success-ratio gate is the only thing that trips on it. It is the throughput
counterpart to configure-phase's latency — the same latency-plus-throughput
pairing ADR-0035 established for the reclaim path.

**Why ≥ 0.99, held.** Bootstrap is a deterministic local operation in the
harness; at steady state it should essentially always succeed. 0.99 tolerates
the occasional transient (a `ctx_canceled` on a churned machine, a single
fenced write across a reconfiguration) without tolerating a systematic failure
mode. It is a dimensionless ratio, so like the other capacity-delivery bars it
is held identical across the ladder. The denominator includes transient
retries, so if benign churn ever pushes the ratio near the bar legitimately,
the fix is to narrow the denominator, not to lower the gate.

**What a breach means.** Machines are repeatedly failing or retrying Configure —
a materialization throughput collapse that latency and coverage gates would
otherwise hide.

### `operatorNodeStateUpdateP99Seconds` — node-publish latency

**What it measures.** The operator publishing `UpcomingNode = Ready` after the
shard signals a machine Configured (`bigfleet_operator_node_state_update_duration_seconds`,
observed in `pkg/operator/upcoming.go`). It is the last hop BigFleet owns before
the cluster's scheduler can act: "the machine is configured; tell the cluster a
node is coming."

**Why this gate exists.** This was, before ADR-0054, the one BigFleet-owned hop
with *zero* gate coverage — instrumented but never read by the runner. It was
not hypothetical: a prior incident (the "Drop S" conflict on the
Configured-phase write) stuck `UpcomingNode` on `Configuring` for tens of
seconds and produced a ≥102 s tail that only ever showed up, implicitly, inside
the end-to-end pod-bind number. Demoting end-to-end pod-bind to informational
would have *lost* that coverage entirely; gating node-publish directly makes it
explicit and is one of the two hops where ADR-0054 strictly *increases*
coverage relative to the gate it replaced.

**Why 1.5 s (dev 5 s) — and why it is apiserver-write-bound, not operator
logic.** The uber-5k ratification (bigfleet-uber #79) measured this p99 at
**~1.024 s** on a clean engine — at the original provisional 1 s bar. The
investigation that followed is the important part: the `handleNodeStateUpdate`
handler is *trivial in-memory compute* (derive a name + phase, build a spec
struct, equality-check) wrapped around **2–3 apiserver round-trips** (`Get`,
`Create`-or-`Patch`, `Status().Patch`, `Delete` at Drained), all inside a
`RetryOnConflict`. So the latency lives in the cluster's apiserver write path —
a dependency BigFleet does not control — not in operator code. Two facts confirm
it: the measurement did *not* track CPU load (the load-106 satellite had the
*lowest* p99, 0.84 s; the load-57 one the highest, 1.05 s), and BigFleet has
already pulled every lever on the *write count* (skip the re-fetch and no-op
status writes, `Patch`/MergeFrom instead of `Update` to kill the ~26% conflict
rate, delete at Drained to stop apiserver working-set growth) — what remains is
the irreducible minimum. This puts node-state-update in the *same class as
`operatorAck`* (bounded by apiserver status-write QPS), so the bar is sized to
the apiserver-write regime, not to an aspirational sub-second target that would
really be gating the cluster's apiserver. 1.5 s gives ~1.5× over the observed
actual without flaking near the boundary; it is **provisional pending the per-op
split** (M79.8 added a `bigfleet_operator_upcoming_node_op_duration_seconds`
histogram that times each apiserver call separately, so the next clean run
*proves* write-bound vs compute-bound and finalizes the bar). Dev profiles hold
5 s for the kine/SQLite WAL-fsync tail — a substrate property, not a BigFleet one.

**What a breach means.** The operator is slow to publish readiness — apiserver
back-pressure, a conflict-retry loop on the UpcomingNode write, or operator
saturation. Capacity is materialized but the cluster can't see it.

### `shardShortfalls` — demand coverage (the contract)

**What it measures.** `sum(bigfleet_shard_shortfalls)` — the number of demand
units Phase 2 could not resolve at the end of the soak. The gate is `== 0`.

**Why this is the most important gate.** Everything else here is a *quality*
measure of how BigFleet delivers capacity; this is the binary statement of
*whether it delivered at all*. BigFleet's contract (ADR-0045) is precisely
"demand is covered by bound capacity" — it explicitly does not promise pod
placement, so this, not a bind percentage, is the assertion that the system did
its job. It was already a precondition for declaring steady state; ADR-0054
promotes it into the post-soak release verdict as well, because a run that
reaches steady state and then develops a standing shortfall must fail.

**Why == 0 and why it's the anti-gaming guard.** There is no headroom to grant:
a shortfall is unmet demand, full stop. It is also the cheapest, most direct
defense against "reframing the SLO to pass" — no reshaping of which latency
percentile we read can make a standing shortfall look acceptable. (It is also
why the success-ratio gate is needed *alongside* it rather than instead of it:
in-flight crediting can make shortfalls read zero while materialization is
actually failing — see that gate.)

**What a breach means.** Demand the engine accepted is not covered by bound
capacity — a genuine engine failure, the most serious result a run can produce.

---

## Engine-throughput gates (the decision engine keeps up)

These three gates assert that the control loop itself runs fast enough that it
is never the bottleneck. They are retained unchanged from before ADR-0054; the
reframe did not touch them.

### `shardCycleDurationP99Seconds` — decision-engine cycle

**What it measures.** Wall-clock of one `shard.runCycle` — reconcile + the three
decision phases + execute (`bigfleet_shard_cycle_duration_seconds`). It is the
heartbeat of the control loop: how long BigFleet takes to look at the world and
decide what to do once.

**Why this gate, why 5 s.** The cycle must complete comfortably within the
cadence at which demand changes, or the engine falls behind and every
downstream latency inflates. 5 s is an envelope with large, intentional
headroom — #78 measured cycle p99 at ~0.255 s, ~20× under the bar. The headroom
is deliberate: this gate is meant to catch a *structural* blow-up (an
accidental O(n²) over the inventory, a lock-convoy on the hot path), not to
police small variance. A breach means a starved shard — the single most
load-bearing failure mode at scale, since a slow cycle makes every other SLO
worse.

### `operatorRollupP99Seconds` — roll-up pipeline turn

**What it measures.** One turn of the operator's roll-up pipeline — collecting a
cluster's demand and sending the `ClusterCapacityNeeds` message
(`bigfleet_operator_rollup_duration_seconds`). It is how a cluster's demand
reaches the shard at all.

**Why this gate, why 1 s.** The roll-up loop fires every 10 s (the default
`RollupInterval`); one turn must finish well within that interval or roll-ups
queue and demand reaches the engine stale. 1 s is sized to the interval the same
way ADR-0020 sizes bind latency to the rollup cadence: comfortably inside the
structural period, not chasing the floor. Dev profiles loosen to 2 s for the
kine write-tail. A breach means demand signal is arriving late — the engine is
deciding on a stale picture of the world.

### `operatorAckP99Seconds` — roll-up acknowledgement

**What it measures.** The latency for the operator to write acknowledgement
status back (`bigfleet_operator_acknowledge_duration_seconds`) — the
user-visible "BigFleet has seen and recorded my capacity request" signal.

**Why this gate, why 12 s (dev 30 s).** This one is bounded by apiserver
status-write QPS, not by BigFleet's own compute, so its threshold reflects a
realistic write budget against a busy apiserver rather than an aspirational
floor; it tightens if the operator gains batched status writes. It is a held
bar per ADR-0028 (per-request, scale-invariant). Dev profiles loosen to 30 s
because kine's single-writer WAL makes status writes much slower than etcd — a
substrate cost, isolated to the dev threshold. A breach means users wait too
long to see their request acknowledged, even if capacity is being delivered
underneath.

---

## Stability and liveness gates

### `maxReclaimActionsDuringSoak` — reclaim quiescence under churn

**What it measures.** The count of Phase 3 reclaim actions emitted over the
settled soak window. Phase 3 is shrinkage-only; at steady demand it should be
near-inert.

**Why bounded, not zero.** This gate has its own history (ADR-0035's amendment).
The intuitive target is zero reclaims at static demand — but that is
unachievable on the real asynchronous engine: acquisition matures
asynchronously (ADR-0021's ~3-cycle bootstrap dwell), so the controller
perpetually hunts a little around its setpoint, producing a small, irreducible
reclaim floor that a synchronous model never shows. The de-tailed settled rate
is ~0.5–0.86/s, so over a 90 s settled window ~45–77 reclaims is the floor; the
bound is set at ~2–3× that (e.g. 150 on dev-50) — high enough to clear the
floor, far below the thousands a real oscillation produces. It is per-profile
because the floor scales with fleet churn.

**What a breach means.** Reclaim/re-bootstrap thrash well above the async floor
— the M67 oscillation class resurfacing, where Phase 1 and Phase 3 disagree and
the fleet churns capacity it should be holding. This is the gate that has caught
real engine oscillations more than once.

### `endToEndPodBindP50Seconds` — common-path bind liveness

**What it measures.** The *median* of the full end-to-end pod-bind latency
(`Pod.creationTimestamp → spec.nodeName`), as seen by the load-driver.

**Why only the p50, and why it is loose.** This is a deliberately coarse
liveness floor, explicitly *not* the release gate (ADR-0054 Half 2). The full
end-to-end metric's *tail* is dominated by costs BigFleet does not control (see
the informational section), so its p99 is not gateable. But its *median* sits
below the scheduler-retry tail — retries hit the minority of pods — so a p50
blow-up means the *common* bind path actually broke, which is a real liveness
signal worth tripping on. 10 s (dev 30 s for the kine tail) is loose on
purpose: it is a tripwire for "the typical pod stopped binding," not a quality
bar.

---

## Informational metrics (reported, never gated)

These are scraped into `summary.json` every run because they are valuable
context, but they are deliberately **not** gates. Documenting *why* they are not
gates is as important as documenting the gates themselves — each one looks like
it should be an SLO and isn't, for a specific reason.

### `internalBindingLatencyP99Seconds` + `internalBindingLatencyMaxSeconds`

The end-to-end pod-bind p99 (and its non-saturating raw-max cross-check). Under
the default harness, every steady/churn pod is placed by the **real, uncapped
kube-scheduler**, so this interval spans two costs that are not BigFleet's
deliverable: the kube-scheduler's own retry/backoff wait for an unschedulable
pod, and the *reprovision back-edge* — a churn-reclaimed pod cannot bind until a
replacement machine is fully provisioned (Create + bootstrap physics).
bigfleet-uber #78 measured this p99 in the hundreds-to-1300 s range *while
BigFleet's own engine stayed clean* (configure-phase 0.56 s, cycle 0.255 s, zero
shortfalls). Gating BigFleet on that number would gate it on the cluster's
scheduler — exactly the first-principle violation — so we report it as
regime-context and gate the capacity-delivery hops instead. (The raw-max gauge
exists because the histogram's old top bucket, 102.4 s, silently clipped the
true tail and produced a falsely-reassuring "76–102 s" reading in #77; the
non-saturating max makes that failure impossible to miss again.)

### `bigfleet_shard_provisioning_latency_seconds`

A per-(cluster, profile-fingerprint) fan-out latency: first roll-up that
observes a fingerprint's demand → a matching machine reaching Configured. It is
genuinely useful for understanding fan-out, but ADR-0017 keeps it a *diagnostic,
never a gate*: its observe-once-and-delete semantics mean it pins to its top
histogram bucket under sustained churn (it saturated at 327.68 s in #78), and it
is per-fingerprint rather than per-machine, so it is neither continuous nor
per-unit-of-work the way a gateable metric must be. `shardConfigurePhaseP99Seconds`
is the gateable expression of the same underlying materialization cost.

---

## Changing an SLO

Thresholds live in each profile's `slo:` block
(`test/scaletest/profiles/*.yaml`); the gate logic and the default targets live
in `pass()` / `sloOverrides` in `test/scaletest/cmd/scaletest-runner/main.go`.
Adding or moving a gate is an ADR-level change when it alters *which metric* the
release verdict reads (that is what ADR-0054 was); re-tuning a *threshold*
against a fresh measurement is a posture change in the
`maxReclaimActionsDuringSoak` class and only needs the number ratified against
the run that motivated it. Either way the rationale travels with the change —
in the `slo:` block comment, the struct field doc, the gate-failure message, and
here.

A run validates these the usual way (see the [scale-test runbook](scaletest.md)
"validation ladder"): `make prevalidate` exercises the gate logic and the
closed-loop sim Docker-free, the dev-50 integration rung confirms the gates
appear and pass on a real (if small) cluster, and only then does a cloud profile
measure them at scale.
