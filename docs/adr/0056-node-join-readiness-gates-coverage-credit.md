# ADR-0056: Coverage credit should be gated on observed node readiness, not provider-asserted Configured state

## Status

**Proposed**, 2026-06-22. **Author decision required** — this touches the provider contract and/or the shard hot path; do not implement pending greenlight.

## Context

The 2026-06-22 adversarial networking review (`reasoning/networking-criticism-review.md`, finding **S1**) surfaced the one networking-adjacent gap that yields *invisible incorrectness* rather than churn: **BigFleet credits demand coverage on provider-asserted machine state, and never confirms the machine actually joined its target cluster's network and reached kubelet readiness.**

The chain:

1. The coverage walk credits machines by provider-reported state — `{Configured, Configuring}` count toward a cluster's coverage, and [ADR-0052] moved the credit one state earlier still, attributing in-flight `Creating` machines against the deficit to fix the pre-Configuring runway over-acquire.
2. That state is **provider-asserted, not data-plane-observed.** A conformant `CapacityProvider` may report a machine `Configured` the moment its `Configure` RPC returns — i.e. when the VM has booted and userdata has been handed off — without any guarantee that the node got a pod-CIDR, that CNI programmed routes, that kubelet registered against cluster B's API server, or that the node reached `Ready`.
3. The one ground-truth signal that *does* know about real kubelet registration — the operator's `NodeStateUpdate` (node name / `nodeRef` / readiness) — flows **outbound only**: the operator writes it to the shard, and by deliberate design "the shard never reads these back" (wire-protocols). There is no reconciliation of credited coverage against observed node readiness.

The failure mode: a provider that reports `Configured` on VM-boot produces **phantom capacity**. The coverage walk counts it, `shardShortfalls` reads zero, the per-cluster `UpcomingNode` shows the node arriving, and the demand is never re-driven — even though the node never became schedulable. The harm is silent: the operator sees "demand covered" while pods stay `Pending`.

**Why existing gates do not close this:**

- [ADR-0054]'s `bootstrapSuccessRatio >= 0.99` catches a provider that *reports* repeated `Configure` **failures** (a materialization-throughput collapse). It does **not** catch a provider that reports `Configure` **success** for a node that never reached `Ready`. Reported-success-without-join is exactly the blind spot.
- `shardShortfalls == 0` is, by ADR-0054's own admission, "blinded by ADR-0052's in-flight crediting" — a machine counts toward coverage before it materializes, so a coverage gap downstream of the provider's assertion is invisible to the shortfall gate.

**Why this is not a re-run of [ADR-0033]:** ADR-0033 ("Phase 1 supply-credit must respect bind readiness") proposed an operator bind-ready signal to throttle Phase 1 credit, and was **Rejected / superseded by [ADR-0035]** — but on the merits of its *motivation*: the bind plateau that motivated it turned out to be a kube-scheduler **ramp-throughput** artifact under high label cardinality, not a BigFleet behaviour, and "ramp is not an SLO." The readback mechanism was dismissed *with* the throughput misdiagnosis; ADR-0033's own postmortem notes "OC1, OC2, OC3 were all clever answers to a wrong question." **This ADR re-raises the readback for a different, never-evaluated question: not throughput, but *correctness* — preventing phantom capacity from a provider's false-`Configured` assertion.** That correctness question was never asked.

## Decision

**Proposed — choose one of the following. Author decision required.**

### Option A — Provider-contract obligation
`Configure` MUST NOT report `Configured` until the provider has observed the node `Ready` in its target cluster. Verification moves into every out-of-tree provider; the conformance suite must add a real (or faithfully simulated) cluster-join scenario — today it never exercises an actual join, so this gap is untested.
- **Pro:** shard stays simple; no change to the outbound-only node-state property.
- **Con:** trusts every provider to implement and not regress the check; a non-conformant provider silently re-introduces the gap. Pushes a cluster-readiness dependency into the provider, which otherwise need not talk to the target cluster's API server.

### Option B — Shard-side readback
The shard reconciles credited coverage against the operator's observed `NodeStateUpdate` readiness signal. A machine reported `Configured` but not observed `Ready` within a deadline is **not counted as covered** (and the demand is re-driven / the machine flagged).
- **Pro:** ground truth lives where BigFleet can see it; providers stay dumb; correctness is enforced centrally.
- **Con:** reverses the deliberate "shard never reads node-state back" property — adds an operator→shard readback to the hot path. (Note: this is a **data-plane** dependency, operator→shard; it introduces **no** coordinator dependency, so static stability is preserved.) Requires defining the readiness deadline and its interaction with [ADR-0052]'s in-flight credit.

### Option C — Two-phase credit (hybrid)
The provider asserts `Configured` on infra-up (preserving [ADR-0054]'s `shardConfigurePhaseP99` materialization-latency semantics), but **coverage credit** is granted only once the shard confirms node `Ready` via the operator signal. Splits "materialized" (provider truth) from "covering demand" (data-plane truth).
- **Pro:** keeps configure-phase latency fast and honest while making coverage reflect reality.
- **Con:** most moving parts; two notions of "done" to keep coherent with [ADR-0052]/[ADR-0054].

### Recommendation (not a decision)
Lean **B or C**: put the correctness check where BigFleet can observe ground truth rather than trusting every out-of-tree provider to verify and never regress. Either way, pair it with a conformance scenario that exercises a real cluster join, since the current suite does not. Option A alone leaves the guarantee only as strong as the least-careful provider.

## Consequences / open questions

- **Deadline tuning** (Option B/C): how long after `Configured` before an un-`Ready` node is decredited? Must not thrash against legitimate slow joins; interacts with the configure-phase SLO.
- **Interaction with [ADR-0052] and [ADR-0054]:** in-flight `Creating` credit and the `bootstrapSuccessRatio` gate stay; this adds the missing *reported-success-without-join* check between them.
- **Static stability:** any readback must remain operator↔shard (data plane). It must not introduce a `pkg/shard` → `pkg/coordinator` dependency.
- **Scope:** this ADR addresses only S1 (silent join failure). The other networking-review findings — network/egress cost (S2), fabric-label honesty (S3), default network locality (S4), host remanence on reuse (S5), mesh de-registration (S6) — are tracked separately; S2/S3/S6 resolve mostly to documented scope-outs, S4/S5 to their own design briefs.

[ADR-0033]: ./0033-phase1-supply-credit-respects-bind-readiness.md
[ADR-0035]: ./0035-scaletest-slos-at-steady-state.md
[ADR-0052]: ./0052-count-in-flight-provision-commitment.md
[ADR-0054]: ./0054-steady-bind-slo-reframe-for-uncapped-scheduler.md
