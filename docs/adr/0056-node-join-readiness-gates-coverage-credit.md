# ADR-0056: Coverage credit should be gated on observed node readiness, not provider-asserted Configured state

## Status

**Accepted — Option A (provider-contract obligation)**, 2026-06-22 (author decision, Lucy Sweet).

`Configure` MUST NOT report a machine `Configured` until the provider has observed the node `Ready` in its target cluster, and the conformance suite gains a real (or faithfully simulated) cluster-join scenario that enforces it. Options B (shard-side readback) and C (two-phase credit) were considered and not chosen — see "Decision rationale" below. Implementation pending: provider-contract documentation + the conformance join scenario, with the gating ideally centralised in `providerkit` so every real provider inherits it.

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

### Option A — Provider-contract obligation ← **CHOSEN**
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

### Decision rationale (Option A chosen)

The Proposed draft *recommended* B or C (put ground truth where BigFleet observes it). The author chose **A** instead, for two reasons that outweigh that:

1. **It keeps the boundary clean.** Node-join correctness is the provider's job — the provider is the only component that talks to the cloud/substrate and to the target cluster. Pushing the check there preserves the engine's simplicity and, critically, **does not reverse the deliberate "the shard never reads node-state back" property** (wire-protocols) — no operator→shard readback enters the hot path, and the shard's coverage math is unchanged.
2. **The "only as strong as the least-careful provider" objection is mitigated by a layered enforcement — honestly, not fully closed.** During implementation the six-RPC surface proved to carry **no node-readiness ground-truth signal**, so the in-tree *black-box* conformance suite cannot distinguish a provider that waits for `Ready` from one that reports `Configured` on boot. Enforcement is therefore layered: (a) the **contract** states the obligation; (b) the **conformance suite** verifies the *reference* implementation honours it and ships the gate pattern; (c) **`providerkit`** centralises the wait-for-`Ready` so every provider built on it inherits it; (d) verifying a *specific* real provider against a real cluster is that provider's own integration test. This is weaker than "skip it and you fail certification," and it is the honest cost of keeping the check at the provider rather than reading node-state back into the shard (Option B). The author accepted that trade to keep `pkg/shard` lean and the node-state path outbound-only.

This is consistent with the out-of-tree-provider model (the contract is the surface; conformance + `providerkit` are the enforcement) and with the hard rule that `pkg/shard` stays lean.

### Implementation

**Done (this ADR's commit, `bigfleet`):**

1. **Obligation stated in the provider contract** — `docs/provider-author-guide.md` ("`Configured` means the node has joined and is Ready"): a machine must not be reported `Configured` until the provider observes the node `Ready`; until then it stays `Configuring` (or `Failed` with `last_error` on timeout). Because `ConfigureRequest` carries only `cluster_id` (a name, not credentials), the guide states the two valid mechanisms — out-of-band cluster read-access, or a substrate signal that reliably implies kubelet registration. (A `provider.proto` doc comment is a toolchain-gated follow-up; the author-guide is the authoritative contract surface.)
2. **Conformance** — `TestConformance_NodeReadiness_ADR0056` proves the reference fake holds `Configuring` until a readiness signal and only then reports `Configured`; wired into `make conformance-self`. It is a *reference-fake* test by necessity (the black-box surface has no readiness ground truth — see rationale point 2).

**Done (separate `bigfleet-providers` commit):**

3. **`providerkit` `ReadinessChecker` hook** — an optional `Backend` capability (`ConfirmNodeReady`). When a backend implements it the kit runs it after `ConfigureInstance` succeeds, under the remaining Configure timeout, holding the machine at `Configuring` until it returns nil and driving it to `Failed` on error/timeout — so a kit-based provider inherits the gate centrally. A backend that does not implement it keeps prior behaviour and the kit logs once at startup that the gate is unenforced. The `_template` provider documents how to implement it.

**In review (`bigfleet-providers` PR #22):**

4. **Frozen conformance-catalogue behaviour + re-certification.** `B708` is added to the frozen registry (set 92 → 93). It is kit-level, not per-provider: the reference `faultprovider` implements `ReadinessChecker` with a `fault-readiness-block` selector, and `TestB708` in the shared fault lane asserts the machine stays `Configuring` (never `Configured`) and times out to `Failed`. The fault lane runs once and merges into every provider's certification, so all 12 re-certify at **93/93** with no per-provider code; the ~31 hardcoded `92` count references are bumped in the same PR. Verified locally (fault lane CERTIFIED, "Timeouts & Failure 8/8 passed", 93 total); the full multi-lane matrix runs in CI. (This resolves the earlier concern that cataloguing would un-certify providers — it does not, because B708 passes via the shared lane the moment it lands.)

No `pkg/shard` / coverage-math change is required by this option.

## Consequences / open questions

- **Deadline tuning** (Option B/C): how long after `Configured` before an un-`Ready` node is decredited? Must not thrash against legitimate slow joins; interacts with the configure-phase SLO.
- **Interaction with [ADR-0052] and [ADR-0054]:** in-flight `Creating` credit and the `bootstrapSuccessRatio` gate stay; this adds the missing *reported-success-without-join* check between them.
- **Static stability:** any readback must remain operator↔shard (data plane). It must not introduce a `pkg/shard` → `pkg/coordinator` dependency.
- **Scope:** this ADR addresses only S1 (silent join failure). The other networking-review findings — network/egress cost (S2), fabric-label honesty (S3), default network locality (S4), host remanence on reuse (S5), mesh de-registration (S6) — are tracked separately; S2/S3/S6 resolve mostly to documented scope-outs, S4/S5 to their own design briefs.

[ADR-0033]: ./0033-phase1-supply-credit-respects-bind-readiness.md
[ADR-0035]: ./0035-scaletest-slos-at-steady-state.md
[ADR-0052]: ./0052-count-in-flight-provision-commitment.md
[ADR-0054]: ./0054-steady-bind-slo-reframe-for-uncapped-scheduler.md
