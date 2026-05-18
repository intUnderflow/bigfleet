# ADR-0033: Phase 1 supply-credit must respect bind readiness, not just provider state

## Status

Proposed, 2026-05-18.

## Context

[ADR-0029] redesigned Phase 1 as an Omega-style OCC. The pre-pass
`SeedConfiguredSupply` (`pkg/decision/occ/seed.go`) walks each
priority-sorted Need's matching `Configured` and `Configuring`
machines and subtracts `EffectiveAllocatable` from the Need's
`AggregateResources` until either supply exhausts or demand is
covered. Anything left over becomes the Need's deficit, which the
OCC worker pool then tries to satisfy from `Idle` or `Speculative`
inventory.

The pre-pass treats a `Configured` machine as supplying its **full
nominal allocatable** the instant it reaches Configured state. That
matches the provider's view of reality and was correct semantics
until the realistic catalog (ADR-0032) exposed the next layer of
the system: kube-scheduler's bind rate is the steady-state gate, not
Phase 1.

The empirical chain that surfaced this (see
`project_lessons_learned.md` "M46 OCC validation"):

1. **uber-5k bigfleet-uber #20** at SHA `c955a0d` reported 99.1 %
   bind ramp on the realistic catalog. Looked like an OCC win.
2. **#22–#25 follow-ups** showed the 99.1 % was a confound: a
   `Provision → Idle` race in
   `pkg/shard/reconcile.go:applyReconciledMachine` was forcing
   ~25 % of Provisions to fire twice. Those redundant Provisions
   accidentally kept the bind path fed.
3. **M48.1 (commit `10986a6`)** closed the race. Ramp dropped to
   ~44 % on the same SHA family because Phase 1 now provisions
   to *exact* nominal demand.
4. **bigfleet-uber #25 diagnosis** identified the actual bind-path
   gate at uber-5k 2-host: ~30 Pods/s aggregate, driven by
   UpcomingNode write rate to inner kwok apiservers (operator-side
   ~0.84/s aggregate creates).
5. **M48.4 (commit `29c7a2e`)** dropped operator-side
   `status_update` conflicts from 0.29/s → 0/s via `client.MergeFrom`
   patches, and cut NodeStateUpdate Ready p99 by 69 % — but the
   ramp ceiling stayed at 42.8 % because Phase 1 is *also* part of
   the gate.

The mechanism is simple:

- **What Phase 1 sees**: N `Configured` machines, each contributing
  e.g. 52 Pod-shaped slots → 52N capacity available right now.
- **What kube-scheduler can actually do**: bind at ~30 Pods/s
  system-wide, capped by NodeStateUpdate write throughput
  downstream of BigFleet.
- **Net effect**: once enough `Configured` machines exist to cover
  *aggregate* demand, Phase 1's `absorbed_by_supply` outcome
  dominates and `emitted_spec` falls to ~0/s. Provisioning stops.
  But binding is still proceeding at 30/s, so many cycles later the
  bind path is still chewing through the queue while no new
  capacity arrives. The system ramps to ~43 % bind-fraction-of-
  demand and plateaus because that's what aggregate demand divided
  by the time the system was provisioning happens to converge to.

The race accidentally fixed this by producing ~25 % more
`Configured` machines than strictly necessary; closing the race
exposed the real bug, which is that **`Configured` machines should
not contribute their full allocatable to Phase 1's
supply-credit until the bind path has demonstrated it can consume
that supply.**

`bigfleet.md` §8 specifies Phase 1 as "walk needs top-down by
priority. Prefer Idle. Fall back to Speculative." Neither the
paper nor [ADR-0029] is wrong about Phase 1 — what's wrong is the
pre-OCC `creditExistingSupply` heuristic that was carried into
[ADR-0029]'s `SeedConfiguredSupply` unchanged. The heuristic is fine
for an inviscid bind path; it over-credits whenever bind is
viscous.

## Goals

1. **Eliminate steady-state over-credit.** When the bind path is
   the gate, Phase 1's `absorbed_by_supply` outcome must
   under-count by exactly the amount of `Configured` capacity that
   has not yet been demonstrated bindable. Phase 1 should continue
   to `emit_spec` until either demand is covered *or* the realised
   bind-ready supply covers it.
2. **No regression at cold-start.** A fresh cluster with zero
   `Configured` machines must still emit Provisions for every
   priority-sorted Need exactly as today.
3. **No regression in the inviscid regime.** A bind path that
   isn't the gate (e.g. dev-50, small uber-5k catalogs) must still
   credit `Configured` machines and avoid double-provisioning.
4. **Preserve [ADR-0027] stage 5.1 attribution.** Phase 1 and
   Phase 3 must continue to attribute supply identically; if Phase 1
   stops crediting an unconfirmed-bindable machine, Phase 3's
   reclaim mirror must also stop crediting it.
5. **Preserve the cost formula and hard rules.** No pluggable
   cost. No cluster-side interruption override. The change is
   purely to *which* machines participate in supply-credit, not how
   their cost is computed.

## Non-goals

- **Increasing the bind path's throughput.** That's downstream of
  BigFleet (kwok / kube-scheduler / kine / apiserver). M44.4 already
  iterated on the harness side; further work belongs in the
  scaletest harness, not BigFleet itself.
- **Adapting Phase 1 to per-cluster bind-rate signals.** Phase 1
  is one shard's view; cross-cluster bind rates aren't directly
  observable from the shard. The signal we adopt must be a property
  of the individual machine, not an aggregate rate.
- **Changes to `bigfleet.md` §8 or [ADR-0029]'s OCC structure.**
  Only `SeedConfiguredSupply`'s admission rule changes.
- **Changes to Phase 2 / Phase 3 victim selection** — only the
  attribution data they read.

## Decision

**Adopt OC1: bind-ready supply-credit.** A `Configured` machine
contributes to `SeedConfiguredSupply` only after the cluster
operator confirms the machine's `UpcomingNode` has reached
`Phase = Ready` (i.e. the fake or real Node exists and
kube-scheduler can place Pods on it). Until then the machine
exists, is in `Configured` state, and is owned by a cluster — but
Phase 1 treats it as having zero available capacity for the
supply-credit pre-pass. The OCC worker pool continues to find
candidates from `Idle` / `Speculative` and Provisions as needed.

### Why OC1 over OC2 / OC3

- **OC2 (time-since-Configured rate gate)** — credit
  `min(EffectiveAllocatable, k × age)` where `k` is a per-cluster
  bind-rate constant. Cheaper to implement (Machine gains a
  `ConfiguredAt time.Time`; no stream message changes) but
  fundamentally a proxy: a machine that takes 60 s to become
  bind-ready and one that takes 2 s are treated identically. The
  constant `k` is also workload-dependent — uber-5k binds at 30/s
  system-wide; uber-50k won't be the same. Picking a constant we
  have to retune at each rung is exactly the kind of speculative
  configurability CLAUDE.md / `feedback_yagni.md` rules out.
- **OC3 (slack factor)** — Phase 1 over-requests by N % to recreate
  what the race accidentally provided. Simplest to implement but
  violates the "Phase 1 emits to exact demand" invariant the rest of
  the system relies on (e.g. Phase 3's reclaim mirror, ADR-0027
  stage 5.1 attribution). It also has no principled stopping point:
  the right N depends on bind rate vs demand rate at each scale.
  Slack factor is the answer if the bind path were a fixed
  bottleneck we accept; it isn't — it's a property we want to
  *observe through to Phase 1*, not paper over.

OC1 is the only option that closes the loop with a real signal
rather than a tuning knob. The signal already exists in the
operator's UpcomingNode reconciler (we know when Phase becomes
Ready — that's exactly what M48.4 was about); we just don't
currently reflect it back to the shard.

### Mechanism

1. **Machine gains a `BindReady bool` field.** Persists across
   reconcile cycles. False by default; set true once the operator
   confirms UpcomingNode.Status.Phase = Ready.
2. **New stream message: `NodeBindReady`** (operator → shard,
   multiplexed on `Shard.Session` per the hard rule of no inbound
   listeners on the operator). Carries `machine_id` and a
   `confirmed_at` timestamp. Idempotent — re-delivery is a no-op.
   `supersedes_key = machine_id` per the §0.1 coalescing rule.
3. **`SeedConfiguredSupply` admission rule**: skip any
   `Configured` machine where `BindReady == false`. `Configuring`
   machines remain admitted as today (they're already pre-credit
   for the in-flight Configure RPC; double-restricting would
   regress the inviscid path).
4. **Phase 3 reclaim mirror**: applies the same admission rule.
   A non-BindReady `Configured` machine cannot be reclaimed via
   the supply-credit attribution path; Phase 3 sees it as
   "claimed by no Need" exactly as Phase 1 does, and Phase 3's
   existing logic for orphaned-Configured handles it.
5. **Reconcile interaction**: BindReady is set by the operator
   stream, not by `provider.List`. `applyReconciledMachine`
   preserves the existing `BindReady` value across reconciles
   (analogous to today's `AssignedPriority` /
   `AssignedInterruptionPenaltyDollars` preservation in
   `reconcile.go:126-129`).
6. **Failure / cluster-loss handling**: if the operator
   disconnects, BindReady is *not* cleared — the machine's bind-
   readiness in the Kubernetes data plane is independent of the
   stream's liveness. If a machine transitions out of Configured
   (Drain, Delete, Fail), BindReady is cleared as part of the state
   transition.

### Cold-start behaviour

A fresh cluster has zero `Configured` machines, so
`SeedConfiguredSupply` credits nothing, and Phase 1 Provisions
every Need from `Idle` / `Speculative`. As Configures complete and
the operator confirms Ready, BindReady flips true machine-by-
machine and supply-credit ramps with bind capacity. This is the
desired behaviour and matches what the race was accidentally
producing.

## Validation

Two-rung validation, mirroring [ADR-0029]'s pattern:

1. **uber-5k 2-host**: bind ramp must reach ≥95 % within the
   realistic-catalog budget. The 42.8 % ceiling observed at
   `29c7a2e` is the regression bar; anything below ~95 % is a
   failure of the design, not a tuning issue.
2. **uber-50k**: bind ramp must reach ≥95 %; per-cycle wall-clock
   p99 ≤ [ADR-0028]'s envelope. Per the uber ladder discipline
   (no pre-filing above the in-flight scale), runs at uber-500k+
   wait for Uber approval before the validation arc continues.

`absorbed_by_supply` rate must remain non-zero in steady state
(i.e. the bind-ready pool eventually catches up and the system
reaches an equilibrium where some cycles credit existing supply
rather than emitting). A pathology where `absorbed_by_supply`
stays at 0 indicates the bind path is permanently capped — that's
a downstream problem, not a Phase 1 regression.

## Alternatives considered

### OC2: time-since-Configured rate gate

`SeedConfiguredSupply` credits `min(EffectiveAllocatable, k ×
age)` where `k` is a configurable bind-rate constant per cluster.

**Pros**: no new stream message; no operator-side change.
Implementation is one new field on Machine
(`ConfiguredAt time.Time`) and a few lines in `seed.go`.

**Cons**: `k` is a per-workload tuning knob the operator can't
introspect. Wrong `k` either over-provisions (low `k`) or under-
provisions (high `k`); the latter just reproduces today's bug at
a different rate. No principled way to set the default.

### OC3: deliberate slack factor

Phase 1 multiplies each Need's `AggregateResources` by `1 + slack`
(e.g. 1.25 to recreate the ~25 % race effect) before passing to
`SeedConfiguredSupply`. The extra demand spills past available
supply and Phase 1 emits Provisions for it.

**Pros**: smallest diff. No new field, no new message. One
multiplication.

**Cons**: violates "Phase 1 emits to exact demand"
([ADR-0027] / [ADR-0029] both assume this). Breaks Phase 3
attribution unless Phase 3 also adopts the same factor —
[ADR-0027] stage 5.1 invariant is fragile to asymmetric supply
accounting. The right `slack` is workload-dependent and would
need re-tuning at every uber-* rung. It's a knob, not a fix.

### Status-quo + downstream tuning

Leave Phase 1 alone; bump UpcomingNode write concurrency, kine
write throughput, etc. until the bind path is no longer the gate.

**Pros**: respects layering — Phase 1 isn't the proximate cause.
**Cons**: the harness limits we'd be tuning around are
properties of the scaletest infrastructure (kwok / kine /
apiserver), not BigFleet. We don't ship those. And the next user
to run BigFleet against a slow customer apiserver hits the same
wall.

## Hard rules touched

This ADR adds one new stream message (`NodeBindReady`) on
`Shard.Session`. Per the §0.1 decisions:

- Operator remains outbound-only — the new message is
  multiplexed on the existing operator-initiated bidi stream, not
  a new RPC.
- `supersedes_key = machine_id` per the coalescing rule, so
  reconnect ordering is safe.
- Conformance: the message is a hint, not a correctness
  requirement. A provider / operator that doesn't emit
  `NodeBindReady` simply has every `Configured` machine stay at
  `BindReady = false` → behaviour reverts to "Phase 1 always
  Provisions, never credits". That's the cold-start path; it's
  correct, just under-efficient. Conformance suite covers the
  emit path; absence is not a failure.

No other hard rule is touched. Cost formula unchanged. Provider
RPC surface unchanged (this is operator ↔ shard, not provider).
Static stability preserved (BindReady is set by the operator
stream, but its absence falls back to "Provision more" — the
shard remains autonomous during coordinator failover and during
operator disconnect).

## Migration plan

Layered behind a `BindReadyCredit` shard config flag, default
`false` in the first commit. Once validated at uber-5k and
uber-50k, flip to default `true` and remove the flag in a
follow-up.

1. **Stage 0**: Wire format. Add `NodeBindReady` to
   `shard.proto`'s `Shard.Session` message types.
2. **Stage 1**: Domain types. Add `Machine.BindReady bool`;
   plumb through proto conversion in `pkg/api/conv`; preserve
   across reconcile.
3. **Stage 2**: Operator emit. When the UpcomingNode reconciler
   observes Status.Phase → Ready, emit `NodeBindReady` on the
   shard stream.
4. **Stage 3**: Shard ingest. Handle `NodeBindReady` in the
   shard's stream loop; flip `BindReady = true` on the inventory
   machine. Idempotent.
5. **Stage 4**: `SeedConfiguredSupply` admission rule, gated on
   the `BindReadyCredit` config flag. Phase 3 mirror.
6. **Stage 5**: uber-5k bigfleet-uber brief; validate ≥95 % ramp.
7. **Stage 6**: uber-50k bigfleet-uber brief; validate ≥95 %
   ramp at scale.
8. **Stage 7**: Default the flag to `true`. Remove the
   conditional after one more clean validation pass.

uber-500k+ remains gated on prior Uber approval per the standing
policy (`project_uber_scale_ladder.md`).

## References

- [ADR-0027] Roll-up demand is a constrained aggregate resource
  request.
- [ADR-0028] Cycle-p99 SLO is regime-parametric.
- [ADR-0029] Phase 1 Omega-style OCC.
- [ADR-0032] Realistic catalog production-calibrated workload
  distribution.
- `project_lessons_learned.md` § "M46 OCC validation: c955a0d's
  99.1 % 'pass' was race-induced over-provisioning".
- bigfleet-uber #25 (bind-path gate diagnosis) and #26 (M48.4
  validation).

[ADR-0027]: ./0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0028]: ./0028-cycle-p99-is-regime-parametric.md
[ADR-0029]: ./0029-phase1-omega-style-occ.md
[ADR-0032]: ./0032-realistic-catalog-production-calibration.md
