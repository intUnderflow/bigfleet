# ADR-0026: The scaletest harness must model the Speculative tier

**Status**: Accepted

**Date**: 2026-05-14

## Context

The papers describe a **two-tier capacity model** (`bigfleet.md` §5, §8):

| State | Host | Cluster | Meaning |
|-------|------|---------|---------|
| **Speculative** | nil | "" | Quota slot — elastic capacity the provider *could* procure |
| **Idle** | set | "" | Real, *owned* hardware, not bound to a cluster |
| Configured | set | set | Real hardware joined to a cluster |

Phase 1: *"Prefer Idle (one bootstrap). **Fall back to Speculative (Create + bootstrap).**"* The `Create` RPC realizes a Speculative quota slot into real Idle hardware (`Speculative → Creating → Idle`). Speculative is BigFleet's entire **elastic-procurement** story — the paper's *"the autoscaler owns the nodes, it provisions them."* The cost model (`effective_cost = price + interruption_probability × interruption_penalty`) exists to choose *between* tiers and *within* the Speculative tier.

`pkg/provider/fake` implements this faithfully: `AddSpeculative` inserts a quota slot, `Create` does `Speculative → Idle`, and the provider conformance self-test (`test/conformance/selftest_test.go`) exercises it. The `sim/` simulator seeds Speculative (`sim/soak.go`'s `SpeculativeSeed`, `sim/runner.go`'s `InitialSpeculative`).

**But the scaletest harness does not.** `cmd/bigfleet/shard.go`'s `seedFakeInventory` calls `AddIdle` (and seeds Configured machines) — it **never calls `AddSpeculative`**. So in every scaletest run:

- The Speculative pool is empty. Phase 1's `take(StateSpeculative, …)` always returns nothing.
- The `Create` RPC is never exercised.
- The `effective_cost` tiebreak is inert — the Idle seed is `CapacityTypeBareMetal` at `price=0`, so there is nothing to be cheaper *than*.
- **The entire elastic-procurement half of BigFleet's design is dead code in the test.**

The harness models only a **fixed pool of pre-shaped owned hardware.** When demand exceeds, drifts from, or fragments against that fixed pool, BigFleet has nowhere to fall back to → `unsatisfied` → shortfall → the run stalls.

### How the gap was found

A multi-week investigation of dev-500's ramp-gate failures (and the `bigfleet-uber` brief #3/#4/#5 "shard Bootstrap-emit ceiling") peeled back layer after layer — the 4 MiB gRPC ceiling (ADR-0026's companion `e186631`), the non-aggregating roll-up (ADR-0024), the `podAffinity` bootstrap deadlock (ADR-0025), a `Phase 1 machinesNeeded` density bug. Each fix exposed the next. The unifying cause underneath all of them: **the harness only ever had a fixed Idle pool, so any demand it could not directly satisfy from that pool became a permanent shortfall** — which is not how the paper's BigFleet behaves. Notably, the `machinesNeeded` density fix — correct in isolation — *regressed* dev-500, because the bug it fixed (Phase 1 over-asking `take` by ~density×, grabbing the whole fixed Idle pool up front) was functionally *load-bearing harness headroom*. Remove the over-grab and there is no elastic tier behind it.

How it crept in: M29 made the seed "Configured/Idle-heavy" to model an already-provisioned fleet — sound *if the Idle seed shapes ⊇ the demand shapes*. M34 then intentionally introduced seed-vs-demand drift ("real fleets show drift"). M34 broke M29's "Idle seed alone is sufficient" assumption but added no Speculative tier to absorb the drift. The gap has been latent since; the `machinesNeeded` over-provisioning bug hid it.

## Decision

**The scaletest harness models both capacity tiers.** `seedFakeInventory` seeds a Speculative pool alongside the Idle and Configured seeds:

1. **New `--seed-speculative N` shard flag** and `shard.seedSpeculative` chart value, plumbed like the existing `--seed-machines`. **Default non-zero** — every scaletest profile exercises both tiers; there is no "fixed-pool-only" regime, because the paper has no such regime.

2. **Speculative slots are drawn from the demand archetype catalog** — the same generator (`archetype.Picker` + `PickSize` + zone) the Idle seed and the load-driver use — so the elastic pool realistically spans the shapes workloads ask for. Sized **generously** (a multiple of the demand machine-count): the realistic model is a cloud provider with abundant capacity of the common shapes, not a pool enumerated to match the workload exactly.

3. **Speculative slots are `CapacityTypeOnDemand` with a non-zero `PricePerHour` and a small `InterruptionProbability`** — so `effective_cost` is meaningful and Phase 1 correctly *prefers* the (cheaper, owned) Idle tier and falls back to the (priced, elastic) Speculative tier. This is also what `sim/` does.

The shard's `reconcile` already pulls provider Speculative machines into shard inventory via `Provider.List` (any state) at the start of every cycle, so no shard-side wiring is needed beyond the seed.

## Consequences

### What this corrects

- BigFleet's elastic-procurement path — `Create`, the Speculative pool, the `effective_cost`-driven tier choice — is finally exercised. The harness covers the *whole* design, not half of it.
- The `Phase 1 machinesNeeded` density fix becomes shippable: Phase 1 provisions the minimal-correct Idle count and falls back to a *real* Speculative tier for drift and fragmentation. The two changes are complementary and land together.
- The ADR-0024/0025 co-location work composes correctly: a `Same(rack)` Need that can't be filled from co-located Idle can `Create` co-located Speculative slots.

### What it means for past results

Every "scale ceiling" in the `scaletest-results` page and the `bigfleet-uber` briefs was, at root, **the fixed Idle seed running out** — not a shard, Phase 1, or operator bottleneck. Those numbers measured the harness's missing tier, not BigFleet's limits. Re-baselining against the two-tier harness is required before any ceiling number is published as a BigFleet property.

### What stays the same

- BigFleet itself — `pkg/shard`, `pkg/decision`, `pkg/operator`, `pkg/provider/fake` — is unchanged. Phase 1 already does the Idle-then-Speculative fallback; the fake provider already implements `Create`/`AddSpeculative`. This ADR is purely a harness-seeding fix.
- The Idle and Configured seeds are unchanged. Speculative is *added*, not substituted.

### Known adjacent gaps (not addressed here)

- **`Idle → Speculative` release is unimplemented.** The state machine has `Idle → Deleting → Speculative` (the paper's "Idle → Speculative lazily per provider" — cloud machines released back to quota), but the decision engine emits no `ActionKind` for it. Reclaimed machines stay Idle (correct for bare metal; the cloud-release path is simply missing). For bounded scaletest runs this is fine — the Speculative seed only depletes on *net-new* demand, and reclaimed machines replenish the Idle pool. Indefinite soaks would eventually need the release path wired; tracked separately.

## Alternatives considered

- **Just make the Idle seed bigger.** Rejected: it doesn't model the paper's two-tier design, leaves `Create` and the cost model untested, and the published numbers would still measure a fixed pool. It's the over-provisioning bug, formalised.
- **Enumerate the demand fingerprint space and seed exactly-matching Speculative slots.** Rejected as less realistic — a real cloud's available capacity isn't enumerated to match one workload; it's abundant capacity of the common shapes. Generous random-draw from the demand catalog is both simpler and more faithful; the small residual tail of uncoverable rare fingerprints is itself realistic (genuine capacity stockouts).

## References

- `bigfleet.md` §5 (machine model — Speculative/Idle/Configured), §8 (Phase 1 Idle-then-Speculative).
- `fleet-scale-kubernetes.md` §6.2 (`AvailableCapacity` — the provider's declared procurable capacity).
- ADR-0022 (`Need.Count` is Pod count; the density model the `machinesNeeded` fix restores).
- ADR-0024, ADR-0025 — earlier layers of the same investigation.
- `sim/soak.go`, `sim/runner.go` — the existing correct pattern for seeding Speculative.
- `test/conformance/selftest_test.go` — provider conformance coverage of `Create`/Speculative.
- `bigfleet-uber` briefs #3–#5 (private) — the empirical "ceiling" data this ADR re-explains.
