# ADR-0022: `Need.Count` is Pod count, not machine count — align implementation with the paper

**Status**: Accepted

**Date**: 2026-05-11

## Context

The scaleway-50k Drop M–AA iteration loop fixed every per-stage chain bug we could find (operator-side write conflicts, pod-shim Bind-conflict reclassification, leaked claim labels, leaked UpcomingNode and fake-Node CRDs) and still landed bind p99 at ~23 s after 30 min of soak, climbing linearly from ~6 s at the start. With every measured chain stage flat, the deeper question came up: what does `Need.Count` mean?

Today, `pkg/decision/phase1_assign.go:103-114`:

```go
fromSupply := n.Count
...
deficit := n.Count - fromSupply
// emits `deficit` Bootstrap actions
```

`n.Count` is treated as **the number of machines wanted**. Each unit emits one Bootstrap, one Configure, one machine. `phase3_reclaim.go:148` matches: `remaining: n.Count` is the slack-machine budget. The bigfleet-unschedulable-pod-controller produces one CR per unschedulable Pod, the operator's rollup sums those into `Need{Profile, Count = N}`, and BigFleet provisions N machines. **One Pod → one CR → one machine.**

Both papers describe a different model.

**`fleet-scale-kubernetes.md` §6.1 + §11.** "One CR per pod. Roll-up aggregates." The scaling analysis claims a roll-up message of ~2 KB regardless of fleet size, on clusters with up to 5 000 nodes each. ~2 KB only holds at 5 000 nodes if `count` is **Pod count** post-aggregation (a handful of `CapacityNeed` rows each carrying thousands), not machine count (which would be capped at the node count of 5 000 anyway, but more importantly would lose the "aggregation" property the paper claims).

**`bigfleet.md` §1, §7, §8.** BigFleet "diffs them [`ClusterCapacityNeeds`] against its own provisioned inventory, and provisions or reclaims nodes." The provider's six-RPC contract (`Create, Configure, Drain, Delete, Get, List`) reveals existing machines and actuates the lifecycle on machines whose IDs the shard chose. Phase 1 "walks needs top-down by priority, prefers Idle, falls back to Speculative." Nothing in the decision-engine description says "emit count Bootstraps"; everything in it says "match demand to supply, fill the gap." The supply is whatever the provider reveals through `List`; the demand is whatever the rollup aggregates.

So the model both papers describe — and the model the author confirmed in discussion — is:

> **Providers show BigFleet machines. Clusters via CapacityRequests show BigFleet aggregate demand. BigFleet puts two and two together.**

No new RPCs, no `Catalog()`, no provider-side packing, no Pod-count concept at the provider boundary. Each side reports what it has; BigFleet's Phase 1 diffs aggregate against aggregate.

The implementation drifted. `Need.Count` was implemented as machine count — which works as long as the harness gives every Pod a unique Profile so `Count` is always 1 (the M35 label-axis multiplier does exactly this), and the bug is invisible. As soon as the harness's Profile cardinality is bounded so multiple Pods share a Profile, BigFleet over-provisions by the density factor: 100 CRs of Profile X → `Count = 100` → 100 machines, when the correct answer is whatever number of machines fits 100 Pods' worth of `Profile.Resources`.

## Decision

Align the implementation with the paper.

1. **`Need.Count` is Pod count.** Document this explicitly in `pkg/needs/needs.go` and in the wire proto's `CapacityNeed.count` field. The rollup aggregates by Profile fingerprint and sums into `Count`; one CR (post-aggregation) contributes one unit; the unit is one Pod's worth of `Profile.Resources`.

2. **Phase 1 / Phase 3 compute machine count from aggregate.** Today's `fromSupply := n.Count` line becomes a comparison in aggregate-resource space. For each Need with `(Profile, Count)`:

   - aggregate demand = `Profile.Resources × Count` (vector multiply across CPU / memory / GPU / ephemeral)
   - existing supply = `Σ machine.Allocatable` for matching machines (Configured + Configuring) of that Profile
   - deficit = max(0, demand[dim] − supply[dim]) per dimension
   - machines_needed_from_idle_or_spec = `ceil(deficit / per-machine Allocatable)` taking the bottleneck dimension's count

   The "per-machine Allocatable" comes from the matching machine inventory — for a homogeneous fleet at 1 Profile = 1 instance shape, it's exactly `Profile.Resources × density_factor`. For mixed inventory the shard picks the largest available match. Idle is preferred (the paper's "one bootstrap" tiebreak) over Speculative.

3. **Profile semantics get one clarification.** Today `Profile.Resources` is treated interchangeably as "per-replica request shape" (CR side) and "per-machine shape" (inventory side). They are not the same: a `Profile.Resources = {1 CPU, 4 GiB}` per-replica request fits 16 times on a `{16 CPU, 64 GiB}` machine. Two paths:
   - **Path A** (smaller change): keep `Profile.Resources` as the per-replica shape, add `Machine.Allocatable` separately. The shard's matching loop pairs them. Speculative quota's stored shape is the per-machine `Allocatable`; the coordinator decides those at quota-allocation time (existing surface).
   - **Path B** (purer): split `Profile` into `WorkloadClass` (per-replica resources + requirements + priority + penalties) and `MachineShape` (per-machine `Allocatable`). CRs reference a `WorkloadClass`; machines have a `MachineShape`. Phase 1 matches WorkloadClass demand to MachineShape supply via the same vector math.

   Path A is the minimal change and is what this ADR proposes. Path B is a separate, future refactor if the per-replica / per-machine distinction needs sharper types.

4. **No provider-contract change.** The provider continues to expose machines via `Create` / `Configure` / `Drain` / `Delete` / `Get` / `List`. The shard continues to identify machines by IDs it (or the coordinator) chose. The machine's `Allocatable` is reported as part of `Get` / `List` (it's already implicit in the existing `Machine` schema via `Host`-side resources; the explicit field is the gap to close).

5. **`pkg/decision/phase3_reclaim.go` mirrors.** Slack supply (Configured machines with no matching demand, or matching demand whose aggregate is already covered by smaller-shape supply) becomes the reclaim budget. The same vector math, sign flipped.

6. **Harness shape.** The scaletest harness's M35 label-axis multiplier currently produces ~1 Pod per Profile (everything unique). To exercise the aggregation math, profiles need cardinality bounded so each Profile sees ~100 CRs aggregating into it (matching real-fleet Deployment shapes). The Pod count target stays the same — scaleway-50k still tests 50 K machines — but the realistic-fleet workload behind it is 5 M Pods, expressed via aggregation rather than 5 M individual CRs.

## Consequences

- **Existing chain fixes (Drop M–AA) keep their value.** Conflict retries, GC of UpcomingNode + fake-Node, claim-label hygiene — none of these depended on the count interpretation; they were real bugs at every scale.
- **Bind throughput at the chain's true steady state should land cleanly under the 15 s SLO** once over-provisioning stops. At scaleway-50k with 50 K machines and ~5 M Pods aggregating via ~500 Profiles, BigFleet emits ~50 K Bootstraps total (vs the current ~5 M's worth if Pods had unique Profiles), and the per-Pod bind work flows through the existing chain.
- **Speculative quota allocation becomes more important.** With one machine satisfying many Pods, the coordinator's choice of speculative shapes drives provider cost. This is a future-work item, not in scope here.
- **Wire-proto `count` doc-comment updates.** The field's semantics need to be spelled out clearly; today the proto comment is silent.
- **Test data in `pkg/decision/` flexes.** Many existing tests assume `Count = 1` because they hand-built a Need with one Pod's worth. They keep working under the new semantics (1 Pod × per-replica resources / per-machine allocatable = 1 machine when both shapes are the same). Tests that intentionally exercise aggregation are the ones to update.

## References

- `pkg/decision/phase1_assign.go:103-114` — the lines that drifted
- `pkg/decision/phase3_reclaim.go:144-148` — same, on the reclaim side
- `pkg/needs/needs.go:199-211` — `Profile` struct, today carries the conflated per-replica / per-machine resources
- `docs/papers/fleet-scale-kubernetes.md` §6.1 (one CR per pod), §6 wire proto (`count` field), §11 (scaling analysis pins `count` to Pod count)
- `docs/papers/bigfleet.md` §1 (BigFleet diffs demand against inventory), §7 (`CapacityProvider` is six RPCs, no Catalog), §8 (Phase 1 walks needs top-down, no count-based emission)
- The Drop M–AA iteration thread (commits `d3e7b0f` through `6d6646e`, plus the deferred Drop S/T/AA cluster-side leak fixes that are still in main)
