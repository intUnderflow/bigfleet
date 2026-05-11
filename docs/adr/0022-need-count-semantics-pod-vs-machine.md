# ADR-0022: `Need.Count` semantics — Pod count vs machine count, and where packing lives

**Status**: Proposed

**Date**: 2026-05-11

## Context

The scaleway-50k Drop-letter iteration loop (Drops M through AA) closed a series of real chain bugs — operator-side write-conflict drops, pod-shim Bind-conflict misclassification, leaked claim labels, leaked UpcomingNodes, leaked fake-Nodes. After all of them the chain still degrades through soak: bind p99 climbs linearly from ~6 s at +5 min to ~25 s at +30 min, with the Configured-machine pool growing 33 K → 46 K over the same window.

While investigating *why* the pool grows, the deeper architectural question emerged: **what does `Need.Count` mean?**

Today, looking at `pkg/decision/phase1_assign.go:103`:

```go
fromSupply := n.Count                  // n is a Need
if fromSupply > s.supplyRemaining {
    fromSupply = s.supplyRemaining
}
...
deficit := n.Count - fromSupply        // emit `deficit` Bootstraps
```

Phase 1 treats `Need.Count` as **the number of machines wanted**. Each unit of `Count` corresponds 1-to-1 with a Bootstrap action and, in turn, with a Configured machine. The same semantics flow through Phase 3 (`pkg/decision/phase3_reclaim.go:148`: `remaining: n.Count`) and the rollup wire-proto's `int32 count = 4` field.

The bigfleet-unschedulable-pod-controller produces one `CapacityRequest` per unschedulable Pod, and the operator's rollup aggregates same-Profile CRs into `Need{Profile, Count = N}`. So the system today says: *N Pods of Profile X → N machines of Profile X*. The 50 K-Pod scaleway-50k profile creates 50 K Pods → 50 K CRs → 50 K Needs → 50 K Configured machines.

That's the **1 Pod = 1 machine** model.

That model is not what real Kubernetes fleets look like. Real production:

- 50 K nodes hosting on the order of **5 M Pods** (typical density ~100 Pods/node)
- A Deployment of 1 000 replicas asking for 1 CPU / 2 GiB each is satisfied by 25 c6a.4xlarge instances at ~40 Pods density, **not** 1 000 separate instances
- kube-scheduler does intra-node packing; the autoscaler decides what new nodes to bring up *and how many of them*, based on aggregate demand and instance economics

The source paper (`docs/papers/fleet-scale-kubernetes.md` §6.1, §6.2, §12) is consistent with the smaller-scope reading: BigFleet is not a scheduler, it does not place pods. But it stops short of stating who decides how many *machines* a given chunk of Pod demand becomes. The current code answers that question implicitly — by treating `Count` as a machine count and never doing any packing math.

The harness's M35 label-axis fingerprint multiplier makes the gap invisible: with every Pod getting a unique Profile, every `Need.Count` is 1 anyway, and the question of "does Count mean Pods or machines?" never bites. As soon as we tune label cardinality so multiple Pods share a Profile (the realistic shape), the current code over-provisions by exactly the density factor.

## Decision

This ADR records the gap and lays out three options. The author signs off on the chosen direction; subsequent ADRs / milestones implement.

### Option A — `Count` stays as machine count; controller does the packing

The wire contract stays. `Need.Count` continues to mean "machines wanted of this Profile." The unschedulable-pod-controller (or whatever upstream produces CRs) is responsible for translating Pod demand into machine demand:

- 1 000 Pods of Profile X, packing density 40/instance → 25 CRs of Profile X, each with `Spec.Count = 1`
- Operator's rollup sums those into `Need{X, Count = 25}`
- BigFleet provisions 25 machines

Implications:
- BigFleet stays out of packing entirely; the controller layer becomes a small bin-packer
- The current controller (`pkg/controller/cr/`) needs that packer added
- The CR CRD doesn't change shape
- Cleanest separation of concerns; biggest jump from current behaviour
- The scaletest harness has to model this — pods don't 1:1 with CRs anymore

### Option B — `Count` becomes Pod count; BigFleet does the packing

The wire contract changes meaning. `Need.Count` becomes "Pods wanting this Profile." Phase 1 computes the machine count itself:

```
density = floor(instance.allocatable / profile.resources_per_pod)
machines_needed = ceil(Need.Count / density)
```

- 1 Pod = 1 CR stays; 1 000 Pods → `Need{Profile, Count = 1 000}`
- Shard's Phase 1 walks Needs, picks an instance type that fits Profile.Resources, computes density, and emits `ceil(Count / density)` Bootstraps
- Reclaim semantics flip the same way: releasing one machine releases `density` Pods worth of capacity

Implications:
- Lowest-overhead at the controller layer (current behaviour, just one CR per Pod)
- Phase 1 / Phase 3 / NeedsTable change to track Pod count and derive machine count on demand
- Instance-type selection now lives in BigFleet (this is the contested boundary — see paper §3)
- The shard knows about Pod resources; AvailableCapacity hints need to expose density too
- The bind chain still binds one Pod per machine — packing only changes Bootstrap counts, not bind behaviour

### Option C — `Count` is a CR-replica multiplier; one CR carries N Pods of work

`Need.Count` means "N replicas of this exact request." Each replica equals one Pod's worth of work for the workload owner, but BigFleet still has to make the call about how many machines to provision.

Practical effect on Phase 1 is identical to Option B (count Pods, derive machines). The difference is in the user-facing CR shape: a CR no longer says "I need a machine," it says "I want to run N of these." Today's controller would generate one CR per Pod with `Count=1`; a smart controller (or a workload owner writing CRs directly, e.g. via Kueue) could collapse to one CR per workload with `Count=N`.

Implications:
- Best alignment with the user's "BigFleet shouldn't care about underlying objects, just the total it must satisfy" framing
- Same shard-side packing as Option B; no separate machine-counting controller as in Option A
- Mildly more disruptive at the CRD layer: a `Count` field needs to be added to `CapacityRequestSpec`, defaulted to 1 for the existing per-Pod controller
- Smooth migration: existing controllers keep producing `Count=1` CRs; new controllers can batch

### Recommendation if forced to choose

Option C is the cleanest articulation of what the user said upstream — "count should be how much to multiply the request by, BigFleet decides the rest." It is a strict superset of today's behaviour (default `Count=1` is the current 1 CR = 1 Pod model). It moves packing into BigFleet, which is consistent with the paper's "decoupled from cluster identity" stance: kube-scheduler places Pods within a cluster, BigFleet decides what *nodes* to spend money on. Packing is a node-sizing decision, not a Pod-placement decision.

But this is the author's call.

## Consequences

Whichever direction is chosen, the following must be addressed:

1. **Wire-proto `count` semantics get a Doc-comment update.** Today the proto comment is silent on Pod vs machine; this ADR's chosen reading needs to be the source of truth.
2. **NeedsTable + Phase 1 / Phase 3 ripple.** Option A: no shard changes. Option B/C: Phase 1's emit math becomes `ceil(Count / density)`, Phase 3's reclaim math the inverse, and the tests around them need to flex.
3. **Instance-type selection.** Option A: no change (controller picks). Option B/C: shard needs to know what instance types are candidates for a Profile and how to compute density. The `CapacityProvider` contract may need an extension (e.g. a `Capabilities()` RPC that reports instance shapes).
4. **AvailableCapacity hint semantics.** Today the hint is one `AvailableCount` per Profile (capacity-side). If Profiles map to multiple machine choices with different densities, the hint needs to either pick a canonical density or expose the per-instance-type breakdown.
5. **Scaletest harness shape.** Whichever option lands, the harness becomes responsible for producing a workload that is *realistic for that model*. For Option C: 50 K nodes / 5 M Pods / N CRs (where N depends on how many distinct Profile classes the workload has) becomes the target shape. The existing M35 label-axis multiplier should be retuned so each Profile has ~100 Pods aggregating into it.
6. **Existing measurements pre-this-ADR are not directly comparable.** The Drop M–AA bind-latency numbers were taken under the implicit 1 Pod = 1 machine model. Once packing density is introduced, the equivalent metric is bind-latency *per Pod*, but the chain's work is per-machine — so we should expect both p99 to drop (fewer Bootstraps per Pod cycle) and absolute bind throughput to climb (chain not bottlenecked on 50 K Bootstraps but on ~500 if density = 100 and Profiles = 500).

## Out of scope for this ADR

- The specific scaletest profile reshape (which axes, what density, what Pod counts) — that belongs in a follow-on ADR or directly in the profile-shaping work, once the count semantics are fixed.
- The packing algorithm's optimality goals (first-fit / best-fit / cost-aware) — for the chosen direction, the simplest reasonable packer ships first; refinements ship as the optimization story plays out.
- Migration story for production CR producers if the wire-proto meaning changes. v1alpha1 → v1beta1 contract bump can defer until the harness validates the new shape.

## References

- `pkg/decision/phase1_assign.go:103` — current `Need.Count` usage
- `pkg/decision/phase3_reclaim.go:148` — same
- `pkg/needs/needs.go:362` — `Need` struct, comment says "two CRs whose Profiles are equal collapse into one Need with Count = 2"
- `docs/papers/fleet-scale-kubernetes.md` §6.1 (one CR per pod), §6.2 (rollup aggregates), §12 (kubectl-scheduler is the scheduler, BigFleet is not)
- ADR-0015, ADR-0016, ADR-0017 — earlier scaletest-realism + per-Pod-binding-latency ADRs that this conversation surfaced as upstream of
