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

### Option D — drop `Count` entirely; demand is expressed as aggregate resources by class

A clarification of B/C the author proposed in discussion. The previous three options all keep `Count` in some form. Option D removes it.

The clean re-framing: **BigFleet only cares that "Cluster A wants 5000 CPUs of class X, 400 GiB of memory of class Y, N GPUs of class Z"**. The class is the Profile (architecture, memory type, GPU type, locality). The amount is an aggregate `ResourceList`, summed across all CRs of that Profile at rollup time. Pod count, replica count, "how many CRs" — none of these are visible to the shard. The shard only sees totals.

CR contract:
- `Spec.Resources` is the **per-replica** resource request (used at rollup time + as the floor when the shard picks an instance type — every machine must fit at least one of these)
- No `Count` field. One CR is one Pod's worth of resources at this point in time. (Or in a future Kueue-shaped batch controller, one CR carries one batch's worth, summed into the rollup the same way.)
- Pod creation, churn, lifecycle — all stays in the cluster operator's world

Operator rollup contract:
```
message CapacityNeed {
  Profile profile          = 1;  // class: arch, memory-class, GPU-class, locality
  ResourceTotal aggregate  = 2;  // sum of Spec.Resources across all matching CRs
  int32 priority           = 3;
  // penalties, topology, etc.
}
```

Shard's Phase 1, on each Need:
1. Compute existing supply = sum of `machine.Allocatable` across Configured machines with matching Profile
2. If supply ≥ demand on every resource dimension, no provisioning — Phase 3 may even reclaim slack
3. Otherwise call `provider.Provision(profile, demand - supply)` — provider returns N machines with their actual Allocatable shapes; shard tracks them through the normal Idle/Configuring/Configured state machine

Reclaim: same calculus reversed. Supply exceeds demand by more than `reclamation_slack` for long enough → pick reclaim victims by `reclamation_penalty`, release until supply ≈ demand.

Provider contract: **the provider owns instance-type selection.** It is the only thing that knows its catalogue, its pricing, its cross-resource constraints. BigFleet's RPC becomes "make sure I have at least this much aggregate of this class"; the provider answers with whatever shapes it produces.

This is a strict subset of today's contract — instead of `Create(profile)` (today, one machine per call), it's `Provision(profile, aggregate)` (provider decides shape + count). The existing six-RPC surface (`Create, Configure, Drain, Delete, Get, List`) stays the same; only `Create`'s shape grows an aggregate parameter and a list-of-machines return. No `Catalog()` RPC needed — BigFleet never needs to *enumerate* instance types, only to *consume* the machines the provider returned.

Implications, distinct from B/C:

- **No Pod-count concept anywhere in BigFleet.** Aligns with the user's framing exactly: "BigFleet shouldn't care about underlying objects, just the total it must satisfy."
- **Heterogeneous Profile resources are fine.** With aggregates, a Profile that includes a mix of "2 CPU 4 GiB" pods and "1 CPU 8 GiB" pods just sums to "3 CPU 12 GiB" demand — the shard hands the aggregate to the provider. With B/C's `Count`, you'd have to either split into two Profiles or pick a representative per-replica shape.
- **Multi-dimensional supply/demand math.** Today's `Count` math is scalar. Aggregate math is vector (CPU, memory, GPU, ephemeral storage). The shard does the *comparison* across resource dimensions ("is supply ≥ demand on every axis?"). The *packing* (which instance types, how many) is the provider's concern — vector bin-packing belongs in the layer that knows the instance catalogue, not in BigFleet.
- **AvailableCapacity hint becomes aggregate-shaped.** Hint says "I can give you up to N CPUs / M GiB of class X with confidence H" instead of "AvailableCount: K machines of Profile P." The provider produces this without committing to a specific instance type.
- **Cleanest paper alignment.** §3 ("Capacity is decoupled from cluster identity") and §6.1 ("One CR per pod. Roll-up aggregates") both feel less strained: the rollup *actually* aggregates into something the shard can use directly, and the provider boundary is "BigFleet doesn't know about instance types" — out-of-tree providers stay genuinely out of tree, with strictly more autonomy than they have today.

### Recommendation

Option D. It's the cleanest match for "BigFleet shouldn't care about Pods or CRs, only the resources to provision," and it makes B/C feel like halfway houses — both still leak Pod-count into the shard's worldview, just at different layers. The cost is bigger (vector packing + provider-catalogue RPC) but the resulting model is closer to what real cloud autoscalers do under the hood and admits cleaner extensions (cost optimization, heterogeneous fleets, mixed-instance-types per Profile).

Options A / B / C are documented above as alternatives the author considered before D was articulated. They are not deleted because the discussion that produced D is in them.

## Consequences

Whichever direction is chosen, the following must be addressed. Option D's column is what the recommendation calls for:

1. **Wire-proto `count` field.** A: stays as machine count. B/C: stays, semantics shift to Pods. D: **removed; replaced by an aggregate `ResourceTotal`**. The CRD's `CapacityRequestSpec.Resources` stays as the per-replica request.
2. **NeedsTable + Phase 1 / Phase 3 ripple.** A: no shard changes. B/C: scalar `ceil(Count / density)`. D: **vector supply-vs-demand comparison** ("is sum of Allocatable across matching machines ≥ aggregate demand on every axis?"). Provisioning gap → call provider; reclaim slack → drain victims. Shard never multiplies.
3. **Instance-type selection.** A: controller. B/C: shard, scalar density. D: **provider does it.** Existing `Create` RPC's request grows an aggregate-demand parameter and its response grows to a list of machines (each with their actual Allocatable). No `Catalog()` RPC — BigFleet never enumerates instance types, only consumes the machines the provider returned.
4. **AvailableCapacity hint semantics.** A: unchanged. B/C: per-Profile capacity in Pods. D: **per-Profile capacity in aggregate resources** ("I can do up to 5 K CPU / 20 TiB of class X at confidence High"), no count, no instance-type breakdown.
5. **Scaletest harness shape.** Whichever option lands, the harness produces a workload realistic for that model. For D: 50 K nodes / 5 M Pods is the demand shape, expressed as aggregate `ResourceList` per Profile (e.g. 500 Profiles × 10 K CPU each across 50 clusters). The current M35 label-axis multiplier turns into a Profile-cardinality knob; the Pod-count knob exists separately and feeds the rollup's aggregate.
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
