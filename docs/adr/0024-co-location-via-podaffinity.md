# ADR-0024: Co-location via podAffinity — the `CoLocation` CR field, roll-up aggregates

**Status**: Accepted

**Date**: 2026-05-14

## Context

The papers are explicit that the roll-up is small and bounded:

- `fleet-scale-kubernetes.md` §6.1: *"One CR per pod. **Roll-up aggregates.**"*
- §11: *"**Roll-up message ~2KB regardless of fleet size.**"*
- `bigfleet.md` §3.1 / §7: roll-ups are full-replacement; the operator translates CRDs to protobuf, *"adding `Same` requirements where co-location is needed"* (§8).

In practice the roll-up was **O(unschedulable-pods)**, not ~2KB. The devpod-5k validation on Uber infrastructure (bigfleet-uber issues #3/#4) showed the shard's NeedsTable frozen at ~49K Needs while 250K+ CapacityRequest CRs were outstanding — Phase 1 correctly emitting almost nothing because the demand never arrived.

Two compounding causes:

1. **The shard's gRPC server inherited the library's 4 MiB `MaxRecvMsgSize` default.** A full-replacement roll-up that crossed 4 MiB made the shard's `Recv` return `ResourceExhausted` and tore the `Shard.Session` stream down; the operator reconnected, re-sent the same oversized roll-up, failed again — a silent reconnect loop. `OperatorRollupDuration` times the *build*, not the *send*, so the failure was invisible. Fixed in commit `e186631` (256 MiB ceiling on all servers + clients via `pkg/grpcutil`) — a safety net so a large roll-up degrades into a slow cycle, never a broken session.

2. **The roll-up never aggregated.** `pkg/operator/rollup.go`'s `coLocationGroup()` read `cr.OwnerReferences[0].UID`, and the unschedulable-pod-controller (UPC) sets that ownerRef to the **Pod itself** (correctly — it is the CR's GC owner). So every CR landed in a unique co-location group, `needs.Aggregate` keyed on `(cluster, fingerprint, group)` merged nothing, and N unschedulable pods produced N Count=1 Needs. A 50K-replica Deployment would emit 50K separate Needs instead of one with `count=50000`. This is the cause that matters: the gRPC limit bump alone just moves the ceiling.

The `coLocationGroup` doc-comment claimed *"conventionally the first ownerRef is the workload"* — but the UPC's actual behaviour makes that false. The co-location-by-owner mechanism was effectively dead: every pod was its own group, so `Same` was never meaningfully emitted, and aggregation never happened.

## Decision

**Derive co-location from the pod's `podAffinity`, carried on the CR as a structured `CoLocation` field, translated to `Same` by the operator at roll-up.**

`podAffinity` is the native Kubernetes way a workload declares "schedule me with my peers". Using it means **zero user-facing change** — no BigFleet-specific annotation a workload owner must add to adopt the autoscaler. It is exactly the "co-location signal" the paper's §8 refers to.

Concretely:

1. **New CRD field — `CapacityRequestSpec.CoLocation *CoLocationTerm`.** Symmetric to the existing `TopologySpread` field (spread is the dual of co-location). `CoLocationTerm` is `{LabelSelector *metav1.LabelSelector, TopologyKey string}` — the autoscaler-relevant projection of one required `podAffinity` term. Optional; absent means "no co-location".

   The CRD continues to use only standard node-selector operators (`In`/`NotIn`/`Exists`/`DoesNotExist`). `CoLocation` is structured intent, not the `Same` operator — `Same` stays protobuf-only, emitted by the operator at roll-up. This keeps the hard rule intact while making the contract explicit, so non-UPC CR sources (Kueue, custom controllers — the UPC is optional per `fleet-scale-kubernetes.md` §12) can express co-location too.

2. **UPC translates `podAffinity` → `CoLocation`.** `buildCRForPod` reads `pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]` and maps its `LabelSelector` + `TopologyKey` onto `Spec.CoLocation`. No required `podAffinity` → `CoLocation` nil. The CR's ownerRef stays the Pod (GC contract unchanged).

3. **Operator derives the aggregation group and the `Same` topology key from `CoLocation`.** `coLocationGroup(cr)` returns a canonical serialization of `Spec.CoLocation` (sorted label selector + topology key), or `""` when `CoLocation` is nil. When non-empty, the operator appends a `Same` requirement keyed on the term's own `TopologyKey`.

   This retires the operator-global `Config.CoLocationKey`: the topology granularity is a per-workload property of the affinity term, not an operator-wide constant.

### Resulting behaviour

- Pods with no `podAffinity` (the overwhelming common case — plain Deployments, Jobs, bare pods) → empty group → CRs aggregate purely by profile fingerprint → ~2KB roll-up, as the paper requires.
- Pods of one co-located workload (same `podAffinity` term) → same group → aggregate into one Need *and* carry a `Same` on the term's topology key.
- Two independent workloads with the same profile shape but different affinity selectors → different groups → stay as separate Needs, each co-located onto its own domain. They are never merged onto one rack.

## Scope

- **Required `podAffinity` only.** `preferredDuringScheduling…` (soft affinity) is advisory and out of scope.
- **First required term only**, matching the existing `requirementsFromPod` treatment of node affinity ("multiple terms / OR semantics are out of scope for v1").
- **`podAntiAffinity` is out of scope.** Anti-affinity is the dual of co-location — conceptually closer to `TopologySpread` — and is not addressed here.

## Consequences

### What we gain

- The roll-up aggregates, and is ~2KB regardless of fleet size — the paper's stated property, now actually true.
- Zero user-facing change. Workloads that already use `podAffinity` get co-location for free; workloads that don't are unaffected.
- The co-location contract is explicit on the CRD, usable by any CR producer, not just the UPC.
- `Same` is emitted *exactly* where co-location is declared — not as a side effect of having a controller, and not via a magic annotation. A plain 50K-replica Deployment is no longer at risk of being forced onto one topology domain.

### What we lose / cost

- A CRD schema regeneration (`make generate`) and a deepcopy update.
- The operator-global `CoLocationKey` config retires — removed from the operator `Config`, the `cmd/operator` flags, the scaletest chart, and the profiles that set it.
- The scaletest harness's `sameRack` archetype switches from a synthetic `ScaletestWorkload` ownerRef on the pod to a real `podAffinity` term — which is a more faithful model of how production workloads express co-location anyway.

### What stays the same

- BigFleet's shard / coordinator / decision engine: unchanged. The shard receives already-aggregated Needs with `Same` baked into profile requirements, exactly as before.
- The wire format: unchanged. `Need.Group` remains in-memory operator state; `Same` continues to travel as a `NodeSelectorRequirement`.
- The CR's GC contract: the CR is still owned by its Pod.

## Alternatives considered

- **A `bigfleet.lucy.sh/co-location-group` annotation users set on pods.** Rejected: it forces workload owners to change their manifests to adopt BigFleet — backwards from the paper's "operator translates existing CRD signals" model. `podAffinity` is the signal users already write.
- **Co-location group = the pod's controlling workload UID** (`GetControllerOf`). Rejected: it would stamp `Same` on every controller-managed workload, over-constraining placement for workloads that never asked for co-location; and decoupling the aggregation key from the `Same` trigger to avoid that just reproduces this ADR's design.
- **Aggregate by profile fingerprint only, drop co-location grouping.** Rejected as a destination: it guarantees ~2KB but silently removes a paper §8 feature and the harness profiles that exercise `Same`.
- **The gRPC limit bump alone (`e186631`).** Necessary but insufficient — it only moves the ceiling. Kept as the companion safety net.

## References

- `fleet-scale-kubernetes.md` §6.1, §7, §8, §11, §12 — one CR per pod, roll-up aggregates, ~2KB, `Same` translation, the UPC is optional.
- `bigfleet.md` §3.1, §8 — full-replacement roll-ups, co-location.
- ADR-0022 (`Need.Count` is Pod count, BigFleet diffs aggregates) — the aggregation model this ADR makes actually hold.
- Commit `e186631` — the 256 MiB gRPC message ceiling (`pkg/grpcutil`).
- `bigfleet-uber` issues #3 and #4 (private) — the empirical data: NeedsTable frozen at ~49K against 250K+ CRs.
