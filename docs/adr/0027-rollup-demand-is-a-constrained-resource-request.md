# ADR-0027: Roll-up demand is a constrained aggregate resource request, not `(per-pod-shape, count)`

**Status**: Accepted. The companion paper revision (`fleet-scale-kubernetes.md` §6, §7) lands with this ADR — revised sections are marked inline in the paper.

**Date**: 2026-05-14

## Context

### The bug

Phase 1's supply accounting (`pkg/decision/phase1_assign.go:104-186`) over-credits supply when demand fingerprints share physical eligibility.

For a Need with fingerprint `fp`, supply is `Σ PodsPerMachine(needShape, machineAllocatable)` over Configured/Configuring machines where `m.AssignedNeedFingerprint == fp`. `PodsPerMachine` is the **dedicated** density — how many pods of *this one shape* fit if the machine were exclusively theirs. But `AssignedNeedFingerprint` is `SourceProfile.Fingerprint()` (`execute.go:210,277`) — the full fingerprint, including the `In`-list requirements and the exact resource shape. So every distinct pod shape is its own supply bucket, and each bucket's capacity is computed as if the machine were dedicated to it.

kube-scheduler does not honour that exclusivity. It places *any* pod whose `In` requirements match a node's concrete labels. So one machine provisioned for fingerprint X also absorbs fingerprints Y, Z, W that share an instance-type-family `In` list. Confirmed on `dev-50`: node `fake-spec-132` (`m6i.large`) hosted **355 pods across 4 distinct resource fingerprints**, all sharing `In [m6i.large, m6i.xlarge, m6i.2xlarge]`.

Consequence: BigFleet credits ~dedicated-density phantom slots to one fingerprint; reality delivers a fraction once co-eligible fingerprints share the node. The surplus-credit logic (`:174-186`) then `absorbed_by_supply`'s the rest of that fingerprint's Needs against phantom capacity. `deficitPods` never goes positive → the `unsatisfied` branch (`:217`) is never reached → no shortfall is recorded → `shortfalls=0` despite genuinely stuck pods. The convergence loop breaks: roll-ups are full-replacement ground truth and keep listing the stuck pods, but BigFleet's internal supply math overrides that signal.

### How it was found

The laptop `dev-500` profile plateaued at ~51%, traced to a laptop-only kine bottleneck on the demand side (kube-scheduler I/O-blocked on kine's sqlite — see `project_dev500_demand_side_ceiling`). The fast `dev-50` profile cleared the kine wall and surfaced *this* bug directly: ramp plateaued ~93%, `shortfalls=0`, and the `fake-spec-132` mixed-fingerprint node gave the mechanism — one machine hosting pods of four distinct resource fingerprints. Independently, `bigfleet-uber` brief #6 ran `devpod-5k` at `d0fc193` on Uber's real-etcd infra: `failed-rampgate`, `absorbed_by_supply=43.8M`, `emitted_idle=973`, `emitted_spec=0`, `unsatisfied=0`, `shortfalls=0`, and the inner agent's unprompted root-cause assessment — "the supply-credit accounting loop in Phase 1 is the fundamental bottleneck." Two independent environments, one signature.

The Phase 1 `machinesNeeded` density fix (ADR-0026 / `d0fc193`) is correct in isolation but *unmasked* this bug rather than causing it: the prior density-1 error was accidentally brute-forcing past the over-credit by over-provisioning. Removing the accidental workaround exposed the real bug. The harness's missing Speculative tier (ADR-0026) is real and fixed, but is **downstream** of this blocker — the Idle pool never drains while emission is suppressed, so the Speculative fallback is never reached (brief #6: 15000/15000 Speculative slots untouched).

### Why it is a modelling decision, not a patch

The papers say BigFleet "diffs aggregates against provisioned inventory" and is emphatic that it is **not a scheduler** ("the autoscaler does not simulate the scheduler"). They are **silent** on how aggregate supply accounting should behave when demand fingerprints share physical eligibility. The current code's implicit answer — per-fingerprint dedicated density — is wrong, and it is wrong *because* of the shape of the demand contract.

### Relationship to ADR-0022

ADR-0022 ("`Need.Count` is Pod count, not machine count") is the **predecessor**, not a conflict. It corrected a different, narrower drift (the implementation treated `Need.Count` as machine count, emitting one Bootstrap per pod) and *already moved toward resource-space reasoning* — "aggregate demand = `Profile.Resources × Count`, vector math, deficit per dimension." It stopped one step short: its `(Profile, Count)` row structure **forces one row per distinct pod-shape** — `Count` only means anything for a uniform shape — and per-shape rows are exactly what recreate the per-`AssignedNeedFingerprint` supply partition that breaks under shared `In`-eligibility. ADR-0022 fixed "many pods, one Profile"; it did not see "many Profiles, one shared machine." This ADR extends it.

Note also: ADR-0022's scaling argument (the paper's ~2 KB roll-up) and its "one CR per pod, roll-up aggregates" language both *favour* this change — eligibility-constraint-sets are coarser than `(shape × constraint)` fingerprints, so the roll-up gets smaller; and "one CR per pod" is preserved as a cluster-internal representation. ADR-0022's deferred "Path B" (split `Profile` into per-replica and per-machine types) is largely made moot — the per-replica shape leaves the wire entirely, surviving only as the indivisibility floor below.

## Decision

**The roll-up demand wire unit is a constrained aggregate resource request.** A `CapacityNeed` no longer describes "`count` pods of shape `resources`"; it describes a resource envelope and the constraints on the machines that satisfy it:

| Field | Meaning |
|-------|---------|
| `requirements` | Per-machine constraints (`In/NotIn/Exists/DoesNotExist/Same` on instance-type, zone, labels). **Unchanged.** |
| `aggregate_resources` | The total resource vector (cpu/memory/gpu/ephemeral) the cluster needs satisfied within this constraint set. **Replaces per-pod `resources`.** |
| `min_unit` | The largest atomic schedulable unit — a resource vector each machine must be able to host. The indivisibility floor; the only residue of "pod shape" on the wire. With the Workload API this is the declared gang/unit size. |
| `priority` | **Unchanged.** |
| `interruption_penalty` | Bucketed dollar cost of interrupting the workload; feeds `effective_cost`. **Carried per `CapacityNeed`, unchanged** — one of the two aggregation-key penalty axes. |
| `reclamation_penalty` | Bucketed dollar cost of giving up the machine–workload pairing; feeds idle-tiebreak, victim scoring, Phase 3 release. **Carried per `CapacityNeed`, unchanged** — the other aggregation-key penalty axis. |
| `spread` | Topology spread. **Unchanged.** `Same` (co-location) stays in `requirements`. |
| ~~`count`~~ | **Removed.** Machine count is BigFleet's *output*, never the cluster's input. |

`interruption_penalty` and `reclamation_penalty` are both declared on the cluster's `CapacityRequest`s and both carried per `CapacityNeed` — unchanged from today. The roll-up aggregation key is `(requirements-set, interruption-penalty-bucket, reclamation-penalty-bucket)` — a **two-axis** penalty boundary, exactly as `Profile.Fingerprint()` already keys on both buckets. Both axes are load-bearing under aggregation: because a `CapacityNeed` is homogeneous in both penalty buckets, every machine provisioned for it inherits an unambiguous stamped penalty pair (`Machine.AssignedInterruptionPenaltyDollars` / `AssignedReclamationPenaltyDollars`) for Phase 2 victim scoring and Phase 3 release to read. Drop either axis and the stamp becomes arbitrary. The operator aggregates within each `(requirements, interruption-bucket, reclamation-bucket)` cell — summing `aggregate_resources`, taking `max(min_unit)`.

### The three-layer separation, made explicit

- **kube-scheduler** — workloads → node placement. *(cluster)*
- **operator** — workloads → constrained aggregate resource demand. *(per-cluster translation: aggregate per `(requirements, penalty-bucket)`, sum resources, compute `min_unit`)*
- **BigFleet** — constrained resource demand → cost-optimal machine procurement. *(fleet-wide)*

Each layer speaks its own vocabulary — pods/nodes ↔ resources+constraints ↔ machines+prices — and none leaks into the next. BigFleet never sees a pod. This is what makes the contract durable across workload primitives (Pods today, the Workload API / Jobs / VMs next): the operator absorbs the translation; BigFleet's contract never moves.

### What Phase 1 does

For each `CapacityNeed`, in priority order:

1. **Demand** = `aggregate_resources` (a vector).
2. **Supply** = `Σ Machine.Allocatable` over matching machines (Configured + Configuring), where "matching" is `MatchProfile` against `requirements`. Resources are additive and fungible — counted **once per machine**, no per-fingerprint partition, no density reconstruction, nothing to phantom-multiply.
3. **Deficit** = `max(0, demand[dim] − supply[dim])` per dimension.
4. Provision the cheapest machine set (by `effective_cost`) covering the deficit, where each machine satisfies `requirements` and can host one `min_unit`. Idle preferred, Speculative fallback — unchanged tiebreaks.
5. Genuine deficit after Idle + Speculative → `unsatisfied` → shortfall. This path becomes **meaningful again** because the deficit is real, not phantom-masked.

### The residual: overlapping eligibility

Two `CapacityNeed`s with *overlapping but not identical* `requirements` (e.g. `In[m6i.large,m6i.xlarge]` and `In[m6i.xlarge,m6i.2xlarge]`) still share eligible supply. This does not vanish — flexible requirements inherently overlap. But moving to resource units makes it **benign**: instead of a phantom credit that *breaks the loop*, it is a bounded under-count that the feedback loop *closes* — every provision adds real fungible resource, so convergence is monotonic. BigFleet handles it conservatively by claiming each unit of supply to one demand (a greedy matching; the allocator's global `claimed` set already does this for `take`, and is extended to cover the existing-supply credit). The exact claiming/matching strategy is an implementation detail, not a contract decision; a more precise covering algorithm can follow without further wire changes.

## Consequences

### What this corrects

- The density-reconstruction half of the over-credit is **structurally eliminated** — there is no `PodsPerMachine` projection and no per-fingerprint partition to inflate.
- The overlapping-eligibility half becomes a bounded, self-correcting imprecision instead of a loop-breaking phantom.
- `unsatisfied` / shortfall escalation becomes correct again — the visibility bug (`shortfalls=0` while pods are stuck) is fixed *as a consequence*, because the deficit is no longer masked.
- The Speculative tier (ADR-0026) becomes reachable once emission is no longer suppressed.
- The cost model (`effective_cost = price + interruption_probability × interruption_penalty`) finally has a clean job: "given a resource envelope and constraints, find the machine set minimising total effective cost." Pure procurement, no scheduling.

### What it costs / what is reworked

- **Wire/proto change to `CapacityNeed`** — a paper revision (`fleet-scale-kubernetes.md` §6.1, §7). Needs author sign-off; ships as a companion paper-diff.
- **`pkg/decision`** — `PodsPerMachine`, `densityFor`, and the per-pod-count Phase 1 path are removed or substantially reworked. `machinesNeeded` becomes a resource-vector covering computation. ADR-0022's M45.1 vector-math intent is *realised* here (it was described in ADR-0022 §Decision-2 but implemented as a pod-count projection); `Machine.Allocatable` (M45.0) survives and is reused directly.
- **Phase 2 / Phase 3** — must be reworked to the new shape. Phase 3 reclaim mirrors Phase 1 in resource-vector space (slack = supply exceeding aggregate demand). Phase 2 victim scoring operates on the new `CapacityNeed`.
- **The operator** gains real translation responsibility — aggregate per `(requirements, penalty-bucket)`, sum resources, compute `min_unit`. This is still *translation*, not capacity decision-making, and it is the correct home for it (next to the scheduler, cluster-local). The per-pod CR + ownerRef-GC withdrawal stays cluster-internal as the operator's *input*.
- **The scaletest harness** demand generation reshapes — but *simplifies* (no density model, no per-pod-shape fingerprint cardinality knob).
- ADR-0022 narrows: "`Need.Count` is Pod count" → "the operator's *input* is per-pod CRs; the roll-up *wire* is aggregate resources." M45's pod-count implementation is partly unwound — but M45 is not yet validated at scale (M45.5/M45.6 still open), so this unwinds unproven work.

### What stays the same

- **The provider contract** — six RPCs (`Create/Configure/Drain/Delete/Get/List`) on machines BigFleet identifies. Unchanged. ADR-0022's "no provider-side packing, no `Catalog()`" holds.
- **Roll-ups are full replacement.** Still true — of resource-shaped demand.
- **`Same` is protobuf-only**, penalty bucketing is powers-of-2 (two axes: interruption and reclamation), the cost formula is fixed. All unchanged; the new aggregation key `(requirements, interruption-bucket, reclamation-bucket)` composes with them.
- **Static stability / hot path.** The resource-vector diff is *cheaper* than per-pod-fingerprint accounting (fewer rows, simple vector arithmetic) and adds no coordinator dependency.

## Alternatives considered

- **Keep pod-shaped, conservative density haircut.** Rejected — a band-aid; the per-fingerprint partition (the structural cause) remains, and a "largest co-eligible shape" haircut needs the same cross-fingerprint analysis anyway.
- **Node-shaped demand** ("I need N nodes of shape X"). Rejected — it pre-empts BigFleet's core procurement-optimisation job. The cluster does not know fleet-wide pricing, spot-vs-reserved, cross-provider availability, or interruption probabilities; pinning SKUs cluster-side defeats "a single fungible pool priced by cost." (An intermediate proposal in the design discussion; resource-shaped is the correct floor.)
- **Occupancy feedback** (operator reports per-node free capacity to the shard). Rejected — new protocol surface and hot-path state, edges toward observing the scheduler, and does not help during the provision→bind lag.
- **Accept + make visible only.** Rejected as a *solution* — it leaves real pods stuck. The visibility fix is delivered here anyway, as a consequence of the deficit becoming real.

## References

- ADR-0022 — `Need.Count` pod-vs-machine; the predecessor this extends.
- ADR-0026 — Speculative tier seed; downstream of this fix.
- `pkg/decision/phase1_assign.go:104-186` — the over-crediting supply accounting.
- `pkg/decision/match.go` — `MatchProfile`; `In`-requirement evaluation (correct; reused).
- `pkg/machine/machine.go` — `AssignedNeedFingerprint`; the per-fingerprint partition.
- `docs/papers/fleet-scale-kubernetes.md` §6.1, §7 — the `CapacityNeed` wire format being revised.
- `docs/papers/bigfleet.md` §1, §8 — "diffs aggregates against provisioned inventory"; "a single fungible pool priced by cost."
- `bigfleet-uber` brief #6 (private) — `devpod-5k` at `d0fc193` on real etcd; the symptom at scale.
- `dev-50` validation runs (`test/scaletest/results/2026-05-14-dev-50-*`) — the mechanism (`fake-spec-132` mixed-fingerprint node).
- memory: `project_dev500_demand_side_ceiling` — the laptop kine wall this investigation passed through first.
