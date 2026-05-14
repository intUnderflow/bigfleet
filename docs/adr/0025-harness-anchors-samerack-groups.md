# ADR-0025: The load-driver anchors sameRack groups — a gang-scheduler stand-in

**Status**: Accepted

**Date**: 2026-05-14

## Context

ADR-0024 made the scaletest harness emit real `podAffinity` on `sameRack`-archetype pods, so the unschedulable-pod-controller translates it into `CapacityRequest.Spec.CoLocation` and the operator turns it into a `Same` requirement at roll-up. This exercises the co-location path end-to-end through real Kubernetes objects.

The kind `dev-500` validation of ADR-0024 surfaced a problem: the run reached **92% binds and stalled** — it could not clear the ramp gate. The ~15% of pods from the `sameRack` archetypes (`gpu-training`, `memory-db`) never bound.

The cause is a **documented Kubernetes limitation**, not a kwok artifact or a BigFleet bug. A `sameRack` pod carries a *self-referential* `requiredDuringSchedulingIgnoredDuringExecution` `podAffinity`: "schedule me on a rack that already has a pod from my group." When a whole group is created at once into a cluster with no running peers, **the first pod can never schedule** — kube-scheduler's `InterPodAffinity` filter requires an already-assigned peer pod, and there isn't one. The Kubernetes pod-affinity design proposal states this directly:

> "if all pods in service S have this RequiredDuringScheduling rule in their PodSpec, then the RequiredDuringScheduling rule will block the first pod of the service from ever scheduling, since it is only allowed to run in a zone with another pod from the same service."

The same document proposes a scheduler self-match exemption ("if the requirement matches a pod's own labels, and there are no other such pods anywhere, then disregard the requirement") — but it is listed as a *proposed* short-term fix only and was never implemented. Current kube-scheduler has no such exemption; the bootstrap deadlock is real on a real cluster too (e.g. `k3s-io/k3s#9350`).

Production fleets don't hit this with raw `podAffinity`. Strict gang co-location is handled in a layer **above** the autoscaler: gang schedulers (Volcano, the coscheduling plugin, Kueue's topology-aware scheduling) place the whole group atomically; or a leader-anchor pattern is used (MPIJob's launcher schedules unconstrained, workers `podAffinity` onto it); or placement-group labels turn it into `nodeAffinity`. The autoscaler's job is to *provision the co-located capacity* — the atomic *placement* onto it is a scheduler-layer concern.

This puts the harness in tension with ADR-0023, which deliberately **removed** harness-side pod binding (pod-shim's blanket `/binding`) because "the harness's choice of scheduler is the dominant variable in the published numbers." We need a way to let `sameRack` pods bind without either (a) reverting ADR-0024's real-`podAffinity` modelling, or (b) silently re-introducing harness binding without justification.

## Decision

**The load-driver force-binds one anchor pod per `sameRack` co-location group**, playing the role a gang scheduler plays in a real fleet.

A reconcile loop in the load-driver, running on a short interval:

1. Lists pods carrying the `scaletest.bigfleet/co-location-group` label, grouped by that label's value.
2. For each group with pending pods but **no pod yet bound to a node** (no anchor): finds a fresh fake-Node — one not yet claimed by any co-location group — whose `Allocatable` fits the group's pod shape.
3. Force-binds one pending group pod onto that node via the `Binding` subresource (the typed `clientset.CoreV1().Pods(ns).Bind()` path — controller-runtime's cache layer has known issues with the binding subresource, per pod-shim's experience).

kwok then marks the anchor `Running`. Real kube-scheduler now sees a running peer with the group label and places the rest of the group onto the same rack via `podAffinity`, normally.

This is **legitimate, not a credibility dodge.** It is not the harness papering over a kube-scheduler weakness — kube-scheduler is behaving correctly; gang bootstrapping is genuinely outside its remit. The load-driver is standing in for the gang scheduler a real fleet would run above the autoscaler. It is the opposite of pod-shim's blanket binding: pod-shim bound *every* pod, replacing the scheduler entirely; this binds *one anchor per sameRack group* (~15% of pods × 1/groupsize), and real kube-scheduler still does all the actual scheduling work.

**BigFleet is entirely unaffected.** It provisions capacity for the aggregated `Same` Need regardless of whether or where pods bind — the kind run confirmed it provisioned the co-located machines correctly. The anchor loop only moves the user-facing *bind metric*; it touches no BigFleet code path.

## Consequences

### What we gain

- `dev-500` (and any `sameRack`-using profile) can clear its ramp gate again, while keeping ADR-0024's real-`podAffinity` modelling — the full `pod → UPC → CoLocation → operator → Same → shard` path runs end-to-end through real objects.
- The harness models the *real* production topology: autoscaler provisions co-located capacity, a gang-scheduler-equivalent places the group atomically onto it.

### What we lose / the caveats

- `sameRack` pods' bind metric is **harness-assisted** for one anchor per group — it is not 100% pure kube-scheduler. This must be stated when interpreting `sameRack` bind latency. The ~85% of non-co-located pods remain pure kube-scheduler (ADR-0023).
- The anchor may land on a fresh fake-Node other than the specific machine BigFleet provisioned for that group's Need. At density-100 with fungible machines this converges (the "displaced" machine is filled by other pods); it is a harness-accounting nicety, not a correctness issue.
- The reconcile loop lists co-location-labelled pods on an interval. Bounded by the `sameRack` fraction (~15%), but at the largest profiles (`scaleway-5m`) this is worth revisiting with a field-selector / informer if it shows up in the apiserver budget.

### What stays the same

- ADR-0023 holds: the harness runs real kube-scheduler, and it does all scheduling for the ~85% non-co-located pods and for the non-anchor members of every `sameRack` group.
- BigFleet — `pkg/shard`, `pkg/operator`, `pkg/decision`, the UPC — unchanged. This is purely a load-driver addition.

## Alternatives considered

- **A — `sameRack` pods become plain pods in Mode=pods** (no `podAffinity`); cover the `Same` path via a Mode=cr profile. Simplest, `dev-500` goes green immediately. Rejected as the primary choice because it drops real-`podAffinity` modelling from the Pod-mode path that every cloud profile uses — the UPC's `podAffinity → CoLocation` step would then only ever be unit-tested, never exercised e2e at scale.
- **B — keep `podAffinity`, exclude `sameRack` pods from the bind gate**, verify the BigFleet side via provisioning metrics. Faithful, but it permanently leaves ~15% of the population unbindable in the harness and needs runner gate-query surgery to special-case them — more machinery for a less complete result.
- **C (chosen)** — anchor one pod per group. Keeps real `podAffinity`, keeps the full e2e path, and the binding is a faithful model of the gang scheduler a real fleet runs.
- **Revert ADR-0024's harness change entirely** — would mean Mode=pods never exercises co-location. Rejected for the same reason as A, more so.

## References

- ADR-0023 — real kube-scheduler in the scaletest harness, retire pod-shim's binding role. This ADR is the scoped, justified exception to it.
- ADR-0024 — co-location via `podAffinity`; the change that surfaced this.
- [Kubernetes pod affinity design proposal](https://github.com/kubernetes/design-proposals-archive/blob/main/scheduling/podaffinity.md) — documents the first-pod bootstrap problem and the proposed-but-unshipped self-match exemption.
- `k3s-io/k3s#9350` — a real-world instance of the same bootstrap deadlock.
- `bigfleet-uber` brief #5 — the devpod-5k cloud re-run; devpod-5k uses no co-location, so it is unaffected by this ADR.
