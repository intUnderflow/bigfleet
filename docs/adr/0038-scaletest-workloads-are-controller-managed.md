# ADR-0038: Scaletest workloads are controller-managed objects, not bare Pods

## Status

Accepted, 2026-05-20.

## Context

The scaletest load-driver creates workloads as **bare Pods** — `corev1.Pod`
objects with no owning controller. The unschedulable-pod controller then
creates a `CapacityRequest` per Pod, owner-referenced to the Pod, so demand
tracks the Pod population.

bigfleet-uber #45 traced an unbounded supply-thrash to this. BigFleet's
Phase 3 correctly identifies genuine excess Configured supply (the seed's
archetype mix never matches the realized demand draw exactly — ~25 surplus
machines per cluster in the uber-5k catalog) and reclaims it. Reclaim drains
the machine: the operator evicts every Pod on the machine's node. Eviction
**deletes** a bare Pod — and there is no controller to recreate it. The
deleted Pod's `CapacityRequest` cascade-GCs with it, so **demand permanently
shrinks** by the machine's Pod count. The next Phase 3 cycle sees even more
apparent excess and reclaims more, which destroys more demand. The result is
a self-sustaining ~26 machines/sec Bootstrap+Reclaim cascade against a demand
that never actually changed — observed across briefs #38–#44, mis-labelled
"steady-state churn" until #45 pinned it.

The cascade is unbounded **only because bare Pods do not survive eviction**.
In a real cluster every workload is managed by a controller — `Deployment`
(via `ReplicaSet`), `StatefulSet`, `Job`. When an autoscaler drains a node,
the evicted Pods are recreated by their controller and rescheduled onto the
remaining capacity. Demand is conserved; the autoscaler's scale-down
self-arrests at the true surplus. Draining nodes that carry workload is the
*correct, central* behaviour of an autoscaler — the bug is not that BigFleet
reclaims machines with Pods on them, it is that the harness modelled
workloads in a way that makes eviction destroy demand.

A harness built on bare Pods also never exercised the real
drain → controller-recreates → reschedule path that production has, so it
could not have surfaced steady-state behaviour faithfully even without #45.

## Decision

The scaletest load-driver creates workloads as **real controller-managed
objects**, and the kwok cluster runs the controllers that reconcile them:

- **Stateless archetypes** (`tiny-stateless`, `cpu-service`, `cpu-batch`,
  `gpu-inference`, `gpu-training-*`, `critical-realtime`) → `Deployment`.
- **Stateful archetypes** (`stateful-db`, `memory-cache`) → `StatefulSet`
  (no `volumeClaimTemplates` — the harness does not model storage; the
  StatefulSet is used for its stable-identity / ordered semantics only).
- One workload object per archetype fingerprint per cluster; `replicas`
  drawn from a service-size distribution, summed to the per-cluster Pod
  target. A workload object's replicas share one shape — which is itself
  more realistic than the previous per-Pod resource re-draw.
- `kube-controller-manager` in the kwok apiserver container runs the
  `deployment`, `replicaset`, and `statefulset` controllers in addition to
  the existing `garbage-collector` and `namespace` controllers.

`cpu-batch`'s finite-lifetime behaviour stays modelled by lifetime-aging
(delete the Pod; the `ReplicaSet` recreates it — a fresh batch run). A
`Job` mapping is a possible future refinement but is out of scope here.

Eviction becomes non-destructive: when Phase 3 reclaims a machine, the
operator's drain evicts the Pods, their controllers recreate them, the
recreated Pods reschedule onto the remaining capacity, and demand is
conserved. Phase 3 reclaims the genuine surplus once and self-arrests.

## Consequences

- The load-driver's hand-rolled Pod-count maintenance (`reconcileTarget`
  acting as a pseudo-`ReplicaSet`) is replaced by real controllers. Churn
  becomes "delete a Pod; its controller recreates it"; scale becomes
  "adjust `replicas`".
- `kube-controller-manager` does more work per kwok cluster — a modest CPU
  cost, scoped to three added controllers. Substrate budgets may need a
  small bump; measured at validation.
- Pre-bind still operates on Pods — the Pods are now created by the
  `ReplicaSet` / `StatefulSet` controllers rather than the load-driver
  directly, but the Unschedulable → UPC → CapacityRequest → bind chain is
  unchanged.
- This is a precondition for ADR-0035's steady-state methodology to hold:
  without controller-managed workloads, every Phase 3 reclaim permanently
  destroys demand and steady state is unreachable by construction.
- No BigFleet (`pkg/decision`, `pkg/operator`) change. #45's verdict
  proposed making Phase 3 avoid machines with bound Pods, or skipping
  eviction on Reclaim — both rejected: they would remove BigFleet's core
  ability to scale a running cluster down, and BigFleet structurally does
  not (and must not) know per-machine Pod counts. The fault was the
  harness's workload model, and the fix belongs there.
