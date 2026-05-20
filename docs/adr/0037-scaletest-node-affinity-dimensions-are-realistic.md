# ADR-0037: Scaletest catalog node-affinity dimensions must be realistic — drop synthetic team/app label axes

## Status

Accepted, 2026-05-20.

## Context

M35 ("label-axis fingerprint multiplier") added a `labelAxes` field to the
scaletest workload-archetype catalog. Its goal was to multiply the number of
distinct demand "fingerprints" the harness presents, so BigFleet's
`MatchProfile` machinery is exercised against production-shaped cardinality
rather than the handful of fingerprints that instance-type × zone alone
produce.

The realistic catalog (`test/scaletest/profiles/archetypes/realistic.yaml`)
declared `labelAxes` on its two highest-weight archetypes:

- `tiny-stateless` (70 % of demand): `scaletest.bigfleet/team` × 40,
  `scaletest.bigfleet/app` × 20 → 800 distinct (team, app) buckets.
- `cpu-service` (12 %): `scaletest.bigfleet/team` × 10.

The harness implements `labelAxes` by emitting them as **required Pod
`nodeAffinity`** (`load-driver`'s `MatchExpressions` under
`RequiredDuringSchedulingIgnoredDuringExecution`), and by stamping the same
key/value pairs onto seeded machine `Profile.Labels`. The CR's `Requirements`
are then derived from the Pod's node-affinity by the unschedulable-pod
controller, so the axes reach BigFleet's matching as intended.

But `nodeAffinity` is also what `kube-scheduler` uses to place Pods on Nodes.
bigfleet-uber #41 measured the consequence directly from `kube-scheduler`'s
`/metrics` on the canonical catalog: the `NodeAffinity` filter plugin rejected
**98.6 %** of placement attempts (322,846 rejects / 4,539 passes), versus
68 % on a catalog with no `labelAxes`. Each `tiny-stateless` Pod requires a
Node carrying *its* specific (team, app) — one of 800 buckets — so the
overwhelming majority of Pods have no matching Node at all, and steady-state
bind plateaued at 9.5 % of target. The synthetic axis cardinality, not the
substrate, was the gate.

The modelling error is the conflation. `team` and `app` are **Pod** labels in
the real world — ownership and cost-allocation metadata. Production Pods do
not declare `nodeAffinity` onto team/app: real node affinity selects
instance-type, zone, GPU model, CPU architecture, and similar hardware
attributes. Routing a synthetic cardinality multiplier through the Pod's
scheduler-visible `nodeAffinity` made `kube-scheduler` reject realistic
placements that a real cluster would accept. It is an artifact, not realism.

## Decision

The scaletest catalog's node-affinity dimensions must mirror realistic
production node affinity: instance-type, zone, and hardware attributes only.
Synthetic ownership axes (`team`, `app`) are **not** valid node-affinity
dimensions.

The `labelAxes` blocks are removed from `realistic.yaml`. The `labelAxes`
*mechanism* (`archetype.LabelAxis`, `Archetype.PickLabels`) is retained as a
general capability — a future catalog may legitimately use it for a real
node-affinity axis such as CPU architecture or GPU model — but the realistic
catalog declares none.

## Consequences

- `kube-scheduler` `NodeAffinity` rejection drops back toward the
  instance-type × zone baseline; steady state becomes reachable, which is the
  precondition for ADR-0035's steady-state SLO measurement.
- BigFleet's demand-fingerprint cardinality drops to what instance-type ×
  zone produces across the catalog (~75 distinct fingerprints). This is
  **accepted**: that is the cardinality a real fleet's node-affinity actually
  presents. ADR-0017's concern — per-CR binding latency versus fingerprint
  fan-out — still holds; this ADR only sets the cardinality to a realistic
  level rather than a synthetic one.
- M35's label-axis multiplier no longer affects the realistic catalog. If a
  future need to stress BigFleet's matching beyond realistic node-affinity
  cardinality arises, it must be modelled somewhere other than Pod
  `nodeAffinity` — and justified against a real workload pattern first.
