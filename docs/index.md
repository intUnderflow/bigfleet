# BigFleet documentation

Start here.

## Orientation

- [`../README.md`](../README.md) — what BigFleet is, status, repo layout
- [`user-stories.md`](user-stories.md) — what life is like with BigFleet, by role
- [`architecture.md`](architecture.md) — two-tier architecture, decision engine, static stability
- [`concepts.md`](concepts.md) — glossary of every domain term you'll see in code and protos

## Using BigFleet

- [`quickstart.md`](quickstart.md) — five-minute kind-based demo
- [`operator-guide.md`](operator-guide.md) — install, metrics, runbook
- [`scaling-guide.md`](scaling-guide.md) — sizing for 10K–100M machines

## Extending BigFleet

- [`provider-author-guide.md`](provider-author-guide.md) — implementing a `CapacityProvider`
- [`api-reference.md`](api-reference.md) — CRDs and gRPC services

## Internals (implementation deep-dives)

Code-level documentation that bridges the high-level guides above to the source — for contributors and maintainers, not end users.

- [`internals/`](internals/README.md) — the internals index: subsystem deep-dives grouped by area (decision engine, shard hot path, coordinator, protocols, operator, scale-test harness, cross-cutting)
- [`internals/decision-map.md`](internals/decision-map.md) — every ADR mapped to where it lives in code and which tests guard it
- [`internals/data-flow.md`](internals/data-flow.md) — read-this-first trace of the full control loop, from unschedulable pod to bound node

## Scale-test history

- [`../test/scaletest/results/README.md`](../test/scaletest/results/README.md) — baseline table per profile + how to add new runs

## Background and history

- [BigFleet paper](https://lucy.sh/bigfleet) — the design paper (canonical; vendored here at [`papers/bigfleet.md`](papers/bigfleet.md))
- [Fleet-Scale Kubernetes paper](https://lucy.sh/fleet-scale-kubernetes) — operating model paper (canonical; vendored here at [`papers/fleet-scale-kubernetes.md`](papers/fleet-scale-kubernetes.md))
- [`plan.md`](plan.md) — full implementation plan, milestones, scale ceilings
- [`adr/`](adr/) — architecture decision records

## When the docs disagree

Order of authority (highest first):

1. The two papers (`papers/`).
2. ADRs that supersede the papers on a specific point.
3. The plan (`plan.md`).
4. Everything else.

If you find a divergence, file a PR — the order above is the project's source-of-truth policy.
