# BigFleet documentation

Start here.

## Orientation

- [`../README.md`](../README.md) — what BigFleet is, status, repo layout
- [`architecture.md`](architecture.md) — two-tier architecture, decision engine, static stability
- [`concepts.md`](concepts.md) — glossary of every domain term you'll see in code and protos

## Using BigFleet

- [`quickstart.md`](quickstart.md) — five-minute kind-based demo
- [`operator-guide.md`](operator-guide.md) — install, metrics, runbook
- [`scaling-guide.md`](scaling-guide.md) — sizing for 10K–100M machines

## Extending BigFleet

- [`provider-author-guide.md`](provider-author-guide.md) — implementing a `CapacityProvider`
- [`api-reference.md`](api-reference.md) — CRDs and gRPC services

## Background and history

- [`papers/bigfleet.md`](papers/bigfleet.md) — the design paper (canonical)
- [`papers/fleet-scale-kubernetes.md`](papers/fleet-scale-kubernetes.md) — operating model paper (canonical)
- [`plan.md`](plan.md) — full implementation plan, milestones, scale ceilings
- [`adr/`](adr/) — architecture decision records

## When the docs disagree

Order of authority (highest first):

1. The two papers (`papers/`).
2. ADRs that supersede the papers on a specific point.
3. The plan (`plan.md`).
4. Everything else.

If you find a divergence, file a PR — the source-of-truth ordering is enforced in [`../CLAUDE.md`](../CLAUDE.md).
