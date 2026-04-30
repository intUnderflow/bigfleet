# ADR-0001: Record architecture decisions

**Status**: Accepted

**Date**: 2026-04-30

## Context

BigFleet is a reference implementation of a published design (`docs/papers/bigfleet.md` and `docs/papers/fleet-scale-kubernetes.md`). The papers are detailed but not exhaustive: many implementation choices — Raft library, deployment topology, on-the-wire encoding tradeoffs, code-organisation rules — fall to the implementer. We will accumulate these choices over the life of the project, and they need to be discoverable and reviewable in isolation.

The plan in `docs/plan.md` is the *target shape*. ADRs are the *audit trail*: they capture why a specific path was chosen given the alternatives that existed at the time, so a reader returning to a decision a year later can recover the reasoning without spelunking commit history.

## Decision

We will record significant architectural decisions as ADRs in `docs/adr/`. Each ADR is a single Markdown file numbered sequentially: `NNNN-short-slug.md`.

Format for each ADR:

- **Title** — short and specific (e.g., "Use hashicorp/raft for the global coordinator").
- **Status** — `Proposed`, `Accepted`, `Superseded by ADR-XXXX`, or `Deprecated`.
- **Date** — ISO date.
- **Context** — what's the problem; what constraints exist; what existed at the time the decision was made.
- **Decision** — what we're doing.
- **Consequences** — what becomes easier, what becomes harder, what's now load-bearing.

ADRs are immutable once accepted. Changing direction means writing a new ADR that supersedes the old one and updating the old one's status to `Superseded by ADR-XXXX`. Don't edit accepted ADRs except to update status.

A decision is "significant" if it's hard to reverse. Wire formats, on-disk schemas, the boundary between coordinator and shard, the provider contract — all in scope. Variable names, function signatures, package layouts — out of scope.

## Consequences

- New contributors have a single place to find decision history.
- Reviewers can demand an ADR before a non-trivial design change lands, which slows down ad-hoc architectural drift.
- ADR sprawl is a real risk: too many ADRs and nobody reads them. Calibration: aim for ADRs to be the answer to "why is this *this* way and not something else?" Bias toward fewer, weightier ADRs.
- Plan and ADRs can drift. When they do, the ADR wins for the specific decision; the plan should be updated to reflect it.
