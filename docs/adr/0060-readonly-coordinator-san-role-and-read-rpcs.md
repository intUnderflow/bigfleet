# ADR-0060: A read-only coordinator SAN role (`bigfleet://readonly`) and general-purpose read RPCs

## Status

**Accepted**, 2026-06-24 (author decision, Lucy Sweet). Amends [ADR-0048].

## Context

Read-only operator tooling — a fleet dashboard, a CLI, alerting, a capacity-planning script — needs to query the coordinator's fleet state. But under [ADR-0048] every coordinator read RPC (`ListShards` / `ListDomainAssignments` / `ListQuotas`) shared a single gate with the mutating admin RPCs: it required the `bigfleet://admin` SAN. So any read-only tool would have to carry an **admin** certificate — one that can *also* call `AssignDomain` / `RemoveShard` / `JoinRaftCluster`. A read-only UI holding an admin credential is precisely the Kubernetes-Dashboard footgun (the unauthenticated/over-privileged dashboard behind the well-known cryptojacking breach): a compromise of the read tool becomes a fleet-mutation capability.

Two adjacent gaps compounded it: the coordinator already accumulates per-shard **soft state** from every `ReportShard` heartbeat (`LatestSummary` / `LatestShortfalls` exist as Go accessors), and a **provider registry** (`State.Providers()`), but neither had an RPC surface — so no tooling could read the inventory/demand snapshot or the registered providers without parsing Raft state.

This surfaced while designing `bigfleet-web-dashboard` (an out-of-tree, read-only web dashboard). The role and the RPCs are deliberately **general-purpose** — useful to any operator tooling, not specific to the dashboard.

## Decision

1. **Add a read-only SAN role, `bigfleet://readonly`.** The coordinator splits its authenticated surface in two (`pkg/coordinator/grpc_server.go`):
   - **Read RPCs** (`ListShards`, `ListDomainAssignments`, `ListQuotas`, and the two new ones below) go through `requireReadIdentity`, which accepts `bigfleet://readonly` **or** `bigfleet://admin` (admin is a superset).
   - **Mutating RPCs** (`AssignDomain`, `UnassignDomain`, `RemoveShard`, `JoinRaftCluster`, `SnapshotSave`) keep `requireAdminIdentity` — a read-only certificate **cannot** mutate the fleet.
   - On plaintext transports both checks are skipped, exactly as before (the trust-the-network default; ADR-0048 §"plaintext"). `ReportShard` keeps its strict per-shard SAN binding.
   This amends [ADR-0048]'s "the whole admin surface requires `bigfleet://admin`" into a read/write split; admin clients are unaffected (admin passes every read check).

2. **Add two general-purpose read RPCs** (leader-only, read-gated), reusing existing messages:
   - **`ListShardReports`** — the coordinator's leader-local soft-state snapshot per shard: the latest `ShardSummary` + top-N `Shortfall` it already holds. Leader-local, **not** Raft-replicated: it is observability-grade, empty right after a failover until shards re-report, and each `ShardReportSnapshot` carries `received_at_unix_ns` so callers can label freshness. (The soft state does not retain a shortfall's `requirements`, so that field is always empty — documented on the message.)
   - **`ListProviders`** — the registered provider backends (`State.Providers()`), mirroring `ListShards`.

## Consequences

- **Read-only tooling needs no admin power.** A dashboard / CLI / alerting integration authenticates with `bigfleet://readonly` and gets a certificate that *physically cannot* change the fleet — closing the over-privileged-read-surface hole. This unblocks `bigfleet-web-dashboard` and any other read consumer.
- **The mutating surface is unchanged** — still `bigfleet://admin`. A future "write from a dashboard" capability would use a *distinct* write identity (e.g. `bigfleet://dashboard-operator`) behind ADR-0048, designed when it is built; it is explicitly out of scope here.
- **No new hot-path dependency.** Both RPCs read control-plane state the coordinator already holds; nothing touches `pkg/shard` or the data plane. `ListShardReports` reads the soft-state maps directly under the server lock (the `LatestSummary`/`LatestShortfalls` accessors take that same lock, so calling them from the handler would deadlock).
- **Soft-state freshness is the caller's to handle.** `ListShardReports` is stale-on-failover by design; consumers must use `received_at_unix_ns` and treat an empty result as "rebuilding after failover", not "zero demand".
- **General-purpose, not dashboard-specific.** The role and RPCs stand on their own for any operator tooling — the dashboard merely consumes them.

[ADR-0048]: ./0048-mtls-and-uri-san-identity.md
