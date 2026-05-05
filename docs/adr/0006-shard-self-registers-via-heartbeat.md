# ADR-0006: Shards self-register with the coordinator via the existing ReportShard heartbeat

**Status**: Accepted

**Date**: 2026-05-05

## Context

Until M12, the coordinator's `service Coordinator` had a single RPC — `ReportShard` — and `MakeAddShardCommand` was only ever invoked from in-process tests. There was no way for a freshly-started shard to appear in coordinator state without a side-channel registration step.

Two candidate shapes:

1. **Dedicated registration RPC.** Shard calls `Register` on startup, coordinator Raft-Applies `AddShard`. Simple, but adds a second authentication / authorisation surface and one more failure-mode-at-startup the shard has to retry.
2. **Self-registration via the existing heartbeat.** Add a `shard_address` field to `ShardReport` so the first heartbeat from an unknown shard carries enough information for the coordinator to Raft-Apply `AddShard` itself. No new RPC.

The shard already pulls from the coordinator via `ReportShard` for instructions — adding a registration RPC just to register would mean the shard's startup flow has *two* coordinator interactions instead of one, with the second being mandatory for any subsequent operation.

## Decision

`ShardReport` gains an optional `shard_address` field (proto field 8). On the first `ReportShard` from a `shard_id` not present in coordinator state, the leader Raft-Applies `AddShard{ID, Address}` synchronously inside the gRPC handler. Subsequent reports take the cheap path (`MarkHeartbeat`).

Errors: `ErrShardExists` (a concurrent register won the race) is silently swallowed because the registration is by definition already done. Other Apply errors propagate as `Unavailable` — the shard's coord-client retries on the next interval, the shard's data plane continues against its existing allocations regardless.

## Consequences

- **One coordinator RPC for the shard's lifecycle.** Less wire surface, less retry choreography.
- **Address recorded on first sight.** If a shard ever changes its advertise address, the coordinator does not learn about the change; the recorded value is the one from the first heartbeat. Re-registration would require an admin RPC (M15's `RemoveShard` + a fresh first heartbeat). v1 doesn't surface this as a documented operational lever; if the cluster topology changes such that addresses need updating, the runbook is "remove and re-add the shard."
- **Coordinator's `bigfleet_coordinator_apply_total` counter is the canonical "shard registered" signal.** Pre-M12 this counter was always zero in cloud runs because no Apply ever fired; it now increments on every shard's first heartbeat. The harness chart's coordinator scrape (commit `039bb62`) is what surfaces this; absent the scrape, the metric exists but nothing publishes it.
- **`MarkHeartbeat` on an unknown shard is still a silent no-op.** The auto-AddShard above ensures the common case is registered, but there's a small race where `MarkHeartbeat` runs before the Apply commits. Subsequent ReportShard cycles re-attempt the lookup — the race resolves by the second heartbeat.
- **Static stability is preserved.** The shard's hot path (cycle / decision engine / inventory) does not depend on registration succeeding. Registration is a coordinator-side bookkeeping operation that lets domain assignments and admin tools see the shard; the shard runs cycles either way.
