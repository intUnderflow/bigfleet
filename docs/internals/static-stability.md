# Static stability: surviving BigFleet being down

Static stability is BigFleet's one non-negotiable property: a managed cluster keeps running on its existing nodes when BigFleet is entirely down, and a shard keeps making provisioning decisions while the coordinator is unreachable. It is not a feature you turn on — it is the *absence* of a dependency, enforced at the type level. This doc traces where that property lives in the code: the import-graph guard that keeps the hot path coordinator-free (`pkg/shard/no_coordinator_dep_test.go`), the state split that makes the property possible (assignment/quota in the coordinator, inventory/needs in the shard), the snapshot model the cycle reads through, and the heartbeat path by which a shard re-converges after a coordinator outage. The `docs/architecture.md` "Static stability" section is the one-screen version; this is the code-grounded one. Read the BigFleet paper §6 (two-tier hierarchy) and ADR-0002 (single-region coordinator) for the rationale — static stability is what makes a single-region coordinator a *defensible* SPOF rather than a fleet-wide one.

## The property, stated precisely

Three claims, each with a distinct enforcement mechanism:

1. **Clusters keep running with BigFleet entirely down.** A `Configured` machine stays Configured; nothing in BigFleet's absence drains it. The cluster's kubelets keep their nodes; pods keep running. BigFleet does not sit in the cluster's data path — it is not a scheduler, not an admission controller, not a CNI. Its absence is invisible to running workloads.
2. **Shards operate autonomously during coordinator failover.** A shard's decision engine (`pkg/decision` Phase 1/2/3) runs against its own inventory and NeedsTable. The coordinator owns no state the cycle reads on the hot path. A coordinator quorum loss pauses cross-shard rebalance and new quota allocation; it does not pause provisioning from a shard's existing allocations.
3. **No hot-path dependency from `pkg/shard` on `pkg/coordinator`.** This is the mechanism that makes (2) true and keeps it true under future edits. It is enforced programmatically.

## The enforcement: an import-graph guard

`pkg/shard/no_coordinator_dep_test.go` is the load-bearing test. It parses `pkg/shard`'s own import set via `go/build` and fails if any import — or any *test* import — has the prefix `github.com/intUnderflow/bigfleet/pkg/coordinator` (`pkg/shard/no_coordinator_dep_test.go:24-33`). Both lists are checked: a test that pulled `pkg/coordinator` transitively would compile the dependency into the package's test binary and is treated as a violation too (`:29`).

The test's own comment states the architectural carve-out (`:15-17`): coordinator client *adapters* may live in a sub-package the hot path does not import, but nothing reachable from the cycle through `pkg/shard`'s own files may touch the coordinator. That carve-out is realized by `pkg/shard/coordclient/` — the *only* shard-side code that knows the coordinator exists. Its package doc is explicit (`pkg/shard/coordclient/coordclient.go:1-12`): it "lives in a sub-package the shard's hot path does not import, and is optional: the shard keeps operating against its existing allocations when the coordinator is unreachable." The dependency arrow points the safe way — `coordclient` imports `pkg/shard` (to read its surface via `ShardView`), never the reverse. The production glue is `coordclient.ViewFromShard` (`pkg/shard/coordclient/adapter.go:14`), which wraps a `*shard.Shard` in the `ShardView` interface; the shard binary wires this up, the shard package itself never references it.

This is why ADR-0002 names the guard directly (`docs/adr/0002-coordinator-topology-single-region.md:39`): "Any future regression that introduces a hot-path coordinator dependency from the data plane breaks the safety net that makes this decision defensible. `pkg/shard` must not import `pkg/coordinator`. CI should enforce this with an import-graph check." The test *is* that check.

## What state lives where

Static stability is a consequence of *which tier owns what*. The split is deliberate: anything the cycle needs every tick lives on the shard; anything slow-changing and globally-shared lives in the coordinator, off the hot path.

**Coordinator (Tier 1) — never read on the cycle hot path:**

- Shard membership (which shards exist, at what address).
- Cluster→shard binding (permanent on first contact; no cluster-lifecycle API).
- Topology-domain→shard assignment (~100K entries at 100M nodes, per the ADR-0002 per-domain model, not per-machine).
- Quota allocations (cluster-level entitlement counters, handed to shards on demand).
- Provider registry.

These are slow-changing (ADR-0002 cites ~10 writes/sec steady-state) and Raft-replicated. The coordinator "does not make provisioning decisions" (`docs/architecture.md`).

**Shard (Tier 2) — everything the cycle touches:**

- **Inventory** — every machine the shard owns, in-memory, in one of eight states. Refilled from the provider's `List` on startup, reconciled every cycle. `pkg/inventory`.
- **NeedsTable** — every managed cluster's last-known full demand, priority-sorted. Each rollup is a full replacement (`Shard.ApplyRollup` → `needs.Table.Replace`, `pkg/shard/shard.go:468`). `pkg/needs`.
- **Sessions** — one operator-initiated bidi `Shard.Session` stream per cluster (`pkg/shard/session.go`). The shard never dials a cluster.
- **Decision engine** — the three-phase synchronous loop (`pkg/shard/shard.go:621` `runCycle`).

Crucially, the cycle never resolves a cluster→shard mapping or rechecks quota mid-flight — those would be coordinator reads on the hot path. The shard already *owns* its clusters (the operator dialed it; the binding is fixed) and already holds whatever quota was allocated. ADR-0002:21 frames the consequence: a regional coordinator outage is "degraded service, not lost service."

### The Phase 3 corollary

The reclaim half of static stability is subtle: a shard with a *stale or empty* view of demand must not drain Configured machines a cluster is still using. Two guards make Phase 3 conservative by construction.

- **Shrinkage-only attribution (ADR-0045).** Phase 3 consumes Phase 1's claimed-set and reclaims only the unclaimed Configured remainder — bound capacity strictly in excess of demand (`pkg/shard/shard.go:689-700`). It runs *no* demand walk of its own. At steady demand it emits nothing. There is no per-cycle keep-set re-derivation that a transient under-read of demand could corrupt into a mass drain.
- **First-rollup gate (ADR-0036).** Phase 3 skips reclaim entirely for any cluster that has not yet sent a rollup since this shard process started — including the shard-restart window (`pkg/shard/shard.go:417-427`, `firstRollupReceived`). An empty NeedsTable means "demand unknown," not "demand zero." Without this, a shard restart would briefly see no demand and drain healthy Configured supply before operators reconnect. The gate is in-memory and per-process — exactly the restart window it is meant to protect.

Together these mean a shard that comes up cold, or briefly loses operator contact, holds existing Configured capacity rather than reclaiming it. That is static stability expressed in the reclaim path.

## The snapshot model and its history (ADR-0003 → M44.4 → M66.1)

The cycle reads inventory through a snapshot. *How* it reads it changed twice, and the history matters because the current synchronous read is itself a static-stability decision — the cycle must act on a view consistent with the most recent provider reconcile, not a stale one.

**ADR-0003 (Superseded).** Originally, building a fresh `*inventory.Snapshot` every cycle was an O(N) walk of `byID` that dominated per-cycle compute (~700 ms of the cycle at 500K machines on the M5 Max, per the ADR). ADR-0003 moved the build off the hot path: a background `foldLoop` goroutine debounced inventory writes and published snapshots through an `atomic.Pointer`, and the cycle read it in O(1) via `CycleSnapshot()` (`docs/adr/0003-...:25-34`). The ADR argued the eventual consistency was *safe* because every Phase 1/2/3 action is idempotent and `inv.Apply`'s state-machine validation rejects an action emitted against a machine that has already moved (`:21`).

**M44.4 Drop A (the supersession).** The argument was sound in the abstract but wrong in practice at real write rates. With the cycle reading a snapshot up to `foldDebounce + buildTime` stale, the engine repeatedly decided to Bootstrap machines that had *already* reached Configured but whose transition the stale snapshot hadn't yet folded in — roughly 50% wasted Bootstraps. M44.4 switched the cycle back to the synchronous `Inventory.Snapshot()`, which builds a fresh consistent view under the read lock (`pkg/shard/shard.go:658-663`):

```go
// Snapshot() builds a fresh consistent view under RLock. The
// background fold goroutine and CycleSnapshot() were removed at
// M44.4 Drop A: stale snapshots caused ~50 % wasted Bootstraps
// for already-Configured machines at real write rates.
snap := s.inv.Snapshot()
```

**M66.1 (cleanup).** The now-redundant `foldLoop` goroutine and the live triple-indexes that fed it were removed (`docs/adr/0003-...:3`; `pkg/inventory/inventory.go:30-31, 152-153`). The status line on ADR-0003 records the whole arc.

The net is that the cycle today reads a *fresh* snapshot each tick — `reconcile` runs first (`pkg/shard/shard.go:649`) so the snapshot reflects the provider's current truth, then `s.inv.Snapshot()` folds it consistently. The O(N) build cost ADR-0003 was avoiding was paid back down elsewhere (incremental reconcile, ADR-0027 indexing), so freshness no longer trades against the cycle SLO. This is a static-stability detail because the alternative — deciding against a stale view — manufactures churn that looks like instability even when the coordinator is perfectly healthy.

## Self-registration and re-convergence after a coordinator outage (ADR-0006)

Static stability says a shard runs without the coordinator; ADR-0006 is how a shard *rejoins* the coordinator without any of that being a precondition for running.

There is no registration RPC. A shard self-registers through the same `ReportShard` heartbeat it already uses to pull instructions. The heartbeat carries an optional `shard_address` (proto field 8); on the first `ReportShard` from an unknown `shard_id`, the coordinator leader Raft-Applies `AddShard{ID, Address}` itself, inside the gRPC handler (`docs/adr/0006-...:18-22`). Subsequent reports take the cheap `MarkHeartbeat` path. The shard side stamps `AdvertiseAddress` onto every report (`pkg/shard/coordclient/coordclient.go:200-209`); the `Config.AdvertiseAddress` doc spells out the self-register mechanism (`:34-42`).

The static-stability guarantees of this path:

- **The data plane does not depend on registration succeeding.** ADR-0006:30 states it directly: "The shard's hot path (cycle / decision engine / inventory) does not depend on registration succeeding. Registration is a coordinator-side bookkeeping operation... the shard runs cycles either way." The `coordclient.Run` loop reconnects with backoff on transport errors, and "the shard keeps operating against its existing allocations regardless of whether this loop is making progress" (`pkg/shard/coordclient/coordclient.go:153-156`).
- **Errors degrade, they don't block.** A lost `AddShard` race returns `ErrShardExists`, silently swallowed (registration is by definition already done). Any other Apply error surfaces as `Unavailable`; the coord-client retries on the next interval and the data plane continues (`docs/adr/0006-...:22`). In `runOnce`, a failed `ReportShard` logs a warning, re-queues pending instruction acks, and returns — no panic, no state change (`pkg/shard/coordclient/coordclient.go:211-220`).
- **Re-convergence is automatic.** After a coordinator outage, the next heartbeat re-registers (if membership was lost) or simply re-establishes the heartbeat. Domain assignments and instruction delivery resume on the coordinator's schedule; the shard's view of *its own* clusters and inventory never went away, so there is nothing to rebuild on the data side. Instruction handlers (`AssignDomain`/`UnassignDomain`, `pkg/shard/shard.go:161-172`) mutate only the shard's domain-ownership set and are idempotent.

One known limitation, by design: the address is recorded on first sight only. If a shard changes its advertise address, the coordinator does not learn it; the runbook is "remove and re-add the shard" (`docs/adr/0006-...:27`). v1 does not surface this as an operational lever.

## Failure modes: what keeps working

This table extends `docs/architecture.md`'s with the code-level reason each row holds. The constant across every row is that the shard's inventory and decision engine never lose their inputs.

| Failure | What keeps working | What pauses | Recovery path |
|---|---|---|---|
| **Coordinator quorum loss / region-down** | Every shard's cycle: reconcile → Phase 1/2/3 → execute, against existing inventory and last-known NeedsTables. Operators stay connected to their shards; rollups still ingest. | Cross-shard rebalance; new quota allocation; cross-shard preemption (ADR-0002:33). New shards can't register until quorum returns, but a *running* shard's data plane is unaffected. | Restore quorum. Next heartbeat re-converges membership (ADR-0006). No data-plane action needed. |
| **Coordinator unreachable from one shard** (partition) | That shard's full data plane. `coordclient.Run` backs off and retries; the cycle never observes the outage. | That shard's instruction delivery and reporting. Acks re-queue (`coordclient.go:214-218`). | Reconnect with backoff. |
| **Shard crash / restart** | Other shards unaffected. On restart the shard rebuilds inventory from `provider.List` (`reconcile`, `pkg/shard/reconcile.go:39`); in-flight transitions resume via List+Get reconcile. | The restarting shard's decisions until first reconcile (`firstReconcileDone` gates `/readyz`, `pkg/shard/shard.go:478-481`) and Phase 3 reclaim until first rollup (ADR-0036 gate). | Auto. Operators reconnect (outbound dial); first rollup clears the Phase 3 gate. |
| **Operator → shard partition** | Cluster runs on its last-known Configured machines — BigFleet is out of the data path. The shard holds the cluster's last NeedsTable; rollups queue operator-side. The shard does not reclaim on operator silence (no rollup ≠ zero demand under ADR-0036; existing demand persists). | Fresh demand signal from that cluster. | Operator reconnects with backoff; sends a full-replacement rollup. |
| **Provider unreachable** | Remaining demand satisfied from the rest of inventory. Affected machines mark `Failed` with `last_error`. | Provisioning of *new* capacity through that provider. | Provider returns; manual cleanup of Failed machines. Exercised by `sim/scenario/provider_failure.go`. |
| **BigFleet entirely down (all tiers)** | Every managed cluster keeps running on existing inventory. Configured machines stay Configured; no drain path fires with the shards down. | All provisioning and reclaim. | Restore BigFleet. Shards reconcile inventory from providers; operators reconnect; demand re-reports via full rollups. |

The thread through every "keeps working" cell is the same: the shard's inputs (inventory, NeedsTable) are local and self-healing from the provider and the operator streams, and the decision engine reads nothing from the coordinator. Break that — add one `pkg/coordinator` import reachable from the cycle — and `no_coordinator_dep_test.go` goes red before the regression can ship.
