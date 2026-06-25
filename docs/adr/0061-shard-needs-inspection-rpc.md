# ADR-0061: A shard-side read-only needs-inspection RPC (per-Need last-cycle verdict)

## Status

**Accepted**, 2026-06-26 (author decision, Lucy Sweet). Author sign-off resolved the two open scope questions: build the **full reason taxonomy (Tier 1 + Tier 2) now**, and place the reads on a **dedicated read-only service** on the shard's gRPC server. Builds on [ADR-0027], [ADR-0042], [ADR-0048], [ADR-0060]; relates to [ADR-0029].

## Context

Operator tooling — a CLI, alerting, capacity planning, and the `bigfleet-web-dashboard` — wants to answer, for one cluster: **which of my needs are satisfied vs unmet, by how much, and why?** Today nothing can.

A cluster's demand lives as the shard's in-memory **NeedsTable** (`pkg/needs`): aggregated, bucketed, full-replacement-per-cluster `Need` rows (`{ClusterID, Profile, AggregateResources vector, MinUnit, Group, AcquisitionParked}`; the Profile is the `(requirements, spread, priority, interruption_penalty_bucket, reclamation_penalty_bucket)` aggregation key — [ADR-0027]). Every cycle the decision engine computes, per Need, the full verdict — satisfied/unmet, residual deficit vector, claimed machines, chosen `Same` domain, satisfiability, parked state — and then **discards it**. What leaves the shard is deliberately lossy: a fingerprint-aggregated, cluster-anonymous, requirements-stripped **top-100 shortfall ledger** plus shard-aggregate Prometheus gauges, surfaced to the leader via `ReportShard` and re-exposed by `ListShardReports` ([ADR-0060]).

So the three read surfaces an out-of-tree, read-only consumer has each fall short of "explore a cluster's needs":
- **`CapacityRequest` CRDs** (per managed cluster) — the operator's *raw pre-aggregation input* (one CR per pod), declared demand only, status `Pending|Acknowledged` (receipt, not satisfaction). Shows what a cluster *asked for*, never what the shard *did*.
- **Prometheus** — shard-aggregate, **no `cluster_id`** on any demand series.
- **`ListShardReports`** — unmet-only, `requirements` stripped, **no `cluster_id`**, fingerprint-aggregated.

The satisfied majority of demand, and the engine's per-Need verdict, are observable nowhere. This is a genuine, general operator question (not a dashboard artifact), so the fix belongs in core as general-purpose tooling — consistent with [ADR-0060], which put the read-only role and read RPCs in core and treated the dashboard as one consumer.

## Decision

Add a **shard-side, read-only, streaming needs-inspection RPC** that returns the per-Need last-cycle verdict the engine already computes, plus the rows it currently discards.

### 1. Owner: the shard, never the coordinator

The data lives in the shard, and `pkg/shard` must not import `pkg/coordinator` (static stability, enforced by `TestStaticStability_ShardDoesNotImportCoordinator`). A coordinator route is doubly wrong: it would couple the shard upward, and the coordinator only retains the aggregated/anonymous/stripped top-100 shortfall ledger — the satisfied majority and per-cluster attribution were never sent up and **cannot** be derived leader-side (the proto already says "query the shard directly for the full requirement set"). So this is a new shard read surface, not a leader-local derivation.

### 2. Transport, gating, discovery

- A new **read-only service** registered on the shard's existing gRPC server (which today serves only the operator-initiated `Shard.Session` bidi stream). A second `RegisterXServer` on the same `grpc.Server` is straightforward; a separate service keeps the cluster-SAN-gated `Session` and the readonly-gated reads cleanly delineated.
- **Gated by `bigfleet://readonly` (or `bigfleet://admin`)**, mirroring the coordinator's `requireReadIdentity` ([ADR-0060]); skipped on plaintext (the trust-the-network default, [ADR-0048]). The shard's mTLS server already does `RequireAndVerifyClientCert`; identity is enforced per-RPC in the handler (a small new shard-side helper, mirroring the coordinator's).
- **Discovery**: a client lists shards on the coordinator (`ListShards`, which carries each shard's advertised `address` and is already readonly-gated) and dials the shard's read RPC directly with the same readonly cert.

### 3. Shape: streaming, per-cluster-filtered, trimmed

- **Server-streaming**, with a **mandatory-or-cheap per-cluster filter** (the NeedsTable is already `byCluster`-indexed, so the filter is free) and pagination. A large shard holds **~50K Need rows** alongside its ~500K-machine inventory (the shard scale ceiling, `docs/plan.md` §3.4); a full unfiltered dump is the wrong default.
- Each row is a `NeedView`: the existing **`CapacityNeed`** wire message (it already carries `requirements / aggregate_resources / min_unit / priority / spread / both penalty buckets / group`) + `cluster_id`, `arrival_unix_nanos`, `profile_fingerprint`, and the **last-cycle verdict** below. It carries a cycle number + timestamp; an empty result means "not yet computed / rebuilding", not "no demand".
- **Trimmed projection only.** The verdict excludes the per-Need `[]machine.ID` lists (`ClaimedMachines` etc.) — across ~50K needs those approach the shard's whole claimed set and would be a material allocation/GC load. We keep counts, not ID lists. (An opt-in "include machine IDs for this one cluster" flag is a possible later extension.)

### 4. The verdict — and the reason taxonomy (the load-bearing decision)

The per-Need verdict: `satisfied`, `residual_deficit` (vector), `claimed_machine_count`, `bootstrap_count`, `provision_count`, `same_domain`, `same_satisfiable`, `acquisition_parked`, `parked_age_cycles`, `age_cycles_unmet`, and `unmet_reason`.

A feasibility audit established that the reason **splits into two tiers**, and the ADR will not paper over the difference:

**Tier 1 — pure retain + serve (no decision-logic change).** Derivable from state the engine already computes and discards:
- `SATISFIED` — `NeedResult.Unsatisfied == false`.
- `TOPOLOGY_UNSATISFIABLE` (for `Same`/co-location) — `Unsatisfied && hasSame && !SameSatisfiable` (`NeedResult.SameSatisfiable`, corroborated by `AcquisitionParked`, [ADR-0042]).
- `UNMET_OTHER` — unsatisfied and not `Same`-unsatisfiable: a single coarse bucket covering "ran out of supply / starved by higher priority / preemption fell short".

Tier 1 also requires one **additive, no-logic-change** plumbing edit: surface `occ.CycleResult.Results` on `Phase1Result` so *satisfied plain* Needs (not just gangs and unsatisfied rows) get a verdict row. The retention itself is a cheap capture at the existing `recordShortfalls` point in `runCycle`, where `p1`/`p2` are already live.

**Tier 2 — split `UNMET_OTHER` into `PRIORITY_STARVED` / `NO_MATCHING_SUPPLY` / `PREEMPTION_EXHAUSTED`.** These three are **not** separable from any retained struct today; the distinguishing facts are computed as locals inside `FindBasic`/`FindSame` (did any `MatchProfile`+`MinUnit`-passing machine exist in *any* state, regardless of claim/precedence?) and inside Phase 2 (was the victim pool provably empty? did picks fall short of the deficit?) and then dropped. Surfacing them is **cheap, behaviour-preserving instrumentation** — new observational booleans/counts that **do not change which machines get claimed** — but it is still a hot-path code edit to the OCC find path and Phase 2, and it needs scale re-validation (allocation/GC inside the cycle SLO). **Author decision (2026-06-26): build Tier 2 now** — the full taxonomy ships in this ADR, with the Tier-2 instrumentation guarded as observation-only (no change to claim selection) and re-validated on a scale rung before release.

(Known limitation either way: a `DoNotSchedule` `TopologySpread` that can't be placed isn't separately derivable today — it would fall into a supply reason. Documented; foldable into a `SPREAD_UNSATISFIABLE` reason later if needed.)

### 5. Retention mechanics

A `needViewLedger` rebuilt once per cycle at the `recordShortfalls` capture point, behind an **RWMutex with build-then-swap** (pre-build the snapshot, swap the pointer under the lock) — not the shortfall ledger's rebuild-under-lock, to keep the per-cycle write critical section O(1) against ~50K rows. The ledger stores the trimmed projection (copying projected fields, **not** retaining `NeedResult.Need` pointers, which would pin the whole demand slice).

## Consequences

- **Operators can finally explore per-cluster needs.** Satisfied-vs-unmet, residual deficit, parked gangs, and (Tier 1) the `Same`-unsatisfiable reason — directly attributable to a cluster, from a CLI / alerting / the dashboard, with a `bigfleet://readonly` cert that cannot mutate the fleet.
- **General-purpose, not dashboard-specific.** The surface is a shard read RPC any operator tool consumes; the dashboard is one client. (Same posture as [ADR-0060].)
- **Static-stability-safe.** A read of retained shard-local state: no `pkg/coordinator` import, no coordinator hot-path dependency, no write path. Tier 1 adds **no** new hot-path computation; Tier 2 adds observational field writes only (behaviour-preserving).
- **Honest costs the build must own:** (a) a trimmed per-Need snapshot at ~50K rows is a few MB rebuilt each cycle — a new steady-state allocation to reflect in the scale profiles and watch on `uber-50k`/`uber-500k`; (b) the read RPC must paginate/stream + per-cluster-filter; (c) it is last-cycle soft state — stale by one cycle, empty before the first cycle and (if ever leader-coupled, which it is not) after a restart.
- **Reason completeness is a choice.** Ship Tier 1 (coarse `UNMET_OTHER`) with zero engine-logic change, or Tier 1 + Tier 2 (full taxonomy) accepting the cheap hot-path instrumentation + scale re-validation. See "Open decisions".

## Decisions (resolved at acceptance) + implementation notes

1. **Reason scope — RESOLVED: Tier 1 + Tier 2 (full taxonomy) now.** The Tier-2 OCC/Phase-2 instrumentation is observation-only and must be proven not to change claim selection (golden-output test against the existing engine fixtures).
2. **Service placement — RESOLVED: a dedicated read-only service** on the shard's gRPC server, separate from `Shard.Session`, readonly-gated.
3. **Demand realism ([ADR-0043])** — the satisfied-vs-unmet-with-reason view is the explicit operator need; recorded.
4. **Staleness/cost contract** (implementation) — last-cycle (one-cycle-stale) is the contract; the RPC stamps cycle number + timestamp and labels "empty == not yet computed". Pagination page size + read cadence settled during implementation against the 50K-rows/shard regime.
5. **Scale gating** (implementation) — the retained trimmed snapshot is a new steady-state allocation; reflect it in the scale profiles and re-measure on a rung (`uber-50k` minimum) before release, per "scale ceilings as we go". Tier-2 instrumentation re-validated on the same rung.

## Alternatives considered

- **Reconstruct the NeedsTable in the dashboard from `CapacityRequest` CRDs** by replaying the operator's aggregation transform. Rejected as the *primary* mechanism: it is declared pre-aggregation demand with skew, shows zero shard outcome (no satisfied/unmet), and version-locks the dashboard to the operator's bucketing. (Still worth shipping as a complementary "declared demand" view — it needs no core change — but it is not the needs explorer.)
- **Extend `ListShardReports`** on the coordinator. Rejected: the coordinator never receives the satisfied rows, per-cluster attribution, or requirements; deriving them leader-side is impossible.

[ADR-0027]: ./0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0029]: ./0029-phase1-omega-style-occ.md
[ADR-0042]: ./0042-addendum-aged-acquisition-parking.md
[ADR-0043]: ./0043-demand-realism-check-before-mechanism.md
[ADR-0048]: ./0048-mtls-and-uri-san-identity.md
[ADR-0060]: ./0060-readonly-coordinator-san-role-and-read-rpcs.md
