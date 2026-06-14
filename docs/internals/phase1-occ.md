# Phase 1 internals: Omega-style optimistic concurrency

Phase 1 is the shard's *acquisition* phase — it walks the cluster demand the shard currently believes and claims inventory to cover it, emitting Bootstrap (Idle) and Provision (Speculative) actions for what it acquires. The pre-OCC implementation read paper §8 literally: one goroutine, priority-sorted single pass, a global claimed-set mutated in place. ADR-0028 measured that single-threaded loop at 5–15 minutes worst-case for uber-50k under the realistic catalog, because its cost scales with *Need cardinality*, not per-iteration cost, and constant-factor levers couldn't reach the envelope (three documented attempts, all reverted). ADR-0029 replaced it with `pkg/decision/occ/` — an Omega-style [Schwarzkopf et al., EuroSys 2013] optimistic-concurrency-control scheduler: a worker pool races over a shared Need queue, each worker computing a proposal against per-cycle shared state and submitting it to a single commit broker that resolves conflicts at machine granularity and enforces priority *reactively* at commit time rather than proactively by pre-ordering the cycle's work. This doc is the why-grounded tour of that package. It assumes [`docs/internals/decision-engine.md`](decision-engine.md) (the three-phase split, the single-attribution contract, the locked formulas) and paper §8/§16; it does not re-explain the machine state machine, the inventory snapshot, or Phase 2/3.

## The §16 re-reading that unlocks concurrency

The whole design rests on one close reading of a hard rule. `bigfleet.md` §16 says "priority is the sole throttling mechanism" — and the pre-OCC loop took that to mandate a strict priority-sorted outer pass. ADR-0029's Context section (lines 44–51) argues the rule actually rules out *other* throttling mechanisms (quota, admission, entitlement); it does **not** require strict priority-ordered traversal of the cycle's work. Priority can be enforced at the commit broker without sacrificing intra-cycle concurrency. That is the key: the *outcome* (the higher-priority Need wins on contention) is preserved verbatim; only the *mechanism* (pre-ordering → reactive conflict resolution at commit) changes. The package's invariant, stated in `phase1_assign.go:88-95`, is that the resulting claimed-set is "identical to a priority-sorted single pass" — concurrency buys wall-clock, not a different answer.

## The cycle shape

`RunCycle` (`cycle.go:71`) owns the lifetimes of `SharedState`, `Broker`, and `PoolCache`, and is the single entry point `decision.Phase1` calls (`phase1_assign.go:101`). One cycle:

1. **Build `SharedState`** wrapping the immutable inventory snapshot (`cycle.go:83`). Stable `*needs.Need` pointers are taken into the caller's `allNeeds` slice; the barrier correlates them back to result indices by position (`cycle.go:90-93`).
2. **Pre-pass: credit existing supply.** `SeedConfiguredSupply` (`seed.go:55`) walks each Need in priority-descending order, single-threaded, claiming its cluster's matching `Configured`/`Configuring` machines until demand is covered or supply exhausts. This is the ADR-0045 *single attribution* — the engine's only supply-attribution arithmetic, which Phase 3 also consumes. The residual deficit per Need is what's left.
3. **Dispatch the worker pool** over `Idle` + `Speculative`. Default `GOMAXPROCS` workers (`cycle.go:57`), one shared unordered queue, no worker specialisation. Only Needs whose post-pre-pass deficit is nonzero are queued (`cycle.go:129-134`) — zero-deficit Needs are fully covered by existing supply and produce no action.
4. **Barrier.** `wg.Wait()` then `close(work)` (`cycle.go:136-137`). A `sync.WaitGroup` counts in-flight Needs; each initial push and each displacement re-queue is a `wg.Add(1)`, each `processNeed` return a `wg.Done()`. When the counter hits zero the queue is drained and no proposals are in flight — the single synchronization point per cycle.
5. **Barrier post-processing**, single-threaded (`cycle.go:143-173`): reconstruct each Need's `BootstrapMachines`/`ProvisionMachines`/`Deficit`/`Unsatisfied` from the *final* claimed-set. Workers never write `NeedResult` during the cycle (`cycle.go:194-195`); only the barrier does, so there is no concurrent read of results.

The returned `CycleResult` carries `Results` (same order as `allNeeds`) and `Claimed` — the flattened claimed-set Phase 3 diffs against the Configured inventory (`cycle.go:24-27`, ADR-0045).

## Shared state and the conflict primitive

`SharedState` (`state.go:26`) holds three things: the read-only snapshot (no lock — captured immutable at cycle start, Omega's "private local copy" [1, §3.4]), the claimed-set (`claimedBy: map[machine.ID]claim` plus the reverse index `claimedByNeed`), and `bucketSeq: map[BucketKey]uint64` — the per-bucket sequence numbers. A single `mu sync.Mutex` guards everything mutable; the broker holds it for the duration of every `Propose`. The reverse index exists so `RunCycle` and `computeDeficit` compute a Need's residual in `O(claimsForNeed)` rather than walking all claims (`state.go:22-25`).

A **`BucketKey`** (`types.go:28`) is `(State, ProfileFP, SameKey, SameValue)` — one bucket per (machine state, profile fingerprint, Same key, Same value), mirroring the pre-OCC `coLocatedBucket` identity (ADR-0024/ADR-0019). Needs without a Same requirement share a single per-(state, profile) bucket with empty Same fields. The bucket is the natural CAS grain because the catalog already partitions demand this way — and crucially, under the realistic catalog bucket count ≈ Need count (ADR-0028's empirical addendum), which is exactly why an O(buckets) aggregate cache delivered no asymptotic win and was reverted. Per-bucket seqnos are the **fine-grained conflict-detection primitive** Omega measured at 2–3× lower spurious-conflict cost than coarse-grained [1, §5.2, Fig. 14].

**What a conflict is.** A worker captures a bucket's seqno at proposal-construction time (`state.BucketSeq`, `cycle.go:227`) and submits it as `ObservedSeq`. At the broker the seqno is the optimistic read: any successful commit touching that bucket increments it (`broker.go:156`), so a mismatch means the bucket changed under the proposer and the proposal is stale. That is the conflict. Note the worker-side `IsClaimed`/`DisplaceableBy` checks are *cheap, advisory* filters (`state.go:165-193`) — the broker under `mu` is the only authoritative check, so a stale worker-side read costs at most an extra round-trip, never a correctness bug.

## The commit broker: optimistic-then-commit

`Broker.Propose` (`broker.go:52`) is the single synchronization point and the only legitimate mutator of the claimed-set and seqnos. It is **plan-then-commit** (ADR-0029's discipline): classify the whole proposal against current state without mutating, let the mode decide commit-or-abort, then apply the mutation atomically. No rollback path is needed because nothing mutates during classification. The flow under `mu`:

1. **Seqno CAS** (`broker.go:58-62`). If `bucketSeq[p.Bucket] != p.ObservedSeq`, return `StatusConflict` with the current seqno — the proposer re-reads and retries. This is the optimistic check.
2. **Plan** (`broker.go:79-93`). Classify every machine in the proposal into three sets without touching state:
   - `newClaim` — currently unclaimed.
   - `displace` — claimed by a *strictly-lower-precedence* incumbent we can evict.
   - `conflicted` — claimed by an incumbent of equal-or-higher precedence we cannot evict. **Equal precedence does not displace** (`broker.go:85-88`): first-mover wins at equal precedence, so the cycle is stable against same-class churn.
3. **Mode decision** (`broker.go:96-107`), still no mutation. `ModeAllOrNothing` aborts to `StatusConflict` if *any* machine is conflicted; `ModeIncremental` aborts only if *nothing* is claimable (`len(newClaim)+len(displace) == 0`).
4. **Commit atomically** (`broker.go:109-168`). Release displaced incumbents first (drop them from both maps), then claim every machine the proposal can take into both `claimedBy` and `claimedByNeed`, then `bucketSeq[p.Bucket] = currentSeq + 1`. Build the `Displaced` list so the proposer's worker can re-queue evicted incumbents.

The mutex is intentional and not the bottleneck: per-proposal work is `O(machines-in-proposal)` — a Need typically claims 1–10 machines — and workers spend their time computing proposals, not at the broker (ADR-0029 §"Commit broker", lines 440–445).

### Precedence

`Precedence` (`types.go:60`) is the lexicographic triple `(Priority, InterruptionPenalty bucket, ReclamationPenalty bucket)`, higher wins (`types.go:68-76`). It holds the raw `PenaltyBucket` ordinal, not dollars — the ordinal is monotone in dollars and avoids a float conversion on every conflict check. Both **distinct** penalties appear: `interruption_penalty` (cost of interrupting the workload) and `reclamation_penalty` (operational value tied to the specific machine) — they are not the same field and not `operational_value` (no such field exists). `PrecedenceFromProfile` (`types.go:80`) is a pure projection, safe from any worker with no shared-state read.

### The two proposal modes

`ProposalMode` (`types.go:37`) is Omega's transaction-semantics distinction [1, §3.4], carried per-proposal rather than splitting goroutine pools (ADR-0029 explicitly rejected a scheduler-kind partition on uniform-per-Need-cost grounds):

- **`ModeIncremental`** (default) — commit the unclaimed-plus-displaceable subset; machines held by immovable incumbents simply don't commit, and the residual flows to the shortfall buffer. The cheaper path, Omega's default [1, §5.2].
- **`ModeAllOrNothing`** — abort the entire proposal if any one machine is conflicted. Reserved for genuine gangs, where partial fill is *semantically wrong*, not just suboptimal. ~2× more conflict-prone [1, §5.2, Fig. 14a], hence used sparingly.

`modeFor` (`cycle.go:337`) classifies purely from the Need's static fields — no operator hint: a Need is `ModeAllOrNothing` iff it carries a `Same` operator **and** its `MinUnit` does not cover its `AggregateResources` (a multi-machine gang one machine alone can't satisfy). Everything else is `ModeIncremental`.

### Displacement and priority on conflict

This is how §16's priority arbitration is preserved without strict-pass ordering (`broker.go:24-31`). When a higher-precedence proposal proposes a machine a lower-precedence incumbent holds, the broker releases the incumbent's claim and returns the displaced Need in `Result.Displaced` (`broker.go:119-136`). The proposer's worker re-queues each displaced `QueuedNeed` — but only if its retry budget survives (`cycle.go:238-242`). Each displacement *decrements* the inherited retry budget (`broker.go:128`), and multiple machines displaced from one incumbent collapse to a single `Displaced` entry keyed on the smallest resulting budget. The displaced incumbent then competes again from a smaller budget; if it loses everywhere and runs out, it shortfalls. The retry budget bounds displacement-cascade depth — ADR-0029's preemption-cascade risk (lines 931–940). Seeded pre-pass claims participate in displacement identically (`state.go:109-127`): a seeded `Configured`/`Configuring` claim is evictable by a strictly-higher-precedence proposer just like any worker claim.

## Per-Need processing and the retry loop

`processNeed` (`cycle.go:196`) runs one `QueuedNeed` through the broker. It tries `Idle` then `Speculative`; for each state it loops while the retry budget holds:

1. Recompute the live deficit (`computeDeficit`, `cycle.go:278`) from the reverse index; if zero, done.
2. Get the pool for `(state, profile)` and find candidates (below).
3. Capture the candidate bucket's seqno, build the `Proposal`, `broker.Propose`.
4. Re-queue any displaced incumbents (budget permitting).
5. On `StatusCommitted`, mark progress and either retry (basic Needs, to pick up the next-cheapest machine on a fresh claimed-set) or break for this state (topology-constrained Needs — below). On `StatusConflict`, decrement the budget and retry with a fresh observation.

The retry budget defaults to 10 (`cycle.go:53`), a deliberately tight cap versus Omega's "abandoned at 1,000 retries" [1, §4] because BigFleet's shortfall protocol is a first-class concept (paper §9): persistent contention is meant to *escalate to the coordinator*, not be absorbed by spinning. Budget exhaustion → the residual deficit ages in the shortfall buffer (in `pkg/shard`; there is no standalone shortfall package — the buffer/aging live in the shard and the deficit is derived here in `pkg/decision`). The `ShardPhase1OCCRetriesExhaustedTotal` metric (`cycle.go:203-213`) is deliberately narrow: it counts only a worker that entered with *real* demand, finished without committing anything, and hit zero budget — catalog-bound zero-deficit Needs and Needs that committed then ran out on a follow-up state don't count, so the metric measures genuine starvation, not noise.

**Why topology-constrained Needs don't retry within a state** (`cycle.go:248-253`, `hasTopologyConstraint` at `cycle.go:265`): `FindSame`/`FindSpread` bucket-selection and skew-accounting restart from a clean slate each call, so a *second* call in the same state after a partial commit could re-pick a different bucket/domain and violate the constraint. Basic Needs are safe to retry because each retry just picks the next-cheapest unclaimed machine.

## Candidate generation and the supply pool cache

Workers don't propose against raw inventory; they propose against a cost-sorted `Pool` per `(state, profile-fingerprint)`. `PoolCache` (`poolcache.go:35`) builds these lazily — `Get` (`poolcache.go:64`) serialises concurrent first-time lookups on a per-key `sync.Once`, so concurrent workers never redo a pool sort, and `once.Do`'s happens-before makes the lock-free read after build race-free. `buildPool` (`poolcache.go:88`) is sort-aware:

- **Idle + a single pinned instance type** reuses the snapshot's pre-sorted shared bucket — no copy, no resort. The cheap common path.
- **Speculative** always copies and re-sorts, because `EffectiveCost` (`sortSpeculativeCandidates`, `poolcache.go:156`) depends on per-machine `InterruptionProbability` × the per-profile `InterruptionPenalty` dollars. This is the locked cost formula `price + interruption_probability × interruption_penalty` (paper §16) materialised as the §8 Speculative tiebreak; `interruption_probability` is provider-declared on the machine, never cluster-supplied.
- **Idle** multi-type/unpinned sorts on `price asc, ID asc` (`sortIdleCandidates`, `poolcache.go:147`) — the §8 idle ordering.

The `Pool.src` slice is immutable after build, so worker goroutines read it lock-free; there is **no per-bucket head cursor** (unlike the pre-OCC `phase1Pool`). Concurrent workers each re-walk `src` from the start and the global claimed-set bears the dedup load (`poolcache.go:14-20`) — head-cursor amortisation doesn't survive concurrency.

`findCandidatesFor` (`cycle.go:306`) routes by constraint, **Same winning over Spread** when both are present (Same is the stronger constraint, matching the pre-OCC allocator's precedence):

- **`FindBasic`** (`candidates.go:42`) — two passes over the cost-sorted pool: pass 1 takes unclaimed machines only (the zero-displacement path that dominates the realistic catalog); pass 2, only if deficit remains, takes displaceable claimed machines (lower-precedence incumbents). Two-pass biases toward zero-cost wins and avoids cascading displacement chains that would burn the retry budget in 1:1 demand-to-supply tests.
- **`FindSpread`** (`candidates.go:299`) — honours a `DoNotSchedule` TopologySpread: per-step it picks the cheapest-head machine among topology buckets whose count won't exceed `min + maxSkew`. The bucket key carries the topology key but an **empty SameValue** — Spread touches machines across multiple domains, so all workers competing for Spread on one key share a single CAS line.
- **`FindSame`** — the constrained heart, below.

`matchProfile` (`match.go:12`) is duplicated from `decision.MatchProfile` to keep `occ` free of an import cycle on `pkg/decision` (the two must stay aligned). It implements `In`/`NotIn`/`Exists`/`DoesNotExist`/`Same`; the `Same` operator on a *single* machine is satisfied by any machine with a value for the key — the *group* constraint is enforced entirely by the bucketing in `candidates.go`, not per-machine.

## Same-constrained needs: one domain or not at all

A co-located (`Same`) Need must be served by a **single topology domain** or not at all (ADR-0040) — and acquisition and crediting must agree on *which* domain, or Phase 1 assembles a cross-domain group that Phase 3 (correctly strict) reclaims half of next cycle: the reclaim↔re-bootstrap oscillation. The fix is that the domain is chosen **once per Need per cycle**, in the pre-pass, jointly over creditable and acquirable supply, and recorded on `SharedState` so acquisition is confined to it.

### The joint domain choice (`seedSameProfile`, `seed.go:152`)

For a Same-Need the pre-pass:

1. Collects the cluster's matching, unclaimed, MinUnit-covering `Configured`+`Configuring` machines in **keep-priority order** (`SortedClusterStateBucket`: price asc, reclamation_penalty desc, ID asc), bucketed by their Same-key value. These are the **creditable** half.
2. Folds in each domain's **acquirable** half — shard-wide unclaimed Idle+Speculative matching the profile — from `SameSupplyIndex` (`foldAcquirable`, `seed.go:317`). Idle has no cluster binding, so acquirable is never creditable (ADR-0041 rider 3).
3. Chooses the single best bucket via `ChooseSameBucket` over those **joint** totals, records the domain and a structural-satisfiability verdict on state (`seed.go:231-232`), and credits *only the creditable members within the chosen domain* via `SeedClaim`.

`findCandidatesFor` then passes the recorded domain into `FindSame` (`cycle.go:316`), which — when the domain is non-empty — **skips bucket scoring entirely** and only considers machines whose Same-key value equals it (`candidates.go:131-168`). The recorded domain survives displacement re-queues because the `QueuedNeed` carries the same `*needs.Need` pointer the pre-pass keyed it under (`state.go:96-107`). When the domain is empty (a Same-Need with no bucket *anywhere* — no creditable and no acquirable supply), `FindSame` falls back to its best-single-value-bucket scoring (atomic-satisfiable preferred, then cheapest head, then most-available; `candidates.go:194-285`).

### `SameSupplyIndex` — the perf contract (`samesupply.go:53`)

The acquirable half is indexed lazily per profile fingerprint: equal fingerprints mean equal profiles (and at most one Same key, per ADR-0024), so the full Idle+Speculative pool is `matchProfile`-walked **once per fingerprint class per cycle**, never once per Need. Quantities are parsed once at index build into dimension-interned `int64` milli-unit vectors (`ParseVec`, `samesupply.go:92`); the per-Need `AcquirableTotals` walk is integer adds and compares only. This is load-bearing: the first implementation re-parsed `resource.Quantity` strings per member per Need (`needs.Covers`/`AddResources` both round-trip through `ParseQuantity`) and that was **58% of shard CPU** at ~2,500 co-located Needs — ~100 s cycles, a starved shard. `BenchmarkAcquirableTotals_Uber5KShape` guards the path. The index is not concurrency-safe; its only consumer is the single-threaded pre-pass.

`ConsumeAcquirable` (`samesupply.go:233`) is the ADR-0041 rider: when a Need's chosen domain falls short of its creditable supply, the workers will fill the residual from that domain's Idle/Speculative — so the pre-pass *virtually consumes* those members against a shared `consumed` ledger, and the next Need ranks against what's actually left. Without it, the moment idle Same-capacity appears every gang ranks the same fresh domain best, the losers assemble nothing, and Phase 3 reclaims their healthy serving machines as unclaimed excess.

### `ChooseSameBucket` — the ranking that kills the oscillation (`samebucket.go:124`)

A strict total order over distinct domain values, refined across three diagnosed oscillation classes:

1. **Satisfiable beats unsatisfiable** (`Total` covers the deficit).
2. **Among satisfiable, greatest *creditable* coverage of the deficit, capped per dimension** (ADR-0045 / M77a). A bound machine *is* the fulfillment, so the domain choice follows the bindings — never the reverse. This rule's history is the day-one dev-50 catch: its two predecessors keyed on creditable *presence* only, which never discriminated on a fleet whose cluster supply is spread across domains, so ranking fell through to acquirable *slack* — and any inventory mutation re-ranked the argmin, flipped the gang's domain, turned its bound machines into off-domain strays Phase 3 reclaimed, and the reclaim moved the next domain's totals: the Bootstrap≈Reclaim lockstep at static demand. Coverage pins a fully-bound domain (coverage 1.0) so nothing holding less of the gang's supply can win.
3. **Tie on capped creditable coverage → greatest *gang-own* coverage** (ADR-0051 / M77g, `samebucket.go:158-168`). Creditable coverage is *cluster*-granular (any same-class gang's machines count), so it ties a domain holding *this* gang's machines with one holding an equal count of an unrelated same-class gang's. `CreditableOwnTotal` — the sub-total from machines whose `(Profile.Fingerprint, AssignedGroup)` match *this* Need's `(fingerprint, Group)` — breaks it, pinning a served gang to the domain its own bound machines occupy through the bootstrap dwell that was the #64 perturbation.
4. **No satisfiable bucket → most-covering `Total`.**
5. **Among unsatisfiable buckets of *equal* joint coverage, greatest creditable coverage** (ADR-0042): switching domains is reserved for *strictly* greater coverage, so a structurally-unsatisfiable gang keeps its concentrated partial assembly and ages quietly in the shortfall buffer instead of flip-flopping (the #56 ~27/sec churn anatomy).
6. **Tiebreak:** larger `Count`, then lexicographically smallest `Value`.

`sameBucketScore` (`samebucket.go:208`) reduces a bucket to a scalar as the sum over the deficit's positive dimensions of the per-dimension `total/deficit` ratio — ratios, not raw quantities, keep cpu/memory/extended dimensions commensurable, and `capped=true` saturates each ratio at 1 so overflowing one dimension can't mask a hole in another. This is the crediting mirror of `FindSame`'s acquisition scoring; it ranks on coverage rather than price because credited supply is already provisioned.

### Within-domain machine stability (M77h, `seed.go:249`)

Even with the domain pinned, churn surfaced *within* it (#65): when a `Configuring→Configured` machine re-sorts ahead of an already-serving incumbent in keep-priority order, the first-N-covering claim loop's stop-when-covered could bump the incumbent out of the claimed subset, leaving it for Phase 3 to reclaim and then re-bootstrap. `incumbentFirst` (`seed.go:279`) is a **stable partition** that claims this gang's own incumbents (`AssignedGroup == n.Group && AssignedNeedFingerprint == n.Profile.Fingerprint()`) before the rest under stop-when-covered. Because the partition is stable, each side keeps its keep-priority order, so a genuinely over-covered gang still sheds its excess incumbents in paper-§8 release order. It reads only current bindings — no memory of past claims — so ADR-0045's no-second-ledger rule holds, and the no-attribution / no-incumbent paths stay byte-identical and allocate nothing.

A **parked** Need (`AcquisitionParked`, ADR-0042 Addendum) is the degenerate case: it folds creditable-only (no acquirable bucket to rank), so the incumbent domain wins trivially; `findCandidatesFor` returns empty candidates for it (`cycle.go:313-315`); it keeps its partial assembly, consumes nothing, acquires nothing, and ages in the shortfall buffer until a periodic re-probe or fresh supply un-parks it. Its creditable claims still keep its serving machines out of Phase 3's excess.

## Machine-count-aware seed sizing (ADR-0044)

ADR-0044 is a *harness* decision, not an engine change, but it's the supply-side substrate Phase 1 runs against and worth knowing when reading scale results. The seed used to pick each machine's archetype by workload-*object* frequency (`Archetype.Weight`), but machine demand per pod differs by two orders of magnitude across archetypes: a core-resource pod packs ~`density` to a machine, while a pod requesting an *extended* resource packs exactly one per machine (device counts don't scale with density). Weight-proportional pools therefore underweighted whole-machine archetypes' supply by ~`density`×, which is how a realism-clean run found GPU-training gangs short 120–238 GPU machines *per zone* on a fleet whose aggregate capacity was ample — the supply mirror of an over-credited Phase 1. ADR-0044 derives machine shares from pod demand (`machineShare ∝ podShare / podsPerMachine`) and floors gang archetypes at `max(GroupSizeRange)` machines per zone, so the largest gang the catalog can draw is satisfiable by construction. Without that floor, every run rediscovers the largest gang as a parked-gang population — which is exactly the regime `ChooseSameBucket` rule 5 and the parking path were measured against.

## What's deferred, and what was rejected

Three follow-ons matter for orientation:

- **Incremental Phase 1** (ADR-0030, **Proposed**) would make steady-state cycle cost `O(churn)` rather than `O(NeedsTable)` by per-Need digesting which Needs/inventory changed since last cycle and processing only the delta, carrying the rest forward as a union claimed-set. It *layers on* OCC — same broker, seqnos, retry budget, displacement — and falls back to a full OCC pass on first cycle, on detected drift, on periodic re-sync (K=100), or when >50% of Needs changed. It is deliberately conditional: promoted only if post-OCC measurement shows steady-state cycles dominated by re-processing unchanged Needs.
- **ParSync partitioned sync** (ADR-0031, **Proposed**) would partition the claimed-set into P partitions and rotate which one each worker reads fresh per cycle (the rest from bounded-staleness carry-forward), reducing spurious-conflict growth as cardinality climbs. The per-bucket seqno CAS is already the right primitive — a stale-partition read is caught at commit. It is **not** needed for any rung of the uber-* ladder as projected (steady-state conflict rate ≤ 0.15 puts its benefit in the ~10–20% range): the per-shard ceiling is 500K machines and BigFleet scales horizontally (uber-1m = 2 shards × 500K), so every multi-shard rung sees the same per-shard workload as uber-500k. ParSync becomes load-bearing only if a future ADR *raises* the per-shard ceiling, or if measured conflict rate trends toward Omega's 1.0 saturation.
- **Bind-ready supply credit** (ADR-0033, **Rejected**, superseded by ADR-0035). ADR-0033 proposed that `SeedConfiguredSupply` skip a `Configured` machine until the operator confirmed its node was bind-ready, to fix a measured bind plateau. The plateau turned out to be a *kube-scheduler* property under high label cardinality that only manifests during *ramp*, and the real bug was the harness gating on ramp percentage as if it were an SLO. ADR-0035 reframes: SLOs are measured at steady state under churn with pre-seeded inventory removing the ramp entirely. No code from ADR-0033 shipped — Phase 1's aggregate-supply math is correct in steady state, where demand rate equals churn rate and supply matches demand precisely as pods replace each other. The standing lesson (recorded in memory): *ramp behaviour is not an SLO*; check whether steady-state SLOs are actually failing before opening a regression investigation.

## Invariants this design holds

- **Every machine is claimed by at most one Need per cycle** — the broker is the sole mutator under `mu`.
- **The post-barrier claimed-set is coherent** and is exactly what Phase 3 diffs against the Configured inventory (ADR-0045 single attribution, ADR-0027 stage 5.1): one walk (the pre-pass), two consumers (Phase 1 sizing, Phase 3 reclaim), so the phases cannot disagree by construction.
- **Priority wins on contention** — enforced reactively at the broker, outcome-identical to a priority-sorted single pass.
- **No hot-path coordinator dependency** — `pkg/decision/occ` imports `inventory`, `machine`, `needs`, `metrics` only; this is intra-shard concurrency, no distributed locking (§16, static stability).
- **Cost formula, provider RPC surface, penalties, roll-up format all unchanged** — ADR-0029 is a `pkg/decision/` iteration-shape rewrite, nothing else.

What's *weakened* (intentionally): strict priority-ordered traversal (mechanism only — outcome preserved) and cross-cycle Action-*order* determinism. The set of Needs satisfied vs shortfalled is deterministic up to commit-ordering of ties; the Action *sequence* within a result is not, so sim goldens assert outcome-equivalence, not exact Action sequences (ADR-0029 §Invariants).
