# The decision engine: the worker loop and three phases

The decision engine is the shard's brain — `pkg/decision/`. Once a cycle, the shard hands it a consistent inventory snapshot and the cluster demand it currently believes, and the engine returns a flat slice of `Action`s: Bootstrap, Provision, Reclaim, Preempt, Delete. It places no pods, runs no quota, watches no providers; it diffs *bound capacity* against *demanded capacity* under the locked cost and victim-score formulas, and emits the minimal set of provider/operator actuations that closes the gap. This doc is the per-phase tour and the *why* behind the structure — the three-phase split, the single-attribution contract, the formulas, and the gates that keep a wrong decision from killing a fleet. It assumes you've read [`docs/architecture.md`](../architecture.md) and paper §8/§16; it does not re-explain the machine state machine or the inventory snapshot.

## Where the engine sits in the cycle

The phases are pure functions over a snapshot — they live in `pkg/decision`, which imports neither `pkg/shard` nor `pkg/coordinator`. The shard's `runCycle` is the only orchestrator. The relevant sequence (`pkg/shard/shard.go:646-780`):

1. `reconcile` against the provider so the snapshot isn't stale (`shard.go:649`).
2. `snap := s.inv.Snapshot()` and `demand := s.needs.Snapshot()` — one RLock-built consistent view, read once at cycle start (`shard.go:663-664`). M44.4 deleted the background-folded stale snapshot: stale views caused ~50% wasted Bootstraps for already-Configured machines.
3. `demand = decision.NormalizeDemand(snap, demand)` — fold sub-machine Same-Needs (ADR-0041; below), once, so all three phases and the attribution probe reason over identical demand (`shard.go:670`).
4. `p1 := decision.Phase1(snap, demand)` (`shard.go:677`).
5. `p2 := decision.Phase2(snap, p1.Unsatisfied, s.cfg.Phase2Options)` — Phase 2 consumes *only* Phase 1's residuals (`shard.go:684`).
6. `p3 := decision.Phase3(snap, p1.Claimed, s.FirstRollupReceived, relPolicy, time.Now())` — Phase 3 consumes Phase 1's claimed-set, not a walk of its own (`shard.go:700`).
7. Collapse `p1.Actions + p2.Actions + p3.Actions` into one queue, apply the actuation rails, enqueue to the async execute pool (`shard.go:777-813`).

All three phases compute against *the same snapshot*. Their outputs are therefore independent and order-free at execute time (`shard.go:771-780`) — there is no intra-cycle data dependency between them, only the explicit hand-offs of `Unsatisfied` (P1→P2) and `Claimed` (P1→P3). That independence is what lets the execute pool drain them in any order, and what makes each phase unit-testable against the papers in isolation.

## Why three phases, not one optimiser

The honest question is why this isn't a single MILP/flow solver that jointly chooses the cost-minimal assignment. Three reasons, all load-bearing:

- **Scale.** A shard carries up to ~500K machines and thousands of Needs and must close a cycle in milliseconds at uber-50k (ADR-0029). A joint optimiser over that product is not a p99-of-milliseconds object. The phases are each near-linear walks with bucket indexes; Phase 1 parallelises across `GOMAXPROCS` workers (ADR-0029, below).
- **The three jobs are genuinely different decisions.** Phase 1 *acquires* against demand. Phase 2 *reallocates* existing capacity by priority — this is the one place §16's priority arbitration bites, and it must be exempt from every volume rail (ADR-0046). Phase 3 *releases* capacity demand no longer claims. Folding them collapses distinct safety properties: you cannot cap reclaims without capping preempts, you cannot gate reclaim on first-rollup without gating acquisition, you cannot make Phase 2 emit Preempts that resolve *next* cycle without a phase boundary to defer to.
- **Paper fidelity over optimality.** Paper §8 *is* three declarative phases, and §16 fixes the cost formula and forbids any pluggable objective. BigFleet deliberately ships the paper's greedy, priority-ordered answer — "what got assigned is identical to a priority-sorted single pass" (ADR-0029) — not a cost-optimal one. A solver would be a different, unsanctioned design.

The phases also share one arithmetic to avoid the failure that motivated ADR-0045: the *single attribution* (below). Two independent supply walks — Phase 1's "what's claimed" and Phase 3's old "what's excess" — drifted, and the drift made Phase 1 provision exactly what Phase 3 reclaimed: the Bootstrap↔Reclaim oscillator. One walk, two consumers, removes that class by construction.

## The locked formulas

These are in `pkg/decision/cost.go` and `pkg/machine/machine.go`, fixed by paper §16 / ADR-0029. They are not pluggable; the package doc-comment says so in as many words (`cost.go:1-8`).

**Effective cost** (`machine.go:297-302`):

```
effective_cost = price_per_hour + (interruption_probability × interruption_penalty)
```

`price` is per-hour, `interruption_penalty` is dollars; the result is a per-hour expected cost comparable across machines. `interruption_probability` is **provider-declared only** — it rides on the `Machine`, validated into `[0,1]` at provider ingest (`machine.go:297-302`, `Invariant` at `machine.go:315`); there is no cluster-side override or max-merge. Effective cost is the Speculative tiebreak in Phase 1 (paper §8: "within speculative: tiebreak by effective_cost"); within Idle the tiebreak is `reclamation_penalty` (machine-tied, the §8 idle tiebreak). These live in the keep-priority order `SortedClusterStateBucket` materialises (price asc, reclamation_penalty desc, ID asc) — see "single attribution" below.

**Victim score** (`cost.go:71-92`), paper §8:

```
score = priority_gap          × w.Priority
      + (1 / drain_seconds)    × w.DrainSpeed
      + (1 / interruption_pen) × w.Interruption
      + (1 / reclamation_pen)  × w.Reclamation
```

Higher = better victim. The reciprocals mean *smaller* drain time and *smaller* penalties contribute *more* — cheap-to-interrupt, fast-draining, low-value machines are preferred victims. Denominators are floored ($0.01 for penalties, 1s for drain) to avoid div-by-zero (`cost.go:77-87`). Note the two **distinct** penalties both appear: `interruption_penalty` (cost of interrupting the workload — also the `effective_cost` term) and `reclamation_penalty` (operational value tied to the specific machine). They are not the same field, not derivable from priority, and not `operational_value` (no such field exists). Weights default to `{1.0, 0.1, 0.1, 0.1}` — priority dominates under normal load; the shard exposes them as config so an operator can scale drain-speed up under saturation, but the *formula* is fixed (`cost.go:40-47`, `phase2_inversions.go:25-36`).

**Drain grace** (`cost.go:102-114`), paper §8, keyed on the priority gap:

| gap | grace | meaning |
|-----|-------|---------|
| > 900K | 10s | extreme — best-effort yields to production |
| > 500K | 30s | large |
| > 100K | 2m | moderate |
| else | 10m | small / equal — full graceful |

A voluntary Phase 3 reclaim has no preemptor, so it gets the most generous 10m tier — `ReclaimGrace` (`cost.go:116-122`). The grace rides the `ReclaimInstruction`/`Preempt` action to the operator, which bounds its cordon + PDB-respecting eviction with it (ADR-0009; below).

## Demand normalization (ADR-0041)

Before any phase runs, `NormalizeDemand` (`normalize.go:61`) folds *foldable* sub-machine Same-Needs into plain atomic aggregates. A Same-carrying Need is foldable iff some machine in the snapshot — the cluster's own Configured/Configuring, or shard-wide Idle/Speculative — matches its Profile and has `EffectiveAllocatable` covering its *whole* `AggregateResources` (`normalize.go:260-313`). Such a gang doesn't need the Same machinery: any single fitting machine hosts it, so co-residency is automatic. Foldable Needs of the same `(cluster, stripped-Profile fingerprint, aggregate)` collapse to one plain Need with the Same requirement stripped, `AggregateResources` summed, and `MinUnit` set to one gang's aggregate (the Fleet-Scale §7 atomicity floor).

The *why*: pre-fold, every sub-machine gang up-rounded to one exclusive machine (~2,400 gangs demanding ~2,400 machines where ~540 existed at uber-5k), while kube-scheduler happily packed many gangs per machine — so the machine-granular claim ledger over-counted and BigFleet massively over-provisioned. Folding makes the vector math count only machines that can host a whole gang, restoring a truthful ledger. The fold is conservative under fragmentation (never overstates feasibility), snapshot-dependent, and recomputed each cycle — if the largest matching machines disappear, the class deterministically reverts to per-gang Same-Needs next cycle (`normalize.go:42-49`). Needs that fit no machine keep their per-gang Same Need: the genuinely cross-machine topology case where machine-exclusive claiming is correct, and where an unsatisfiable Same becomes a shortfall (never cross-shard resolution).

## Phase 1 — assignment (delegates to `pkg/decision/occ`)

`Phase1` (`phase1_assign.go:100`) is a thin adapter: it calls `occ.RunCycle(snap, allNeeds)` and translates the result into `Action`s. Idle commits become `ActionKindBootstrap` (idle → configured, one bootstrap); Speculative commits become `ActionKindProvision` (Create + Configure); residuals become `UnsatisfiedNeed`s for Phase 2 (`phase1_assign.go:112-142`).

The accounting rule is ADR-0045's: **capacity counts for a cluster iff it is bound to that cluster.** Binding (Configure) is the atomic act of fulfillment; the machine state machine is the only supply ledger. The pre-pass credits the cluster's Configured *and Configuring* machines at gross `EffectiveAllocatable` — a binding counts from the moment it's made, before the node exists, so double-supply is impossible by construction. Bound < demand → fulfill the difference; bound ≥ demand → BigFleet's job is done (`phase1_assign.go:76-86`). Deliberately *not* modeled: per-machine consumption, residual fit, whether the cluster's scheduler can actually place pods on the bound capacity. BigFleet is not a scheduler; satisfied-but-stuck (fragmentation at equal priority) is the cluster's problem, resolved by its own kube-scheduler preemption or revised demand (ADR-0045 §4).

### Inside `occ` — Omega-style OCC (ADR-0029)

`occ.RunCycle` (`occ/cycle.go:71`) realises paper §8's "walk needs top-down by priority" as *commit-time enforcement*, not a strict single-threaded pass:

1. **Pre-pass** (`SeedConfiguredSupply`, `occ/seed.go:55`) — single-threaded, priority-descending. For each Need it walks the cluster's matching Configured/Configuring machines in keep-priority order and claims them via `state.SeedClaim` until demand is covered or supply exhausts. This is the existing-supply credit; it emits no action. Priority order here preserves §16 (priority is the sole throttle).
2. **Worker pool** (`occ/cycle.go:117-137`) — `GOMAXPROCS` workers race over a shared queue of the Needs with non-zero post-pre-pass deficit. Each `processNeed` (`occ/cycle.go:196`) tries Idle then Speculative, builds candidates from the appropriate pool, and submits a `Proposal` to the commit `Broker`. The broker is the single synchronization point: a mutex serialises commits, per-bucket sequence numbers detect stale reads, and a `Precedence` comparison decides conflict winners — `(priority, interruption_penalty bucket, reclamation_penalty bucket)`, lexicographic (`occ/types.go:53-86`). A higher-precedence proposer *displaces* a lower incumbent, which re-enters the queue with a decremented retry budget.
3. **Barrier** (`occ/cycle.go:136-175`) — `wg.Wait()`, then single-threaded post-processing reconstructs each Need's Bootstrap/Provision/Deficit and the flattened `Claimed` set from the final claimed-state. Workers never write results during the cycle; only the barrier does.

The result is "identical to a priority-sorted single pass at the level of what got assigned" (paper §8 ADR-0029 note) — but parallel, dropping uber-50k cycle p99 from minutes to milliseconds. Two proposal modes (`occ/types.go:37-51`): `ModeIncremental` (partial fill fine — the shortfall buffer absorbs residual) and `ModeAllOrNothing` (gang-scheduled: a Same Need whose `MinUnit` spans multiple machines — partial fill is semantically wrong). `modeFor` (`occ/cycle.go:337`) classifies purely from the Need's static fields; no operator hint.

Same- and Spread-constrained Needs are domain-aware: the pre-pass chooses one domain jointly over creditable + acquirable supply via `ChooseSameBucket`, records it on shared state, and confines both crediting and acquisition (`FindSame`) to it (`occ/seed.go:122-265`). Choosing the domain *twice* — creditable-only in the pre-pass, best-Idle in `FindSame` — was the original Bootstrap↔Reclaim oscillation: Phase 1 assembled cross-domain groups, Phase 3 (correctly strict) reclaimed the off-domain half, and it re-bootstrapped scattered (ADR-0040 + Addendum). M77h extends the pin to machine granularity: a gang's own incumbents are claimed before equivalents under stop-when-covered, so a freshly-matured machine doesn't bump a serving incumbent out of the claimed subset (`occ/seed.go:233-311`, ADR-0051).

### The single attribution

`Phase1Result.Claimed` (`phase1_assign.go:16-25`) is the cycle's final claimed-set: every machine the attribution walk counted toward a Need. **This is the engine's only supply-attribution arithmetic.** Phase 3 takes it as input and reclaims exactly the Configured machines absent from it. The pre-pass walks `SortedClusterStateBucket` in keep-priority order (price asc, reclamation_penalty desc, ID asc) precisely so the unclaimed remainder it leaves is the paper-§8 release-order tail (`occ/seed.go:18-25`). One implementation, two consumers — the phases *cannot* disagree about which machine serves which Need. (`SatisfiedGangs` and the per-Need `ClaimedMachines` are read-only diagnostic surfaces for the M77g gang-oscillation probe; no engine behaviour rides on them — `phase1_assign.go:27-51`.)

## Phase 2 — priority inversions (preempt)

`Phase2` (`phase2_inversions.go:66`) takes Phase 1's `Unsatisfied` and tries to satisfy each by preempting *strictly lower-priority* Configured machines. It does **not** move the machine into the target cluster — it only emits `ActionKindPreempt`; the victim drains to Idle, and *next* cycle's Phase 1 acquires it. That deferral is exactly why Phase 2 is a separate phase: it changes the world for the next cycle, not this one (`phase2_inversions.go:38-43`).

Mechanics: sort unresolved by priority desc so the highest-priority preemptor gets first refusal of the cheapest victims (`phase2_inversions.go:88-93`). For each `(fingerprint, preemptor-priority)` key, build and `VictimScore`-sort the candidate pool *once* and drain it across all sharing Needs (M27 cache, `phase2_inversions.go:95-198`) — collapsing 100×100K MatchProfile calls to 5×100K at the M13 shape. An M30.1 short-circuit skips the whole pool walk when the pool's minimum `AssignedPriority` ≥ the preemptor's (no possible victim), using the snapshot's pre-computed min-priority indexes (`phase2_inversions.go:139-166`, `267-283`). Victims are drained from the sorted head until the freed `EffectiveAllocatable` covers the deficit vector, skipping claimed and too-small (`< MinUnit`) candidates (`phase2_inversions.go:200-234`). Each pick gets `DrainGrace(preemptorPriority, victimPriority)` (`phase2_inversions.go:242`).

Eligibility scoping (M68, joining ADR-0045): a victim must host ≥ one `MinUnit` chunk; Same Needs preempt **only inside their chosen domain** (`phase2_inversions.go:121-133`, `227-231`) — preempting in another domain frees capacity the gang can't assemble on, masking the shortfall; no recorded domain → skip rather than preempt domain-blind; `AcquisitionParked` Needs (ADR-0042) never preempt (`phase2_inversions.go:108-119`). Scoping changes *who* is eligible; the §8 score formula and grace table are paper-locked and untouched. What even preemption can't fix becomes `Unresolved` (`phase2_inversions.go:247-252`), which the shard turns into shortfall seeds (`shard.go:958-962`).

Phase 2 is the one phase that *is* §16 priority arbitration — it allocates contested capacity by priority — so ADR-0046 deliberately **exempts Preempts from every volume rail** (below).

## Phase 3 — reclaim + release

`Phase3` (`phase3_reclaim.go:77`) is two walks over the same snapshot and the same `claimed` set from Phase 1.

**Reclaim — shrinkage only (ADR-0045).** For each cluster with Configured machines, walk its `SortedClusterStateBucket(StateConfigured)` and emit `ActionKindReclaim` for every machine *not* in `claimed` (`phase3_reclaim.go:80-102`). That's it — Phase 3 re-derives no keep-set and carries no satisfaction arithmetic of its own. A Configured machine is excess iff this cycle's single attribution left it unclaimed, i.e. the cluster's bound capacity exceeds its demand by at least that machine. At steady demand Phase 1 acquires until the walk covers, nothing is unclaimed, and **Phase 3 emits nothing** (`phase3_reclaim.go:37-57`). Excess appears only when demand shrinks below what's bound (the §8 scale-down path: CR garbage-collected → next roll-up has fewer Needs → reclaim). The unclaimed remainder is emitted in keep-priority order — the §8 release order (cheapest-per-hour first), so the cap below keeps the head and defers the tail without reordering. Grace is `ReclaimGrace` (10m).

**Release — Idle → Speculative (M73 / ADR-0049).** The §8 second half: walk Idle machines; for any not in `claimed` whose `CapacityType` hold has expired (measured from the snapshot's `IdleSince` stamp against `now`), emit `ActionKindDelete` (`phase3_reclaim.go:104-131`). `ReleasePolicy` (`release.go:56-81`) holds per type: on-demand 10m, spot ~1m, and **bare-metal / reserved / unspecified forever** — the policy type *cannot express* releasing fixed capacity, so no config mistake can delete owned hardware. A zero-value policy releases nothing. The hold window, not a cap, is what spreads releases over time (ADR-0049). The defaults are constants — there is deliberately no flag or chart value; the one override is `shard.Config.ReleasePolicy`, which exists so the sim can pass short holds (sim cycles ≈ time).

### Two gates, and why they differ

- **First-rollup gate (ADR-0036)** applies to *reclaim* only (`phase3_reclaim.go:85-87`). A cluster's empty NeedsTable means "haven't told me yet" during the install/restart window, not "no demand" — reclaiming on it would drain every Configured machine before the operator pipeline catches up, violating static stability. `ClusterReadyFn` (`s.FirstRollupReceived`) gates the per-cluster loop; an empty *reported* rollup correctly resumes reclaim.
- The gate **deliberately does NOT apply to release** (`phase3_reclaim.go:69-76`, ADR-0049). An Idle machine is bound to no cluster and counts for nothing (ADR-0045), so releasing it can never take capacity from anyone. The hold window plus restart-resets-the-clock stamp semantics are the protection; the worst case of a wrong release is one re-buy (a Create, which costs no money — the hourly bill *stops*), not a workload death. The release/re-buy loop *cannot close*: Phase 1 acquires only on deficit, a deficit shields the idle machine into `claimed` for its whole hold, and a re-bought machine is bound and can't re-release without a fresh demand shrink + full hold. Worst-case adversarial churn is one Create per machine per hold period — exactly what the §8 per-tier numbers tune.

## Actuation: from decision to execution

The phases stay pure; the rails live at the shard's actuation boundary (ADR-0046) and can never change *what* the engine wants, only *how fast* it gets it (`safety.go:1-12`).

- **Reclaim blast-radius cap (rail 1).** `capReclaims` (`safety.go:141-193`) caps each cluster to `max(1, ⌊fraction × Configured⌋)` Reclaims per cycle (default 0.05). It keeps the head of Phase 3's release-order emission and defers the tail; the surplus re-derives next cycle (Phase 3 is idempotent against an unchanged snapshot) and, unlike `MaxActionsPerCycle`, does *not* trigger an immediate follow-up — its job is spreading risk over wall-clock time. **Only Reclaim is capped.** Preempts are §16 territory (capping them would make a high-priority workload's access depend on how many other preemptions fired — the throttle §16 forbids); Bootstraps/Provisions are acquisitions whose worst case is spend. The cap converts "fleet gone in one cycle" into ≥20 cycles of emission with 10m drain grace ahead of the first kill — operator reaction time.
- **Reclaim drain mechanism (ADR-0009).** The grace rides the `ReclaimInstruction` to the operator, which cordons synchronously, acks on cordon (the post-condition the shard cares about), then drains *async* in a background goroutine via `policy/v1` Eviction — PDBs enforced at the apiserver, not re-implemented. The shard learns the machine returned to Idle from the next `NodeStateUpdate`, not a deferred ack.
- **Kill switch (rail 3) / dry-run.** Both run the full cycle — reconcile, all three phases, shortfall recording, AvailableCapacity, probes — and suppress only execution (`shard.go:826-866`). The point is observability-preserving paralysis: the operator stops the bleeding and still watches the engine's intentions. Pause counts as `suppressed`, dry-run as `dryrun`; rail 1 and `MaxActionsPerCycle` are skipped under both (nothing executes, so no drain rate to bound).
- **Ingest validation.** `machine.Invariant` bounds provider-declared cost inputs (`price ≥ 0`, `interruption_probability ∈ [0,1]`) at the reconcile/Create-ack boundary; a bad record is rejected loudly and the inventory keeps its last-known-good (`safety.go:204-228`).

## Shortfall: buffer in `pkg/shard`, deficit derived here

There is no standalone shortfall package. The *deficit derivation* lives in the engine: `occ` computes each Need's residual deficit vector (ADR-0027: a resource vector, not a pod count), Phase 1 surfaces it as `UnsatisfiedNeed.Deficit`, Phase 2 carries what preemption couldn't fix into `Phase2Result.Unresolved`. The *buffer* — the aging ledger bounded to 100 per shard, with the paper-§9 age→escalate logic — lives in `pkg/shard` (`report.go`), fed by `recordShortfalls(p2.Unresolved)` (`shard.go:958-962`). The engine derives the number; the shard remembers it across cycles and reports the top-100 to the coordinator. Keeping the ledger out of `pkg/decision` keeps the phases pure (no per-cluster trust/age state) and the engine free of any cross-cycle memory beyond the inventory snapshot itself.

## References

- Papers: bigfleet.md §8 (the three phases, victim score, drain grace, release order), §16 (fixed cost formula, two penalties, priority is the sole throttle).
- ADRs: [0009](../adr/0009-reclaim-uses-policy-v1-eviction-and-async-drain.md) (eviction + async drain), [0027](../adr/0027-rollup-demand-is-a-constrained-resource-request.md) (resource-vector demand), [0029](../adr/0029-phase1-omega-style-occ.md) (Phase 1 OCC), [0036](../adr/0036-phase3-gated-by-first-rollup.md) (first-rollup gate), [0040](../adr/0040-same-domain-attribution-unified.md)/[0041](../adr/0041-sub-machine-same-needs-fold-into-atomic-aggregates.md)/[0042](../adr/0042-addendum-aged-acquisition-parking.md)/[0051](../adr/0051-gang-granular-domain-attribution.md) (Same-domain attribution), [0045](../adr/0045-consumed-capacity-in-the-attribution-model.md) (single attribution / shrinkage-only reclaim), [0046](../adr/0046-actuation-safety-rails.md) (actuation rails), [0049](../adr/0049-idle-speculative-release-hold-window.md) (idle-release hold window).
- Code: `pkg/decision/{cost,match,normalize,action,phase1_assign,phase2_inversions,phase3_reclaim,release}.go`, `pkg/decision/occ/{cycle,seed,types}.go`, `pkg/machine/machine.go` (`EffectiveCost`), `pkg/shard/{shard,safety,report}.go`.
