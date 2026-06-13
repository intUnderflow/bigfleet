# Philosophy-conformance audit — June 2026

**Question (the third June audit):** does the code actually embody the model — ADR-0045 plus the papers — or just pass its tests? Six predicates were audited: clusters demand, BigFleet decides; capacity counts iff bound; never model packing; YAGNI on signals; smartness at the edges; papers as source of truth.

**Method:** 52 agents (2026-06-12): six conformance lenses — satisfaction arithmetic, the Same/gang layer, demand authority, dead signals, edge placement, papers cross-check, plus a state-machine/ledger lens folded under the first — a completeness critic, and an adversarial verifier on every non-conformant claim, instructed to refute against the code. Doc comments were distrusted by instruction. The two critic-originated non-conformant findings (X2, X3) were not separately verifier-passed; everything else non-conformant was.

**Verdict:** the model holds where it is load-bearing. All 38 conformant findings include every locked formula, the binding-as-fulfillment ledger, full-replacement semantics, priority-as-sole-throttle, and the no-packing rule — the verifiers sustained zero refutations against the core arithmetic. The deviations cluster at the edges and the periphery: Phase 2 never joined the ADR-0040/0045 unification, the reference UPC mistranslated demand at the exact seam ADR-0045 makes load-bearing, and the coordinator periphery is the repo's largest concentration of unconsumed plumbing. Counts at audit time: **38 conformant / 14 violations / 12 tensions / 17 dead-signals / 3 refuted by the verifiers** (84 findings; the refuted three were 2 tensions + 1 dead-signal, downgraded with every cited fact verified true but the classification failed). The non-conformant set contains cross-lens duplicates — the utilisation placeholders were independently found by three lenses, the stale fingerprint doc by two — which is convergence, not inflation.

## Resolved since the audit ran

M67 (`83b2fe5`), M68 (`7d28c9f`), and M68b (`ee48adb`) landed the same day and closed 18 of the 43 standing non-conformant findings. Post-resolution: **18 RESOLVED / 12 OPEN / 13 AUTHOR-QUEUE.**

- **M67 — Phase 3 shrinkage-only (ADR-0045):** deleted the keep-set mirror machinery whole — `claimMatching`, `claimMatchingSame`, the four ADR-0027→0042 mirror patches (G5). The ADR-0041 acquirable-consumption ledger was deliberately hoisted into the Phase 1 seed rather than orphaned (the cross-package orphan risk G5 flagged), and Phase 3 now reads Phase 1's claimed-set — one attribution site by construction. Phase 3: 28ms → 0.05ms.
- **M68 — Phase 2 joins the unification:** victim eligibility scoped by the Need's constraint scope — MinUnit chunk filter, Same Needs confined to their Phase-1-chosen domain, parked Needs never preempt (S6, G7). The #309 mystery resolved as two parts: cross-cluster inversions had always fired; intra-cluster non-firing is the bound-counts contract working. Shortfall ledger: same-fingerprint deficits now vector-sum per cycle and age once (S7).
- **M68b — honest edges + dead-signal deletions:** terminal Pods release their CRs (D9); per-Pod resources use kube-scheduler's effective-request rule (D10, half of E7); `pod.Spec.NodeSelector` merges into CR requirements (E7); fractional dollar penalties bucket correctly — `500m` no longer flattens to $0 (D7); roll-up ingest validates symmetrically with the provider boundary, rejecting loudly with `rollups_rejected_total` (D8); the suppressed Draining→Idle frame now reaches the previous cluster's session, so UpcomingNode GC fires (L7, first leg). Dead signals deleted with consumers re-verified: utilisation fractions with proto numbers reserved (S10/C7/L8), OCC introspection (S12), two no-op chart values (C12), three unpopulatable AvailableCapacity spec fields plus `estimatedReadyTime` (C10, C11), ScheduleAnyway spread terms at both demand edges (D13).

The original findings are preserved below for the record; the status column is post-resolution reality.

## Lens 1 — Satisfaction arithmetic

Phase 1's credit pre-pass, deficit computation, and the OCC broker verified as pure bound-vs-demand in each Need's constraint scope — gross machine capacity, no residual math, binding counted from Idle→Configuring. Every `min_unit` consumer is demand-shape arithmetic, none placement feasibility. The deviations lived at the edges of the diff (Phase 2 scoping, the shortfall ledger), both since fixed. (The state-machine/ledger sub-lens additionally verified: no second ledger persists across cycles anywhere; restart rebuilds solely from provider List + the metadata echo; ADR-0033's bind-readiness bit — the strongest historical challenge to "counts from the binding" — was rejected and nothing shipped.)

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| S1 | conformant | Phase 1 credit/deficit is pure bound-vs-demand in constraint scope | `occ/seed.go:46-100`, `occ/cycle.go:264-275` | keep | — |
| S2 | conformant | OCC broker is concurrency control only; gang mode is demand-shape | `occ/broker.go:52-170` | keep | — |
| S3 | conformant | min_unit census: every consumer demand-shape, none packing | full non-test census | keep | — |
| S4 | conformant | Shortfall pipeline carries no second satisfaction arithmetic | `shard.go:915-922`, `report.go:75-99` | keep | — |
| S5 | tension | ADR-0041 foldability: demand-shape sizing or scheduler anticipation? | `normalize.go:61-140,260-313` | author ruling in ADR-0045 vocabulary | AUTHOR-QUEUE |
| S6 | violation | Phase 2 credited preempted capacity with no domain/MinUnit scope | `phase2_inversions.go:135-176` | fix in core | RESOLVED(7d28c9f) |
| S7 | violation | Shortfall ledger collapsed distinct Needs last-writer-wins by fingerprint | `report.go:105-131` | sum per cycle | RESOLVED(7d28c9f) |
| S8 | tension | Creating-window binding gap; dedup rests on pendingActions, not the ledger | `execute.go:177-230`, `occ/seed.go:70` | record accepted-surplus ruling | OPEN |
| S9 | violation | Idle tiebreak omits reclamation_penalty — structurally vacuous as modeled | `poolcache.go:147-154` vs paper §8 | paper-diff or machine-tied source | AUTHOR-QUEUE |
| S10 | dead-signal | ShardSummary utilisation fields never computed, never read | `report.go:18-48` | delete + reserve | RESOLVED(ee48adb) |
| S11 | dead-signal | AssignedNeedFingerprint doc teaches the dead attribution model; Drop J over-keyed | `machine.go:171-183` | rewrite comment, relax key | OPEN |
| S12 | dead-signal | Unconsumed OCC introspection (Result.Conflicted, PrecedenceAt) | `broker.go`, `state.go:195-211` | delete | RESOLVED(ee48adb) |

## Lens 2 — Same/gang layer

The layer splits cleanly: the Phase 1 half (joint domain choice, domain-confined credit/acquisition, the fold, stateless sticky rules) is in-contract "deciding where to fulfill" — verified down to the vector ops as gross bound-vs-demand in the wire's own constraint scope. The Phase 3 half existed solely to mirror keep-sets and died with M67. Parking is the genuine author call: its evidence base was triply stale (artifact demand, artifact seed, pre-M67 Phase 3) and ADR-0045 simultaneously rejects its category and carves it out by name.

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| G1 | conformant | Phase 1 Same machinery is in-contract where-to-fulfill, not packing | `samebucket.go:81-129`, `occ/seed.go:127-193` | keep | — |
| G2 | conformant | ADR-0041 fold is wire-faithful normalization (motivating comment reads packing-flavored) | `normalize.go:61-140` | keep; reword comment | — |
| G3 | conformant | Shortfall buffer is the honest queue; parking never mutes it | `shard.go:915-965` | keep | — |
| G4 | conformant | Gang/phase probes: sanctioned, flag-gated, read-only instrumentation | `shard.go:684-718,1002-1006` | keep; trim reclaim-probe half post-M67 | — |
| G5 | violation | Phase 3 Same-parity keep-set machinery (incl. occ-side orphan risk) | `phase3_reclaim.go:76-322`, `samesupply.go:215-256` | delete with M67 | RESOLVED(83b2fe5) |
| G6 | tension | Parking: fulfillment pacing vs demand judgment; evidence triply stale; post-M67 flips strand, not churn | `shard.go:1035-1097`; ADR-0045:60-65 vs :85-87 | post-M67 re-measure decides | OPEN (M78 re-measure) |
| G7 | violation | Phase 2 domain-blind and parking-blind victim selection for gangs | `phase2_inversions.go:55-197` | scope victims, skip parked | RESOLVED(7d28c9f) |
| G8 | dead-signal | SameSatisfiable plumbing single-consumer | verifier: consumer live + load-bearing (ADR-0042 Addendum d2); deletion-hygiene rider only | — | REFUTED |
| G9 | tension | `CapacityNeed.group` contingent on parking verdict | verifier: parking kept (M66.4), consumer sanctioned; doc-rot fix only | — | REFUTED |

## Lens 3 — Demand authority

The demand spine — UPC → CR → roll-up → Session → NeedsTable.Replace — is faithful: full replacement atomic end to end, newest-wins coalescing, no cluster-side probability or cost signal anywhere, Same protobuf-only, and the ADR-0036 gate correctly refuses to fabricate "zero demand" from silence. The sharp defects were at the reference edge, where translation silently mutated valid demand — all fixed in M68b. The genuine collision is ADR-0046's rail-2 quarantine, which holds a legal full-replacement message on shape.

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| D1 | conformant | Rail-1 reclaim cap meters BigFleet's own actuation, never demand | `safety.go:126-191` | keep | — |
| D2 | conformant | ADR-0036 gate: "hasn't spoken" ≠ "said zero"; explicit zero is honored | `shard.go:310-317,394-416` | keep | — |
| D3 | conformant | NormalizeDemand is feasibility-preserving and packing-free | `normalize.go:61-140,255-330` | keep | — |
| D4 | conformant | Parking holds BigFleet's retries, not the cluster's demand; arbitrated twice | `shard.go:1025-1097`, ADR-0045 consequences | keep | — |
| D5 | conformant | Full replacement honored end to end; no demand-side overrides of provider signals | `needs.go:449-460`, `rollup.go:63-72` | keep | — |
| D6 | tension | Rail-2 quarantine holds a legal full-replacement roll-up on shape — new rows included | `safety.go:40-124`, `shard.go:431-459` | author-arbitrate (keep+amend wording / move to operator / narrow to 0-row) | AUTHOR-QUEUE |
| D7 | violation | Fractional dollar penalties flattened to $0; locked $0.50 bucket unreachable | `rollup.go:304,309` (`AsInt64` bool dropped) | fix at edge | RESOLVED(ee48adb) |
| D8 | violation | Ingest validation asymmetric with provider boundary: parse-to-zero, unbounded bucket enum, invisible rejection | `conv.go:152-156`, `resources.go:69-124` | validate at boundary, loudly | RESOLVED(ee48adb) |
| D9 | violation | Terminal Pods kept CRs — phantom demand pins machines under shrinkage-only Phase 3 | `controller.go:155-203` | fix in UPC | RESOLVED(ee48adb) |
| D10 | violation | Init-container sums inflated aggregates and MinUnit | `controller.go:365-381` | effective-request arithmetic | RESOLVED(ee48adb) |
| D11 | tension | Untranslatable CRs (Gt/Lt) silently skipped; CRD admits operators the wire can't carry | `rollup.go:156-159,319-331`; CRD enum-free | CRD enum + skip counter; Gt/Lt support is a wire decision | OPEN |
| D12 | tension | UPC affinity narrowing (first-term-only) | verifier: already arbitrated in ADR-0024's Scope section; docs nit only | — | REFUTED |
| D13 | dead-signal | ScheduleAnyway spread terms consumed by nothing; degrade aggregation + row-count guard | `candidates.go:419-426` sole consumer is DoNotSchedule | drop at the edge | RESOLVED(ee48adb) |

## Lens 4 — Dead-signals census

Post-M66.1 the demand wire is clean: every `capacity.proto` field has a verified consumer and zero packing vocabulary crosses the wire — the YAGNI predicate holds by construction where it matters most. The dead weight clusters in the coordinator's upward-reporting/instruction surfaces and the CRD side of the M66.1 sweep that was never finished. Metrics discipline is genuinely good.

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| C1 | conformant | capacity.proto: every field consumed, no packing vocabulary | `conv.go:26-77` + consumer census | keep | — |
| C2 | conformant | M72 shard_metadata: all four keys decoded and consumed | `shardmetadata.go:55-77` + consumers | keep; fix stale fingerprint doc | — |
| C3 | conformant | CR.status.phase is closed-loop, not write-only | `rollup.go:175,402,408` | keep; fix concepts.md "Shortfall" phase | — |
| C4 | conformant | Metrics census: near-total consumer coverage; diagnostics have written purposes | dashboards + runner queries | keep | — |
| C5 | conformant | M70b decision audit log: honest fields, named consumer | `safety.go:237-250` | keep | — |
| C6 | dead-signal | Instruction pipeline: no emitter, no-op handlers, three dead wire payloads | `grpc_server.go:214` zero callers; `adapter.go:74-76` | reserve or finish | AUTHOR-QUEUE |
| C7 | dead-signal | ShardSummary upward telemetry stored never read; utilisation never computed | `report.go:25-48`, `grpc_server.go:240-254` | delete/reserve | RESOLVED(ee48adb) utilisation; remainder queued |
| C8 | dead-signal | Quota subsystem has no write path; list RPC can only return empty | `fsm.go:141-143` zero issuers; no SetQuota RPC | delete until designed, or add write path | AUTHOR-QUEUE |
| C9 | dead-signal | Session fields the peer discards: ack metadata, ttl_seconds, nodes_started, BootstrapRequest.cluster_id | `stream.go:337-339`, `session.go:335,377-389` | implement ttl or reserve | OPEN |
| C10 | dead-signal | AvailableCapacity CRD: 3 orphaned spec fields; required cost field never populated | `availablecapacity_types.go:42-62`; `upcoming.go:251-273` | finish M66.1; arbitrate cost | RESOLVED(ee48adb) orphans; cost rides AC arbitration |
| C11 | dead-signal | UpcomingNode.status.estimatedReadyTime permanently nil, fictional consumer in doc | `upcomingnode_types.go:70-73` | delete | RESOLVED(ee48adb) |
| C12 | dead-signal | Chart documents rbac.create / podMonitor.enabled, renders neither | `values.yaml:130-144`, no template refs | delete or implement | RESOLVED(ee48adb) |
| C13 | tension | CapacityType claims to drive an idle-hold policy that doesn't exist | `machine.go:73` vs zero implementations | fix comment or fold into the M73 idle-release ADR | OPEN (M73) |

## Lens 5 — Edge placement

Placement discipline is strong where already legislated: paper-locked policy fixed in core exactly as §8/§16 demand, ADR-0046 rails chart-owned at the actuation boundary, workload policy resolving through documented override chains at the edge, with the out-of-tree provider edge as the fully-realized model. The weaknesses were at the demand seam ADR-0045 just made load-bearing — the reference UPC mistranslating demand, and wire-contract text the core does not honor.

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| E1 | conformant | Drain-grace table and ReclaimGrace paper-locked in core; cluster lever (PDBs, penalties) at edge | `cost.go:102-122` = paper §8 verbatim | keep | — |
| E2 | conformant | ADR-0046 rails at the actuation boundary, deployment-owned via chart | `safety.go:35-57`, chart values | keep | — |
| E3 | conformant | Penalty placement: bucket vocabulary in core, dollar values at edge | `needs.go:30-60`, `controller.go:75-87` | keep | — |
| E4 | conformant | Operator policy points are seams with override layers (chart lags binary flags) | `operator.go:80-112`, bootstrap template chain | keep; add chart passthrough | — |
| E5 | conformant | Provider edge is the realized template of smartness-at-the-edges | `--provider-addr` + conformance + author guide | keep; mirror for demand edge | — |
| E6 | tension | Victim weights claim configurability no surface delivers; DrainSpeed term mathematically inert | `cost.go:24-27`, `phase2_inversions.go:26,33,148` | constants-until-evidence vs rail-style plumbing | OPEN |
| E7 | violation | UPC dropped pod.Spec.NodeSelector and inflated init resources — manufactured satisfied-but-stuck | `controller.go:318-340,365-381`; plan even required nodeSelector | fix in UPC | RESOLVED(ee48adb) |
| E8 | violation | UPC self-description still documents the pre-ADR-0039 unschedulable filter — at the seam custom CR sources copy | `cmd/.../main.go:1-4`, user-stories.md:7, scaling-guide.md:71 | doc fix to ADR-0039 contract | OPEN |
| E9 | dead-signal | BootstrapBlobResponse.ttl_seconds produced, contractually promised, never consumed (structurally unenforceable today) | shard.proto:93-95; `session.go:329-336` | enforce or delete claim | OPEN |
| E10 | violation | ReclaimInstruction "node names" are machine IDs; node_name never sent — written contract diverges from the wire | shard.proto:110-111 vs `session.go:361-375`; cordon NotFound swallowed | arbitrate identity convention or plumb node names | AUTHOR-QUEUE |
| E11 | tension | pkg/operator is a sealed binary; demand edge lacks the guide/conformance artifacts the provider edge has | unexported session/translation machinery; no operator-author guide | document the CR seam or open Go seams | OPEN (seam recorded in ADR-0045 future-work; artifacts unbuilt) |

## Lens 6 — Papers cross-check

ADR-0045's central claim — restoration, not revision — checks out on a full line-by-line read of both papers: nothing mandates schedulability assurance, packing, or residual math, and the anchor sentences state the sharpened model verbatim. "The papers need no diff" is slightly overstated: the two sentences whose loose readings produced the deleted defect classes each deserve a Revised-at-implementation blockquote. The reverse sweep found the §9 coordinator response exists only as dead plumbing, and AvailableCapacity is the contract's one genuinely speculative signal.

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| P1 | conformant | §13's scale-down line IS shrinkage-only Phase 3 (ADR-0045 mis-cites it as §8) | bigfleet.md:116 | keep; one-line cite fix | — |
| P2 | conformant | No paper text mandates schedulability assurance; no-packing anchors explicit and repeated | fleet-scale §5:34, bigfleet §2:12 | keep | — |
| P3 | conformant | min_unit consumed strictly as the §7 per-machine floor | `occ/seed.go:85`, `match.go:21-23` | keep | — |
| P4 | conformant | ADR-0039 revision blocks more load-bearing under M67, not stale | fleet-scale:75-77 | keep | — |
| P5 | conformant | §9 shard-side shortfall matches paper text field-for-field | `report.go:56-131` | keep | — |
| P6 | conformant | §16 priority-sole-throttle holds; coordinator "quota" is supply-side slices, never demand admission | bigfleet:124 vs :51; no admission predicate | keep | — |
| P7 | tension | §8 "reclaim excess" leaves timing undefined; the keep-set misreading is documented history | bigfleet.md:84; four blockquote precedents | paper blockquote (ADR-0045) | AUTHOR-QUEUE |
| P8 | tension | §7's "Σ machine.Allocatable" omits the bound-to-requesting-cluster scope | fleet-scale:89 vs ADR-0045:23-26 | amend revision block | AUTHOR-QUEUE |
| P9 | dead-signal | §9 coordinator response wire-plumbed end to end with zero consumers; proto comment overclaims | coordinator.proto:215-216; `LatestShortfalls` zero callers | schedule the milestone or trim | AUTHOR-QUEUE |
| P10 | tension | AvailableCapacity: paper-contract CRD, real per-cycle cost, no programmatic consumer | fleet-scale §6.2; `available_capacity.go:106-164` | keep+document consumer, or demote to opt-in | AUTHOR-QUEUE |
| P11 | dead-signal | CONFIDENCE_LOW unreachable: paper promises four levels, shard emits three | `available_capacity.go:191-196` | define Low or paper-diff the ladder | AUTHOR-QUEUE (with P10) |
| P12 | tension | Rail-2 quarantine unreflected in the paper's "full replacement, every cycle" — load-bearing under shrinkage-only Phase 3 | fleet-scale:93; `safety.go:93-124` | optional paper blockquote | AUTHOR-QUEUE (with D6) |

## Lens 7 — State-machine ledger

ADR-0045's core claim — binding is the act of fulfillment and the machine state machine is the only supply ledger — is true in the engine: the binding is written at Idle→Configuring before any node exists, every consumer of the diff counts exactly {Configured, Configuring}, all other bookkeeping is cycle-scoped, and a restarted shard rebuilds solely from provider List/Get plus the Configure-time echo. The one violation was at the ledger's cluster-side edge, not its core.

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| L1 | conformant | Binding atomic; bound set {Configured, Configuring} agrees at every site (by convention, not predicate) | `execute.go:282-285`, `occ/seed.go:70-91` | keep; extract shared IsBound helper | — |
| L2 | conformant | All bookkeeping outside the state machine is cycle-scoped views or actuation dedup | `occ/cycle.go:76-78`, pendingActions | keep | — |
| L3 | conformant | Restart rebuilds the ledger from List + shard_metadata echo only; dataDir holds the epoch alone | `reconcile.go:148-163`, `restart_test.go` | keep | — |
| L4 | conformant | ADR-0033 bind-readiness was rejected, nothing shipped — the rule survived its strongest challenge | ADR-0033 Status: Rejected; no BindReady anywhere | keep; cite at seed.go | — |
| L5 | conformant | Transitional states + Failed excluded identically everywhere | zero pkg/decision refs to Draining/Deleting/Failed/Creating | keep | — |
| L6 | conformant | Parking ledger holds demand-class aging, never binding facts | `shard.go:252-259`; key has no machine IDs | keep; M67 deletes its Phase 3 arm | — |
| L7 | violation | UpcomingNode terminal lifecycle keyed on frames the shard never emitted; drain-status walk dead on the wire | `shard.go:1321-1324` + `execute.go:438-447`; node_name never set | emit terminal frame; settle node identity | RESOLVED(ee48adb) frame routing; identity leg AUTHOR-QUEUE |
| L8 | dead-signal | Utilisation placeholder zeros on the coordinator wire (= S10/C7) | `report.go:18-19,44-47` | delete | RESOLVED(ee48adb) |
| L9 | dead-signal | AssignedNeedFingerprint's documented ledger role no longer exists (= S11) | `machine.go:171-183`; sole read is Drop J dedup | re-document as dedup tag | OPEN |

## Completeness critic

The critic audited what the six-lens framing structurally missed: the coordinator (headline-conformant — zero fulfillment decisions, but the largest dead-signal concentration), the scaletest runner (whose steady-state gate mechanically asserts the packing promise ADR-0045 disclaims), Phase 2 preemption (squarely inside the model), and the conformance suite (clean — it positively pins binding-as-fulfillment on out-of-tree providers).

| ID | Class | Finding | Evidence | Disposition | Status |
|---|---|---|---|---|---|
| X1 | conformant | Coordinator makes zero fulfillment decisions; static stability by construction | `EnqueueInstruction` zero callers; phases run shard-local | keep | — |
| X2 | dead-signal | Coordinator periphery: unwritable quota, zero-utilisation, unread shortfall copy, no-op stubs acking ACCEPTED | `fsm.go:285-286`, `adapter.go:74-76` | arbitrate per item | utilisation RESOLVED(ee48adb); quota/stubs AUTHOR-QUEUE |
| X3 | violation | Runner steady-state gate fails runs below binds ≥ 99% of pods — the old model as a mechanical pass/fail | `scaletest-runner/main.go:1191-1236` | execute the gate redefinition ADR-0045 directs | OPEN (M77 gate swap; spec in plan §12) |
| X4 | conformant | Phase 2 preemption inside the model: bound-vs-demand residuals in, single ledger out, no busy-bit | `phase2_inversions.go:137-179` | keep | — |
| X5 | conformant | Conformance suite pins binding-clears-on-Drain on providers; no old-model expectations | `metadata_test.go:94-125` | keep | — |

## The refuted three

Each verifier confirmed every cited fact and rejected the classification: G8 (SameSatisfiable is live and load-bearing — a deletion-hygiene rider on parking, not a dead signal), G9 (`group` has a sanctioned, behavior-affecting consumer since parking was kept), D12 (the UPC's affinity narrowing was already arbitrated in ADR-0024's Scope section). Residue: the stale `needs.go` Group doc comment ("from CR ownerReferences" — false) flagged by both G9 and D5 is still unfixed.

## Author arbitration queue

Mirrors plan §12's philosophy-audit arbitrations; the loop proceeds on everything not listed here.

1. **ADR-0041 foldability ruling** (S5) — demand-shape sizing vs scheduler anticipation; one paragraph in ADR-0045 vocabulary settles it. The fold cannot move to the operator (no catalog knowledge), which is the strongest argument for sanctioning it in core.
2. **ADR-0046 roll-up quarantine vs the full-replacement hard rule** (D6, P12) — keep with amended rule wording, move the guard to the operator edge, or narrow to the pure-empty case. The rule text and the rail currently contradict each other silently.
3. **§9 coordinator shortfall response** (P9, C6, X2) — schedule it as a milestone (papers win) or trim the dead plumbing and paper-diff. Today's half-pipeline is the worst of both.
4. **AvailableCapacity** (P10, P11, C10 cost residue) — document it as the designated smarter-operator input (and define CONFIDENCE_LOW, populate or drop Cost), or deprecate. Do not silently delete; the paper names it.
5. **Two one-line paper blockquotes** (P7, P8) — §8 excess timing (excess = bound − demand, on shrinkage only) and §7's Σ scoped to the requesting cluster's bound machines. Both sentences have proven misreadings.
6. **reclamation_penalty idle-tiebreak omission** (S9) — paper-diff (tiebreak collapses to price under workload-stamped semantics) or open the provider contract to a machine-tied value. Do not sort on a field that is always zero.
7. **machine_id ≡ node-name identity convention** (E10, L7 residue) — declare it a bootstrap-contract obligation (and make cordon NotFound loud) or plumb real node names through NodeStateUpdate. The reference drain path only coheres under the undocumented convention.
8. **Quota subsystem + instruction pipeline deletion scope** (C8, C6, X2) — delete until designed, or build the missing write/delivery halves. Includes flipping the no-op stubs to reject so unimplemented instructions are visible on the wire.

The full per-finding evidence, verifier verdicts, and refutation notes remain in the audit run's archive; this document is the distilled, post-resolution record.
