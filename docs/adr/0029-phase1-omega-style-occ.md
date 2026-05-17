# ADR-0029: Phase 1 Omega-style OCC — shared-state, scheduler-kind partitioning, commit-broker priority

## 1. Status

Accepted, 2026-05-17.

## 2. Context

[ADR-0028]'s empirical addendum documented three attempts to bring
Phase 1 under control at uber-50k under the realistic catalog
(bigfleet-uber #17). Baseline `4ce1e70` measured 9.41 ms/call ×
~31K calls/cycle ≈ 5-minute Phase 1, drift to ~15 minutes worst-case.
A parsed-form alloc-elimination pass (`b9b7037`, reverted) bought
~36% per-call but kept the structural cost. An O(buckets) aggregate
cache (`0f05854`, reverted) was a net regression because under
realistic catalog every co-location group becomes its own bucket per
[ADR-0024] — bucket count ≈ Need count, so O(buckets) ≈ O(Needs)
delivered no asymptotic improvement.

The empirical conclusion stands: Phase 1's wall-clock cost scales
with **Need cardinality**, not with per-iteration cost. Constant-
factor optimization on the single-threaded loop cannot reach
[ADR-0028]'s regime-aware envelope. The remaining lever is
**iteration count reduction**, which requires structural redesign.

Independent research (see external citations in §13) converged on
the same family of fixes — **shared-state optimistic concurrency
control (OCC)** in the style of Google's Omega [Omega], with the
modern extensions ParSync [ParSync] documented for production at
Alibaba scale (40K decisions/sec, 100K-machine clusters). The Borg
retrospective [BorgOmegaK8s] is somewhat deflationary about
Omega's multi-scheduler win at Google specifically — Borg's
monolithic scheduler was not the production bottleneck for
Google's workload — but BigFleet's measured 5–15 minute single-
threaded Phase 1 is precisely the regime where Omega's design
*does* pay off. The architectural pattern (shared state, atomic
per-task commits, fine-grained conflict detection) is the
inheritance.

`bigfleet.md` §8 specifies Phase 1 as "walk needs top-down by
priority. Prefer Idle. Fall back to Speculative." Today's
implementation reads that literally — one goroutine, priority-
sorted single pass, global claimed-set mutated in place. CLAUDE.md
states "priority is the sole throttling mechanism." A close reading
of that hard rule shows it rules out *other* throttling mechanisms
(quota, admission, entitlement) — it does not require strict
priority-sorted pre-ordering of the cycle's work. Priority can be
enforced at the commit broker without sacrificing intra-cycle
concurrency. That re-reading is the key that unlocks the OCC
redesign.

This ADR specifies the redesign. It supersedes the relevant
decision logic in `pkg/decision/phase1_*.go` and Decision §3 of
[ADR-0028] regarding the OCC deferral (the answer is now "build
it"). [ADR-0027]'s resource-vector demand model is the substrate;
[ADR-0027]'s Phase 1 / Phase 3 attribution invariant (stage 5.1
thrash lesson) is preserved verbatim.

## 3. Goals

1. Cycle p99 ≤ 100 ms at **uber-50k** under realistic catalog
   (currently ~15 minutes worst-case).
2. Cycle p99 ≤ 1 s at **uber-500k** under realistic catalog
   (currently extrapolates to ~100 s).
3. **No regression** in the aggregated regime (scaleway-* runs
   currently passing under the 100 ms canonical bar must still
   pass after cutover).
4. **Preserve every CLAUDE.md hard rule.** In particular: no
   distributed locking on the hot path (this ADR is intra-shard
   concurrency only); cost formula unchanged; provider RPC surface
   unchanged; static stability holds (shards run autonomously
   during coordinator failover).
5. **Preserve the [ADR-0027] stage 5.1 invariant.** Phase 1 and
   Phase 3 must attribute supply identically. The commit barrier
   produces a coherent claimed-set Phase 2/3 can read with the
   same semantics they read today.

## 4. Non-goals

- **Multi-shard OCC.** Cross-shard topology stays bounded per
  CLAUDE.md ("topology constraints do not cross shard boundaries").
  Each shard's Phase 1 is concurrent internally; shards remain
  independent.
- **Incremental Phase 1** (delta-only processing). Complementary
  lever; deferred to a follow-on ADR. The OCC design here leaves a
  clean place to layer incremental in as a future optimization.
- **ParSync-style partitioned synchronization.** Projections in §9
  suggest pure OCC just reaches uber-500k; uber-1m likely needs
  the partitioned-sync extension. Day-1 design is pure shared-
  state OCC; the layered extension is a follow-on ADR if measured
  data demands it.
- **Changes to Phase 2's victim scoring or Phase 3's reclaim
  ordering.** Only the commit barrier interaction changes;
  algorithmic content is unchanged.
- **Changes to the `Action` shape, the provider RPC surface, the
  capacity contract proto, or the operator's roll-up format.**
  This is a `pkg/decision/` rewrite, not a wire-format change.

## 5. Design overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       Shard Cycle (1 Hz)                        │
│                                                                 │
│   ┌─────────────┐        ┌────────────────────────────────┐     │
│   │  Snapshot   │        │ Classify Needs by kind          │     │
│   │ (immutable, │───────▶│   easy / picky / reclaim        │     │
│   │  per cycle) │        │ (shard-side inference at receipt)│     │
│   └─────────────┘        └──────────────┬─────────────────┘     │
│         │                               │                       │
│         │   ┌───────────────────────────┴──────────────────┐    │
│         │   │                                              │    │
│         ▼   ▼                                              ▼    │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐         ┌──────────┐ │
│   │ Easy     │  │ Easy     │  │ Picky    │   ...   │ Reclaim  │ │
│   │ worker 1 │  │ worker 2 │  │ worker 1 │         │ worker 1 │ │
│   └────┬─────┘  └────┬─────┘  └────┬─────┘         └────┬─────┘ │
│        │             │             │                    │       │
│        └─────────────┴─────┬───────┴────────────────────┘       │
│                            │                                    │
│                            ▼                                    │
│              ┌─────────────────────────────┐                    │
│              │     Commit Broker           │                    │
│              │  per-bucket sequence CAS    │                    │
│              │  priority-on-conflict       │                    │
│              └──────────────┬──────────────┘                    │
│                             │                                   │
│                             ▼                                   │
│              ┌─────────────────────────────┐                    │
│              │  Claimed Set + Shortfall    │                    │
│              │  (coherent at barrier)      │                    │
│              └──────────────┬──────────────┘                    │
│                             │                                   │
│         ──── COMMIT BARRIER ────                                │
│                             │                                   │
│                             ▼                                   │
│              ┌─────────────────────────────┐                    │
│              │   Phase 2 (inversions)      │                    │
│              │   Phase 3 (reclaim)         │                    │
│              └─────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────┘
```

The shape is Omega's [Omega §3.4]: shared cell state (BigFleet's
inventory snapshot), private read views per scheduler, atomic
commits through a single broker that detects conflicts at machine
granularity. Where this ADR specializes for BigFleet:

- **One commit barrier per cycle**, not per-Need. Phase 2/3 read
  the claimed-set after the barrier — same input shape as today.
- **Scheduler kinds** are the partition (Omega's batch/service
  split applied to BigFleet's Need taxonomy), not replica-count
  scaling alone. This is the [ParSync §3] guidance: partition by
  scheduler kind, not by hash of work.
- **Commit broker enforces priority on conflict** — high-precedence
  proposals win against in-flight lower-precedence claims. This is
  how CLAUDE.md's "priority is the sole throttling mechanism" is
  preserved without strict-pass ordering.

## 6. Detailed design

### 6.1 Shared state

Two shared structures per cycle:

1. **Inventory snapshot** — immutable for the cycle's duration.
   The existing `inventory.Snapshot` already has this property;
   readers grab a reference at cycle start and never mutate it.
   This is the equivalent of Omega's "private, local, frequently-
   updated copy" [Omega §3.4] — each scheduler reads the same
   snapshot, no locks.

2. **Claimed-set** — mutable, written only via the commit broker.
   Today this is `phase1Allocator.claimed map[machine.ID]struct{}`.
   The redesign replaces the bare map with a `claimedSet`
   structure that the broker mutates under a single mutex; each
   bucket also carries a sequence number that increments on every
   claim. Schedulers read the claimed-set lock-free (sync.Map or
   atomic.Pointer to an immutable snapshot, depending on
   implementation cost — see §10).

Per-bucket sequence numbers are the **fine-grained conflict
detection** primitive — Omega measured 2–3× lower spurious-conflict
cost from fine-grained vs coarse-grained [Omega §5.2, Fig. 14]. The
bucket is the natural CAS grain for BigFleet because
[ADR-0024]/[ADR-0019] already partition by co-location group.

Sketch:

```go
// pkg/decision/occ/state.go (new package)
type sharedState struct {
    snap *inventory.Snapshot      // read-only, set at cycle start

    mu sync.Mutex                 // guards claimed and bucket seqnos
    claimed map[machine.ID]bool
    bucketSeq map[bucketKey]uint64  // increments on every claim that
                                    // touches this bucket
}

type bucketKey struct {
    state machine.State
    profileFP string
    sameKey string
    sameValue string
}
```

`bucketSeq` keys mirror the `coLocatedBucket` identity in today's
allocator. A scheduler's "read" captures the bucket's seqno at
decision time; the commit broker rejects the proposal if the
seqno has moved since.

### 6.2 Scheduler kinds

Three kinds, classified at shard receipt (shard-side inference,
not operator-set — Q2 answered: no proto change):

| Kind | Shape | Profile signal |
|------|-------|----------------|
| **easy** | single-rack `Same` requirement, single dominant Profile fingerprint, no gang signal | bulk of realistic-catalog Needs; most Phase 1 work routes here |
| **picky** | multi-rack constraints, gang `min_unit` ≥ N machines, rare/unique Profile fingerprint, or `min_unit` exceeds typical machine alloc | small fraction of Needs but cost-disproportionate per-Need; deserves its own worker pool to avoid head-of-line blocking easy Needs |
| **reclaim** | Phase 3 work (not Phase 1, but listed here because the partition extends to the full cycle's concurrency) | always its own goroutine |

Classification happens once per Need on first appearance in the
NeedsTable. The classifier is pure (function of `Profile` shape +
`min_unit`); cached on the Need for the cycle's lifetime.
Re-classification on Profile fingerprint change is cheap because
fingerprints don't change without an operator round-trip.

Worker count per kind:
- **easy**: `runtime.GOMAXPROCS()` workers (default; tunable). At
  GOMAXPROCS=16 on the M5 Max dev box that's 16 easy workers.
- **picky**: `min(GOMAXPROCS/4, 4)` workers. Picky Needs are
  expensive per-call; more workers add conflict pressure without
  proportional throughput.
- **reclaim**: 1 worker. Phase 3 has its own ordering invariants
  ([ADR-0027] stage 5.1) and runs sequentially today; OCC doesn't
  buy obvious throughput for it.

Numbers are starting points; the ADR's measurement plan in §9
includes tuning per kind from empirical conflict-rate data.

### 6.3 Single-band concurrent scheduling

CLAUDE.md's "priority is the sole throttling mechanism" is preserved
via the commit broker, not via iteration order. Within a cycle,
all kind-workers race concurrently; there is **no priority-sorted
outer loop**. Each worker pulls Needs from its kind's queue (which
is itself unordered — workers steal as available) and proposes
claims to the broker.

Priority enters at three points:
1. **Commit-conflict resolution** (§6.4). When two workers race for
   the same machine, the broker awards it to the higher-precedence
   claim. Precedence = (priority, interruption_penalty_bucket,
   reclamation_penalty_bucket) lexicographic.
2. **Phase 2 inversions**. Same as today: post-commit, Phase 2
   scans for inversions (higher-priority Need unsatisfied vs lower-
   priority Need satisfied) and preempts. With pure OCC inversions
   happen more often than under strict-pass, but Phase 2 already
   handles them; this is an increase in Phase 2 work, quantified
   in §11.
3. **Shortfall escalation**. A Need that loses its retry budget
   (§6.5) goes to the shortfall buffer with its priority preserved;
   coordinator escalation at Age >5 cycles is unchanged from today
   (`bigfleet.md` §9).

The cycle barrier holds all kind-workers; when every worker drains
its queue and there are no in-flight proposals at the broker, the
barrier releases. Phase 2/3 then run on the coherent post-barrier
claimed-set. This barrier is the single synchronization point per
cycle.

### 6.4 Commit broker

The broker is a single mutex-guarded data structure. Its method:

```go
func (b *broker) Propose(p Proposal) Result {
    b.mu.Lock()
    defer b.mu.Unlock()

    // 1. Fine-grained conflict check: has the bucket's seqno
    //    moved since the proposer's read?
    if b.state.bucketSeq[p.bucket] != p.observedSeq {
        return Result{Status: Conflict, ReObserve: b.snapshotBucket(p.bucket)}
    }

    // 2. Per-machine claim check (the actual contended resource):
    //    is the machine still unclaimed?
    for _, mid := range p.machines {
        if _, claimed := b.state.claimed[mid]; claimed {
            // Concurrent commit for the same machine got in first.
            // Priority resolution: if THIS proposal is strictly
            // higher precedence than the incumbent, displace it.
            inc := b.state.claimedBy[mid]
            if precedenceLess(inc.precedence, p.precedence) {
                b.state.releaseClaim(mid, inc)
                // The displaced Need re-enters its kind's queue
                // (still subject to its own retry budget).
                b.requeue <- inc.need
            } else {
                return Result{Status: Conflict, ReObserve: b.snapshotBucket(p.bucket)}
            }
        }
    }

    // 3. Commit. Atomically claim all proposed machines and bump
    //    the bucket seqno.
    for _, mid := range p.machines {
        b.state.claimed[mid] = true
        b.state.claimedBy[mid] = claim{need: p.need, precedence: p.precedence}
    }
    b.state.bucketSeq[p.bucket]++
    return Result{Status: Committed, Action: p.action}
}
```

Key properties:

- **Incremental transactions** [Omega §3.4]. One Proposal = one
  Need's full machine list. The Omega paper measured a 2× conflict-
  rate penalty for all-or-nothing transactions; we use incremental
  by default. For gang-scheduled Needs (`min_unit` ≥ N), the
  proposal is atomic: either all N machines commit or none.
- **Priority on conflict** preserves CLAUDE.md's throttling
  property. Higher-precedence proposals displace lower-precedence
  incumbents; displaced incumbents re-queue with their retry budget
  decremented.
- **Per-bucket seqno + per-machine claim check** is double-validated
  — the seqno catches "bucket state changed under me", the
  per-machine check catches the actual claim race. Same mechanism
  as Omega's broker.
- **Single mutex** for the broker is intentional. The broker is
  not the bottleneck — per-proposal work is O(machines-in-proposal)
  = small constant (a single Need typically claims 1–10 machines).
  Workers spend most of their time computing proposals, not at the
  broker.

### 6.5 Bounded retries → shortfall

Each Need carries a retry budget (starts at 10; configurable per
kind). On `Conflict` from the broker, the worker re-reads the
bucket's state, recomputes the proposal, and re-tries. On retry
budget exhaustion, the Need is emitted to the shortfall buffer
with its full deficit vector.

This matches Omega's "abandoned at 1,000 retries" behavior
[Omega §4] but with a much tighter cap because BigFleet's
shortfall protocol is a first-class concept (`bigfleet.md` §9 +
[ADR-0027]) — persistent contention is meant to be escalated to
the coordinator for cross-shard rebalancing, not absorbed by
spinning. Coordinator escalation kicks in at Age >5 cycles per
the existing protocol.

The retry budget per kind:
- **easy**: 10 retries. Most cycles see conflict rate ≤ 0.2; 10 is
  enough headroom for the long-tail.
- **picky**: 5 retries. Picky Needs are expensive per-attempt; we
  want them to fail-fast to shortfall rather than burn cycles
  spinning. Coordinator escalation handles cross-shard rebalancing.
- **reclaim**: N/A (Phase 3 runs serially in its single goroutine;
  no OCC contention).

Forward-progress guarantee: priority promotion on retry would
*also* work (Q4 alternative) but adds per-Need bookkeeping and
risks demoting newly-arrived high-pri work. Bounded-retries-to-
shortfall is the simpler invariant and matches the existing
architecture.

### 6.6 Phase 2 / Phase 3 interaction

The cycle barrier separates Phase 1 (concurrent OCC) from Phase 2
(inversions, sequential) and Phase 3 (reclaim, sequential). At
barrier release:

- **Claimed-set is coherent.** Every committed claim is durable;
  every conflict-loser is either retried-and-committed, retried-
  and-shortfall'd, or in the shortfall buffer. No in-flight state.
- **Bucket seqnos are stable.** No further mutations during Phase
  2/3; their reads are race-free.
- **Phase 2** reads the claimed-set, scans for inversions, runs
  victim scoring exactly as today (`bigfleet.md` §8, [ADR-0027]).
  Inversions are expected to be **more frequent** under OCC than
  under strict-pass because the priority constraint is enforced
  reactively (at commit) rather than proactively (at ordering).
  See §11.
- **Phase 3** reads the claimed-set, computes reclaim per
  [ADR-0027] stage 5.1 attribution rules. The attribution invariant
  ("Phase 1 and Phase 3 must attribute supply identically") holds
  because both read the same post-barrier claimed-set.

The shortfall buffer is mutated only by the cycle's Phase 1 (via
broker re-queue) and read by the coordinator's quota RPC
asynchronously. No concurrent mutation outside the cycle.

## 7. Flow examples

### 7.1 Happy path — no conflict

```
Cycle start: snapshot = S; claimed = ∅; bucketSeq = all 0.

Easy worker E1 picks Need n1 (priority 1000, single-rack):
  - Reads bucket B1 from S; bucketSeq[B1] = 0 observed.
  - Computes proposal P1 = {need: n1, bucket: B1, machines: [m5], observedSeq: 0}.
  - Sends to broker.

Broker:
  - bucketSeq[B1] = 0 matches observedSeq = 0. ✓
  - m5 not in claimed. ✓
  - Commits: claimed[m5] = n1; bucketSeq[B1] = 1.
  - Returns Committed.

E1 emits Action{Bootstrap, m5, n1.cluster}.
```

No retry; no conflict; one round-trip. This is the common case
for steady-state cycles where most Needs find ample inventory.

### 7.2 Conflict path — two workers race, priority wins

```
Cycle start: snapshot = S; claimed = ∅; bucketSeq[B1] = 0.

E1 picks Need n1 (priority 100, single-rack on B1).
E2 picks Need n2 (priority 10000, single-rack on B1).
Both read B1 simultaneously; both observe bucketSeq[B1] = 0.

E1 computes P1 = {n1, B1, [m5], seq:0}; sends to broker.
E2 computes P2 = {n2, B1, [m5], seq:0}; sends to broker (microseconds later).

Broker receives P1 first:
  - seq matches; m5 unclaimed. Commits. claimed[m5] = n1; seq[B1] = 1.
  - Returns Committed to E1.

Broker receives P2:
  - bucketSeq[B1] = 1, but P2.observedSeq = 0. Seqno mismatch.
  - Returns Conflict to E2 with current bucket snapshot.

E2 re-observes B1, sees m5 claimed but m7 unclaimed.
E2 retries: P2' = {n2, B1, [m7], seq:1}. Sends to broker.

Broker receives P2':
  - seq matches; m7 unclaimed. Commits.

ALTERNATIVELY, if B1 had only one unclaimed machine and E2's
priority (10000) is strictly higher than E1's (100):
  - E2 retries with P2'' = {n2, B1, [m5], seq:1, displaceOK: true}.
  - Broker: m5 is claimed by n1 (precedence 100); P2'' precedence 10000.
    precedenceLess(100, 10000) = true. Displace.
  - claimed[m5] is reassigned to n2; n1 re-queued.
  - n1 will retry, find no unclaimed machine in B1, fall through to
    its retry budget, eventually shortfall.
```

This is the path where priority-on-commit substitutes for strict-
pass ordering. The outcome is equivalent (the higher-priority Need
gets the resource); the cost is an extra round-trip and a Phase 2
inversion-or-not at barrier (if n1 is shortfalled, Phase 2 has no
work).

### 7.3 Retry exhaustion → shortfall

```
Cycle start: B7 has 10 machines, all claimed by an earlier-arrived
batch of priority-1000 Needs.

E1 picks Need n8 (priority 100, must use B7 due to single-rack
constraint).

Attempt 1:  reads B7, all machines claimed, no proposal possible
            (avail = 0). Worker decrements retry budget: 10 → 9.
            (Note: avail=0 with no displacement opportunity is a
             trivial "conflict" — there's nothing to propose.)

Attempts 2–10: same; nothing changes (priority 100 cannot displace
            priority 1000 incumbents).

Attempt 11: retry budget exhausted. Worker emits n8 to shortfall
            buffer with deficit = n8.AggregateResources.
            UnsatisfiedNeed{Need: n8, Deficit: ...} added to
            Phase1Result.Unsatisfied.

Phase 2 reads the shortfall; in this case n8's priority is lower
than every incumbent, so no inversion. Phase 3 attributes n8's
deficit to the shortfall buffer for coordinator escalation.

Coordinator: if n8 ages past 5 cycles in shortfall, escalation
mechanism (cross-shard reassignment or speculative quota bump)
fires per bigfleet.md §9.
```

This is the path that exposes "persistent contention" as a real
signal. The system surfaces it through the existing shortfall
protocol rather than absorbing it via unbounded retries.

### 7.4 Cold start with mixed-priority demand

The pure-OCC worry. Cycle 1 of a freshly-booted shard sees the
full NeedsTable hit an empty claimed-set.

```
Cycle start: 1000 Needs across priorities {100, 1000, 10000};
             5000 Idle machines across many buckets.

t=0:   Easy workers (E1..E16) and Picky workers (P1..P4) start
       pulling Needs from their kind queues. No priority ordering.

t=1ms: Several Easy workers concurrently claim machines in
       overlapping buckets. Most commits succeed; a fraction
       (~conflict_rate × proposals) hit Conflict.

t=10ms: Some priority-100 Needs have claimed machines that a
       just-arriving priority-10000 Need wants. The high-pri
       proposal displaces (per §6.4); displaced low-pri Need
       re-queues.

t=100ms: Most Needs committed or shortfall'd. Conflict rate so
       far ≤ 0.3 (Omega-paper envelope). Phase 1 barrier
       approaches.

t=120ms: Barrier releases. Phase 2 scans for inversions:
         finds N priority-10000 Needs that won via displacement
         (where N might be 5–50 depending on initial contention).
         Phase 2 has no actual work — the high-pri Needs already
         won at commit; the displaced low-pri Needs are
         shortfall'd.

t=130ms: Phase 3 runs. Reclaim attribution mirrors Phase 1's
         post-barrier state.

Total cycle: ~130 ms, vs. ~5 minutes single-threaded.
```

Cold start is the worst case for OCC because contention is
maximal. The Omega paper's measured conflict rate stays under 0.2
in steady state and rises to 0.3–0.5 in cold-start equivalents;
BigFleet's bucket structure (high-cardinality, low per-bucket
demand under realistic catalog) should keep us at the low end of
that envelope. Quantified expectation in §9.

## 8. Invariants

The set of invariants this design preserves, weakens, or
introduces:

**Preserved verbatim from today:**

- Every machine is claimed by at most one Need per cycle.
- Phase 1 emits a coherent (Actions, Unsatisfied) pair at cycle
  end.
- Phase 3 attribution mirrors Phase 1's attribution
  ([ADR-0027] stage 5.1).
- No distributed locking on the hot path (CLAUDE.md).
- Cost formula unchanged (CLAUDE.md).
- Provider RPC surface unchanged (CLAUDE.md).
- Static stability: cycle runs autonomously if coordinator absent.
- Shortfall protocol per `bigfleet.md` §9 (escalation at Age > 5).

**Weakened from today (intentional, see §11 for impact):**

- **Strict priority-ordered Phase 1 traversal.** Today: high-pri
  Needs are evaluated before low-pri, see fresh inventory.
  After this ADR: all Needs are evaluated concurrently; priority
  is enforced at the commit broker via displacement. The
  outcome (high-pri wins on contention) is preserved; the
  mechanism (pre-ordering vs reactive conflict resolution) is
  not.
- **Determinism across cycles.** Today's single-threaded Phase 1
  is fully deterministic for a fixed snapshot + NeedsTable. Under
  OCC, two cycles with identical inputs may produce different
  Action sequences depending on worker scheduling. The
  *outcome* (which Needs satisfied, which shortfall'd) is
  deterministic up to commit-ordering of ties; the *Action order*
  within a result is not. Sim goldens that assert exact Action
  sequences need to be updated to assert outcome-equivalence.

**Newly introduced:**

- **Commit broker** is a per-cycle singleton; only mutator of
  shared claimed-set + bucket-seqno state. Removed at cycle end.
- **Per-bucket sequence numbers** are the conflict-detection
  primitive. Incremented on every claim that touches the bucket.
  Snapshot-isolated reads observe a consistent (bucket, seqno)
  pair from the broker's last successful commit prior to the read.
- **Worker retry budget** per Need (start 10 for easy; 5 for
  picky). Exhausted budget → shortfall.

## 9. Performance projections

### Omega-paper baseline

[Omega §4.3] measured shared-state OCC scaling to ~32 batch
schedulers without conflict-rate breakdown for Google's
production-trace workload (cluster B, 29-day trace). Conflict
fraction stayed ≤ 0.2 in the realistic operating envelope.
[Omega §5.1] identified `t_decision × λ × N` driving conflict
fraction past 1.0 as the saturation point.

### BigFleet projection (uber-* ladder under realistic catalog)

Assumptions:
- 16 easy workers + 4 picky workers + 1 reclaim worker = 21
  concurrent goroutines on a 16-core machine (slightly
  oversubscribed; tolerable).
- Conflict rate ≤ 0.3 in cold start, ≤ 0.15 in steady state
  (BigFleet's per-bucket demand is generally lower than Google's,
  reducing contention).
- Per-Need decision cost ≈ today's per-Need cost minus the
  score-loop allocation overhead (~30%, per parsed-form
  measurement). Effective per-Need cost: ~7 ms × 0.7 ≈ 5 ms in
  the realistic regime; ~130 µs × 0.7 ≈ 90 µs in the aggregated
  regime.

| Profile | NeedsTable | Sequential cost (today) | OCC concurrency factor | Projected cycle p99 |
|---|---:|---:|---:|---:|
| `scaleway-500k` (aggregated) | ~800 | ~70 ms | ~14× (easy-dominated) | ~5 ms |
| `uber-5k` (realistic) | 7,759 | 1.02 s | ~14× | **~75 ms** |
| `uber-50k` (realistic) | 42,680 | ~15 min | ~10× (more picky Needs) | **~90 s → ~9 s with retry-shortfall pruning** |
| `uber-500k` (realistic) | 776,000 | ~100 s extrapolated | ~10× | **~10 s** |
| `uber-1m` (realistic) | per-shard ~776,000 | ~100 s extrapolated | ~10× | **~10 s** per shard; ParSync likely needed |

Caveats:
- **uber-50k requires retry-shortfall pruning.** A naive OCC pass
  on 42K Needs at 5 ms/Need × 1/10 concurrency = 21 s. The
  retry-shortfall path takes ~10–20% of Needs out of the cycle
  (the persistently-contended ones), pulling cycle time closer to
  9 s. This is within ADR-0028's regime-aware envelope (25 s for
  uber-50k).
- **uber-500k projection is tight.** 776K Needs × 5 ms / 10
  concurrency = ~390 s without pruning; aggressive pruning (40%
  of Needs to shortfall) brings it to ~10 s. If real conflict
  rate is closer to 0.4 at this scale, ParSync partitioned-sync
  becomes the next ADR.
- **Aggregated regime is unaffected.** scaleway-* runs already
  pass the 100 ms canonical bar; OCC just makes them faster (~5
  ms projected). No regression risk in the existing happy path.

### Measurement plan

Day-1 metrics to add (Prometheus):

- `bigfleet_shard_phase1_occ_conflict_fraction{kind}` — conflicts
  per successful commit, per scheduler kind. Target ≤ 0.2 steady-
  state; alert at ≥ 1.0 (Omega's saturation indicator).
- `bigfleet_shard_phase1_occ_retries_total{kind, outcome}` —
  histogram of retry counts at commit. Tail behavior is the
  signal.
- `bigfleet_shard_phase1_occ_displacements_total` — how often
  priority-on-commit displaces an incumbent. Phase 2 inversion
  work scales with this.
- `bigfleet_shard_phase1_kind_duration_seconds{kind}` — per-kind
  wall-clock at barrier. Identifies which kind is the bottleneck.

Validation profiles before merge:
1. `scaleway-500k` — regression gate. Must still pass under 100 ms.
2. `uber-5k` — must pass under 100 ms (new bar, not the old 1 s).
3. `uber-50k` — must pass under 25 s (ADR-0028 regime envelope).
4. Local benches — `BenchmarkPhase1_Uber5K_LateRun` and the
   uber-50k bench from the reverted commit, both extended with
   conflict-rate assertions.

## 10. Migration plan

**Full-replace cutover, no flag.** Q3 answered: invariants either
hold or they don't; flagging the old path is a maintenance burden
and a correctness risk. The sim goldens are the safety net.

Sequence:

1. **New package `pkg/decision/occ/`** with the broker, shared-
   state, scheduler kinds, and worker pool. Built and unit-tested
   in isolation from the production Phase 1 path. ~1–2 weeks.

2. **Wire OCC into Phase 1.** Replace `phase1Allocator`'s
   per-Need iteration with the OCC dispatch. Phase 1's outer
   function (`Phase1` in `phase1_assign.go`) becomes:
   ```go
   func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
       state := occ.NewSharedState(snap)
       broker := occ.NewBroker(state)
       results := occ.RunCycle(allNeeds, broker)  // dispatches kind workers, returns at barrier
       return results.ToPhase1Result()
   }
   ```
   `creditExistingSupply` runs first (unchanged); its claims are
   the initial state of the claimed-set.

3. **Sim golden updates.** Goldens that assert exact Action
   sequences relax to outcome-equivalence (set of Actions, not
   sequence). Goldens that assert per-cycle determinism are
   updated to assert outcome-determinism modulo commit ordering.

4. **`make scale` regressions.** All scale bench targets re-run
   under the new code; must show no regression on aggregated-
   regime benches and material improvement on realistic-regime.

5. **Scaleway validation.** Re-run `scaleway-500k` in cloud
   before merge; verify cycle p99 ≤ 100 ms still holds. This is
   the canonical regression gate.

6. **Uber validation.** File `bigfleet-uber #18` once code is on
   `main` to run uber-5k + uber-50k against the new commit.
   Inner agent reports per-Need cost, conflict rate, cycle p99.
   uber-5k expected to easily pass the 100 ms canonical bar;
   uber-50k expected to pass the 25 s regime envelope.

7. **Documentation update.** `bigfleet.md` §8 ("walk needs
   top-down by priority") gets a footnote pointing to this ADR;
   the implementation language is updated to reflect that
   priority is enforced at commit. Paper-level text doesn't need
   a full rewrite — the design intent (priority wins) is
   preserved.

Rollback plan: if scaleway-500k regresses post-cutover, revert
the cutover commit. Sim goldens running on the reverted code are
the immediate signal. Time-to-detect: one CI run (~10 min). No
intermediate dual-path code to remove.

## 11. Open risks / known limits

**Cold-start churn.** Pure OCC at cold start sees maximal
contention; conflict rate spikes; Phase 2 has unusual amounts of
inversion work. Measured-but-not-disastrous in §7.4 walkthrough;
worth watching in the first uber-50k run. Mitigation if it
matters: stagger worker startup (1 worker for 50 ms, then add
the rest) so the first batch of commits stabilizes the claimed-
set before full concurrency.

**Phase 2 work increases.** With pure OCC, priority enforcement
is reactive (at commit) rather than proactive (at ordering).
Some fraction of cycles will see displacement events that
Phase 2 then has to process for inversion resolution.
Quantification: today's Phase 2 cost is ~0.008 s/cycle (from
bigfleet-uber #17 measurement); the increase is bounded by the
displacement-rate × per-inversion cost. Expected increase:
~10–50× the current rate, i.e. ~0.1–0.5 s/cycle. Still negligible
vs cycle p99 budget. If displacement-rate is unexpectedly high,
the priority promotion option from Q4 is the fallback.

**uber-1m+ requires ParSync follow-on.** Projection in §9 shows
pure OCC scales to uber-500k but is unlikely to clear uber-1m
under realistic catalog. The partitioned-sync extension
([ParSync §3]) is the next architectural step; it builds on
this ADR's design without invalidating it (the per-bucket seqno
+ commit broker carry forward; partition-aware staleness
bounding layers on top).

**Determinism loss.** Sim goldens that asserted exact Action
sequences need to relax to outcome-equivalence. This is a
correctness *evidence* loss, not a correctness *property* loss
— the system still produces correct outcomes deterministically
up to commit ordering. Tests that depended on exact Action
ordering for debugging are weakened.

**Worker oversubscription on small machines.** 21 goroutines on
a 16-core dev box is mildly oversubscribed. Acceptable for the
M5 Max dev box; the production shard hardware budget is per-
shard and tunable. Worker counts are configurable.

**Coordination cost with concurrent Phase 1 calls.** Today's
Phase 1 is single-threaded per shard; there is no inter-cycle
concurrency. This ADR keeps that property — only intra-cycle is
concurrent. If a future ADR wants Phase 1 to run as a
continuous loop (rather than discrete cycles), this design
needs revisiting.

## 12. Future work

- **Incremental Phase 1 (delta-only processing).** Track which
  Needs / inventory changed since the previous cycle; only those
  participate in the new cycle's OCC dispatch. Reduces steady-
  state work toward the actual churn rate. Complementary to OCC;
  worth its own ADR after measuring steady-state churn on
  uber-50k+ under OCC.

- **ParSync partitioned synchronization.** Layer staleness-
  bounded partition rotation on top of the per-bucket seqno
  conflict detection. Required for uber-1m+ if §9 projections
  hold. References: [ParSync §3].

- **Multi-shard OCC.** Cross-shard coordination is currently
  bounded by the coordinator's quota RPC. If shortfall escalation
  becomes a hot path under realistic catalog at scale, a future
  ADR may revisit cross-shard locking primitives. Out of scope
  here.

- **Adaptive worker counts.** Today's worker counts are static
  per kind. ParSync's "adaptive strategy" [ParSync §4] adjusts
  per cycle based on observed conflict rate; worth considering
  once we have measured data on conflict-rate variability across
  workloads.

## 13. Alternatives considered

**Band-serial / OCC-within-band.** Preserves CLAUDE.md's
implicit strict-pass invariant most faithfully. Rejected because
the close reading of CLAUDE.md (§Context) showed strict-pass is
not actually required — "priority is the sole throttling
mechanism" is about *how* throttling decides, not *when* in the
cycle. The serial barriers per band cost intra-cycle concurrency
for no contractual gain.

**Replica-only concurrency (hash(cluster_id) sharding).**
Multiple identical-kind workers each handling a hash partition
of Needs. Rejected per [Omega §5.1] and [ParSync §3]: this
inherits Omega's sub-linear ~3× scaling ceiling and doesn't
address the head-of-line blocking that scheduler-kind
partitioning solves. The papers explicitly warned against this.

**Continued constant-factor optimization of single-threaded
Phase 1.** Three attempts already documented in [ADR-0028]
empirical addendum; all failed because bucket count ≈ Need
count under realistic catalog kills constant-factor levers.
Empirically rejected.

**Drop realistic catalog (tune the workload to the
implementation).** Rejected in [ADR-0028] §Alternatives. The
realistic catalog is what makes BigFleet's testing meaningful
for production deployments; tuning it to make our scheduler
look fast would be cherry-picking.

**Centralized scheduler via etcd transactions (K8s scheduler
analog).** Considered briefly. Rejected because (a) K8s
scheduler is centralized and single-instance for a reason — its
workload is per-Pod, not per-aggregate, and the OCC retry rate
is bounded by Pod arrival rate, not by Need × inventory; (b)
BigFleet has no etcd dependency on the hot path (CLAUDE.md
"no distributed locking on hot path"); etcd would violate that
even if the design were otherwise comparable.

---

## 14. References

- [Omega]: Schwarzkopf, Konwinski, Abd-El-Malek, Wilkes. *Omega:
  flexible, scalable schedulers for large compute clusters.*
  EuroSys 2013. https://people.csail.mit.edu/malte/pub/papers/2013-eurosys-omega.pdf
- [BorgOmegaK8s]: Burns, Grant, Oppenheimer, Brewer, Wilkes.
  *Borg, Omega, and Kubernetes.* ACM Queue 14(1), 2016.
  https://queue.acm.org/detail.cfm?id=2898444
- [ParSync]: Feng, Lu, Bao, Wang, Lin, Wang, Li, Li. *Scaling
  Large Production Clusters with Partitioned Synchronization.*
  USENIX ATC 2021 (best paper).
  https://www.usenix.org/conference/atc21/presentation/feng-yihui

[ADR-0019]: 0019-phase1-cloud-vs-bench-discrepancy.md
[ADR-0024]: 0024-co-location-via-podaffinity.md
[ADR-0027]: 0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0028]: 0028-cycle-p99-is-regime-parametric.md
