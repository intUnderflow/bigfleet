# ADR-0029: Phase 1 Omega-style OCC — shared-state, commit-broker priority, dual-mode commits

## Status

Accepted, 2026-05-17.

## Context

[ADR-0028]'s empirical addendum documented three attempts to bring
Phase 1 under control at uber-50k under the realistic catalog
(scale-test bench). Baseline `4ce1e70` measured 9.41 ms/call ×
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

Independent research (see external citations in References) converged on
the same family of fixes — **shared-state optimistic concurrency
control (OCC)** in the style of Google's Omega [1], with the
modern extensions ParSync [3] documented for production at
Alibaba scale (40K decisions/sec, 100K-machine clusters). The Borg
retrospective [2] is somewhat deflationary about
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
sorted single pass, global claimed-set mutated in place.
`bigfleet.md` §16 states "priority is the sole throttling
mechanism." A close reading
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

## Goals

1. Cycle p99 ≤ 100 ms at **uber-50k** under realistic catalog
   (currently ~15 minutes worst-case).
2. Cycle p99 ≤ 1 s at **uber-500k** under realistic catalog
   (currently extrapolates to ~100 s).
3. **No regression** in the aggregated regime (scaleway-* runs
   currently passing under the 100 ms canonical bar must still
   pass after cutover).
4. **Preserve every hard rule.** In particular: no
   distributed locking on the hot path (this ADR is intra-shard
   concurrency only); cost formula unchanged; provider RPC surface
   unchanged; static stability holds (shards run autonomously
   during coordinator failover).
5. **Preserve the [ADR-0027] stage 5.1 invariant.** Phase 1 and
   Phase 3 must attribute supply identically. The commit barrier
   produces a coherent claimed-set Phase 2/3 can read with the
   same semantics they read today.

## Non-goals

- **Multi-shard OCC.** Cross-shard topology stays bounded per
  `bigfleet.md` §16 ("topology constraints do not cross shard
  boundaries").
  Each shard's Phase 1 is concurrent internally; shards remain
  independent.
- **Incremental Phase 1** (delta-only processing). Complementary
  lever; deferred to a follow-on ADR. The OCC design here leaves a
  clean place to layer incremental in as a future optimization.
- **ParSync-style partitioned synchronization.** Not required for
  any rung of the uber-* ladder. Beyond uber-500k the paper caps a
  shard's machine count at the 500K ceiling and BigFleet scales
  horizontally via sharding (uber-1m = 2 shards × 500K, uber-5m =
  10 shards × 500K). Each individual shard sees the same per-shard
  workload at every multi-shard rung, so OCC's per-shard envelope
  carries through unchanged. ParSync becomes interesting only if a
  future ADR raises the per-shard ceiling itself; deferred to a
  follow-on ADR conditional on that change.
- **Changes to Phase 2's victim scoring or Phase 3's reclaim
  ordering.** Only the commit barrier interaction changes;
  algorithmic content is unchanged.
- **Changes to the `Action` shape, the provider RPC surface, the
  capacity contract proto, or the operator's roll-up format.**
  This is a `pkg/decision/` rewrite, not a wire-format change.

## Design overview

```
┌─────────────────────────────────────────────────────────────────┐
│                       Shard Cycle (1 Hz)                        │
│                                                                 │
│   ┌─────────────┐        ┌────────────────────────────────┐     │
│   │  Snapshot   │        │ Need queue                     │     │
│   │ (immutable, │───────▶│ (all Needs, one shared queue)  │     │
│   │  per cycle) │        │                                │     │
│   └─────────────┘        └──────────────┬─────────────────┘     │
│         │                               │                       │
│         │   ┌───────────────────────────┴────────────┐          │
│         │   │                                        │          │
│         ▼   ▼                                        ▼          │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│   │ Worker 1 │  │ Worker 2 │  │   ...    │  │ Worker N │        │
│   └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│        │             │             │              │              │
│        └─────────────┴─────┬───────┴──────────────┘              │
│                            │                                    │
│                proposals   │   (each proposal carries a mode:   │
│                            ▼    incremental | all-or-nothing)   │
│              ┌─────────────────────────────┐                    │
│              │     Commit Broker           │                    │
│              │  per-bucket sequence CAS    │                    │
│              │  priority-on-conflict       │                    │
│              │  proposal modes:            │                    │
│              │    - incremental (default)  │                    │
│              │    - all-or-nothing (gangs) │                    │
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

The shape is Omega's [1, §3.4]: shared cell state (BigFleet's
inventory snapshot), private read views per worker, atomic commits
through a single broker that detects conflicts at machine
granularity. Where this ADR specializes for BigFleet:

- **One commit barrier per cycle**, not per-Need. Phase 2/3 read
  the claimed-set after the barrier — same input shape as today.
- **Two commit modes at the broker** — incremental (default;
  per-Omega [1, §3.4] the cheaper path) and all-or-nothing (for
  gang-scheduled Needs whose partial fill is unacceptable).
  Workers classify each Need at pickup and set the mode flag on
  their proposal; one shared worker pool serves both modes.
- **Commit broker enforces priority on conflict** — high-precedence
  proposals win against in-flight lower-precedence claims. This is
  how `bigfleet.md` §16's "priority is the sole throttling
  mechanism" is preserved without strict-pass ordering.

Notably absent: no scheduler-kind partition (Omega's batch/service
split). BigFleet's measured workload (scale-test bench) shows
uniform per-Need cost, not bimodal — the queue-isolation that
motivated Omega's split doesn't pay off here. The Omega-faithful
transaction-semantics distinction (incremental vs all-or-nothing)
lives at the broker as a per-proposal mode, not as a worker-kind
split. See Alternatives considered for the full rationale.

## Detailed design

### Shared state

Two shared structures per cycle:

1. **Inventory snapshot** — immutable for the cycle's duration.
   The existing `inventory.Snapshot` already has this property;
   readers grab a reference at cycle start and never mutate it.
   This is the equivalent of Omega's "private, local, frequently-
   updated copy" [1, §3.4] — each scheduler reads the same
   snapshot, no locks.

2. **Claimed-set** — mutable, written only via the commit broker.
   Today this is `phase1Allocator.claimed map[machine.ID]struct{}`.
   The redesign replaces the bare map with a `claimedSet`
   structure that the broker mutates under a single mutex; each
   bucket also carries a sequence number that increments on every
   claim. Schedulers read the claimed-set lock-free (sync.Map or
   atomic.Pointer to an immutable snapshot, depending on
   implementation cost — see Migration plan).

Per-bucket sequence numbers are the **fine-grained conflict
detection** primitive — Omega measured 2–3× lower spurious-conflict
cost from fine-grained vs coarse-grained [1, §5.2, Fig. 14]. The
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

### Worker pool

One shared worker pool of N goroutines, all pulling from one
shared Need queue. No worker specialisation: every worker can
process any Need.

The Need's commit semantics (incremental vs all-or-nothing) are
decided per-Need at pickup based on Profile shape (see Commit
broker below). The pool itself is uniform.

Default pool size: `runtime.GOMAXPROCS()`. On the M5 Max dev box
(GOMAXPROCS=16) that's 16 workers. The choice trades CPU
saturation against conflict rate — Omega's measurement showed up
to ~32 concurrent schedulers without breakdown for Google's
workload [1, §4.3, §5.1]; BigFleet's per-bucket demand under the
realistic catalog is generally lower, so pressure on the broker is
expected to be lighter than Omega's measurements.

Worker count is tunable from a flag (`--phase1-workers`); the
measurement plan in Performance projections includes verifying the
default is appropriate for production shard hardware. ParSync's
adaptive-strategy guidance [3, §4] suggests dynamic worker counts
keyed to observed conflict rate; deferred to a follow-on ADR if
the static default proves wrong.

Phase 3 reclaim runs in its own goroutine after the cycle barrier
(see Phase 2 / Phase 3 interaction). It is not part of the Phase 1
worker pool; OCC doesn't buy obvious throughput for it and its
ordering invariants ([ADR-0027] stage 5.1) require the
post-barrier coherent claimed-set as input.

### Single-band concurrent scheduling

`bigfleet.md` §16's "priority is the sole throttling mechanism"
is preserved via the commit broker, not via iteration order. Within a cycle,
all workers race concurrently; there is **no priority-sorted
outer loop**. Each worker pulls Needs from the shared queue (which
is itself unordered — workers race for the next Need as available) and proposes
claims to the broker.

Priority enters at three points:
1. **Commit-conflict resolution** (Commit broker). When two workers race for
   the same machine, the broker awards it to the higher-precedence
   claim. Precedence = (priority, interruption_penalty_bucket,
   reclamation_penalty_bucket) lexicographic.
2. **Phase 2 inversions**. Same as today: post-commit, Phase 2
   scans for inversions (higher-priority Need unsatisfied vs lower-
   priority Need satisfied) and preempts. With pure OCC inversions
   happen more often than under strict-pass, but Phase 2 already
   handles them; this is an increase in Phase 2 work, quantified
   in Open risks.
3. **Shortfall escalation**. A Need that loses its retry budget
   (Bounded retries → shortfall) goes to the shortfall buffer with its priority preserved;
   coordinator escalation at Age >5 cycles is unchanged from today
   (`bigfleet.md` §9).

The cycle barrier holds all workers; when the shared queue drains
and there are no in-flight proposals at the broker, the barrier
releases. Phase 2/3 then run on the coherent post-barrier
claimed-set. This barrier is the single synchronization point per
cycle.

### Commit broker

The broker is a single mutex-guarded data structure. Workers
construct proposals that carry a **mode** field selecting the
transaction semantics per [1, §3.4]:

```go
type ProposalMode int

const (
    // ModeIncremental: best-effort partial commit. Machines that
    // are still unclaimed at commit time are claimed; conflicted
    // machines are dropped from the proposal but the rest commit.
    // This is Omega's default and measured cheaper [1, §5.2].
    ModeIncremental ProposalMode = iota

    // ModeAllOrNothing: atomic commit. Either every proposed
    // machine commits or none do. If any one is conflicted, the
    // entire proposal fails and the Need retries from scratch.
    // Reserved for gang-scheduled Needs whose partial fill is
    // unacceptable. Measured ~2× more conflict-prone than
    // ModeIncremental [1, §5.2, Fig. 14a]; use sparingly.
    ModeAllOrNothing
)

type Proposal struct {
    Need        *needs.Need
    Machines    []machine.ID
    Bucket      bucketKey
    ObservedSeq uint64
    Precedence  precedence
    Mode        ProposalMode
}
```

The broker's main method follows a **plan-then-commit** discipline:
the entire proposal is classified against current state first
(without mutating anything), then mode-specific logic decides
commit-or-abort. This eliminates the need for rollback on
`ModeAllOrNothing` abort because no mutations happen until the
commit is known to succeed.

```go
func (b *broker) Propose(p Proposal) Result {
    b.mu.Lock()
    defer b.mu.Unlock()

    // 1. Fine-grained conflict check: has the bucket's seqno moved
    //    since the proposer's read?
    if b.state.bucketSeq[p.Bucket] != p.ObservedSeq {
        return Result{Status: Conflict, ReObserve: b.snapshotBucket(p.Bucket)}
    }

    // 2. Plan: classify each proposed machine without mutating state.
    //
    //      newClaim   — machine is currently unclaimed
    //      displace   — machine is claimed by a lower-precedence
    //                   incumbent we can displace
    //      conflicted — machine is claimed by an incumbent we
    //                   cannot displace
    type displacement struct {
        mid machine.ID
        old claim
    }
    var newClaim []machine.ID
    var displace []displacement
    var conflicted []machine.ID
    for _, mid := range p.Machines {
        if _, claimed := b.state.claimed[mid]; claimed {
            inc := b.state.claimedBy[mid]
            if precedenceLess(inc.precedence, p.Precedence) {
                displace = append(displace, displacement{mid: mid, old: inc})
            } else {
                conflicted = append(conflicted, mid)
            }
        } else {
            newClaim = append(newClaim, mid)
        }
    }

    // 3. Mode-specific commit decision (still no mutations yet).
    if p.Mode == ModeAllOrNothing && len(conflicted) > 0 {
        // Atomic: any conflict aborts. Since planning didn't mutate
        // state, nothing to roll back.
        return Result{Status: Conflict, ReObserve: b.snapshotBucket(p.Bucket)}
    }

    // 4. Atomically apply the commit. From here, state is mutated.
    //    Displaced incumbents release their claim and re-queue with
    //    their own retry budget decremented.
    for _, d := range displace {
        delete(b.state.claimed, d.mid)
        delete(b.state.claimedBy, d.mid)
        b.requeue <- d.old.need
    }
    //    Newly-claimed (previously unclaimed) and displaced-and-
    //    reclaimed machines now belong to this proposal.
    committed := append(append([]machine.ID(nil), newClaim...), midsOf(displace)...)
    for _, mid := range committed {
        b.state.claimed[mid] = true
        b.state.claimedBy[mid] = claim{need: p.Need, precedence: p.Precedence}
    }
    b.state.bucketSeq[p.Bucket]++

    return Result{
        Status:     Committed,
        Committed:  committed,
        Conflicted: conflicted, // empty for atomic commits; may be non-empty for incremental
    }
}
```

For `ModeIncremental`, `conflicted` may be non-empty even after a
successful commit — the worker receives the partial-commit set and
emits a shortfall for the residual deficit. For `ModeAllOrNothing`,
a successful commit always has `conflicted` empty (atomicity).

**Per-Need mode selection.** Workers set the mode at proposal
construction based on Profile shape:

- Default: `ModeIncremental`. Covers the vast majority of
  realistic-catalog Needs, where partial satisfaction is fine and
  any shortfall flows through the standard buffer.
- `ModeAllOrNothing` if the Need has a `min_unit` whose
  satisfaction requires multiple machines AND the Need carries a
  `Same` requirement (i.e., it's gang-scheduled with co-location).
  This is the BigFleet equivalent of MPI-style gang scheduling
  [1, §3.4]. The classification is pure (function of Profile +
  `min_unit`); no operator-side hint needed.

Key properties:

- **Incremental is the default; all-or-nothing is the exception.**
  Per Omega's measurement [1, §5.2, Fig. 14a], all-or-nothing
  roughly 2× the conflict fraction. Reserving it for genuine
  gangs (where partial fill is semantically wrong, not just
  suboptimal) is the right cost/benefit trade.
- **Priority on conflict** preserves the throttling property
  required by `bigfleet.md` §16. Higher-precedence proposals
  displace lower-precedence incumbents; displaced incumbents
  re-queue with their retry budget decremented. Displacement
  applies in both modes.
- **Per-bucket seqno + per-machine claim check** is double-
  validated — the seqno catches "bucket state changed under me",
  the per-machine check catches the actual claim race. Same
  mechanism as Omega's broker [1, §5.2].
- **Single mutex** for the broker is intentional. The broker is
  not the bottleneck — per-proposal work is O(machines-in-
  proposal) = small constant (a single Need typically claims 1–10
  machines).
  Workers spend most of their time computing proposals, not at the
  broker.

### Bounded retries → shortfall

Each Need carries a retry budget (starts at 10; tunable via
`--phase1-retries`). On `Conflict` from the broker, the worker
re-reads the bucket's state, recomputes the proposal, and
re-tries. On retry budget exhaustion, the Need is emitted to the
shortfall buffer with its full deficit vector.

This matches Omega's "abandoned at 1,000 retries" behavior
[1, §4] but with a much tighter cap because BigFleet's
shortfall protocol is a first-class concept (`bigfleet.md` §9 +
[ADR-0027]) — persistent contention is meant to be escalated to
the coordinator for cross-shard rebalancing, not absorbed by
spinning. Coordinator escalation kicks in at Age >5 cycles per
the existing protocol.

Retry budget:
- **All Needs**: 10 retries default. Most cycles see conflict
  rate ≤ 0.2; 10 is enough headroom for the long-tail. Tunable
  per shard via flag.
- **Gang Needs (`ModeAllOrNothing`)** decrement the retry budget
  on EVERY failed atomic commit, not per-machine. A gang that hits
  the cap goes to shortfall as a whole; partial-credit doesn't
  apply for them by definition. Coordinator escalation per the
  existing protocol handles cross-shard rebalancing.

Forward-progress guarantee: priority promotion on retry would
*also* work (Q4 alternative) but adds per-Need bookkeeping and
risks demoting newly-arrived high-pri work. Bounded-retries-to-
shortfall is the simpler invariant and matches the existing
architecture.

### Phase 2 / Phase 3 interaction

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
  See Open risks.
- **Phase 3** reads the claimed-set, computes reclaim per
  [ADR-0027] stage 5.1 attribution rules. The attribution invariant
  ("Phase 1 and Phase 3 must attribute supply identically") holds
  because both read the same post-barrier claimed-set.

The shortfall buffer is mutated only by the cycle's Phase 1 (via
broker re-queue) and read by the coordinator's quota RPC
asynchronously. No concurrent mutation outside the cycle.

## Flow examples

### Happy path — no conflict

```
Cycle start: snapshot = S; claimed = ∅; bucketSeq = all 0.

Worker W1 picks Need n1 (priority 1000, single-rack):
  - Reads bucket B1 from S; bucketSeq[B1] = 0 observed.
  - Computes proposal P1 = {need: n1, bucket: B1, machines: [m5], observedSeq: 0}.
  - Sends to broker.

Broker:
  - bucketSeq[B1] = 0 matches observedSeq = 0. ✓
  - m5 not in claimed. ✓
  - Commits: claimed[m5] = n1; bucketSeq[B1] = 1.
  - Returns Committed.

W1 emits Action{Bootstrap, m5, n1.cluster}.
```

No retry; no conflict; one round-trip. This is the common case
for steady-state cycles where most Needs find ample inventory.

### Conflict path — two workers race, priority wins

```
Cycle start: snapshot = S; claimed = ∅; bucketSeq[B1] = 0.

W1 picks Need n1 (priority 100, single-rack on B1).
W2 picks Need n2 (priority 10000, single-rack on B1).
Both read B1 simultaneously; both observe bucketSeq[B1] = 0.

W1 computes P1 = {n1, B1, [m5], seq:0}; sends to broker.
W2 computes P2 = {n2, B1, [m5], seq:0}; sends to broker (microseconds later).

Broker receives P1 first:
  - seq matches; m5 unclaimed. Commits. claimed[m5] = n1; seq[B1] = 1.
  - Returns Committed to W1.

Broker receives P2:
  - bucketSeq[B1] = 1, but P2.observedSeq = 0. Seqno mismatch.
  - Returns Conflict to W2 with current bucket snapshot.

W2 re-observes B1, sees m5 claimed but m7 unclaimed.
W2 retries: P2' = {n2, B1, [m7], seq:1}. Sends to broker.

Broker receives P2':
  - seq matches; m7 unclaimed. Commits.

ALTERNATIVELY, if B1 had only one unclaimed machine and W2's
priority (10000) is strictly higher than W1's (100):
  - W2 retries with P2'' = {n2, B1, [m5], seq:1, displaceOK: true}.
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

### Retry exhaustion → shortfall

```
Cycle start: B7 has 10 machines, all claimed by an earlier-arrived
batch of priority-1000 Needs.

W1 picks Need n8 (priority 100, must use B7 due to single-rack
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

### Cold start with mixed-priority demand

The pure-OCC worry. Cycle 1 of a freshly-booted shard (or the
arrival of a large deploy-burst on an established shard — see
Open risks) sees a wave of Needs hit an empty or near-empty
claimed-set.

```
Cycle start: 1000 Needs across priorities {100, 1000, 10000};
             5000 Idle machines across many buckets.

t=0:    Workers (W1..W16) start pulling Needs from the shared
        queue. No priority ordering. Each Need's per-call cost
        averages ~5ms (per-Need decision work; see Performance
        projections).

t=0–200ms: Many workers concurrently scan/propose against
        overlapping buckets. Initial conflict rate is elevated
        (~0.3) because the claimed-set is empty and many proposals
        land on the same hot buckets. Per Omega [1, §5.1], cold-
        start conflict spikes are typical; conflict rate decays
        toward steady-state (~0.15) as the claimed-set fills out.

t=200–500ms: Workers grind through the queue. Each round-trip
        includes propose → conflict-or-commit → (on conflict)
        re-observe → re-propose. With conflict rate 0.3, average
        proposals per Need = 1.43; with 16-way concurrency and
        5ms/proposal, 1000 Needs × 1.43 × 5ms / 16 ≈ 450ms.

t=500–800ms: Tail Needs: those that lost retry budget go to
        shortfall; rest commit. Total throughput dominated by the
        slowest worker's queue draining.

t=~800ms-1s: Barrier releases when queue is empty and no
        in-flight proposals remain at the broker. Phase 2 scans
        for inversions (most high-pri Needs already won via
        commit-broker displacement, so Phase 2's inversion list is
        small — but it does have work cleaning up Needs that were
        displaced and shortfall'd).

t=~1s:  Phase 3 runs. Reclaim attribution mirrors Phase 1's
        post-barrier state.

Total cold-start cycle: ~500ms–1s, vs ~5 minutes single-threaded.
```

Earlier drafts of this section quoted ~120ms for this scenario;
that number undercounted both conflict-retry overhead and the
broker mutex contention under maximal cold-start concurrency.
The revised projection is ~500ms–1s, derived from 1000 × 1.43 ×
5ms / 16 + barrier overhead. Steady-state cycles after cold-start
are much faster (~100ms range, see Performance projections).

Cold start is the worst case for OCC because contention is
maximal. The Omega paper's measured conflict rate stays under 0.2
in steady state and rises to 0.3–0.5 in cold-start equivalents [1,
§5.1]; BigFleet's bucket structure (high-cardinality, low
per-bucket demand under the realistic catalog) should keep us
toward the low end of that envelope. Quantified expectation in
Performance projections.

## Invariants

The set of invariants this design preserves, weakens, or
introduces:

**Preserved verbatim from today:**

- Every machine is claimed by at most one Need per cycle.
- Phase 1 emits a coherent (Actions, Unsatisfied) pair at cycle
  end.
- Phase 3 attribution mirrors Phase 1's attribution
  ([ADR-0027] stage 5.1).
- No distributed locking on the hot path (`bigfleet.md` §16).
- Cost formula unchanged (`bigfleet.md` §4).
- Provider RPC surface unchanged (`bigfleet.md` §7).
- Static stability: cycle runs autonomously if coordinator absent.
- Shortfall protocol per `bigfleet.md` §9 (escalation at Age > 5).

**Weakened from today (intentional, see Open risks for impact):**

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
- **Worker retry budget** per Need (start 10, tunable).
  Exhausted budget → shortfall.

## Performance projections

### Omega-paper baseline

[1, §4.3] measured shared-state OCC scaling to ~32 batch
schedulers without conflict-rate breakdown for Google's
production-trace workload (cluster B, 29-day trace). Conflict
fraction stayed ≤ 0.2 in the realistic operating envelope.
[1, §5.1] identified `t_decision × λ × N` driving conflict
fraction past 1.0 as the saturation point.

### BigFleet projection (uber-* ladder under realistic catalog)

**Note on catalog calibration**: the numbers in the table below
are projected against the prior (uncalibrated) `realistic.yaml`
catalog whose NeedsTable shape was measured at the scale-test bench baseline
(~388 Needs/cluster, dominated by sameRack groups misclassified
as gangs). The scale-test review recalibrated the catalog against
industry production patterns (~70% tiny-stateless long tail, true gang
fraction ~3%, ~42% topology-spread carrying). Re-baselining
against the corrected catalog is the immediate post-merge work
(see Migration plan). The projections here are conservative —
the corrected catalog has fewer gang Needs and more independent
small Needs, both of which reduce per-Need cost and conflict rate.

Assumptions:
- 16 workers in one shared pool on a 16-core machine for Phase 1
  (`runtime.GOMAXPROCS()` default). Phase 3 runs in a single
  goroutine after the cycle barrier — not part of Phase 1's
  worker pool.
- Conflict rate ≤ 0.3 in cold start, ≤ 0.15 in steady state.
  BigFleet's per-bucket demand under the realistic catalog is
  lower than Google's batch-vs-service split, reducing contention.
  Production NUMA hardware may push these numbers up at uber-500k+
  (see Open risks).
- Per-Need decision cost ≈ today's per-Need cost minus the
  score-loop allocation overhead (~30%, per parsed-form
  measurement). Effective per-Need cost: ~7 ms × 0.7 ≈ 5 ms in
  the realistic regime; ~130 µs × 0.7 ≈ 90 µs in the aggregated
  regime.
- Per-Need cost is **tri-modal** in practice (fast-absorb /
  fast-shortfall / slow-score), not unimodal. The slow-score
  path dominates wall-clock and is what the projections target;
  fast-absorb and fast-shortfall paths complete in microseconds
  regardless of OCC concurrency.

| Profile | NeedsTable | Sequential cost (today) | OCC concurrency factor | Projected cycle p99 |
|---|---:|---:|---:|---:|
| `scaleway-500k` (aggregated) | ~800 | ~70 ms | ~14× | ~5 ms |
| `uber-5k` (realistic) | 7,759 | 1.02 s | ~14× | **~75 ms** |
| `uber-50k` (realistic) | 42,680 | ~15 min | ~10× | **~90 s → ~9 s with retry-shortfall pruning** |
| `uber-500k` (realistic) | 776,000 | ~100 s extrapolated | ~10× | **~10 s** |
| `uber-1m` (realistic, 2 shards) | per-shard ~776,000 | ~100 s extrapolated | ~10× | **~10 s** per shard (same per-shard cost as uber-500k) |
| `uber-5m` (realistic, 10 shards) | per-shard ~776,000 | ~100 s extrapolated | ~10× | **~10 s** per shard (same per-shard cost as uber-500k) |

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
  rate is closer to 0.4 at this scale, an additional structural
  intervention may be needed — but ParSync is the wrong tool
  here (it scales a single scheduler beyond the per-shard
  ceiling, which BigFleet caps at 500K machines anyway).
  uber-1m / uber-5m run additional shards rather than larger
  shards.
- **Aggregated regime is unaffected.** scaleway-* runs already
  pass the 100 ms canonical bar; OCC just makes them faster (~5
  ms projected). No regression risk in the existing happy path.

### Measurement plan

Day-1 metrics to add (Prometheus):

- `bigfleet_shard_phase1_occ_conflict_fraction{mode}` — conflicts
  per successful commit, per proposal mode (incremental |
  all-or-nothing). Target ≤ 0.2 steady-state; alert at ≥ 1.0
  (Omega's saturation indicator).
- `bigfleet_shard_phase1_occ_retries_total{mode, outcome}` —
  histogram of retry counts at commit, per proposal mode. Tail
  behavior is the signal.
- `bigfleet_shard_phase1_occ_displacements_total` — how often
  priority-on-commit displaces an incumbent. Phase 2 inversion
  work scales with this.
- `bigfleet_shard_phase1_proposal_duration_seconds{mode}` —
  per-proposal wall-clock at the broker. Identifies whether
  all-or-nothing commits dominate worker time.

Validation profiles before merge:
1. `scaleway-500k` — regression gate. Must still pass under 100 ms.
2. `uber-5k` — must pass under 100 ms (new bar, not the old 1 s).
3. `uber-50k` — must pass under 25 s (ADR-0028 regime envelope).
4. Local benches — `BenchmarkPhase1_Uber5K_LateRun` and the
   uber-50k bench from the reverted commit, both extended with
   conflict-rate assertions.

## Migration plan

**Full-replace cutover, no flag.** Q3 answered: invariants either
hold or they don't; flagging the old path is a maintenance burden
and a correctness risk. The sim goldens are the safety net.

Sequence:

1. **New package `pkg/decision/occ/`** with the broker, shared-
   state, and worker pool. Built and unit-tested
   in isolation from the production Phase 1 path. ~1–2 weeks.

2. **Wire OCC into Phase 1.** Replace `phase1Allocator`'s
   per-Need iteration with the OCC dispatch. Phase 1's outer
   function (`Phase1` in `phase1_assign.go`) becomes:
   ```go
   func Phase1(snap *inventory.Snapshot, allNeeds []needs.Need) Phase1Result {
       state := occ.NewSharedState(snap)
       broker := occ.NewBroker(state)
       results := occ.RunCycle(allNeeds, broker)  // dispatches workers, returns at barrier
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

6. **Scale-test validation.** File a scale-test follow-up once
   code is on `main` to run uber-5k + uber-50k against the new
   commit. Bench runners report per-Need cost, conflict rate, cycle p99.
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

## Open risks / known limits

**Cold-start churn.** Pure OCC at cold start sees maximal
contention; conflict rate spikes; Phase 2 has unusual amounts of
inversion work. Measured-but-not-disastrous in the cold-start flow example;
worth watching in the first uber-50k run. Mitigation if it
matters: stagger worker startup (1 worker for 50 ms, then add
the rest) so the first batch of commits stabilizes the claimed-
set before full concurrency.

**Phase 2 work increases.** With pure OCC, priority enforcement
is reactive (at commit) rather than proactive (at ordering).
Some fraction of cycles will see displacement events that
Phase 2 then has to process for inversion resolution.
Quantification: today's Phase 2 cost is ~0.008 s/cycle (from
scale-test bench measurement); the increase is bounded by the
displacement-rate × per-inversion cost. Expected increase:
~10–50× the current rate, i.e. ~0.1–0.5 s/cycle. Still negligible
vs cycle p99 budget. If displacement-rate is unexpectedly high,
the priority promotion option from Q4 is the fallback.

**Multi-shard rungs inherit the per-shard envelope, not a larger
one.** uber-1m and uber-5m run additional shards (2 and 10
respectively) rather than larger shards — `bigfleet.md` §5 / §11
caps a shard's machine count at the 500K ceiling. Per-shard
NeedsTable at every multi-shard rung stays at uber-500k's ~776K.
The OCC envelope projected for uber-500k therefore carries through
unchanged across uber-1m and uber-5m. The risks that DO appear at
multi-shard scale are coordinator-side (cross-shard quota
arbitration, shortfall escalation rate) and out of scope for this
ADR.

**Determinism loss.** Sim goldens that asserted exact Action
sequences need to relax to outcome-equivalence. This is a
correctness *evidence* loss, not a correctness *property* loss
— the system still produces correct outcomes deterministically
up to commit ordering. Tests that depended on exact Action
ordering for debugging are weakened.

**Worker oversubscription on small machines.** 16 Phase 1 workers
plus 1 Phase 3 worker = 17 goroutines on a 16-core dev box is
mildly oversubscribed. Acceptable for the M5 Max dev box; the
production shard hardware budget is per-shard and tunable. Worker
counts are configurable via the `--phase1-workers` flag.

**PodDisruptionBudget (PDB) gap.** Production K8s workloads carry
PDBs (`minAvailable: 2`, `maxUnavailable: 25%`) that bound
concurrent disruption. Today's BigFleet preemption model is
"priority wins, full stop" — it does not consult PDBs anywhere
in the Phase 2 victim-scoring path or the coordinator's cross-
shard preemption. This ADR's commit-broker displacement adds
*more* displacement opportunities (every cycle's broker can
displace, not just Phase 2) and therefore exacerbates the
existing gap. Not introduced by this ADR but worsened by it.
Surfaced during scale-test review. Fix is a separate follow-on ADR
that designs PDB-respecting preemption uniformly across Phase 1
commit-broker, Phase 2 victim scoring, and coordinator cross-
shard preemption.

**Preemption cascades.** When a high-priority Need displaces a
lower-priority incumbent, the displaced incumbent re-queues and
may itself displace another lower-priority incumbent. Under
bursty high-priority arrivals, this creates chains of re-queues.
The Phase 2 work-increase estimate above (~10–50× current rate)
may undercount cascades; a single displacement can trigger N
re-queues whose costs compound. Mitigation if observed:
displacement-rate metric (already proposed in Measurement plan)
flags it; the per-Need retry budget bounds the cascade depth
(each re-queue decrements; eventually shortfalls).

**Deploy-burst is the recurring cold-start event.** The ADR's
cold-start analysis treated cold-start as a once-per-boot event.
In production (per scale-test review) deploy-bursts — service
rollouts creating 50–500 Pods over 30 seconds, batched by canary
% — happen many times per day per cluster and produce the same
contention shape as a fresh boot (many Needs arriving at the
same buckets simultaneously). The cold-start projection
(~500ms–1s) should be read as "every-deploy-burst worst case,"
not "once at startup." The `churnPerMinute` field in profiles
(currently 0.02) understates this; modeling deploy-bursts
explicitly is realistic-catalog follow-on work.

**NUMA broker mutex contention at uber-500k+.** Single mutex on
the commit broker is fine at uber-50k (~2,600 proposals/sec) but
on multi-socket production shard hardware the cross-NUMA cache-
coherence overhead (~100–500 ns per lock acquisition) becomes
non-trivial at uber-500k+ proposal rates. Monitor via the
`bigfleet_shard_phase1_proposal_duration_seconds` histogram;
the fallback is per-bucket-shard broker partitioning (rather
than a single broker), which is a localized change.

**Realistic-catalog calibration.** The Performance projections
table was built against the pre-calibration catalog (388
Needs/cluster, sameRack-misclassified-as-gang dominated).
The scale-test review recalibrated the catalog to industry production
patterns (70% tiny long-tail, ~3% true gangs, ~42% spread-carrying); the
re-baseline run is the immediate post-merge work. Expected: the
corrected catalog reduces both per-Need cost (more independent
small Needs) and conflict rate (fewer gang Needs hitting hot
buckets simultaneously). Numbers will tighten favorably, not
adversely.

**Per-Need cost is tri-modal, not unimodal.** Production data
suggests three regimes:
- *fast-absorb*: Need matches existing Configured supply →
  `creditExistingSupply` covers it without Phase 1 work
- *fast-shortfall*: bucket fully claimed → take returns nil
  immediately
- *slow-score*: real work — score loop walks unclaimed machines
  in the chosen bucket

Performance projections target the slow-score path, which
dominates wall-clock under realistic workloads. The other two
paths complete in microseconds regardless of OCC concurrency.
Worth noting because cycle-time histograms will show the
trimodality and shouldn't be misinterpreted as bimodal cost.

**Coordination cost with concurrent Phase 1 calls.** Today's
Phase 1 is single-threaded per shard; there is no inter-cycle
concurrency. This ADR keeps that property — only intra-cycle is
concurrent. If a future ADR wants Phase 1 to run as a
continuous loop (rather than discrete cycles), this design
needs revisiting.

## Future work

- **Incremental Phase 1 (delta-only processing).** Track which
  Needs / inventory changed since the previous cycle; only those
  participate in the new cycle's OCC dispatch. Reduces steady-
  state work toward the actual churn rate. Complementary to OCC;
  worth its own ADR after measuring steady-state churn on
  uber-50k+ under OCC.

- **ParSync partitioned synchronization.** Layer staleness-
  bounded partition rotation on top of the per-bucket seqno
  conflict detection. Only becomes interesting if a future ADR
  raises the per-shard machine ceiling above 500K — at today's
  cap, BigFleet scales horizontally via sharding instead.
  References: [3, §3].

- **Multi-shard OCC.** Cross-shard coordination is currently
  bounded by the coordinator's quota RPC. If shortfall escalation
  becomes a hot path under realistic catalog at scale, a future
  ADR may revisit cross-shard locking primitives. Out of scope
  here.

- **Adaptive worker counts.** Today's worker count is static.
  ParSync's "adaptive strategy" [3, §4] adjusts per cycle based
  on observed conflict rate; worth considering once we have
  measured data on conflict-rate variability across workloads.

- **Scheduler-kind partition** (Omega-style easy/picky split).
  Discussed and rejected in Alternatives considered on uniform-
  per-Need-cost grounds. If production telemetry surfaces a
  bimodal cost distribution (e.g., gang Needs become significantly
  more common and significantly more expensive than the median),
  splitting into per-kind worker pools — with mode-specific retry
  budgets and conflict-rate metrics — is the next architectural
  step. The current single-pool design is forward-compatible:
  workers already carry a per-Need mode flag, so adding a queue
  per mode is a localized change.

## Alternatives considered

**Band-serial / OCC-within-band.** Preserves the existing
implementation's implicit strict-pass invariant most faithfully.
Rejected because the close reading in Context showed strict-pass
is not actually required by `bigfleet.md` — "priority is the sole
throttling mechanism" (§16) is about *how* throttling decides, not
*when* in the cycle. The serial barriers per band cost intra-cycle concurrency
for no contractual gain.

**Replica-only concurrency (hash(cluster_id) sharding).**
Multiple identical workers each handling a hash partition of
Needs. Rejected per [1, §5.1] and [3, §3]: this inherits Omega's
sub-linear ~3× scaling ceiling and doesn't address head-of-line
blocking when one partition's work runs long. The papers
explicitly warned against this.

**Omega-style scheduler-kind partition (easy / picky).**
Separate worker pools per Need shape, modelled on Omega's
batch/service split. Rejected on workload grounds: the scale-test
bench measured uniform per-Need cost across the realistic catalog,
not the bimodal distribution that motivates Omega's split. Queue
isolation between kinds doesn't reduce conflict rate (kinds share
the same inventory) — it only protects against head-of-line
blocking that we haven't measured. The Omega-faithful transaction-
semantics distinction (incremental vs all-or-nothing) is preserved
as a per-proposal mode at the broker, which captures the
asymmetric cost without splitting goroutine pools. Add scheduler
kinds back as a follow-on ADR if a bimodal cost distribution
emerges in production telemetry.

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
BigFleet has no etcd dependency on the hot path
(`bigfleet.md` §16: "no distributed locking on hot path"); etcd
would violate that
even if the design were otherwise comparable.

---

## References

1. Schwarzkopf, Konwinski, Abd-El-Malek, Wilkes.
   *Omega: flexible, scalable schedulers for large compute clusters.*
   EuroSys 2013.
   https://people.csail.mit.edu/malte/pub/papers/2013-eurosys-omega.pdf
2. Burns, Grant, Oppenheimer, Brewer, Wilkes.
   *Borg, Omega, and Kubernetes.* ACM Queue 14(1), 2016.
   https://queue.acm.org/detail.cfm?id=2898444
3. Feng, Lu, Bao, Wang, Lin, Wang, Li, Li.
   *Scaling Large Production Clusters with Partitioned Synchronization.*
   USENIX ATC 2021 (best paper).
   https://www.usenix.org/conference/atc21/presentation/feng-yihui

[ADR-0019]: 0019-phase1-cloud-vs-bench-discrepancy.md
[ADR-0024]: 0024-co-location-via-podaffinity.md
[ADR-0027]: 0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0028]: 0028-cycle-p99-is-regime-parametric.md
