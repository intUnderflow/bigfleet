# ADR-0031: ParSync-style partitioned synchronization — conditional follow-on for raised per-shard ceilings

## Status

Proposed, 2026-05-17. Conditional follow-on to [ADR-0029].
Promoted from Proposed to Accepted when **measured evidence**
shows OCC's conflict rate is dominating cycle p99 — either
because a future per-shard ceiling raise pushed the working set
past OCC's comfortable envelope, or because real-world conflict
rates at existing rungs are high enough that contention-reduction
would materially improve cycle latency. Either trigger justifies
the implementation cost.

Not strictly required for any rung of the uber-* ladder as
[ADR-0029] currently projects (steady-state conflict rate ≤ 0.15
puts ParSync's marginal benefit in the ~10–20% cycle-time range
at uber-500k), but worth promoting if those projections are
empirically wrong or if the deploy-burst pattern (recurring
cold-start equivalents per [ADR-0029] Open risks) drives
sustained high contention.

## Context

[ADR-0029] redesigns Phase 1 as a single-shard OCC scheduler. Its
performance envelope was projected at the per-shard ceiling
(`bigfleet.md` §5: 500K machines, §11: per-shard inventory
bounded by that ceiling). Above that, BigFleet **scales
horizontally** via additional shards: uber-1m runs as 2 × 500K,
uber-5m as 10 × 500K. Each individual shard sees the same
per-shard workload — `bigfleet.md` §11 is explicit that "roll-up
message ~2KB regardless of fleet size; per-cluster behaviour is
identical at any scale." OCC's per-shard envelope therefore
carries through unchanged across multi-shard rungs.

This is structurally distinct from how Alibaba's ParSync paper
[1] approached the problem. ParSync took a *single* scheduler past
the Omega-style OCC envelope by partitioning the cell state and
rotating which partition each scheduler reads with bounded
staleness — eliminating spurious conflicts that arose when many
schedulers re-read the same full state. Their measurement
context: 40 K decisions/sec on 100 K-machine clusters [1, §1].

For BigFleet:
- Per shard, OCC's envelope is comfortable up to the 500K
  ceiling. ParSync's win at uber-50k–500k is small or zero —
  the conflict-rate curve hasn't bent.
- Above the per-shard ceiling, the paper's current model says
  "add another shard," not "make one shard bigger." Multiple
  shards interact at the coordinator layer, not at the OCC
  layer.

ParSync therefore becomes interesting only if a **future** ADR
raises the per-shard ceiling — e.g., because operating a smaller
number of bigger shards has lower coordinator-side cost than
operating many small shards. That's a paper-level decision
("bigfleet.md" would need an explicit ceiling-raise) which this
ADR does not make. This ADR exists to record the design intent
for that scenario, so the path is documented when/if the
ceiling-raise question arises.

## Goals

If/when this ADR is promoted from Proposed to Accepted:

1. **Push OCC's single-shard envelope past 500K machines** by
   reducing conflict-rate growth as inventory cardinality
   increases.
2. **Layer on top of [ADR-0029]'s existing OCC infrastructure**
   — don't replace the commit broker, per-bucket sequence
   numbers, or the priority-on-conflict semantics.
3. **Preserve [ADR-0027] stage 5.1 attribution mirroring** with
   Phase 3.

## Non-goals

- **Replace OCC**. ParSync is purely a refinement to OCC's
  contention surface; it doesn't change the commit semantics.
- **Multi-shard reasoning**. Cross-shard coordination is the
  coordinator's job per `bigfleet.md` §11; nothing in this ADR
  touches cross-shard.
- **Pre-empt a per-shard ceiling raise**. This ADR doesn't argue
  for raising the ceiling; it only describes how OCC would
  extend if someone else does.

## When this becomes worth implementing

Two equally-valid promotion triggers. Either is sufficient on its
own; both are observable.

### Trigger 1 — measured conflict rate dominates cycle p99

[ADR-0029] projects OCC at ≤0.15 conflict rate steady-state under
the realistic catalog. If post-merge measurements show conflict
rate materially higher (e.g., ≥0.3 sustained, or ≥0.5 during
deploy-bursts), then conflict-retry overhead is dominating cycle
p99 and ParSync is the right lever — even at uber-50k or uber-
500k where OCC alone clears envelopes.

ParSync's expected impact (per [1, §3]'s analytical model and
their measured curves):

| Conflict rate before | After P=8 partition rotation |
|---|---|
| 0.15 | ~0.05 (small improvement) |
| 0.30 | ~0.10 (meaningful improvement) |
| 0.50 | ~0.15 (substantial improvement) |
| 0.80 (approaching saturation) | ~0.30 (back in safe zone) |

The "faster is better" case: even at 0.15 baseline conflict rate,
ParSync trims ~10% from cycle wall time. That's worth doing if
the implementation cost is justified.

The "must do" case: if conflict rate trends toward saturation
(approaching 1.0), ParSync moves from optimization to requirement
— without it, cycles don't converge.

### Trigger 2 — per-shard ceiling raise

Independent of conflict rate at today's ceiling: if a future ADR
raises the per-shard machine ceiling above the current 500K cap
in `bigfleet.md` §5 / §11, OCC's working set grows. At ~1M
machines/shard ParSync helps modestly; at ~2M+ it becomes
load-bearing because OCC's conflict rate crosses Omega's measured
saturation threshold [2, §5.1].

This trigger is independent of trigger 1 — a ceiling raise is a
paper-level design choice (e.g., because operating fewer-larger
shards has lower coordinator-side cost than many-small shards)
that doesn't depend on whether the existing per-shard envelope
has problems.

### Why this isn't needed for the uber-* ladder *as currently projected*

| Rung | Per-shard machines | Per-shard Needs | Multi-shard? |
|---|---|---|---|
| uber-5k | ~5K | ~7,759 | 1 shard |
| uber-50k | ~50K | ~42,680 | 1 shard |
| uber-500k | ~500K | ~776,000 | 1 shard (at ceiling) |
| uber-1m | ~500K | ~776,000 | 2 shards × 500K |
| uber-5m | ~500K | ~776,000 | 10 shards × 500K |

Every multi-shard rung sees the **same per-shard workload** as
uber-500k. OCC's projected ~10 s cycle p99 at uber-500k therefore
carries through to uber-1m and uber-5m unchanged. The fleet-level
scaling is the coordinator's responsibility (cross-shard quota
arbitration, shortfall escalation) — out of scope for both
[ADR-0029] and this ADR.

ParSync is **not** the answer to "scale OCC past 776K
Needs/shard" — that question doesn't currently exist in BigFleet
because the per-shard ceiling caps it. If/when the ceiling is
raised:

- If raised to ~1M machines/shard → ~1.5M Needs/shard. OCC's
  envelope is tight but probably still works. Maybe ParSync
  helps; maybe not. Measure first.
- If raised to ~2M machines/shard or above → OCC's conflict
  rate likely exceeds Omega's measured ≤ 1.0 saturation
  threshold [2, §5.1]. ParSync becomes load-bearing.

This ADR is the design that gets pulled off the shelf in case (b).

## Design overview

ParSync's core mechanism [1, §3]: **partition the shared cell
state into P partitions; round-robin synchronise one partition
per scheduler per cycle**. Each scheduler operates on a view
where one partition is fresh and the others are slightly stale.
Their analytical conflict-rate model:

```
expected_conflicts ≈ NK − S_idle + S_idle × (1 − K/S_idle)^N
```

where N = scheduler count, K = decisions/sec/scheduler, S_idle =
idle slots per partition. The partition rotation shifts staleness
from "uniformly stale" to "asymmetrically stale" (one partition
fresh, others 1–P cycles old). [1, §3.2] argues that the
asymmetry is itself the win: a smaller maximum staleness saves
more conflicts than the larger minimum adds.

For BigFleet, the partition dimension would be **per-bucket**
(matching ADR-0029's per-bucket sequence-number CAS). Each
worker on each cycle freshly re-reads one partition of the
claimed-set; the others come from carry-forward (similar
structure to [ADR-0030]'s incremental design but applied to
inventory state, not Need state).

```
┌────────────────────────────────────────────────────────────────┐
│                  ParSync-extended OCC cycle                    │
│                                                                │
│   Claimed-set is divided into P partitions (by bucket-hash)    │
│   ┌──────────┬──────────┬─────┬──────────┐                     │
│   │ part 0   │ part 1   │ ... │ part P-1 │                     │
│   └──────────┴──────────┴─────┴──────────┘                     │
│                                                                │
│   Each worker on cycle N:                                      │
│     - Reads partition (N mod P) fresh from broker              │
│     - Reads partitions (N-1) ... (N-P+1) from carry-forward    │
│     - Proposes against (fresh ∪ stale) state                   │
│                                                                │
│   Commit broker validates ALL partitions at commit time        │
│   (per-bucket seqno CAS catches staleness conflicts)           │
│                                                                │
│   Three strategies for partition selection per ParSync §4:     │
│     - latency-first   : pick from freshest partition only      │
│     - quality-first   : pick best slot regardless of partition │
│     - adaptive        : switch based on observed delay         │
└────────────────────────────────────────────────────────────────┘
```

Per-bucket seqno CAS from [ADR-0029] is already the right
primitive: a stale read on a partition is detected at commit
time and the proposal retries with a fresh read. The new
machinery is: **how partitions are chosen for the fresh read**,
and **how stale reads are tolerated** when their staleness is
within the rotation window.

## Detailed design (sketch)

This ADR is Proposed; the detailed design fills in when it's
promoted. Sketch level only:

### Partition assignment

Buckets are hashed into P partitions deterministically. P is a
configuration parameter; ParSync's measurement [1, §5] tested
P ∈ {4, 8, 16, 32}; P=8 was their best for 16 schedulers / 100K
machines. BigFleet's analog at hypothetical 2M-machine shards
would scale similarly — likely P = 16–32.

### Partition rotation

Each cycle, each worker selects which partition to refresh.
ParSync's three strategies:

- **Latency-first**: rotate strictly through partitions, always
  using the freshest one. Lowest conflict rate; least
  flexibility on choice.
- **Quality-first**: search all partitions; pick the best
  candidate regardless of freshness. Highest scheduling quality;
  may have higher conflict rate.
- **Adaptive**: switch between latency-first and quality-first
  based on observed scheduling delay. The recommended default.

For BigFleet, start with **adaptive** by default; expose
latency-first / quality-first as `--phase1-parsync-strategy`
flag overrides.

### Conflict resolution

Unchanged from [ADR-0029]: the commit broker validates per-
bucket sequence numbers at proposal time. A stale-partition
read that resulted in a now-invalid proposal returns Conflict;
the worker re-observes the conflicted partition (forced
freshness on that partition) and retries.

### Carry-forward state

Carry-forward of stale-partition state across cycles is
structurally similar to [ADR-0030]'s carry-forward for unchanged
Needs. The two ADRs would share infrastructure for state
preservation across cycle boundaries — they'd likely be
implemented together if both are promoted.

### Drift detection

ParSync's bounded-staleness property means at most P cycles
between any partition's fresh-read. Drift cannot accumulate
beyond P cycles; the periodic refresh enforces this. No
additional drift-detection mechanism beyond [ADR-0030]'s if
that's already in place; if not, a per-partition digest check
serves the same purpose.

## Performance projections (hypothetical)

These numbers are illustrative only. Real projections would be
derived once a per-shard ceiling raise is on the table and the
measurement context is concrete.

Assuming a hypothetical 2 M-machine shard at ~3 M Needs:

- OCC alone: conflict rate ~0.6 (in Omega's danger zone);
  cycle p99 ~30 s estimated; sub-100 ms target out of reach.
- OCC + ParSync (P=16): conflict rate ~0.15 (similar to OCC at
  uber-50k today); cycle p99 ~3 s estimated.

A 10× improvement at the contention frontier. The cost is the
implementation complexity (carry-forward state, partition
selection logic, possibly the adaptive strategy's feedback
loop) and an extra memory hit for P-partition state.

## Invariants

Preserved from [ADR-0029]: every OCC invariant (commit-broker
priority, per-bucket seqno, retry budget, attribution mirroring).
Preserved from [ADR-0027]: stage 5.1 mirroring with Phase 3.

New invariants if implemented:
- **Bounded staleness**: any partition is at most P cycles
  stale before being refreshed.
- **Commit-time consistency**: the broker validates per-bucket
  seqno on every proposal; a stale read producing an invalid
  proposal returns Conflict (not Committed-with-stale-data).

## Open risks (for the hypothetical future implementation)

- **Carry-forward memory growth**: per-partition state per
  worker × P partitions × N workers can be material on
  large-machine-count shards. Bound carefully.
- **Adaptive strategy oscillation**: ParSync's adaptive
  strategy can oscillate between latency-first and quality-
  first under specific workloads. Their paper [1, §4] notes
  this risk.
- **Workload-shape sensitivity**: ParSync's win is largest
  when contention is high and per-bucket demand is uniform.
  BigFleet's realistic catalog has highly varied per-bucket
  demand (some buckets hot, most cold); the win may be
  smaller than ParSync's measurements suggest.

## Future work

- **Per-shard ceiling raise ADR (prerequisite).** Before this
  ADR can be promoted, a separate ADR must justify and specify
  a higher per-shard ceiling than the current 500K. That ADR
  decides whether the operational benefit of fewer-larger-
  shards outweighs the increased per-shard complexity.
- **Cross-shard state synchronisation** is an entirely
  different problem from ParSync's per-shard partition
  rotation. ParSync doesn't help there; the coordinator's
  cross-shard mechanisms ([ADR-0028]'s deferred PDB work)
  remain the right tool.

## Alternatives considered

- **Skip ParSync; let the per-shard ceiling stay at 500K.**
  The current `bigfleet.md` design. Multi-shard fan-out is the
  scaling axis above 500K. This is the **default** if no
  per-shard ceiling raise is justified.
- **OCC alone past 500K**. The hypothesis that Omega-style OCC
  scales indefinitely. Empirically [2, §5.1] this breaks at
  conflict rate ≥ 1.0 — a specific scale-dependent threshold.
  Without ParSync, the ceiling is set by where OCC's conflict
  curve crosses 1.0 — likely between 1–2 M machines/shard
  depending on workload.
- **Pre-emptive implementation now** (without waiting for the
  ceiling-raise justification). Rejected: implementation
  complexity is non-trivial; the win is zero until the
  ceiling-raise question exists; YAGNI per `CLAUDE.md`.

## References

1. Feng, Lu, Bao, Wang, Lin, Wang, Li, Li.
   *Scaling Large Production Clusters with Partitioned Synchronization.*
   USENIX ATC 2021 (best paper).
   https://www.usenix.org/conference/atc21/presentation/feng-yihui
2. Schwarzkopf, Konwinski, Abd-El-Malek, Wilkes.
   *Omega: flexible, scalable schedulers for large compute clusters.*
   EuroSys 2013.
   https://people.csail.mit.edu/malte/pub/papers/2013-eurosys-omega.pdf

[ADR-0027]: 0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0028]: 0028-cycle-p99-is-regime-parametric.md
[ADR-0029]: 0029-phase1-omega-style-occ.md
