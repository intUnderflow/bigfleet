# The machine state machine

This is the code-level deep-dive on `pkg/machine` — the domain `Machine` record, its
state machine, the stable `ID` that survives the whole lifecycle, and the `Profile`
aggregation type. It bridges the high-level table in [concepts.md](../concepts.md#machine-state-machine)
("Machine state machine") and paper §5 ("Shard architecture / Machine model") down to the
source. Read those two first for the *what*; this doc is the *why* and the exact contract.
If you only need the legal transitions at a glance, jump to [The transition table](#the-transition-table).

`pkg/machine` is the bottom of the shard's dependency stack: `pkg/inventory`, `pkg/decision`,
`pkg/conv`, and `pkg/provider/fake` all build on these types. It imports nothing from BigFleet —
only stdlib (`errors`, `fmt`, `math`, and `strconv` in `shardmetadata.go`). That isolation is
deliberate (see [Why plain Go structs](#why-plain-go-structs-not-proto)).

---

## The eight states

`State` is a `uint8` (`pkg/machine/machine.go:29`). There are **eight** values, but the
operative set the engine reasons about is **seven** — the eighth, `StateUnspecified`, is the
zero value and is never a legal resting state for a real machine. CLAUDE.md's shorthand
("3 stable + 4 transitional + Failed" = 7) counts the seven real states; concepts.md and the
`State` doc comment (`machine.go:28`) say "eight" because they count `StateUnspecified` itself.
The code adds `StateUnspecified = iota` as the guard zero value so that a
default-constructed `Machine{}` fails `Invariant()` (`pkg/machine/machine.go:341`) rather than
masquerading as a valid Speculative slot.

| `State` const | Name | Host | Cluster | Class | Meaning |
|---|---|---|---|---|---|
| `StateUnspecified` | — | — | — | zero/guard | default value; never legal at rest |
| `StateSpeculative` | `Speculative` | nil | `""` | **stable** | quota slot, not yet provisioned |
| `StateCreating` | `Creating` | nil | `""` | transitional | provider running `Create` (Speculative → Idle) |
| `StateIdle` | `Idle` | set | `""` | **stable** | real hardware, no cluster binding |
| `StateConfiguring` | `Configuring` | set | maybe | transitional | provider running `Configure` (Idle → Configured) |
| `StateConfigured` | `Configured` | set | set | **stable** | real hardware, joined to a cluster, serving |
| `StateDraining` | `Draining` | set | set | transitional | provider running `Drain` (Configured → Idle) |
| `StateDeleting` | `Deleting` | set | `""` | transitional | provider running `Delete` (Idle → Speculative) — **cloud only** |
| `StateFailed` | `Failed` | — | — | terminal-pending-cleanup | last transition timed out; `LastError` set |

`State.String()` (`pkg/machine/machine.go:44`) is the canonical name source; an out-of-range
value renders as `State(N)`, so a corrupted byte is visible in logs rather than blank.

### Stable vs transitional — and why it matters

`State.IsStable()` (`pkg/machine/machine.go:69`) returns true for exactly
`{Speculative, Idle, Configured}`. This is the load-bearing distinction, not a label:
**the decision engine only allocates from, and only emits transitions that begin in, a stable
state.** A machine mid-`Configure` is neither claimable as free capacity nor a valid preemption
victim until it settles. `Failed` is *not* stable for the engine's purposes — `IsStable()`
returns false for it — even though it is a resting state; a failed machine is inventory awaiting
cleanup, never a candidate. `machine_test.go:71` pins the exact membership of both sets.

The three stable states are distinguished by the `(Host, Cluster)` cross-product, and the paper
states the one impossible corner explicitly (§5): `host=nil, cluster≠0` cannot occur. `Invariant()`
enforces the full grid (`pkg/machine/machine.go:315`):

- `Speculative`, `Creating` → host **must be empty**, cluster **must be empty** (a quota slot has no hardware).
- `Idle`, `Configuring`, `Deleting` → host **must be set**; cluster may be set during `Configuring` because the destination is already known.
- `Configured`, `Draining` → host **and** cluster must be set.
- `Failed` → no structural grid; but `LastError` **must** be non-empty (a Failed machine with no diagnosis is itself an invariant violation — `machine.go:338`).

---

## The transition table

`Idle` is the hub. Every cross-cluster transfer routes through it — there is no direct
`Configured(A) → Configured(B)`. The paper (§5) states the canonical path:
`Configured → Drain → Idle → Bootstrap → Configured(new cluster)`.

```
                Create                Configure
  Speculative ─────────▶ Creating ───────────────▶ Idle ──────────▶ Configuring
       ▲                    │                       ▲  ▲                 │
       │ Delete             │ (timeout)             │  │                 │ (timeout / rollback)
       │                    ▼                       │  │      ┌──────────┼──────────┐
   Deleting ◀── Idle      Failed                    │  │      ▼          ▼          ▼
  (cloud only)             ▲ ▲ ▲                  Draining  Configured  Failed     Idle
       │                   │ │ │                     ▲          │              (rollback,
       │ (timeout)         │ │ └── Draining          │          │ Drain          M44.4)
       ▼                   │ └──── Deleting          └──────────┘
     Failed                └────── Creating
```

The legal edges are encoded as a static map, `validTransitions` (`pkg/machine/machine.go:235`):

| From | Allowed `to` |
|---|---|
| `Speculative` | `Creating` |
| `Creating` | `Idle`, `Failed` |
| `Idle` | `Configuring`, `Deleting` |
| `Configuring` | `Configured`, `Failed`, **`Idle` (rollback)** |
| `Configured` | `Draining` |
| `Draining` | `Idle`, `Failed` |
| `Deleting` | `Speculative`, `Failed` |
| `Failed` | *(none — terminal-pending-cleanup)* |

Two edges have non-obvious rationale, both documented inline:

1. **`Deleting` is cloud-only.** `Idle → Deleting → Speculative` reclaims real hardware back to
   a pure quota slot. For bare metal the hardware is permanent, so the provider rejects the
   `Delete` call (`machine.go:240`); the machine stays `Idle` forever. This matches paper §8
   Phase 3: "Idle → Speculative lazily per provider (bare metal: forever; on-demand: minutes;
   spot: ~1m)." The state machine permits the edge; the provider decides whether to walk it.

2. **`Configuring → Idle` is a rollback, distinct from `Configuring → Failed`** (M44.4 Drop B,
   `machine.go:228`). When the shard abandons a bootstrap *before the provider is touched* —
   e.g. the operator-side `BootstrapRequest` times out under the cycle context — the machine
   must return to the `Idle` pool for retry next cycle. `Failed` is terminal, so routing every
   orchestration timeout to `Failed` permanently shrank the Idle pool. The rollback edge keeps
   the machine reusable. The two cases are semantically different: rollback = "we never got
   far enough to consume the machine"; `Failed` = "a real provider-side failure consumed it."

### How transitions are checked

`CanTransition(from, to)` (`pkg/machine/machine.go:251`) is the predicate;
`CheckTransition(from, to)` (`machine.go:268`) is the error-returning wrapper that wraps the
`ErrInvalidTransition` sentinel (`machine.go:263`) with `from → to` detail. `CanTransition`
special-cases `to == StateFailed`: **any** transitional state (`Creating`, `Configuring`,
`Draining`, `Deleting`) may fail, so that edge is computed (`from` is one of the four
transitionals) rather than enumerated in the map. Stable states cannot fail directly — a stable
machine is at rest, with no in-flight provider operation to time out. `machine_test.go:11`
exhaustively lists the twelve legal edges; `machine_test.go:36` lists representative illegal
ones (e.g. `Speculative → Idle` skipping the host-attach step; `Configured → Configuring`
skipping the drain; `Failed → Idle` auto-resurrection).

**Who enforces it.** `pkg/machine` only *defines* the table; it does not gate any write. The two
consumers are:

- `pkg/inventory/inventory.go:100` — `Inventory.Apply` calls `CheckTransition(old.State, m.State)`
  on every state-changing write, plus `m.Invariant()` first (`inventory.go:90`). This is where a
  reconciler's bad output is rejected: an invalid transition surfaces as an `Apply` error, not
  a silent corruption. (A same-state `Apply`, e.g. an `Idle → Idle` reconcile field-merge,
  skips the transition check by design — `inventory.go:99`.)
- `pkg/provider/fake` — the test fixture walks the same table so simulated providers can't
  fabricate illegal sequences.

Keeping the table in `pkg/machine` and the enforcement in `pkg/inventory` means both the
production inventory and the test provider share one source of truth for legality.

---

## The stable `ID`

`ID` is a `string` (`pkg/machine/machine.go:21`), and its defining property is that it is
**stable across the entire lifecycle**. A `Speculative` quota slot that gets provisioned into a
real `Idle` host keeps the same `ID`; only the `Host` reference fills in (`machine.go:18`). The
identity is BigFleet's, assigned when the slot is created, and it never changes as the machine
walks Speculative → Creating → Idle → Configuring → Configured → … → Idle → Deleting →
Speculative. This is why the engine can reason about "this machine" continuously: the `ID` is the
join key across inventory snapshots, decision actions, provider RPCs (idempotency is keyed by
`(machine_id, target_state)`, paper §5), and the durable `shard_metadata` (below). `ClusterID`
(`machine.go:25`) is the orthogonal binding — empty means unbound; it is *not* part of the
machine's identity, it is part of its current assignment.

---

## The `Machine` record and its assignment fields

`Machine` (`pkg/machine/machine.go:134`) has the **same shape regardless of state** — there is
no per-state subtype. The fields split into three groups:

**Identity / placement** — `ID`, `State`, `Host` (`HostRef{Provider, Ref}`, `machine.go:101`),
`Cluster`, `Profile`.

**Provider-declared cost inputs** — `PricePerHour` (USD/hour, zero for bare metal) and
`InterruptionProbability` (hourly, `[0,1]`). These feed the locked cost formula and are
**provider-declared only**: `machine.go:145` states "No cluster-side override," matching the
hard rule. They are validated at ingest by `Invariant()` (see [Cost inputs](#the-cost-formula-and-its-input-bounds)).

**Assignment state** — the `Assigned*` fields, meaningful only around `Configured`:

| Field | Used by | Set / cleared |
|---|---|---|
| `AssignedPriority` (`int32`) | Phase 2 victim scoring (priority gap) | set on Phase 1 Bootstrap/Provision; cleared on drain |
| `AssignedInterruptionPenaltyDollars` | Phase 2 victim scoring; `EffectiveCost` | set at Configure |
| `AssignedReclamationPenaltyDollars` | Phase 2 victim scoring; Phase 3 release tiebreak | set at Configure |
| `AssignedNeedFingerprint` | Phase 1 exact 1:1 Need attribution | set on Configuring → Configured; cleared on drain |
| `AssignedGroup` | ADR-0051 Same-domain gang tiebreak | set at Configure; cleared on drain |

Two of these encode subtle decisions worth flagging:

- **The two penalties are distinct and both stored.** `AssignedInterruptionPenaltyDollars` is the
  cost of *interrupting the workload* (it appears in `EffectiveCost` and victim scoring);
  `AssignedReclamationPenaltyDollars` is the operational value tied to *this specific machine*
  (e.g. burned-in GPUs — `machine.go:166`), used in victim scoring and the Phase 3 release
  tiebreak. They are not the same quantity and neither is derivable from priority. (The known
  hallucination is a single `operational_value` field; the real model is these two separate
  dollar values.)

- **`AssignedNeedFingerprint` exists because `machine.Profile` cannot stand in for it**
  (`machine.go:171`). `needs.Profile` holds *requirements* (label selectors); `machine.Profile`
  holds *resolved attributes*. One machine subset-matches many Needs' selectors, so counting
  assignments by profile-match over-counts whenever same-cluster Needs have nested selectors.
  Phase 1's deficit math needs exact 1:1 attribution — "how many machines are *currently assigned
  to this Need*," not "how many *could match* it" — so the satisfied Need's fingerprint is stamped
  onto the machine at Configure time.

`Allocatable` vs `Profile.Resources`: `Profile.Resources` is what the hardware *is* (its base
shape); `Allocatable` is what it *actually provides* for Phase 1's aggregate-supply diff (ADR-0027,
aggregate-demand-vs-aggregate-supply). `EffectiveAllocatable()` (`machine.go:281`) returns
`Allocatable` when non-empty and falls back to `Profile.Resources` otherwise, so construction
sites and tests that never set `Allocatable` keep working and a provider only populates it when
real capacity diverges from the base shape.

---

## The `Profile` aggregation type

`Profile` (`pkg/machine/machine.go:114`) is "the bundle of attributes that make two machines
functionally interchangeable for assignment." Two machines with the same `Profile` are
equivalent inputs to a Phase 1 satisfaction check — this is the equivalence class the inventory
buckets by (e.g. `Inventory` keys snapshots on `(state, instanceType)`,
`pkg/inventory/inventory.go:169`).

Fields: `InstanceType`, `Zone`, `CapacityType`, `Resources` (`map[string]string` of canonical
quantity strings like `"96"`, `"768Gi"` — stored as strings to match the proto wire format,
avoiding a parse on every conversion), and `Labels` (provider-supplied, matched against
node-selector requirements). Note that since ADR-0027, demand no longer carries a per-replica
shape, so `Resources` is **purely machine-descriptive** (`machine.go:123`); the per-machine
capacity that Phase 1's resource-vector diff sums is `Allocatable` (falling back to
`Profile.Resources`), never demand's.

`CapacityType` (`machine.go:74`) is its own `uint8` enum —
`{Unspecified, BareMetal, Reserved, OnDemand, Spot}` — and is the "cost-of-holding category"
that drives idle-hold policy: the marginal cost of holding an idle bare-metal box is zero
(paper §4: fixed capacity, "the decision is allocation"), while an idle spot/on-demand instance
costs money and should be released. This is the same axis that decides whether the
`Idle → Deleting → Speculative` edge is ever walked.

---

## The cost formula and its input bounds

`EffectiveCost(interruptionPenaltyDollars)` (`pkg/machine/machine.go:297`) is the **locked**
formula (CLAUDE.md, ADR-0029, paper §16):

```
effective_cost = price_per_hour + (interruption_probability × interruption_penalty_dollars)
```

It is not pluggable and not configurable (`machine.go:296`). The result is a per-hour expected
cost comparable to another machine's price; the comment is honest that the formula is
"dimensionally inconsistent on paper" (per-hour `price` summed with a dollar-derived term) but
that this is the locked design choice. A negative penalty is clamped to zero (`machine.go:298`).

`Invariant()` (`machine.go:315`) validates **both** the structural state grid (above) and the
two provider-declared cost inputs: `InterruptionProbability` must be in `[0,1]` and not NaN
(`machine.go:344`); `PricePerHour` must be `≥ 0` and not NaN (`machine.go:347`). The cost-bound
half was added in the **ADR-0046 addendum** (M70). The production-readiness audit (arc 3) found
that a provider returning a negative price or a probability `> 1` fed straight into `EffectiveCost`
unchecked, and that `Invariant` ran only inside `inventory.Insert/Apply` with its errors discarded
at the reconcile call sites. The fix makes `Invariant` the screen at the shard's provider-ingest
boundary too — the reconcile slow paths and the `Create` ack — with policy **reject, loudly**
(`bigfleet_shard_machines_rejected_total{reason}`, never crash, never silently accept). See the
[actuation-safety-rails ADR](../adr/0046-actuation-safety-rails.md) addendum, "`machine.Invariant`
at provider ingest." `machine_test.go:168` pins the NaN/negative cost cases.

This is the [actuation safety](#tie-to-actuation-safety-rails-adr-0046) connection at the
`pkg/machine` layer: `pkg/machine` supplies the predicate; the rails in `pkg/shard` decide what
happens when it fires.

---

## Durable assignment state: `shard_metadata`

`shardmetadata.go` (M72) is the mechanism that lets the `Assigned*` fields survive a shard
restart. The shard holds assignment state in memory only; on restart it would zero. The provider,
however, is asked to **store and echo verbatim** an opaque `shard_metadata` map (provider.proto):
the shard writes it at Configure time and recovers it from `List`/`Get` after a restart.

- `EncodeShardMetadata(...)` (`pkg/machine/shardmetadata.go:36`) builds the map from the values
  `executeBootstrap` stamps at Configure time. The well-known keys are namespaced under
  `bigfleet.lucy.sh/` (the CRD group), so a raw metadata dump is self-identifying
  (`shardmetadata.go:19`).
- `Machine.DecodeShardMetadata()` (`shardmetadata.go:56`) restores the `Assigned*` fields at
  reconcile ingest. Its decode contract is deliberately forgiving (`shardmetadata.go:48`):
  - **Absent keys leave the field untouched** — an in-process fake may deliver `Assigned*`
    directly on the struct, so decode must not zero them just because the echo map is empty/partial.
  - **Unknown keys are ignored** — they belong to a newer shard.
  - **A malformed value is skipped, not fatal** — one mangled entry must not void the rest of the
    machine's protection state; all decode failures are `errors.Join`ed and returned for the
    caller to log, but the good fields are still applied.

  `shardmetadata_test.go` exercises all four behaviours (round-trip, absent-keys, malformed-skip,
  nil-map no-op).

Critically, the **inventory does not retain the raw map** (`machine.go:212`): reconcile decodes
the well-known keys into the typed `Assigned*` fields and drops `ShardMetadata`, keeping the
hot-path record inside the paper §9 budget of ~30–55 bytes per machine. The map's job is durable
transport, not hot-path storage. Why this matters: the recovered fields are exactly the inputs to
paper §8 victim scoring and Phase 1's 1:1 Need attribution — without them, a restarted shard would
forget which machines are protected by a high-priority workload and could mis-preempt them
(`shardmetadata.go:9`).

`AssignedGroup` (ADR-0051 / M77g) rides as one more store-and-echo key (`shardmetadata.go:24`,
empty for non-gang assignments). It carries the co-location `Group` so a restarted shard rebuilds
the gang attribution the Same-domain tiebreak reads — `AssignedNeedFingerprint` alone can't tell
two same-profile gangs apart.

---

## Why plain Go structs, not proto

The package comment (`pkg/machine/machine.go:1`) states it directly: these are **deliberately
plain Go structs, not proto-generated types.** The reasons are hot-path economics:

- **Value semantics.** `Machine` is passed and copied by value through the decision engine and
  inventory snapshots; proto messages carry internal state (unexported mutex/cache fields, the
  `sync.Once` lazy-init) that make value copies unsafe and force pointer-and-allocation discipline.
- **No proto runtime overhead.** No reflection-based marshaling, no `protoimpl` indirection on
  the path that runs over millions of records every cycle.
- **Small struct footprint.** Paper §9 budgets ~30–55 bytes per machine so that 500K machines fit
  in ~20MB (L3-resident, paper line 88). A proto message's footprint and per-field accessor
  overhead would not fit that budget. (This is also why the raw `ShardMetadata` map is dropped
  after decode — see above.)

Conversion to/from the wire protos happens once, at the gRPC boundary, in **`pkg/conv`**
(`MachineFromProto` / `MachineToProto` / `stateFromProto` / `MachineStateToProto`,
`pkg/conv/conv.go:201`, `:249`, `:285`, `:309`). The hot path never touches a proto type.

> **Stale reference — flag for fix.** The `pkg/machine/machine.go:9` package comment says
> conversion happens "in pkg/api/conv (added in M3 when the shard speaks gRPC)." There is no
> `pkg/api/conv`; the conversion package is **`pkg/conv`** (`conv.go`). The comment predates the
> package's final location. The doc above cites the correct path; the source comment should be
> corrected to `pkg/conv` in a follow-up. (Per CLAUDE.md "paper/plan wins, fix the code, note the
> divergence" — this is a code/comment divergence, noted here.)

---

## Tie to actuation safety rails (ADR-0046)

`pkg/machine` is the *predicate* layer; the *policy* lives in `pkg/shard`. Two of the M70 rails
lean directly on this package:

1. **`machine.Invariant` at provider ingest.** As above, the cost-input bounds (price `≥ 0` /
   not NaN, probability in `[0,1]`) are checked here, but the **reject-loudly** policy
   (`bigfleet_shard_machines_rejected_total{reason}`, mark a bad `Create` ack `Failed`, keep the
   last-known-good record) is enforced at the shard boundary. The audit's finding was precisely
   that `Invariant` existed but its errors were *discarded*; the rail wires the existing predicate
   into a mandatory gate. There is no knob — "it is not a rail an operator tunes, it is the
   contract being enforced" ([ADR-0046](../adr/0046-actuation-safety-rails.md) addendum).

2. **The Failed-is-terminal property bounds blast radius.** Because `StateFailed` has no outgoing
   transitions (`validTransitions` omits it entirely, `machine.go:246`) and `CanTransition`
   refuses `Failed → *`, a failed machine is inventory awaiting cleanup, never silently retried.
   This is the structural reason the M44.4 `Configuring → Idle` rollback edge had to exist: the
   alternative — routing every orchestration timeout to the terminal `Failed` — would permanently
   shrink the Idle pool, an unbounded slow-bleed the rails could not see. The rollback keeps a
   transient failure recoverable; `Failed` is reserved for failures that genuinely consumed the
   machine.

The other two rails (the Phase-3 reclaim blast-radius cap and the empty-roll-up quarantine) live
entirely in `pkg/shard` and do not touch `pkg/machine`; see
[ADR-0046](../adr/0046-actuation-safety-rails.md) for those. The relevant point for *this* layer:
`pkg/machine` defines what a valid machine and a valid transition *are*; everything that decides
what to *do* about a violation is one layer up, which keeps `pkg/machine` dependency-free and the
state machine itself purely declarative.
