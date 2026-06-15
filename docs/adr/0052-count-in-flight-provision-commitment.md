# ADR-0052: the shard counts its own in-flight provision commitment against the deficit

## Status

Accepted, 2026-06-15 — author decision (ship the amendment after the
#66/#74 diagnosis → cloud A/B → the Opus re-arch scout). Amends
ADR-0045's "no in-flight discounting, no second ledger" (0045 §Decision,
L40) for the pre-Configuring runway only. Engine change + a narrow
machine.Invariant relaxation; no wire, no paper diff. Refines/extends
ADR-0051 (gang-granular attribution) one machine-state earlier.

## Context

The #66/#74 "pre-Configuring runway" over-acquire. A machine the shard
itself launched (Speculative→Creating, the Create RPC) is invisible to:
the coverage walk, which credits only {Configured, Configuring}
(pkg/decision/occ/seed.go:90, :161); the acquirable pool, which is only
{Idle, Speculative} (pkg/decision/occ/cycle.go:215); and attribution —
Provision stamps nothing (pkg/shard/execute.go:193, a nil mutator). By
ADR-0045's "capacity counts iff bound," a Creating machine is not bound,
so it correctly counts for nobody — and the shard therefore re-derives
the full deficit every cycle and re-acquires. Over-acquire = dwell+1,
dwell = providerCreateLatency ÷ cyclePeriod.

A cloud A/B (bigfleet-uber #75; Create-latency 0 / 30s / 60s, churn held
fixed) confirmed this is **material**: Provisions + reclaim-actions scale
**super-linearly** with Create latency (82 → 9,073 → ~196K), runaway at a
realistic 60s provider Create (shardCycle p99 0.5s → 7.2s). Coverage
stayed met (shortfalls ≈ 0) — it is resource-waste + engine-degradation,
not a coverage failure. The kwok harness's ~1.6s fake-Create badly
under-represented it; on a real provider's minutes-long Create it is
catastrophic. This is the exact analog of the *Configuring*-invisibility
that M77g/h (ADR-0051) closed, one machine-state earlier.

A two-axis state-machine re-architecture was scouted as an alternative
(see ADR-0053) and judged **worse** for this bug: it does not fix the
over-acquire (it needs this identical accounting change anyway) and
carries a 149-reference, 9-package blast radius. So the fix is this
surgical amendment, not a restructure.

## Decision

The coverage walk credits, in addition to {Configured, Configuring}, the
shard's **own in-flight Creating machines**: a Creating machine carrying
the matching attribution (AssignedGroup for `Same` gangs;
AssignedNeedFingerprint otherwise) counts toward that Need's coverage.

This amends ADR-0045's L40. ADR-0045 said "a binding counts from the
moment it is made — before the node exists." This extends the principle
**one step earlier than the binding**: **the shard counts its own
in-flight provision commitment.** A Creating machine the shard itself
launched for a Need is supply-in-flight.

Why this is *not* what ADR-0045 rejected:

- **Not "in-flight discounting" of arbitrary supply.** Only the shard's
  OWN committed machine, attributed to the Need it was provisioned for,
  counts. An unattributed Creating machine still counts for nobody.
- **Not a second ledger.** The Creating machine exists in the one supply
  ledger — the machine state machine. The credit is read from current
  state, not from any parallel structure or cross-cycle memory.
- **Not a grace / aging rule.** It is **state-driven, no clock**: the
  credit exists exactly while the machine is Creating-with-attribution,
  and auto-expires by transition — it matures (→Idle→Configuring→
  Configured, then counted by the existing rule) or Fails (→ drops out,
  the deficit re-opens next cycle).
- **Double-supply stays impossible.** A Creating machine is credited once
  (in coverage) and is never also in the acquirable pool ({Idle,
  Speculative}) — so the deficit it shrinks is the same deficit the
  acquirable claims fill; counting it simply means the shard claims fewer
  Idle/Speculative this cycle instead of re-Provisioning.

### Implementation

1. **Stamp at Provision-time.** At Speculative→Creating
   (pkg/shard/execute.go), stamp AssignedGroup + AssignedNeedFingerprint
   from the Need the acquisition was planned for — mirroring the
   Configure-time stamp (M72 / ADR-0051, execute.go:286-294). The Create
   action must carry its target Need's attribution.
2. **Credit gang-own / fingerprint-own Creating** in the coverage walk
   (pkg/decision/occ/seed.go), reusing the existing own-predicate
   (seed.go:199: AssignedGroup == Need.Group ∧ AssignedNeedFingerprint ==
   Need.Fingerprint()), extended to include StateCreating. Creating is
   NOT added to the acquirable pool.
3. **Relax machine.Invariant** (pkg/machine/machine.go:317-323) to permit
   AssignedGroup + AssignedNeedFingerprint (both Go-only fields, not on
   the wire) on a Creating machine. Host and Cluster stay forbidden on
   Creating — it is still not bound.

Covers both `Same` (gang-own coverage) and non-`Same` (fingerprint-own),
because the over-acquire is general (the sim repro's deficit=1 arm is
non-gang).

Attribution precision: a Creating machine is unbound (no cluster), and
`Profile.Fingerprint()` / `Group` omit the cluster ID, so the **non-`Same`**
credit is per-*fingerprint*, not per-*cluster* — a Creating machine one
cluster provisioned can be credited to a higher-priority same-fingerprint
Need in another cluster. This is bounded and self-correcting (the shared
claim ledger credits each machine exactly once, so total credit equals the
actual own-Creating count; coverage stays met; the matured Idle is shared
anyway) and does not reintroduce the over-acquire. The `Same` arm is
gang-exact (Group is gang-unique). Cross-cluster per-cluster precision
would require a cluster hint on the Provision attribution — out of scope;
recorded so the precision limit is designed-for, not rediscovered.

## Consequences

- The over-acquire collapses to 1 acquisition per genuine loss. The sim
  gate (sim/preconfiguring_runway_repro_test.go) **flips**: the
  over-acquire law assertion goes from `acq == dwell+1` to `acq == 1`, and
  the sustained-churn reclaims/loss drops to the dwell0 (instant-Create)
  async-actuation floor. That flip is the fix's acceptance test.
- **Bounded exposure:** a Creating machine that Fails before maturing
  leaves a one-cycle under-acquire (the deficit re-opens the next cycle
  when the Failed machine drops out of the credited set). Self-correcting;
  the repro converges to bound = demand. Same shape as a Configuring
  machine failing today.
- **No wire / paper change.** AssignedGroup/AssignedNeedFingerprint on
  Creating are Go-only; the MachineState wire enum and the provider
  contract are untouched. The paper's §5 (Host, Cluster) model and §8/§16
  division of labour are unchanged — "counts from commitment" is
  consistent with §5's "a binding counts from the moment it is made."
- **Validation:** the flipped sim gate (Docker-free), then a warm-host
  cloud re-run of the #75 A/B — the over-acquire must now be ~flat across
  Create latency and RAS at the dwell0 floor.
- The residual steady-state pod-bind latency under churn that remains
  after this fix is genuine reprovision cost (a lost machine cannot rebind
  faster than the provider Create), and is an SLO-scoping question, not an
  engine defect — tracked separately.
