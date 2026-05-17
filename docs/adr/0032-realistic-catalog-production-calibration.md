# ADR-0032: Realistic archetype catalog — production-calibrated workload distribution

## Status

Accepted, 2026-05-17.

## Context

The `realistic.yaml` archetype catalog (introduced in [ADR-0015],
extended in [ADR-0024], [ADR-0026], [ADR-0027]) is the workload
model against which every scaletest result is graded. Six
archetypes — gpu-training, gpu-inference, cpu-batch, cpu-service,
memory-db, critical-realtime — picked weighted-random to model
fleet demand.

scale-test bench measured Phase 1 at uber-50k under this
catalog at 9.41 ms/call × ~31K calls/cycle ≈ 5-minute Phase 1.
The diagnosis chain ([ADR-0028] empirical addendum →
scale-test review (follow-up)) traced this through
multiple layers:

1. NeedsTable size was ~388 Needs/cluster, not the ~8 the
   aggregated regime suggested.
2. The high Need count came from sameRack archetypes (gpu-
   training, memory-db) producing per-group Needs.
3. Under [ADR-0029]'s original mode classifier, 99% of Needs
   classified as `ModeAllOrNothing` — but Omega measured all-or-
   nothing as 2× the conflict-fraction cost of incremental.
4. The 99% AllOrNothing rate was a **catalog artifact**, not a
   workload reality: most "sameRack" archetypes (memory-db,
   caches) tolerate partial fills. Only true MPI-style gangs
   (gpu-training) need atomic semantics.

the scale-test review went deeper: even without the gang-
misclassification, the catalog substantially diverged from
industry production patterns. Specifically:

- **Long tail missing entirely.** Production fleets are bottom-
  heavy with tiny stateless services (200–500m CPU / 256–512 Mi
  per Pod). The prior catalog's smallest archetype was 2 CPU /
  8 Gi — it excluded the modal Pod by 4–10×. Roughly 60–70% of
  production Pods fell outside the modelled range.
- **Sidecars conflated with separate Pods.** Sidecars are
  containers within a Pod (sharing lifecycle, namespace, and
  summed resource request), not standalone scheduling units.
  The prior catalog modelled some workload as bare app
  containers; real Pods carry ~150–200m CPU / ~200–300 Mi of
  mesh + observability sidecar overhead on top.
- **Gang sizes understated.** Production ML training spans three
  tiers (hyperparameter sweeps 2–8 GPU, standard training
  16–32 GPU, foundation models 64–1000+ GPU). The prior 2–8
  range covered only the smallest tier.
- **Topology spread missing.** ~30–50% of production Pods carry
  `topologySpreadConstraints` — none of the prior archetypes
  modelled this.
- **Priority distribution wrong.** Prior catalog had everything
  at priority 100 or 1000+; production skews heavily to default
  (~85% of Pods), with a small fraction at elevated and
  critical levels.
- **Profile fingerprint cardinality understated.** Prior catalog
  produced ~8–10 fingerprints/cluster; production has ~30–60 in
  steady state due to long-tail label variety.

The cumulative effect: the catalog was harder than production
along the wrong axes (oversized resources, oversized gangs,
single-priority bin) and easier than production along axes that
matter (no long-tail Pod count, no spread, no sidecar overhead).
Performance projections derived against it were systematically
miscalibrated. The 99% AllOrNothing was the most visible
artifact but not the only one.

## Decision

Adopt the catalog shape that emerged from the design-review thread in
the scale-test review, with the small further refinement that
sidecars are folded into per-Pod resource shape (not modelled
as standalone Pods).

### Archetype set and weights

Ten archetypes (was six), weighted by Pod-count distribution:

| Tier | Weight | Resource shape (w/ typical sidecar overhead) | Priority | Co-location | Gang | Spread prob |
|------|-------:|---|---|---|---|------:|
| `tiny-stateless` | **70%** | 300–400m CPU / 384–500 Mi | default (100) | no | no | 0.45 (zone, max-skew 2, ScheduleAnyway) |
| `cpu-service` | **12%** | 2.2 CPU / 8.5 Gi (or 4.2 / 16.5 Gi) | elevated (1000) | no | no | 0.75 (zone, max-skew 1, DoNotSchedule) |
| `cpu-batch` | **6%** | 4 CPU / 8 Gi (or 8 / 16 Gi) | preemptible (100) | no | no | 0.15 (zone, max-skew 2, ScheduleAnyway) |
| `memory-cache` | **3%** | 2 CPU / 16 Gi | elevated (1000) | rack | no (allowPartial: true) | n/a |
| `stateful-db` | **3%** | 8 CPU / 32 Gi | elevated (1000) | rack | no (allowPartial: true) | n/a |
| `gpu-inference` | **2%** | 1 GPU / 8 CPU / 32 Gi | elevated (1000) | no | no | 0.50 (zone, max-skew 2, ScheduleAnyway) |
| `gpu-training-small` | **2%** | 8 GPU / 64 CPU / 256 Gi | elevated (1000) | rack | **yes, 2–8** | n/a |
| `gpu-training-medium` | **2%** | 8 GPU / 64 CPU / 256 Gi | elevated (1000) | rack | **yes, 16–32** | n/a |
| `gpu-training-large` | **1%** | 8 GPU / 128 CPU / 512 Gi | elevated (1000) | rack | **yes, 64–256** | n/a |
| `critical-realtime` | **1%** | 4 CPU / 8 Gi | critical (1,000,000) | no | no | 1.00 (zone, max-skew 1, DoNotSchedule) |

(Integer YAML weights with 1 = minimum representable; the last
two tiers are rounded up from 0.3% / 0.2% target weights.)

Weighted spread-carrying fraction: ~42.6% of Needs. Maps to
industry patterns where ~30–50% of Pods carry topology spread.

### Schema extensions

Two new optional fields on `Archetype`:

1. **`allowPartial: bool`** (forward-compat). Marks an archetype
   as "co-located but not gang" — emits the `Same()` requirement
   but the workload tolerates partial fills (replicas join the
   group incrementally). Today every Need is partial-fill-
   tolerant by default; AllowPartial records authorial intent so
   a future ADR adding explicit gang semantics can derive the
   opt-in from the catalog. memory-cache and stateful-db carry
   `allowPartial: true`. gpu-training-* (true gangs) do not.

2. **`spreadConstraintProb: float64`** + **`spreadConstraint:
   {topologyKey, maxSkew, whenUnsatisfiable}`**. Per-Pod /
   per-CR probability of emitting a topology spread constraint.
   The load-driver rolls the dice per Pod emission; on success
   it emits `Pod.Spec.TopologySpreadConstraints` (Pod mode) or
   `CR.Spec.TopologySpread` (CR mode). UPC's pod→CR translator
   already carries these through to the operator's roll-up.

### Sidecar treatment

Sidecars are containers within a Pod, not standalone Pods. Their
resource overhead (~150–200m CPU / ~200–300 Mi for typical
mesh + observability) is **inflated into the per-Pod resource
shape** of archetypes that carry them. Concretely:

- `tiny-stateless` modelled as 300–400m / 384–500 Mi (bare app
  ~200m / 256 Mi + typical 1–2 sidecars).
- `cpu-service` modelled as 2.2 CPU / 8.5 Gi (the sidecar
  overhead is the same absolute amount; relatively smaller bump
  against a large app container).
- `cpu-batch`, `gpu-*` don't run the mesh / observability
  sidecar stack — no inflation.

Sidecars are **not** a separate archetype. An earlier proposed
shape (15% sidecar weight) double-counted them as separate
scheduling units, which doesn't match Kubernetes Pod
semantics.

### What this re-baselines

Every scaletest run after this commit measures against the
re-calibrated catalog. Performance projections in [ADR-0029]
were built against the pre-calibration catalog and are now
conservative — the new catalog has:

- Fewer gang Needs (~3% AllOrNothing vs prior ~99% under
  ADR-0029's earlier inferred classifier)
- More independent small Needs (~70% tiny-stateless with no
  co-location or gang semantics)
- Higher Profile fingerprint cardinality from long-tail label
  variety (~30–60/cluster vs prior ~8–10)
- Realistic topology spread on ~42% of Needs (was 0%)

Phase 1 / Phase 3 attribution invariant ([ADR-0027] stage 5.1)
holds: the catalog change affects what the load-driver emits;
the shard's claim and reclaim attribution logic is unchanged.

## Consequences

- **Re-baseline of uber-5k** (the scale-test review's expected
  next step) against the corrected catalog. Expected: per-Need
  cost histogram, mode classification breakdown (~97% Inc /
  ~3% AllOrNothing), conflict-rate estimate, NeedsTable
  cardinality. Prior uber-5k baseline at `00ef120` (130 µs/Need,
  1.02 s cycle p99, 247K binds / 249K target) remains the
  published realistic-regime row in `docs/scaletest-results.md`
  but is annotated as pre-calibration.

- **Comparability with pre-calibration runs is annotated, not
  silent.** `docs/scaletest-results.md`'s realistic-regime
  section grows a "calibration generation" column or footnote
  distinguishing pre-#19 from post-#19 results. Pre-calibration
  numbers are not deleted — they're the empirical evidence
  underlying [ADR-0028]'s addendum and [ADR-0029]'s motivation.

- **[ADR-0029]'s performance projections** are now conservative
  rather than optimistic. The corrected catalog should make
  cycle p99 numbers tighten favorably (fewer gang Needs ⇒
  lower per-call cost; more independent small Needs ⇒ lower
  conflict rate). The post-merge re-baseline measures by how
  much.

- **`docs/scaletest-progress.svg`** and related visualisations
  may need a regenerate after the re-baseline; the
  `site/scripts/sync-scaletest.mjs` already emits both files
  from the run results.

- **Operator translation paths are unchanged.** The new
  `TopologySpreadConstraints` and `allowPartial` are
  load-driver / catalog concerns; UPC and operator rollup
  already handle Pod → CR → Need carry-through for spread per
  the existing [fleet-scale-kubernetes.md] §6 capacity contract.

- **Bench fixtures referencing the old catalog shape** (e.g.,
  `BenchmarkPhase1_Uber5K_LateRun`'s `realisticCatalog()`
  fixture in test code) may need updating to match the new
  archetype set. The harness load-driver reads the YAML, but
  inline Go test fixtures don't; spot-check during the re-
  baseline.

## Future work

Four catalog-side improvements deferred to follow-on work:

- **Cross-shard topology spread** for stateful archetypes
  (sharded databases). Production DBs emit `Same(rack)` per
  shard with different anchor racks per shard — this is
  operator-emergent and currently not modelled by the load-
  driver. Addressed by extending the load-driver to choose
  different anchor rack values per shard during emission.
  Doesn't require a catalog schema change; just a load-driver
  behavior addition.

- **Deploy-burst modelling.** `churnPerMinute: 0.02` smears
  churn evenly. Production sees deploy-burst patterns: 50–500
  Pods over 30 seconds, batched by canary %. This looks
  structurally like cold-start to OCC ([ADR-0029] Open risks
  flags it). Modelling: add `burstPattern` field to load
  profile (interval, magnitude, ramp shape).

- **Init containers**. Pod request is `max(sum_init, sum_app)`.
  Most init containers are tiny; some (ML model artifact
  downloaders) briefly hold large memory. Negligible for v1
  but tracking as a known omission.

- **PodDisruptionBudgets**. The realistic catalog should
  eventually model PDBs (`maxUnavailable: 25%`, etc.). This
  ties to the PDB-respecting preemption ADR (deferred per
  [ADR-0029] Open risks); not a catalog-only concern.

## Alternatives considered

- **Leave the catalog as-is; document the gap.** Considered
  briefly. Rejected because BigFleet's headline performance
  numbers are measured against this catalog — letting the
  miscalibration stand would mean every uber-* result is
  benchmarked against a workload that doesn't represent
  production. The cost of doing the re-calibration once is
  smaller than the cost of misattributing future scaletest
  signals.

- **Tweak weights only; keep the same six archetypes.**
  Considered. Rejected because the long-tail absence is
  structural — there's no archetype the modal production Pod
  fits into in the prior catalog. Just up-weighting the
  smallest existing archetype (cpu-service at 2 CPU / 8 Gi)
  doesn't capture 200m / 256Mi reality. A new archetype is
  needed.

- **Operator-side opt-in for `allowPartial`** via a new proto
  field on `CapacityNeed`. Rejected for v1 because the
  distinction doesn't yet matter at the BigFleet wire level
  (today every Need is partial-fill-tolerant; ADR-0029's
  `ModeAllOrNothing` is reserved for future explicit opt-in).
  Adding a proto field now would commit to wire-format
  evolution before there's a runtime behavior that depends
  on it. The harness's `allowPartial` is documentation /
  forward-compat; a future ADR will add the proto field when
  the runtime needs it.

- **Model sidecars as a separate "sidecar" archetype with 15%
  weight** (initial proposal during the design-review thread).
  Rejected because Kubernetes sidecars are containers within
  a Pod, not standalone Pods. They share lifecycle, namespace,
  and resource accounting. Modelling them as separate Pods
  would double-count fleet body and produce Pod-level resource
  numbers that don't match real cluster topologies. Folded
  into per-Pod resource overhead instead.

- **Bake `allowPartial` into archetype runtime semantics now**
  (instead of as forward-compat documentation). Rejected for
  the same reason as the proto field option: the runtime
  doesn't have an `AllOrNothing` mode today (per [ADR-0029],
  v1 is incremental-only). Once a future ADR adds the gang
  semantics, the `allowPartial` flag is what drives the
  classifier.

[ADR-0015]: 0015-realistic-archetype-improvements.md
[ADR-0024]: 0024-co-location-via-podaffinity.md
[ADR-0026]: 0026-scaletest-models-speculative-tier.md
[ADR-0027]: 0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0028]: 0028-cycle-p99-is-regime-parametric.md
[ADR-0029]: 0029-phase1-omega-style-occ.md
[fleet-scale-kubernetes.md]: ../papers/fleet-scale-kubernetes.md
