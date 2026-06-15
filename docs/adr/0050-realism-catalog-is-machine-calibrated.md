# ADR-0050: the realism catalog is calibrated to a realistic machine fleet, via per-archetype node-packing density

## Status

Accepted, 2026-06-13 — author decision in design dialogue. Amends
M66.2's "GPU density = 1" and ADR-0044's `PodsPerMachine`. Harness /
realism scope (no engine change). Implementation is the first step of
M78 (the cloud realism baseline); the coverage catalog
`realistic-dev` (dev-50) is unaffected.

## Context

The cloud realism catalog (`realistic.yaml`) was calibrated as a
realistic **pod-count** distribution (the 2026-05-17 industry pass:
~70% tiny-stateless, ~7% "interesting" GPU/stateful). The author's
intent for M78 is a realistic **machine** fleet to baseline against,
on the reasonable hope that a realistic pod mix would yield one.

It does not, and cannot, with the existing model. Worked through:
the load-driver draws workload *objects* by `weight`, so realized
pod-share ∝ `weight × E[replicas]`; ADR-0044 then sizes machines as
`podShare ÷ podsPerMachine`, where `podsPerMachine` is the global
density (100) for cpu/mem shapes but **1 for any GPU shape**. The
emergent machine mix is **~92% GPU** (gpu-training-large alone ~62%).
Fixing only the object-vs-pod draw moves it to ~88% — barely.

The reason is physical, not a bug in the draw: **for a whole-machine
workload, pod-share *is* machine-share.** A 160-node training gang
reads as "~0.1% of pods" but is literally ~3% of a 5,000-machine
fleet. Commodity pods pack ~100/machine; whole-machine GPU packs 1.
That 100× spread means pod-realism and machine-realism diverge, and
no reweighting of a pod distribution reconciles them. A realistic pod
mix with ~7% GPU pods mechanically *implies* an ~90% GPU machine
fleet — which no production fleet resembles, failing ADR-0043's own
test.

## Decision

1. **The realism catalog is calibrated to a realistic machine fleet**
   (BigFleet allocates machines; M78's SLOs are machine-allocation
   SLOs). The pod-count distribution becomes a *derived* property, not
   the calibration target. Target machine mix (the author's strawman):

   | tier | ~% machines | calibrated by |
   |---|---|---|
   | general compute (tiny/cpu-service/cpu-batch/critical) | ~82% | pod-share within the tier |
   | gang DBs (memory-cache/stateful-db) | ~3% | pod-share (cpu/mem, density-packed) |
   | GPU inference | ~5% | pod-share, **once densified** (below) |
   | GPU training small+medium | ~7% | machine-share (whole-machine) |
   | GPU training large (foundation) | ~3% | machine-share (whole-machine) |

   ⇒ ~15% GPU, already generous (many real fleets are <5%). Weights
   are back-solved: `weight ∝ machineShare × podsPerMachine / E[replicas]`.

2. **Per-archetype node-packing density replaces the GPU=1 special
   case.** M66.2 over-corrected: the bug was scaling GPU by the *cpu*
   density (100 → phantom 800-GPU nodes), and it was patched to "never
   scale GPU." The correct model is that every archetype has a
   `podsPerNode` = how many of its pods a real node of its class
   holds, and the seed machine = `pod resources × podsPerNode` for
   **all** resources including GPU:
   - cpu/mem archetypes: `podsPerNode` = global density (100).
   - **GPU inference: 8** — gpu:1 pods packed onto an 8-GPU node
     (MIG/time-slice/multi-GPU box). Node = gpu:8, cpu:64, mem:256.
     This is the one tier where pod-realism and machine-realism
     reconcile: realistic inference pod-share → 1/8 the machines.
   - GPU training (small/medium/large): **1** — gpu:8 pods take a
     whole 8-GPU node. Node = the pod, 1 pod/machine. Genuinely
     whole-machine; pod-share ≡ machine-share, no reconciliation
     possible, so calibrated in machine terms.

   `PodsPerMachine`/`MachinesForPods`/`scaleResourceMap` all read this
   per-archetype factor; the global `seedDensityMultiplier` becomes
   the default for archetypes that don't set one.

## Consequences

- The realism baseline (M78 uber-5k) finally measures a realistic
  mixed fleet, not a 92%-GPU one — its cycle/rollup/bind SLOs become
  representative rather than GPU-gang-dominated.
- **gpu-training-large is the lumpy term.** At ~3% machine-share each
  gang is 64–256 machines = ~1–2 concurrent in a 5,000-machine fleet,
  so a probabilistic weighted draw gives it ±100% variance (0, 1, or
  2 gangs) and a bimodal baseline. **Open follow-up:** likely move
  large foundation-training out of the *steady* baseline and into a
  burst/event scenario (the load-driver already has burst events,
  ADR-0015 §3); the steady baseline keeps general + DBs + inference +
  small/medium training. Implemented first with large included at low
  weight; the sim measures the actual variance and we decide.
  **DONE (#327):** gpu-training-large is now `burstOnly` in
  realistic.yaml — a new archetype flag that excludes it from the steady
  draw (`NewPicker`), the steady seed (`podShare`/`machineShares`/
  `MachineAllocation`/`MachinesForPods`), and its gang floor
  (`gangFloor`), while keeping its full definition so a burst event can
  reference it by name. weight:0 alone was insufficient: the per-gang
  floor (max(groupSizeRange) × zones) is applied regardless of weight, so
  a weight-0 gang would still seed a whole zone-floor of Configured
  machines the steady demand never asks for (a seed↔demand mismatch →
  Phase 3 reclaim every cycle). Foundation training is now injected by a
  burst event in the 5k.yaml realism profile (`loadProfile.bursts`, one
  64–256-node gang mid-soak, live-filled from the Speculative pool). The
  steady GPU machine-share fell ~15%→~12.4%, still inside the realistic
  band. The load-driver's burst path was taught to honour
  `bursts[].archetype` (it previously drew from the steady picker), and
  the V2 profile path was given a `loadProfile.bursts` field (the chart
  toYaml's it through to the load-driver) so the burst is not silently
  dropped.
- Within the cpu tier, pod-share realism is preserved (the 70%-tiny
  shape) — that part of the author's hope holds, because those
  archetypes are a small, density-packed machine-share regardless.
- `realistic-dev` (dev-50 coverage catalog) is untouched: dev-50 wants
  every path drawn every run; skew is a feature there.
- M66.2's `scaleResourceMap` "extended resources are physical device
  counts, never scaled" comment is replaced by the node-packing model.
  The GPU-density contradiction it fixed does not return: GPU scales by
  1 or 8 (its real node packing), never by 100.
