# ADR-0043: harness-observed triggers get a demand-realism check before mechanism ships

## Status

Accepted, 2026-06-11.

## Context

The complexity audit (docs/complexity-audit-2026-06.md) traced the
single largest pool of unforced engine complexity — the ADR-0042
layer: rule 5, the parking ledger and its tunables, the re-probe
path, the SameSatisfiable plumbing — to one catalog archetype.
`gpu-training-large` demanded 64–256 *rack-coherent* whole-GPU nodes
against racks that physically hold ~50; the resulting ~190
perpetually-unsatisfiable gangs were faithfully observed in cloud
runs, diagnosed across four briefs, and answered with engine
mechanism. Every step was locally rigorous. The unasked question was
the first one: **is the demand that triggers this something a
production fleet would ever emit?** Real systems place gangs that
size at zone/pod scope; the trigger was a harness artifact, and the
mechanism built against it survives only until the catalog is
corrected (M66.2 / M66.4).

This was not the first instance — the May dev-50 era similarly
chased a plateau whose root included a seed/demand shape mismatch —
but it was the most expensive, because the mechanism it produced
carries permanent state, tunables, and a wire field.

## Decision

Any ADR whose motivating evidence is **harness-observed** (a
scaletest run, the closed-loop sim, a synthetic profile) must contain
a **Demand realism** section answering, before any mechanism is
designed:

1. What demand shape triggers the pathology, stated concretely
   (archetype, gang size, constraint scope, rate)?
2. Would a production fleet emit that shape? Cite either the papers,
   a real-world reference, or physical constraints (rack occupancy,
   device counts). "The catalog generates it" is not an answer.
3. If the shape is unrealistic: fix the harness first, re-measure,
   and only then decide whether residual (now realistic) evidence
   still warrants mechanism.

ADRs triggered by production incidents or paper requirements are
exempt — the realism of reality is not in question.

The section is a gate, not a formality: an ADR that cannot answer (2)
affirmatively does not proceed to mechanism. The reviewer (the author
for this repo) checks it the way "does this introduce a hot-path
coordinator dependency?" is checked for `pkg/shard` changes.

## Consequences

- The cost is one paragraph per ADR and occasionally a humbling
  re-measure; the benefit, measured retrospectively against ADR-0042,
  is several hundred engine LOC, two tunables, a wire field, and four
  cloud-run round-trips.
- The check protects mechanism work, not diagnosis: instrumenting and
  root-causing harness-observed pathologies (probes, diagnostic
  briefs) needs no realism gate — only the decision to change engine
  behaviour does.
- CLAUDE.md's working-discipline list gains the rule alongside the
  static-stability check.
