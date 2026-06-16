#!/usr/bin/env node
// sync-scaletest.mjs — read every test/scaletest/results/*/summary.json
// and emit two artifacts under /docs:
//
//   docs/scaletest-results.md   ← rendered page with table + embedded SVG
//   docs/scaletest-progress.svg ← log-scale bar chart of cycle p99 over time
//
// The Starlight build's existing sync-docs step then copies these into
// site/src/content/docs/. Keeping /docs as the canonical source means
// the GitHub README rendering and the website rendering stay aligned
// from one set of inputs.

import { promises as fs } from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..", "..");
const resultsDir = path.join(repoRoot, "test", "scaletest", "results");
const docsDir = path.join(repoRoot, "docs");

async function readRuns() {
  const entries = await fs.readdir(resultsDir, { withFileTypes: true });
  const runs = [];
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const summary = path.join(resultsDir, e.name, "summary.json");
    try {
      const text = await fs.readFile(summary, "utf8");
      if (!text.trim()) continue; // 0-byte / corrupt summary
      const j = JSON.parse(text);
      runs.push({
        dir: e.name,
        runId: j.runId,
        profile: j.profile,
        recordedPassed: !!j.passed, // what the runner said at the time
        cycleP99: numericOrNull(j.metrics?.shardCycleDurationP99Seconds),
        ackP99: numericOrNull(j.metrics?.operatorAckP99Seconds),
        rollupP99: numericOrNull(j.metrics?.operatorRollupP99Seconds),
        active: numericOrNull(j.metrics?.loadgenCRsActive),
        totalCRs: j.scale?.totalCrs ?? null,
        // Multi-shard / inventory totals (M12 onwards). Pre-M12
        // summaries don't carry these; default shardReplicas to 1
        // so historical rows render unchanged.
        shardReplicas: j.scale?.shardReplicas ?? 1,
        aggregateInventory: j.scale?.aggregateInventory ?? null,
      });
    } catch {
      // dir without a parseable summary.json — skip silently.
    }
  }
  for (const r of runs) {
    r.targetMet = sustainedLoadMet(r);
    r.passed = effectivePassed(r);
  }
  // Chronological order by directory prefix (UTC timestamp).
  runs.sort((a, b) => a.dir.localeCompare(b.dir));
  return runs;
}

// numericOrNull collapses the runner's -1 sentinel for "query failed"
// and any non-finite junk into null so the page never claims a run
// hit some impossible value.
function numericOrNull(v) {
  if (typeof v !== "number") return null;
  if (!Number.isFinite(v)) return null;
  if (v < 0) return null;
  return v;
}

// sustainedLoadMet reports whether the run actually held target load
// throughout the soak. A historical run with passed:true but active <<
// target was an under-loaded measurement — the SLO numbers don't apply
// to a shard that wasn't being driven. Returns null when we can't
// tell (older summaries without loadgenCRsActive); the caller treats
// null as "unknown, don't claim valid."
function sustainedLoadMet(r) {
  if (typeof r.active !== "number" || typeof r.totalCRs !== "number" || r.totalCRs <= 0) {
    return null;
  }
  return r.active >= 0.999 * r.totalCRs;
}

// effectivePassed re-derives pass/fail under the *current* runner SLO
// definition, regardless of what the original summary.json recorded.
// Older runs were graded by an earlier runner that didn't gate on
// sustained load, so several have passed:true despite measuring
// against a 30%-loaded shard. The page uses this for chart colour and
// the outcome column so readers see the correct call.
function effectivePassed(r) {
  if (r.targetMet === false) return false;
  // SLO ceilings (kept in sync with test/scaletest/cmd/scaletest-runner/main.go pass()).
  if (r.cycleP99 != null && r.cycleP99 > 0.1) return false;
  if (r.rollupP99 != null && r.rollupP99 > 1.0) return false;
  if (r.ackP99 != null && r.ackP99 > 12.0) return false;
  return true;
}

// shortLabel: derive a compact milestone tag from the run dir suffix.
// Falls back to the trailing segment when no suffix is recognised.
function shortLabel(dir) {
  const m = dir.match(/^\d{8}-\d{6}-(.+)$/);
  if (!m) return dir;
  return m[1];
}

// Filter to the scaleway-500k progression — that's the line everyone
// cares about (cumulative cycle p99 reduction across M11.x). Other
// runs (dev-5k, scaleway-50k baselines) live in the table only.
function progressionRuns(runs) {
  return runs.filter(
    (r) => r.profile === "scaleway-500k" && typeof r.cycleP99 === "number",
  );
}

function fmtSeconds(s) {
  if (s == null) return "—";
  if (s < 1) return `${Math.round(s * 1000)} ms`;
  return `${s.toFixed(2)} s`;
}

// Produce a log-scale bar chart of cycle p99 across the 500k run
// progression. SVG is hand-rolled (no charting deps) so the output is
// stable, readable in PRs, and fast to ship.
function renderSvg(progression) {
  const W = 900;
  const H = 380;
  const margin = { top: 40, right: 30, bottom: 90, left: 70 };
  const innerW = W - margin.left - margin.right;
  const innerH = H - margin.top - margin.bottom;

  if (progression.length === 0) {
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}"><text x="20" y="40">no scaleway-500k runs yet</text></svg>`;
  }

  // Log scale: clamp values to [0.01, 10] for the axis range, which
  // covers the M11 trajectory comfortably.
  const yMin = 0.01;
  const yMax = 10;
  const logMin = Math.log10(yMin);
  const logMax = Math.log10(yMax);
  const yScale = (v) => {
    const lv = Math.log10(Math.max(v, yMin));
    return innerH - ((lv - logMin) / (logMax - logMin)) * innerH;
  };

  const slotW = innerW / progression.length;
  const barW = slotW * 0.7;

  const sloMs = 100; // 100ms cycle SLO

  let bars = "";
  let labels = "";
  for (let i = 0; i < progression.length; i++) {
    const r = progression[i];
    const x = i * slotW + (slotW - barW) / 2;
    const y = yScale(r.cycleP99);
    const h = innerH - y;
    const fill = r.passed ? "#2ecc71" : "#e74c3c";
    bars += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${h.toFixed(1)}" fill="${fill}" rx="2" />\n`;
    bars += `<text x="${(x + barW / 2).toFixed(1)}" y="${(y - 6).toFixed(1)}" text-anchor="middle" font-family="ui-sans-serif, system-ui" font-size="11" fill="#333">${fmtSeconds(r.cycleP99)}</text>\n`;
    const lbl = shortLabel(r.dir);
    labels += `<text x="${(x + barW / 2).toFixed(1)}" y="${(innerH + 12).toFixed(1)}" text-anchor="end" font-family="ui-sans-serif, system-ui" font-size="11" fill="#555" transform="rotate(-30 ${(x + barW / 2).toFixed(1)} ${(innerH + 12).toFixed(1)})">${lbl}</text>\n`;
  }

  // Y-axis tick marks at 10 ms, 100 ms, 1 s, 10 s.
  let yticks = "";
  for (const v of [0.01, 0.1, 1, 10]) {
    const y = yScale(v);
    yticks += `<line x1="0" y1="${y.toFixed(1)}" x2="${innerW}" y2="${y.toFixed(1)}" stroke="#eee" stroke-dasharray="2 2" />\n`;
    yticks += `<text x="-10" y="${(y + 4).toFixed(1)}" text-anchor="end" font-family="ui-sans-serif, system-ui" font-size="11" fill="#555">${fmtSeconds(v)}</text>\n`;
  }
  // SLO line at 100ms.
  const sloY = yScale(sloMs / 1000);
  yticks += `<line x1="0" y1="${sloY.toFixed(1)}" x2="${innerW}" y2="${sloY.toFixed(1)}" stroke="#3498db" stroke-dasharray="6 4" />\n`;
  yticks += `<text x="${innerW - 4}" y="${(sloY - 6).toFixed(1)}" text-anchor="end" font-family="ui-sans-serif, system-ui" font-size="11" fill="#3498db">100 ms SLO</text>\n`;

  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${W} ${H}" role="img" aria-label="scaleway-500k cycle p99 across milestones">
  <text x="${W / 2}" y="22" text-anchor="middle" font-family="ui-sans-serif, system-ui" font-size="14" font-weight="600" fill="#222">scaleway-500k shard cycle p99 across milestones (log scale)</text>
  <g transform="translate(${margin.left},${margin.top})">
    ${yticks}
    ${bars}
    ${labels}
  </g>
</svg>
`;
}

function renderMarkdown(runs, progression) {
  const first = progression[0];
  // Headline is the most recent run that genuinely passes today's SLO
  // (target load sustained + cycle/rollup/ack within ceilings), not
  // whatever was last in the directory. Older "passed: true" runs that
  // didn't hold target load are intentionally excluded — they were
  // measuring against an under-loaded shard and the SLO numbers from
  // them don't say anything about behaviour at the actual benchmark.
  const validPasses = progression.filter((r) => r.passed);
  const headline = validPasses[validPasses.length - 1] ?? null;

  let header = `# Scale-test results

Each run is one full pass through the scaletest harness: chart install, ramp to steady state, soak, prometheus snapshot, summary. Runs live in [\`test/scaletest/results/\`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results) on GitHub; this page is generated from each run's \`summary.json\` and refreshes whenever the site builds.

Two regimes, two grading rules. The **aggregated-catalog ladder** below is graded against the canonical bar — sustained active CRs ≥ 99.9 % of target, cycle p99 ≤ 100 ms, rollup p99 ≤ 1 s, ack p99 ≤ 12 s. Older runs recorded as \`passed: true\` by an earlier runner without a sustained-load gate appear as ✗ when they didn't hold target load. The **realistic-catalog ladder** (uber-*) sits in a separate section below and is graded against the regime-aware envelopes defined in [ADR-0028](./adr/0028-cycle-p99-is-regime-parametric.md): per-Need cost ≤ 200 µs (constant), with cycle and ramp envelopes scaling with NeedsTable cardinality.

`;
  if (first) {
    header += `## Per-shard 500K optimisation trajectory

The paper sets 500K machines as the per-shard cycle ceiling (Phase 3 walks the inventory each cycle), so most BigFleet optimisation work happens against the scaleway-500k profile: a single shard, 50 simulated clusters, 50 000 demand CRs, 500 000 pre-seeded inventory machines on Scaleway Kapsule (PRO2-M, fr-par / nl-ams). Each milestone landed a real shard or harness change; the chart below tracks shard cycle p99 across those runs. Multi-shard tests (scaleway-1m, scaleway-5m) build on the per-shard ceiling validated here.

`;
    if (headline) {
      const reduction = (((first.cycleP99 - headline.cycleP99) / first.cycleP99) * 100).toFixed(1);
      header += `**${fmtSeconds(first.cycleP99)} → ${fmtSeconds(headline.cycleP99)}** (${reduction} % reduction). The most recent single-shard run that meets the SLO at full sustained load is [\`${shortLabel(headline.dir)}\`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/${headline.dir}).

`;
    } else {
      header += `Headline numbers omitted — there is no single-shard 500K run that currently meets the SLO at full sustained load.

`;
    }
    header += `![per-shard 500K cycle p99 across milestones](./scaletest-progress.svg)

The dashed blue line is the 100 ms cycle SLO. Bars are coloured green only when the run held target load *and* hit every SLO ceiling.

`;
  }

  let table = `## All runs

The rundir name encodes the fleet size tested (scaleway-500k = single-shard 500K, scaleway-1m = 2 × 500K, scaleway-5m = 10 × 500K — the paper's per-shard ceiling is 500K, so multi-shard runs are aggregate-named). "load" is observed sustained CRs over the target. A run only passes if it held target load *and* hit every SLO.

| run | cycle p99 | ack p99 | rollup p99 | load | pass |
|---|---|---|---|---|---|
`;
  for (const r of runs) {
    const tag = shortLabel(r.dir);
    const loadCol =
      r.active != null && r.totalCRs != null
        ? `${r.active} / ${r.totalCRs}`
        : r.active != null
          ? `${r.active}`
          : "—";
    const pass = r.passed ? "✓" : "✗";
    table += `| [\`${tag}\`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/${r.dir}) | ${fmtSeconds(r.cycleP99)} | ${fmtSeconds(r.ackP99)} | ${fmtSeconds(r.rollupP99)} | ${loadCol} | ${pass} |\n`;
  }

  return header + table + realisticSection() + `\n*Generated from \`test/scaletest/results/*/summary.json\` by \`site/scripts/sync-scaletest.mjs\`. Outcomes recomputed under the current SLO bar.*\n`;
}

// Realistic-regime ladder rows. These runs exercise the full
// realistic.yaml catalog on Uber-donated compute and are NOT checked into
// test/scaletest/results/ — only sanitised aggregate values appear here;
// raw run data stays internal. Graded against the reframed steady-state
// SLOs in ADR-0054 / docs/slos.md: under a default uncapped kube-scheduler
// we gate BigFleet's own capacity-delivery hops, not the end-to-end
// pod-bind tail (scheduler retry/backoff + reprovision back-edge —
// informational). As new rungs land (uber-50k → uber-500k), add a row with
// the inner agent's sanitised values.
const realisticRuns = [
  {
    profile: "uber-5k",
    commit: "cee793e",
    configureP99: 0.31,
    bootstrapSuccess: 1.0,
    nodeStateP99: 1.024,
    rollupP99: 0.65,
    cycleP99: 0.255,
    ackP99: 0.64,
    shortfalls: 0,
    bindP50: 1.6,
    passed: true,
  },
  {
    // Next rung. Pending a fresh run on the ADR-0054 methodology; the
    // earlier 50k attempt predates ADR-0050/0054 and ran on an
    // incomparable compressed setup, so it is not shown under the
    // reframed gates.
    profile: "uber-50k",
    pending: true,
  },
];

function realisticSection() {
  let s = `\n## Realistic-regime ladder (uber-*)

These runs exercise the full \`realistic.yaml\` archetype catalog (gpu-training, memory-db, co-location gangs) on Uber-donated compute, graded against the reframed steady-state SLOs in [ADR-0054] / [SLOs](./slos.md). Under a **default, uncapped kube-scheduler** we gate BigFleet's own capacity-delivery hops; the end-to-end pod-bind tail is dominated by the scheduler's retry/backoff and the reprovision back-edge, so it is reported informational, not gated.

Gates: configure-phase ≤ 15 s · bootstrap success ≥ 0.99 · node-state ≤ 1.5 s · rollup ≤ 1 s · cycle ≤ 5 s · ack ≤ 12 s · shortfalls = 0 · bind p50 ≤ 10 s.

| profile | commit | configure p99 | bootstrap | node-state p99 | rollup p99 | cycle p99 | ack p99 | shortfalls | bind p50 | pass |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
`;
  for (const r of realisticRuns) {
    if (r.pending) {
      s += `| \`${r.profile}\` | — | — | — | — | — | — | — | — | — | pending |\n`;
      continue;
    }
    s += `| \`${r.profile}\` | \`${r.commit}\` | ${fmtSeconds(r.configureP99)} | ${r.bootstrapSuccess.toFixed(2)} | ${fmtSeconds(r.nodeStateP99)} | ${fmtSeconds(r.rollupP99)} | ${fmtSeconds(r.cycleP99)} | ${fmtSeconds(r.ackP99)} | ${r.shortfalls} | ${fmtSeconds(r.bindP50)} | ${r.passed ? "✓" : "✗"} |\n`;
  }
  s += `
End-to-end pod-bind p99 is informational only — dominated by the uncapped scheduler's retry/backoff and the reprovision back-edge; it varies widely run-to-run (tens to hundreds of seconds) and is not a BigFleet SLO.

[ADR-0054]: ./adr/0054-steady-bind-slo-reframe-for-uncapped-scheduler.md
`;
  return s;
}

async function main() {
  // test/scaletest/results/ is gitignored (raw per-run data, dev-box-local).
  // On a fresh clone / CI build host (e.g. Cloudflare Pages) it is absent, so
  // fall back to the committed docs/scaletest-results.md + .svg snapshots
  // instead of hard-failing the whole site build with ENOENT.
  try {
    await fs.access(resultsDir);
  } catch {
    console.error(
      `scaletest-sync: ${resultsDir} not present — keeping the committed ` +
        `docs/scaletest-results.md and docs/scaletest-progress.svg as-is ` +
        `(skipping regeneration).`,
    );
    return;
  }

  const runs = await readRuns();
  const progression = progressionRuns(runs);

  await fs.mkdir(docsDir, { recursive: true });
  await fs.writeFile(path.join(docsDir, "scaletest-results.md"), renderMarkdown(runs, progression));
  await fs.writeFile(path.join(docsDir, "scaletest-progress.svg"), renderSvg(progression));

  console.error(`scaletest-sync: ${runs.length} runs (${progression.length} on the 500k progression)`);
}

main().catch((err) => {
  console.error("sync-scaletest failed:", err);
  process.exit(1);
});
