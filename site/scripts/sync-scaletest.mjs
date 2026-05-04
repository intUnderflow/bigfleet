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
      const j = JSON.parse(await fs.readFile(summary, "utf8"));
      runs.push({
        dir: e.name,
        runId: j.runId,
        profile: j.profile,
        passed: !!j.passed,
        cycleP99: j.metrics?.shardCycleDurationP99Seconds ?? null,
        ackP99: j.metrics?.operatorAckP99Seconds ?? null,
        rollupP99: j.metrics?.operatorRollupP99Seconds ?? null,
        active: j.metrics?.loadgenCRsActive ?? null,
        totalCRs: j.scale?.totalCrs ?? null,
      });
    } catch {
      // dir without a summary.json (interrupted run, prom snapshot only).
      // Skip silently.
    }
  }
  // Chronological order by directory prefix (UTC timestamp).
  runs.sort((a, b) => a.dir.localeCompare(b.dir));
  return runs;
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
  const last = progression[progression.length - 1];
  let header = `# Scale-test results

Each run is one full pass through the scaletest harness: chart install, ramp to steady state, soak, prometheus snapshot, summary. Runs live in [\`test/scaletest/results/\`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results) on GitHub; this page is generated from each run's \`summary.json\` and refreshes whenever the site builds.

`;
  if (first && last) {
    header += `## Cumulative trajectory at 500K

The scaleway-500k profile is the production-shape benchmark: 50 simulated clusters, 50 000 demand CRs, 500 000 pre-seeded inventory machines on a 5-node Scaleway Kapsule (PRO2-M, nl-ams). Each milestone landed a real shard or harness change; the line below tracks shard cycle p99 over them.

**${fmtSeconds(first.cycleP99)} → ${fmtSeconds(last.cycleP99)}** (${(((first.cycleP99 - last.cycleP99) / first.cycleP99) * 100).toFixed(1)} % reduction across ${progression.length} runs).

![scaleway-500k cycle p99 across milestones](./scaletest-progress.svg)

The 100 ms SLO line is the runner's gate. The first passing run was \`${last.dir}\`.

`;
  }

  let table = `## All runs

| run | profile | cycle p99 | ack p99 | rollup p99 | active CRs | passed |
|---|---|---|---|---|---|---|
`;
  for (const r of runs) {
    const tag = shortLabel(r.dir);
    const pass = r.passed ? "✓" : "✗";
    table += `| [\`${tag}\`](https://github.com/intUnderflow/bigfleet/tree/main/test/scaletest/results/${r.dir}) | ${r.profile ?? "—"} | ${fmtSeconds(r.cycleP99)} | ${fmtSeconds(r.ackP99)} | ${fmtSeconds(r.rollupP99)} | ${r.active ?? "—"} | ${pass} |\n`;
  }

  return header + table + `\n*Generated from \`test/scaletest/results/*/summary.json\` by \`site/scripts/sync-scaletest.mjs\`.*\n`;
}

async function main() {
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
