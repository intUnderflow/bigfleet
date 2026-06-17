#!/usr/bin/env node
// sync-scaletest.mjs — generate docs/scaletest-results.md, the public
// scale-test results page (rendered on the website + GitHub).
//
// DATA-DRIVEN: the tables are built from the run folders committed under
// test/scaletest/results/. To publish a run, drop two files in its folder and
// `git add -f` them — no edits to this script:
//
//   test/scaletest/results/<run>/summary.json  — the sanitised run numbers
//                                                 (produced by the runner /
//                                                 bigfleet-uber publish-run.sh)
//   test/scaletest/results/<run>/page.json     — how it appears on the page:
//       { "section": "ladder" | "variant",
//         "displayName": "uber-5k",            // row label
//         "scale": "~5,000-pod fleet · 1 shard",
//         "commit": "cee793e",                  // ladder rungs
//         "shape": "one shard sustaining …",    // ladder rungs (scorecard caption)
//         "order": 1,                           // sort within its section
//         "note": "…" }                         // optional, not rendered
//
// page.json is also the opt-in marker: results/ is gitignored except for
// force-added published runs, but the local dev box additionally holds dozens
// of UNTRACKED dev/scaleway run dirs. Keying on page.json means those are
// invisible here, so the page renders identically whether generated locally,
// in CI, or in the Cloudflare Pages build (which only ever sees committed
// files). Run numbers and pass/fail are derived from summary.json against the
// gate set below — never hand-typed — so the page can't drift from the data.
//
// The only static inputs are the SLO bars (the spec, from docs/slos.md /
// ADR-0054) and the future-rung roadmap (intent — those rungs have no run yet).

import { promises as fs } from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..", "..");
const docsDir = path.join(repoRoot, "docs");
const resultsDir = path.join(repoRoot, "test", "scaletest", "results");
const GH = "https://github.com/intUnderflow/bigfleet/tree/main";

// ---------------------------------------------------------------------------
// Static spec: the gate set (BigFleet's own capacity-delivery hops).
// Keep in sync with docs/slos.md / ADR-0054. `metric` is the summary.json key;
// `ok` is the pass predicate (used to derive ✓/✗ and the rung verdict).
// ---------------------------------------------------------------------------
const GATES = [
  { name: "shortfalls",             metric: "shardShortfalls",                   gate: "= 0",     kind: "count", ok: (v) => v === 0,   breach: "demand left unmet — the one contract violation, no headroom by construction" },
  { name: "bootstrap success",      metric: "bootstrapSuccessRatio",             gate: "≥ 0.99",  kind: "ratio", ok: (v) => v >= 0.99, breach: "node materialisation is failing, not merely slow" },
  { name: "configure-phase p99",    metric: "shardConfigurePhaseP99Seconds",     gate: "≤ 15 s",  kind: "secs",  ok: (v) => v <= 15,   breach: "a machine is taking too long to become a configured node" },
  { name: "node-state-publish p99", metric: "operatorNodeStateUpdateP99Seconds", gate: "≤ 1.5 s", kind: "secs",  ok: (v) => v <= 1.5,  breach: "the operator is slow to publish the ready node back to the cluster" },
  { name: "roll-up p99",            metric: "operatorRollupP99Seconds",          gate: "≤ 1 s",   kind: "secs",  ok: (v) => v <= 1,    breach: "the operator is slow to report a cluster's demand" },
  { name: "shard cycle p99",        metric: "shardCycleDurationP99Seconds",      gate: "≤ 5 s",   kind: "secs",  ok: (v) => v <= 5,    breach: "the decision loop is falling behind demand" },
  { name: "ack p99",                metric: "operatorAckP99Seconds",             gate: "≤ 12 s",  kind: "secs",  ok: (v) => v <= 12,   breach: "capacity-request acknowledgement is backing up" },
  { name: "pod-bind p50",           metric: "endToEndPodBindP50Seconds",         gate: "≤ 10 s",  kind: "secs",  ok: (v) => v <= 10,   breach: "the common (median) bind path broke — a loose liveness floor" },
];

// Static roadmap: rungs that have no run folder yet (intent, not measured data).
// Once a rung is run, drop its summary.json + page.json{section:"ladder"} in a
// folder and it supersedes the roadmap entry of the same profile.
const ROADMAP = [
  { profile: "uber-50k", scale: "next rung", status: "next",
    note: "the next rung — held until a test fleet large enough to run it without host oversubscription is available, so it is measured on the same methodology rather than a compressed one. (Single-threaded Phase 1 cost grows with demand cardinality — see ADR-0028 — so a larger rung is also where parallel Phase 1 earns its place.)" },
  { profile: "uber-500k", scale: "planned", status: "planned" },
  { profile: "uber-1m",   scale: "planned", status: "planned" },
  { profile: "uber-5m",   scale: "planned", status: "planned" },
];

const STATUS = {
  passed:  "✅ passed",
  failed:  "❌ failed",
  next:    "⏳ next",
  planned: "▫️ planned",
};
const STATUS_EMOJI = { passed: "✅", failed: "❌", next: "⏳", planned: "▫️" };

// ---------------------------------------------------------------------------
// Read committed runs from results/. A run is any dir holding BOTH page.json
// and summary.json; everything else (including untracked dev-box junk dirs) is
// skipped. Returns [] if results/ is absent — caller keeps the committed page.
// ---------------------------------------------------------------------------
async function readRuns() {
  let entries;
  try {
    entries = await fs.readdir(resultsDir, { withFileTypes: true });
  } catch {
    return [];
  }
  const runs = [];
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const dir = path.join(resultsDir, e.name);
    let page, summary;
    try {
      page = JSON.parse(await fs.readFile(path.join(dir, "page.json"), "utf8"));
      summary = JSON.parse(await fs.readFile(path.join(dir, "summary.json"), "utf8"));
    } catch {
      continue; // not a published run (missing/invalid page.json or summary.json)
    }
    let csv = false;
    try {
      await fs.access(path.join(dir, "chain-numbers.csv"));
      csv = true;
    } catch {}
    runs.push({ dir: e.name, page, summary, csv });
  }
  return runs;
}

const byOrder = (a, b) => (a.page.order ?? 0) - (b.page.order ?? 0);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmtSeconds(s) {
  if (typeof s !== "number" || !Number.isFinite(s)) return "—";
  if (s < 1) return `${Math.round(s * 1000)} ms`;
  return `${s.toFixed(2)} s`;
}

// Render a gate metric value from a summary, in the gate's units.
function fmtGate(summary, g) {
  const v = summary.metrics?.[g.metric];
  if (typeof v !== "number" || !Number.isFinite(v)) return "—";
  if (g.kind === "count") return String(v);
  if (g.kind === "ratio") return v.toFixed(2);
  return fmtSeconds(v);
}

function gatePasses(summary, g) {
  const v = summary.metrics?.[g.metric];
  return typeof v === "number" && Number.isFinite(v) && g.ok(v);
}

function allGatesPass(summary) {
  return GATES.every((g) => gatePasses(summary, g));
}

function commas(n) {
  return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function loadStr(summary) {
  const a = summary.metrics?.loadgenCRsActive;
  const t = summary.scale?.totalCrs;
  if (typeof a === "number" && typeof t === "number") return `${commas(a)} / ${commas(t)}`;
  if (typeof a === "number") return commas(a);
  return "—";
}

function folderLink(dir) {
  return `[run folder ↗](${GH}/test/scaletest/results/${dir})`;
}

function capitalise(s) {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

function lead(ladderRuns) {
  const rungs = ladderRuns.map((r) => ({
    profile: r.page.displayName,
    status: allGatesPass(r.summary) ? "passed" : "failed",
  }));
  const strip = [...rungs, ...ROADMAP]
    .map((r) => `\`${r.profile}\` ${STATUS_EMOJI[r.status]}`)
    .join(" · ");
  return `# Scale-test results

BigFleet turns each cluster's capacity demand into provisioned, configured nodes through pluggable providers — it does **not** place pods ([what BigFleet is](./papers/bigfleet.md)). This page is the canonical record of how far that is proven, against the full \`realistic.yaml\` workload catalog (gpu-training, memory-db, co-location gangs) and a **real, default, _uncapped_ kube-scheduler**. BigFleet is graded only on the capacity-delivery hops it *owns* — never the cluster's scheduler — and is forbidden from reconfiguring that scheduler to make its own SLO pass ([what we gate](#what-we-gate-and-why-the-bar-is-honest)).

**Ladder:** ${strip}`;
}

function scorecard(canonical) {
  const { summary, page } = canonical;
  let s = `## Headline result — \`${page.displayName}\` (commit \`${page.commit}\`)

${capitalise(page.shape)} — every hop BigFleet owns inside SLO, **zero unmet demand**.

| gate | result | bar |
|---|---:|---:|
`;
  for (const g of GATES) {
    const mark = gatePasses(summary, g) ? "✓" : "✗";
    s += `| ${g.name} | **${fmtGate(summary, g)}** ${mark} | ${g.gate} |\n`;
  }
  s += `
> End-to-end pod-bind p99 is **not** gated and is large by design — it is dominated by the uncapped scheduler's retry/backoff and the reprovision back-edge, neither of which is BigFleet's deliverable. See [what we gate](#what-we-gate-and-why-the-bar-is-honest).
`;
  return s;
}

function whatWeGate() {
  const gated = GATES.map((g) => `- **${g.name}** ${g.gate} — breach means ${g.breach}.`).join("\n");
  return `## What we gate, and why the bar is honest

The principle ([ADR-0054](./adr/0054-steady-bind-slo-reframe-for-uncapped-scheduler.md), [full justification in SLOs](./slos.md)): **gate BigFleet's deliverable, never an uncontrolled dependency.** The harness runs a real, *uncapped* kube-scheduler and a real provisioning back-edge; the latencies those impose are *reported*, never gated — and BigFleet may not cap the scheduler to make its own numbers pass (author decision). So the bar decomposes "demand observed → machine materialised → node published" into the per-hop bars BigFleet actually owns, measured at **steady state under churn** (not the cold-start ramp — ramp is capacity exploration, not pass/fail).

**Gated — BigFleet's own hops:**
${gated}

**Informational — reported, never gated:** end-to-end pod-bind p99 + raw-max, and fingerprint fan-out latency. The pod-bind tail runs to hundreds of seconds because a churn-reclaimed pod cannot re-bind until a replacement machine is provisioned (the reprovision back-edge) and because the uncapped scheduler backs off on retry — physics outside BigFleet's contract.

Two of the gates are anti-gaming guards: **shortfalls = 0** has no percentile headroom — no reshape makes unmet demand acceptable — and **bootstrap success** catches a materialisation-throughput collapse that latency-plus-shortfall gates alone could miss. The reframe strictly *increased* coverage (the node-state-publish hop was previously ungated).`;
}

function ladderSection(ladderRuns) {
  let s = `## The validated-scale ladder (uber-*)

The workload is the full \`realistic.yaml\` archetype catalog — gpu-training, memory-db, co-location gangs — calibrated to a realistic machine fleet (ADR-0050): the hard demand shape, not a toy. One rung is published; the larger rungs are sequential and gated on **test-fleet capacity, not on the engine** — what each rung costs to run, and why 500k/5m need dedicated infrastructure, is in [scale-test resource requirements](./scaletest-resource-requirements.md). Each rung's full numbers live in its run folder; the headline scorecard above carries uber-5k's gate values.

| rung | scale | status | data |
|---|---|:--|:--|
`;
  for (const r of ladderRuns) {
    const status = allGatesPass(r.summary) ? "passed" : "failed";
    s += `| \`${r.page.displayName}\` | ${r.page.scale} | ${STATUS[status]} | ${folderLink(r.dir)} |\n`;
  }
  for (const r of ROADMAP) {
    s += `| \`${r.profile}\` | ${r.scale} | ${STATUS[r.status]} | — |\n`;
  }
  const next = ROADMAP.find((r) => r.status === "next");
  if (next?.note) {
    s += `\n**\`${next.profile}\`** — ${next.note}\n`;
  }
  s += `\n_${STATUS.next} and ${STATUS.planned} are sequencing states, not failures — the ladder is in progress._\n`;
  return s;
}

function reproduce() {
  return `## Reproduce & trust

The profiles and substrates are committed and substrate-agnostic ([ADR-0034](./adr/0034-scaletest-byo-substrate.md)) — bring your own substrate and run the same gate:

\`\`\`
make scaletest PROFILE=test/scaletest/profiles/5k.yaml SUBSTRATE=test/scaletest/substrates/example-fat-host.yaml
\`\`\`

\`uber-5k\` is the published *label* for the \`5k.yaml\` profile run on Uber-donated compute — there is no \`uber-5k.yaml\` to hunt for. Example substrates ship for a laptop and for fatter hosts: [\`example-kind-laptop\`](${GH}/test/scaletest/substrates/example-kind-laptop.yaml), [\`example-mid-host\`](${GH}/test/scaletest/substrates/example-mid-host.yaml), [\`example-fat-host\`](${GH}/test/scaletest/substrates/example-fat-host.yaml).

**Recreate the dashboard.** The Grafana dashboard ships in the repo ([\`dashboards/scaletest.json\`](${GH}/test/scaletest/chart/dashboards/scaletest.json)); point it at any Prometheus carrying BigFleet's metrics. Published canonical runs also include a Prometheus snapshot you can load to replay the run's status over time (added per run as it is published).

**Per-run artefacts.** Raw run data (logs, full Prometheus) is dev-box-local and not committed; this page is the canonical record. The sanitised numeric results that *are* committed — each run's \`summary.json\` plus a \`chain-numbers.csv\` time-series — live in that run's folder, linked from the ladder and the configuration-variant table.`;
}

function footnoteAndAppendix(variantRuns) {
  let s = `## How a result is graded

Every gate is measured at **steady state under sustained churn**, never during the cold-start ramp ([ADR-0035](./adr/0035-scaletest-slos-at-steady-state.md)): ramp is capacity exploration, not a pass/fail signal. Per-machine and per-frame bars are held identical across the whole ladder; only genuinely size-scaling quantities get size-dependent thresholds ([ADR-0028](./adr/0028-cycle-p99-is-regime-parametric.md)). Pass/fail on this page is computed from each run's committed \`summary.json\` against the current gate set, so a run's own recorded verdict may differ (e.g. against a since-retired saturated bind-latency metric). Separately, the shard's per-cycle decision cost was driven from seconds to tens of milliseconds over the engine-optimisation milestones — the headroom the cycle gate now runs against.`;

  if (variantRuns.length > 0) {
    s += `

<details>
<summary><strong>uber-5k configuration runs</strong> (transparency — pre-reframe metric set)</summary>

Earlier \`uber-5k\` runs across host/cluster configurations, each with its sanitised numbers committed in the linked run folder. These predate the ADR-0054 reframe — older metric set, and the bind-latency p99 they recorded was the since-retired saturated metric — so the folder's numbers are on the metrics they captured, not the current gate set, and these are configuration variants, **not** ladder rungs.

| run | load | pass | data |
|---|---|:--:|:--|
`;
    for (const r of variantRuns) {
      let data = folderLink(r.dir);
      if (r.csv) data += ` · [csv ↗](${GH}/test/scaletest/results/${r.dir}/chain-numbers.csv)`;
      s += `| \`${r.page.displayName}\` | ${loadStr(r.summary)} | ${r.summary.passed ? "✓" : "✗"} | ${data} |\n`;
    }
    s += `
</details>`;
  }
  return s;
}

function renderPage({ ladderRuns, variantRuns, canonical }) {
  return [
    lead(ladderRuns),
    scorecard(canonical),
    whatWeGate(),
    ladderSection(ladderRuns),
    reproduce(),
    footnoteAndAppendix(variantRuns),
    `\n*Generated from \`test/scaletest/results/*/{summary,page}.json\` by \`site/scripts/sync-scaletest.mjs\`.*\n`,
  ].join("\n\n");
}

async function main() {
  const runs = await readRuns();
  const ladderRuns = runs.filter((r) => r.page.section === "ladder").sort(byOrder);
  const variantRuns = runs.filter((r) => r.page.section === "variant").sort(byOrder);

  if (ladderRuns.length === 0) {
    // No published ladder run found (e.g. results/ absent on a stripped build
    // host). Keep the committed docs/scaletest-results.md rather than emit a
    // page with no headline.
    console.error(
      "scaletest-sync: no published ladder runs under test/scaletest/results/ " +
        "(a run needs page.json{section:ladder} + summary.json) — keeping the " +
        "committed docs/scaletest-results.md as-is.",
    );
    return;
  }

  // Headline = the first passing ladder rung, else the first rung.
  const canonical = ladderRuns.find((r) => allGatesPass(r.summary)) ?? ladderRuns[0];

  await fs.mkdir(docsDir, { recursive: true });
  await fs.writeFile(
    path.join(docsDir, "scaletest-results.md"),
    renderPage({ ladderRuns, variantRuns, canonical }),
  );
  console.error(
    `scaletest-sync: wrote docs/scaletest-results.md (${ladderRuns.length} ladder, ${variantRuns.length} variant runs)`,
  );
}

main().catch((err) => {
  console.error("sync-scaletest failed:", err);
  process.exit(1);
});
