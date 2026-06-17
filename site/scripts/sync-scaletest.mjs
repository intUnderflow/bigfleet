#!/usr/bin/env node
// sync-scaletest.mjs — generate docs/scaletest-results.md, the public
// scale-test results page (rendered on the website + GitHub).
//
// Array-driven + hand-authored: the page is built entirely from the data and
// prose in THIS file, so it is deterministic and has NO dependency on the
// gitignored test/scaletest/results/ tree. (Earlier versions read each run's
// summary.json and rendered an SVG cycle-p99 trajectory; that scaleway
// dev-iteration content was retired — the live story is the realistic-catalog
// ladder graded on the ADR-0054 steady-state SLOs.)
//
// To publish a new rung or update a number: edit the data tables below and
// re-run `node site/scripts/sync-scaletest.mjs`. Keep the values in sync with
// docs/slos.md (the bar) and the run's sanitised verdict.

import { promises as fs } from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..", "..");
const docsDir = path.join(repoRoot, "docs");
const GH = "https://github.com/intUnderflow/bigfleet/tree/main";

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

// The gate set — BigFleet's own capacity-delivery hops (ADR-0054 / docs/slos.md).
// Order is the decompose order: demand observed → machine materialised → node
// published, plus the throughput/coverage guards. Keep in sync with slos.md.
const GATES = [
  { key: "shortfalls",       name: "shortfalls",            gate: "= 0",      kind: "count", breach: "demand left unmet — the one contract violation, no headroom by construction" },
  { key: "bootstrapSuccess", name: "bootstrap success",     gate: "≥ 0.99",   kind: "ratio", breach: "node materialisation is failing, not merely slow" },
  { key: "configureP99",     name: "configure-phase p99",   gate: "≤ 15 s",   kind: "secs",  breach: "a machine is taking too long to become a configured node" },
  { key: "nodeStateP99",     name: "node-state-publish p99",gate: "≤ 1.5 s",  kind: "secs",  breach: "the operator is slow to publish the ready node back to the cluster" },
  { key: "rollupP99",        name: "roll-up p99",           gate: "≤ 1 s",    kind: "secs",  breach: "the operator is slow to report a cluster's demand" },
  { key: "cycleP99",         name: "shard cycle p99",       gate: "≤ 5 s",    kind: "secs",  breach: "the decision loop is falling behind demand" },
  { key: "ackP99",           name: "ack p99",               gate: "≤ 12 s",   kind: "secs",  breach: "capacity-request acknowledgement is backing up" },
  { key: "bindP50",          name: "pod-bind p50",          gate: "≤ 10 s",   kind: "secs",  breach: "the common (median) bind path broke — a loose liveness floor" },
];

// The validated-scale ladder. status ∈ passed | next | planned.
// Numbers for the passed rung are the run's sanitised verdict values (kept in
// sync with docs/slos.md), NOT read from a summary.json.
const ladder = [
  {
    profile: "uber-5k", status: "passed", commit: "cee793e",
    scale: "~5,000-pod realistic-catalog fleet · 1 shard",
    // dataDir: published run folder for the canonical cee793e artifact. null
    // until the sanitised summary.json + chain-numbers.csv + prom snapshot land
    // under test/scaletest/results/ — until then the row shows "publishing" and
    // the headline scorecard above carries the gate values.
    dataDir: null,
    shortfalls: 0, bootstrapSuccess: 1.0, configureP99: 0.31, nodeStateP99: 1.024,
    rollupP99: 0.65, cycleP99: 0.255, ackP99: 0.64, bindP50: 1.6,
    shape: "one shard sustaining the full realistic-catalog demand of a simulated fleet (~5,000 pods of demand) through a real, default, uncapped kube-scheduler",
  },
  {
    profile: "uber-50k", status: "next", scale: "next rung",
    note: "the next rung — held until a test fleet large enough to run it without host oversubscription is available, so it is measured on the same methodology rather than a compressed one. (Single-threaded Phase 1 cost grows with demand cardinality — see ADR-0028 — so a larger rung is also where parallel Phase 1 earns its place.)",
  },
  { profile: "uber-500k", status: "planned", scale: "planned" },
  { profile: "uber-1m",   status: "planned", scale: "planned" },
  { profile: "uber-5m",   status: "planned", scale: "planned" },
];

// Configuration-variant runs whose sanitised numbers ARE committed under
// test/scaletest/results/. Pre-ADR-0054 metric set (no node-state/bootstrap
// gates; the bind-latency p99 they recorded was the since-retired saturated
// metric) — kept for transparency, shown on the metrics they captured.
// csv: a sanitised chain-numbers.csv time-series is committed in the dir.
const configRuns = [
  { label: "uber-5k (single-host)", dir: "2026-05-13-uber-5k",            cycleP99: 0.127, rollupP99: 0.329, ackP99: 0.583, configureP99: 0.019, shortfalls: 0, load: "5,831 / 500,000",     passed: false, csv: true },
  { label: "uber-5k-wide",          dir: "2026-05-13-uber-5k-wide",       cycleP99: 1.016, rollupP99: 1.205, ackP99: 0.640, configureP99: 0.156, shortfalls: 0, load: "499,993 / 500,000",   passed: false, csv: true },
  { label: "uber-5k-2host",         dir: "2026-05-16-uber-5k-2host-20x25k",cycleP99: 1.019, rollupP99: 0.497, ackP99: 0.296, configureP99: 0.482, shortfalls: 1, load: "249,995 / 250,000",   passed: true,  csv: false },
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmtSeconds(s) {
  if (s == null) return "—";
  if (s < 1) return `${Math.round(s * 1000)} ms`;
  return `${s.toFixed(2)} s`;
}

// Render a gate's value for a run, in the gate's units.
function fmtValue(run, g) {
  const v = run[g.key];
  if (v == null) return "—";
  if (g.kind === "count") return String(v);
  if (g.kind === "ratio") return v.toFixed(2);
  return fmtSeconds(v);
}

const STATUS = {
  passed:  "✅ passed",
  next:    "⏳ next",
  planned: "▫️ planned",
};

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

function lead() {
  const strip = ladder
    .map((r) => `\`${r.profile}\` ${r.status === "passed" ? "✅" : r.status === "next" ? "⏳" : "▫️"}`)
    .join(" · ");
  return `# Scale-test results

BigFleet turns each cluster's capacity demand into provisioned, configured nodes through pluggable providers — it does **not** place pods ([what BigFleet is](./papers/bigfleet.md)). This page is the canonical record of how far that is proven, against the full \`realistic.yaml\` workload catalog (gpu-training, memory-db, co-location gangs) and a **real, default, _uncapped_ kube-scheduler**. BigFleet is graded only on the capacity-delivery hops it *owns* — never the cluster's scheduler — and is forbidden from reconfiguring that scheduler to make its own SLO pass ([what we gate](#what-we-gate-and-why-the-bar-is-honest)).

**Ladder:** ${strip}`;
}

function scorecard() {
  const run = ladder.find((r) => r.status === "passed");
  let s = `## Headline result — \`${run.profile}\` (commit \`${run.commit}\`)

${capitalise(run.shape)} — every hop BigFleet owns inside SLO, **zero unmet demand**.

| gate | result | bar |
|---|---:|---:|
`;
  for (const g of GATES) {
    s += `| ${g.name} | **${fmtValue(run, g)}** ✓ | ${g.gate} |\n`;
  }
  s += `
> End-to-end pod-bind p99 is **not** gated and is large by design — it is dominated by the uncapped scheduler's retry/backoff and the reprovision back-edge, neither of which is BigFleet's deliverable. See [what we gate](#what-we-gate-and-why-the-bar-is-honest).
`;
  return s;
}

function whatWeGate() {
  let gated = GATES.map((g) => `- **${g.name}** ${g.gate} — breach means ${g.breach}.`).join("\n");
  return `## What we gate, and why the bar is honest

The principle ([ADR-0054](./adr/0054-steady-bind-slo-reframe-for-uncapped-scheduler.md), [full justification in SLOs](./slos.md)): **gate BigFleet's deliverable, never an uncontrolled dependency.** The harness runs a real, *uncapped* kube-scheduler and a real provisioning back-edge; the latencies those impose are *reported*, never gated — and BigFleet may not cap the scheduler to make its own numbers pass (author decision). So the bar decomposes "demand observed → machine materialised → node published" into the per-hop bars BigFleet actually owns, measured at **steady state under churn** (not the cold-start ramp — ramp is capacity exploration, not pass/fail).

**Gated — BigFleet's own hops:**
${gated}

**Informational — reported, never gated:** end-to-end pod-bind p99 + raw-max, and fingerprint fan-out latency. The pod-bind tail runs to hundreds of seconds because a churn-reclaimed pod cannot re-bind until a replacement machine is provisioned (the reprovision back-edge) and because the uncapped scheduler backs off on retry — physics outside BigFleet's contract.

Two of the gates are anti-gaming guards: **shortfalls = 0** has no percentile headroom — no reshape makes unmet demand acceptable — and **bootstrap success** catches a materialisation-throughput collapse that latency-plus-shortfall gates alone could miss. The reframe strictly *increased* coverage (the node-state-publish hop was previously ungated).`;
}

function ladderSection() {
  let s = `## The validated-scale ladder (uber-*)

The workload is the full \`realistic.yaml\` archetype catalog — gpu-training, memory-db, co-location gangs — calibrated to a realistic machine fleet (ADR-0050): the hard demand shape, not a toy. One rung is published; the larger rungs are sequential and gated on test-fleet capacity, not on the engine. Each rung's full numbers live in its run folder; the headline scorecard above carries uber-5k's gate values.

| rung | scale | status | data |
|---|---|:--|:--|
`;
  for (const r of ladder) {
    let data = "—";
    if (r.dataDir) {
      data = `[run folder ↗](${GH}/test/scaletest/results/${r.dataDir})`;
    } else if (r.status === "passed") {
      data = "publishing"; // canonical artifact in flight
    }
    s += `| \`${r.profile}\` | ${r.scale} | ${STATUS[r.status]} | ${data} |\n`;
  }
  const next = ladder.find((r) => r.status === "next");
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

function footnoteAndAppendix() {
  let s = `## How a result is graded

Every gate is measured at **steady state under sustained churn**, never during the cold-start ramp ([ADR-0035](./adr/0035-scaletest-slos-at-steady-state.md)): ramp is capacity exploration, not a pass/fail signal. Per-machine and per-frame bars are held identical across the whole ladder; only genuinely size-scaling quantities get size-dependent thresholds ([ADR-0028](./adr/0028-cycle-p99-is-regime-parametric.md)). The page states each run's verdict under the *current* SLO set, so a committed \`summary.json\` may record an older verdict than shown here (e.g. against a since-retired saturated bind-latency metric). Separately, the shard's per-cycle decision cost was driven from seconds to tens of milliseconds over the engine-optimisation milestones — the headroom the cycle gate now runs against.

<details>
<summary><strong>uber-5k configuration runs</strong> (transparency — pre-reframe metric set)</summary>

Earlier \`uber-5k\` runs across host/cluster configurations, each with its sanitised numbers committed in the linked run folder. These predate the ADR-0054 reframe — older metric set, and the bind-latency p99 they recorded was the since-retired saturated metric — so the folder's numbers are on the metrics they captured, not the current gate set, and these are configuration variants, **not** ladder rungs.

| run | load | pass | data |
|---|---|:--:|:--|
`;
  for (const r of configRuns) {
    let data = `[run folder ↗](${GH}/test/scaletest/results/${r.dir})`;
    if (r.csv) data += ` · [csv ↗](${GH}/test/scaletest/results/${r.dir}/chain-numbers.csv)`;
    s += `| \`${r.label}\` | ${r.load} | ${r.passed ? "✓" : "✗"} | ${data} |\n`;
  }
  s += `
</details>`;
  return s;
}

function capitalise(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

function renderPage() {
  return [
    lead(),
    scorecard(),
    whatWeGate(),
    ladderSection(),
    reproduce(),
    footnoteAndAppendix(),
    `\n*Generated from \`site/scripts/sync-scaletest.mjs\`.*\n`,
  ].join("\n\n");
}

async function main() {
  await fs.mkdir(docsDir, { recursive: true });
  await fs.writeFile(path.join(docsDir, "scaletest-results.md"), renderPage());
  console.error("scaletest-sync: wrote docs/scaletest-results.md");
}

main().catch((err) => {
  console.error("sync-scaletest failed:", err);
  process.exit(1);
});
