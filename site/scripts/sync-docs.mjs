#!/usr/bin/env node
// sync-docs.mjs — copy canonical Markdown from ../../docs/ into
// site/src/content/docs/ as Starlight-compatible pages.
//
// What it does:
//   1. Walks ../../docs/ for every *.md file.
//   2. Extracts the first H1 as the page title.
//   3. Strips that H1 from the body (Starlight renders the title from
//      frontmatter; keeping the H1 produces an awkward double heading).
//   4. Rewrites internal `*.md` links to clean Starlight URLs.
//   5. Prepends YAML frontmatter (`title:` plus an optional
//      `description:` derived from the first paragraph).
//   6. Writes the transformed file under site/src/content/docs/.
//
// The sync target paths mirror docs/ structure:
//   docs/architecture.md          → src/content/docs/architecture.md
//   docs/papers/bigfleet.md       → src/content/docs/papers/bigfleet.md
//   docs/adr/0001-foo.md          → src/content/docs/adr/0001-foo.md
//
// Anything not matching this pattern (e.g. site/src/content/docs/index.mdx)
// is left alone. The synced files are listed in site/.gitignore so the
// canonical source of truth stays /docs.

import { promises as fs } from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..", "..");
const docsRoot = path.join(repoRoot, "docs");
const outRoot = path.resolve(here, "..", "src", "content", "docs");

async function walk(dir) {
  const out = [];
  for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...(await walk(p)));
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      out.push(p);
    }
  }
  return out;
}

function escapeYaml(s) {
  return s.replace(/"/g, '\\"');
}

function extractTitle(src) {
  const m = src.match(/^#\s+(.+?)\s*$/m);
  return m ? m[1].trim() : null;
}

function extractDescription(src, titleLine) {
  const after = titleLine
    ? src.slice(src.indexOf(titleLine) + titleLine.length)
    : src;
  const para = after
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .find((p) => p && !p.startsWith("#") && !p.startsWith("```"));
  if (!para) return null;
  // Collapse whitespace, strip Markdown link syntax, cap at ~200 chars.
  const flat = para
    .replace(/\s+/g, " ")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[`*_]/g, "");
  return flat.length > 200 ? flat.slice(0, 197) + "…" : flat;
}

// Rewrite `[text](relative/path.md)` → `[text](/relative/path/)`.
// Internal-only; leaves http(s) and anchor links alone.
function rewriteLinks(src, fromRelDir) {
  return src.replace(/\]\(([^)]+\.md)(#[^)]*)?\)/g, (_, target, anchor) => {
    if (/^https?:/.test(target)) return `](${target}${anchor ?? ""})`;
    // Resolve relative-to-source-file path against a virtual `/`.
    const resolved = path.posix.normalize(
      path.posix.join("/", fromRelDir, target),
    );
    const clean = resolved.replace(/\.md$/, "/");
    return `](${clean}${anchor ?? ""})`;
  });
}

async function processFile(absSrc) {
  const rel = path.relative(docsRoot, absSrc);
  // Skip docs/index.md: the site has a richer hand-written landing
  // page at src/content/docs/index.md, and we don't want sync to
  // overwrite it with the docs-index map page.
  if (rel === "index.md") return null;
  const relDir = path.dirname(rel);
  const dest = path.join(outRoot, rel);
  const raw = await fs.readFile(absSrc, "utf8");

  const titleMatch = raw.match(/^#\s+.+$/m);
  const title = extractTitle(raw) ?? rel;
  const desc = extractDescription(raw, titleMatch ? titleMatch[0] : null);

  // Strip the first H1 line; Starlight renders it from frontmatter.
  let body = raw;
  if (titleMatch) {
    body = body.replace(titleMatch[0], "").replace(/^\s*\n/, "");
  }
  body = rewriteLinks(body, relDir === "." ? "" : relDir);

  const fmLines = ["---", `title: "${escapeYaml(title)}"`];
  if (desc) fmLines.push(`description: "${escapeYaml(desc)}"`);
  fmLines.push("---", "", "");
  const fm = fmLines.join("\n");

  await fs.mkdir(path.dirname(dest), { recursive: true });
  await fs.writeFile(dest, fm + body);
  return rel;
}

async function main() {
  const files = await walk(docsRoot);
  const written = [];
  for (const f of files) {
    const rel = await processFile(f);
    if (rel !== null) written.push(rel);
  }
  written.sort();
  console.log(`sync-docs: wrote ${written.length} files into ${outRoot}`);
  for (const w of written) console.log(`  ${w}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
