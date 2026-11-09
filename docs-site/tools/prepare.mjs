import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import { createCodeIndex } from "../.vitepress/theme/code-index.mjs";
import { codeReferences, referencesSymbol } from "./code-references.mjs";
import { indexRevision } from "./index-revision.mjs";
import {
  createContentPolicy,
  isDevelopmentArchive,
} from "./content-policy.mjs";

const site = path.resolve(import.meta.dirname, ".."),
  root = path.resolve(site, "..");
const contentRoot = process.env.DOCS_SOURCE_ROOT || root;
const contentPolicy = JSON.parse(
  fs.readFileSync(path.join(site, "content-policy.json"), "utf8"),
);
const publication = createContentPolicy(contentPolicy);
const provenance = JSON.parse(
  fs.readFileSync(path.join(site, "source-provenance.json"), "utf8"),
);
const releaseFiles = new Set(
  process.env.DOCS_SOURCE_REF || provenance.releaseContentIntegrated
    ? []
    : provenance.additionalFiles
        .map(publication.sourcePath)
        .filter(Boolean)
        .map((file) => file.replace(/^docs\//, "")),
);
const source = path.join(contentRoot, "docs"),
  output = path.join(site, ".generated");
// This directory contains build output only. Remove it so tagged builds cannot
// inherit documents from a different revision.
if (output !== path.join(site, ".generated"))
  throw Error("Unexpected output directory");
fs.rmSync(output, { recursive: true, force: true });
fs.mkdirSync(output, { recursive: true });
const walk = (dir) =>
  fs
    .readdirSync(dir, { withFileTypes: true })
    .flatMap((e) =>
      e.isDirectory() ? walk(path.join(dir, e.name)) : [path.join(dir, e.name)],
    );
const sha = (value) => crypto.createHash("sha256").update(value).digest("hex");
const revision = execFileSync(
  "git",
  process.env.DOCS_SOURCE_REF
    ? ["rev-parse", process.env.DOCS_SOURCE_REF]
    : ["rev-parse", "HEAD"],
  { cwd: root, encoding: "utf8" },
).trim();
const inventory = [],
  documents = [],
  duplicates = [],
  hashes = new Map(),
  references = new Map();
const route = (file) =>
  file === "README.md"
    ? "/documentation-hub"
    : "/" +
      file
        .replace(/^\./, "")
        .replace(/\.md$/, "")
        .replace(/(^|\/)README$/, "$1index");
for (const file of walk(source)) {
  const originalRelative = path.relative(source, file).replaceAll("\\", "/");
  const published = publication.sourcePath("docs/" + originalRelative);
  if (!published) continue;
  const relative = published.replace(/^docs\//, "");
  const bytes = fs.readFileSync(file);
  inventory.push({ path: relative, sha256: sha(bytes), bytes: bytes.length });
  const dest = path.join(
    output,
    relative.replace(/^\./, "").replace(/(^|\/)README\.md$/, "$1index.md"),
  );
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  if (!relative.endsWith(".md")) {
    fs.writeFileSync(dest, bytes);
    const asset = path.join(output, "public", relative);
    fs.mkdirSync(path.dirname(asset), { recursive: true });
    fs.writeFileSync(asset, bytes);
    continue;
  }
  let text = bytes.toString("utf8").replace(/\r\n/g, "\n");
  const releaseReference = releaseFiles.has(relative);
  const archived = isDevelopmentArchive(relative),
    originalTitle = text.match(/^#\s+(.+)$/m)?.[1];
  const archiveTitle = contentPolicy.titles[published];
  if (archiveTitle && originalTitle !== archiveTitle) {
    const number = originalRelative.match(/pr(\d+)/i)?.[1];
    text = text.replace(
      /^#\s+.+$/m,
      "# " +
        archiveTitle +
        (number
          ? "\n\nSource: [" +
            originalTitle +
            "](https://github.com/orneryd/NornicDB/pull/" +
            number +
            ")."
          : "\n\nOriginal report: " + originalTitle + "."),
    );
  }
  references.set(
    relative,
    releaseReference || archived ? new Set() : codeReferences(text),
  );
  const title =
    text
      .match(/^#\s+(.+)$/m)?.[1]
      ?.replace(/\p{Extended_Pictographic}|\uFE0F/gu, "")
      .trim() ||
    relative.split("/").at(-1).replace(".md", "").replaceAll("-", " ");
  const hash = sha(text.replace(/\r/g, "").trim());
  if (hashes.has(hash))
    duplicates.push({ path: relative, canonical: hashes.get(hash) });
  else hashes.set(hash, relative);
  documents.push({
    id: relative,
    title,
    url: route(relative),
    text: text.replace(/[#*`]/g, ""),
    category: archived
      ? "Development archive"
      : releaseReference
        ? "release-branch reference"
        : relative.split("/")[0],
    semantic: !releaseReference && !archived,
  });
  if (archived) {
    const note =
      "\n\n::: info Development archive\nThis report records a specific change or qualification run. Its findings describe that source snapshot, not the current API contract.\n:::\n";
    text = text.replace(/^(# .+)$/m, "$1" + note);
  } else if (releaseReference) {
    const note = `\n\n::: info Release branch reference\nThis page records work on release/v1.4.4 at [${provenance.preservedFrom.slice(0, 8)}](https://github.com/orneryd/NornicDB/tree/${provenance.preservedFrom}). It may describe changes not yet on main.\n:::\n`;
    if (/^# .+$/m.test(text)) text = text.replace(/^(# .+)$/m, "$1" + note);
    else text += note;
  }
  // README routes become index routes. Links to repository source use immutable revisions.
  text = text.replace(/\]\(([^\s)]+)([^)]*)\)/g, (all, link, rest) => {
    if (/^(https?:|mailto:|#)/.test(link)) return all;
    // Publish editor-local references from historical reports as pinned source links.
    const localSource = link.match(
      /^file:\/\/~\/src\/NornicDB\/(.+?)(?:#(\d+)-\d+)?$/,
    );
    if (localSource)
      return `](https://github.com/orneryd/NornicDB/blob/${releaseReference ? provenance.preservedFrom : revision}/${localSource[1]}${localSource[2] ? "#L" + localSource[2] : ""}${rest})`;
    const [url, anchor] = link.split("#"),
      [pathname, query] = url.split("?"),
      resolved = pathname.startsWith("/")
        ? path.resolve(source, "." + pathname)
        : path.resolve(path.dirname(file), pathname);
    if (!resolved.startsWith(source + path.sep) && resolved !== source) {
      const repoPath = path
        .relative(contentRoot, resolved)
        .replaceAll("\\", "/");
      return `](https://github.com/orneryd/NornicDB/blob/${releaseReference ? provenance.preservedFrom : revision}/${repoPath}${anchor ? "#" + anchor : ""}${rest})`;
    }
    if (resolved === path.join(source, "README.md"))
      return `](/documentation-hub.md${anchor ? "#" + anchor : ""}${rest})`;
    const target = publication.linkPath(
      "docs/" + path.relative(source, resolved).replaceAll("\\", "/"),
    );
    if (!target)
      return `](/search?q=${encodeURIComponent(path.basename(pathname, ".md").replaceAll("-", " "))}${rest})`;
    const routePath =
      "/" +
      target.replace(/^docs\//, "").replace(/(^|\/)README\.md$/, "$1index.md") +
      (pathname.endsWith("/") ? "/index.md" : "");
    return `](${routePath}${query ? "?" + query : ""}${anchor ? "#" + anchor : ""}${rest})`;
  });
  fs.writeFileSync(dest, text);
}
const archivedDocuments = documents.filter((doc) =>
  isDevelopmentArchive(doc.id),
);
function archiveIndex(relative, title, intro, entries) {
  const text =
    "# " +
    title +
    "\n\n" +
    intro +
    "\n\n" +
    entries.map((doc) => `- [${doc.title}](${doc.url})`).join("\n") +
    "\n";
  const destination = path.join(
    output,
    relative.replace(/(^|\/)README\.md$/, "$1index.md"),
  );
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.writeFileSync(destination, text);
  documents.push({
    id: relative,
    title,
    url: route(relative),
    text,
    category: "Development archive",
    semantic: false,
  });
  references.set(relative, new Set());
}
const pullRequestRecords = archivedDocuments.filter((doc) =>
  doc.id.startsWith("development/archive/pull-requests/"),
);
if (pullRequestRecords.length)
  archiveIndex(
    "development/archive/pull-requests/README.md",
    "Pull request records",
    "Implementation reviews and measured qualification results. Pull request identifiers remain in each report as provenance.",
    pullRequestRecords,
  );
if (archivedDocuments.length)
  archiveIndex(
    "development/archive/README.md",
    "Development archive",
    "Historical implementation reviews, benchmark baselines, and qualification records. These records are excluded from the current semantic index.",
    archivedDocuments,
  );
const graphFile = path.join(site, ".source-graph.json");
for (const directory of new Set(
  inventory.map((f) => path.posix.dirname(f.path)),
)) {
  if (directory === ".") continue;
  const index = path.join(output, directory, "index.md");
  if (!fs.existsSync(index)) {
    const files = inventory.filter(
      (f) =>
        path.posix.dirname(f.path) === directory && !f.path.endsWith(".md"),
    );
    if (files.length) {
      fs.mkdirSync(path.dirname(index), { recursive: true });
      fs.writeFileSync(
        index,
        "# Source artifacts\n\n" +
          files
            .map((f) => `- [${path.posix.basename(f.path)}](/${f.path})`)
            .join("\n"),
      );
    }
  }
}
if (!fs.existsSync(graphFile))
  throw Error("Generate the source graph first: npm run graph");
const graph = JSON.parse(
  fs.readFileSync(graphFile, "utf8").replace(/^\uFEFF/, ""),
);
const graphifyFile = path.join(site, ".graphify-calls.json");
let graphifyCount = 0;
if (!process.env.DOCS_SOURCE_REF && fs.existsSync(graphifyFile)) {
  const extracted = JSON.parse(fs.readFileSync(graphifyFile, "utf8"));
  const tree = execFileSync("git", ["rev-parse", "HEAD:pkg"], {
    cwd: root,
    encoding: "utf8",
  }).trim();
  if (extracted.packageTree !== tree)
    throw Error(
      "Graphify source is stale; regenerate or remove the generated call artifact.",
    );
  const seen = new Set(graph.links.map((e) => e.source + "\0" + e.target));
  for (const edge of extracted.links)
    if (!seen.has(edge.source + "\0" + edge.target)) {
      graph.links.push(edge);
      seen.add(edge.source + "\0" + edge.target);
      graphifyCount++;
    }
}
const publicDir = path.join(output, "public");
fs.mkdirSync(publicDir, { recursive: true });
const snippets = path.join(publicDir, "snippets");
fs.mkdirSync(snippets, { recursive: true });
const docCode = {};
const codeDocuments = [];
const nameCounts = new Map();
for (const node of graph.nodes) {
  nameCounts.set(node.name, (nameCounts.get(node.name) || 0) + 1);
  const qualified = node.package.split("/").at(-1) + "." + node.name;
  nameCounts.set(qualified, (nameCounts.get(qualified) || 0) + 1);
}
for (const node of graph.nodes) {
  node.hash = sha(node.body.replace(/\r/g, "").trim());
  codeDocuments.push({
    id: node.id,
    title: node.name,
    url: "/explore?symbol=" + encodeURIComponent(node.id),
    text: node.body,
    category: "Code",
    file: node.file,
  });
  node.key = sha(node.id).slice(0, 24);
  const allDocs = documents
    .filter((d) => referencesSymbol(references.get(d.id), node, nameCounts))
    .map((d) => ({ id: d.id, title: d.title, url: d.url }));
  node.docs = allDocs.slice(0, 8);
  for (const doc of allDocs) {
    const list = (docCode[doc.url] ||= []);
    if (list.length < 5)
      list.push({ id: node.id, name: node.name, file: node.file, docs: [doc] });
  }
  fs.writeFileSync(
    path.join(snippets, node.key + ".json"),
    JSON.stringify({ ...node, revision }),
  );
  delete node.body;
}
fs.writeFileSync(
  path.join(publicDir, "doc-code.json"),
  JSON.stringify(docCode),
);
fs.writeFileSync(
  path.join(publicDir, "code-graph.json"),
  JSON.stringify({
    revision,
    indexRevision: indexRevision(documents),
    provenance: graphifyCount
      ? `Graphify static extraction and Go AST (${graphifyCount} additional Graphify call edges). Dynamic dispatch may be incomplete. This is not a runtime stack.`
      : "Go AST; package-resolved direct calls. Dynamic dispatch and runtime stacks are not represented.",
    ...graph,
  }),
);
fs.writeFileSync(
  path.join(publicDir, "search-index.json"),
  JSON.stringify(documents),
);
fs.writeFileSync(
  path.join(publicDir, "code-search-index.json"),
  JSON.stringify(createCodeIndex(codeDocuments)),
);
fs.writeFileSync(
  path.join(site, "corpus-manifest.json"),
  JSON.stringify(
    { revision, files: inventory, exactDuplicates: duplicates },
    null,
    2,
  ),
);
fs.writeFileSync(
  path.join(output, "library.md"),
  "# Documentation library\n\nEvery published document, grouped by topic.\n\n" +
    [...new Set(documents.map((d) => d.category))]
      .map(
        (category) =>
          `## ${category.replaceAll("-", " ")}\n\n` +
          documents
            .filter((d) => d.category === category)
            .map((d) => `- [${d.title}](${d.url})`)
            .join("\n"),
      )
      .join("\n\n"),
);
console.log(
  `Inventoried ${inventory.length} source files; ${documents.length} documents; ${graph.nodes.length} symbols; ${graph.links.length} direct call edges; ${duplicates.length} exact duplicates.`,
);
