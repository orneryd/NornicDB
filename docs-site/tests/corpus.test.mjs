import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { createContentPolicy } from "../tools/content-policy.mjs";
import { checkMaintenanceScope } from "../tools/maintenance-scope.mjs";
const root = path.resolve("..");

test("retired archives stay unpublished and historical source links remain public", () => {
  const docs = JSON.parse(
    fs.readFileSync(".generated/public/search-index.json", "utf8"),
  );
  assert.ok(docs.every((doc) => !doc.id.startsWith("development/archive/")));
  assert.equal(fs.existsSync(".generated/development/archive"), false);
  const benchmark = fs.readFileSync(
    ".generated/performance/benchmarks-vs-neo4j.md",
    "utf8",
  );
  assert.doesNotMatch(benchmark, /file:/);
  for (const [file, line] of [
    ["create", 855],
    ["create", 1000],
    ["create", 1100],
    ["executor", 2360],
  ])
    assert.ok(benchmark.includes(`/pkg/cypher/${file}.go#L${line})`));
});
const policy = createContentPolicy(
  JSON.parse(fs.readFileSync("content-policy.json", "utf8")),
);
const tracked = execFileSync(
  "git",
  ["ls-tree", "-r", "--name-only", "origin/main", "docs"],
  { cwd: root, encoding: "utf8" },
)
  .trim()
  .split("\n");
test("main documentation is retained at its canonical path except explicit user removals", () => {
  for (const file of tracked)
    if (policy.sourcePath(file))
      assert.ok(fs.existsSync(path.join(root, policy.sourcePath(file))), file);
});
test("additional release documentation remains present", () => {
  const provenance = JSON.parse(
    fs.readFileSync("source-provenance.json", "utf8"),
  );
  for (const file of provenance.additionalFiles)
    if (policy.sourcePath(file))
      assert.ok(fs.existsSync(path.join(root, policy.sourcePath(file))), file);
});
test("release records follow their integration status in the current semantic snapshot", () => {
  const provenance = JSON.parse(
    fs.readFileSync("source-provenance.json", "utf8"),
  );
  const releases = new Set(
    provenance.additionalFiles
      .map(policy.sourcePath)
      .filter(Boolean)
      .map((file) => file.replace(/^docs\//, "")),
  );
  const documents = JSON.parse(
    fs.readFileSync(".generated/public/search-index.json", "utf8"),
  );
  const map = JSON.parse(
    fs.readFileSync(".generated/public/doc-code.json", "utf8"),
  );
  for (const doc of documents.filter((doc) => releases.has(doc.id))) {
    if (provenance.releaseContentIntegrated) {
      assert.equal(doc.semantic, true, doc.id);
      continue;
    }
    assert.equal(doc.semantic, false, doc.id);
    assert.equal(map[doc.url], undefined, doc.id);
    const file = doc.id
      .replace(/^\./, "")
      .replace(/(^|\/)README\.md$/, "$1index.md");
    assert.match(
      fs.readFileSync(path.join(".generated", file), "utf8"),
      /::: info Release branch reference/,
    );
  }
});
test("generated inventory covers every source file, including historical artifacts", () => {
  const manifest = JSON.parse(fs.readFileSync("corpus-manifest.json", "utf8"));
  const inventory = new Set(manifest.files.map((f) => "docs/" + f.path));
  for (const file of tracked)
    if (policy.sourcePath(file))
      assert.ok(inventory.has(policy.sourcePath(file)), file);
});
test("search and navigation retain the original documentation hub beneath the new home page", () => {
  const docs = JSON.parse(
    fs.readFileSync(".generated/public/search-index.json", "utf8"),
  );
  assert.equal(
    docs.find((doc) => doc.id === "README.md").url,
    "/documentation-hub",
  );
  assert.ok(fs.existsSync(".generated/documentation-hub.md"));
});
test("documentation checks respect the declared PR scope", (t) => {
  const scope = checkMaintenanceScope(root);
  t.diagnostic(`verified ${scope} scope`);
});
test("published source graph edges resolve and code snippets match their hashes", () => {
  const graph = JSON.parse(
    fs.readFileSync(".generated/public/code-graph.json", "utf8"),
  );
  assert.equal(
    graph.revision,
    execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
    "current source links must use the integrated release head, not its main base",
  );
  const ids = new Set(graph.nodes.map((n) => n.id));
  assert.equal(
    ids.size,
    graph.nodes.length,
    "every declaration must have a unique search ID",
  );
  assert.ok(ids.size > 1000);
  for (const edge of graph.links) {
    assert.ok(ids.has(edge.source));
    assert.ok(ids.has(edge.target));
  }
  for (const node of graph.nodes.slice(0, 20)) {
    const snippet = JSON.parse(
      fs.readFileSync(`.generated/public/snippets/${node.key}.json`, "utf8"),
    );
    assert.equal(snippet.hash, node.hash);
    assert.ok(snippet.body);
    assert.equal(
      createHash("sha256")
        .update(snippet.body.replace(/\r/g, "").trim())
        .digest("hex"),
      node.hash,
    );
  }
});

test("the contribution page renders and indexes the canonical repository guide", () => {
  const docs = JSON.parse(
    fs.readFileSync(".generated/public/search-index.json", "utf8"),
  );
  const doc = docs.find((doc) => doc.id === "development/contributing.md");
  assert.ok(doc);
  assert.match(doc.text, /Candidates may be pushed for review/);
  assert.match(doc.text, /go mod download/);
  const page = fs.readFileSync(
    ".generated/development/contributing.md",
    "utf8",
  );
  assert.match(page, /\]\(\/development\/embedding\.md\)/);
  const manifest = JSON.parse(fs.readFileSync("corpus-manifest.json", "utf8"));
  assert.equal(
    manifest.files.find((f) => f.path === doc.id).sha256,
    createHash("sha256")
      .update(fs.readFileSync("../CONTRIBUTING.md"))
      .digest("hex"),
  );
});
