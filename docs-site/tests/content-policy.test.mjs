import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { createHash } from "node:crypto";
import { isUtf8 } from "node:buffer";
import { createContentPolicy } from "../tools/content-policy.mjs";
const rules = JSON.parse(fs.readFileSync("content-policy.json", "utf8")),
  policy = createContentPolicy(rules);
test("only explicitly approved obsolete documentation is removed", () => {
  assert.equal(policy.sourcePath("docs/.flagged-internal.md"), null);
  for (const target of Object.values(rules.relocations).filter((p) =>
    p.startsWith("docs/development/archive/"),
  ))
    assert.equal(policy.sourcePath(target), null);
  for (const [original, target] of Object.entries(rules.relocations))
    if (target.startsWith("docs/development/archive/"))
      assert.equal(policy.sourcePath(original), null);
  for (const file of rules.removed) {
    assert.match(
      file,
      /^docs\/(?:plans\/|development\/archive\/|performance\/(?:vector-ann|testdata\/|release144-verification-convergence\.md))/,
    );
    assert.equal(policy.sourcePath(file), null, file);
    assert.equal(fs.existsSync("../" + file), false, file);
  }
  assert.equal(
    policy.linkPath("docs/plans/bm25-snowball-stemmer-plugin-rfc.md"),
    "docs/user-guides/bm25-stemmers.md",
  );
  assert.equal(
    policy.sourcePath("docs/plans/canonical-bootstrap.cypher"),
    "docs/examples/canonical-bootstrap.cypher",
  );
});
test("relocated benchmark artifacts preserve their original recorded data", () => {
  for (const [from, digest] of Object.entries(rules.originalHashes)) {
    let bytes = fs.readFileSync("../" + policy.sourcePath(from));
    if (isUtf8(bytes) && !bytes.includes(0))
      bytes = Buffer.from(bytes.toString("utf8").replace(/\r\n/g, "\n"));
    assert.equal(
      createHash("sha256").update(bytes).digest("hex"),
      digest,
      from,
    );
  }
});
test("featured reports use topic paths and retain the large graph and parser measurements", () => {
  assert.match(
    fs.readFileSync("../docs/performance/benchmarks/shortest-path.md", "utf8"),
    /3,969,418/,
  );
  assert.match(
    fs.readFileSync(
      "../docs/performance/benchmarks/parser-scaling/report.md",
      "utf8",
    ),
    /Growth exponents/,
  );
  assert.match(
    fs.readFileSync(
      "../docs/performance/benchmarks/database-comparison/sweep.md",
      "utf8",
    ),
    /LadybugDB/,
  );
  for (const target of Object.values(rules.relocations))
    assert.doesNotMatch(target, /(?:^|\/)pr\d+|northwind-\d|vector-ann-\d/);
});
