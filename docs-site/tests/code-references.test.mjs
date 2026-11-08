import { test } from "node:test";
import assert from "node:assert/strict";
import { codeReferences, referencesSymbol } from "../tools/code-references.mjs";
test("ordinary documentation prose does not invent code relationships", () => {
  const refs = codeReferences(
    "This unsupported search option can read a file. The status is `unsupported`.",
  );
  for (const word of ["unsupported", "search", "read"])
    assert.equal(refs.has(word), false);
});
test("query functions cannot link to unrelated private helpers or ambiguous symbols", () => {
  const counts = new Map([
    ["max", 1],
    ["Search", 2],
    ["Service.Search", 1],
  ]);
  assert.equal(
    referencesSymbol(
      codeReferences("RETURN max(n.value)"),
      { name: "max", file: "pkg/adminimport/neo4j_csv.go" },
      counts,
    ),
    false,
  );
  assert.equal(
    referencesSymbol(
      codeReferences("`Search()`"),
      { name: "Search", file: "pkg/search/a.go" },
      counts,
    ),
    false,
  );
  assert.equal(
    referencesSymbol(
      codeReferences("`Service.Search()`"),
      { name: "Service.Search", file: "pkg/search/search.go" },
      counts,
    ),
    true,
  );
  assert.equal(
    referencesSymbol(
      codeReferences("pkg/adminimport/neo4j_csv.go"),
      { name: "max", file: "pkg/adminimport/neo4j_csv.go" },
      counts,
    ),
    true,
  );
});
test("a property or another package Version method cannot link to the build-version function", () => {
  const node = {
      name: "Version",
      package: "pkg/buildinfo",
      file: "pkg/buildinfo/version.go",
    },
    counts = new Map([
      ["Version", 1],
      ["buildinfo.Version", 1],
    ]);
  assert.equal(
    referencesSymbol(
      codeReferences("`Version` and plugin.Version()"),
      node,
      counts,
    ),
    false,
  );
  assert.equal(
    referencesSymbol(codeReferences("buildinfo.Version()"), node, counts),
    true,
  );
});
test("explicit source paths, code spans, calls and qualified symbols are linked", () => {
  const refs = codeReferences(
    "Use unsupported() or NormalizeValue(), then Service.Search in pkg/search/search.go.",
  );
  for (const name of [
    "unsupported",
    "NormalizeValue",
    "Service.Search",
    "pkg/search/search.go",
  ])
    assert.equal(refs.has(name), true, name);
});
