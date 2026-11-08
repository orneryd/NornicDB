import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createCodeIndex,
  searchCode,
} from "../.vitepress/theme/code-index.mjs";
const documents = [
  {
    id: "a",
    title: "StorageEngine.SearchNodes",
    file: "pkg/storage/search.go",
    text: "func SearchNodes() {" + " padding".repeat(100) + " rareDeepToken }",
  },
  {
    id: "b",
    title: "Query",
    file: "pkg/cypher/query.go",
    text: "func Query() { return nil }",
  },
];
test("full code bodies are searchable without sending full bodies to the browser", () => {
  const index = createCodeIndex(documents);
  assert.ok(index.documents[0].text.length <= 350);
  assert.equal(searchCode(index, "rareDeepToken")[0].id, "a");
});
test("code search recognizes camel case, identifiers and partial names", () => {
  const index = createCodeIndex(documents);
  for (const query of [
    "SearchNodes",
    "search nodes",
    "StorageEng",
    "storage",
    "SEARCHNODES",
  ])
    assert.equal(searchCode(index, query)[0].id, "a", query);
});
