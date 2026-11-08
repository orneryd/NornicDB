import { test } from "node:test";
import assert from "node:assert/strict";
import { indexRevision } from "../tools/index-revision.mjs";
test("semantic snapshots agree across filesystem order and line endings", () => {
  const a = {
      id: "README.md",
      title: "Start",
      url: "/index",
      text: "First\nSecond",
      category: "guides",
    },
    b = {
      id: "a.md",
      title: "Guide",
      url: "/a",
      text: "Body",
      category: "guides",
    };
  assert.equal(
    indexRevision([a, b]),
    indexRevision([b, { ...a, text: "First\r\nSecond" }]),
  );
  assert.notEqual(
    indexRevision([a, b]),
    indexRevision([{ ...a, text: "Updated" }, b]),
  );
  assert.notEqual(
    indexRevision([a, b]),
    indexRevision([{ ...a, semantic: false }, b]),
  );
});
