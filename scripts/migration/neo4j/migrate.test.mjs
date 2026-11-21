import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { applyBatch, edgeUpsertCypher, NODE_UPSERT_CYPHER, replicateEdges } from "./migrate.mjs";

const result = (count) => ({ records: [{ get: (key) => key === "applied" ? count : undefined }] });

test("shared destination queries and confirmed result", async () => {
  const queries = JSON.parse(readFileSync(new URL("./queries.json", import.meta.url)));
  assert.equal(NODE_UPSERT_CYPHER, queries.nodes);
  assert.equal(edgeUpsertCypher("KNOWS"), queries.edges.replaceAll("{{type}}", "`KNOWS`"));
  const rows = [{ id: "1" }, { id: "2" }];
  const target = { run: async (query, params) => {
    assert.equal(query, queries.nodes);
    assert.deepEqual(params, { rows });
    return result(2);
  } };
  assert.equal(await applyBatch(target, NODE_UPSERT_CYPHER, rows), 2);
});

test("incomplete, missing, extra results and driver errors reject", async () => {
  for (const reply of [result(0), result(undefined), { records: [] }, { records: [...result(1).records, ...result(1).records] }]) {
    await assert.rejects(applyBatch({ run: async () => reply }, NODE_UPSERT_CYPHER, [{ id: "1" }]), /migration batch incomplete/);
  }
  await assert.rejects(applyBatch({ run: async () => { throw new Error("driver failed"); } }, NODE_UPSERT_CYPHER, [{ id: "1" }]), /driver failed/);
});

test("edge loop submits shared upsert, rejects partial target and dry run makes no writes", async () => {
  const row = { id: "r1", startId: "n1", endId: "n2", props: {} };
  const source = { run: async () => ({ records: [{ get: (key) => row[key] }] }) };
  let writes = 0;
  const target = { run: async (q, params) => {
    writes++;
    assert.equal(q, edgeUpsertCypher("KNOWS"));
    assert.deepEqual(params, { rows: [row] });
    return result(1);
  } };
  await replicateEdges(source, target, ["KNOWS"], new Set(), 500, false);
  assert.equal(writes, 1);
  await replicateEdges(source, target, ["KNOWS"], new Set(), 500, true);
  assert.equal(writes, 1);
  await assert.rejects(replicateEdges(source, { run: async () => result(0) }, ["KNOWS"], new Set(), 500, false), /target applied 0 of 1/);
});
