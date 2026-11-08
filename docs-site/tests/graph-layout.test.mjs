import { test } from "node:test";
import assert from "node:assert/strict";
import {
  indexGraph,
  neighborhood,
  fractalAnchors,
} from "../.vitepress/theme/graph-layout.mjs";
const graph = {
  nodes: Array.from({ length: 100 }, (_, i) => ({
    id: String(i),
    name: "Function" + i,
    file: `pkg/${i % 2 ? "search" : "storage"}/file${i % 3}.go`,
  })),
  links: Array.from({ length: 99 }, (_, i) => ({
    source: String(Math.floor(i / 3)),
    target: String(i + 1),
  })),
};
test("recursive neighborhoods remain bounded and preserve edge endpoints", () => {
  const index = indexGraph(graph),
    view = neighborhood(index, "0", { depth: 3, maxNodes: 20 });
  assert.ok(view.nodes.length <= 20);
  assert.ok(view.nodes.length > 4);
  const ids = new Set(view.nodes.map((n) => n.id));
  assert.equal(ids.size, view.nodes.length);
  for (const edge of view.links) {
    assert.ok(ids.has(edge.source));
    assert.ok(ids.has(edge.target));
  }
});
test("caller traversal follows incoming edges and handles cycles", () => {
  const index = indexGraph({
      ...graph,
      links: [...graph.links, { source: "10", target: "0" }],
    }),
    view = neighborhood(index, "0", { direction: "in", depth: 1 });
  assert.deepEqual(
    view.nodes.map((n) => n.id),
    ["0", "10"],
  );
});
test("fractal anchors are deterministic, finite and occupy three dimensions", () => {
  const view = neighborhood(indexGraph(graph), "0", { depth: 3 }),
    first = fractalAnchors(view.nodes, "0"),
    second = fractalAnchors(view.nodes, "0");
  assert.deepEqual([...first.anchors], [...second.anchors]);
  assert.ok(first.clusters.length);
  assert.deepEqual(first.anchors.get("0"), { x: 0, y: 0, z: 0 });
  assert.ok([...first.anchors.values()].some((p) => Math.abs(p.z) > 1));
  for (const p of first.anchors.values())
    for (const coordinate of Object.values(p))
      assert.ok(Number.isFinite(coordinate));
});
