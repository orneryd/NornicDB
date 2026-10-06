import assert from "node:assert/strict";
import { test } from "node:test";
import { callTreeLayout, layoutCallBranches, layoutSymbolBundles, packageForSymbol, rootCameraFrame } from "../src/utils/graphifyLayout.ts";

test("call traversal follows incoming and outgoing edges without revisiting cycles", () => {
  const links = [
    { source: "caller", target: "main" },
    { source: "caller", target: "callee" },
    { source: "callee", target: "main" },
    { source: "caller", target: "main" },
    { source: "main", target: "main" },
    { source: "island", target: "island-child" },
  ];
  assert.deepEqual([...callTreeLayout(links, "main")].sort(), [
    ["callee", 1], ["caller", 1], ["main", 0],
  ]);
});

test("call traversal accepts force-graph endpoints and an absent root", () => {
  assert.deepEqual([...callTreeLayout([
    { source: { id: "child" }, target: { id: "main" } },
  ], "main")], [["main", 0], ["child", 1]]);
  assert.equal(callTreeLayout([], null).size, 0);
});

test("branches are parent-relative, cycle-safe, deterministic and preserve disconnected calls", () => {
  const ids = ["main", "caller", "leaf", "island", "island-child"];
  const links = [
    { source: "caller", target: "main" },
    { source: "caller", target: "leaf" },
    { source: "leaf", target: "leaf" },
    { source: "island", target: "island-child" },
  ];
  const positions = layoutCallBranches(ids, links, "main");
  assert.equal(positions.size, ids.length);
  assert.deepEqual(positions.get("main"), { x: 0, y: 0, z: 0 });
  assert.deepEqual([...positions], [...layoutCallBranches([...ids].reverse(), [...links].reverse(), "main")]);
  for (const position of positions.values()) {
    assert.ok(Object.values(position).every(Number.isFinite));
  }
  const caller = positions.get("caller");
  const leaf = positions.get("leaf");
  assert.ok(Math.hypot(leaf.x - caller.x, leaf.y - caller.y) < 110);
  assert.ok(Math.hypot(leaf.x, leaf.y) > Math.hypot(caller.x, caller.y));
  assert.equal(layoutCallBranches([], [], null).size, 0);
});

test("reference bridges keep call branches in the rooted component", () => {
  const ids = ["main", "call", "context", "caller", "leaf"];
  const positions = layoutCallBranches(ids, [
    { source: "main", target: "call" },
    { source: "call", target: "context" },
    { source: "caller", target: "context" },
    { source: "caller", target: "leaf" },
  ], "main");
  assert.equal(positions.size, ids.length);
  assert.ok(Math.hypot(positions.get("caller").x, positions.get("caller").y) < 500);
  assert.ok(positions.get("leaf").x > positions.get("caller").x);
});

test("large fans spread leaves without stretching single-child trunks", () => {
  const leaves = Array.from({ length: 2000 }, (_, index) => `leaf-${index}`);
  const positions = layoutCallBranches(["main", "hub", ...leaves], [
    { source: "main", target: "hub" },
    ...leaves.map(id => ({ source: "hub", target: id })),
  ], "main");
  const hub = positions.get("hub");
  assert.ok(Math.hypot(hub.x, hub.y) < 110);
  const distances = leaves.map(id => {
    const point = positions.get(id);
    return Math.hypot(point.x - hub.x, point.y - hub.y, point.z - hub.z);
  });
  assert.ok(Math.max(...distances) - Math.min(...distances) > 100);
  assert.ok(Math.min(...distances) > 400);
});

test("call branches occupy three dimensions rather than a fixed plane", () => {
  const children = Array.from({ length: 12 }, (_, index) => `call-${index}`);
  const positions = layoutCallBranches(["main", ...children], children.map(id => (
    { source: "main", target: id }
  )), "main");
  for (const axis of ["x", "y", "z"]) {
    const coordinates = [...positions.values()].map(point => point[axis]);
    assert.ok(Math.max(...coordinates) - Math.min(...coordinates) > 100, `${axis} must branch in 3D`);
  }
});

test("symbol bundles stay local to their nearest call and preserve disconnected symbols", () => {
  const nodes = [
    { id: "main", kind: "call" }, { id: "child", kind: "call" },
    { id: "ctx", kind: "variable" }, { id: "field", kind: "variable" },
    { id: "Type", kind: "type" }, { id: "island", kind: "document" },
  ];
  const links = [
    { source: "main", target: "child" }, { source: "child", target: "ctx" },
    { source: "ctx", target: "field" }, { source: "Type", target: "main" },
  ];
  const bundles = layoutSymbolBundles(nodes, links, "main");
  assert.equal(bundles.size, 4);
  assert.equal(bundles.get("ctx").anchorId, "child");
  assert.equal(bundles.get("field").anchorId, "child");
  assert.equal(bundles.get("Type").anchorId, "main");
  assert.equal(bundles.get("island").anchorId, null);
  assert.deepEqual([...bundles], [...layoutSymbolBundles([...nodes].reverse(), [...links].reverse(), "main")]);
  for (const offset of bundles.values()) {
    assert.ok(Math.hypot(offset.x, offset.y, offset.z) < 150);
  }
  assert.equal(layoutSymbolBundles([], [], null).size, 0);
});

test("thousands of disconnected calls form a sparse cloud near the main tree", () => {
  const islands = Array.from({ length: 3000 }, (_, index) => `island-${index}`);
  const positions = layoutCallBranches(["main", "child", ...islands], [
    { source: "main", target: "child" },
  ], "main");
  const cloud = islands.map(id => positions.get(id));
  assert.ok(Math.max(...cloud.map(point => Math.hypot(point.x, point.y, point.z))) < 1000);
  assert.equal(new Set(cloud.map(point => JSON.stringify(point))).size, islands.length);
  assert.ok(Math.max(...cloud.map(point => point.z)) - Math.min(...cloud.map(point => point.z)) > 100);
});

test("load framing targets the root rather than a disconnected-cloud centroid", () => {
  const root = { id: "main", x: 12, y: -8, z: 4 };
  const frame = rootCameraFrame([root, { id: "island", x: 900, y: 0, z: 0 }], "main", 1200, 800, 50);
  assert.deepEqual(frame.target, { x: 12, y: -8, z: 4 });
  assert.equal(frame.position.x, root.x);
  assert.equal(frame.position.y, root.y);
  assert.ok(frame.position.z > 1000);
});

test("disconnected nodes form distinct compact package clouds", () => {
  const ids = Array.from({ length: 80 }, (_, index) => `node-${String(index).padStart(3, "0")}`);
  const packages = new Map(ids.map((id, index) => [id, index % 2 === 0 ? "pkg/storage" : "pkg/cypher"]));
  const positions = layoutCallBranches(["main", ...ids], [], "main", packages);
  const groups = ["pkg/storage", "pkg/cypher"].map(packageName => ids.filter(id => packages.get(id) === packageName).map(id => positions.get(id)));
  const centers = groups.map(points => ({
    x: points.reduce((total, point) => total + point.x, 0) / points.length,
    y: points.reduce((total, point) => total + point.y, 0) / points.length,
    z: points.reduce((total, point) => total + point.z, 0) / points.length,
  }));
  assert.ok(Math.hypot(centers[0].x-centers[1].x, centers[0].y-centers[1].y, centers[0].z-centers[1].z) > 200);
  groups.forEach((points, index) => {
    assert.ok(points.every(point => Math.hypot(point.x-centers[index].x, point.y-centers[index].y, point.z-centers[index].z) < 150));
  });
});

test("package identities use explicit metadata, source directories, or external symbol prefixes", () => {
  assert.equal(packageForSymbol({ packageName: "  storage  ", sourceFile: "pkg/other/file.go" }), "storage");
  assert.equal(packageForSymbol({ sourceFile: "pkg/storage/nodes.go" }), "pkg/storage");
  assert.equal(packageForSymbol({ sourceFile: "pkg\\cypher\\call.go" }), "pkg/cypher");
  assert.equal(packageForSymbol({ sourceFile: "main.go" }), ".");
  assert.equal(packageForSymbol({ label: "context.Context" }), "context");
  assert.equal(packageForSymbol({}), "unknown");
});