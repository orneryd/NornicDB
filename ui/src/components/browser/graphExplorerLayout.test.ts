import { describe, expect, it } from "vitest";

import type { GraphExplorerViewModel } from "../../graph/viewModel";
import { buildGraphExplorerLayout } from "./graphExplorerLayout";

const viewModelFixture: GraphExplorerViewModel = {
  labels: ["Company", "Person"],
  relationshipTypes: ["KNOWS", "WORKS_AT"],
  renderedNodeCount: 4,
  renderedEdgeCount: 3,
  renderedNodes: [
    { id: "node-1", labels: ["Person"], properties: { name: "Ada" } },
    { id: "node-2", labels: ["Company"], properties: { name: "Nornic" } },
    { id: "node-3", labels: ["Person"], properties: { name: "Lin" } },
    { id: "node-4", labels: ["Person"], properties: { name: "Eve" } },
  ],
  renderedEdges: [
    { id: "edge-1", type: "WORKS_AT", source: "node-1", target: "node-2" },
    { id: "edge-2", type: "KNOWS", source: "node-1", target: "node-3" },
    { id: "edge-3", type: "KNOWS", source: "node-1", target: "node-4" },
  ],
};

describe("graph explorer layout", () => {
  it("produces deterministic positions for rendered nodes", () => {
    const layout = buildGraphExplorerLayout(viewModelFixture, []);

    expect(Object.keys(layout.nodes)).toEqual(["node-1", "node-2", "node-3", "node-4"]);
    expect(layout.nodes["node-1"]).toEqual({ id: "node-1", x: 460, y: 90 });
    expect(layout.nodes["node-2"]).toEqual({ id: "node-2", x: 630, y: 260 });
    expect(layout.nodes["node-3"]).toEqual({ id: "node-3", x: 460, y: 430 });
    expect(layout.nodes["node-4"]).toEqual({ id: "node-4", x: 290, y: 260 });
  });

  it("pins a focused node to the center to anchor the neighborhood", () => {
    const layout = buildGraphExplorerLayout(viewModelFixture, ["node-3"]);

    expect(layout.nodes["node-3"]).toEqual({ id: "node-3", x: 460, y: 260 });
    expect(layout.nodes["node-1"].y).toBeLessThan(layout.nodes["node-2"].y);
    expect(layout.nodes["node-1"].y).toBeLessThan(layout.nodes["node-4"].y);
  });

  it("keeps a single node graph centered", () => {
    const layout = buildGraphExplorerLayout(
      {
        labels: ["Person"],
        relationshipTypes: [],
        renderedNodes: [
          { id: "solo", labels: ["Person"], properties: { name: "Solo" } },
        ],
        renderedEdges: [],
        renderedNodeCount: 1,
        renderedEdgeCount: 0,
      },
      ["solo"],
    );

    expect(layout.nodes[soloKey(layout)]).toEqual({ id: "solo", x: 460, y: 260 });
  });

  it("grid layout places nodes in a degree-sorted grid without regard to focus", () => {
    const layout = buildGraphExplorerLayout(viewModelFixture, [], "grid");

    // 4 nodes → 2x2 grid; node-1 has degree 3 (index 0), rest degree 1 (sorted by id)
    // cols=2, colStep=(920-112)/1=808; rows=2, rowStep=(520-112)/1=408
    expect(layout.nodes["node-1"]).toEqual({ id: "node-1", x: 56, y: 56 });
    expect(layout.nodes["node-2"]).toEqual({ id: "node-2", x: 864, y: 56 });
    expect(layout.nodes["node-3"]).toEqual({ id: "node-3", x: 56, y: 464 });
    expect(layout.nodes["node-4"]).toEqual({ id: "node-4", x: 864, y: 464 });
  });

  it("grid layout is deterministic for a single-node graph", () => {
    const layout = buildGraphExplorerLayout(
      {
        labels: ["Person"],
        relationshipTypes: [],
        renderedNodes: [{ id: "solo", labels: ["Person"], properties: {} }],
        renderedEdges: [],
        renderedNodeCount: 1,
        renderedEdgeCount: 0,
      },
      [],
      "grid",
    );

    expect(layout.nodes["solo"]).toEqual({ id: "solo", x: 460, y: 260 });
  });

  it("hierarchy layout places high-degree nodes in the top row", () => {
    const layout = buildGraphExplorerLayout(viewModelFixture, [], "hierarchy");

    // node-1 (degree 3) → top row (y=56); node-2/3/4 (degree 1) → bottom row (y=464)
    expect(layout.nodes["node-1"]).toEqual({ id: "node-1", x: 460, y: 56 });
    expect(layout.nodes["node-2"]).toEqual({ id: "node-2", x: 56, y: 464 });
    expect(layout.nodes["node-3"]).toEqual({ id: "node-3", x: 460, y: 464 });
    expect(layout.nodes["node-4"]).toEqual({ id: "node-4", x: 864, y: 464 });
  });

  it("hierarchy layout centers a single-degree-group graph", () => {
    // All nodes same degree (isolated) → one row centered
    const layout = buildGraphExplorerLayout(
      {
        labels: ["Person"],
        relationshipTypes: [],
        renderedNodes: [
          { id: "a", labels: ["Person"], properties: {} },
          { id: "b", labels: ["Person"], properties: {} },
        ],
        renderedEdges: [],
        renderedNodeCount: 2,
        renderedEdgeCount: 0,
      },
      [],
      "hierarchy",
    );

    // One row at y=260 (center), two nodes spaced across width
    expect(layout.nodes["a"]).toEqual({ id: "a", x: 56, y: 260 });
    expect(layout.nodes["b"]).toEqual({ id: "b", x: 864, y: 260 });
  });
});

function soloKey(layout: ReturnType<typeof buildGraphExplorerLayout>): string {
  return Object.keys(layout.nodes)[0];
}
