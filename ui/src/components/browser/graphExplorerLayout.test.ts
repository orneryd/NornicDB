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
});

function soloKey(layout: ReturnType<typeof buildGraphExplorerLayout>): string {
  return Object.keys(layout.nodes)[0];
}
