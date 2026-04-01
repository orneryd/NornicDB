import { describe, expect, it } from "vitest";

import {
  buildGraphExplorerViewModel,
  getDefaultGraphExplorerFilters,
  supportsGraphDepthControl,
} from "./viewModel";
import type { GraphContractResponse } from "./types";

const graphFixture: GraphContractResponse = {
  nodes: [
    { id: "node-1", labels: ["Person"], properties: { name: "Ada" } },
    { id: "node-2", labels: ["Company"], properties: { name: "Nornic" } },
    { id: "node-3", labels: ["Person", "Author"], properties: { name: "Lin" } },
  ],
  edges: [
    { id: "edge-1", type: "WORKS_AT", source: "node-1", target: "node-2" },
    { id: "edge-2", type: "KNOWS", source: "node-1", target: "node-3" },
    { id: "edge-3", type: "WORKS_AT", source: "node-3", target: "node-2" },
  ],
  meta: {
    database: "tenant-a",
    generated_from: "neighborhood",
    node_count: 3,
    edge_count: 3,
    truncated: false,
  },
};

describe("graph explorer view model", () => {
  it("derives sorted filter options and preserves all graph data by default", () => {
    const viewModel = buildGraphExplorerViewModel(
      graphFixture,
      getDefaultGraphExplorerFilters(),
    );

    expect(viewModel.labels).toEqual(["Author", "Company", "Person"]);
    expect(viewModel.relationshipTypes).toEqual(["KNOWS", "WORKS_AT"]);
    expect(viewModel.renderedNodes).toHaveLength(3);
    expect(viewModel.renderedEdges).toHaveLength(3);
  });

  it("filters nodes by label and constrains edges to visible endpoints", () => {
    const viewModel = buildGraphExplorerViewModel(graphFixture, {
      label: "Person",
      relationshipType: null,
    });

    expect(viewModel.renderedNodes.map((node) => node.id)).toEqual(["node-1", "node-3"]);
    expect(viewModel.renderedEdges.map((edge) => edge.id)).toEqual(["edge-2"]);
  });

  it("filters relationships deterministically without mutating node visibility", () => {
    const viewModel = buildGraphExplorerViewModel(graphFixture, {
      label: null,
      relationshipType: "WORKS_AT",
    });

    expect(viewModel.renderedNodes.map((node) => node.id)).toEqual([
      "node-1",
      "node-2",
      "node-3",
    ]);
    expect(viewModel.renderedEdges.map((edge) => edge.id)).toEqual(["edge-1", "edge-3"]);
  });

  it("preserves diff statuses in the rendered list model", () => {
    const viewModel = buildGraphExplorerViewModel(
      {
        ...graphFixture,
        nodes: [
          { ...graphFixture.nodes[0], status: "added" },
          { ...graphFixture.nodes[1], status: "changed" },
        ],
        edges: [{ ...graphFixture.edges[0], status: "removed" }],
      },
      getDefaultGraphExplorerFilters(),
    );

    expect(viewModel.renderedNodes.map((node) => node.status)).toEqual(["added", "changed"]);
    expect(viewModel.renderedEdges.map((edge) => edge.status)).toEqual(["removed"]);
  });

  it("only enables depth controls for supported graph modes", () => {
    expect(supportsGraphDepthControl("neighborhood")).toBe(true);
    expect(supportsGraphDepthControl("expand")).toBe(true);
    expect(supportsGraphDepthControl("path")).toBe(false);
    expect(supportsGraphDepthControl("diff")).toBe(false);
  });
});
