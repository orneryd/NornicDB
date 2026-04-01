import { describe, expect, it } from "vitest";

import type { BrowserGraphHandoff } from "../../utils/browserUrlState";
import {
  buildGraphExplorerTemporalHandoff,
  getGraphExplorerRequestMode,
  supportsGraphExplorerTemporalFlow,
} from "./graphExplorerTemporal";

describe("graph explorer temporal helpers", () => {
  const neighborhoodHandoff: BrowserGraphHandoff = {
    mode: "neighborhood",
    nodeIds: ["node-1", "node-2"],
  };

  it("recognizes the active request mode from the handoff", () => {
    expect(getGraphExplorerRequestMode(neighborhoodHandoff)).toBe("standard");
    expect(
      getGraphExplorerRequestMode({
        ...neighborhoodHandoff,
        mode: "temporal",
        asOf: "2026-03-15T00:00:00Z",
      }),
    ).toBe("temporal");
    expect(
      getGraphExplorerRequestMode({
        ...neighborhoodHandoff,
        mode: "diff",
        asOf: "2026-03-15T00:00:00Z",
      }),
    ).toBe("diff");
  });

  it("only enables the temporal form for node-centered graph flows", () => {
    expect(supportsGraphExplorerTemporalFlow(neighborhoodHandoff)).toBe(true);
    expect(
      supportsGraphExplorerTemporalFlow({
        mode: "path",
        nodeIds: ["node-1", "node-2"],
        sourceNodeId: "node-1",
        targetNodeId: "node-2",
      }),
    ).toBe(false);
  });

  it("builds temporal and diff handoffs without widening URL state", () => {
    expect(
      buildGraphExplorerTemporalHandoff(
        neighborhoodHandoff,
        "temporal",
        " 2026-03-15T00:00:00Z ",
        "",
      ),
    ).toEqual({
      mode: "temporal",
      nodeIds: ["node-1", "node-2"],
      sourceNodeId: undefined,
      targetNodeId: undefined,
      asOf: "2026-03-15T00:00:00Z",
      compareTo: undefined,
    });

    expect(
      buildGraphExplorerTemporalHandoff(
        neighborhoodHandoff,
        "diff",
        "2026-03-15T00:00:00Z",
        " 2026-03-20T00:00:00Z ",
      ),
    ).toEqual({
      mode: "diff",
      nodeIds: ["node-1", "node-2"],
      sourceNodeId: undefined,
      targetNodeId: undefined,
      asOf: "2026-03-15T00:00:00Z",
      compareTo: "2026-03-20T00:00:00Z",
    });
  });

  it("returns to the standard neighborhood flow by clearing temporal fields", () => {
    expect(
      buildGraphExplorerTemporalHandoff(
        {
          mode: "diff",
          nodeIds: ["node-1", "node-2"],
          asOf: "2026-03-15T00:00:00Z",
          compareTo: "2026-03-20T00:00:00Z",
        },
        "standard",
        "",
        "",
      ),
    ).toEqual({
      mode: "neighborhood",
      nodeIds: ["node-1", "node-2"],
      sourceNodeId: undefined,
      targetNodeId: undefined,
      asOf: undefined,
      compareTo: undefined,
    });
  });
});
