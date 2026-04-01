import { describe, expect, it } from "vitest";

import type { BrowserGraphHandoff } from "../../utils/browserUrlState";
import {
  buildGraphExplorerPathHandoff,
  getGraphExplorerPathDraft,
} from "./graphExplorerPath";

const neighborhoodHandoff: BrowserGraphHandoff = {
  mode: "neighborhood",
  nodeIds: ["node-2", "node-1"],
};

describe("graph explorer path helpers", () => {
  it("builds a path handoff without widening browser route state", () => {
    expect(buildGraphExplorerPathHandoff(neighborhoodHandoff, " node-9 ", "node-3")).toEqual({
      mode: "path",
      nodeIds: ["node-3", "node-9"],
      sourceNodeId: "node-9",
      targetNodeId: "node-3",
      asOf: undefined,
      compareTo: undefined,
    });
  });

  it("prefers path ids already present on the handoff", () => {
    expect(
      getGraphExplorerPathDraft(
        {
          mode: "path",
          nodeIds: ["node-1", "node-2"],
          sourceNodeId: "node-2",
          targetNodeId: "node-1",
        },
        ["node-1", "node-2", "node-3"],
      ),
    ).toEqual({
      sourceNodeId: "node-2",
      targetNodeId: "node-1",
    });
  });

  it("falls back to the first available pair when the handoff does not carry a path yet", () => {
    expect(getGraphExplorerPathDraft(neighborhoodHandoff, ["node-3", "node-1", "node-2"]))
      .toEqual({
        sourceNodeId: "node-1",
        targetNodeId: "node-2",
      });
  });
});
