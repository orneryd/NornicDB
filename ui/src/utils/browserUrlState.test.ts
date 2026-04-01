import { describe, expect, it } from "vitest";

import {
  buildGraphHandoffParams,
  buildNeighborhoodGraphHandoff,
  mergeBrowserUrlState,
  normalizeGraphHandoff,
  readBrowserUrlState,
} from "./browserUrlState";

describe("browser URL state helpers", () => {
  it("reads browser state from shareable query params", () => {
    const state = readBrowserUrlState(
      new URLSearchParams(
        "database=tenant-a&view=graph&graph=path&graphNodeIds=node-2,node-1,node-2&graphSource=node-1&graphTarget=node-2&graphAsOf=2026-04-01T00:00:00Z&graphCompareTo=2026-04-02T00:00:00Z",
      ),
    );

    expect(state).toEqual({
      database: "tenant-a",
      view: "graph",
      graph: {
        mode: "path",
        nodeIds: ["node-1", "node-2"],
        sourceNodeId: "node-1",
        targetNodeId: "node-2",
        asOf: undefined,
        compareTo: undefined,
      },
    });
  });

  it("merges database and view updates without clobbering other route state", () => {
    const next = mergeBrowserUrlState(
      new URLSearchParams("graph=expand&graphNodeIds=node-9&view=compact"),
      { database: "tenant-a", view: "search" },
    );

    expect(next.toString()).toBe(
      "graph=expand&graphNodeIds=node-9&view=search&database=tenant-a",
    );
  });

  it("drops default query-view routing and clears graph params cleanly", () => {
    const next = mergeBrowserUrlState(
      new URLSearchParams(
        "database=tenant-a&view=search&graph=diff&graphNodeIds=node-1&graphCompareTo=2026-04-02T00:00:00Z",
      ),
      {
        view: "query",
        graph: null,
      },
    );

    expect(next.toString()).toBe("database=tenant-a");
  });

  it("builds graph handoff params for future graph shell entry points", () => {
    const params = buildGraphHandoffParams({
      mode: "neighborhood",
      nodeIds: ["node-1", "node-2"],
      asOf: "2026-04-01T00:00:00Z",
    });

    expect(params.toString()).toBe("view=graph&graph=neighborhood&graphNodeIds=node-1%2Cnode-2");
  });

  it("builds normalized neighborhood handoffs from selected node ids", () => {
    expect(
      buildNeighborhoodGraphHandoff(["node-2", " node-1 ", "node-2"]),
    ).toEqual({
      mode: "neighborhood",
      nodeIds: ["node-1", "node-2"],
      sourceNodeId: undefined,
      targetNodeId: undefined,
      asOf: undefined,
      compareTo: undefined,
    });
  });

  it("falls back to the focused node when graph handoff selections are empty", () => {
    expect(buildNeighborhoodGraphHandoff([], " node-4 ")).toEqual({
      mode: "neighborhood",
      nodeIds: ["node-4"],
      sourceNodeId: undefined,
      targetNodeId: undefined,
      asOf: undefined,
      compareTo: undefined,
    });
  });

  it("returns null when graph handoff selections are empty", () => {
    expect(buildNeighborhoodGraphHandoff([" ", ""])).toBeNull();
  });

  it("falls back to safe defaults for invalid URL values", () => {
    const state = readBrowserUrlState(
      new URLSearchParams("database=%20%20&tab=graph&graph=wat&graphNodeIds=node-1"),
    );

    expect(state).toEqual({
      database: null,
      view: "query",
      graph: null,
    });
  });

  it("accepts legacy tab params when restoring browser state", () => {
    const state = readBrowserUrlState(new URLSearchParams("database=tenant-a&tab=search"));

    expect(state).toEqual({
      database: "tenant-a",
      view: "search",
      graph: null,
    });
  });

  it("normalizes graph handoffs by clearing stale fields for node-centered modes", () => {
    expect(
      normalizeGraphHandoff({
        mode: "expand",
        nodeIds: ["node-2", "node-1"],
        sourceNodeId: "node-9",
        targetNodeId: "node-8",
        asOf: "2026-04-01T00:00:00Z",
        compareTo: "2026-04-02T00:00:00Z",
      }),
    ).toEqual({
      mode: "expand",
      nodeIds: ["node-1", "node-2"],
      sourceNodeId: undefined,
      targetNodeId: undefined,
      asOf: undefined,
      compareTo: undefined,
    });
  });
});
