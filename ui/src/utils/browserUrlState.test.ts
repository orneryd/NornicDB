import { describe, expect, it } from "vitest";

import {
  buildGraphHandoffParams,
  mergeBrowserUrlState,
  readBrowserUrlState,
} from "./browserUrlState";

describe("browser URL state helpers", () => {
  it("reads browser state from shareable query params", () => {
    const state = readBrowserUrlState(
      new URLSearchParams(
        "database=tenant-a&tab=search&graph=path&graphNodeIds=node-1,node-2&graphSource=node-1&graphTarget=node-2&graphAsOf=2026-04-01T00:00:00Z&graphCompareTo=2026-04-02T00:00:00Z",
      ),
    );

    expect(state).toEqual({
      database: "tenant-a",
      tab: "search",
      graph: {
        mode: "path",
        nodeIds: ["node-1", "node-2"],
        sourceNodeId: "node-1",
        targetNodeId: "node-2",
        asOf: "2026-04-01T00:00:00Z",
        compareTo: "2026-04-02T00:00:00Z",
      },
    });
  });

  it("merges database and tab updates without clobbering other route state", () => {
    const next = mergeBrowserUrlState(
      new URLSearchParams("graph=expand&graphNodeIds=node-9&view=compact"),
      { database: "tenant-a", tab: "search" },
    );

    expect(next.toString()).toBe(
      "graph=expand&graphNodeIds=node-9&view=compact&database=tenant-a&tab=search",
    );
  });

  it("drops default query-tab routing and clears graph params cleanly", () => {
    const next = mergeBrowserUrlState(
      new URLSearchParams(
        "database=tenant-a&tab=search&graph=diff&graphNodeIds=node-1&graphCompareTo=2026-04-02T00:00:00Z",
      ),
      {
        tab: "query",
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

    expect(params.toString()).toBe(
      "graph=neighborhood&graphNodeIds=node-1%2Cnode-2&graphAsOf=2026-04-01T00%3A00%3A00Z",
    );
  });

  it("falls back to safe defaults for invalid URL values", () => {
    const state = readBrowserUrlState(
      new URLSearchParams("database=%20%20&tab=graph&graph=wat&graphNodeIds=node-1"),
    );

    expect(state).toEqual({
      database: null,
      tab: "query",
      graph: null,
    });
  });
});
