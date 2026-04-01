import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "./api";
import { buildGraphRoute } from "../graph/requests";

describe("graph api client", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("builds db-qualified neighborhood requests through the graph request layer", async () => {
    const response = {
      nodes: [],
      edges: [],
      meta: {
        database: "tenant-a",
        generated_from: "neighborhood",
        node_count: 0,
        edge_count: 0,
        truncated: false,
      },
    };

    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(JSON.stringify(response)));

    await expect(
      api.neighborhood({
        database: "tenant-a",
        node_ids: ["node-1"],
        depth: 2,
      }),
    ).resolves.toEqual(response);

    expect(fetchMock).toHaveBeenCalledWith(
      buildGraphRoute("tenant-a", "/neighborhood"),
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          node_ids: ["node-1"],
          depth: 2,
        }),
      }),
    );
  });

  it("surfaces backend graph errors with the existing api error parsing", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ message: "graph exploded" }), {
        status: 500,
      }),
    );

    await expect(
      api.diff({
        database: "tenant-a",
        node_ids: ["node-1"],
        as_of: "2026-03-15T00:00:00Z",
      }),
    ).rejects.toThrow("graph exploded");
  });
});
