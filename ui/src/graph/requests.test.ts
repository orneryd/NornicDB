import { describe, expect, it } from "vitest";
import {
  buildGraphDiffRequest,
  buildGraphExpandRequest,
  buildGraphNeighborhoodRequest,
  buildGraphPathRequest,
  buildGraphRoute,
  buildGraphTemporalRequest,
} from "./requests";

describe("graph request builders", () => {
  it("builds db-qualified backend graph routes", () => {
    expect(buildGraphRoute("nornic", "/neighborhood")).toBe("/nornicdb/graph/nornic/neighborhood");
    expect(buildGraphRoute("tenant-a", "expand")).toBe("/nornicdb/graph/tenant-a/expand");
    expect(buildGraphRoute("tenant-a", "/path")).toBe("/nornicdb/graph/tenant-a/path");
    expect(buildGraphRoute("tenant-a", "/temporal")).toBe("/nornicdb/graph/tenant-a/temporal");
    expect(buildGraphRoute("tenant-a", "/diff")).toBe("/nornicdb/graph/tenant-a/diff");
  });

  it("builds neighborhood requests with the backend payload shape", () => {
    const request = buildGraphNeighborhoodRequest({
      database: "nornic",
      node_ids: ["1", "2"],
      existing_node_ids: ["3"],
      existing_edge_ids: ["e-1"],
      depth: 2,
      limit: 100,
      labels: ["Document"],
      relationship_types: ["RELATES_TO"],
      as_of: "2026-03-31T12:00:00Z",
    });

    expect(request.url).toBe("/nornicdb/graph/nornic/neighborhood");
    expect(request.body).toEqual({
      node_ids: ["1", "2"],
      existing_node_ids: ["3"],
      existing_edge_ids: ["e-1"],
      depth: 2,
      limit: 100,
      labels: ["Document"],
      relationship_types: ["RELATES_TO"],
      as_of: "2026-03-31T12:00:00Z",
    });
    expect(request.init).toMatchObject({
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
    });
    expect(request.init.body).toBe(JSON.stringify(request.body));
  });

  it("builds expand requests without altering the payload shape", () => {
    const request = buildGraphExpandRequest({
      database: "tenant-a",
      node_ids: ["1", "2"],
      existing_node_ids: ["3"],
      existing_edge_ids: ["e-2"],
      depth: 1,
      limit: 50,
      relationship_types: ["RELATES_TO"],
      labels: ["Document"],
      as_of: "2026-03-21T00:00:00Z",
    });

    expect(request.url).toBe("/nornicdb/graph/tenant-a/expand");
    expect(JSON.parse(request.init.body)).toEqual({
      node_ids: ["1", "2"],
      existing_node_ids: ["3"],
      existing_edge_ids: ["e-2"],
      depth: 1,
      limit: 50,
      relationship_types: ["RELATES_TO"],
      labels: ["Document"],
      as_of: "2026-03-21T00:00:00Z",
    });
  });

  it("builds path requests using backend field names", () => {
    const request = buildGraphPathRequest({
      database: "tenant-a",
      source_node_id: "node-123",
      target_node_id: "node-456",
      limit: 4,
      labels: ["Person"],
      relationship_types: ["KNOWS"],
      as_of: "2026-03-01T00:00:00Z",
    });

    expect(request.url).toBe("/nornicdb/graph/tenant-a/path");
    expect(JSON.parse(request.init.body)).toEqual({
      source_node_id: "node-123",
      target_node_id: "node-456",
      limit: 4,
      labels: ["Person"],
      relationship_types: ["KNOWS"],
      as_of: "2026-03-01T00:00:00Z",
    });
  });

  it("builds temporal requests for historical reconstruction", () => {
    const request = buildGraphTemporalRequest({
      database: "tenant-a",
      as_of: "2026-03-15T00:00:00Z",
      node_ids: ["node-123"],
      labels: ["Document"],
      relationship_types: ["MENTIONS"],
    });

    expect(request.url).toBe("/nornicdb/graph/tenant-a/temporal");
    expect(JSON.parse(request.init.body)).toEqual({
      as_of: "2026-03-15T00:00:00Z",
      node_ids: ["node-123"],
      labels: ["Document"],
      relationship_types: ["MENTIONS"],
    });
  });

  it("builds diff requests for comparison mode", () => {
    const request = buildGraphDiffRequest({
      database: "tenant-a",
      as_of: "2026-03-15T00:00:00Z",
      compare_to: "2026-03-20T00:00:00Z",
      node_ids: ["node-123"],
      labels: ["Document"],
      relationship_types: ["MENTIONS"],
    });

    expect(request.url).toBe("/nornicdb/graph/tenant-a/diff");
    expect(JSON.parse(request.init.body)).toEqual({
      as_of: "2026-03-15T00:00:00Z",
      compare_to: "2026-03-20T00:00:00Z",
      node_ids: ["node-123"],
      labels: ["Document"],
      relationship_types: ["MENTIONS"],
    });
  });

  it("rejects blank database names in request routing", () => {
    expect(() =>
      buildGraphNeighborhoodRequest({
        database: "   ",
        node_ids: ["1"],
      }),
    ).toThrow("Database name is required for graph requests");
  });
});
