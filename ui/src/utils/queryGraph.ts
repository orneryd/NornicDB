import type {
  CypherResponse,
  GraphNeighborhoodResponse,
  GraphNodePayload,
  GraphEdgePayload,
} from "./api";
import { extractNodeFromResult } from "./nodeUtils.ts";

export function graphFromQuery(
  result: CypherResponse | null,
  database: string,
  limit = 200,
): GraphNeighborhoodResponse {
  const nodes = new Map<string, GraphNodePayload>(),
    edges = new Map<string, GraphEdgePayload>();
  const identity = (value: unknown) =>
    typeof value === "string" ? value.replace(/^[45]:[^:]+:/, "") : "";
  const visit = (value: unknown) => {
    if (Array.isArray(value)) {
      value.forEach(visit);
      return;
    }
    if (!value || typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    const source = identity(
        record.startNodeElementId ?? record.start_node ?? record.source,
      ),
      target = identity(
        record.endNodeElementId ?? record.end_node ?? record.target,
      );
    if (source && target && typeof record.type === "string") {
      const id = identity(record.elementId ?? record.id);
      if (id)
        edges.set(id, {
          id,
          source,
          target,
          type: record.type,
          properties: (record.properties as Record<string, unknown>) ?? {},
        });
    } else if (
      Array.isArray(record.labels) ||
      typeof record._nodeId === "string" ||
      (typeof record.elementId === "string" &&
        record.elementId.startsWith("4:"))
    ) {
      const node = extractNodeFromResult(record);
      if (node) nodes.set(node.id, node);
    } else Object.values(record).forEach(visit);
  };
  for (const entry of result?.results ?? [])
    for (const data of entry.data) visit(data.row);
  for (const edge of edges.values())
    for (const id of [edge.source, edge.target])
      if (!nodes.has(id))
        nodes.set(id, { id, labels: ["Endpoint"], properties: {} });
  const visible = [...nodes.values()].slice(0, limit),
    ids = new Set(visible.map((n) => n.id));
  const links = [...edges.values()].filter(
    (edge) => ids.has(edge.source) && ids.has(edge.target),
  );
  return {
    nodes: visible,
    edges: links,
    meta: {
      database,
      generated_from: "query",
      node_count: visible.length,
      edge_count: links.length,
      truncated: nodes.size > limit || links.length < edges.size,
    },
  };
}
