import type { GraphNodePayload, SearchResult } from "./api";

/**
 * Node utility functions extracted from Browser.tsx
 * These functions handle node data extraction and formatting
 */

export interface ExtractedNode {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
}

/**
 * Extract node data from Cypher result cell
 * Supports both Neo4j format (nested properties) and legacy format (flat properties)
 */
export function extractNodeFromResult(
  cell: Record<string, unknown>,
): ExtractedNode | null {
  if (!cell || typeof cell !== "object") return null;

  // Get ID - Neo4j uses elementId (e.g., "4:nornicdb:123"), also check _nodeId and id
  let id = "";
  if (typeof cell.elementId === "string") {
    // Extract actual ID from elementId format "4:nornicdb:actualId"
    const elementId = cell.elementId;
    const parts = elementId.split(":");
    id = parts.length >= 3 ? parts.slice(2).join(":") : elementId;
  } else {
    id = (cell._nodeId || cell.id) as string;
  }
  if (!id) return null;

  // Get labels
  let labels: string[] = [];
  if (Array.isArray(cell.labels)) {
    labels = cell.labels as string[];
  } else if (cell.type && typeof cell.type === "string") {
    labels = [cell.type];
  }

  // Check for Neo4j format with nested properties object
  if (
    cell.properties &&
    typeof cell.properties === "object" &&
    !Array.isArray(cell.properties)
  ) {
    // Neo4j format: { elementId, labels, properties: {...} }
    return {
      id,
      labels,
      properties: cell.properties as Record<string, unknown>,
    };
  }

  // Legacy/fallback: Properties are the rest of the fields (excluding metadata)
  const excludeKeys = new Set([
    "_nodeId",
    "id",
    "elementId",
    "labels",
    "meta",
    "properties",
  ]);
  const properties: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(cell)) {
    if (!excludeKeys.has(key)) {
      properties[key] = value;
    }
  }

  return { id, labels, properties };
}

/**
 * Get a preview string from node properties
 * Looks for common fields like title, name, text, content, description, path
 */
export function getNodePreview(properties: Record<string, unknown>): string {
  const previewFields = [
    "title",
    "name",
    "text",
    "content",
    "description",
    "path",
  ];
  for (const field of previewFields) {
    if (properties[field] && typeof properties[field] === "string") {
      return properties[field] as string;
    }
  }
  return JSON.stringify(properties).slice(0, 100);
}

export function graphNodeToSearchResult(node: GraphNodePayload): SearchResult {
  return {
    node: {
      id: node.id,
      labels: node.labels,
      properties: node.properties,
      created_at: "",
    },
    score: node.score ?? 0,
  };
}

/**
 * Check if a property is read-only
 * These properties are managed by the system and should not be edited
 */
export function isReadOnlyProperty(key: string): boolean {
  const readOnlyKeys = [
    "embedded_at",
    "embedding_dimensions",
    "embedding_model",
    "has_embedding",
    "db",
    "chunk_count",
    "embedding", // Also exclude embedding from editing
  ];
  return readOnlyKeys.includes(key);
}

/**
 * Get all node IDs from query results
 * Extracts node IDs from all rows in the query result
 */
export function getAllNodeIdsFromQueryResults(
  cypherResult: {
    results: Array<{
      data: Array<{
        row: unknown[];
      }>;
    }>;
  } | null,
): string[] {
  if (!cypherResult || !cypherResult.results[0]) {
    return [];
  }

  const nodeIds: string[] = [];
  for (const row of cypherResult.results[0].data) {
    for (const cell of row.row) {
      if (cell && typeof cell === "object") {
        const cellObj = cell as Record<string, unknown>;
        if (cellObj.elementId || cellObj.id || cellObj._nodeId) {
          const nodeData = extractNodeFromResult(cellObj);
          if (nodeData && !nodeIds.includes(nodeData.id)) {
            nodeIds.push(nodeData.id);
          }
        }
      }
    }
  }
  return nodeIds;
}
