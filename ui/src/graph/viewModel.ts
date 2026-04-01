import type { GraphContractResponse, GraphGeneratedFrom } from "./types";

export interface GraphExplorerFilters {
  label: string | null;
  relationshipType: string | null;
}

export interface GraphExplorerViewModel {
  labels: string[];
  relationshipTypes: string[];
  renderedNodes: GraphContractResponse["nodes"];
  renderedEdges: GraphContractResponse["edges"];
  renderedNodeCount: number;
  renderedEdgeCount: number;
}

export function getDefaultGraphExplorerFilters(): GraphExplorerFilters {
  return {
    label: null,
    relationshipType: null,
  };
}

export function supportsGraphDepthControl(mode: GraphGeneratedFrom): boolean {
  return mode === "neighborhood" || mode === "expand";
}

function getUniqueSortedValues(values: string[]): string[] {
  return Array.from(
    new Set(
      values
        .map((value) => value.trim())
        .filter(Boolean),
    ),
  ).sort((left, right) => left.localeCompare(right));
}

export function buildGraphExplorerViewModel(
  graph: GraphContractResponse,
  filters: GraphExplorerFilters,
): GraphExplorerViewModel {
  const labels = getUniqueSortedValues(graph.nodes.flatMap((node) => node.labels));
  const relationshipTypes = getUniqueSortedValues(graph.edges.map((edge) => edge.type));

  const renderedNodes = graph.nodes.filter((node) => {
    if (!filters.label) {
      return true;
    }
    return node.labels.includes(filters.label);
  });

  const renderedNodeIds = new Set(renderedNodes.map((node) => node.id));
  const renderedEdges = graph.edges.filter((edge) => {
    if (!renderedNodeIds.has(edge.source) || !renderedNodeIds.has(edge.target)) {
      return false;
    }
    if (!filters.relationshipType) {
      return true;
    }
    return edge.type === filters.relationshipType;
  });

  return {
    labels,
    relationshipTypes,
    renderedNodes,
    renderedEdges,
    renderedNodeCount: renderedNodes.length,
    renderedEdgeCount: renderedEdges.length,
  };
}
