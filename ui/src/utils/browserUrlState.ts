export type BrowserViewMode = "query" | "search" | "graph";
export type GraphHandoffMode = "neighborhood" | "expand" | "path" | "temporal" | "diff";

export interface BrowserGraphHandoff {
  mode: GraphHandoffMode;
  nodeIds: string[];
  sourceNodeId?: string;
  targetNodeId?: string;
  asOf?: string;
  compareTo?: string;
}

export interface BrowserUrlState {
  database: string | null;
  view: BrowserViewMode;
  graph: BrowserGraphHandoff | null;
}

const VIEW_PARAM = "view";
const LEGACY_TAB_PARAM = "tab";
const DATABASE_PARAM = "database";
const GRAPH_MODE_PARAM = "graph";
const GRAPH_NODE_IDS_PARAM = "graphNodeIds";
const GRAPH_SOURCE_NODE_ID_PARAM = "graphSource";
const GRAPH_TARGET_NODE_ID_PARAM = "graphTarget";
const GRAPH_AS_OF_PARAM = "graphAsOf";
const GRAPH_COMPARE_TO_PARAM = "graphCompareTo";

function normalizeOptional(value: string | null | undefined): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeList(value: string | null): string[] {
  if (!value) return [];
  return Array.from(
    new Set(
      value
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean),
    ),
  ).sort((left, right) => left.localeCompare(right));
}

function normalizeView(value: string | null): BrowserViewMode {
  switch (value) {
    case "search":
    case "graph":
      return value;
    default:
      return "query";
  }
}

function normalizeLegacyTab(value: string | null): BrowserViewMode {
  return value === "search" ? "search" : "query";
}

function normalizeGraphMode(value: string | null): GraphHandoffMode | null {
  switch (value) {
    case "neighborhood":
    case "expand":
    case "path":
    case "temporal":
    case "diff":
      return value;
    default:
      return null;
  }
}

export function readBrowserUrlState(searchParams: URLSearchParams): BrowserUrlState {
  const mode = normalizeGraphMode(searchParams.get(GRAPH_MODE_PARAM));
  const nodeIds = normalizeList(searchParams.get(GRAPH_NODE_IDS_PARAM));
  const graph = mode
    ? {
        mode,
        nodeIds,
        sourceNodeId: normalizeOptional(searchParams.get(GRAPH_SOURCE_NODE_ID_PARAM)),
        targetNodeId: normalizeOptional(searchParams.get(GRAPH_TARGET_NODE_ID_PARAM)),
        asOf: normalizeOptional(searchParams.get(GRAPH_AS_OF_PARAM)),
        compareTo: normalizeOptional(searchParams.get(GRAPH_COMPARE_TO_PARAM)),
      }
    : null;

  return {
    database: normalizeOptional(searchParams.get(DATABASE_PARAM)) ?? null,
    view:
      searchParams.get(VIEW_PARAM) !== null
        ? normalizeView(searchParams.get(VIEW_PARAM))
        : normalizeLegacyTab(searchParams.get(LEGACY_TAB_PARAM)),
    graph,
  };
}

export function mergeBrowserUrlState(
  currentSearchParams: URLSearchParams,
  nextState: Partial<BrowserUrlState>,
): URLSearchParams {
  const current = readBrowserUrlState(currentSearchParams);
  const merged: BrowserUrlState = {
    database: nextState.database === undefined ? current.database : nextState.database,
    view: nextState.view ?? current.view,
    graph: nextState.graph === undefined ? current.graph : nextState.graph,
  };

  const nextParams = new URLSearchParams(currentSearchParams);

  if (merged.database) {
    nextParams.set(DATABASE_PARAM, merged.database);
  } else {
    nextParams.delete(DATABASE_PARAM);
  }

  nextParams.delete(LEGACY_TAB_PARAM);

  if (merged.view !== "query") {
    nextParams.set(VIEW_PARAM, merged.view);
  } else {
    nextParams.delete(VIEW_PARAM);
  }

  if (merged.graph) {
    nextParams.set(GRAPH_MODE_PARAM, merged.graph.mode);

    if (merged.graph.nodeIds.length > 0) {
      nextParams.set(GRAPH_NODE_IDS_PARAM, merged.graph.nodeIds.join(","));
    } else {
      nextParams.delete(GRAPH_NODE_IDS_PARAM);
    }

    const graphOptionalParams: Array<[string, string | undefined]> = [
      [GRAPH_SOURCE_NODE_ID_PARAM, merged.graph.sourceNodeId],
      [GRAPH_TARGET_NODE_ID_PARAM, merged.graph.targetNodeId],
      [GRAPH_AS_OF_PARAM, merged.graph.asOf],
      [GRAPH_COMPARE_TO_PARAM, merged.graph.compareTo],
    ];

    for (const [key, value] of graphOptionalParams) {
      if (value) {
        nextParams.set(key, value);
      } else {
        nextParams.delete(key);
      }
    }
  } else {
    nextParams.delete(GRAPH_MODE_PARAM);
    nextParams.delete(GRAPH_NODE_IDS_PARAM);
    nextParams.delete(GRAPH_SOURCE_NODE_ID_PARAM);
    nextParams.delete(GRAPH_TARGET_NODE_ID_PARAM);
    nextParams.delete(GRAPH_AS_OF_PARAM);
    nextParams.delete(GRAPH_COMPARE_TO_PARAM);
  }

  return nextParams;
}

export function buildGraphHandoffParams(graph: BrowserGraphHandoff): URLSearchParams {
  return mergeBrowserUrlState(new URLSearchParams(), { view: "graph", graph });
}

export function buildNeighborhoodGraphHandoff(
  nodeIds: string[],
): BrowserGraphHandoff | null {
  const normalizedNodeIds = Array.from(
    new Set(
      nodeIds
        .map((nodeId) => nodeId.trim())
        .filter(Boolean),
    ),
  ).sort((left, right) => left.localeCompare(right));

  if (normalizedNodeIds.length === 0) {
    return null;
  }

  return {
    mode: "neighborhood",
    nodeIds: normalizedNodeIds,
  };
}

export function readBrowserRouteState(searchParams: URLSearchParams): BrowserUrlState {
  return readBrowserUrlState(searchParams);
}

export function patchBrowserRouteState(
  currentSearchParams: URLSearchParams,
  nextState: Partial<BrowserUrlState>,
): URLSearchParams {
  return mergeBrowserUrlState(currentSearchParams, nextState);
}
