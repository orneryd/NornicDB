import { useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  FilterX,
  LoaderCircle,
  RefreshCw,
  RotateCcw,
  Share2,
  Waypoints,
  X,
} from "lucide-react";

import type { BrowserGraphHandoff } from "../../utils/browserUrlState";
import { api, type SearchResult } from "../../utils/api";
import type { GraphContractResponse, GraphNodeModel } from "../../graph/types";
import {
  buildGraphExplorerViewModel,
  getDefaultGraphExplorerFilters,
  supportsGraphDepthControl,
  type GraphExplorerFilters,
} from "../../graph/viewModel";
import { getNodePreview } from "../../utils/nodeUtils";

interface GraphExplorerPanelProps {
  handoff: BrowserGraphHandoff | null;
  selectedDatabase: string | null;
  onNodeSelect: (node: SearchResult) => void;
  onClearGraph: () => void;
  onUpdateHandoff: (handoff: BrowserGraphHandoff) => void;
}

interface GraphExplorerState {
  loading: boolean;
  error: string | null;
  graph: GraphContractResponse | null;
  resolvedDatabase: string | null;
}

function formatTimestamp(value?: string): string {
  if (!value) return "not set";
  return value;
}

async function executeGraphHandoff(
  database: string,
  handoff: BrowserGraphHandoff,
  depth: number,
): Promise<GraphContractResponse> {
  switch (handoff.mode) {
    case "neighborhood":
      if (handoff.nodeIds.length === 0) {
        throw new Error("Select at least one node to open a graph neighborhood.");
      }
      return api.neighborhood({
        database,
        node_ids: handoff.nodeIds,
        depth,
      });
    case "expand":
      if (handoff.nodeIds.length === 0) {
        throw new Error("Select at least one node to expand the graph.");
      }
      return api.expand({
        database,
        node_ids: handoff.nodeIds,
        depth,
      });
    case "path":
      if (!handoff.sourceNodeId || !handoff.targetNodeId) {
        throw new Error("Graph path requests require both a source node and a target node.");
      }
      return api.path({
        database,
        source_node_id: handoff.sourceNodeId,
        target_node_id: handoff.targetNodeId,
      });
    case "temporal":
      if (handoff.nodeIds.length === 0 || !handoff.asOf) {
        throw new Error("Temporal graph requests require node ids and an as-of timestamp.");
      }
      return api.temporal({
        database,
        node_ids: handoff.nodeIds,
        as_of: handoff.asOf,
      });
    case "diff":
      if (handoff.nodeIds.length === 0 || !handoff.asOf) {
        throw new Error("Graph diff requests require node ids and an as-of timestamp.");
      }
      return api.diff({
        database,
        node_ids: handoff.nodeIds,
        as_of: handoff.asOf,
        compare_to: handoff.compareTo,
      });
  }
}

function toSearchResult(node: GraphNodeModel): SearchResult {
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

export function GraphExplorerPanel({
  handoff,
  selectedDatabase,
  onNodeSelect,
  onClearGraph,
  onUpdateHandoff,
}: GraphExplorerPanelProps) {
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [depth, setDepth] = useState(1);
  const [filters, setFilters] = useState<GraphExplorerFilters>(getDefaultGraphExplorerFilters);
  const [state, setState] = useState<GraphExplorerState>({
    loading: false,
    error: null,
    graph: null,
    resolvedDatabase: null,
  });

  useEffect(() => {
    setDepth(1);
    setFilters(getDefaultGraphExplorerFilters());
  }, [
    handoff?.mode,
    handoff?.sourceNodeId,
    handoff?.targetNodeId,
    handoff?.asOf,
    handoff?.compareTo,
    handoff?.nodeIds.join(","),
  ]);

  useEffect(() => {
    let cancelled = false;

    const loadGraph = async () => {
      if (!handoff) {
        setState({
          loading: false,
          error: null,
          graph: null,
          resolvedDatabase: selectedDatabase,
        });
        return;
      }

      setState((current) => ({
        ...current,
        loading: true,
        error: null,
      }));

      try {
        const database = await api.resolveDatabaseName(selectedDatabase);
        const graph = await executeGraphHandoff(database, handoff, depth);
        if (cancelled) return;

        setState({
          loading: false,
          error: null,
          graph,
          resolvedDatabase: database,
        });
      } catch (err) {
        if (cancelled) return;

        setState({
          loading: false,
          error: err instanceof Error ? err.message : "Failed to load graph explorer data.",
          graph: null,
          resolvedDatabase: selectedDatabase,
        });
      }
    };

    void loadGraph();

    return () => {
      cancelled = true;
    };
  }, [depth, handoff, refreshNonce, selectedDatabase]);

  const focusNodeIds = useMemo(() => new Set(handoff?.nodeIds ?? []), [handoff]);
  const supportsDepth = handoff ? supportsGraphDepthControl(handoff.mode) : false;
  const viewModel = useMemo(() => {
    if (!state.graph) {
      return null;
    }
    return buildGraphExplorerViewModel(state.graph, filters);
  }, [filters, state.graph]);

  useEffect(() => {
    if (!viewModel) {
      return;
    }

    setFilters((current) => {
      const next = {
        label: current.label && viewModel.labels.includes(current.label) ? current.label : null,
        relationshipType:
          current.relationshipType &&
          viewModel.relationshipTypes.includes(current.relationshipType)
            ? current.relationshipType
            : null,
      };

      if (
        next.label === current.label &&
        next.relationshipType === current.relationshipType
      ) {
        return current;
      }

      return next;
    });
  }, [viewModel]);

  if (!handoff) {
    return (
      <div className="flex-1 p-6">
        <div className="rounded-2xl border border-dashed border-norse-rune bg-norse-shadow/40 p-6">
          <div className="text-lg font-semibold text-white">Graph Explorer</div>
          <p className="mt-2 text-sm text-norse-silver">
            Select nodes from query or search results, then use Open in Graph to inspect a
            database-scoped neighborhood.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between gap-3 px-6 py-4 border-b border-norse-rune bg-norse-shadow/30">
        <div>
          <h2 className="text-lg font-semibold text-white">Graph Explorer</h2>
          <p className="text-sm text-norse-silver">
            {handoff.mode} on {state.resolvedDatabase ?? selectedDatabase ?? "resolving database"}
          </p>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          {handoff.mode === "neighborhood" && handoff.nodeIds.length > 0 && (
            <button
              type="button"
              onClick={() =>
                onUpdateHandoff({
                  ...handoff,
                  mode: "expand",
                })
              }
              className="inline-flex items-center gap-2 px-3 py-1.5 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog"
            >
              <Waypoints className="w-4 h-4" />
              Expand Focus
            </button>
          )}
          <button
            type="button"
            onClick={() => setRefreshNonce((value) => value + 1)}
            disabled={state.loading}
            className="inline-flex items-center gap-2 px-3 py-1.5 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${state.loading ? "animate-spin" : ""}`} />
            Refresh
          </button>
          <button
            type="button"
            onClick={onClearGraph}
            className="inline-flex items-center gap-2 px-3 py-1.5 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog"
          >
            <X className="w-4 h-4" />
            Clear Graph
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-6 space-y-4">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">Database</div>
            <div className="mt-2 text-sm text-white">{state.resolvedDatabase ?? "resolving"}</div>
          </div>
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">Focus nodes</div>
            <div className="mt-2 text-sm text-white">{handoff.nodeIds.length || 0}</div>
          </div>
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">As Of</div>
            <div className="mt-2 text-sm text-white">{formatTimestamp(handoff.asOf)}</div>
          </div>
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">Compare To</div>
            <div className="mt-2 text-sm text-white">{formatTimestamp(handoff.compareTo)}</div>
          </div>
        </div>

        {state.loading && (
          <div className="rounded-2xl border border-norse-rune bg-norse-shadow/40 p-6 text-sm text-norse-silver flex items-center gap-3">
            <LoaderCircle className="w-4 h-4 animate-spin" />
            Loading graph data...
          </div>
        )}

        {state.error && (
          <div className="rounded-2xl border border-red-500/30 bg-red-500/10 p-6 text-sm text-red-300 flex items-start gap-3">
            <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
            <div>{state.error}</div>
          </div>
        )}

        {!state.loading && !state.error && state.graph && (
          <>
            <div className="rounded-2xl border border-norse-rune bg-norse-shadow/40 p-4 space-y-4">
              <div className="flex flex-wrap items-end gap-4">
                {supportsDepth && (
                  <label className="flex flex-col gap-2 text-sm text-norse-silver">
                    Depth
                    <select
                      value={String(depth)}
                      onChange={(event) => setDepth(Number(event.target.value))}
                      className="min-w-32 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                    >
                      {[1, 2, 3].map((value) => (
                        <option key={value} value={value}>
                          {value}
                        </option>
                      ))}
                    </select>
                  </label>
                )}
                <label className="flex flex-col gap-2 text-sm text-norse-silver">
                  Label Filter
                  <select
                    value={filters.label ?? ""}
                    onChange={(event) =>
                      setFilters((current) => ({
                        ...current,
                        label: event.target.value || null,
                      }))
                    }
                    className="min-w-40 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                  >
                    <option value="">All labels</option>
                    {viewModel?.labels.map((label) => (
                      <option key={label} value={label}>
                        {label}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="flex flex-col gap-2 text-sm text-norse-silver">
                  Relationship Filter
                  <select
                    value={filters.relationshipType ?? ""}
                    onChange={(event) =>
                      setFilters((current) => ({
                        ...current,
                        relationshipType: event.target.value || null,
                      }))
                    }
                    className="min-w-48 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                  >
                    <option value="">All relationships</option>
                    {viewModel?.relationshipTypes.map((relationshipType) => (
                      <option key={relationshipType} value={relationshipType}>
                        {relationshipType}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  type="button"
                  onClick={() => setFilters(getDefaultGraphExplorerFilters())}
                  className="inline-flex items-center gap-2 px-3 py-2 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog"
                >
                  <FilterX className="w-4 h-4" />
                  Reset Filters
                </button>
                {supportsDepth && (
                  <button
                    type="button"
                    onClick={() => setDepth(1)}
                    disabled={depth === 1}
                    className="inline-flex items-center gap-2 px-3 py-2 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
                  >
                    <RotateCcw className="w-4 h-4" />
                    Reset Depth
                  </button>
                )}
              </div>

              <div className="flex flex-wrap items-center gap-4 text-sm">
                <span className="text-white">{state.graph.meta.node_count} nodes returned</span>
                <span className="text-white">{state.graph.meta.edge_count} edges returned</span>
                <span className="text-norse-silver">
                  Rendered:{" "}
                  <span className="text-white">
                    {viewModel?.renderedNodeCount ?? 0} nodes / {viewModel?.renderedEdgeCount ?? 0}{" "}
                    edges
                  </span>
                </span>
                <span className="text-norse-silver">
                  Source: <span className="text-white">{state.graph.meta.generated_from}</span>
                </span>
                {state.graph.meta.depth !== undefined && (
                  <span className="text-norse-silver">
                    Server depth: <span className="text-white">{state.graph.meta.depth}</span>
                  </span>
                )}
                {state.graph.meta.truncated && (
                  <span className="inline-flex items-center gap-1 text-amber-300">
                    <Share2 className="w-4 h-4" />
                    Truncated response
                  </span>
                )}
              </div>

              {state.graph.meta.warnings && state.graph.meta.warnings.length > 0 && (
                <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200">
                  {state.graph.meta.warnings.join(" ")}
                </div>
              )}
            </div>

            {state.graph.nodes.length === 0 && state.graph.edges.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-norse-rune bg-norse-shadow/30 p-6 text-sm text-norse-silver">
                The graph request returned no nodes or edges for this handoff.
              </div>
            ) : viewModel && viewModel.renderedNodeCount === 0 && viewModel.renderedEdgeCount === 0 ? (
              <div className="rounded-2xl border border-dashed border-norse-rune bg-norse-shadow/30 p-6 text-sm text-norse-silver">
                No graph elements match the current filters.
              </div>
            ) : (
              <div className="grid gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(0,1.2fr)]">
                <section className="rounded-2xl border border-norse-rune bg-norse-shadow/30 overflow-hidden">
                  <div className="border-b border-norse-rune px-4 py-3 text-sm font-medium text-white">
                    Nodes
                  </div>
                  <div className="divide-y divide-norse-rune">
                    {viewModel?.renderedNodes.map((node) => (
                      <button
                        key={node.id}
                        type="button"
                        onClick={() => onNodeSelect(toSearchResult(node))}
                        className={`w-full text-left px-4 py-3 hover:bg-nornic-primary/10 transition-colors ${
                          focusNodeIds.has(node.id) ? "bg-nornic-primary/10" : ""
                        }`}
                      >
                        <div className="flex items-center justify-between gap-3">
                          <div>
                            <div className="text-sm font-medium text-white">{node.id}</div>
                            <div className="text-xs text-norse-fog">
                              {node.labels.join(", ") || "Unlabeled"}
                            </div>
                          </div>
                          {node.status && (
                            <span className="text-xs uppercase tracking-wide text-norse-silver">
                              {node.status}
                            </span>
                          )}
                        </div>
                        <p className="mt-2 text-sm text-norse-silver line-clamp-2">
                          {getNodePreview(node.properties)}
                        </p>
                      </button>
                    ))}
                  </div>
                </section>

                <section className="rounded-2xl border border-norse-rune bg-norse-shadow/30 overflow-hidden">
                  <div className="border-b border-norse-rune px-4 py-3 text-sm font-medium text-white">
                    Edges
                  </div>
                  <div className="divide-y divide-norse-rune">
                    {!viewModel || viewModel.renderedEdges.length === 0 ? (
                      <div className="px-4 py-6 text-sm text-norse-silver">No edges returned.</div>
                    ) : (
                      viewModel.renderedEdges.map((edge) => (
                        <div key={edge.id} className="px-4 py-3">
                          <div className="text-sm text-white">{edge.type}</div>
                          <div className="mt-1 text-xs text-norse-fog">
                            {edge.source} → {edge.target}
                          </div>
                          {edge.status && (
                            <div className="mt-2 text-xs uppercase tracking-wide text-norse-silver">
                              {edge.status}
                            </div>
                          )}
                        </div>
                      ))
                    )}
                  </div>
                </section>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
