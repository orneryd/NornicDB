import { useEffect, useId, useMemo, useState } from "react";
import {
  AlertCircle,
  LoaderCircle,
  Share2,
  Waypoints,
} from "lucide-react";

import type { BrowserGraphHandoff } from "../../utils/browserUrlState";
import { api, type SearchResult } from "../../utils/api";
import type { GraphContractResponse, GraphNodeModel } from "../../graph/types";
import {
  buildGraphExplorerViewModel,
  getDefaultGraphExplorerFilters,
  type GraphExplorerFilters,
} from "../../graph/viewModel";
import { GraphCanvas } from "./GraphCanvas";
import { getNodePreview } from "../../utils/nodeUtils";
import { NodeDetailsPanel } from "./NodeDetailsPanel";
import { type GraphLayoutMode } from "./graphExplorerLayout";

export interface GraphExplorerPanelControls {
  filters: GraphExplorerFilters;
  depth: number;
  layoutMode: GraphLayoutMode;
  resolvedDatabase: string | null;
  graph: GraphContractResponse | null;
  loading: boolean;
  labels: string[];
  relationshipTypes: string[];
  onFiltersChange: (filters: GraphExplorerFilters) => void;
  onDepthChange: (depth: number) => void;
  onLayoutChange: (mode: GraphLayoutMode) => void;
  onRefresh: () => void;
}

interface SimilarExpansion {
  nodeId: string;
  results: SearchResult[];
  loading: boolean;
}

interface GraphExplorerPanelProps {
  handoff: BrowserGraphHandoff | null;
  selectedDatabase: string | null;
  selectedNodeId?: string | null;
  selectedNodeIds?: string[];
  selectedNode: SearchResult | null;
  expandedSimilar: SimilarExpansion | null;
  onNodeSelect: (node: SearchResult) => void;
  onExploreNode: (nodeId: string) => void;
  onClearGraph: () => void;
  onUpdateHandoff: (handoff: BrowserGraphHandoff) => void;
  onCloseDetails: () => void;
  onFindSimilar: (nodeId: string) => Promise<void>;
  onCollapseSimilar: () => void;
  onUpdateProperties: (
    nodeId: string,
    props: Record<string, unknown>,
  ) => Promise<{ success: boolean; error?: string }>;
  onExposeControls?: (controls: GraphExplorerPanelControls) => void;
}

interface GraphExplorerState {
  loading: boolean;
  error: string | null;
  graph: GraphContractResponse | null;
  resolvedDatabase: string | null;
}

function getDiffStatusClasses(status?: GraphNodeModel["status"]): string {
  switch (status) {
    case "added":
      return "border-emerald-400/30 bg-emerald-500/10 text-emerald-200";
    case "removed":
      return "border-red-400/30 bg-red-500/10 text-red-200";
    case "changed":
      return "border-amber-400/30 bg-amber-500/10 text-amber-200";
    default:
      return "border-norse-rune bg-norse-shadow/30 text-norse-silver";
  }
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
      return api.neighborhood({ database, node_ids: handoff.nodeIds, depth });
    case "expand":
      if (handoff.nodeIds.length === 0) {
        throw new Error("Select at least one node to expand the graph.");
      }
      return api.expand({ database, node_ids: handoff.nodeIds, depth });
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
      return api.temporal({ database, node_ids: handoff.nodeIds, as_of: handoff.asOf });
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
  selectedNodeId,
  selectedNode,
  expandedSimilar,
  onNodeSelect,
  onExploreNode,
  onCloseDetails,
  onFindSimilar,
  onCollapseSimilar,
  onUpdateProperties,
  onExposeControls,
}: GraphExplorerPanelProps) {
  const loadingStatusId = useId();
  const errorStatusId = useId();
  const summaryStatusId = useId();

  const [refreshNonce, setRefreshNonce] = useState(0);
  const [depth, setDepth] = useState(1);
  const [layoutMode, setLayoutMode] = useState<GraphLayoutMode>("radial");
  const [filters, setFilters] = useState<GraphExplorerFilters>(getDefaultGraphExplorerFilters);
  const [state, setState] = useState<GraphExplorerState>({
    loading: false,
    error: null,
    graph: null,
    resolvedDatabase: null,
  });

  // Reset depth/filters when handoff changes
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

  // Load graph data
  useEffect(() => {
    let cancelled = false;

    const loadGraph = async () => {
      if (!handoff) {
        setState({ loading: false, error: null, graph: null, resolvedDatabase: selectedDatabase });
        return;
      }
      setState((current) => ({ ...current, loading: true, error: null }));
      try {
        const database = await api.resolveDatabaseName(selectedDatabase);
        const graph = await executeGraphHandoff(database, handoff, depth);
        if (cancelled) return;
        setState({ loading: false, error: null, graph, resolvedDatabase: database });
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
    return () => { cancelled = true; };
  }, [depth, handoff, refreshNonce, selectedDatabase]);

  const focusNodeIds = useMemo(() => new Set(handoff?.nodeIds ?? []), [handoff]);

  const viewModel = useMemo(() => {
    if (!state.graph) return null;
    return buildGraphExplorerViewModel(state.graph, filters);
  }, [filters, state.graph]);

  // Sync stale filters when label/relationship options change
  useEffect(() => {
    if (!viewModel) return;
    setFilters((current) => {
      const next = {
        label: current.label && viewModel.labels.includes(current.label) ? current.label : null,
        relationshipType:
          current.relationshipType && viewModel.relationshipTypes.includes(current.relationshipType)
            ? current.relationshipType
            : null,
      };
      if (next.label === current.label && next.relationshipType === current.relationshipType) {
        return current;
      }
      return next;
    });
  }, [viewModel]);

  // Expose controls state upward so Browser sidebar can render GraphExplorerControls
  useEffect(() => {
    if (!onExposeControls) return;
    onExposeControls({
      filters,
      depth,
      layoutMode,
      resolvedDatabase: state.resolvedDatabase,
      graph: state.graph,
      loading: state.loading,
      labels: viewModel?.labels ?? [],
      relationshipTypes: viewModel?.relationshipTypes ?? [],
      onFiltersChange: setFilters,
      onDepthChange: setDepth,
      onLayoutChange: setLayoutMode,
      onRefresh: () => setRefreshNonce((n) => n + 1),
    });
  }, [
    filters,
    depth,
    layoutMode,
    state.resolvedDatabase,
    state.graph,
    state.loading,
    viewModel,
    onExposeControls,
  ]);

  if (!handoff) {
    return null;
  }

  return (
    <div className="flex-1 min-w-0 min-h-0 flex overflow-hidden">
      {/* Main: canvas + lists */}
      <main className="flex-1 min-w-0 min-h-0 flex flex-col overflow-hidden">
        {/* Summary bar */}
        <div className="border-b border-norse-rune bg-norse-shadow/20 px-6 py-4 shrink-0">
          <div id={summaryStatusId} aria-live="polite" className="flex flex-wrap items-center gap-4 text-sm">
            <span className="text-white">{state.graph?.meta.node_count ?? 0} nodes returned</span>
            <span className="text-white">{state.graph?.meta.edge_count ?? 0} edges returned</span>
            <span className="text-norse-silver">
              Rendered:{" "}
              <span className="text-white">
                {viewModel?.renderedNodeCount ?? 0} nodes / {viewModel?.renderedEdgeCount ?? 0} edges
              </span>
            </span>
            <span className="text-norse-silver">
              Source: <span className="text-white">{state.graph?.meta.generated_from ?? "-"}</span>
            </span>
            {state.graph?.meta.truncated && (
              <span className="inline-flex items-center gap-1 text-amber-300">
                <Share2 className="w-4 h-4" />
                Truncated response
              </span>
            )}
          </div>
          {state.graph?.meta.warnings && state.graph.meta.warnings.length > 0 && (
            <div className="mt-3 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200">
              {state.graph.meta.warnings.join(" ")}
            </div>
          )}
        </div>

        {/* Content */}
        <div className="flex-1 min-h-0 overflow-auto p-6 space-y-4">
          {state.loading && (
            <div
              id={loadingStatusId}
              role="status"
              aria-live="polite"
              className="rounded-2xl border border-norse-rune bg-norse-shadow/40 p-6 text-sm text-norse-silver flex items-center gap-3"
            >
              <LoaderCircle className="w-4 h-4 animate-spin" />
              Loading graph data...
            </div>
          )}

          {state.error && (
            <div
              id={errorStatusId}
              role="alert"
              className="rounded-2xl border border-red-500/30 bg-red-500/10 p-6 text-sm text-red-300 flex items-start gap-3"
            >
              <AlertCircle className="w-4 h-4 mt-0.5 shrink-0" />
              <div>{state.error}</div>
            </div>
          )}

          {!state.loading && !state.error && state.graph && (
            state.graph.nodes.length === 0 && state.graph.edges.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-norse-rune bg-norse-shadow/30 p-6 text-sm text-norse-silver">
                The graph request returned no nodes or edges for this handoff.
              </div>
            ) : viewModel && viewModel.renderedNodeCount === 0 && viewModel.renderedEdgeCount === 0 ? (
              <div className="rounded-2xl border border-dashed border-norse-rune bg-norse-shadow/30 p-6 text-sm text-norse-silver">
                No graph elements match the current filters.
              </div>
            ) : (
              <>
                <div className="rounded-2xl border border-norse-rune bg-norse-shadow/30 p-4">
                  {viewModel && (
                    <GraphCanvas
                      viewModel={viewModel}
                      focusNodeIds={handoff.nodeIds}
                      selectedNodeId={selectedNodeId}
                      layoutMode={layoutMode}
                      onNodeSelect={(node) => onNodeSelect(toSearchResult(node))}
                    />
                  )}
                </div>

                <div className="grid gap-4 xl:grid-cols-2">
                  <section
                    aria-labelledby="graph-explorer-nodes-heading"
                    className="rounded-2xl border border-norse-rune bg-norse-shadow/30 overflow-hidden"
                  >
                    <div
                      id="graph-explorer-nodes-heading"
                      className="border-b border-norse-rune px-4 py-3 text-sm font-medium text-white"
                    >
                      Nodes ({viewModel?.renderedNodeCount ?? 0})
                    </div>
                    <div className="max-h-[28rem] overflow-auto divide-y divide-norse-rune">
                      {viewModel?.renderedNodes.map((node) => (
                        <div
                          key={node.id}
                          className={`px-4 py-3 hover:bg-nornic-primary/10 transition-colors ${
                            focusNodeIds.has(node.id) ? "bg-nornic-primary/10" : ""
                          }`}
                        >
                          <button
                            type="button"
                            onClick={() => onNodeSelect(toSearchResult(node))}
                            aria-current={focusNodeIds.has(node.id) ? "true" : undefined}
                            aria-label={`Open node ${node.id}${node.labels.length > 0 ? ` (${node.labels.join(", ")})` : ""}`}
                            className="w-full text-left"
                          >
                            <div className="flex items-center justify-between gap-3">
                              <div>
                                <div className="text-sm font-medium text-white">{node.id}</div>
                                <div className="text-xs text-norse-fog">
                                  {node.labels.join(", ") || "Unlabeled"}
                                </div>
                              </div>
                              {node.status && (
                                <span
                                  className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs uppercase tracking-wide ${getDiffStatusClasses(node.status)}`}
                                >
                                  {node.status}
                                </span>
                              )}
                            </div>
                            <p className="mt-2 text-sm text-norse-silver line-clamp-2">
                              {getNodePreview(node.properties)}
                            </p>
                          </button>
                          <button
                            type="button"
                            onClick={() => onExploreNode(node.id)}
                            className="mt-3 inline-flex items-center gap-1 rounded-md border border-norse-rune px-2 py-1 text-xs text-norse-silver transition-colors hover:border-norse-fog hover:text-white"
                          >
                            <Waypoints className="h-3.5 w-3.5" />
                            Explore neighborhood
                          </button>
                        </div>
                      ))}
                    </div>
                  </section>

                  <section
                    aria-labelledby="graph-explorer-edges-heading"
                    className="rounded-2xl border border-norse-rune bg-norse-shadow/30 overflow-hidden"
                  >
                    <div
                      id="graph-explorer-edges-heading"
                      className="border-b border-norse-rune px-4 py-3 text-sm font-medium text-white"
                    >
                      Edges ({viewModel?.renderedEdgeCount ?? 0})
                    </div>
                    <div className="max-h-[20rem] overflow-auto divide-y divide-norse-rune">
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
                              <div
                                className={`mt-2 inline-flex items-center rounded-full border px-2 py-0.5 text-xs uppercase tracking-wide ${getDiffStatusClasses(edge.status)}`}
                              >
                                {edge.status}
                              </div>
                            )}
                          </div>
                        ))
                      )}
                    </div>
                  </section>
                </div>
              </>
            )
          )}
        </div>
      </main>

      {/* Node details — right aside */}
      <aside className="border-l border-norse-rune bg-norse-night/40 min-h-0 overflow-hidden w-[22rem] shrink-0">
        <NodeDetailsPanel
          selectedNode={selectedNode}
          expandedSimilar={expandedSimilar}
          onClose={onCloseDetails}
          onFindSimilar={onFindSimilar}
          onCollapseSimilar={onCollapseSimilar}
          onNodeSelect={onNodeSelect}
          onExploreNode={(nodeId) => onExploreNode(nodeId)}
          onUpdateProperties={onUpdateProperties}
          onRefresh={() => setRefreshNonce((n) => n + 1)}
        />
      </aside>
    </div>
  );
}
