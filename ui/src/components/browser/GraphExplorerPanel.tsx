import { useEffect, useId, useMemo, useState } from "react";
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
import {
  buildGraphExplorerTemporalHandoff,
  getGraphExplorerRequestMode,
  supportsGraphExplorerTemporalFlow,
  type GraphExplorerRequestMode,
} from "./graphExplorerTemporal";
import {
  buildGraphExplorerPathHandoff,
  getGraphExplorerPathDraft,
} from "./graphExplorerPath";
import {
  isGraphPathRequestDisabled,
  isGraphTemporalRequestDisabled,
} from "./graphExplorerA11y";

interface GraphExplorerPanelProps {
  handoff: BrowserGraphHandoff | null;
  selectedDatabase: string | null;
  selectedNodeId?: string | null;
  selectedNodeIds?: string[];
  onNodeSelect: (node: SearchResult) => void;
  onExploreNode: (nodeId: string) => void;
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
  selectedNodeId,
  selectedNodeIds = [],
  onNodeSelect,
  onExploreNode,
  onClearGraph,
  onUpdateHandoff,
}: GraphExplorerPanelProps) {
  const controlsHeadingId = useId();
  const pathHelpId = useId();
  const temporalHelpId = useId();
  const displayHelpId = useId();
  const loadingStatusId = useId();
  const errorStatusId = useId();
  const summaryStatusId = useId();
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [depth, setDepth] = useState(1);
  const [filters, setFilters] = useState<GraphExplorerFilters>(getDefaultGraphExplorerFilters);
  const [requestMode, setRequestMode] = useState<GraphExplorerRequestMode>("standard");
  const [asOfInput, setAsOfInput] = useState("");
  const [compareToInput, setCompareToInput] = useState("");
  const [pathSourceInput, setPathSourceInput] = useState("");
  const [pathTargetInput, setPathTargetInput] = useState("");
  const [pathDraftWasInferred, setPathDraftWasInferred] = useState(false);
  const [manualSeedInput, setManualSeedInput] = useState("");
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
    if (!handoff) {
      setRequestMode("standard");
      setAsOfInput("");
      setCompareToInput("");
      setPathSourceInput("");
      setPathTargetInput("");
      setPathDraftWasInferred(false);
      return;
    }

    setRequestMode(getGraphExplorerRequestMode(handoff));
    setAsOfInput(handoff.asOf ?? "");
    setCompareToInput(handoff.compareTo ?? "");
  }, [handoff]);

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
  const supportsTemporalFlow = handoff ? supportsGraphExplorerTemporalFlow(handoff) : false;
  const preferredPathNodeIds = useMemo(
    () => Array.from(new Set([selectedNodeId ?? "", ...selectedNodeIds])).filter(Boolean),
    [selectedNodeId, selectedNodeIds],
  );
  const viewModel = useMemo(() => {
    if (!state.graph) {
      return null;
    }
    return buildGraphExplorerViewModel(state.graph, filters);
  }, [filters, state.graph]);
  const pathOptions = useMemo(
    () => (state.graph ? state.graph.nodes.map((node) => node.id) : []),
    [state.graph],
  );
  const isPathSubmitDisabled = isGraphPathRequestDisabled(pathSourceInput, pathTargetInput);
  const isTemporalSubmitDisabled = isGraphTemporalRequestDisabled(requestMode, asOfInput);

  useEffect(() => {
    if (!handoff || pathOptions.length === 0) {
      setPathSourceInput("");
      setPathTargetInput("");
      setPathDraftWasInferred(false);
      return;
    }

    const draft = getGraphExplorerPathDraft(handoff, pathOptions, preferredPathNodeIds);
    setPathSourceInput(draft.sourceNodeId);
    setPathTargetInput(draft.targetNodeId);
    setPathDraftWasInferred(draft.inferred);
  }, [handoff, pathOptions, preferredPathNodeIds]);

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

  const applyPathRequest = () => {
    if (!handoff || isPathSubmitDisabled) {
      return;
    }

    onUpdateHandoff(buildGraphExplorerPathHandoff(handoff, pathSourceInput, pathTargetInput));
  };

  const applyTemporalRequest = () => {
    if (!handoff || isTemporalSubmitDisabled) {
      return;
    }

    onUpdateHandoff(
      buildGraphExplorerTemporalHandoff(handoff, requestMode, asOfInput, compareToInput),
    );
  };

  const applyManualSeedRequest = () => {
    const nodeIds = Array.from(
      new Set(
        manualSeedInput
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
      ),
    );

    if (nodeIds.length === 0) {
      return;
    }

    onUpdateHandoff({
      mode: "neighborhood",
      nodeIds,
    });
  };

  if (!handoff) {
    return (
      <div className="flex-1 p-6">
        <div className="rounded-2xl border border-dashed border-norse-rune bg-norse-shadow/40 p-6">
          <div className="text-lg font-semibold text-white">Graph Explorer</div>
          <p className="mt-2 text-sm text-norse-silver">
            Happy path: pick any node in query results, search results, node details, or this graph list,
            then choose <span className="text-white">Explore neighborhood</span>. The Browser will switch here,
            seed the request, and run the neighborhood fetch automatically.
          </p>
          <ol className="mt-4 list-decimal space-y-2 pl-5 text-sm text-norse-silver">
            <li>Run a Cypher query or semantic search.</li>
            <li>Select a node and click <span className="text-white">Explore neighborhood</span>.</li>
            <li>Adjust depth, filters, path, or temporal controls once the graph loads.</li>
          </ol>
          <div className="mt-6 rounded-xl border border-norse-rune bg-norse-night/30 p-4">
            <div className="text-sm font-medium text-white">Manual seed</div>
            <p className="mt-1 text-xs text-norse-silver">
              Want to start directly in Graph Explorer? Enter one or more comma-separated node ids.
            </p>
            <div className="mt-3 flex flex-wrap items-end gap-3">
              <label className="flex min-w-[18rem] flex-1 flex-col gap-2 text-sm text-norse-silver">
                Seed node ids
                <input
                  type="text"
                  value={manualSeedInput}
                  onChange={(event) => setManualSeedInput(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      applyManualSeedRequest();
                    }
                  }}
                  placeholder="node-1, node-2"
                  className="px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                />
              </label>
              <button
                type="button"
                onClick={applyManualSeedRequest}
                disabled={manualSeedInput.trim().length === 0}
                className="inline-flex items-center gap-2 px-3 py-2 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
              >
                Explore neighborhood
              </button>
            </div>
          </div>
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
                  mode: "expand",
                  nodeIds: handoff.nodeIds,
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
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-6">
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">Database</div>
            <div className="mt-2 text-sm text-white">{state.resolvedDatabase ?? "resolving"}</div>
          </div>
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">Focus nodes</div>
            <div className="mt-2 text-sm text-white">{handoff.nodeIds.length || 0}</div>
          </div>
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">Path Source</div>
            <div className="mt-2 text-sm text-white">{handoff.sourceNodeId ?? "not set"}</div>
          </div>
          <div className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-4">
            <div className="text-xs uppercase tracking-wide text-norse-fog">Path Target</div>
            <div className="mt-2 text-sm text-white">{handoff.targetNodeId ?? "not set"}</div>
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
          <>
            <div className="rounded-2xl border border-norse-rune bg-norse-shadow/40 p-4 space-y-4">
              <section aria-labelledby={controlsHeadingId} className="space-y-4">
                <div>
                  <h3 id={controlsHeadingId} className="text-sm font-medium text-white">
                    Explorer controls
                  </h3>
                  <p className="mt-1 text-sm text-norse-silver">
                    Adjust request inputs and display filters. Press Enter in a text field to apply the matching request.
                  </p>
                </div>

                <div className="grid gap-4 xl:grid-cols-3">
                  {pathOptions.length > 0 && (
                    <fieldset
                      aria-describedby={pathHelpId}
                      className="rounded-xl border border-norse-rune bg-norse-night/30 p-4"
                    >
                      <legend className="px-1 text-sm font-medium text-white">Path request</legend>
                      <p id={pathHelpId} className="mt-1 text-xs text-norse-silver">
                        Choose two different node ids to replace the current graph with a path request.
                        {pathDraftWasInferred ? " Drafts were inferred from the current graph focus or selected nodes." : ""}
                      </p>
                      <div className="mt-3 flex flex-wrap items-end gap-4">
                        <label className="flex flex-col gap-2 text-sm text-norse-silver">
                          Path source node id
                          <input
                            type="text"
                            list="graph-path-node-options"
                            value={pathSourceInput}
                            onChange={(event) => {
                              setPathSourceInput(event.target.value);
                              setPathDraftWasInferred(false);
                            }}
                            onKeyDown={(event) => {
                              if (event.key === "Enter") {
                                event.preventDefault();
                                applyPathRequest();
                              }
                            }}
                            placeholder="node id"
                            aria-describedby={pathHelpId}
                            className="min-w-48 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                          />
                        </label>
                        <label className="flex flex-col gap-2 text-sm text-norse-silver">
                          Path target node id
                          <input
                            type="text"
                            list="graph-path-node-options"
                            value={pathTargetInput}
                            onChange={(event) => {
                              setPathTargetInput(event.target.value);
                              setPathDraftWasInferred(false);
                            }}
                            onKeyDown={(event) => {
                              if (event.key === "Enter") {
                                event.preventDefault();
                                applyPathRequest();
                              }
                            }}
                            placeholder="node id"
                            aria-describedby={pathHelpId}
                            className="min-w-48 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                          />
                        </label>
                        <button
                          type="button"
                          onClick={applyPathRequest}
                          disabled={isPathSubmitDisabled}
                          aria-disabled={isPathSubmitDisabled}
                          className="inline-flex items-center gap-2 px-3 py-2 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
                        >
                          Find path
                        </button>
                        <datalist id="graph-path-node-options">
                          {pathOptions.map((nodeId) => (
                            <option key={nodeId} value={nodeId} />
                          ))}
                        </datalist>
                      </div>
                    </fieldset>
                  )}

                  {supportsTemporalFlow && (
                    <fieldset
                      aria-describedby={temporalHelpId}
                      className="rounded-xl border border-norse-rune bg-norse-night/30 p-4"
                    >
                      <legend className="px-1 text-sm font-medium text-white">Temporal request</legend>
                      <p id={temporalHelpId} className="mt-1 text-xs text-norse-silver">
                        Switch between live, as-of, and diff requests. As-of is required for snapshot and diff modes.
                      </p>
                      <div className="mt-3 flex flex-wrap items-end gap-4">
                        <label className="flex flex-col gap-2 text-sm text-norse-silver">
                          Request mode
                          <select
                            value={requestMode}
                            onChange={(event) =>
                              setRequestMode(event.target.value as GraphExplorerRequestMode)
                            }
                            aria-describedby={temporalHelpId}
                            className="min-w-40 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                          >
                            <option value="standard">Live neighborhood</option>
                            <option value="temporal">As-of snapshot</option>
                            <option value="diff">Diff</option>
                          </select>
                        </label>
                        <label className="flex flex-col gap-2 text-sm text-norse-silver">
                          As-of timestamp
                          <input
                            type="text"
                            value={asOfInput}
                            onChange={(event) => setAsOfInput(event.target.value)}
                            onKeyDown={(event) => {
                              if (event.key === "Enter") {
                                event.preventDefault();
                                applyTemporalRequest();
                              }
                            }}
                            placeholder="2026-03-15T00:00:00Z"
                            aria-describedby={temporalHelpId}
                            className="min-w-56 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                          />
                        </label>
                        {requestMode === "diff" && (
                          <label className="flex flex-col gap-2 text-sm text-norse-silver">
                            Compare-to timestamp
                            <input
                              type="text"
                              value={compareToInput}
                              onChange={(event) => setCompareToInput(event.target.value)}
                              onKeyDown={(event) => {
                                if (event.key === "Enter") {
                                  event.preventDefault();
                                  applyTemporalRequest();
                                }
                              }}
                              placeholder="2026-03-20T00:00:00Z"
                              aria-describedby={temporalHelpId}
                              className="min-w-56 px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                            />
                          </label>
                        )}
                        <button
                          type="button"
                          onClick={applyTemporalRequest}
                          disabled={isTemporalSubmitDisabled}
                          aria-disabled={isTemporalSubmitDisabled}
                          className="inline-flex items-center gap-2 px-3 py-2 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
                        >
                          Apply request
                        </button>
                      </div>
                    </fieldset>
                  )}

                  <fieldset
                    aria-describedby={displayHelpId}
                    className="rounded-xl border border-norse-rune bg-norse-night/30 p-4"
                  >
                    <legend className="px-1 text-sm font-medium text-white">Display filters</legend>
                    <p id={displayHelpId} className="mt-1 text-xs text-norse-silver">
                      Narrow the returned graph and reset filters or depth without clearing the whole explorer.
                    </p>
                    <div className="mt-3 flex flex-wrap items-end gap-4">
                      {supportsDepth && (
                        <label className="flex flex-col gap-2 text-sm text-norse-silver">
                          Neighborhood depth
                          <select
                            value={String(depth)}
                            onChange={(event) => setDepth(Number(event.target.value))}
                            aria-describedby={displayHelpId}
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
                        Label filter
                        <select
                          value={filters.label ?? ""}
                          onChange={(event) =>
                            setFilters((current) => ({
                              ...current,
                              label: event.target.value || null,
                            }))
                          }
                          aria-describedby={displayHelpId}
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
                        Relationship filter
                        <select
                          value={filters.relationshipType ?? ""}
                          onChange={(event) =>
                            setFilters((current) => ({
                              ...current,
                              relationshipType: event.target.value || null,
                            }))
                          }
                          aria-describedby={displayHelpId}
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
                        Reset filters
                      </button>
                      {supportsDepth && (
                        <button
                          type="button"
                          onClick={() => setDepth(1)}
                          disabled={depth === 1}
                          aria-disabled={depth === 1}
                          className="inline-flex items-center gap-2 px-3 py-2 text-sm text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
                        >
                          <RotateCcw className="w-4 h-4" />
                          Reset depth
                        </button>
                      )}
                    </div>
                  </fieldset>
                </div>
              </section>

              <div id={summaryStatusId} aria-live="polite" className="flex flex-wrap items-center gap-4 text-sm">
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
                {handoff.sourceNodeId && (
                  <span className="text-norse-silver">
                    Path source: <span className="text-white">{handoff.sourceNodeId}</span>
                  </span>
                )}
                {handoff.targetNodeId && (
                  <span className="text-norse-silver">
                    Path target: <span className="text-white">{handoff.targetNodeId}</span>
                  </span>
                )}
                <span className="text-norse-silver">
                  As of: <span className="text-white">{formatTimestamp(state.graph.meta.as_of)}</span>
                </span>
                <span className="text-norse-silver">
                  Compare to:{" "}
                  <span className="text-white">{formatTimestamp(state.graph.meta.compare_to)}</span>
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

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                <div className="rounded-xl border border-norse-rune bg-norse-night/40 p-3">
                  <div className="text-xs uppercase tracking-wide text-norse-fog">
                    Generated From
                  </div>
                  <div className="mt-2 text-sm text-white">{state.graph.meta.generated_from}</div>
                </div>
                <div className="rounded-xl border border-norse-rune bg-norse-night/40 p-3">
                  <div className="text-xs uppercase tracking-wide text-norse-fog">As Of</div>
                  <div className="mt-2 text-sm text-white">
                    {formatTimestamp(state.graph.meta.as_of)}
                  </div>
                </div>
                <div className="rounded-xl border border-norse-rune bg-norse-night/40 p-3">
                  <div className="text-xs uppercase tracking-wide text-norse-fog">Compare To</div>
                  <div className="mt-2 text-sm text-white">
                    {formatTimestamp(state.graph.meta.compare_to)}
                  </div>
                </div>
                <div className="rounded-xl border border-norse-rune bg-norse-night/40 p-3">
                  <div className="text-xs uppercase tracking-wide text-norse-fog">
                    Response Flags
                  </div>
                  <div className="mt-2 text-sm text-white">
                    {state.graph.meta.truncated ? "truncated" : "complete"}
                  </div>
                </div>
              </div>
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
                  <div className="divide-y divide-norse-rune">
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
                                className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs uppercase tracking-wide ${getDiffStatusClasses(
                                  node.status,
                                )}`}
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
                            <div
                              className={`mt-2 inline-flex items-center rounded-full border px-2 py-0.5 text-xs uppercase tracking-wide ${getDiffStatusClasses(
                                edge.status,
                              )}`}
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
            )}
          </>
        )}
      </div>
    </div>
  );
}
