import { QueryGraphScene } from "./QueryGraphScene";
import { Disclosure } from "../common/Disclosure";
import { Button } from "../common/Button";
import { useViewState } from "../../utils/useUrlState";
/* eslint-disable jsx-a11y/prefer-tag-over-role */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Loader2, RefreshCw } from "lucide-react";
import {
  api,
  type GraphEdgePayload,
  type GraphNeighborhoodResponse,
  type GraphNodePayload,
  type GraphPropertyFilter,
  type SearchResult,
} from "../../utils/api";
import { FilterChipList, PropertyFilterList } from "../graphFilterControls";
import { getNodePreview } from "../../utils/nodeUtils";

interface GraphExplorerPanelProps {
  selectedDatabase: string;
  selectedNode: SearchResult | null;
  selectedRelationship: GraphEdgePayload | null;
  rootNodeId: string | null;
  queryGraph: GraphNeighborhoodResponse;
  onRootNodeChange: (nodeId: string | null) => void;
  onNodeSelect: (node: GraphNodePayload) => void;
  onRelationshipSelect: (edge: GraphEdgePayload) => void;
}

const DEFAULT_LIMIT = 200;

interface GraphExplorerFilters {
  includeLabels: string[];
  includeEdgeTypes: string[];
  includeNames: string[];
  includeProps: GraphPropertyFilter[];
  excludeLabels: string[];
  excludeEdgeTypes: string[];
  excludeNames: string[];
  excludeProps: GraphPropertyFilter[];
}

function getGraphNodeLabel(node: GraphNodePayload): string {
  const previewFields = ["title", "name", "code", "path", "id"];
  for (const field of previewFields) {
    const value = node.properties[field];
    if (typeof value === "string" && value.trim() !== "") {
      return value;
    }
  }
  return getNodePreview(node.properties);
}

export function GraphExplorerPanel({
  selectedDatabase,
  selectedNode,
  selectedRelationship,
  rootNodeId,
  queryGraph,
  onRootNodeChange,
  onNodeSelect,
  onRelationshipSelect,
}: GraphExplorerPanelProps) {
  const [depth, setDepth] = useViewState("graph.depth", 3);
  const [neighborhood, setGraph] = useState<GraphNeighborhoodResponse | null>(
    null,
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Filter entries are add/remove lists; the neighborhood request reads the
  // current lists through filtersRef so adding a chip does not auto-reload
  // (the apply button drives the fetch). All lists persist in the URL.
  const [includeLabels, setIncludeLabels] = useViewState<string[]>(
    "graph.includeLabels",
    [],
  );
  const [includeEdgeTypes, setIncludeEdgeTypes] = useViewState<string[]>(
    "graph.includeEdgeTypes",
    [],
  );
  const [includeNames, setIncludeNames] = useViewState<string[]>(
    "graph.includeNames",
    [],
  );
  const [includeProps, setIncludeProps] = useViewState<GraphPropertyFilter[]>(
    "graph.includeProps",
    [],
  );
  const [excludeLabels, setExcludeLabels] = useViewState<string[]>(
    "graph.excludeLabels",
    [],
  );
  const [excludeEdgeTypes, setExcludeEdgeTypes] = useViewState<string[]>(
    "graph.excludeEdgeTypes",
    [],
  );
  const [excludeNames, setExcludeNames] = useViewState<string[]>(
    "graph.excludeNames",
    [],
  );
  const [excludeProps, setExcludeProps] = useViewState<GraphPropertyFilter[]>(
    "graph.excludeProps",
    [],
  );
  const filtersRef = useRef<GraphExplorerFilters>({
    includeLabels,
    includeEdgeTypes,
    includeNames,
    includeProps,
    excludeLabels,
    excludeEdgeTypes,
    excludeNames,
    excludeProps,
  });
  filtersRef.current = {
    includeLabels,
    includeEdgeTypes,
    includeNames,
    includeProps,
    excludeLabels,
    excludeEdgeTypes,
    excludeNames,
    excludeProps,
  };
  const [activeComponent, setActiveComponent] = useViewState<number | null>(
    "graph.component",
    null,
  );
  const effectiveRootNodeId =
    rootNodeId ??
    (queryGraph.nodes.length ? null : (selectedNode?.node.id ?? null));
  const graph = effectiveRootNodeId
    ? neighborhood
    : queryGraph.nodes.length
      ? queryGraph
      : null;
  const [find, setFind] = useViewState("graph.find", "");
  const request = useRef(0);
  const [edgeId] = useViewState("graph.edge", "");
  const selectEdge = useRef(onRelationshipSelect);
  selectEdge.current = onRelationshipSelect;
  useEffect(() => {
    if (!edgeId) return;
    const edge = graph?.edges.find((edge) => edge.id === edgeId);
    if (edge) selectEdge.current(edge);
  }, [edgeId, graph]);
  const nodesById = useMemo(() => {
    const map = new Map<string, GraphNodePayload>();
    for (const node of graph?.nodes ?? []) {
      map.set(node.id, node);
    }
    return map;
  }, [graph]);

  const activeComponentNodeIds = useMemo(() => {
    if (activeComponent == null || !graph) {
      return null;
    }
    return new Set(
      (graph.components?.[activeComponent]?.nodes ?? []).map((node) => node.id),
    );
  }, [activeComponent, graph]);

  const visibleNodes = useMemo(
    () =>
      activeComponentNodeIds
        ? (graph?.nodes.filter((node) => activeComponentNodeIds.has(node.id)) ??
          [])
        : (graph?.nodes ?? []),
    [graph, activeComponentNodeIds],
  );
  const visibleEdges = useMemo(
    () =>
      activeComponentNodeIds
        ? (graph?.edges.filter(
            (edge) =>
              activeComponentNodeIds.has(edge.source) &&
              activeComponentNodeIds.has(edge.target),
          ) ?? [])
        : (graph?.edges ?? []),
    [graph, activeComponentNodeIds],
  );

  const loadNeighborhood = useCallback(async () => {
    if (!effectiveRootNodeId) {
      setGraph(null);
      setError(null);
      return;
    }

    const id = ++request.current;
    setLoading(true);
    setError(null);
    try {
      const filters = filtersRef.current;
      const response = await api.getGraphNeighborhood({
        nodeIds: [effectiveRootNodeId],
        depth,
        limit: DEFAULT_LIMIT,
        database: selectedDatabase,
        labels: filters.includeLabels,
        relationshipTypes: filters.includeEdgeTypes,
        includeNames: filters.includeNames,
        excludeNames: filters.excludeNames,
        includeProperties: filters.includeProps,
        excludeProperties: filters.excludeProps,
        excludeLabels: filters.excludeLabels,
        excludeRelationshipTypes: filters.excludeEdgeTypes,
      });
      if (id === request.current) setGraph(response);
    } catch (err) {
      if (id !== request.current) return;
      setGraph(null);
      setError(
        err instanceof Error
          ? err.message
          : "Failed to load neighborhood graph",
      );
    } finally {
      if (id === request.current) setLoading(false);
    }
  }, [depth, effectiveRootNodeId, selectedDatabase]);

  useEffect(() => {
    if (!effectiveRootNodeId) {
      setGraph(null);
      setError(null);
      return;
    }
    void loadNeighborhood();
    return () => {
      request.current++;
    };
  }, [effectiveRootNodeId, loadNeighborhood]);

  const selectedNodeId = selectedNode?.node.id ?? null;
  const selectedRelationshipId = selectedRelationship?.id ?? null;

  return (
    <div className="flex-1 flex flex-col p-4 gap-4 overflow-hidden">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <h2 className="text-sm font-medium text-white">Graph explorer</h2>
          <p className="text-xs text-norse-silver mt-1">
            Inspect query results in 3D or expand a selected node’s
            neighborhood.
          </p>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <label className="text-xs text-norse-silver flex items-center gap-2">
            Depth
            <input
              type="number"
              value={depth}
              min={1}
              step={1}
              inputMode="numeric"
              onChange={(e) => {
                const nextDepth = Number(e.target.value);
                if (!Number.isFinite(nextDepth)) {
                  return;
                }
                setDepth(Math.max(1, Math.trunc(nextDepth)));
              }}
              className="w-20 px-2 py-1 bg-norse-stone border border-norse-rune rounded text-white"
            />
          </label>
          <button
            type="button"
            onClick={() => void loadNeighborhood()}
            disabled={loading || !effectiveRootNodeId}
            className="inline-flex items-center gap-2 px-3 py-1.5 text-sm bg-norse-stone border border-norse-rune rounded text-white hover:bg-norse-rune disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </button>
          {queryGraph.nodes.length > 0 && effectiveRootNodeId && (
            <Button variant="secondary" onClick={() => onRootNodeChange(null)}>
              Back to query graph
            </Button>
          )}
          {selectedNodeId && selectedNodeId !== effectiveRootNodeId && (
            <button
              type="button"
              onClick={() => onRootNodeChange(selectedNodeId)}
              className="px-3 py-1.5 text-sm bg-nornic-primary text-white rounded hover:bg-nornic-secondary"
            >
              Explore selected node’s neighbors
            </button>
          )}
        </div>
      </div>

      <Disclosure
        compact
        id="graph-filters"
        title="Neighborhood filters: labels, relationships, names and properties"
      >
        <div className="mt-2 space-y-2">
          <FilterChipList
            label="include labels"
            entries={includeLabels}
            onAdd={setIncludeLabels}
            onRemove={(entry) =>
              setIncludeLabels(includeLabels.filter((item) => item !== entry))
            }
            placeholder="e.g. Person"
          />
          <FilterChipList
            label="exclude labels"
            entries={excludeLabels}
            onAdd={setExcludeLabels}
            onRemove={(entry) =>
              setExcludeLabels(excludeLabels.filter((item) => item !== entry))
            }
            placeholder="e.g. Hidden"
          />
          <FilterChipList
            label="include edge types"
            entries={includeEdgeTypes}
            onAdd={setIncludeEdgeTypes}
            onRemove={(entry) =>
              setIncludeEdgeTypes(
                includeEdgeTypes.filter((item) => item !== entry),
              )
            }
            placeholder="e.g. KNOWS"
          />
          <FilterChipList
            label="exclude edge types"
            entries={excludeEdgeTypes}
            onAdd={setExcludeEdgeTypes}
            onRemove={(entry) =>
              setExcludeEdgeTypes(
                excludeEdgeTypes.filter((item) => item !== entry),
              )
            }
            placeholder="e.g. IMPORTS"
          />
          <FilterChipList
            label="include symbol names"
            entries={includeNames}
            onAdd={setIncludeNames}
            onRemove={(entry) =>
              setIncludeNames(includeNames.filter((item) => item !== entry))
            }
            placeholder="e.g. main()"
          />
          <FilterChipList
            label="exclude symbol names"
            entries={excludeNames}
            onAdd={setExcludeNames}
            onRemove={(entry) =>
              setExcludeNames(excludeNames.filter((item) => item !== entry))
            }
            placeholder="e.g. context.Context"
          />
          <PropertyFilterList
            label="include properties"
            entries={includeProps}
            onAdd={setIncludeProps}
            onRemove={(entry) =>
              setIncludeProps(includeProps.filter((item) => item !== entry))
            }
          />
          <PropertyFilterList
            label="exclude properties"
            entries={excludeProps}
            onAdd={setExcludeProps}
            onRemove={(entry) =>
              setExcludeProps(excludeProps.filter((item) => item !== entry))
            }
          />
        </div>
        <div className="mt-2 flex items-center gap-2">
          <button
            type="button"
            onClick={() => void loadNeighborhood()}
            disabled={loading || !effectiveRootNodeId}
            className="rounded bg-sky-500/90 hover:bg-sky-400 text-slate-950 text-xs font-semibold px-3 py-1 disabled:opacity-40"
          >
            apply filters
          </button>
          <button
            type="button"
            onClick={() => {
              setIncludeLabels([]);
              setIncludeEdgeTypes([]);
              setIncludeNames([]);
              setIncludeProps([]);
              setExcludeLabels([]);
              setExcludeEdgeTypes([]);
              setExcludeNames([]);
              setExcludeProps([]);
            }}
            disabled={loading || !effectiveRootNodeId}
            className="rounded border border-norse-rune bg-norse-stone px-3 py-1 text-xs text-norse-silver/80 hover:bg-norse-rune disabled:opacity-40"
          >
            reset
          </button>
        </div>
        <div className="mt-1.5 text-[10px] text-norse-silver/50 font-mono leading-snug">
          names match the symbol name exactly · properties match key + optional
          value, optionally scoped to a label or edge type — exclusions may
          split the graph into disconnected components
        </div>
      </Disclosure>

      {!effectiveRootNodeId && !graph && (
        <div className="flex-1 flex items-center justify-center rounded-xl border border-dashed border-norse-rune bg-norse-shadow/30 p-6 text-center text-norse-silver">
          <div>
            <p>
              Run a query returning nodes or relationships, or select a search
              result to explore its neighborhood.
            </p>
            <p className="text-sm text-norse-fog mt-2">
              Your query results stay available when you switch tabs.
            </p>
          </div>
        </div>
      )}

      {error && (
        <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {(effectiveRootNodeId || graph) && !error && (
        <div className="flex-1 min-h-0 rounded-xl border border-norse-rune bg-norse-shadow/30 overflow-hidden">
          {loading && !graph ? (
            <div className="h-full flex items-center justify-center text-norse-silver gap-2">
              <Loader2 className="w-5 h-5 animate-spin" />
              Loading neighborhood graph...
            </div>
          ) : graph ? (
            <div className="h-full flex flex-col">
              <div className="px-4 py-2 border-b border-norse-rune text-xs text-norse-silver flex items-center justify-between flex-wrap gap-2">
                <span>
                  Root:{" "}
                  <span className="text-white font-mono">
                    {effectiveRootNodeId ?? "Query results"}
                  </span>
                </span>
                <span className="flex items-center gap-2">
                  <span>
                    {graph.nodes.length} nodes, {graph.edges.length} edges
                    {(graph.components?.length ?? 0) > 1 &&
                      ` · ${graph.components?.length} components`}
                    {graph.meta.truncated ? " (limited to 200 nodes)" : ""}
                  </span>
                  {(graph.components?.length ?? 0) > 1 && (
                    <select
                      aria-label="Connected component"
                      value={activeComponent ?? "all"}
                      onChange={(event) =>
                        setActiveComponent(
                          event.target.value === "all"
                            ? null
                            : Number(event.target.value),
                        )
                      }
                      className="rounded border border-norse-rune bg-norse-stone px-1.5 py-0.5 text-xs text-norse-silver focus:outline-none"
                    >
                      <option value="all">all components</option>
                      {graph.components?.map((component, index) => (
                        <option key={index} value={index}>
                          component {index + 1} ({component.meta.node_count}{" "}
                          nodes)
                        </option>
                      ))}
                    </select>
                  )}
                </span>
              </div>
              <QueryGraphScene
                nodes={visibleNodes}
                edges={visibleEdges}
                selectedId={selectedNodeId}
                selectedEdgeId={selectedRelationshipId}
                rootId={effectiveRootNodeId}
                onNodeSelect={onNodeSelect}
                onEdgeSelect={onRelationshipSelect}
              />
              <Disclosure
                compact
                id="graph-browser"
                title="Nodes and relationships (keyboard access)"
              >
                <label className="block text-sm text-norse-silver">
                  Find in this graph
                  <input
                    className="block w-full mt-1 p-2 rounded border border-norse-rune bg-norse-stone text-white"
                    value={find}
                    onChange={(event) => setFind(event.target.value)}
                  />
                </label>
                <div className="grid md:grid-cols-2 gap-4 mt-3">
                  <section aria-label="Graph nodes">
                    <h3 className="text-sm font-semibold text-white">Nodes</h3>
                    <ul className="max-h-56 overflow-auto">
                      {graph.nodes
                        .filter(
                          (node) =>
                            (!activeComponentNodeIds ||
                              activeComponentNodeIds.has(node.id)) &&
                            `${getGraphNodeLabel(node)} ${node.labels.join(" ")}`
                              .toLowerCase()
                              .includes(find.toLowerCase()),
                        )
                        .map((node) => (
                          <li key={node.id}>
                            <button
                              type="button"
                              className="w-full text-left p-2 rounded text-norse-silver hover:bg-norse-rune"
                              aria-pressed={node.id === selectedNodeId}
                              onClick={() => onNodeSelect(node)}
                            >
                              {getGraphNodeLabel(node)}{" "}
                              <span className="text-xs">
                                ({node.labels.join(", ")})
                              </span>
                            </button>
                          </li>
                        ))}
                    </ul>
                  </section>
                  <section aria-label="Graph relationships">
                    <h3 className="text-sm font-semibold text-white">
                      Relationships
                    </h3>
                    <ul className="max-h-56 overflow-auto">
                      {graph.edges
                        .filter(
                          (edge) =>
                            (!activeComponentNodeIds ||
                              (activeComponentNodeIds.has(edge.source) &&
                                activeComponentNodeIds.has(edge.target))) &&
                            edge.type
                              .toLowerCase()
                              .includes(find.toLowerCase()),
                        )
                        .map((edge) => (
                          <li key={edge.id}>
                            <button
                              type="button"
                              className="w-full text-left p-2 rounded text-norse-silver hover:bg-norse-rune"
                              aria-pressed={edge.id === selectedRelationshipId}
                              onClick={() => onRelationshipSelect(edge)}
                            >
                              {nodesById.has(edge.source)
                                ? getGraphNodeLabel(nodesById.get(edge.source)!)
                                : edge.source}{" "}
                              → {edge.type} →{" "}
                              {nodesById.has(edge.target)
                                ? getGraphNodeLabel(nodesById.get(edge.target)!)
                                : edge.target}
                            </button>
                          </li>
                        ))}
                    </ul>
                  </section>
                </div>
              </Disclosure>
            </div>
          ) : (
            <div className="h-full flex items-center justify-center text-norse-silver">
              No graph data returned for the selected root node.
            </div>
          )}
        </div>
      )}

      {effectiveRootNodeId && graph && nodesById.has(effectiveRootNodeId) && (
        <div className="text-xs text-norse-silver">
          Root preview:{" "}
          <span className="text-white">
            {getGraphNodeLabel(
              nodesById.get(effectiveRootNodeId) as GraphNodePayload,
            )}
          </span>
        </div>
      )}
    </div>
  );
}
