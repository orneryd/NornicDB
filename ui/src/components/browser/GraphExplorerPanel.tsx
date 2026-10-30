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
import {
  FilterChipList,
  PropertyFilterList,
} from "../graphFilterControls";
import { getNodePreview } from "../../utils/nodeUtils";

interface GraphExplorerPanelProps {
  selectedDatabase: string;
  selectedNode: SearchResult | null;
  selectedRelationship: GraphEdgePayload | null;
  rootNodeId: string | null;
  onRootNodeChange: (nodeId: string) => void;
  onNodeSelect: (node: GraphNodePayload) => void;
  onRelationshipSelect: (edge: GraphEdgePayload) => void;
}

interface NodePosition {
  x: number;
  y: number;
}

const VIEW_WIDTH = 960;
const VIEW_HEIGHT = 680;
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

function buildInitialPositions(
  graph: GraphNeighborhoodResponse,
  rootNodeId: string,
): Record<string, NodePosition> {
  const positions: Record<string, NodePosition> = {};
  const adjacency = new Map<string, Set<string>>();

  for (const node of graph.nodes) {
    adjacency.set(node.id, new Set<string>());
  }
  for (const edge of graph.edges) {
    if (!adjacency.has(edge.source)) {
      adjacency.set(edge.source, new Set<string>());
    }
    if (!adjacency.has(edge.target)) {
      adjacency.set(edge.target, new Set<string>());
    }
    adjacency.get(edge.source)?.add(edge.target);
    adjacency.get(edge.target)?.add(edge.source);
  }

  const levels = new Map<string, number>();
  if (adjacency.has(rootNodeId)) {
    const queue: string[] = [rootNodeId];
    levels.set(rootNodeId, 0);
    while (queue.length > 0) {
      const current = queue.shift();
      if (!current) {
        continue;
      }
      const currentLevel = levels.get(current) ?? 0;
      for (const neighbor of adjacency.get(current) ?? []) {
        if (!levels.has(neighbor)) {
          levels.set(neighbor, currentLevel + 1);
          queue.push(neighbor);
        }
      }
    }
  }

  const grouped = new Map<number, GraphNodePayload[]>();
  const disconnected: GraphNodePayload[] = [];
  for (const node of graph.nodes) {
    const level = levels.get(node.id);
    if (level === undefined) {
      disconnected.push(node);
      continue;
    }
    const bucket = grouped.get(level) ?? [];
    bucket.push(node);
    grouped.set(level, bucket);
  }

  positions[rootNodeId] = { x: VIEW_WIDTH / 2, y: VIEW_HEIGHT / 2 };
  const sortedLevels = Array.from(grouped.keys()).sort((a, b) => a - b);
  for (const level of sortedLevels) {
    if (level === 0) {
      continue;
    }
    const nodes = (grouped.get(level) ?? []).sort((a, b) =>
      a.id.localeCompare(b.id),
    );
    const radius = 120 + (level - 1) * 120;
    const step = (Math.PI * 2) / Math.max(nodes.length, 1);
    nodes.forEach((node, index) => {
      const angle = -Math.PI / 2 + index * step;
      positions[node.id] = {
        x: VIEW_WIDTH / 2 + Math.cos(angle) * radius,
        y: VIEW_HEIGHT / 2 + Math.sin(angle) * radius,
      };
    });
  }

  if (disconnected.length > 0) {
    const y = VIEW_HEIGHT - 70;
    const step = VIEW_WIDTH / (disconnected.length + 1);
    disconnected
      .sort((a, b) => a.id.localeCompare(b.id))
      .forEach((node, index) => {
        positions[node.id] = { x: step * (index + 1), y };
      });
  }

  return positions;
}

export function GraphExplorerPanel({
  selectedDatabase,
  selectedNode,
  selectedRelationship,
  rootNodeId,
  onRootNodeChange,
  onNodeSelect,
  onRelationshipSelect,
}: GraphExplorerPanelProps) {
  const [depth, setDepth] = useState(3);
  const [graph, setGraph] = useState<GraphNeighborhoodResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Filter entries are add/remove lists; the neighborhood request reads the
  // current lists through filtersRef so adding a chip does not auto-reload
  // (the apply button drives the fetch). All lists persist in localStorage.
  const EXPLORER_FILTERS_STORAGE_KEY = "nornicdb.explorer.filters.v1";
  interface StoredExplorerFilters {
    includeLabels?: string[];
    includeEdgeTypes?: string[];
    includeNames?: string[];
    includeProps?: GraphPropertyFilter[];
    excludeLabels?: string[];
    excludeEdgeTypes?: string[];
    excludeNames?: string[];
    excludeProps?: GraphPropertyFilter[];
  }
  const initialFilters = useMemo<StoredExplorerFilters>(() => {
    try {
      const raw = localStorage.getItem(EXPLORER_FILTERS_STORAGE_KEY);
      if (!raw) return {};
      const parsed = JSON.parse(raw) as StoredExplorerFilters;
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  }, []);
  const [includeLabels, setIncludeLabels] = useState<string[]>(
    initialFilters.includeLabels ?? [],
  );
  const [includeEdgeTypes, setIncludeEdgeTypes] = useState<string[]>(
    initialFilters.includeEdgeTypes ?? [],
  );
  const [includeNames, setIncludeNames] = useState<string[]>(
    initialFilters.includeNames ?? [],
  );
  const [includeProps, setIncludeProps] = useState<GraphPropertyFilter[]>(
    initialFilters.includeProps ?? [],
  );
  const [excludeLabels, setExcludeLabels] = useState<string[]>(
    initialFilters.excludeLabels ?? [],
  );
  const [excludeEdgeTypes, setExcludeEdgeTypes] = useState<string[]>(
    initialFilters.excludeEdgeTypes ?? [],
  );
  const [excludeNames, setExcludeNames] = useState<string[]>(
    initialFilters.excludeNames ?? [],
  );
  const [excludeProps, setExcludeProps] = useState<GraphPropertyFilter[]>(
    initialFilters.excludeProps ?? [],
  );
  const filtersKey = JSON.stringify([
    includeLabels,
    includeEdgeTypes,
    includeNames,
    includeProps,
    excludeLabels,
    excludeEdgeTypes,
    excludeNames,
    excludeProps,
  ]);
  useEffect(() => {
    localStorage.setItem(
      EXPLORER_FILTERS_STORAGE_KEY,
      JSON.stringify({
        includeLabels,
        includeEdgeTypes,
        includeNames,
        includeProps,
        excludeLabels,
        excludeEdgeTypes,
        excludeNames,
        excludeProps,
      }),
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filtersKey]);
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
  const [activeComponent, setActiveComponent] = useState<number | null>(null);
  const [positions, setPositions] = useState<Record<string, NodePosition>>({});
  const [dragging, setDragging] = useState<{
    nodeId: string;
    offsetX: number;
    offsetY: number;
  } | null>(null);
  const svgRef = useRef<SVGSVGElement | null>(null);

  const effectiveRootNodeId = rootNodeId ?? selectedNode?.node.id ?? null;

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
      (graph.components?.[activeComponent]?.nodes ?? []).map(
        (node) => node.id,
      ),
    );
  }, [activeComponent, graph]);

  const loadNeighborhood = useCallback(async () => {
    if (!effectiveRootNodeId) {
      setGraph(null);
      setError(null);
      return;
    }

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
      setGraph(response);
      setPositions(buildInitialPositions(response, effectiveRootNodeId));
      setActiveComponent(null);
    } catch (err) {
      setGraph(null);
      setError(
        err instanceof Error
          ? err.message
          : "Failed to load neighborhood graph",
      );
    } finally {
      setLoading(false);
    }
  }, [depth, effectiveRootNodeId, selectedDatabase]);

  useEffect(() => {
    if (!effectiveRootNodeId) {
      setGraph(null);
      setPositions({});
      setError(null);
      return;
    }
    void loadNeighborhood();
  }, [effectiveRootNodeId, loadNeighborhood]);

  useEffect(() => {
    if (!dragging) {
      return;
    }

    const toSvgPoint = (event: MouseEvent) => {
      const svg = svgRef.current;
      if (!svg) {
        return null;
      }
      const rect = svg.getBoundingClientRect();
      if (rect.width === 0 || rect.height === 0) {
        return null;
      }
      return {
        x: ((event.clientX - rect.left) / rect.width) * VIEW_WIDTH,
        y: ((event.clientY - rect.top) / rect.height) * VIEW_HEIGHT,
      };
    };

    const handleMove = (event: MouseEvent) => {
      const point = toSvgPoint(event);
      if (!point) {
        return;
      }
      setPositions((prev) => ({
        ...prev,
        [dragging.nodeId]: {
          x: Math.max(
            40,
            Math.min(VIEW_WIDTH - 40, point.x - dragging.offsetX),
          ),
          y: Math.max(
            40,
            Math.min(VIEW_HEIGHT - 40, point.y - dragging.offsetY),
          ),
        },
      }));
    };

    const handleUp = () => setDragging(null);

    window.addEventListener("mousemove", handleMove);
    window.addEventListener("mouseup", handleUp);
    return () => {
      window.removeEventListener("mousemove", handleMove);
      window.removeEventListener("mouseup", handleUp);
    };
  }, [dragging]);

  const handleNodeMouseDown = (
    event: React.MouseEvent<Element>,
    nodeId: string,
  ) => {
    const svg = svgRef.current;
    if (!svg) {
      return;
    }
    const rect = svg.getBoundingClientRect();
    const current = positions[nodeId];
    if (!current || rect.width === 0 || rect.height === 0) {
      return;
    }
    const point = {
      x: ((event.clientX - rect.left) / rect.width) * VIEW_WIDTH,
      y: ((event.clientY - rect.top) / rect.height) * VIEW_HEIGHT,
    };
    setDragging({
      nodeId,
      offsetX: point.x - current.x,
      offsetY: point.y - current.y,
    });
  };

  const selectedNodeId = selectedNode?.node.id ?? null;
  const selectedRelationshipId = selectedRelationship?.id ?? null;

  return (
    <div className="flex-1 flex flex-col p-4 gap-4 overflow-hidden">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <h2 className="text-sm font-medium text-white">Neighborhood Graph</h2>
          <p className="text-xs text-norse-silver mt-1">
            Explore connected nodes and click any node or relationship to
            reflect it in details.
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
          {selectedNodeId &&
            effectiveRootNodeId &&
            selectedNodeId !== effectiveRootNodeId && (
              <button
                type="button"
                onClick={() => onRootNodeChange(selectedNodeId)}
                className="px-3 py-1.5 text-sm bg-nornic-primary text-white rounded hover:bg-nornic-secondary"
              >
                Use Selected Node As Root
              </button>
            )}
        </div>
      </div>

      <details className="rounded-lg border border-norse-rune bg-norse-shadow/30 px-3 py-2">
        <summary className="cursor-pointer select-none text-xs text-norse-silver/80 hover:text-white">
          filters (labels · edge types · names · properties)
        </summary>
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
            disabled={loading}
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
            disabled={loading}
            className="rounded border border-norse-rune bg-norse-stone px-3 py-1 text-xs text-norse-silver/80 hover:bg-norse-rune disabled:opacity-40"
          >
            reset
          </button>
        </div>
        <div className="mt-1.5 text-[10px] text-norse-silver/50 font-mono leading-snug">
          names match the symbol name exactly · properties match key +
          optional value, optionally scoped to a label or edge type —
          exclusions may split the graph into disconnected components
        </div>
      </details>

      {!effectiveRootNodeId && (
        <div className="flex-1 flex items-center justify-center rounded-xl border border-dashed border-norse-rune bg-norse-shadow/30 p-6 text-center text-norse-silver">
          <div>
            <p>
              Select a node from search, then click this tab to view
              neighborhood.
            </p>
            <p className="text-sm text-norse-fog mt-2">
              The graph explorer is additive and uses the existing node details
              panel on the right.
            </p>
          </div>
        </div>
      )}

      {effectiveRootNodeId && error && (
        <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {effectiveRootNodeId && !error && (
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
                    {effectiveRootNodeId}
                  </span>
                </span>
                <span className="flex items-center gap-2">
                  <span>
                    {graph.meta.node_count} nodes, {graph.meta.edge_count} edges
                    {(graph.components?.length ?? 0) > 1 &&
                      ` · ${graph.components?.length} components`}
                    {graph.meta.truncated ? " (truncated)" : ""}
                  </span>
                  {(graph.components?.length ?? 0) > 1 && (
                    <select
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
                          component {index + 1} ({component.meta.node_count} nodes)
                        </option>
                      ))}
                    </select>
                  )}
                </span>
              </div>
              <div className="flex-1 min-h-0">
                <svg
                  ref={svgRef}
                  viewBox={`0 0 ${VIEW_WIDTH} ${VIEW_HEIGHT}`}
                  aria-label="Graph neighborhood explorer"
                  className="w-full h-full bg-[radial-gradient(circle_at_top,_rgba(255,255,255,0.04),_transparent_45%),linear-gradient(180deg,rgba(255,255,255,0.02),rgba(0,0,0,0))]"
                >
                  <title>Graph neighborhood explorer</title>
                  <g>
                    {graph.edges.map((edge) => {
                      const source = positions[edge.source];
                      const target = positions[edge.target];
                      if (!source || !target) {
                        return null;
                      }
                      const midX = (source.x + target.x) / 2;
                      const midY = (source.y + target.y) / 2;
                      const isSelected = edge.id === selectedRelationshipId;
                      const isDimmed =
                        activeComponentNodeIds != null &&
                        (!activeComponentNodeIds.has(edge.source) ||
                          !activeComponentNodeIds.has(edge.target));
                      return (
                        <g key={edge.id} opacity={isDimmed ? 0.12 : 1}>
                          <line
                            x1={source.x}
                            y1={source.y}
                            x2={target.x}
                            y2={target.y}
                            stroke={
                              isSelected
                                ? "#f6c177"
                                : "rgba(148, 163, 184, 0.55)"
                            }
                            strokeWidth={isSelected ? 3 : 1.75}
                          />
                          <line
                            x1={source.x}
                            y1={source.y}
                            x2={target.x}
                            y2={target.y}
                            stroke="transparent"
                            strokeWidth={14}
                          />
                          <foreignObject
                            x={midX - 44}
                            y={midY - 18}
                            width={88}
                            height={24}
                          >
                            <button
                              type="button"
                              aria-label={`Select relationship ${edge.type}`}
                              onClick={() => onRelationshipSelect(edge)}
                              className="w-full h-full bg-transparent border-0 cursor-pointer"
                            />
                          </foreignObject>
                          <text
                            x={midX}
                            y={midY - 6}
                            textAnchor="middle"
                            className="fill-norse-silver text-[11px] pointer-events-none select-none"
                          >
                            {edge.type}
                          </text>
                        </g>
                      );
                    })}
                  </g>

                  <g>
                    {graph.nodes.map((node) => {
                      const position = positions[node.id];
                      if (!position) {
                        return null;
                      }
                      const isRoot = node.id === effectiveRootNodeId;
                      const isSelected = node.id === selectedNodeId;
                      const isDimmed =
                        activeComponentNodeIds != null &&
                        !activeComponentNodeIds.has(node.id);
                      const label = getGraphNodeLabel(node);
                      return (
                        <g
                          key={node.id}
                          transform={`translate(${position.x}, ${position.y})`}
                          opacity={isDimmed ? 0.12 : 1}
                          className="cursor-pointer"
                        >
                          <circle
                            r={isRoot ? 24 : 20}
                            fill={
                              isRoot
                                ? "#60a5fa"
                                : isSelected
                                  ? "#34d399"
                                  : "#1f2937"
                            }
                            stroke={
                              isSelected
                                ? "#bbf7d0"
                                : isRoot
                                  ? "#bfdbfe"
                                  : "#64748b"
                            }
                            strokeWidth={isSelected || isRoot ? 3 : 2}
                          />
                          <text
                            y={5}
                            textAnchor="middle"
                            className="fill-white text-[11px] font-medium pointer-events-none select-none"
                          >
                            {node.labels[0] ?? "Node"}
                          </text>
                          <foreignObject
                            x={-76}
                            y={28}
                            width={152}
                            height={52}
                            className="pointer-events-none"
                          >
                            <div className="text-center text-[11px] leading-4 text-norse-silver break-words px-1">
                              {label}
                            </div>
                          </foreignObject>
                          <foreignObject x={-28} y={-28} width={56} height={56}>
                            <button
                              type="button"
                              aria-label={`Select node ${label}`}
                              onMouseDown={(event) =>
                                handleNodeMouseDown(event, node.id)
                              }
                              onClick={(event) => {
                                event.stopPropagation();
                                onNodeSelect(node);
                              }}
                              className="w-full h-full rounded-full bg-transparent border-0 cursor-pointer"
                            />
                          </foreignObject>
                        </g>
                      );
                    })}
                  </g>
                </svg>
              </div>
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
