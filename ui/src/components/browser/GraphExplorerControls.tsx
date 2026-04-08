import { useEffect, useId, useMemo, useState } from "react";
import { FilterX, RefreshCw, RotateCcw, Waypoints, X } from "lucide-react";

import type { BrowserGraphHandoff } from "../../utils/browserUrlState";
import type { GraphContractResponse } from "../../graph/types";
import {
  getDefaultGraphExplorerFilters,
  supportsGraphDepthControl,
  type GraphExplorerFilters,
} from "../../graph/viewModel";
import { type GraphLayoutMode } from "./graphExplorerLayout";
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

function formatTimestamp(value?: string): string {
  if (!value) return "not set";
  return value;
}

export interface GraphExplorerControlsProps {
  handoff: BrowserGraphHandoff | null;
  selectedDatabase: string | null;
  resolvedDatabase: string | null;
  graph: GraphContractResponse | null;
  loading: boolean;
  selectedNodeId?: string | null;
  selectedNodeIds?: string[];
  filters: GraphExplorerFilters;
  depth: number;
  layoutMode: GraphLayoutMode;
  labels: string[];
  relationshipTypes: string[];
  onRefresh: () => void;
  onClearGraph: () => void;
  onUpdateHandoff: (handoff: BrowserGraphHandoff) => void;
  onFiltersChange: (filters: GraphExplorerFilters) => void;
  onDepthChange: (depth: number) => void;
  onLayoutChange: (mode: GraphLayoutMode) => void;
  onManualSeed: (nodeIds: string[]) => void;
}

export function GraphExplorerControls({
  handoff,
  selectedDatabase,
  resolvedDatabase,
  graph,
  loading,
  selectedNodeId,
  selectedNodeIds = [],
  filters,
  depth,
  layoutMode,
  labels,
  relationshipTypes,
  onRefresh,
  onClearGraph,
  onUpdateHandoff,
  onFiltersChange,
  onDepthChange,
  onLayoutChange,
  onManualSeed,
}: GraphExplorerControlsProps) {
  const controlsHeadingId = useId();
  const pathHelpId = useId();
  const temporalHelpId = useId();
  const displayHelpId = useId();

  const [requestMode, setRequestMode] = useState<GraphExplorerRequestMode>("standard");
  const [asOfInput, setAsOfInput] = useState("");
  const [compareToInput, setCompareToInput] = useState("");
  const [pathSourceInput, setPathSourceInput] = useState("");
  const [pathTargetInput, setPathTargetInput] = useState("");
  const [pathDraftWasInferred, setPathDraftWasInferred] = useState(false);
  const [manualSeedInput, setManualSeedInput] = useState("");

  const supportsDepth = handoff ? supportsGraphDepthControl(handoff.mode) : false;
  const supportsTemporalFlow = handoff ? supportsGraphExplorerTemporalFlow(handoff) : false;

  const preferredPathNodeIds = useMemo(
    () => Array.from(new Set([selectedNodeId ?? "", ...selectedNodeIds])).filter(Boolean),
    [selectedNodeId, selectedNodeIds],
  );

  const pathOptions = useMemo(
    () => (graph ? graph.nodes.map((node) => node.id) : []),
    [graph],
  );

  const isPathSubmitDisabled = isGraphPathRequestDisabled(pathSourceInput, pathTargetInput);
  const isTemporalSubmitDisabled = isGraphTemporalRequestDisabled(requestMode, asOfInput);

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

  const applyPathRequest = () => {
    if (!handoff || isPathSubmitDisabled) return;
    onUpdateHandoff(buildGraphExplorerPathHandoff(handoff, pathSourceInput, pathTargetInput));
  };

  const applyTemporalRequest = () => {
    if (!handoff || isTemporalSubmitDisabled) return;
    onUpdateHandoff(buildGraphExplorerTemporalHandoff(handoff, requestMode, asOfInput, compareToInput));
  };

  const applyManualSeedRequest = () => {
    const nodeIds = Array.from(
      new Set(
        manualSeedInput
          .split(",")
          .map((v) => v.trim())
          .filter(Boolean),
      ),
    );
    if (nodeIds.length === 0) return;
    onManualSeed(nodeIds);
  };

  if (!handoff) {
    return (
      <div className="flex-1 min-h-0 overflow-auto p-4">
        <div className="rounded-2xl border border-dashed border-norse-rune bg-norse-shadow/40 p-4">
          <div className="text-sm font-semibold text-white">Graph Explorer</div>
          <p className="mt-2 text-xs text-norse-silver">
            Pick any node in query results or search results, then choose{" "}
            <span className="text-white">Explore neighborhood</span>. Or enter node ids below.
          </p>
          <div className="mt-4 rounded-xl border border-norse-rune bg-norse-night/30 p-3">
            <div className="text-xs font-medium text-white">Manual seed</div>
            <div className="mt-2 flex flex-col gap-2">
              <label className="flex flex-col gap-1 text-xs text-norse-silver">
                Node ids (comma-separated)
                <input
                  type="text"
                  value={manualSeedInput}
                  onChange={(e) => setManualSeedInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") { e.preventDefault(); applyManualSeedRequest(); }
                  }}
                  placeholder="node-1, node-2"
                  className="px-3 py-2 text-sm bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                />
              </label>
              <button
                type="button"
                onClick={applyManualSeedRequest}
                disabled={manualSeedInput.trim().length === 0}
                className="inline-flex items-center gap-2 px-3 py-2 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
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
    <div className="flex-1 min-h-0 overflow-auto">
      {/* Header */}
      <div className="flex items-center justify-between gap-3 px-4 py-3 border-b border-norse-rune bg-norse-shadow/30 sticky top-0 z-10">
        <div>
          <h2 className="text-sm font-semibold text-white">Graph Explorer</h2>
          <p className="text-xs text-norse-silver">
            {handoff.mode} · {resolvedDatabase ?? selectedDatabase ?? "resolving"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onRefresh}
            disabled={loading}
            className="inline-flex items-center gap-1 px-2 py-1 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
            Refresh
          </button>
          <button
            type="button"
            onClick={onClearGraph}
            className="inline-flex items-center gap-1 px-2 py-1 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog"
          >
            <X className="w-3.5 h-3.5" />
            Clear
          </button>
        </div>
      </div>

      <div className="p-4 space-y-3">
        {/* Stats grid */}
        <div className="grid grid-cols-2 gap-2">
          {[
            ["Database", resolvedDatabase ?? "resolving"],
            ["Focus nodes", String(handoff.nodeIds.length || 0)],
            ["Path source", handoff.sourceNodeId ?? "not set"],
            ["Path target", handoff.targetNodeId ?? "not set"],
            ["As of", formatTimestamp(handoff.asOf)],
            ["Compare to", formatTimestamp(handoff.compareTo)],
          ].map(([label, value]) => (
            <div key={label} className="rounded-xl border border-norse-rune bg-norse-shadow/40 p-3">
              <div className="text-xs uppercase tracking-wide text-norse-fog">{label}</div>
              <div className="mt-1 text-xs text-white truncate" title={value}>{value}</div>
            </div>
          ))}
        </div>

        {/* Explorer controls */}
        <section aria-labelledby={controlsHeadingId} className="space-y-3 rounded-2xl border border-norse-rune bg-norse-shadow/40 p-3">
          <div>
            <h3 id={controlsHeadingId} className="text-xs font-medium text-white">Explorer controls</h3>
            <p className="mt-0.5 text-xs text-norse-silver">
              Adjust inputs and filters. Press Enter in a text field to apply.
            </p>
          </div>

          {handoff.mode === "neighborhood" && handoff.nodeIds.length > 0 && (
            <button
              type="button"
              onClick={() => onUpdateHandoff({ mode: "expand", nodeIds: handoff.nodeIds })}
              className="inline-flex items-center gap-2 px-3 py-2 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog"
            >
              <Waypoints className="w-3.5 h-3.5" />
              Expand Focus
            </button>
          )}

          {/* Path request */}
          {pathOptions.length > 0 && (
            <fieldset aria-describedby={pathHelpId} className="rounded-xl border border-norse-rune bg-norse-night/30 p-3">
              <legend className="px-1 text-xs font-medium text-white">Path request</legend>
              <p id={pathHelpId} className="mt-1 text-xs text-norse-silver">
                Choose two node ids to replace the graph with a path request.
                {pathDraftWasInferred ? " Drafts inferred from current focus." : ""}
              </p>
              <div className="mt-3 flex flex-col gap-3">
                <label className="flex flex-col gap-1 text-xs text-norse-silver">
                  Source node id
                  <input
                    type="text"
                    list="graph-path-node-options"
                    value={pathSourceInput}
                    onChange={(e) => { setPathSourceInput(e.target.value); setPathDraftWasInferred(false); }}
                    onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); applyPathRequest(); } }}
                    placeholder="node id"
                    className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                  />
                </label>
                <label className="flex flex-col gap-1 text-xs text-norse-silver">
                  Target node id
                  <input
                    type="text"
                    list="graph-path-node-options"
                    value={pathTargetInput}
                    onChange={(e) => { setPathTargetInput(e.target.value); setPathDraftWasInferred(false); }}
                    onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); applyPathRequest(); } }}
                    placeholder="node id"
                    className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                  />
                </label>
                <button
                  type="button"
                  onClick={applyPathRequest}
                  disabled={isPathSubmitDisabled}
                  className="inline-flex items-center justify-center gap-2 px-3 py-2 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
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

          {/* Temporal request */}
          {supportsTemporalFlow && (
            <fieldset aria-describedby={temporalHelpId} className="rounded-xl border border-norse-rune bg-norse-night/30 p-3">
              <legend className="px-1 text-xs font-medium text-white">Temporal request</legend>
              <p id={temporalHelpId} className="mt-1 text-xs text-norse-silver">
                Switch between live, as-of, and diff modes.
              </p>
              <div className="mt-3 flex flex-col gap-3">
                <label className="flex flex-col gap-1 text-xs text-norse-silver">
                  Request mode
                  <select
                    value={requestMode}
                    onChange={(e) => setRequestMode(e.target.value as GraphExplorerRequestMode)}
                    className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                  >
                    <option value="standard">Live neighborhood</option>
                    <option value="temporal">As-of snapshot</option>
                    <option value="diff">Diff</option>
                  </select>
                </label>
                <label className="flex flex-col gap-1 text-xs text-norse-silver">
                  As-of timestamp
                  <input
                    type="text"
                    value={asOfInput}
                    onChange={(e) => setAsOfInput(e.target.value)}
                    onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); applyTemporalRequest(); } }}
                    placeholder="2026-03-15T00:00:00Z"
                    className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                  />
                </label>
                {requestMode === "diff" && (
                  <label className="flex flex-col gap-1 text-xs text-norse-silver">
                    Compare-to timestamp
                    <input
                      type="text"
                      value={compareToInput}
                      onChange={(e) => setCompareToInput(e.target.value)}
                      onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); applyTemporalRequest(); } }}
                      placeholder="2026-03-20T00:00:00Z"
                      className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white placeholder:text-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                    />
                  </label>
                )}
                <button
                  type="button"
                  onClick={applyTemporalRequest}
                  disabled={isTemporalSubmitDisabled}
                  className="inline-flex items-center justify-center gap-2 px-3 py-2 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
                >
                  Apply request
                </button>
              </div>
            </fieldset>
          )}

          {/* Display filters */}
          <fieldset aria-describedby={displayHelpId} className="rounded-xl border border-norse-rune bg-norse-night/30 p-3">
            <legend className="px-1 text-xs font-medium text-white">Display filters</legend>
            <p id={displayHelpId} className="mt-1 text-xs text-norse-silver">
              Narrow the graph without clearing the explorer.
            </p>
            <div className="mt-3 flex flex-col gap-3">
              {supportsDepth && (
                <label className="flex flex-col gap-1 text-xs text-norse-silver">
                  Neighborhood depth
                  <select
                    value={String(depth)}
                    onChange={(e) => onDepthChange(Number(e.target.value))}
                    className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                  >
                    {[1, 2, 3].map((v) => (
                      <option key={v} value={v}>{v}</option>
                    ))}
                  </select>
                </label>
              )}
              <label className="flex flex-col gap-1 text-xs text-norse-silver">
                Label filter
                <select
                  value={filters.label ?? ""}
                  onChange={(e) => onFiltersChange({ ...filters, label: e.target.value || null })}
                  className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                >
                  <option value="">All labels</option>
                  {labels.map((label) => (
                    <option key={label} value={label}>{label}</option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1 text-xs text-norse-silver">
                Relationship filter
                <select
                  value={filters.relationshipType ?? ""}
                  onChange={(e) => onFiltersChange({ ...filters, relationshipType: e.target.value || null })}
                  className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                >
                  <option value="">All relationships</option>
                  {relationshipTypes.map((rt) => (
                    <option key={rt} value={rt}>{rt}</option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1 text-xs text-norse-silver">
                Layout
                <select
                  value={layoutMode}
                  onChange={(e) => onLayoutChange(e.target.value as GraphLayoutMode)}
                  className="px-3 py-2 text-xs bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                >
                  <option value="radial">Radial</option>
                  <option value="grid">Grid</option>
                  <option value="hierarchy">Hierarchy</option>
                </select>
              </label>
              <button
                type="button"
                onClick={() => onFiltersChange(getDefaultGraphExplorerFilters())}
                className="inline-flex items-center justify-center gap-2 px-3 py-2 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog"
              >
                <FilterX className="w-3.5 h-3.5" />
                Reset filters
              </button>
              {supportsDepth && (
                <button
                  type="button"
                  onClick={() => onDepthChange(1)}
                  disabled={depth === 1}
                  className="inline-flex items-center justify-center gap-2 px-3 py-2 text-xs text-norse-silver border border-norse-rune rounded-lg hover:text-white hover:border-norse-fog disabled:opacity-50"
                >
                  <RotateCcw className="w-3.5 h-3.5" />
                  Reset depth
                </button>
              )}
            </div>
          </fieldset>
        </section>
      </div>
    </div>
  );
}
