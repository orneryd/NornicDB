/**
 * NodeDetailsPanel - Node details view with properties, labels, embedding
 * Extracted from Browser.tsx for reusability
 */

import { useState } from "react";
import { X, Sparkles, Edit, ChevronRight, Database, Loader2, Waypoints } from "lucide-react";
import { EmbeddingStatus } from "../common/EmbeddingStatus";
import { PropertyEditor } from "../common/PropertyEditor";
import { JsonPreview } from "../common/JsonPreview";
import { isReadOnlyProperty, getNodePreview } from "../../utils/nodeUtils";
import type { SearchResult } from "../../utils/api";

interface NodeDetailsPanelProps {
  selectedNode: SearchResult | null;
  expandedSimilar: {
    nodeId: string;
    results: SearchResult[];
    loading: boolean;
  } | null;
  onClose: () => void;
  onFindSimilar: (nodeId: string) => void;
  onCollapseSimilar: () => void;
  onNodeSelect: (result: SearchResult) => void;
  onExploreNode: (nodeId: string) => void;
  onUpdateProperties: (nodeId: string, props: Record<string, unknown>) => Promise<{ success: boolean; error?: string }>;
  onRefresh: () => void;
}

export function NodeDetailsPanel({
  selectedNode,
  expandedSimilar,
  onClose,
  onFindSimilar,
  onCollapseSimilar,
  onNodeSelect,
  onExploreNode,
  onUpdateProperties,
  onRefresh,
}: NodeDetailsPanelProps) {
  const [editingNodeId, setEditingNodeId] = useState<string | null>(null);
  const [editingProperties, setEditingProperties] = useState<Record<string, unknown>>({});

  if (!selectedNode) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center text-norse-silver">
          <Database className="w-12 h-12 mx-auto mb-3 opacity-30" />
          <p>Select a node to view details</p>
          <p className="text-sm text-norse-fog mt-1">
            Run a query or search to get started
          </p>
        </div>
      </div>
    );
  }

  const isExpanded = expandedSimilar?.nodeId === selectedNode.node.id;

  return (
    <>
      <div className="flex items-center justify-between p-4 border-b border-norse-rune">
        <h2 className="font-medium text-white">Node Details</h2>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => onExploreNode(selectedNode.node.id)}
            className="flex items-center gap-1 px-3 py-1 text-sm rounded border border-norse-rune text-norse-silver transition-colors hover:border-norse-fog hover:text-white"
          >
            <Waypoints className="w-3 h-3" />
            Explore neighborhood
          </button>
          <button
            type="button"
            onClick={() => {
              if (isExpanded) {
                onCollapseSimilar();
              } else {
                onFindSimilar(selectedNode.node.id);
              }
            }}
            className={`flex items-center gap-1 px-3 py-1 text-sm rounded transition-colors ${
              isExpanded
                ? "bg-frost-ice text-norse-night hover:bg-frost-ice/90"
                : "bg-frost-ice/20 text-frost-ice hover:bg-frost-ice/30"
            }`}
          >
            <Sparkles className="w-3 h-3" />
            {isExpanded ? "Hide Similar" : "Find Similar"}
          </button>
          <button
            type="button"
            onClick={onClose}
            className="p-1 hover:bg-norse-rune rounded transition-colors"
          >
            <X className="w-4 h-4 text-norse-silver" />
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-4">
        {/* Labels */}
        <div className="mb-4">
          <h3 className="text-xs font-medium text-norse-silver mb-2">LABELS</h3>
          <div className="flex flex-wrap gap-2">
            {(selectedNode.node.labels as string[]).map((label) => (
              <span
                key={label}
                className="px-3 py-1 bg-frost-ice/20 text-frost-ice rounded-full text-sm"
              >
                {String(label)}
              </span>
            ))}
          </div>
        </div>

        {/* ID */}
        <div className="mb-4">
          <h3 className="text-xs font-medium text-norse-silver mb-2">ID</h3>
          <code className="text-sm text-valhalla-gold font-mono">
            {selectedNode.node.id}
          </code>
        </div>

        {/* Embedding Status */}
        {selectedNode.node.properties.has_embedding === true && (
          <div className="mb-4">
            <h3 className="text-xs font-medium text-norse-silver mb-2">
              EMBEDDING
            </h3>
            <EmbeddingStatus
              embedding={{
                has_embedding: selectedNode.node.properties.has_embedding,
                embedding_dimensions: selectedNode.node.properties.embedding_dimensions,
                embedding_model: selectedNode.node.properties.embedding_model,
                embedded_at: selectedNode.node.properties.embedded_at,
              }}
            />
          </div>
        )}

        {/* Scores */}
        {(selectedNode.rrf_score != null ||
          (selectedNode.vector_rank != null && selectedNode.vector_rank > 0) ||
          (selectedNode.bm25_rank != null && selectedNode.bm25_rank > 0)) && (
          <div className="mb-4 flex gap-4">
            {selectedNode.rrf_score != null && (
              <div>
                <h3 className="text-xs font-medium text-norse-silver mb-1">
                  RRF Score
                </h3>
                <span className="text-nornic-accent">
                  {selectedNode.rrf_score.toFixed(4)}
                </span>
              </div>
            )}
            {selectedNode.vector_rank != null && selectedNode.vector_rank > 0 && (
              <div>
                <h3 className="text-xs font-medium text-norse-silver mb-1">
                  Vector Rank
                </h3>
                <span className="text-frost-ice">#{selectedNode.vector_rank}</span>
              </div>
            )}
            {selectedNode.bm25_rank != null && selectedNode.bm25_rank > 0 && (
              <div>
                <h3 className="text-xs font-medium text-norse-silver mb-1">
                  BM25 Rank
                </h3>
                <span className="text-valhalla-gold">#{selectedNode.bm25_rank}</span>
              </div>
            )}
          </div>
        )}

        {/* Properties */}
        <div className="mb-4">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs font-medium text-norse-silver">PROPERTIES</h3>
            {editingNodeId !== selectedNode.node.id && (
              <button
                type="button"
                onClick={() => {
                  setEditingNodeId(selectedNode.node.id);
                  setEditingProperties({ ...selectedNode.node.properties });
                }}
                className="flex items-center gap-1 px-2 py-1 text-xs bg-nornic-primary/20 hover:bg-nornic-primary/30 text-nornic-primary rounded"
              >
                <Edit className="w-3 h-3" />
                Edit
              </button>
            )}
          </div>

          {editingNodeId === selectedNode.node.id ? (
            <PropertyEditor
              properties={editingProperties}
              onSave={async (updatedProps) => {
                const result = await onUpdateProperties(
                  selectedNode.node.id,
                  updatedProps
                );
                if (result.success) {
                  setEditingNodeId(null);
                  setEditingProperties({});
                  onRefresh();
                } else {
                  alert(`Failed to update: ${result.error}`);
                }
              }}
              onCancel={() => {
                setEditingNodeId(null);
                setEditingProperties({});
              }}
            />
          ) : (
            <div className="space-y-2">
              {Object.entries(selectedNode.node.properties)
                .filter(([key]) => key !== "embedding")
                .map(([key, value]) => {
                  const isReadOnly = isReadOnlyProperty(key);
                  return (
                    <div
                      key={key}
                      className={`bg-norse-stone rounded-lg p-3 ${
                        isReadOnly ? "opacity-75" : ""
                      }`}
                    >
                      <div className="flex items-center gap-2 mb-1">
                        <ChevronRight className="w-3 h-3 text-norse-fog" />
                        <span className="text-sm text-frost-ice font-medium">
                          {key}
                        </span>
                        {isReadOnly && (
                          <span className="text-xs text-norse-fog italic">
                            (read-only)
                          </span>
                        )}
                      </div>
                      <div className="pl-5 break-words overflow-wrap-anywhere">
                        <JsonPreview data={value} expanded />
                      </div>
                    </div>
                  );
                })}
            </div>
          )}
        </div>

        {/* Inline Similar Items Expansion - matches search page style */}
        {isExpanded && expandedSimilar && (
          <div className="ml-4 mt-2 mb-3 border-l-2 border-frost-ice/30 pl-3 animate-in slide-in-from-top-2 duration-200">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-frost-ice flex items-center gap-1">
                <Sparkles className="w-3 h-3" />
                Similar Items ({expandedSimilar.results.length})
              </span>
              {onCollapseSimilar && (
                <button
                  type="button"
                  onClick={onCollapseSimilar}
                  className="text-xs text-norse-fog hover:text-white transition-colors"
                >
                  Close
                </button>
              )}
            </div>

            {expandedSimilar.loading ? (
              <div className="flex items-center gap-2 text-norse-fog text-sm py-2">
                <Loader2 className="w-4 h-4 animate-spin" />
                Finding similar...
              </div>
            ) : expandedSimilar.results.length === 0 ? (
              <p className="text-xs text-norse-fog py-2">No similar items found</p>
            ) : (
              <div className="space-y-1">
                {expandedSimilar.results.map((similar) => (
                  <div
                    key={similar.node.id}
                    className="rounded bg-norse-shadow/50 border border-transparent hover:border-frost-ice/20 transition-colors"
                  >
                    <button
                      type="button"
                      onClick={() => onNodeSelect(similar)}
                      className="w-full text-left p-2"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-1 flex-wrap min-w-0">
                          {similar.node.labels
                            .slice(0, 2)
                            .map((label) => (
                              <span
                                key={label}
                                className="px-1.5 py-0.5 text-xs bg-frost-ice/10 text-frost-ice/80 rounded flex-shrink-0"
                              >
                                {label}
                              </span>
                            ))}
                        </div>
                        <span className="text-xs text-valhalla-gold/70 flex-shrink-0">
                          {similar.score.toFixed(2)}
                        </span>
                      </div>
                      <p className="text-xs text-norse-silver/80 break-words overflow-wrap-anywhere mt-1 min-w-0">
                        {getNodePreview(similar.node.properties)}
                      </p>
                    </button>
                    <div className="px-2 pb-2">
                      <button
                        type="button"
                        onClick={() => onExploreNode(similar.node.id)}
                        className="inline-flex items-center gap-1 rounded border border-norse-rune px-2 py-1 text-[11px] text-norse-silver transition-colors hover:border-norse-fog hover:text-white"
                      >
                        <Waypoints className="h-3 w-3" />
                        Neighborhood
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </>
  );
}

