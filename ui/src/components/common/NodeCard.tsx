/**
 * NodeCard - Reusable node card component for search results
 * Extracted from Browser.tsx for reusability
 */

import { Sparkles, Loader2 } from "lucide-react";
import { getNodePreview } from "../../utils/nodeUtils";
import type { SearchResult } from "../../utils/api";

interface NodeCardProps {
  result: SearchResult;
  isSelected: boolean;
  isActive: boolean;
  expandedSimilar?: {
    nodeId: string;
    results: SearchResult[];
    loading: boolean;
  } | null;
  onSelect: () => void;
  onToggleSelect: () => void;
  onFindSimilar?: () => void; // Reserved for future use
  onCollapseSimilar?: () => void;
}

export function NodeCard({
  result,
  isSelected,
  isActive,
  expandedSimilar,
  onSelect,
  onToggleSelect,
  onFindSimilar: _onFindSimilar, // Reserved for future use - prefix with _ to mark as intentionally unused
  onCollapseSimilar,
}: NodeCardProps) {
  const isExpanded = expandedSimilar?.nodeId === result.node.id;

  return (
    <div>
      {/* Main result card */}
      <div
        className={`w-full text-left p-3 rounded-lg border transition-colors ${
          isActive
            ? "bg-nornic-primary/20 border-nornic-primary"
            : isSelected
            ? "bg-nornic-primary/10 border-nornic-primary/50"
            : "bg-norse-stone border-norse-rune hover:border-norse-fog"
        }`}
      >
        <div className="flex items-start gap-2">
          <input
            type="checkbox"
            checked={isSelected}
            onChange={(e) => {
              e.stopPropagation();
              onToggleSelect();
            }}
            onClick={(e) => e.stopPropagation()}
            className="mt-1 cursor-pointer"
          />
          <button
            type="button"
            onClick={onSelect}
            className="flex-1 text-left min-w-0"
          >
            <div className="flex items-center justify-between mb-1 gap-2">
              <div className="flex items-center gap-2 flex-wrap min-w-0">
                {result.node.labels.map((label) => (
                  <span
                    key={label}
                    className="px-2 py-0.5 text-xs bg-frost-ice/20 text-frost-ice rounded flex-shrink-0"
                  >
                    {label}
                  </span>
                ))}
              </div>
              <span className="text-xs text-valhalla-gold flex-shrink-0">
                Score: {result.score.toFixed(2)}
              </span>
            </div>
            <p className="text-sm text-norse-silver break-words overflow-wrap-anywhere min-w-0">
              {getNodePreview(result.node.properties)}
            </p>
          </button>
        </div>
      </div>

      {/* Inline Similar Expansion */}
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
                <button
                  type="button"
                  key={similar.node.id}
                  onClick={onSelect}
                  className="w-full text-left p-2 rounded bg-norse-shadow/50 hover:bg-norse-shadow border border-transparent hover:border-frost-ice/20 transition-colors"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-1">
                      {similar.node.labels
                        .slice(0, 2)
                        .map((label) => (
                          <span
                            key={label}
                            className="px-1.5 py-0.5 text-xs bg-frost-ice/10 text-frost-ice/80 rounded"
                          >
                            {label}
                          </span>
                        ))}
                    </div>
                    <span className="text-xs text-valhalla-gold/70">
                      {similar.score.toFixed(2)}
                    </span>
                  </div>
                  <p className="text-xs text-norse-silver/80 break-words overflow-wrap-anywhere mt-1">
                    {getNodePreview(similar.node.properties)}
                  </p>
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

