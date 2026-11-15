/**
 * NodeCard - Reusable node card component for search results
 * Extracted from Browser.tsx for reusability
 */

import { getNodePreview } from "../../utils/nodeUtils";
import type { SearchResult } from "../../utils/api";

interface NodeCardProps {
  result: SearchResult;
  isSelected: boolean;
  isActive: boolean;
  onSelect: () => void;
  onToggleSelect: () => void;
}

export function NodeCard({
  result,
  isSelected,
  isActive,
  onSelect,
  onToggleSelect,
}: NodeCardProps) {

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
            aria-label={`Select ${getNodePreview(result.node.properties) || result.node.id}`}
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


    </div>
  );
}

