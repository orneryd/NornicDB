/**
 * SearchPanel - Semantic search input and results
 * Extracted from Browser.tsx for reusability
 */

import { Search } from "lucide-react";
import { NodeCard } from "../common/NodeCard";
import { SelectionToolbar } from "../common/SelectionToolbar";
import type { SearchResult } from "../../utils/api";

interface SearchPanelProps {
  searchQuery: string;
  setSearchQuery: (query: string) => void;
  searchLoading: boolean;
  searchResults: SearchResult[];
  searchError: string | null;
  /** Current database name for semantic search (empty = default). Shown as context. */
  selectedDatabase: string;
  selectedNodeIds: Set<string>;
  selectedNode: SearchResult | null;
  deleteError: string | null;
  expandedSimilar: {
    nodeId: string;
    results: SearchResult[];
    loading: boolean;
  } | null;
  onExecute: () => void;
  onNodeSelect: (result: SearchResult) => void;
  onToggleSelect: (nodeId: string) => void;
  onSelectAll: (nodeIds: string[]) => void;
  onClearSelection: () => void;
  onDeleteClick: () => void;
  onFindSimilar: (nodeId: string) => void;
  onCollapseSimilar: () => void;
  deleting?: boolean;
}

export function SearchPanel({
  searchQuery,
  setSearchQuery,
  searchLoading,
  searchResults,
  searchError,
  selectedDatabase,
  selectedNodeIds,
  selectedNode,
  deleteError,
  expandedSimilar,
  onExecute,
  onNodeSelect,
  onToggleSelect,
  onSelectAll,
  onClearSelection,
  onDeleteClick,
  onFindSimilar,
  onCollapseSimilar,
  deleting = false,
}: SearchPanelProps) {
  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onExecute();
  };

  const allSelected =
    searchResults.length > 0 &&
    searchResults.every((r) => selectedNodeIds.has(r.node.id));

  const searchTargetLabel = selectedDatabase ? selectedDatabase : "default";

  return (
    <div className="flex-1 flex flex-col p-4 gap-4">
      <p className="text-xs text-norse-silver">
        Searching in: <span className="font-medium text-norse-silver">{searchTargetLabel}</span>
      </p>
      <form onSubmit={handleSubmit} className="flex gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-norse-fog" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white placeholder-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary"
            placeholder="Search nodes semantically..."
          />
        </div>
        <button
          type="submit"
          disabled={searchLoading}
          className="px-4 py-2 bg-nornic-primary text-white rounded-lg hover:bg-nornic-secondary disabled:opacity-50 transition-colors"
        >
          {searchLoading ? "..." : "Search"}
        </button>
      </form>

      {searchError && (
        <div className="p-3 bg-amber-500/10 border border-amber-500/30 rounded-lg">
          <p className="text-sm text-amber-300">{searchError}</p>
        </div>
      )}

      {/* Search Results */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <SelectionToolbar
          selectedCount={selectedNodeIds.size}
          onDelete={onDeleteClick}
          onClear={onClearSelection}
          deleting={deleting}
        />

        {/* Delete Error Display */}
        {deleteError && (
          <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg m-2">
            <p className="text-sm text-red-400 font-mono">{deleteError}</p>
          </div>
        )}

        <div className="flex-1 overflow-auto space-y-2 p-2">
          {searchResults.length > 0 && (
            <div className="mb-2">
              <input
                type="checkbox"
                checked={allSelected}
                onChange={(e) => {
                  if (e.target.checked) {
                    onSelectAll(searchResults.map((r) => r.node.id));
                  } else {
                    onClearSelection();
                  }
                }}
                className="cursor-pointer mr-2"
              />
              <span className="text-xs text-norse-silver">Select all</span>
            </div>
          )}

          {searchResults.map((result) => (
            <NodeCard
              key={result.node.id}
              result={result}
              isSelected={selectedNodeIds.has(result.node.id)}
              isActive={selectedNode?.node.id === result.node.id}
              expandedSimilar={
                expandedSimilar?.nodeId === result.node.id
                  ? expandedSimilar
                  : undefined
              }
              onSelect={() => onNodeSelect(result)}
              onToggleSelect={() => onToggleSelect(result.node.id)}
              onFindSimilar={() => onFindSimilar(result.node.id)}
              onCollapseSimilar={onCollapseSimilar}
            />
          ))}

          {searchResults.length === 0 &&
            searchQuery &&
            !searchLoading && (
              <p className="text-center text-norse-silver py-8">
                No results found
              </p>
            )}
        </div>
      </div>
    </div>
  );
}

