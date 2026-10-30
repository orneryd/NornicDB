/**
 * SelectionToolbar - Toolbar for selected nodes (delete, clear)
 * Extracted from Browser.tsx for reusability
 */

import { Trash2 } from "lucide-react";

interface SelectionToolbarProps {
  selectedCount: number;
  onDelete: () => void;
  onClear: () => void;
  deleting?: boolean;
}

export function SelectionToolbar({
  selectedCount,
  onDelete,
  onClear,
  deleting = false,
}: SelectionToolbarProps) {
  const hasSelection = selectedCount > 0;

  return (
    <div className="flex items-center gap-2 p-2 bg-norse-shadow border-b border-norse-rune">
      <span className="text-sm text-norse-silver">
        {selectedCount} selected
      </span>
      <button
        type="button"
        onClick={onDelete}
        disabled={!hasSelection || deleting}
        className="flex items-center gap-1 px-3 py-1 text-sm bg-red-500/20 hover:bg-red-500/30 text-red-400 rounded disabled:opacity-50 disabled:cursor-not-allowed"
      >
        <Trash2 className="w-4 h-4" />
        Delete
      </button>
      <button
        type="button"
        onClick={onClear}
        disabled={!hasSelection}
        className="px-3 py-1 text-sm text-norse-silver hover:text-white disabled:opacity-50 disabled:cursor-not-allowed"
      >
        Clear
      </button>
    </div>
  );
}
