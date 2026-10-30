/**
 * DeleteConfirmModal - Confirmation modal for deleting nodes
 * Extracted from Browser.tsx for reusability
 */

import { Trash2 } from "lucide-react";

interface DeleteConfirmModalProps {
  isOpen: boolean;
  nodeCount: number;
  deleting: boolean;
  onConfirm: () => Promise<void>;
  onCancel: () => void;
}

export function DeleteConfirmModal({
  isOpen,
  nodeCount,
  deleting,
  onConfirm,
  onCancel,
}: DeleteConfirmModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-norse-deep border border-norse-rune rounded-xl p-6 max-w-md mx-4 shadow-2xl">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-red-500/20 rounded-lg">
            <Trash2 className="w-6 h-6 text-red-400" />
          </div>
          <h3 className="text-lg font-semibold text-white">
            Delete {nodeCount} Node{nodeCount !== 1 ? "s" : ""}?
          </h3>
        </div>
        <p className="text-norse-silver mb-2">
          This will{" "}
          <span className="text-red-400 font-medium">
            permanently delete
          </span>{" "}
          the selected node{nodeCount !== 1 ? "s" : ""} and all associated data.
        </p>
        <p className="text-norse-silver text-sm mb-6">
          This action will remove:
          <ul className="list-disc list-inside mt-2 space-y-1">
            <li>
              The node{nodeCount !== 1 ? "s" : ""} and all properties
            </li>
            <li>All associated embeddings</li>
            <li>All connected edges (relationships)</li>
          </ul>
          <span className="text-red-400 font-medium mt-2 block">
            This action cannot be undone.
          </span>
        </p>
        <div className="flex gap-3 justify-end">
          <button
            type="button"
            onClick={onCancel}
            disabled={deleting}
            className="px-4 py-2 rounded-lg text-norse-silver hover:text-white hover:bg-norse-rune transition-all disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={deleting}
            className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 rounded-lg font-medium transition-all disabled:opacity-50"
          >
            {deleting
              ? "Deleting..."
              : `Yes, Delete ${nodeCount} Node${nodeCount !== 1 ? "s" : ""}`}
          </button>
        </div>
      </div>
    </div>
  );
}

