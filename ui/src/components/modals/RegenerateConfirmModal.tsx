/**
 * RegenerateConfirmModal - Confirmation modal for regenerating embeddings
 * Extracted from Browser.tsx for reusability
 */

import { Zap } from "lucide-react";
import {Modal} from '../common/Modal';

interface RegenerateConfirmModalProps {
  isOpen: boolean;
  totalEmbeddings: number;
  database?: string;
  onConfirm: () => void;
  onCancel: () => void;
}

export function RegenerateConfirmModal({
  isOpen,
  totalEmbeddings,
  database,
  onConfirm,
  onCancel,
}: RegenerateConfirmModalProps) {
  if (!isOpen) return null;

  return (
    <Modal isOpen={isOpen} onClose={onCancel} title="Regenerate embeddings">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-red-500/20 rounded-lg">
            <Zap className="w-6 h-6 text-red-400" />
          </div>
          <h3 className="text-lg font-semibold text-white">
            {database ? `Regenerate embeddings in ${database}?` : 'Regenerate all embeddings?'}
          </h3>
        </div>
        <p className="text-norse-silver mb-2">
          This will{" "}
          <span className="text-red-400 font-medium">
            {database ? `clear managed embeddings in ${database}` : 'clear all existing embeddings'}
          </span>{" "}
          and regenerate them from scratch.
        </p>
        {!database && <p className="text-norse-silver text-sm mb-6">
          This operation runs in the background. You have{" "}
          <span className="text-valhalla-gold">
            {totalEmbeddings.toLocaleString()}
          </span>{" "}
          embeddings that will be regenerated.
        </p>}
        <div className="flex gap-3 justify-end">
          <button
            type="button"
            onClick={onCancel}
            autoFocus
            data-autofocus
            className="px-4 py-2 rounded-lg text-norse-silver hover:text-white hover:bg-norse-rune transition-all"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={onConfirm}
            className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 rounded-lg font-medium transition-all"
          >
            {database ? 'Regenerate database embeddings' : 'Yes, Regenerate All'}
          </button>
        </div>
    </Modal>
  );
}

