/**
 * RegenerateConfirmModal - Confirmation modal for regenerating embeddings.
 *
 * Lists the databases with a checkbox each. The default database starts selected and
 * the others do not; "Select all" selects every database, or clears the selection when
 * all are already selected. Confirming clears and regenerates the embeddings of the
 * selected databases only.
 */

import { useEffect, useRef, useState } from "react";
import { Zap } from "lucide-react";

interface RegenerateConfirmModalProps {
  isOpen: boolean;
  totalEmbeddings: number;
  /** Databases that can be regenerated (system excluded). */
  databases: string[];
  /** Pre-selected when the modal opens. */
  defaultDatabase: string;
  onConfirm: (databases: string[]) => void;
  onCancel: () => void;
}

export function RegenerateConfirmModal({
  isOpen,
  totalEmbeddings,
  databases,
  defaultDatabase,
  onConfirm,
  onCancel,
}: RegenerateConfirmModalProps) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const selectAllRef = useRef<HTMLInputElement>(null);

  // Every time the modal opens, start from "the default database only".
  useEffect(() => {
    if (isOpen) {
      setSelected(
        new Set(databases.includes(defaultDatabase) ? [defaultDatabase] : []),
      );
    }
  }, [isOpen, databases, defaultDatabase]);

  const allSelected =
    databases.length > 0 && databases.every((name) => selected.has(name));
  const someSelected = selected.size > 0 && !allSelected;

  // A partially selected list shows "Select all" as indeterminate.
  useEffect(() => {
    if (selectAllRef.current) {
      selectAllRef.current.indeterminate = someSelected;
    }
  }, [someSelected]);

  if (!isOpen) return null;

  const toggle = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) {
        next.delete(name);
      } else {
        next.add(name);
      }
      return next;
    });
  };

  const toggleAll = () => {
    setSelected(allSelected ? new Set() : new Set(databases));
  };

  // Keep the list order for the request.
  const chosen = databases.filter((name) => selected.has(name));

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="regenerate-embeddings-title"
        className="bg-norse-deep border border-norse-rune rounded-xl p-6 max-w-md w-full mx-4 shadow-2xl"
      >
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 bg-red-500/20 rounded-lg">
            <Zap className="w-6 h-6 text-red-400" />
          </div>
          <h3
            id="regenerate-embeddings-title"
            className="text-lg font-semibold text-white"
          >
            Regenerate Embeddings?
          </h3>
        </div>
        <p className="text-norse-silver mb-2">
          This will{" "}
          <span className="text-red-400 font-medium">
            clear the existing embeddings
          </span>{" "}
          of the selected databases and regenerate them from scratch.
        </p>
        <p className="text-norse-silver text-sm mb-4">
          This operation runs in the background. The server holds{" "}
          <span className="text-valhalla-gold">
            {totalEmbeddings.toLocaleString()}
          </span>{" "}
          embeddings in all.
        </p>

        <fieldset className="mb-6 border border-norse-rune rounded-lg">
          <legend className="sr-only">Databases to regenerate</legend>
          <label className="flex items-center gap-2 px-3 py-2 border-b border-norse-rune text-sm text-white select-none cursor-pointer">
            <input
              ref={selectAllRef}
              type="checkbox"
              checked={allSelected}
              onChange={toggleAll}
              disabled={databases.length === 0}
              className="cursor-pointer"
              aria-label="Select all databases"
            />
            Select all
          </label>
          <div className="max-h-56 overflow-y-auto py-1">
            {databases.length === 0 ? (
              <p className="px-3 py-2 text-sm text-norse-silver">
                No databases found.
              </p>
            ) : (
              databases.map((name) => (
                <label
                  key={name}
                  className="flex items-center gap-2 px-3 py-1.5 text-sm text-norse-silver hover:text-white select-none cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={selected.has(name)}
                    onChange={() => toggle(name)}
                    className="cursor-pointer"
                  />
                  <span className="font-mono">{name}</span>
                  {name === defaultDatabase && (
                    <span className="text-xs text-valhalla-gold">default</span>
                  )}
                </label>
              ))
            )}
          </div>
        </fieldset>

        <div className="flex gap-3 justify-end">
          <button
            type="button"
            onClick={onCancel}
            className="px-4 py-2 rounded-lg text-norse-silver hover:text-white hover:bg-norse-rune transition-all"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => onConfirm(chosen)}
            disabled={chosen.length === 0}
            className="px-4 py-2 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 rounded-lg font-medium transition-all disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {chosen.length === 1
              ? "Regenerate 1 database"
              : `Regenerate ${chosen.length} databases`}
          </button>
        </div>
      </div>
    </div>
  );
}
