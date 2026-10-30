/**
 * QueryPanel - Cypher query editor and results
 * Extracted from Browser.tsx for reusability
 */

import { useState, useRef } from "react";
import { Play, History } from "lucide-react";
import { QueryResultsTable } from "./QueryResultsTable";
import { SelectionToolbar } from "../common/SelectionToolbar";
import { QueryAutocomplete } from "./QueryAutocomplete";

interface QueryPanelProps {
  cypherQuery: string;
  setCypherQuery: (query: string) => void;
  queryHistory: string[];
  queryLoading: boolean;
  queryError: string | null;
  cypherResult: {
    results: Array<{
      columns: string[] | null;
      data: Array<{
        row: unknown[];
        meta: unknown[];
      }>;
    }>;
  } | null;
  cypherResults: Array<{
    statement: string;
    status: "pending" | "running" | "success" | "error";
    durationMs?: number;
    result?: {
      results: Array<{
        columns: string[] | null;
        data: Array<{
          row: unknown[];
          meta: unknown[];
        }>;
      }>;
    };
    error?: string;
  }>;
  selectedNodeIds: Set<string>;
  deleteError: string | null;
  onExecute: (continueOnError: boolean) => void;
  onNodeSelect: (nodeData: { id: string; labels: string[]; properties: Record<string, unknown> }) => void;
  onSelectionChange: (nodeIds: string[]) => void;
  onClearSelection: () => void;
  onDeleteClick: () => void;
  deleting?: boolean;
}

export function QueryPanel({
  cypherQuery,
  setCypherQuery,
  queryHistory,
  queryLoading,
  queryError,
  cypherResult,
  cypherResults,
  selectedNodeIds,
  deleteError,
  onExecute,
  onNodeSelect,
  onSelectionChange,
  onClearSelection,
  onDeleteClick,
  deleting = false,
}: QueryPanelProps) {
  const [showHistory, setShowHistory] = useState(false);
  const [autocompleteEnabled, setAutocompleteEnabled] = useState(true);
  const [continueOnError, setContinueOnError] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const runningIndex = cypherResults.findIndex((r) => r.status === "running");
  const completedCount = cypherResults.filter((r) => r.status === "success" || r.status === "error").length;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onExecute(continueOnError);
  };

  const handleSuggestionSelect = (suggestion: string) => {
    setCypherQuery(suggestion);
    // Focus back on textarea
    setTimeout(() => {
      textareaRef.current?.focus();
      // Move cursor to end
      if (textareaRef.current) {
        textareaRef.current.setSelectionRange(
          suggestion.length,
          suggestion.length
        );
      }
    }, 0);
  };

  return (
    <div className="flex-1 flex flex-col p-4 gap-4">
      <form onSubmit={handleSubmit} className="flex flex-col gap-3">
        <div className="relative pb-32">
          <textarea
            ref={textareaRef}
            value={cypherQuery}
            onChange={(e) => setCypherQuery(e.target.value)}
            className="cypher-editor w-full h-32 p-3 resize-none"
            placeholder="MATCH (n) RETURN n LIMIT 25"
            spellCheck={false}
          />
          <div className="absolute top-2 right-2 flex items-center gap-1">
            <button
              type="button"
              onClick={() => setShowHistory(!showHistory)}
              className="p-1.5 rounded hover:bg-norse-rune transition-colors"
              title="Query History"
            >
              <History className="w-4 h-4 text-norse-silver" />
            </button>
            <button
              type="button"
              onClick={() => setAutocompleteEnabled(!autocompleteEnabled)}
              className={`p-1.5 rounded transition-colors ${
                autocompleteEnabled
                  ? "bg-nornic-primary/20 hover:bg-nornic-primary/30"
                  : "hover:bg-norse-rune"
              }`}
              title={autocompleteEnabled ? "Disable AI Autocomplete" : "Enable AI Autocomplete"}
            >
              <svg
                className="w-4 h-4 text-norse-silver"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M13 10V3L4 14h7v7l9-11h-7z"
                />
              </svg>
            </button>
          </div>
          <QueryAutocomplete
            query={cypherQuery}
            onSuggestionSelect={handleSuggestionSelect}
            enabled={autocompleteEnabled && !queryLoading}
            textareaRef={textareaRef}
          />
        </div>

        {showHistory && queryHistory.length > 0 && (
          <div className="bg-norse-stone border border-norse-rune rounded-lg p-2 max-h-40 overflow-y-auto">
            {queryHistory.map((q) => (
              <button
                key={q}
                type="button"
                onClick={() => {
                  setCypherQuery(q);
                  setShowHistory(false);
                }}
                className="w-full text-left px-2 py-1 text-sm text-norse-silver hover:bg-norse-rune rounded truncate"
              >
                {q}
              </button>
            ))}
          </div>
        )}

        <button
          type="submit"
          disabled={queryLoading}
          className="flex items-center justify-center gap-2 px-4 py-2 bg-nornic-primary text-white rounded-lg hover:bg-nornic-secondary disabled:opacity-50 transition-colors"
        >
          <Play className="w-4 h-4" />
          {queryLoading ? "Executing..." : "Run Query"}
        </button>
        <label className="flex items-center gap-2 text-xs text-norse-silver select-none">
          <input
            type="checkbox"
            checked={continueOnError}
            onChange={(e) => setContinueOnError(e.target.checked)}
            className="cursor-pointer"
          />
          Continue on error
        </label>
      </form>

      {queryError && (
        <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg">
          <p className="text-sm text-red-400 font-mono">{queryError}</p>
        </div>
      )}

      {queryLoading && cypherResults.length > 0 && (
        <div className="p-3 bg-amber-500/10 border border-amber-500/30 rounded-lg">
          <p className="text-sm text-amber-300 font-mono">
            Processing {completedCount}/{cypherResults.length}
            {runningIndex >= 0 ? ` (running statement ${runningIndex + 1})` : ""}
          </p>
        </div>
      )}

      {/* Query Results */}
      {((cypherResults.length > 0) || cypherResult?.results[0]) && (
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

          <div className="flex-1 overflow-auto p-2 space-y-3">
            {cypherResults.length > 0 ? (
              cypherResults.map((entry, idx) => (
                <div key={`${idx}:${entry.statement.slice(0, 32)}`} className="border border-norse-rune rounded-lg overflow-hidden bg-norse-stone/30">
                  <div className="px-3 py-2 border-b border-norse-rune bg-norse-shadow/50">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-xs font-mono text-norse-silver truncate">
                        #{idx + 1}: {entry.statement}
                      </p>
                      <div className="flex items-center gap-2 text-xs">
                        <span className={`px-2 py-0.5 rounded ${
                          entry.status === "success"
                            ? "bg-green-500/20 text-green-300"
                            : entry.status === "error"
                            ? "bg-red-500/20 text-red-300"
                            : entry.status === "running"
                            ? "bg-amber-500/20 text-amber-300"
                            : "bg-norse-rune text-norse-silver"
                        }`}>
                          {entry.status}
                        </span>
                        {entry.durationMs != null && (
                          <span className="text-norse-silver">{entry.durationMs} ms</span>
                        )}
                      </div>
                    </div>
                  </div>

                  {entry.error && (
                    <div className="p-3 bg-red-500/10 border-b border-red-500/30">
                      <p className="text-sm text-red-400 font-mono">{entry.error}</p>
                    </div>
                  )}

                  {(() => {
                    const entryResult = entry.result;
                    if (!entryResult?.results?.[0]) {
                      return entry.status === "running" || entry.status === "pending" ? (
                        <div className="p-3 text-sm text-norse-silver font-mono">
                          {entry.status === "running" ? "Running..." : "Queued..."}
                        </div>
                      ) : (
                        <div className="p-3 text-sm text-norse-silver font-mono">No result rows</div>
                      );
                    }
                    return (
                    <QueryResultsTable
                      cypherResult={entryResult}
                      selectedNodeIds={selectedNodeIds}
                      onNodeSelect={onNodeSelect}
                      onSelectionChange={onSelectionChange}
                    />
                    );
                  })()}
                </div>
              ))
            ) : (
              cypherResult?.results[0] && (
                <QueryResultsTable
                  cypherResult={cypherResult}
                  selectedNodeIds={selectedNodeIds}
                  onNodeSelect={onNodeSelect}
                  onSelectionChange={onSelectionChange}
                />
              )
            )}
          </div>
        </div>
      )}
    </div>
  );
}
