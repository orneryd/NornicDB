import {useViewState} from '../../utils/useUrlState';
/**
 * QueryPanel - Cypher query editor and results
 * Extracted from Browser.tsx for reusability
 */

import { useRef,useId } from "react";
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
  const editorId=useId();
  const [showHistory, setShowHistory] = useViewState('history', false);
  const [autocompleteEnabled, setAutocompleteEnabled] = useViewState('autocomplete', true);
  const [sampleLoaded,setSampleLoaded] = useViewState('sampleLoaded',false);
  const [optionsOpen,setOptionsOpen] = useViewState('executionOptions',false);
  const [continueOnError, setContinueOnError] = useViewState('continueOnError', false);
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
    <div className="workspace-query flex-1 flex flex-col p-4 gap-4">
      <form onSubmit={handleSubmit} className="flex flex-col gap-3">
        <div className="editor-heading"><label htmlFor={editorId} className="text-sm text-norse-silver">Cypher query</label>
            <div className="editor-tools">
              <button type="button" onClick={() => setShowHistory(!showHistory)} aria-expanded={showHistory}><History className="w-4 h-4" /> History</button>
              <button type="button" onClick={() => setAutocompleteEnabled(!autocompleteEnabled)} aria-pressed={autocompleteEnabled}>AI autocomplete {autocompleteEnabled ? 'on' : 'off'}</button>
            </div>
          </div>
        <div className="relative">
          <textarea
            id={editorId}
            ref={textareaRef}
            value={cypherQuery}
            onChange={(e) => {setSampleLoaded(false);setCypherQuery(e.target.value);}}
            className="cypher-editor w-full h-32 p-3 resize-none"
            placeholder="MATCH (n) RETURN n LIMIT 25"
            spellCheck={false}
            onKeyDown={event => {if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {event.preventDefault(); if (!queryLoading) onExecute(continueOnError);}}}
          />
          <QueryAutocomplete
            query={cypherQuery}
            onSuggestionSelect={handleSuggestionSelect}
            enabled={autocompleteEnabled && !queryLoading}
            textareaRef={textareaRef}
          />
        </div>

        {showHistory && queryHistory.length===0 && <p role="status" className="text-sm text-norse-silver">No queries in this session yet.</p>}
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

        <div className="query-command-bar">
        <button
          type="submit"
          disabled={queryLoading}
          className="flex items-center justify-center gap-2 px-4 py-2 bg-nornic-primary text-white rounded-lg hover:bg-nornic-secondary disabled:opacity-50 transition-colors"
        >
          <Play className="w-4 h-4" />
          {queryLoading ? "Executing..." : "Run Query"}
        </button>
        <span className="shortcut-hint">Ctrl / ⌘ + Enter</span>
        <details className="query-options" open={optionsOpen} onToggle={event => {if(event.currentTarget.open !== optionsOpen) setOptionsOpen(event.currentTarget.open);}}><summary>Execution options</summary>
        <label className="flex items-center gap-2 text-xs text-norse-silver select-none">
          <input
            type="checkbox"
            checked={continueOnError}
            onChange={(e) => setContinueOnError(e.target.checked)}
            className="cursor-pointer"
          />
          Continue on error
        </label>
        </details>
        </div>
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

      {!(cypherResults.length > 0 || cypherResult?.results[0]) && !queryLoading && !queryError && <section className="workspace-ready" aria-label="Query guidance">
        <h2>Ready when you are</h2><p>Run a Cypher query to see rows here. Select a result to inspect its properties or explore it in the graph.</p>
        <button type="button" onClick={() => {handleSuggestionSelect('MATCH (n) RETURN n LIMIT 25');setSampleLoaded(true);}}>Use a sample query <span aria-hidden="true">→</span></button>{sampleLoaded&&<p role="status" className="sample-loaded">Sample query loaded. Review it in the editor, then choose Run Query.</p>}
      </section>}
      {(cypherResults.length > 0 || cypherResult?.results[0]) && <div className="results-heading"><h2>Results</h2><span>Select a node to inspect its properties</span></div>}
      {/* Query Results */}
      {((cypherResults.length > 0) || cypherResult?.results[0]) && (
        <div className="flex-1 flex flex-col overflow-hidden">
          {selectedNodeIds.size > 0 && <SelectionToolbar
            selectedCount={selectedNodeIds.size}
            onDelete={onDeleteClick}
            onClear={onClearSelection}
            deleting={deleting}
          />}

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
