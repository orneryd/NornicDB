/**
 * QueryResultsTable - Table view for Cypher query results
 * Extracted from Browser.tsx for reusability
 */

import { ExpandableCell } from "../common/ExpandableCell";
import { extractNodeFromResult, getAllNodeIdsFromQueryResults } from "../../utils/nodeUtils";


interface QueryResultsTableProps {
  cypherResult: {
    results: Array<{
      columns: string[] | null;
      data: Array<{
        row: unknown[];
        meta: unknown[];
      }>;
    }>;
  } | null;
  selectedNodeIds: Set<string>;
  onNodeSelect: (nodeData: { id: string; labels: string[]; properties: Record<string, unknown> }) => void;
  onToggleSelect: (nodeId: string) => void;
  onSelectAll: (nodeIds: string[]) => void;
  onClearSelection: () => void;
}

export function QueryResultsTable({
  cypherResult,
  selectedNodeIds,
  onNodeSelect,
  onToggleSelect,
  onSelectAll,
  onClearSelection,
}: QueryResultsTableProps) {
  if (!cypherResult || !cypherResult.results[0]) {
    return null;
  }

  const allNodeIds = getAllNodeIdsFromQueryResults(cypherResult);
  const allSelected = allNodeIds.length > 0 && allNodeIds.every(id => selectedNodeIds.has(id));
  const columns = cypherResult.results[0].columns ?? [];

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="flex-1 overflow-auto">
        <table className="result-table">
          <thead>
            <tr>
              <th className="w-12">
                <input
                  type="checkbox"
                  checked={allSelected}
                  onChange={(e) => {
                    if (e.target.checked) {
                      onSelectAll(allNodeIds);
                    } else {
                      onClearSelection();
                    }
                  }}
                  onClick={(e) => e.stopPropagation()}
                  className="cursor-pointer"
                />
              </th>
              {columns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cypherResult.results[0].data.map((row, rowIndex) => {
              // Extract node ID from first node-like object in row
              let nodeId: string | null = null;
              for (const cell of row.row) {
                if (cell && typeof cell === "object") {
                  const cellObj = cell as Record<string, unknown>;
                  if (cellObj.elementId || cellObj.id || cellObj._nodeId) {
                    const nodeData = extractNodeFromResult(cellObj);
                    if (nodeData) {
                      nodeId = nodeData.id;
                      break;
                    }
                  }
                }
              }

              const handleRowClick = () => {
                if (nodeId) {
                  // Find first node-like object in row and select it
                  for (const cell of row.row) {
                    if (cell && typeof cell === "object") {
                      const cellObj = cell as Record<string, unknown>;
                      if (
                        cellObj.elementId ||
                        cellObj.id ||
                        cellObj._nodeId
                      ) {
                        const nodeData = extractNodeFromResult(cellObj);
                        if (nodeData) {
                          onNodeSelect(nodeData);
                          break;
                        }
                      }
                    }
                  }
                }
              };

              return (
                <tr
                  key={`row-${rowIndex}-${nodeId || 'no-node'}`}
                  onClick={handleRowClick}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      handleRowClick();
                    }
                  }}
                  tabIndex={0}
                  className={`cursor-pointer hover:bg-nornic-primary/10 ${
                    nodeId && selectedNodeIds.has(nodeId)
                      ? "bg-nornic-primary/20"
                      : ""
                  }`}
                >
                  <td
                    onClick={(e) => e.stopPropagation()}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.stopPropagation();
                      }
                    }}
                    className="align-top"
                  >
                    {nodeId && (
                      <div className="flex flex-col items-start gap-2">
                        <input
                          type="checkbox"
                          checked={selectedNodeIds.has(nodeId)}
                          onChange={(e) => {
                            e.stopPropagation();
                            onToggleSelect(nodeId);
                          }}
                          className="cursor-pointer"
                        />
                      </div>
                    )}
                  </td>
                  {row.row.map((cell, cellIndex) => {
                    const cellKey = typeof cell === "object" && cell !== null && "elementId" in cell
                      ? String((cell as Record<string, unknown>).elementId || cellIndex)
                      : String(cell) || cellIndex;
                    return (
                    <td key={cellKey} className="font-mono text-xs">
                      <ExpandableCell data={cell} />
                    </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-norse-silver mt-2 px-2">
        {cypherResult.results[0].data.length} row(s) returned
      </p>
    </div>
  );
}
