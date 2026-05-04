/**
 * QueryResultsTable - Table view for Cypher query results
 * Extracted from Browser.tsx for reusability
 */

import { useMemo } from "react";
import { UiGrid } from "@ornery/ui-grid-react";
import type { GridCellTemplateContext, GridColumnDef, GridOptions, GridRecord } from "@ornery/ui-grid-core";
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

  const result = cypherResult.results[0];

  const { allNodeIds, allSelected, columnDefs, gridData } = useMemo(() => {
    const nextColumns = result.columns ?? [];
    const nextAllNodeIds = getAllNodeIdsFromQueryResults(cypherResult);
    const nextAllSelected =
      nextAllNodeIds.length > 0 &&
      nextAllNodeIds.every((id) => selectedNodeIds.has(id));

    const nextGridData: GridRecord[] = result.data.map((row, rowIndex) => {
      let nodeId: string | null = null;
      let nodeData: { id: string; labels: string[]; properties: Record<string, unknown> } | null = null;

      for (const cell of row.row) {
        if (cell && typeof cell === "object") {
          const cellObj = cell as Record<string, unknown>;
          if (cellObj.elementId || cellObj.id || cellObj._nodeId) {
            const extracted = extractNodeFromResult(cellObj);
            if (extracted) {
              nodeId = extracted.id;
              nodeData = extracted;
              break;
            }
          }
        }
      }

      const record: GridRecord = {
        __gridId: `result-row-${rowIndex}-${nodeId ?? "no-node"}`,
        __nodeId: nodeId,
        __nodeData: nodeData,
      };

      nextColumns.forEach((column, index) => {
        record[column] = row.row[index];
      });

      return record;
    });

    const nextColumnDefs: GridColumnDef[] = [
      {
        name: "__select__",
        displayName: "Select",
        width: "56px",
        enableSorting: false,
        enableFiltering: false,
      },
      ...nextColumns.map((column) => ({
        name: column,
        displayName: column,
        field: column,
        type: "object" as const,
        width: "minmax(12rem, 1fr)",
      })),
    ];

    return {
      allNodeIds: nextAllNodeIds,
      allSelected: nextAllSelected,
      columnDefs: nextColumnDefs,
      gridData: nextGridData,
    };
  }, [cypherResult, result.columns, result.data, selectedNodeIds]);

  const gridOptions = useMemo<GridOptions>(
    () => ({
      id: "query-results-grid",
      data: gridData,
      columnDefs,
      rowIdentity: (row) => String(row.__gridId),
      enableSorting: true,
      enableFiltering: false,
      enableCellEdit: false,
      viewportHeight: 520,
      emptyMessage: "No rows returned",
    }),
    [columnDefs, gridData],
  );

  const renderCell = (ctx: GridCellTemplateContext) => {
    const row = ctx.row as GridRecord & {
      __nodeId?: string | null;
      __nodeData?: { id: string; labels: string[]; properties: Record<string, unknown> } | null;
    };
    const nodeId = row.__nodeId ?? null;
    const nodeData = row.__nodeData ?? null;

    if (ctx.column.name === "__select__") {
      return (
        <div className="flex items-center justify-center h-full">
          <input
            type="checkbox"
            checked={Boolean(nodeId && selectedNodeIds.has(nodeId))}
            onChange={(event) => {
              event.stopPropagation();
              if (nodeId) {
                onToggleSelect(nodeId);
              }
            }}
            onClick={(event) => event.stopPropagation()}
            disabled={!nodeId}
            className="cursor-pointer"
          />
        </div>
      );
    }

    const value = ctx.value;

    if (value && typeof value === "object") {
      return (
        <div className="font-mono text-xs py-1 space-y-1">
          {nodeData ? (
            <button
              type="button"
              onClick={() => onNodeSelect(nodeData)}
              className="text-[11px] uppercase tracking-wide text-nornic-primary hover:text-white"
            >
              Open
            </button>
          ) : null}
          <ExpandableCell data={value} />
        </div>
      );
    }

    const displayValue =
      value === null
        ? "null"
        : value === undefined || value === ""
          ? "-"
          : String(value);

    const clickable = Boolean(nodeData);

    return (
      <button
        type="button"
        onClick={() => {
          if (nodeData) {
            onNodeSelect(nodeData);
          }
        }}
        disabled={!clickable}
        className={`w-full text-left font-mono text-xs py-1 ${clickable ? "cursor-pointer hover:text-white" : "cursor-default"}`}
      >
        {displayValue}
      </button>
    );
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="flex items-center justify-between gap-3 px-2 pb-2">
        <span className="text-xs text-norse-silver">
          {selectedNodeIds.size} selected
        </span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => onSelectAll(allNodeIds)}
            disabled={allNodeIds.length === 0 || allSelected}
            className="text-xs text-nornic-primary disabled:text-norse-fog disabled:cursor-not-allowed"
          >
            Select all
          </button>
          <button
            type="button"
            onClick={onClearSelection}
            disabled={selectedNodeIds.size === 0}
            className="text-xs text-norse-silver disabled:text-norse-fog disabled:cursor-not-allowed"
          >
            Clear
          </button>
        </div>
      </div>
      <div className="flex-1 overflow-hidden nornic-grid">
        <UiGrid options={gridOptions} cellRenderer={renderCell} />
      </div>
      <p className="text-xs text-norse-silver mt-2 px-2">
        {result.data.length} row(s) returned
      </p>
    </div>
  );
}
