/**
/**
 * QueryResultsTable - Table view for Cypher query results
 * Extracted from Browser.tsx for reusability
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { UiGrid } from "@ornery/ui-grid-react";
import type {
  GridCellTemplateContext,
  GridColumnDef,
  GridOptions,
  GridRecord,
  UiGridApi,
} from "@ornery/ui-grid-core";
import { ExpandableCell } from "../common/ExpandableCell";
import { extractNodeFromResult } from "../../utils/nodeUtils";

type QueryResultNodeData = {
  id: string;
  labels: string[];
  properties: Record<string, unknown>;
};

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
  onNodeSelect: (nodeData: QueryResultNodeData) => void;
  onSelectionChange: (nodeIds: string[]) => void;
}

export function QueryResultsTable({
  cypherResult,
  selectedNodeIds,
  onNodeSelect,
  onSelectionChange,
}: QueryResultsTableProps) {
  if (!cypherResult || !cypherResult.results[0]) {
    return null;
  }

  const result = cypherResult.results[0];

  const [gridApi, setGridApi] = useState<UiGridApi | null>(null);
  const isSyncingSelectionRef = useRef(false);

  const { columnDefs, gridData } = useMemo(() => {
    const nextColumns = result.columns ?? [];

    const nextGridData: GridRecord[] = result.data.map((row, rowIndex) => {
      let nodeId: string | null = null;
      let nodeData: QueryResultNodeData | null = null;

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
        name: "nornicSelect",
        displayName: "Select",
        field: "nornicSelect",
        width: "96px",
        headerRenderer: () => "",
        sortable: false,
        filterable: false,
        enableCellEdit: false,
      },
      ...nextColumns.map((column) => ({
        name: column,
        displayName: column,
        field: column,
        enableCellEdit: false,
        type: "object" as const,
        width: "minmax(12rem, 1fr)",
      })),
    ];

    return {
      columnDefs: nextColumnDefs,
      gridData: nextGridData,
    };
  }, [cypherResult, result.columns, result.data]);

  const gridOptions = useMemo<GridOptions>(
    () => ({
      id: "query-results-grid",
      data: gridData,
      columnDefs,
      rowIdentity: (row) => String(row.__gridId),
      enableSorting: true,
      enableFiltering: false,
      enableCellEdit: false,
      enableRowSelection: true,
      enableRowHeaderSelection: true,
      enableFullRowSelection: true,
      enableSelectAll: true,
      enableSelectionBatchEvent: true,
      isRowSelectable: (row) => Boolean(row.entity.__nodeId),
      viewportHeight: 520,
      emptyMessage: "No rows returned",
    }),
    [columnDefs, gridData],
  );

  const getSelectableNodeIds = (rows: GridRecord[]) =>
    rows
      .map((row) => row.__nodeId)
      .filter(
        (nodeId): nodeId is string =>
          typeof nodeId === "string" && nodeId.length > 0,
      );

  useEffect(() => {
    if (!gridApi?.selection) {
      return;
    }

    const syncSelectionFromGrid = () => {
      if (isSyncingSelectionRef.current) {
        return;
      }

      const nextSelectedRows = gridApi.selection.getSelectedRows?.() ?? [];
      onSelectionChange(getSelectableNodeIds(nextSelectedRows));
    };

    const unsubscribeSingle = gridApi.selection.on.rowSelectionChanged(() => {
      syncSelectionFromGrid();
    });
    const unsubscribeBatch = gridApi.selection.on.rowSelectionChangedBatch(
      () => {
        syncSelectionFromGrid();
      },
    );

    return () => {
      unsubscribeSingle();
      unsubscribeBatch();
    };
  }, [gridApi, onSelectionChange]);

  useEffect(() => {
    if (!gridApi?.selection) {
      return;
    }

    const targetRows = gridData.filter((row) =>
      selectedNodeIds.has(String(row.__nodeId ?? "")),
    );
    const currentSelectedRows = gridApi.selection.getSelectedRows?.() ?? [];
    const targetIds = new Set(getSelectableNodeIds(targetRows));
    const currentIds = new Set(getSelectableNodeIds(currentSelectedRows));

    if (
      targetIds.size === currentIds.size &&
      Array.from(targetIds).every((nodeId) => currentIds.has(nodeId))
    ) {
      return;
    }

    isSyncingSelectionRef.current = true;
    try {
      gridApi.selection.clearSelectedRows?.();
      targetRows.forEach((row) => {
        gridApi.selection.selectRow?.(row);
      });
    } finally {
      isSyncingSelectionRef.current = false;
    }
  }, [gridApi, gridData, selectedNodeIds]);

  // Stable renderer identity matters: the UiGrid React wrapper drops
  // and re-portals every cell when cellRenderers identity changes, so
  // useCallback keeps the same closure across renders. Mirrors the
  // /react demo's pattern of declaring renderers once on the component
  // (statusCellRenderer is a class arrow property there).
  const renderSelectCell = useCallback(
    (ctx: GridCellTemplateContext) => {
      const row = ctx.row as GridRecord & {
        __nodeData?: QueryResultNodeData | null;
      };
      const nodeData = row.__nodeData ?? null;

      if (!nodeData) {
        return <div className="text-xs text-norse-fog py-1">-</div>;
      }

      return (
        <div className="py-1" onClick={(event) => event.stopPropagation()}>
          <button
            type="button"
            onClick={() => onNodeSelect(nodeData)}
            className="inline-flex items-center rounded-full border border-nornic-primary/40 bg-nornic-primary/10 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-wide text-nornic-primary hover:border-nornic-primary hover:bg-nornic-primary/20 hover:text-white"
          >
            Select
          </button>
        </div>
      );
    },
    [onNodeSelect],
  );

  const renderCell = useCallback((ctx: GridCellTemplateContext) => {
    const value = ctx.value;

    if (value && typeof value === "object") {
      return (
        <div className="font-mono text-xs py-1">
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

    return (
      <div className="w-full text-left font-mono text-xs py-1">
        {displayValue}
      </div>
    );
  }, []);

  const cellRenderers = useMemo(
    () =>
      Object.fromEntries(
        columnDefs.map(({ name }) => [
          name,
          name === "nornicSelect" ? renderSelectCell : renderCell,
        ]),
      ),
    [columnDefs, renderCell, renderSelectCell],
  );

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <div className="flex-1 overflow-hidden nornic-grid">
        <UiGrid
          options={gridOptions}
          cellRenderers={cellRenderers}
          onRegisterApi={setGridApi}
        />
      </div>
      <p className="text-xs text-norse-silver mt-2 px-2">
        {result.data.length} row(s) returned
      </p>
    </div>
  );
}
