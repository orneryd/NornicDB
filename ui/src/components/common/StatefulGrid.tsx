import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { UiGrid, type UiGridProps } from "@ornery/ui-grid-react";
import type { GridSavedState, UiGridApi } from "@ornery/ui-grid-core";
import { useMediaQuery } from "../../utils/useMediaQuery";
import { useViewState } from "../../utils/useUrlState";

/** Canonical URL bridge for grid view state; never serializes records or drafts. */
export function StatefulGrid({height,...props}:UiGridProps & {height?:number}) {
  const narrow = useMediaQuery("(max-width: 767px)");
  const [api, setApi] = useState<UiGridApi | null>(null);
  const [view, setView] = useViewState<GridSavedState | null>(
    `grid.${props.options.id}`,
    null,
  );
  const initial = useRef<GridSavedState | null>(null);
  const restoring = useRef(false);
  const viewJson = JSON.stringify(view);
  const currentJson = useRef(viewJson);
  currentJson.current = viewJson;
  const register = useCallback(
    (next: UiGridApi) => {
      initial.current = next.saveState.save();
      setApi(next);
      props.onRegisterApi?.(next);
    },
    [props.onRegisterApi],
  );
  useEffect(() => {
    if (!api) return;
    const persist = () => {
      if (restoring.current) return;
      const next = api.saveState.save();
      if (JSON.stringify(next) !== currentJson.current) {
        currentJson.current = JSON.stringify(next);
        setView(next);
      }
    };
    const subscriptions = [
      api.core.on.sortChanged(persist),
      api.core.on.filterChanged(persist),
      api.core.on.groupingChanged(persist),
      api.core.on.columnOrderChanged(persist),
      api.pagination.on.paginationChanged(persist),
      api.expandable.on.rowExpandedStateChanged(persist),
      api.pinning.on.columnPinned(persist),
    ];
    return () => subscriptions.forEach((unsubscribe) => unsubscribe());
  }, [api, setView]);
  useEffect(() => {
    if (!api || !initial.current) return;
    restoring.current = true;
    try {
      const state=view ?? initial.current;
      const fixedPins=Object.fromEntries(props.options.columnDefs.filter(column=>column.enablePinning===false&&(column.pinnedLeft||column.pinnedRight)).map(column=>[column.name,column.pinnedLeft?'left':'right'])) as Record<string,'left'|'right'>;
      const runtime=state as GridSavedState & {pinnedColumns?:Record<string,'left'|'right'>};
      const restored=Object.keys(fixedPins).length?{...state,pinning:{...state.pinning,...fixedPins},pinnedColumns:{...runtime.pinnedColumns,...fixedPins}}:state;
      // Keep the identifying left column visible without letting actions consume
      // the remaining mobile viewport. The URL remains the saved desktop view.
      const pinning=Object.fromEntries(Object.entries(restored.pinning ?? {}).filter(([,side])=>!narrow || side!=="right"));
      const pinnedColumns=Object.fromEntries(Object.entries((restored as GridSavedState & {pinnedColumns?:Record<string,string>}).pinnedColumns ?? {}).filter(([,side])=>!narrow || side!=="right"));
      const responsiveState={...restored,pinning,pinnedColumns};
      api.saveState.restore(responsiveState);
    } finally {
      restoring.current = false;
    }
  }, [api, viewJson, props.options.columnDefs, narrow]);
  // Approved SDK workaround: expanded heights are omitted from its virtual window.
  // https://github.com/orneryd/ui-grid/issues/7410
  // SDK runtime save() uses expandedRows; its published declaration still says expandable.
  const runtimeView = view as
    | (GridSavedState & { expandedRows?: Record<string, boolean> })
    | null;
  const expanded = Object.values(
    runtimeView?.expandedRows ?? view?.expandable ?? {},
  ).some(Boolean);
  const renderAll=Boolean(expanded&&props.options.enableExpandable&&(props.options.data?.length??0)>(props.options.virtualizationThreshold??20));
  const options=useMemo(()=>{
    return {rowHeight:40,headerRowHeight:40,...props.options,columnDefs:props.options.columnDefs.map(column=>narrow && column.pinnedRight?{...column,pinnedRight:false}:column),...(renderAll?{virtualizationThreshold:Number.MAX_SAFE_INTEGER}:{}),...(height!==undefined?{enableMinHeightCheck:false}:{})};
  },[props.options,renderAll,height,narrow]);
  const grid=<UiGrid {...props} options={options} onRegisterApi={register}/>;
  const filters=Object.entries((view as (GridSavedState & {activeFilters?:Record<string,string>}) | null)?.activeFilters ?? view?.filters ?? {}).filter(([,value])=>value.trim());
  return <div className="nornic-grid" style={height!==undefined?{height}: {height:"100%"}}>
    {props.options.enableFiltering && <div className="grid-filter-summary" role="group" aria-label="Table filters">
      <span>{filters.length ? `${filters.length} active ${filters.length===1?'filter':'filters'}` : 'Filter by column'}</span>
      {filters.length>0 ? <><span className="grid-filter-values">{filters.map(([name,value])=>`${props.options.columnDefs.find(column=>column.name===name)?.displayName ?? name}: ${value}`).join(' · ')}</span><button type="button" onClick={()=>api?.core.clearAllFilters()}>Clear filters</button></> : <span className="grid-filter-hint">Enter a value below a column heading</span>}
    </div>}
    <div className="grid-surface">{grid}</div>
  </div>;
}
