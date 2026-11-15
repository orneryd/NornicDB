import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { UiGrid, type UiGridProps } from "@ornery/ui-grid-react";
import type { GridSavedState, UiGridApi } from "@ornery/ui-grid-core";
import { useViewState } from "../../utils/useUrlState";

/** Canonical URL bridge for grid view state; never serializes records or drafts. */
export function StatefulGrid({height,...props}:UiGridProps & {height?:number}) {
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
      api.saveState.restore(restored);
    } finally {
      restoring.current = false;
    }
  }, [api, viewJson, props.options.columnDefs]);
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
    if(!renderAll&&height===undefined)return props.options;
    return {...props.options,...(renderAll?{virtualizationThreshold:Number.MAX_SAFE_INTEGER}:{}),...(height!==undefined?{enableMinHeightCheck:false}:{})};
  },[props.options,renderAll,height]);
  const grid=<UiGrid {...props} options={options} onRegisterApi={register}/>;
  return <div className="nornic-grid" style={height!==undefined?{height}: {height:"100%"}}>{grid}</div>;
}
