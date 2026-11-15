import {useCallback,useRef} from 'react';
import {useBeforeUnload,useBlocker} from 'react-router-dom';
import {Modal} from './Modal';
/** Block changes of editing context, while leaving URL-only control state usable. */
export function useUnsavedChanges(changed:boolean,contextParams:readonly string[]=[]){
 const dirty=useRef(changed);dirty.current=changed;
 const watched=useRef(contextParams);watched.current=contextParams;
 const blocker=useBlocker(useCallback(({currentLocation,nextLocation})=>dirty.current&&(currentLocation.pathname.replace(/\/$/,'')!==nextLocation.pathname.replace(/\/$/,'')||watched.current.some(key=>new URLSearchParams(currentLocation.search).get(key)!==new URLSearchParams(nextLocation.search).get(key))),[]));
 useBeforeUnload(useCallback(event=>{if(dirty.current){event.preventDefault();event.returnValue='';}},[]));
 const allowLeave=()=>{dirty.current=false;};
 const warning=<Modal isOpen={blocker.state==='blocked'} onClose={()=>{if(blocker.state==='blocked')blocker.reset();}} title="Leave unsaved changes?" size="sm" className="unsaved-edit-dialog">
  <p className="text-sm text-norse-silver">Your changes have not been saved. Keep editing, or discard them and leave.</p>
  <div className="flex gap-3 mt-5"><button type="button" data-autofocus className="px-3 py-2 border border-norse-rune text-white" onClick={()=>{if(blocker.state==='blocked')blocker.reset();}}>Keep editing</button><button type="button" className="px-3 py-2 border border-red-400/40 text-red-300" onClick={()=>{if(blocker.state==='blocked'){dirty.current=false;blocker.proceed();}}}>Discard changes and leave</button></div>
 </Modal>;
 return {allowLeave,warning};
}
