import {useCallback, useEffect, useRef, useState} from 'react';
import {useViewState} from '../../utils/useUrlState';
import {api, type DatabaseInfo} from '../../utils/api';

const setting='db.nornic.embedding.enabled';
type RowState={enabled?:boolean; saving?:boolean; error?:string; retryValue?:boolean; notice?:string};

/** Database generation switches are immediate settings, separate from regeneration. */
export function ManagedEmbeddingDatabases({workerEnabled}:{workerEnabled:boolean}) {
 const [databases,setDatabases]=useState<DatabaseInfo[]>([]);
 const [rows,setRows]=useState<Record<string,RowState>>({});
 const [error,setError]=useState('');
 const [loading,setLoading]=useState(true);
 const [filter,setFilter]=useViewState('embeddingDatabase','');
 const [status,setStatus]=useViewState('embeddingStatus','all');
 const saving=useRef(new Set<string>());
 const loadingRef=useRef(false);
 const revision=useRef<Record<string,number>>({});
 const mounted=useRef(true);
 const load=useCallback(async()=>{
  if(loadingRef.current)return;
  loadingRef.current=true;
  try {
   const list=await api.listDatabases();
   if(!mounted.current)return;
   setDatabases(list);setError('');
   const physical=list.filter(db=>db.name!=='system'&&db.type!=='composite');
   // Bound configuration requests instead of issuing one request per database at once.
   for(let start=0;start<physical.length;start+=4) {
    await Promise.all(physical.slice(start,start+4).map(async db=>{
     if(saving.current.has(db.name))return;
     const startedRevision=revision.current[db.name]??0;
     try {
      const config=await api.getDatabaseConfig(db.name);
      const value=config.effective[setting];
      if(value!=='true'&&value!=='false')throw new Error('The server did not report generation status.');
      if(mounted.current&&!saving.current.has(db.name)&&(revision.current[db.name]??0)===startedRevision)setRows(previous=>({...previous,[db.name]:{...previous[db.name],enabled:value==='true',...(previous[db.name]?.retryValue===undefined?{error:undefined}: {})}}));
     } catch(failure) {
      if(mounted.current&&!saving.current.has(db.name)&&(revision.current[db.name]??0)===startedRevision)setRows(previous=>({...previous,[db.name]:{...previous[db.name],error:failure instanceof Error?failure.message:'Could not load status.'}}));
     }
    }));
   }
  } catch(failure) {if(mounted.current)setError(failure instanceof Error?failure.message:'Could not load databases.');}
  finally {loadingRef.current=false;if(mounted.current)setLoading(false);}
 },[]);
 useEffect(()=>{mounted.current=true;void load();const timer=setInterval(()=>void load(),10000);return()=>{mounted.current=false;clearInterval(timer);};},[load]);
 const change=async(name:string,enabled:boolean)=>{
  if(saving.current.has(name))return;
  saving.current.add(name);
  revision.current[name]=(revision.current[name]??0)+1;
  setRows(previous=>({...previous,[name]:{...previous[name],saving:true,error:undefined,notice:undefined,retryValue:undefined}}));
  try {
   const result=await api.patchDatabaseConfig(name,{[setting]:String(enabled)});
   if(result.effective[setting]!==String(enabled))throw new Error('The server did not apply the change.');
   if(mounted.current)setRows(previous=>({...previous,[name]:{enabled,notice:enabled?'Generation turned on.':'Generation turned off.'}}));
  } catch(failure) {
   if(mounted.current)setRows(previous=>({...previous,[name]:{...previous[name],saving:false,error:failure instanceof Error?failure.message:'Could not save change.',retryValue:enabled}}));
  } finally {saving.current.delete(name);}
 };
 const visible=databases.filter(db=>db.name.toLowerCase().includes(filter.trim().toLowerCase())&&(status==='all'||(status==='on'?rows[db.name]?.enabled===true:rows[db.name]?.enabled===false)));
 const configurable=databases.filter(db=>db.name!=='system'&&db.type!=='composite');
 const on=configurable.filter(db=>rows[db.name]?.enabled===true).length;
 const off=configurable.filter(db=>rows[db.name]?.enabled===false).length;
 return <section className="managed-embedding-databases" aria-labelledby="managed-generation-heading">
  <div className="flex flex-wrap items-center justify-between gap-3"><h2 id="managed-generation-heading" className="text-lg font-semibold">Managed generation by database</h2><button type="button" className="text-sm text-emerald-400 min-h-10" onClick={()=>void load()}>Refresh status</button></div>
  <p className="text-sm text-norse-silver">Changes apply immediately. Turning generation off keeps existing embeddings; work already in flight may finish. Turning it on queues eligible records in the background.</p>
  {!workerEnabled&&<p className="text-sm text-amber-300">The server embedding worker is unavailable. Database settings are saved, but generation needs a configured, running worker.</p>}
  <div className="embedding-database-filters">
   <label>Filter database names<input type="search" value={filter} onChange={event=>setFilter(event.target.value)} /></label>
   <label>Generation status<select value={status} onChange={event=>setStatus(event.target.value)}><option value="all">All databases</option><option value="on">On</option><option value="off">Off</option></select></label>
   <p aria-live="polite">{on} on · {off} off · {configurable.length-on-off} unknown</p>
  </div>
  {error&&<p role="alert" className="text-red-300">{error} <button type="button" onClick={()=>void load()} className="underline">Retry loading databases</button></p>}
  {loading&&<p role="status">Loading database settings…</p>}
  <table className="embedding-database-table"><caption className="sr-only">Managed embedding generation for each database</caption><thead><tr><th scope="col">Database</th><th scope="col">Generation</th><th scope="col">Status</th></tr></thead><tbody>
   {visible.map(db=>{const row=rows[db.name]??{};const unsupported=db.name==='system'||db.type==='composite';return <tr key={db.name}>
    <th scope="row">{db.name}<span className="block text-xs font-normal text-norse-silver">{db.type==='composite'?'Composite':db.name==='system'?'System':'Standard'}</span></th>
    <td>{unsupported?<span className="text-norse-silver">Not applicable</span>:<button type="button" role="switch" aria-label={`Managed generation for ${db.name}`} aria-checked={row.enabled??false} aria-describedby={`embedding-status-${encodeURIComponent(db.name)}`} disabled={row.enabled===undefined||row.saving||Boolean(row.error&&row.retryValue===undefined)} onClick={()=>void change(db.name,!row.enabled)} className="embedding-generation-switch"><span className="embedding-switch-track" aria-hidden="true"><span /></span><span>{row.enabled===undefined?'Unknown':row.enabled?'On':'Off'}</span></button>}</td>
    <td id={`embedding-status-${encodeURIComponent(db.name)}`}>
     {unsupported?<span className="text-norse-silver">{db.type==='composite'?'Manage generation on its constituent databases.':'System records are not embedded.'}</span>:<>
      <span role="status">{row.saving?'Saving…':row.error?'Unavailable':row.notice??(row.enabled===undefined?'Loading…':row.enabled?'New and updated records are eligible.':'Automatic generation is paused.')}</span>
      {row.error&&<p role="alert" className="text-red-300">{row.error} <button type="button" className="underline" onClick={()=>row.retryValue===undefined?void load():void change(db.name,row.retryValue)}>Retry {db.name}</button></p>}
     </>}
    </td>
   </tr>;})}
  </tbody></table>
  {!loading&&!visible.length&&!error&&<p className="text-sm text-norse-silver">No databases match these filters.</p>}
  <p className="text-xs text-norse-silver">Only database administrators can change generation settings. Status refreshes every 10 seconds.</p>
 </section>;
}
