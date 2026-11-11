import {useEffect, useState} from 'react';
import {api, type DatabaseInfo} from '../../utils/api';

export function DatabaseSummary({database,refreshKey}: {database: string | null;refreshKey?:number}) {
 const [state,setState]=useState<{name:string; requested:string|null;info?:DatabaseInfo; error?:string} | null>(null);
 useEffect(()=>{
   let current=true;
   setState(previous=>previous?.requested===database?previous:null);
   api.getResolvedDatabaseName(database ?? undefined).then(async name=>({name,requested:database,info:await api.getDatabaseInfo(name)}))
     .then(value=>{if(current)setState(value)})
     .catch(error=>{if(current)setState({name:database || 'Default database',requested:database,error:error.message})});
   return ()=>{current=false};
 },[database,refreshKey]);
 return <div className="border-b border-norse-rune px-4 py-2 text-sm text-norse-silver" role="status">
   {!state ? 'Loading database status…' : state.error ? `Database status unavailable: ${state.error}` : <>
    <strong className="text-white">{state.name}</strong> · {state.info?.nodeCount.toLocaleString()} nodes · {state.info?.edgeCount.toLocaleString()} edges
   </>}
 </div>;
}
