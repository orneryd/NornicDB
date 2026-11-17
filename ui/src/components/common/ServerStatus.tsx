import {useEffect} from 'react';
import {Activity} from 'lucide-react';
import {useAppStore} from '../../store/appStore';
import {useViewState} from '../../utils/useUrlState';

/** Shared header status; the mounted header owns its refresh lifecycle. */
export function ServerStatus() {
 const stats=useAppStore(state=>state.stats);
 const connected=useAppStore(state=>state.connected);
 const fetchStats=useAppStore(state=>state.fetchStats);
 const [open,setOpen]=useViewState('server',false);
 useEffect(()=>{
  void fetchStats();
  const timer=setInterval(()=>void fetchStats(),5000);
  return()=>clearInterval(timer);
 },[fetchStats]);
 return <div className="header-server-status">
  <span className={`header-server-connection ${connected?'text-nornic-primary':'text-red-400'}`}><Activity className="w-4 h-4" aria-hidden="true"/>{connected?'Connected':'Disconnected'}</span>
  <details className="server-status-popover" open={open} onToggle={event=>{if(event.currentTarget.open!==open)setOpen(event.currentTarget.open)}}>
   <summary>Server status</summary>
   <dl aria-label="Server status details">
    <dt>Server-reported nodes</dt><dd>{stats?.database?.nodes?.toLocaleString()??'Unavailable'}</dd>
    <dt>Server-reported edges</dt><dd>{stats?.database?.edges?.toLocaleString()??'Unavailable'}</dd>
    <dt>Uptime</dt><dd>{stats?.server?`${Math.floor(stats.server.uptime_seconds/3600)}h ${Math.floor(stats.server.uptime_seconds%3600/60)}m`:'Unavailable'}</dd>
   </dl>
  </details>
 </div>;
}
