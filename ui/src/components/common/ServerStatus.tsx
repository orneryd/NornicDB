import {useEffect} from 'react';
import {Activity} from 'lucide-react';
import {useMonitoringStore,acquireMonitoring} from '../../store/monitoringStore';
import {Link,useLocation} from 'react-router-dom';
import {useViewState} from '../../utils/useUrlState';

/** Shared header status; the mounted header owns its refresh lifecycle. */
export function ServerStatus() {
 const {stats:reported,ready,connected,updatedAt,refreshing,error,hidden}=useMonitoringStore();
 const location=useLocation();const details=location.pathname.startsWith('/operations/health');
 const [open,setOpen]=useViewState('server',false);
 useEffect(()=>acquireMonitoring(details),[details]);
 const stats=ready?reported:null;
 const fresh=ready&&connected&&updatedAt!==null&&Date.now()-updatedAt<15000;
 return <div className="header-server-status">
  <span className={`header-server-connection ${fresh?'text-nornic-primary':'text-red-400'}`}><Activity className="w-4 h-4" aria-hidden="true"/>{!ready?'Checking':hidden?'Paused':fresh?'Connected':'Disconnected'}</span>
  <details className="server-status-popover" open={open} onToggle={event=>{if(event.currentTarget.open!==open)setOpen(event.currentTarget.open)}}>
   <summary>Server status</summary>
   <dl aria-label="Server status details">
    <dt>Server-reported nodes</dt><dd>{stats?.database?.nodes?.toLocaleString()??'Unavailable'}</dd>
    <dt>Server-reported edges</dt><dd>{stats?.database?.edges?.toLocaleString()??'Unavailable'}</dd>
    <dt>Uptime</dt><dd>{stats?.server?`${Math.floor(stats.server.uptime_seconds/3600)}h ${Math.floor(stats.server.uptime_seconds%3600/60)}m`:'Unavailable'}</dd>
    <dt>Last update</dt><dd>{updatedAt?new Date(updatedAt).toLocaleTimeString():'Waiting for a sample'}</dd>
    {error&&<dd className="text-red-300">{error}</dd>}
    <dd>{refreshing?'Refreshing…':hidden?'Updates resume when this tab is visible.':''}</dd>
    <dd><Link className="text-nornic-primary underline" to="/operations/health">Open health dashboard</Link></dd>
   </dl>
  </details>
 </div>;
}
