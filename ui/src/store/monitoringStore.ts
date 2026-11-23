import {create} from 'zustand';
import {api,type DatabaseStats} from '../utils/api';
import {appendHealthPoint,familyValue,meanLatency,rate,type MetricsSnapshot,type HealthPoint} from '../utils/monitoring';

interface MonitoringState {ready:boolean;stats:DatabaseStats|null;connected:boolean;updatedAt:number|null;error:string;metrics:MetricsSnapshot|null;metricsError:string;metricsStatus:number|null;history:HealthPoint[];paused:boolean;refreshing:boolean;hidden:boolean;clearHistory:()=>void}
export const useMonitoringStore=create<MonitoringState>(set=>({ready:false,stats:null,connected:false,updatedAt:null,error:'',metrics:null,metricsError:'',metricsStatus:null,history:[],paused:false,refreshing:false,hidden:false,clearHistory:()=>set({history:[]})}));
let identityReady=false;
let consumers=0,detailConsumers=0,timer:ReturnType<typeof setInterval>|undefined,request:AbortController|null=null,owner:string|undefined,generation=0;
function resetPrivateHistory(){useMonitoringStore.setState({stats:null,metrics:null,history:[],updatedAt:null,connected:false});}
async function poll(){
 if(request||document.hidden||!consumers||!identityReady)return;
 const current=generation,controller=new AbortController();request=controller;
 const timeout=setTimeout(()=>{controller.abort();if(current===generation)useMonitoringStore.setState({...(!stats?{stats:null,connected:healthy,error:'Status refresh timed out'}:{}),metricsError:detailConsumers?'Metrics refresh timed out':''});},10000);
 useMonitoringStore.setState({refreshing:true});
 const started=Date.now();let stats:DatabaseStats|null=null,metrics:MetricsSnapshot|null=null,healthy=false;
 const valid=()=>current===generation&&!controller.signal.aborted;
 const health=api.getHealth(controller.signal).then(()=>{if(valid()){healthy=true;useMonitoringStore.setState({connected:true,updatedAt:Date.now()});}return true;}).catch(()=>false);
 try{
  await Promise.all([
   health,
   api.getStatus(controller.signal).then(value=>{if(valid()){stats=value;useMonitoringStore.setState({stats:value,connected:true,updatedAt:Date.now(),error:''});}}).catch(async error=>{if(valid()){const status=(error as {status?:number}).status;if(status===401||status===403)resetPrivateHistory();const message=error instanceof Error?error.message:'Status request failed';const reachable=await health;if(valid())useMonitoringStore.setState({stats:null,connected:reachable,updatedAt:reachable?Date.now():null,error:message});}}),
   detailConsumers?api.getMonitoringMetrics(controller.signal).then(value=>{if(valid()){metrics=value;if(useMonitoringStore.getState().metrics?.scope&&useMonitoringStore.getState().metrics?.scope!==value.scope){useMonitoringStore.setState({history:[]});lastMetrics=null;}useMonitoringStore.setState({metrics:value,metricsError:'',metricsStatus:200});}}).catch(error=>{if(valid()){const status=(error as {status?:number}).status??null;useMonitoringStore.setState({metrics:null,metricsError:error instanceof Error?error.message:'Metrics request failed',metricsStatus:status});if(status===401||status===403)useMonitoringStore.setState({history:[]});}}):Promise.resolve()
  ]);
  if(!valid())return;
  const state=useMonitoringStore.getState();
  if(stats&&!state.paused){
   const s=stats as DatabaseStats,latest=state.history[state.history.length-1],seconds=latest?(started-latest.at)/1000:0;
   const restart=Boolean(latest&&s.server.uptime_seconds<latest.uptime);
   const families=(metrics as MetricsSnapshot|null)?.families??[];
   const previousMetrics=lastMetrics;
   const point:HealthPoint={at:started,uptime:s.server.uptime_seconds,requests:s.server.requests,errors:s.server.errors,active:s.server.active,nodes:s.database.nodes,edges:s.database.edges,restart,
    requestRate:latest&&!restart?rate(s.server.requests,latest.requests,seconds):null,errorRate:latest&&!restart?rate(s.server.errors,latest.errors,seconds):null,
    rss:familyValue(families,'process_resident_memory_bytes'),heap:familyValue(families,'go_memstats_heap_alloc_bytes'),goroutines:familyValue(families,'go_goroutines'),queue:familyValue(families,'nornicdb_embed_queue_depth'),
    latency:latest&&!restart&&seconds>0&&seconds<=15&&previousMetrics&&metrics?meanLatency(families,previousMetrics.families):null};
   useMonitoringStore.setState({history:appendHealthPoint(state.history,point)});
  }
  lastMetrics=metrics;
 }finally{clearTimeout(timeout);if(request===controller){request=null;useMonitoringStore.setState({refreshing:false});}}
}
let lastMetrics:MetricsSnapshot|null=null;
function visibility(){useMonitoringStore.setState({hidden:document.hidden});if(!document.hidden)void poll();}
/** Each mounted header joins one tab-wide sampler. History never leaves memory. */
export function acquireMonitoring(details:boolean){
 consumers++;if(details)detailConsumers++;
 if(consumers===1){useMonitoringStore.setState({ready:false});identityReady=false;generation++;const current=generation;
  // Identity is used only to discard another account's browser-held samples;
  // server authorization remains authoritative on both monitoring requests.
  void api.checkAuth().then(profile=>{if(current!==generation)return;const next=profile.user??'anonymous';if(owner!==undefined&&owner!==next){resetPrivateHistory();lastMetrics=null;}owner=next;identityReady=true;useMonitoringStore.setState({ready:true});void poll();});
  timer=setInterval(()=>void poll(),5000);document.addEventListener('visibilitychange',visibility);visibility();
 }else if(details)void poll();
 return()=>{consumers--;if(details)detailConsumers--;if(!consumers){identityReady=false;generation++;clearInterval(timer);document.removeEventListener('visibilitychange',visibility);request?.abort();request=null;lastMetrics=null;useMonitoringStore.setState({ready:false,refreshing:false});}};
}
export function refreshMonitoring(){return poll();}
