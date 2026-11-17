export type MetricNumber=number|string;
export interface MetricSample {
 label?:{name:string;value:string}[];
 gauge?:{value:MetricNumber};counter?:{value:MetricNumber};untyped?:{value:MetricNumber};
 histogram?:{sampleCount?:MetricNumber;sampleSum?:MetricNumber;bucket?:{cumulativeCount:MetricNumber;upperBound:MetricNumber}[];[key:string]:unknown};
 summary?:{sampleCount?:MetricNumber;sampleSum?:MetricNumber;quantile?:{quantile:MetricNumber;value:MetricNumber}[]};
 [key:string]:unknown;
}
export interface MetricFamily {name:string;help?:string;type:string;unit?:string;metric?:MetricSample[]}
export interface MetricsSnapshot {captured_at:string;families:MetricFamily[];scope?:'server'|'database'}
export interface HealthPoint {at:number;uptime:number;requests:number;errors:number;active:number;nodes:number;edges:number;restart:boolean;rss:number|null;heap:number|null;goroutines:number|null;queue:number|null;requestRate:number|null;errorRate:number|null;latency:number|null}
export const HISTORY_LIMIT=360;
export const HISTORY_MS=30*60*1000;
/** Parse the existing Prometheus text endpoint, preserving labelled series. */
export function parseMetrics(text:string):MetricsSnapshot {
 const families=new Map<string,MetricFamily>();
 const samples=new Map<string,Map<string,MetricSample>>();
 const decode=(value:string)=>value.replace(/\\(\\|"|n)/g,(_,letter:string)=>letter==='n'?'\n':letter);
 for(const line of text.split(/\r?\n/)){
  const metadata=line.match(/^# (HELP|TYPE|UNIT) (\S+)\s+(.+)$/);
  if(metadata){const [,kind,name,value]=metadata;const family=families.get(name)??{name,type:'UNTYPED',metric:[]};if(kind==='TYPE')family.type=value.toUpperCase();else if(kind==='HELP')family.help=decode(value);else family.unit=value;families.set(name,family);continue;}
  if(!line.trim()||line.startsWith('#'))continue;
  const match=line.match(/^([a-zA-Z_:][\w:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\S+)?$/);
  if(!match)throw Error('The metrics response contains an invalid sample.');
  const [,name,labelText='',raw]=match;const labels:{name:string;value:string}[]=[];
  const pattern=/([a-zA-Z_][\w]*)="((?:\\.|[^"\\])*)"(?:,\s*|$)/g;let label:RegExpExecArray|null;
  while((label=pattern.exec(labelText)))labels.push({name:label[1],value:decode(label[2])});
  let family=families.get(name),suffix='';
  if(!family){const parent=name.replace(/_(bucket|sum|count)$/,'');family=families.get(parent);if(family)suffix=name.slice(parent.length);}
  if(!family){family={name,type:'UNTYPED',metric:[]};families.set(name,family);}
  const special=family.type==='HISTOGRAM'?'le':family.type==='SUMMARY'?'quantile':'';
  const identity=labels.filter(item=>item.name!==special).sort((a,b)=>a.name.localeCompare(b.name));
  const key=JSON.stringify(identity);let series=samples.get(family.name);if(!series){series=new Map();samples.set(family.name,series);}
  let sample=series.get(key);if(!sample){sample={label:identity};series.set(key,sample);family.metric!.push(sample);}
  if(family.type==='HISTOGRAM'){
   sample.histogram??={};if(suffix==='_count')sample.histogram.sampleCount=raw;else if(suffix==='_sum')sample.histogram.sampleSum=raw;else if(suffix==='_bucket'){sample.histogram.bucket??=[];sample.histogram.bucket.push({upperBound:labels.find(item=>item.name==='le')?.value??'+Inf',cumulativeCount:raw});}
  }else if(family.type==='SUMMARY'){
   sample.summary??={};if(suffix==='_count')sample.summary.sampleCount=raw;else if(suffix==='_sum')sample.summary.sampleSum=raw;else{sample.summary.quantile??=[];sample.summary.quantile.push({quantile:labels.find(item=>item.name==='quantile')?.value??'',value:raw});}
  }else if(family.type==='COUNTER')sample.counter={value:raw};else if(family.type==='GAUGE')sample.gauge={value:raw};else sample.untyped={value:raw};
 }
 return {captured_at:new Date().toISOString(),families:[...families.values()].sort((a,b)=>a.name.localeCompare(b.name))};
}
export function finite(value:unknown):number|null {if(value===undefined||value===null||value==='')return null;const n=Number(value);return Number.isFinite(n)?n:null;}
export function scalar(sample:MetricSample):number|null {return finite(sample.gauge?.value??sample.counter?.value??sample.untyped?.value);}
export function familyValue(families:MetricFamily[],name:string):number|null {
 const samples=families.find(f=>f.name===name)?.metric;if(!samples?.length)return null;
 const values=samples.map(scalar);return values.some(v=>v===null)?null:values.reduce<number>((sum,value)=>sum+(value??0),0);
}
export function rate(current:number,previous:number,seconds:number):number|null {return seconds>0&&seconds<=15&&current>=previous?(current-previous)/seconds:null;}
export function meanLatency(current:MetricFamily[],previous:MetricFamily[]):number|null {
 const now=current.find(f=>f.name==='nornicdb_http_request_duration_seconds')?.metric;
 const before=previous.find(f=>f.name==='nornicdb_http_request_duration_seconds')?.metric;
 if(!now?.length||!before?.length)return null;
 const key=(s:MetricSample)=>JSON.stringify([...(s.label??[])].sort((a,b)=>a.name.localeCompare(b.name)));
 const old=new Map(before.map(s=>[key(s),s]));let count=0,sum=0;
 for(const sample of now){const prior=old.get(key(sample));if(!prior)return null;
  const n=finite(sample.histogram?.sampleCount),p=finite(prior.histogram?.sampleCount),s=finite(sample.histogram?.sampleSum),q=finite(prior.histogram?.sampleSum);
  if(n===null||p===null||s===null||q===null||n<p||s<q)return null;
  count+=n-p;sum+=s-q;
 }
 return count>0?sum/count*1000:null;
}
export function appendHealthPoint(history:HealthPoint[],point:HealthPoint):HealthPoint[]{return [...history.filter(p=>p.at>point.at-HISTORY_MS),point].slice(-HISTORY_LIMIT);}
export function nearestPointIndex(points:{at:number}[],time:number):number {
 let nearest=-1,distance=Infinity;
 for(let index=0;index<points.length;index++){const delta=Math.abs(points[index].at-time);if(delta<distance){distance=delta;nearest=index;}}
 return nearest;
}
export function metricGroup(name:string):string {
 const aliases:Record<string,string>={nornicdb_uptime_seconds:'runtime',nornicdb_info:'runtime',nornicdb_requests_total:'http',nornicdb_errors_total:'http',nornicdb_active_requests:'http',nornicdb_nodes_total:'storage',nornicdb_edges_total:'storage',nornicdb_embeddings_processed:'embed',nornicdb_embeddings_failed:'embed',nornicdb_embedding_worker_running:'embed',nornicdb_slow_queries_total:'cypher',nornicdb_slow_query_threshold_ms:'cypher'};
 if(aliases[name])return aliases[name];
 const group=name.startsWith('nornicdb_')?name.slice(9).split('_')[0]:name.startsWith('go_')||name.startsWith('process_')?'runtime':'other';
 return ({process:'runtime',bolt:'http',mvcc:'storage'} as Record<string,string>)[group]??group;
}
export function metricUnit(name:string):string {return /_bytes(?:_total)?$/.test(name)?'bytes':/_seconds(?:_total)?$/.test(name)?'seconds':/ratio$/.test(name)?'ratio':'count';}
export function formatValue(value:number|null,unit='count'):string {
 if(value===null)return 'Unavailable';
 if(unit==='bytes'){const magnitude=value===0?0:Math.min(4,Math.max(0,Math.floor(Math.log2(Math.abs(value))/10)));return `${(value/1024**magnitude).toLocaleString(undefined,{maximumFractionDigits:1})} ${['B','KiB','MiB','GiB','TiB'][magnitude]}`;}
 const precision=value!==0&&Math.abs(value)<.01?{maximumSignificantDigits:3}:{maximumFractionDigits:2};
 if(unit==='ms')return `${value.toLocaleString(undefined,precision)} ms`;
 if(unit==='seconds')return `${value.toLocaleString(undefined,precision)} s`;
 if(unit==='rate')return `${value.toLocaleString(undefined,precision)}/s`;
 return value.toLocaleString(undefined,precision);
}
export function familyDisplay(family:MetricFamily):string {
 const samples=family.metric??[];if(samples.length!==1)return `${samples.length.toLocaleString()} series`;
 const sample=samples[0];const count=finite(sample.histogram?.sampleCount??sample.summary?.sampleCount);
 return count!==null?`${formatValue(count)} observations`:formatValue(scalar(sample),family.unit||metricUnit(family.name));
}
