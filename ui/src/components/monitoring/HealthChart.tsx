import {useState} from 'react';
import {formatValue,nearestPointIndex,type HealthPoint} from '../../utils/monitoring';

export interface ChartSeries {key:keyof HealthPoint;label:string;color:string}
export function HealthChart({title,description,points,series,unit}:{title:string;description:string;points:HealthPoint[];series:ChartSeries[];unit:string}){
 const [cursor,setCursor]=useState<number|null>(null);
 const values=points.flatMap(point=>series.map(item=>point[item.key])).filter((v):v is number=>typeof v==='number'&&Number.isFinite(v));
 const upper=values.length?Math.max(...values):0,high=upper>0?upper:1,low=0;
 const first=points[0]?.at??0,last=points[points.length-1]?.at??first;
 const x=(at:number)=>48+(at-first)/Math.max(last-first,1)*504,y=(v:number)=>132-(v-low)/(high-low)*110;
 const selected=cursor===null?points[points.length-1]:points[Math.min(cursor,points.length-1)];
 const labelTime=(at:number)=>new Date(at).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'});
 return <section className="health-chart" aria-label={title}>
  <header><div><h2>{title}</h2><p>{description}</p></div>{selected&&<time>{labelTime(selected.at)}</time>}</header>
  <div className="health-chart-values">{series.map(item=><span key={item.key}><i style={{background:item.color}}/>{item.label}<strong>{formatValue(selected&&typeof selected[item.key]==='number'?selected[item.key] as number:null,unit)}</strong></span>)}</div>
  <svg viewBox="0 0 580 165" role="img" aria-label={`${title}. ${values.length?'Use Inspect samples for exact values.':'Waiting for measurements.'}`} onPointerMove={event=>{if(!points.length)return;const box=event.currentTarget.getBoundingClientRect();const fraction=Math.max(0,Math.min(1,((event.clientX-box.left)/box.width*580-48)/504));setCursor(nearestPointIndex(points,first+fraction*(last-first)));}} onPointerLeave={()=>setCursor(null)}>
   {[22,77,132].map((height,index)=><g key={height}><line x1="48" y1={height} x2="552" y2={height} stroke="#2a3247" strokeDasharray="3 5"/><text x="43" y={height+4} textAnchor="end">{formatValue(high*(1-index/2),unit)}</text></g>)}
   {series.map(item=>{let previous:HealthPoint|undefined;const path=points.map(point=>{const value=point[item.key];if(typeof value!=='number'||!Number.isFinite(value)){previous=undefined;return '';}
    const move=!previous||point.restart||point.at-previous.at>8000;previous=point;return `${move?'M':'L'}${x(point.at)},${y(value)}`;
   }).join(' ');return <path key={item.key} d={path} fill="none" stroke={item.color} strokeWidth="2" vectorEffect="non-scaling-stroke"/>;})}
   {selected&&cursor!==null&&<line x1={x(selected.at)} x2={x(selected.at)} y1="18" y2="132" stroke="#b9c8dc" strokeDasharray="3 4"/>}
   {points.filter(point=>point.restart).map(point=><g key={point.at}><title>Server restart detected</title><line x1={x(point.at)} x2={x(point.at)} y1="18" y2="132" stroke="#e8be63" strokeDasharray="2 5"/><text x={x(point.at)+4} y="16">Restart</text></g>)}
   {values.length===0&&<text x="300" y="82" textAnchor="middle" className="health-chart-empty">Waiting for reported measurements</text>}
   {points.length>0&&<><text x="48" y="155">{labelTime(first)}</text><text x="552" y="155" textAnchor="end">{labelTime(last)}</text></>}
  </svg>
  <details><summary>Inspect samples</summary><label className="health-chart-scrubber">Sample<input type="range" min="0" max={Math.max(0,points.length-1)} value={cursor??Math.max(0,points.length-1)} disabled={!points.length} onChange={event=>setCursor(Number(event.target.value))} aria-label={`Inspect ${title} sample`}/></label><div className="health-sample-table"><table><thead><tr><th>Time</th>{series.map(item=><th key={item.key}>{item.label}</th>)}</tr></thead><tbody>{points.slice().reverse().map(point=><tr key={point.at}><td>{labelTime(point.at)}{point.restart?' · restart':''}</td>{series.map(item=><td key={item.key}>{formatValue(typeof point[item.key]==='number'?point[item.key] as number:null,unit)}</td>)}</tr>)}</tbody></table></div></details>
 </section>;
}
