import {ArrowLeft,Loader2,RefreshCw} from 'lucide-react';
import {getNodePreview} from '../../utils/nodeUtils';
import type {SearchResult} from '../../utils/api';
export function InspectorSimilarView({selected,result,onBack,onRetry,onSelect}:{selected:SearchResult;result:{nodeId:string;results:SearchResult[];loading:boolean;error?:string}|null;onBack:()=>void;onRetry:()=>void;onSelect:(result:SearchResult)=>void}){
 const current=result?.nodeId===selected.node.id?result:null;
 return <section className="similar-records-view" aria-label="Similar records">
  <button type="button" className="similar-back" onClick={onBack}><ArrowLeft size={14}/> Back to details</button>
  <p className="text-sm text-norse-silver">Similar to <strong className="text-white">{getNodePreview(selected.node.properties)}</strong></p>
  <p className="text-xs text-norse-silver">Inspect a match to compare its properties. Your query results stay in place.</p>
  {!current||current.loading?<p role="status" className="similar-loading"><Loader2 size={16} className="animate-spin"/> Finding similar records…</p>:current.error?<div role="alert" className="field-error"><p>{current.error}</p><button type="button" className="similar-back" onClick={onRetry}>Try again</button></div>:<>
   <div className="similar-results-heading"><p role="status">{current.results.length} matches</p><button type="button" onClick={onRetry}><RefreshCw size={13}/> Refresh</button></div>
   {current.results.length===0?<p className="text-sm text-norse-silver">No similar records found for this node.</p>:<ul className="similar-record-list">{current.results.map(match=><li key={match.node.id}><div><strong>{getNodePreview(match.node.properties)}</strong><span>{match.node.labels.join(' · ') || 'Unlabelled node'}</span><code>{match.node.id}</code></div><div className="similar-result-action"><span>Score {match.score.toFixed(2)}</span><button type="button" onClick={()=>onSelect(match)} aria-label={`Inspect ${getNodePreview(match.node.properties)}`}>Inspect →</button></div></li>)}</ul>}
  </>}
 </section>;
}
