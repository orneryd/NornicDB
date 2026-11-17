import {test} from 'node:test';
import assert from 'node:assert/strict';
import {parseMetrics,scalar,meanLatency,rate,appendHealthPoint,formatValue,familyDisplay,nearestPointIndex} from '../src/utils/monitoring.ts';
test('Prometheus parser preserves escaped labels, histograms, summaries and special values',()=>{
 const data=parseMetrics(`# HELP sample_seconds A duration\n# TYPE sample_seconds histogram
sample_seconds_bucket{database="one}two",le="0.1"} 2
sample_seconds_bucket{database="one}two",le="+Inf"} 4
sample_seconds_sum{database="one}two"} 0.7
sample_seconds_count{database="one}two"} 4
# TYPE size_bytes gauge
size_bytes{label="quote\\\" and slash\\\\ and line\\n"} +Inf
# TYPE duration summary
duration{quantile="0.5"} 3
duration_sum 7
duration_count 2
# TYPE requests_total counter
requests_total 9`);
 assert.equal(data.families.length,4);
 const hist=data.families.find(f=>f.name==='sample_seconds').metric[0];
 assert.equal(hist.label[0].value,'one}two');assert.equal(hist.histogram.bucket.length,2);assert.equal(hist.histogram.sampleCount,'4');
 const gauge=data.families.find(f=>f.name==='size_bytes').metric[0];assert.equal(gauge.label[0].value,'quote" and slash\\ and line\n');assert.equal(scalar(gauge),null);
 assert.equal(data.families.find(f=>f.name==='duration').metric[0].summary.quantile[0].value,'3');
 assert.equal(familyDisplay(data.families.find(f=>f.name==='requests_total')),'9');
 assert.throws(()=>parseMetrics('this is not a metric response'));
});
test('rates and interval latency never invent values across resets or missing series',()=>{
 assert.equal(rate(20,10,5),2);assert.equal(rate(1,10,5),null);assert.equal(rate(20,10,0),null);assert.equal(rate(20,10,40),null);
 const family=(count,sum)=>[{name:'nornicdb_http_request_duration_seconds',type:'HISTOGRAM',metric:[{label:[{name:'method',value:'GET'}],histogram:{sampleCount:count,sampleSum:sum}}]}];
 assert.equal(meanLatency(family(20,2.5),family(10,2)),50);
 assert.equal(meanLatency(family(1,0.1),family(10,2)),null);
 assert.equal(meanLatency(family(20,2.5),[]),null);
});
test('history is bounded by both sample count and elapsed time',()=>{
 let history=[];for(let i=0;i<400;i++)history=appendHealthPoint(history,{at:i*5000});
 assert.equal(history.length,360);history=appendHealthPoint(history,{at:4000000});assert.equal(history.length,1);
 assert.equal(formatValue(null,'bytes'),'Unavailable');assert.equal(formatValue(0,'bytes'),'0 B');assert.equal(formatValue(1048576,'bytes'),'1 MiB');
});
test('chart inspection follows timestamps rather than index spacing across gaps',()=>{
 const points=[{at:0},{at:20000},{at:25000}];
 assert.equal(nearestPointIndex(points,8000),0);
 assert.equal(nearestPointIndex(points,17000),1);
 assert.equal(nearestPointIndex(points,24000),2);
 assert.equal(nearestPointIndex([],100),-1);
});
