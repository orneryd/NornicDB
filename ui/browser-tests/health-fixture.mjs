import {fixture} from './fixture.mjs';
export async function healthFixture(page,admin=true){
 const base=await fixture(page);base.roles=admin?['admin']:['viewer'];let index=0,failed=false,requests=2000;
 const metrics=()=>`# HELP nornicdb_uptime_seconds Server uptime in seconds
# TYPE nornicdb_uptime_seconds gauge
nornicdb_uptime_seconds ${7200+index*5}
# HELP nornicdb_nodes_total Readable nodes
# TYPE nornicdb_nodes_total gauge
nornicdb_nodes_total ${admin?13160:12840}
# HELP nornicdb_cypher_queries_total Completed Cypher queries by database and result
# TYPE nornicdb_cypher_queries_total counter
nornicdb_cypher_queries_total{database="catalog",result="success"} ${12400+index*12}
${admin?`nornicdb_cypher_queries_total{database="finance-private",result="success"} 800`:''}
# HELP nornicdb_storage_nodes_total Stored nodes by database
# TYPE nornicdb_storage_nodes_total gauge
nornicdb_storage_nodes_total{database="catalog"} 12840
${admin?`nornicdb_storage_nodes_total{database="finance-private"} 320`:''}
${admin?`# HELP process_resident_memory_bytes Resident memory size in bytes
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes ${825000000+Math.sin(index*.45)*24000000+index*800000}
# HELP go_memstats_heap_alloc_bytes Bytes allocated on the Go heap
# TYPE go_memstats_heap_alloc_bytes gauge
go_memstats_heap_alloc_bytes ${240000000+(index%6)*14000000}
# HELP go_goroutines Number of goroutines currently running
# TYPE go_goroutines gauge
go_goroutines ${88+index%5}
# HELP nornicdb_embed_queue_depth Items waiting for embedding generation
# TYPE nornicdb_embed_queue_depth gauge
nornicdb_embed_queue_depth ${Math.max(0,45-index*2)}
# HELP nornicdb_embed_worker_running Whether the embedding worker is running
# TYPE nornicdb_embed_worker_running gauge
nornicdb_embed_worker_running 1
# HELP nornicdb_http_request_duration_seconds HTTP request duration
# TYPE nornicdb_http_request_duration_seconds histogram
nornicdb_http_request_duration_seconds_bucket{method="GET",path_template="/db/{database}",le="0.005"} ${30+index*5}
nornicdb_http_request_duration_seconds_bucket{method="GET",path_template="/db/{database}",le="0.02"} ${70+index*12}
nornicdb_http_request_duration_seconds_bucket{method="GET",path_template="/db/{database}",le="0.1"} ${90+index*18}
nornicdb_http_request_duration_seconds_bucket{method="GET",path_template="/db/{database}",le="+Inf"} ${100+index*20}
nornicdb_http_request_duration_seconds_sum{method="GET",path_template="/db/{database}"} ${2+index*.36+Math.sin(index*.4)*.1}
nornicdb_http_request_duration_seconds_count{method="GET",path_template="/db/{database}"} ${100+index*20}
# HELP nornicdb_search_queries_total Search queries completed
# TYPE nornicdb_search_queries_total counter
nornicdb_search_queries_total{database="catalog",method="hybrid"} ${440+index*4}
# HELP nornicdb_cache_size_bytes Cache size in bytes
# TYPE nornicdb_cache_size_bytes gauge
nornicdb_cache_size_bytes{cache="node"} 67108864`:''}
`;
 await page.route('**/status',async route=>{
  if(new URL(route.request().url()).pathname!=='/status')return route.fallback();index++;requests+=18+index%4;
  await route.fulfill({status:failed?503:200,json:failed?{message:'Server busy'}:{status:'running',server:{version:'1.5.0',uptime_seconds:7200+index*5,requests,errors:10+Math.floor(index/5),active:2+index%3},database:{nodes:admin?13160:12840,edges:admin?57050:56210}}});
 });
 await page.route('**/metrics',async route=>{if(new URL(route.request().url()).pathname!=='/metrics')return route.fallback();await route.fulfill({status:failed?503:200,contentType:'text/plain',headers:{'X-NornicDB-Metrics-Scope':admin?'server':'database'},body:failed?'Unavailable':metrics()});});
 return {fail:(value=true)=>{failed=value;},get index(){return index;}};
}
