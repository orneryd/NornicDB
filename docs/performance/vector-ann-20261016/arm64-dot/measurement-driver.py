import argparse,sys,json,time,concurrent.futures
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0,'/Users/timothysweet/src/NornicDB-ann-benchmark/testing/benchmarks/vector_ann')
import run
import numpy as np
import requests
ap=argparse.ArgumentParser();ap.add_argument('--binary',required=True);ap.add_argument('--label',required=True);ap.add_argument('--profile',action='store_true');args=ap.parse_args()
root=Path('/Users/timothysweet/src/NornicDB/bench-data/vector_ann/profiling')
base=json.loads((root/'baseline/nornicdb-hnsw-balanced.json').read_text())
cfg=Path(base['settings']['NORNICDB_CONFIG']);data=cfg.parent/cfg.name.removesuffix('.empty-config.yaml');assert data.is_dir()
queries=np.load(root.parent/'corpus/queries.npy').tolist();truth=np.load(root.parent/'corpus/truth.npy')
opts=SimpleNamespace(nornic_bin=args.binary,nornic_gpu=False,nornic_http_port=18474,nornic_bolt_port=18687,nornic_grpc_port=16334,nornic_telemetry_port=19190)
mode=next(m for m in run.MODES if m['name']=='hnsw-balanced');mode={**mode,'nornic':dict(mode['nornic'])}
if args.profile:mode['nornic'].update(NORNICDB_PPROF_ENABLED='true',NORNICDB_PPROF_LISTEN='127.0.0.1:16606')
engine=run.NornicEngine(opts)
try:
 points,_=engine.start(mode,data,root/f'{args.label}-server.log');engine.wait_ready(points,queries[0])
 def search(q,ef=512):return points.Search(run.qg.SearchPoints(collection_name=run.COLLECTION,vector=q,limit=10,params=run.qg.SearchParams(hnsw_ef=ef) if ef else None),timeout=60)
 for q in queries[:100]:search(q)
 if args.profile:
  with concurrent.futures.ThreadPoolExecutor(1) as pool:
   future=pool.submit(requests.get,'http://127.0.0.1:16606/debug/pprof/profile?seconds=15',timeout=30)
   count=0;start=time.monotonic()
   while time.monotonic()-start<17:search(queries[count%len(queries)]);count+=1
   response=future.result();response.raise_for_status();(root/f'{args.label}.cpu.pprof').write_bytes(response.content)
   print('Profile queries:',count,flush=True)
 else:
  results=[]
  for ef in [None,100,512]:
   for q in queries[:100]:search(q,ef)
   latencies=[];hits=[]
   for q in queries:
    start=time.perf_counter();response=search(q,ef);latencies.append((time.perf_counter()-start)*1000)
    hits.append([int(p.id.num) for p in response.result])
   # Corpus IDs are zero-based numerical IDs in the original harness.
   recall=sum(len(set(h)&set(map(int,t[:10]))) for h,t in zip(hits,truth))/(len(hits)*10)
   start=time.perf_counter()
   with concurrent.futures.ThreadPoolExecutor(8) as pool:list(pool.map(lambda q:search(q,ef),queries))
   qps=len(queries)/(time.perf_counter()-start)
   results.append({'ef':ef,'qps':qps,'rss_bytes':engine.server.rss(),'p50_ms':float(np.percentile(latencies,50)),'p95_ms':float(np.percentile(latencies,95)),'recall@10':recall,'ids':hits})
  (root/f'{args.label}.json').write_text(json.dumps({'binary':args.binary,'runs':results},indent=2))
  print(json.dumps([{k:v for k,v in r.items() if k!='ids'} for r in results]),flush=True)
finally:
 engine.close()
 if engine.server:engine.server.stop()
