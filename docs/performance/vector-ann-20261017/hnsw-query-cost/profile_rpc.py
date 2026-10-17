import sys,time,json,threading,urllib.request
from pathlib import Path
sys.path.insert(0,str(Path('testing/benchmarks/vector_ann').resolve()))
import run
original=run.measure
counter=0
def profiled(points,queries,truth,k,params,warmup,concurrency):
 global counter
 result=original(points,queries,truth,k,params,warmup,concurrency)
 counter+=1
 out=Path('bench-data/vector_ann/profiling/hnsw-cost')/f'case-{counter}'
 out.mkdir(exist_ok=True)
 errors=[]
 def capture():
  try:
   with urllib.request.urlopen('http://127.0.0.1:19191/debug/pprof/profile?seconds=30',timeout=45) as response:
    (out/'cpu.pb.gz').write_bytes(response.read())
  except Exception as e:errors.append(str(e))
 thread=threading.Thread(target=capture);thread.start()
 vectors=[q.tolist() for q in queries]
 start=time.perf_counter();n=0
 while time.perf_counter()-start<31:
  points.Search(run.qg.SearchPoints(collection_name=run.COLLECTION,vector=vectors[n%len(vectors)],limit=k,params=params),timeout=60)
  n+=1
 thread.join()
 if errors:raise RuntimeError(errors)
 for profile in ['heap','allocs','mutex','block']:
  with urllib.request.urlopen(f'http://127.0.0.1:19191/debug/pprof/{profile}',timeout=15) as response:
   (out/f'{profile}.pb.gz').write_bytes(response.read())
 (out/'diagnostic.json').write_text(json.dumps(dict(requests=n,wall_s=time.perf_counter()-start,params=str(params),instrumented=True),indent=2))
 print('Profile captured',out,n,'requests',flush=True)
 return result
run.measure=profiled
run.main()
