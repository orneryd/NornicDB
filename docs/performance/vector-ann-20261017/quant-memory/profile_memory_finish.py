import sys,json,time,urllib.request,subprocess
from pathlib import Path
import psutil
sys.path.insert(0,str(Path('testing/benchmarks/vector_ann').resolve()))
import run
original=run.measure
binary=Path(sys.argv[sys.argv.index('--nornic-bin')+1]).resolve()
base=Path('bench-data/vector_ann/profiling/quant-memory-current')
count=4

def profile(points,queries,truth,k,params,warmup,concurrency):
 global count
 result=original(points,queries,truth,k,params,warmup,concurrency)
 count+=1
 out=base/f'case-{count}';out.mkdir(exist_ok=True)
 matches=[]
 for p in psutil.process_iter(['pid','cmdline']):
  try:
   args=p.info['cmdline']
   if args and Path(args[0]).resolve()==binary:matches.append(p)
  except (psutil.NoSuchProcess,psutil.AccessDenied):pass
 assert len(matches)==1,[(p.pid,p.cmdline()) for p in matches]
 proc=matches[0]
 info={'pid':proc.pid,'params':str(params),'rss_before_gc':proc.memory_info().rss,'instrumented':True}
 for name,query in [('heap-before.txt','heap?debug=1'),('heap-after.pb.gz','heap?gc=1'),('heap-after.txt','heap?debug=1')]:
  with urllib.request.urlopen('http://127.0.0.1:19191/debug/pprof/'+query,timeout=90) as r:(out/name).write_bytes(r.read())
 info['rss_after_gc']=proc.memory_info().rss
 for name,args in [('vmmap-summary.txt',['vmmap','-summary',str(proc.pid)]),('vmmap-resident.txt',['vmmap','-resident',str(proc.pid)])]:
  r=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
  (out/name).write_bytes(r.stdout);info[name+'_exit']=r.returncode
 (out/'snapshot.json').write_text(json.dumps(info,indent=2)+'\n')
 print('Memory snapshot',out,info,flush=True)
 return result
run.measure=profile
run.main()
