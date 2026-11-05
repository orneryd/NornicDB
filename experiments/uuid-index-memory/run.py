#!/usr/bin/env python3
"""Run fresh processes sequentially; never compare Go versions or shared heaps."""
import argparse, hashlib, json, os, platform, subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--repeats',type=int,default=3);p.add_argument('--modes',default='baseline,paged,packed,packed-hash');p.add_argument('--output',default='final.jsonl');a=p.parse_args()
root=Path(__file__).resolve().parent; out=root/'results';out.mkdir(exist_ok=True)
env=dict(os.environ,GOMAXPROCS='4',GOGC='100',GOMEMLIMIT='off')
meta={'platform':platform.platform(),'cpu':next((l.split(':',1)[1].strip() for l in Path('/proc/cpuinfo').read_text().splitlines() if l.startswith('model name')),''),'env':{k:env[k] for k in ['GOMAXPROCS','GOGC','GOMEMLIMIT']},'repeats':a.repeats}
source=Path(a.binary).resolve().parent/'main.go'
if not source.exists():source=root/'main.go'
meta['source_sha256']=hashlib.sha256(source.read_bytes()).hexdigest()
meta['base_commit']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
(out/(Path(a.output).stem+'-environment.json')).write_text(json.dumps(meta,indent=2)+'\n')
with (out/a.output).open('w') as f:
 for n,corpus,sparse in [(100000,'namespace',False),(1000000,'namespace',False),(1000000,'mixed',False),(4096,'namespace',True)]:
  for repeat in range(a.repeats):
   for mode in a.modes.split(','):
    args=[a.binary,'-mode',mode,'-n',str(n),'-corpus',corpus]+(['-sparse'] if sparse else [])
    r=json.loads(subprocess.check_output(args,env=env,text=True));r['repeat']=repeat+1;f.write(json.dumps(r)+'\n');f.flush()
    print(n,corpus,sparse,repeat+1,mode,round(r['retained_bytes']/1048576,2),'MiB',flush=True)
