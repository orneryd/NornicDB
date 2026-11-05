#!/usr/bin/env python3
"""Recheck dense and sparse behavior of the rejected paged reverse structures."""
import argparse,json,os,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);a=p.parse_args()
out=Path(__file__).resolve().parent/'results'/'rejected.jsonl'
env=dict(os.environ,GOMAXPROCS='4',GOGC='100',GOMEMLIMIT='off')
with out.open('w') as f:
 for n,sparse in [(1000000,False),(4096,True)]:
  for repeat in range(1,4):
   for mode in ['paged','packed']:
    cmd=[a.binary,'-mode',mode,'-n',str(n),'-corpus','namespace']+(['-sparse'] if sparse else [])
    r=json.loads(subprocess.check_output(cmd,text=True,env=env));r['repeat']=repeat;f.write(json.dumps(r)+'\n');f.flush();print(n,sparse,repeat,mode,flush=True)
