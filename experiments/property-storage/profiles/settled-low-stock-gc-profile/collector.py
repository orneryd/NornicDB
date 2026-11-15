"""Focused HTTP regression check using unchanged Northwind query definitions."""
import hashlib,json,math,os,re,statistics,sys,time,urllib.request
from pathlib import Path
os.environ['GODEBUG']='gctrace=1'
sys.argv=['focused','--old-bin','/baseline/nornicdb','--new-bin','/src/nornicdb-scan-fixed','--old-data','/lab-data/settled-comparison-0jswgod6/stock-northwind','--new-data','/lab-data/settled-comparison-0jswgod6/fast-northwind','--client','/tmp/northwind-report','--docs','/corpus','--out','/profiles/settled-low-stock-gc-profile','--prepared','--fixture-receipt','/profiles/fixture-normalization/metadata.json']
scope={}
exec(Path('/src/experiments/property-storage/projected-query/run_lab.py').read_text().split('for label in (() if args.prepared')[0],scope)
out=scope['out']
queries=dict(re.findall(r'name:\s*"([^"]+)"[\s\S]*?cypher:\s*`([^`]+)`',Path('/src/testing/benchmarks/northwind_power/main.go').read_text()))
names=('low_stock_products','products_in_category')
def query(q):
    data=json.dumps({'statements':[{'statement':q}]}).encode()
    req=urllib.request.Request('http://127.0.0.1:17474/db/nornic/tx/commit',data=data,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=120) as r: result=json.load(r)
    assert not result.get('errors'),result
    return hashlib.sha256(json.dumps(result['results'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
expected=None
for side in ('old','new'):
    process_start=time.monotonic()
    proc,log=scope['start'](side,side)
    try:
        samples=[]
        for i in range(100):
            start=time.monotonic(); digest=query(queries['low_stock_products']); end=time.monotonic()
            if expected is None: expected=digest
            assert digest==expected
            samples.append({'request':i,'start_s':start-process_start,'end_s':end-process_start,'duration_ms':(end-start)*1000})
        (out/(side+'.json')).write_text(json.dumps(samples,indent=2)+'\n')
        print(side,'captured 100 request boundaries with GC tracing',flush=True)
    finally: scope['stop'](proc,log)
(out/'metadata.json').write_text(json.dumps({'purpose':'profile remaining low-stock tails against GC intervals, not a benchmark','queries_per_side':100,'old_binary':'/baseline/nornicdb','new_binary':'/src/nornicdb-scan-fixed','godebug':'gctrace=1','query_cache_size':0,'result_sha256':expected},indent=2)+'\n')
