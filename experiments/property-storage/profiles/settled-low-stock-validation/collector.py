"""Focused HTTP regression check using unchanged Northwind query definitions."""
import hashlib,json,math,os,re,statistics,sys,time,urllib.request
from pathlib import Path
os.environ.pop('NORNICDB_PPROF_ENABLED',None)
sys.argv=['focused','--old-bin','/baseline/nornicdb','--new-bin','/src/nornicdb-scan-fixed','--old-data','/lab-data/settled-comparison-0jswgod6/stock-northwind','--new-data','/lab-data/settled-comparison-0jswgod6/fast-northwind','--client','/tmp/northwind-report','--docs','/corpus','--out','/profiles/settled-low-stock-validation','--prepared','--fixture-receipt','/profiles/fixture-normalization/metadata.json']
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
expected={}
for trial in range(1,5):
    for side in (('old','new') if trial%2 else ('new','old')):
        tag=f'{side}-{trial}'
        proc,log=scope['start'](side,tag)
        try:
            records=[]
            for name in names:
                digest=query(queries[name]); expected.setdefault(name,digest)
                assert digest==expected[name],(tag,name,'reference mismatch')
                for _ in range(2): assert query(queries[name])==digest
                samples=[]
                for _ in range(30):
                    started=time.perf_counter(); actual=query(queries[name]); samples.append((time.perf_counter()-started)*1000)
                    assert actual==digest,(tag,name,'result mismatch')
                ordered=sorted(samples)
                row={'name':name,'queries':30,'mean_ms':statistics.mean(samples),'p50_ms':statistics.median(samples),'p95_ms':ordered[math.ceil(len(samples)*.95)-1],'p99_ms':ordered[-1],'result_sha256':digest,'samples_ms':samples}
                records.append(row)
                print(tag,name,round(row['mean_ms'],3),round(row['p95_ms'],3),flush=True)
            (out/(tag+'.json')).write_text(json.dumps(records,indent=2)+'\n')
        finally: scope['stop'](proc,log)
(out/'metadata.json').write_text(json.dumps({'protocol':'HTTP transaction commit','query_definitions':'testing/benchmarks/northwind_power/main.go','old_binary':'/baseline/nornicdb','new_binary':'/src/nornicdb-scan-fixed','trials':4,'order':'alternating','warmup':2,'reference':1,'samples':30,'query_cache_size':0,'fixture_receipt':scope['fixture_receipt'],'query_hashes':expected,'note':'HTTP totals include response parsing and hashing; not comparable to Bolt-only timings.'},indent=2)+'\n')
print('All focused query fingerprints match.',flush=True)
