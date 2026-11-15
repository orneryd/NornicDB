import concurrent.futures,json,os,sys,time,urllib.request,re
from pathlib import Path
os.environ['NORNICDB_PPROF_ENABLED']='true'
os.environ['NORNICDB_PPROF_LISTEN']='127.0.0.1:19091'
sys.argv=['profile','--old-bin','/baseline/nornicdb','--new-bin','/src/nornicdb-scan-fixed','--old-data','/lab-data/settled-comparison-0jswgod6/stock-northwind','--new-data','/lab-data/settled-comparison-0jswgod6/fast-northwind','--client','/tmp/northwind-report','--docs','/corpus','--out','/profiles/settled-scan-fixed-cpu','--prepared']
scope={}
exec(Path('/src/experiments/property-storage/projected-query/run_lab.py').read_text().split('for label in (() if args.prepared')[0],scope)
out=scope['out']
queries=dict(re.findall(r'name:\s*"([^"]+)"[\s\S]*?cypher:\s*`([^`]+)`',Path('/src/testing/benchmarks/northwind_power/main.go').read_text()))
def fetch(route,path):
    with urllib.request.urlopen('http://127.0.0.1:19091/debug/pprof/'+route,timeout=90) as r: path.write_bytes(r.read())
def query(q):
    data=json.dumps({'statements':[{'statement':q}]}).encode()
    req=urllib.request.Request('http://127.0.0.1:17474/db/nornic/tx/commit',data=data,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=120) as r: result=json.load(r)
    assert not result.get('errors'),result
    return result
for label in ('new',):
    proc,log=scope['start'](label,label)
    try:
        for name in ('low_stock_products','products_in_category'):
            print(label,name,flush=True)
            query(queries[name])
            prefix=out/(label+'-'+name)
            fetch('heap?gc=1',Path(str(prefix)+'-heap-before.pb.gz'))
            fetch('heap?gc=1',Path(str(prefix)+'-heap-before.pb.gz'))
            fetch('allocs',Path(str(prefix)+'-alloc-before.pb.gz'))
            with concurrent.futures.ThreadPoolExecutor(1) as pool:
                profile=pool.submit(fetch,'profile?seconds=12',Path(str(prefix)+'-cpu.pb.gz'))
                for _ in range(24): result=query(queries[name])
                profile.result()
            Path(str(prefix)+'-result.json').write_text(json.dumps(result))
            fetch('heap?gc=1',Path(str(prefix)+'-heap-after.pb.gz'))
            fetch('heap?gc=1',Path(str(prefix)+'-heap-after.pb.gz'))
            fetch('allocs',Path(str(prefix)+'-alloc-after.pb.gz'))
    finally: scope['stop'](proc,log)


