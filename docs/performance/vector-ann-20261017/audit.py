import json,math,re,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'testing/benchmarks/vector_ann'))
import run
root=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).resolve().parent
source='7c201376d1203a651fc625869fb98df7be746ac4'
baseline='239b057e7c8797597b757d8a4b948c1af6034572'
expected={
 'ram-original':{('nornicdb','bq-asym8+rescore8-ram-original')},
 'baseline':{('nornicdb','hnsw-fast+original-file')},
 'file-current':{('nornicdb','hnsw-fast+original-file')},
 'cpu':{(engine,m['name']) for m in run.MODES for engine,key in [('nornicdb','nornic'),('qdrant','qdrant')] if key in m},
 'rescore8':{(engine,m+'+rescore8') for m in ['pq-x16','pq-x32','pq-x64','bq','bq-asym8'] for engine in ['nornicdb','qdrant']},
}
corpus=None;checked=0;curves=0
for phase,required in expected.items():
 results={}
 for path in (root/phase).glob('*.json'):
  d=json.loads(path.read_text())
  if 'engine' in d:results[(d['engine'],d['mode'])]=(path,d)
 assert set(results)==required,(phase,'missing',required-set(results),'unexpected',set(results)-required)
 for identity,(path,d) in results.items():
  assert (d['vectors'],d['loaded_points'],d['dimensions'],d['k'])==(88155,88155,1024,10),path
  settings=d['provenance']['nornic_build_settings']
  assert settings['vcs.revision']==(baseline if phase=='baseline' else source),path
  assert settings['vcs.modified']=='false',path
  hashes=d['provenance']['corpus_sha256']
  if corpus is None:corpus=hashes
  assert hashes==corpus,path
  assert d['rss_bytes_after_queries']>0 and d['disk_bytes']>0,path
  mode=next(m for m in run.MODES if m['name']==d['base_mode'])
  assert [r['hnsw_ef'] for r in d['runs']]==([None,16,32,64,128,256,512] if mode.get('sweep') else [None]),path
  for r in d['runs']:
   assert r['queries']==1000 and r['concurrency']==8,path
   assert 0<=r['recall@10']<=1,path
   assert all(math.isfinite(r[k]) and r[k]>0 for k in ['p50_ms','p95_ms','p99_ms','qps']),path
   assert r['p50_ms']<=r['p95_ms']<=r['p99_ms'],path
   if mode.get('qdrant',{}).get('quantization'):
    q=r['search_params']['quantization']
    assert q['rescore'] is True,path
    want=8 if phase in ('rescore8','ram-original') else 2 if mode['qdrant']['quantization']=='binary' else 1
    assert q['oversampling']==want,path
   curves+=1
  if d['engine']=='nornicdb':
   assert d['settings']['NORNICDB_GPU_BACKEND']=='cpu' and not d['gpu_requested'],path
   if d['strategy'].startswith('HNSW'):
    assert d['build_backend']=='CPU',path
    text=path.with_suffix('.log').read_text()
    built=re.findall(r'HNSW index created: [^\n]*? vectors=(\d+)',text)
    assert built and int(built[-1])==88155,(path,'constructed population',built)
    q=mode.get('qdrant',{}).get('quantization')
    if q:
     match=re.search(r'\((\d+) encoded bytes\)',d['strategy']);assert match,path
     expected_bytes=88155*1028 if q=='scalar' else 88155*128 if q=='binary' else 88155*(1024//(mode['qdrant'].get('compression',16)//4))+1024*256*4
     assert int(match[1])==expected_bytes,(path,match[1],expected_bytes)
  checked+=1
print(json.dumps({'cases':checked,'query_curves':curves,'vectors_per_case':88155,'dimensions':1024,'queries_per_curve':1000,'timed_source':source,'baseline_source':baseline,'corpus_sha256':corpus},indent=2))
