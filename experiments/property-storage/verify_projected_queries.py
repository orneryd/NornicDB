"""Validate saved paired receipts and print medians of per-run statistics."""
import json
from pathlib import Path
from statistics import median

root=Path(__file__).parent/'profiles'
directories=('combined-query','combined-query-scans','point-tuned-query','point-seek-query','point-fixed-query','point-final-query','point-current-query','point-current-full-cold','direct-property-ab','mixed-scaling','northwind-current-warm','settled-warm-validation','settled-scaling-validation','settled-cold-validation','settled-northwind-uncached')
for directory in directories:
 folder=root/directory
 if not folder.exists(): continue
 metadata=json.loads((folder/'metadata.json').read_text())
 clients=metadata.get('clients',(1,8))
 trials=metadata.get('trials',(1,2,3))
 northwind=metadata.get('workload')=='northwind'
 print('\n'+directory)
 for workers in clients:
  groups={label:[json.loads((folder/f'{label}-{workers}-{trial}.json').read_text()) for trial in trials] for label in ('old','new')}
  field='queries' if northwind else 'phases'
  names=[p['name'] for p in groups['old'][0][field]]
  assert all([p['name'] for p in run[field]]==names for runs in groups.values() for run in runs)
  if northwind:
   assert len(names)==14
  else:
   assert len({r['corpus_sha256'] for runs in groups.values() for r in runs})==1
  for index,name in enumerate(names):
   for old,new in zip(groups['old'],groups['new']):
    a,b=old[field][index],new[field][index]
    if northwind:
     assert a['correctness_ok'] and b['correctness_ok']
     assert (a['name'],a['row_count'],a['result_hash'])==(b['name'],b['row_count'],b['result_hash'])
    else:
     assert (a['name'],a['queries'],a['result_sha256'])==(b['name'],b['queries'],b['result_sha256'])
   keys=('mean_ms','median_ms','p95_ms','p99_ms') if northwind else ('mean_ms','p50_ms','p95_ms','p99_ms','queries_second','sampled_server_peak_rss_kib')
   result=[{key:round(median(r[field][index][key] for r in groups[label]),4) for key in keys} for label in ('old','new')]
   print(workers,name,json.dumps(result))
  if northwind:
   print('overall_mean_ms',[median(r['overall_mean_ms'] for r in groups[label]) for label in ('old','new')])
print('All compared fingerprints match.')
