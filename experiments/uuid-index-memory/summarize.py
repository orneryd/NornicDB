#!/usr/bin/env python3
import json,statistics
from pathlib import Path
here=Path(__file__).resolve().parent/'results'
summary=[]
for filename in ['verified.jsonl','builder.jsonl','rejected.jsonl']:
 p=here/filename
 if not p.exists():continue
 rows=[json.loads(l) for l in p.read_text().splitlines()]
 groups={}
 for r in rows:groups.setdefault((r['n'],r['corpus'],r['sparse'],r['mode']),[]).append(r)
 for key,rs in groups.items():
  r=dict(zip(['n','corpus','sparse','mode'],key));r['file']=filename;r['samples']=len(rs)
  for name in ['retained_bytes','scan_heap_bytes','retained_objects','forward_hit_ns','forward_miss_ns','reverse_hit_ns','reverse_bytes_per_op','build_ms','half_deleted_retained_bytes','churn_retained_bytes','rebuilt_retained_bytes','rebuild_ms']:
   vals=[x[name] for x in rs if name in x]
   if vals:r[name]={'median':statistics.median(vals),'min':min(vals),'max':max(vals)}
  summary.append(r)
(here/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for r in summary:
 if r['n']==1000000 and r['corpus']=='namespace':print(r['file'],r['mode'],r['samples'],round(r['retained_bytes']['median']/1048576,2),round(r['forward_hit_ns']['median'],1),round(r['reverse_hit_ns']['median'],1))
