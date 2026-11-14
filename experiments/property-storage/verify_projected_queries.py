"""Validate raw Bolt receipts and print median per-run measurements."""
import json
from pathlib import Path
from statistics import median

root=Path(__file__).parent/'profiles'
for directory in ('combined-query','combined-query-scans','point-tuned-query','point-seek-query'):
    folder=root/directory
    if not folder.exists(): continue
    print('\n'+directory)
    for clients in (1,8):
        groups={}
        for label in ('old','new'):
            runs=[json.loads((folder/f'{label}-{clients}-{trial}.json').read_text()) for trial in (1,2,3)]
            assert len({r['corpus_sha256'] for r in runs})==1
            groups[label]=runs
        assert groups['old'][0]['corpus_sha256']==groups['new'][0]['corpus_sha256']
        names=[p['name'] for p in groups['old'][0]['phases']]
        assert all([p['name'] for p in run['phases']]==names for runs in groups.values() for run in runs)
        for index,phase in enumerate(groups['old'][0]['phases']):
            for trial in range(3):
                old=groups['old'][trial]['phases'][index]
                new=groups['new'][trial]['phases'][index]
                assert (old['name'],old['queries'],old['result_sha256'])==(new['name'],new['queries'],new['result_sha256'])
            result=[]
            for label in ('old','new'):
                phases=[r['phases'][index] for r in groups[label]]
                result.append({key:round(median(p[key] for p in phases),4) for key in ('mean_ms','p50_ms','p95_ms','p99_ms','queries_second','sampled_server_peak_rss_kib')})
            print(clients,phase['name'],json.dumps(result))
print('All compared fingerprints match.')