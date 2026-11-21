"""Render the saved paired timing and memory receipts; no database operations."""
import argparse
import json
from pathlib import Path
from statistics import median

root=Path(__file__).parent/'results'
repo='https://github.com/orneryd/NornicDB'
branch='feature/projected-reads-v1.5.0'
labels={'document_projection':'Document point','northwind_projection':'Product point','mixed_projection':'50/50 mixed point','document_batch_projection':'Batch: 16 documents','document_full_control':'Widen to large fields','document_full_warm_control':'Repeat large fields'}

def paired(name):
 folder=root/name; meta=json.loads((folder/'metadata.json').read_text(encoding="utf-8")); groups={}
 field='queries' if meta['workload']=='northwind' else 'phases'
 for clients in meta['clients']:
  groups[clients]={side:[json.loads((folder/f'{side}-{clients}-{trial}.json').read_text(encoding="utf-8")) for trial in meta['trials']] for side in ['old','new']}
  for old,new in zip(groups[clients]['old'],groups[clients]['new']):
   assert [x['name'] for x in old[field]]==[x['name'] for x in new[field]]
   for a,b in zip(old[field],new[field]):
    if field=='queries': assert a['correctness_ok'] and b['correctness_ok'] and (a['row_count'],a['result_hash'])==(b['row_count'],b['result_hash'])
    else: assert old['corpus_sha256']==new['corpus_sha256'] and (a['queries'],a['result_sha256'])==(b['queries'],b['result_sha256'])
 return meta,groups

def stats(name,clients):
 meta,groups=paired(name)
 assert meta['measurement_kind']=='timing'
 return {side:{phase['name']:{key:median(run['phases'][i][key] for run in groups[clients][side]) for key in ['mean_ms','p95_ms','p99_ms','queries_second','sampled_server_peak_rss_kib']} for i,phase in enumerate(groups[clients][side][0]['phases'])} for side in ['old','new']}

def table(headers,rows):
 return '\n'.join(['| '+' | '.join(headers)+' |','| '+' | '.join(['---']*len(headers))+' |']+['| '+' | '.join(map(str,row))+' |' for row in rows])

def delta(old,new): return (new/old-1)*100

def render(pr=False):
 link=lambda path: (f'{repo}/blob/{branch}/testing/benchmarks/projected_reads/results/'+path) if pr else path
 folder_link=lambda path: (f'{repo}/tree/{branch}/testing/benchmarks/projected_reads/results/'+path) if pr else path
 text=[]
 if not pr:text+=['# Default projected-read comparison','']
 text+=['Release `a492f2acd` → PR runtime `9653bb773`. No feature flag or experimental format is used. Both builds read fresh copies of identical ordinary-format fixtures. Query-result caching is disabled; normal storage/parser/OS caches remain.','']
 mixed=stats('mixed-timing',8);a=mixed['old']['mixed_projection'];b=mixed['new']['mixed_projection']
 text+=[f'At eight clients, mixed-point p95 is **{a["p95_ms"]:.3f} → {b["p95_ms"]:.3f} ms** ({delta(a["p95_ms"],b["p95_ms"]):+.1f}%), and throughput is **{a["queries_second"]:,.0f} → {b["queries_second"]:,.0f} queries/s** ({delta(a["queries_second"],b["queries_second"]):+.1f}%).','', '### Mixed corpus: eight clients','']
 rows=[]
 for name,title in labels.items():
  a,b=mixed['old'][name],mixed['new'][name]
  rows.append([title,f'{a["p95_ms"]:.3f}',f'{b["p95_ms"]:.3f}',f'{a["p99_ms"]:.3f}',f'{b["p99_ms"]:.3f}'])
 text+=[table(['Workload','Release p95 ms','PR p95 ms','Release p99 ms','PR p99 ms'],rows),'','Four alternating trials, 8,000 measured calls per phase and 64 warm-ups. Values are medians of per-run statistics, not pooled percentiles. Batch p99 is worse despite lower p95; this is not a universal latency improvement.','', '### Sustained mixed concurrency','']
 rows=[]
 for clients in [8,16,32]:
  s=stats('mixed-scaling',clients);a,b=s['old']['mixed_projection'],s['new']['mixed_projection']
  rows.append([clients,f'{a["queries_second"]:,.0f} → {b["queries_second"]:,.0f}',f'{a["p95_ms"]:.3f} → {b["p95_ms"]:.3f}',f'{a["p99_ms"]:.3f} → {b["p99_ms"]:.3f}',f'{a["sampled_server_peak_rss_kib"]/1024:,.0f} → {b["sampled_server_peak_rss_kib"]/1024:,.0f}'])
 text+=[table(['Clients','Queries/s: release → PR','p95 ms: release → PR','p99 ms: release → PR','Peak RSS MiB: release → PR'],rows),'','120,000 measured calls per run, 8,192 warm-ups and two alternating trials. Peak RSS is sampled from the server every 10 ms. Throughput includes client fingerprint work and shares the four-CPU quota; query latency excludes fingerprint hashing.','']
 cold=stats('large-field-cold',8);a=cold['old']['document_full_control'];b=cold['new']['document_full_control']
 text+=[f'The equal process-cold large-field control improves p95 from **{a["p95_ms"]:.3f} to {b["p95_ms"]:.3f} ms** at eight clients. The first large-field phase in the mixed sequence instead pays for widening previously narrow images; the release had already cached those fields. The repeated large-field phase follows that fill. These controls return body/noise/metrics properties, not whole entities. Process-cold does not mean cold device caches.','', '### Northwind: all 14 queries','']
 if (root/'northwind-timing/metadata.json').exists():
  meta,groups=paired('northwind-timing');groups=groups[1];rows=[]
  for i,q in enumerate(groups['old'][0]['queries']):
   a=median(r['queries'][i]['mean_ms'] for r in groups['old']);b=median(r['queries'][i]['mean_ms'] for r in groups['new'])
   rows.append([f'`{q["name"]}`',f'{a:,.2f}',f'{b:,.2f}',f'{delta(a,b):+.1f}%'])
  a=median(r['overall_mean_ms'] for r in groups['old']);b=median(r['overall_mean_ms'] for r in groups['new']);rows.append(['**Overall mean**',f'**{a:,.2f}**',f'**{b:,.2f}**',f'**{delta(a,b):+.1f}%**'])
  text+=[table(['Query','Release mean ms','PR mean ms','Change'],rows),'','Two alternating-order trials, three measured calls per query per trial and one reference/warm-up call. These six observations per query do not establish stable tail percentiles. All row counts and canonical result fingerprints match.','']
  rss={side:median(r['sampled_server_rss']['peak_kib'] for r in runs)/1024 for side,runs in groups.items()}
  text+=[f'Northwind median sampled peak RSS: **{rss["old"]:,.0f} → {rss["new"]:,.0f} MiB**.','']
 else:text+=['Paired timing trials are still running. No memory-pass latency numbers are substituted here.','']
 text+=['### Memory profiles','']
 memory=json.loads((root/'memory-summary.json').read_text(encoding="utf-8"));rows=[]
 for name,title in [('mixed-memory','Mixed: 20,000 calls + 64 warm-ups, 8 clients'),('northwind-memory','Northwind: reference + 1 measured call/query')]:
  a,b=memory[name]['old'],memory[name]['new'];rows.append([f'[{title}]({folder_link(name+"/")})',f'{a["alloc_bytes"]/1048576:,.1f} → {b["alloc_bytes"]/1048576:,.1f}',f'{delta(a["alloc_bytes"],b["alloc_bytes"]):+.1f}%',f'{a["heap_bytes"]/1048576:,.1f} → {b["heap_bytes"]/1048576:,.1f}',f'{delta(a["heap_bytes"],b["heap_bytes"]):+.1f}%'])
 text+=[table(['Separate profiling pass','Allocated MiB: release → PR','Change','Retained heap MiB: release → PR','Change'],rows),'','Allocated bytes are cumulative sampled allocations, not live RAM. Heap snapshots follow two explicit GCs. Northwind allocations are essentially unchanged and its sampled retained heap is slightly higher; no Northwind memory reduction is established. Profiling-pass latency fields are excluded from timing tables. Raw `.pb.gz` snapshots and allocation/heap top reports are linked in the dataset folders above.','']
 if not pr:
  text+=['### Single-client results','']
  s=stats('mixed-timing',1);rows=[]
  for name,title in labels.items():
   a,b=s['old'][name],s['new'][name];rows.append([title,f'{a["mean_ms"]:.3f} → {b["mean_ms"]:.3f}',f'{a["p95_ms"]:.3f} → {b["p95_ms"]:.3f}',f'{a["p99_ms"]:.3f} → {b["p99_ms"]:.3f}',f'{a["queries_second"]:,.0f} → {b["queries_second"]:,.0f}'])
  text+=[table(['Workload','Mean ms: release → PR','p95 ms: release → PR','p99 ms: release → PR','Queries/s: release → PR'],rows),'','### Method and reproduction','','- Go 1.27.3, Linux/amd64, `noui,nolocalllm`, four CPUs, 10 GiB container memory, `GOGC=400`, Go soft limit 8 GiB. No builds, tests or pprof processes overlapped timed queries.','- The mixed fixture has 97,440 Northwind nodes and 312,050 relationships, plus 4,352 document records. The pure Northwind fixture omits document records.','- Both source copies use the same table layout: combined fixture in L6; pure Northwind in four L0 tables. No data-format changes or storage-size gains are claimed.','- Frozen documentation comes from `ddc59dc20:docs` with CRLF line endings: 272 files × 16 replicas. Corpus SHA-256: `2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`. Both binaries verified all stored document fields against that source.','- Document identities, text, status, 4 KiB independent byte noise and numeric lists are deterministic. The [file manifest](corpus-manifest.json), [binary provenance](provenance.json) and [fixture receipt](fixture-receipt.json) identify the inputs.','- The format-compatible Badger fork changes only its integration note relative to upstream v4.9.6. No fork-runtime optimization is included in this comparison.','','See the [harness instructions](../README.md). Each dataset folder records the full command settings in `metadata.json` and contains every paired result receipt. `verify/` contains whole-corpus verification output. Regenerate this report with `python3 testing/benchmarks/projected_reads/summarize.py`.','','Inspect a memory capture locally after downloading its folder:','','```sh','go tool pprof -top -sample_index=alloc_space -base mixed-memory/old-8-1-alloc-before.pb.gz mixed-memory/old-8-1-alloc-after.pb.gz','go tool pprof -top -sample_index=inuse_space mixed-memory/new-8-1-heap-after.pb.gz','```','']
 else:text+=[f'[Full report, single-client controls and raw receipts]({link("README.md")}) · [Reproduction harness]({repo}/tree/{branch}/testing/benchmarks/projected_reads)','']
 return '\n'.join(text)

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--pr',action='store_true');args=p.parse_args()
 if args.pr:print(render(True))
 else:(root/'README.md').write_text(render(),encoding='utf-8')