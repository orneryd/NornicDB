"""Verify saved experiment receipts and regenerate the review report."""
import json
import pathlib
import re
import statistics

root = pathlib.Path(__file__).resolve().parent
receipts = root / 'receipts'
def read(name):
    return json.loads((receipts / name).read_text())

docs = [read('docs-%d.json' % i) for i in range(1, 4)]
assert len({r['CorpusSHA256'] for run in docs for r in run}) == 1
med = lambda enabled, field: statistics.median(next(r[field] for r in run if r['Enabled'] == enabled) for run in docs)
off = med(False, 'DirectoryBytes')
on = med(True, 'DirectoryBytes')
lines = ['# Experimental property storage: integration results', '',
'Branch: `experiment/projected-badger-v1.5.0`, based on release head `ddc59dc20`. Production implementation: `0621c3dbb`; additional numeric and projection checks: `a6ef83d44`. These measurements used public Badger fork `2f4175a83c6c`, before cross-record SST integration. They do not qualify the newer `adf08d82495b` layout. The repository has not been renamed.', '',
'## Feature flag', '',
'`NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED` defaults to false. It controls new per-property encodings through the existing node/edge property owner. This branch reads its experimental records with the flag disabled. Stock release binaries cannot read those physical extensions; use disposable test stores. No cross-entity column segments or physical partial disk reads are claimed.', '',
'## Documentation-derived Nornic store', '',
'273 Markdown documents from the documentation tree at `0621c3dbb`, including the experimental guide, seed stable path-hash IDs. Each node stores original text, 256 low-range integer metrics, a status and 4 KiB of independently hash-derived bytes. Three independent runs compare normal and experimental writes through public Nornic APIs, then verify projections, full values and reopen. File lengths include every closed-store file; they are not allocated filesystem sectors. Badger already uses its normal compression in both cases. Corpus SHA-256: `%s`.' % docs[0][0]['CorpusSHA256'], '',
'| Metric | Flag off | Flag on |', '|---|---:|---:|',
'| Median total file bytes | %s | %s |' % (format(int(off), ','), format(int(on), ',')),
'| Median seed wall time, ms | %.2f | %.2f |' % (med(False,'SeedNS')/1e6,med(True,'SeedNS')/1e6), '',
'**Total file-size reduction: %.2f%%.** The short seed measurements include cold codec setup and show a write-time cost. Read timings in these receipts include warm caches and coarse Windows timers, so they do not establish a general read-speed or RSS improvement.' % (100*(1-on/off)), '',
'## Projected-array decoder', '',
'CPU=1, three 300 ms Go benchmark repetitions on Windows/amd64, i9-9900KF. Select one small property while omitting a 4,096-value integer array from an already resident node body. This calls the canonical Nornic property decoder; it is not a server or disk benchmark.', '',
'| Omitted array | Flag off, median ns | Flag on, median ns | Bytes/op, both | Allocs/op, both |', '|---|---:|---:|---:|---:|']
bench = (receipts/'projection-bench.log').read_text()
for shape in ('low-range','random-int64'):
    values = {}
    for state in ('false','true'):
        matches = re.findall(r'BenchmarkExperimentalProjectedPropertySkip/'+re.escape(shape)+r'/enabled='+state+r'\s+\d+\s+(\d+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op',bench)
        assert len(matches) == 3
        values[state] = tuple(int(statistics.median(int(v[i]) for v in matches)) for i in range(3))
    assert values['false'][1:] == values['true'][1:]
    lines.append('| %s | %s | %s | %d | %d |' % (shape,format(values['false'][0],','),format(values['true'][0],','),values['true'][1],values['true'][2]))
lines += ['', 'A compressed array is skipped by its extension length instead of walking each MessagePack element. Random integers stay in the ordinary representation and show no comparable gain. Allocation counts do not improve in this benchmark.', '',
'## Northwind: clean comparison', '',
'The repository benchmark script ran with 48,000 products, 48,000 orders, 96 categories, 144 suppliers, 1,200 customers, 30 measured iterations and five warmups. Each run seeded **97,440 nodes and 312,050 relationships**. Both parser modes ran serially from fresh stores. Async writes were disabled; graph-only search settings and seed/batch parameters were identical. Linux Docker laboratory: four CPU quota, 10 GiB memory limit, Go 1.27.3, `noui,nolocalllm` build tags. No competing test jobs ran during this final pair.', '',
'| Parser | Seed off/on, s | Mean query off/on, ms | Allocated disk off/on, bytes |', '|---|---:|---:|---:|']
for mode in ('nornicdb','nornicdb-antlr'):
    a = read('control-off-%s.json'%mode)
    b = read('control-on-%s.json'%mode)
    assert a['seed_counts'] == b['seed_counts']
    assert a['seed_nodes'] == b['seed_nodes'] == 97440
    assert a['seed_relationships'] == b['seed_relationships'] == 312050
    assert len(a['queries']) == len(b['queries']) == 14
    for x,y in zip(a['queries'],b['queries']):
        assert x['correctness_ok'] and y['correctness_ok']
        assert (x['name'],x['row_count'],x['result_hash']) == (y['name'],y['row_count'],y['result_hash'])
    da = (receipts/('control-off-%s.disk-bytes.txt'%mode)).read_text().strip()
    db = (receipts/('control-on-%s.disk-bytes.txt'%mode)).read_text().strip()
    lines.append('| %s | %.3f / %.3f | %.4f / %.4f | %s / %s |' % (mode,a['seed_duration_ms']/1000,b['seed_duration_ms']/1000,a['overall_mean_ms'],b['overall_mean_ms'],format(int(da),','),format(int(db),',')))
lines += ['',
'All 14 query row counts and built-in fingerprints match in both modes. The benchmark normalizes floats to 12 significant digits for fingerprints; raw aggregation samples can differ in final floating-point summation bits. Separate storage tests verify exact IEEE bits, including signed zero, infinities, subnormals and distinct NaN payloads.', '',
'This is one clean pair, not a statistical non-regression bound. Warm query means include the existing result-cache behavior. Northwind primarily exercises small scalar properties, so it is a compatibility and overhead check rather than a prediction of large-property gains. Disk values here come from the existing script\'s `du -sk`, unlike the documentation fixture\'s file-length sums.', '',
'## Earlier runs and test status', '',
'The initial stock-release versus feature-on full-size receipts also match all 14 fingerprints in both modes. Their timings were contaminated by competing tests/race compilation, including one much slower seed. They are retained as `initial-*` evidence and are not used for the clean timing conclusion.', '',
'- Passed: focused configuration and storage tests; fresh-process default-off/environment tests; packed integer and compressed float-array type checks; projection skip/corruption checks; logical snapshot, native backup/restore and reopen checks.',
'- Passed: the Cypher large-property regression under both Nornic and ANTLR, including string equality, unique constraints, updates and reads after disabling writes.',
'- Passed: focused config/storage race tests, fork package race tests and affected Nornic/fork package vet.',
'- Full `make test` with `noui,nolocalllm`: **stopped at the maintainer request while the broader storage implementation continues**. Full suites are deferred. This is not a full-suite pass or a production-readiness claim.',
'- The first Linux full run was superseded after temporary-module source-analysis checks inherited the laboratory Go workspace. The same workcensus package passed with `GOWORK=off` and the exact dependency in the module cache. A Windows make attempt failed at Unix-shell lint recipes; an older Windows storage run was canceled when superseded. Neither is counted as a pass.', '',
'## Reproduce', '',
'See [the integration guide](../../docs/development/experimental-property-storage.md) for the runtime flag and compatibility boundaries. The fork is public; ordinary dependency resolution needs no private GitHub credential. The saved JSON, benchmark logs and this verifier are review evidence.', '',
'```sh',
'python experiments/property-storage/verify_results.py',
"GOFLAGS='-tags=noui,nolocalllm' make test",
"go test -race -tags noui,nolocalllm ./pkg/config ./pkg/storage -run '^TestExperimental' -count=1",
"go test -tags noui,nolocalllm ./pkg/storage -run '^$' -bench '^BenchmarkExperimentalProjectedPropertySkip$' -benchmem -benchtime=300ms -count=3 -cpu=1",
'```', '']
(root/'REPORT.md').write_text('\n'.join(lines),encoding='utf-8')
print('Verified documentation receipts, projection medians and Northwind fingerprints.')
