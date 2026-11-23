# N152-90 controlled public retrieval measurements

Release baseline: `18b36a85d10e775b9d5d3500ecf3482682754a8d`.
Local macOS arm64; `GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOGC=60`, Go 1.27.4,
`-tags=noui,nolocalllm`, concurrency1, public first-page retrieval. Explicit
1024-dimensional embeddings, controlled reranker, result caches disabled; no
external provider or query-embedding latency. Fixture/index setup is outside the
benchmark timers. CPU/memory profiles include setup and background work, so
whole-profile percentages are not pure measured-operation percentages.

No owned Go build/test workload overlapped these final measurements. Ordinary
macOS background services were active. These are named local controls, not the
private provider-included latency report or the 16-way workload.

## Filtered first-page results

Medians; ranges and sample cadence below. Latency percentages are relative to
baseline. The 1K ranges overlap; no small-population speedup is claimed.

| Population / procedure | Baseline ms | Candidate ms | Change | Baseline ops/s | Candidate ops/s | Baseline range ms | Candidate range ms |
|---|---:|---:|---:|---:|---:|---|---|
| 1000 / retrieve | 7.693 | 7.635 | -0.75% | 129.990 | 130.971 | 7.433–7.889 | 7.577–7.802 |
| 1000 / rretrieve | 7.571 | 7.719 | +1.95% | 132.078 | 129.558 | 7.557–8.463 | 7.692–7.964 |
| 2000 / retrieve | 37.676 | 21.725 | -42.34% | 26.542 | 46.029 | 37.613–37.876 | 21.627–21.962 |
| 2000 / rretrieve | 37.820 | 21.777 | -42.42% | 26.441 | 45.920 | 37.655–37.855 | 21.731–21.864 |
| 12.5K / retrieve | 126.731 | 60.172 | -52.52% | 7.891 | 16.619 | 125.687–127.403 | 59.938–62.159 |
| 12.5K / rretrieve | 127.911 | 61.004 | -52.31% | 7.818 | 16.392 | 127.812–128.096 | 60.010–61.194 |
| 49K / retrieve | 1151.953 | 861.592 | -25.21% | 0.868 | 1.161 | 1144.987–1164.832 | 859.424–866.518 |
| 49K / rretrieve | 1155.452 | 859.887 | -25.58% | 0.865 | 1.163 | 1140.066–1180.767 | 855.417–912.261 |

| Population / procedure | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op | Cadence |
|---|---:|---:|---:|---:|---|
| 1000 / retrieve | 6658876 | 6778254 | 63198 | 63469 | 100x, five samples |
| 1000 / rretrieve | 6748377 | 6868362 | 63506 | 63777 | 100x, five samples |
| 2000 / retrieve | 30356188 | 18737611 | 324221 | 168719 | 100x, five samples |
| 2000 / rretrieve | 30455639 | 18832440 | 324535 | 169030 | 100x, five samples |
| 12.5K / retrieve | 88554988 | 44073431 | 1054638 | 440988 | 10x, three samples |
| 12.5K / rretrieve | 88854755 | 44036422 | 1054969 | 441287 | 10x, three samples |
| 49K / retrieve | 1052365392 | 892614818 | 5181022 | 3044262 | 3x, three samples |
| 49K / rretrieve | 1049878146 | 890797538 | 5178144 | 3040005 | 3x, three samples |

At 1K, candidate bytes/op increase about 1.8% and allocations by 271/op (about 0.43%);
this is not a zero-allocation claim. Larger filtered workloads reduce allocation
substantially. Correctness/read-count assertions preserve admitted identities.

## Plain controls and rejected intermediate candidate

Plain paths do not execute the changed EXISTS-prefix work; no speedup is
attributed to the change. Final 1K/2K controls show no positive median above 2.10%;
some negative medians fluctuate substantially and are not credited to the fix.
Final 49K plain medians change +2.46%/+0.11%, with overlapping ranges.
The initial final 12.5K retrieve/plain 10x3 median was +6.23%, with overlapping
ranges. A longer matched 100x5 control gives 2.654478→2.665646ms (+0.42%, ranges
2.639834–2.670030 /2.653058–2.920021); rretrieve/plain gives 3.735284→3.726116ms
(-0.25%, overlapping ranges). Both logs and the initial noisier result remain.

The earlier error-corrected candidate was not accepted:1K rretrieve/EXISTS
regressed 12.1% in 20x5 and 7.55% in 100x5. Profiling showed repeated predicate
selection on each bound-chain call. Retaining the existing immutable complete
plan and consuming its existing explicit error contract reduced prefix CPU
samples from 1.75s to 1.17s in the small 100x5 profiles and removed 738 allocations/op
from the 1K filtered candidate. These are sampled attribution observations,
not an independent full-workload CPU-speedup claim.

## Reproduction and source applicability

Build the test binary from this branch; select
`-test.run='^$' -test.bench='^BenchmarkSearchSourceRetrieval$/^POPULATION$/^first-page$/^(retrieve|rretrieve)$/^(plain|exists)$/^concurrency1$'`
with the table's `-test.benchtime`/`-test.count`, `-test.cpuprofile` and
`-test.memprofile`. Baseline binaries used pristine release production plus the
identical benchmark fixture. The small baseline uses a Go overlay replacing only
`subquery_value_expression.go` and `traversal.go` with exact release files;
compiling the test binary is not a test pass.

Measurements preceded a mechanical extraction of the existing prefix owner from
`traversal.go` into `traversal_prefix.go` to comply with the 2500-line file limit.
The extracted block is byte-identical to the measured body (SHA256
`f45f6ff2757a05f62213bb5f8380bc6d795d5361c7fcb86982e6e1feffb75595`);
no function, signature or evaluation order changed. Final race verification runs
after extraction. Traversal is 2397 lines; prefix owner 190 lines.

Raw logs/profile families: `/tmp/n152-90-{before,plan-12k}*`,
`/tmp/n152-90-{before-49k,plan-49k}*`,
`/tmp/n152-90-{small-repeat-before,plan-small}*`,
`/tmp/n152-90-plan-control-{before,after}*`. Required review evidence is archived
under `~/.codex/artifacts/n152-90-20261121/` before workspace cleanup.
