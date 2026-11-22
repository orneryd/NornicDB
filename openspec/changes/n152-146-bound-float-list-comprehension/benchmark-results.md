# Bound-parameter admission measurements

Status: matched baseline/after complete; measured correctness overhead retained by parent review. No no-regression or full performance-qualification claim.

Apple M2 Max, darwin/arm64, Go1.27.4, GOMAXPROCS=2, GOMEMLIMIT=1500MiB,
CGO enabled, tags noui,nolocalllm; default GOGC. Baseline source f77ec7706 with
exact same benchmark overlay intended for the candidate. Public query outcomes
are asserted: existing MERGE workload has zero rows, filtered traversal returns
50 rows, tiny rebinding returns the current supplied value. Result caching is
disabled by the existing benchmark executor helper; compiled syntax can be reused.

Quiet window coordinated with parent and other active agent after compilation;
no competing task-owned Go checks. Existing workload runs:100 iterations ×5;
small rebinding:10000 iterations ×5. CPU profiles captured during each run.
Profiles include fixture/setup as well as query work; do not attribute all samples
to admission. Both logs ended PASS.

| Workload | Baseline median ns/op | Range ns/op | Median B/op | Median allocs/op |
| --- | ---: | --- | ---: | ---: |
| BenchmarkMatchUnwindMatchRelationshipMerge | 8278116 | 8268720–8424211 | 8540416 | 71939 |
| BenchmarkChainedMatchAggregatedWithFilter | 2351920 | 2329702–2395793 | 2288727 | 38876 |
| BenchmarkNOR79BoundParameterRebinding | 16071 | 15772–17394 | 3431 | 47 |

Raw `/tmp/n152-146-baseline-{workloads,rebinding}.log` and matching `.pprof`;
retained binary `/tmp/n152-146-baseline.test`. Command for each:

```
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB /tmp/n152-146-baseline.test -test.run '^$' -test.bench '^(BenchmarkMatchUnwindMatchRelationshipMerge|BenchmarkChainedMatchAggregatedWithFilter)$' -test.benchtime=100x -test.count=5 -test.benchmem -test.cpuprofile=/tmp/n152-146-baseline-workloads.pprof
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB /tmp/n152-146-baseline.test -test.run '^$' -test.bench '^BenchmarkNOR79BoundParameterRebinding$' -test.benchtime=10000x -test.count=5 -test.benchmem -test.cpuprofile=/tmp/n152-146-baseline-rebinding.pprof
```

Host disk exhaustion subsequently required shared cache cleanup. The matched comparison below reruns the preserved baseline binary alongside
the candidate, retaining these first pre-edit observations as evidence.

## Matched final comparison

After host cleanup, preserved baseline and final candidate binaries ran in one
quiet window coordinated with parent/N143/N138. Same hardware/configuration,
benchmark source, row assertions, repetitions and iteration counts as above.
All four logs PASS; CPU profiles captured during each run. Rebuild time excluded.
Candidate production diff SHA256:
`0316b0ded7665c1c7bde7c13e4352834784ea0ee37480b3b54487e5c7f682fc7`.

| Workload | Baseline median ns/op (range) | Candidate median ns/op (range) | Change | B/op baseline→candidate | allocs/op baseline→candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Relationship MERGE | 8314880 (8234070–8451655) | 8379673 (8353067–8390755) | +0.78% | 8540354→8547681 | 71938→71989 |
| Filtered traversal | 2369609 (2364706–2408252) | 2577440 (2547072–2641671) | +8.77% | 2288731→2309843 | 38876→39101 |
| Cached parameter rebinding | 15751 (15435–16253) | 29835 (29666–30008) | +89.42% | 3431→8072 | 47→85 |

MERGE ranges overlap. Filtered traversal and tiny rebinding ranges do not: these
are measured correctness costs, not within-noise or performance-pass results.
The tiny query adds14.084µs and4641B/38allocations per call. Its candidate CPU
profile attributes420ms cumulative of1490ms sampled to request semantic admission,
250ms to semanticScopeWalk; the baseline admission contribution is negligible.
Both profiles also include runtime allocation/page management and setup; these
samples do not attribute the entire timing difference to one function. The larger
combined profile attributes100ms of5790ms to the uncached admission owner.
Parent review disposition is recorded below; no speculative optimization applied.

Raw artifacts: `/tmp/n152-146-paired-{baseline,after}-{workloads,rebinding}.log`
and `.pprof`; exact commands in `/tmp/n152-146-paired-driver.log`, runner
`/tmp/n152-146-paired-bench.py`. Final binary `/tmp/n152-146-final.test`.

Parent review accepts retaining the measured cost as the necessary correctness
pass; this is not a performance optimization or no-regression qualification.
Profile caller audit attributes100% of admission samples in both named runs to
public `execute`, with no second `executeInternal` caller. Source tracing confirms
`executeImplicitAsync` and the implicit write callback continue through
`executeWithoutTransaction`. The request gate performs one uncached canonical
walk after a cached compile-phase lookup. Actual nested statements have their own
admission. No accidental repeated full parameter walk was found in these
workloads; broader release/performance qualification remains pending.
