# HNSW query-cost profiling and mapped rescoring

File-backed scalar HNSW improves from **0.565 to 0.470 ms p50**, **0.700 to
0.542 ms p95**, and **8,288 to 9,151 queries/sec** in fresh full-corpus runs.
Recall@10 remains **99.59%**. Whole-process RSS rises **3,180 to 3,360 MB**
(+5.65%): mapped original-vector pages become resident. This removes a measured
I/O bottleneck, but does not establish latency or memory parity with another engine.

## Source and workload

Before: `c0dee89553de6ff0521433e2af6fd1fc9a457b56`.
After and checked production source: `ee2daadfb307172cceac3b5ba49a3e4d7636572b`.
Both binaries are clean, VCS-stamped native ARM64/CGO builds with
`go build -buildvcs=true -tags 'noui nolocalllm'`.
Release base: `f2ddd27cfae3788076778a0a0d48c827df046a3b`.
Later report edits do not alter runtime source.

Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0, Go 1.26.6. All **88,155 × 1024**
cached normalized vectors and **1,000 queries**, truth top-100, measured K=10.
No AWS/Titan calls. Scalar/PQ use ef=50, rescoring 1×; binary uses ef=50,
rescoring 2×. Public top-10 overfetch produces candidate pools 15/30 respectively.
Latency and recall use sequential requests after 100 warmups; throughput uses
8 clients on the existing shared gRPC channel, without separately checking
concurrent recall. Each case constructs fresh indexes. Heavy verification,
profiles and published timings run separately. No competitor was rerun.

## What the profiles establish

- Before, scalar [`Pread` stacks](case-2/cpu-cum.txt) account for **57.81% of
  sampled CPU**: 47.48% under candidate rescoring, 10.33% under final vector
  selection. Socket-read stacks account for approximately 2.52%. These are CPU
  samples, not per-request wall-time percentages.
- After, candidate rescoring accounts for **1.28% of sampled CPU** and original
  vector `Pread` is absent from the hotspot table. The largest remaining sample
  is **62.36% in `runtime.pthread_cond_signal`**, through scheduler wakeups.
  [CPU top](mapped-case-1/cpu-top.txt), [cumulative](mapped-case-1/cpu-cum.txt).
- A separate two-second runtime trace attributes **80.51% of its recorded
  scheduler delay** to the gRPC connection/stream-header path, primarily
  `http2Server.operateHeaders` and control-buffer sends; response writes account
  for 13.45%. [Scheduler delay profile](trace-case-1/sched-top.txt). This links
  the next investigation to HTTP/2 control-buffer scheduling on a reused
  connection, not connection establishment. Scheduler delay and sampled CPU are
  different measurements; these percentages must not be added or interpreted
  as end-to-end latency shares.
- Scalar native dot-product calls still appear (~7.71% cumulative `cgocall`).
  The separate original-RAM float profile also shows native dot calls and
  per-neighbor string-map/locking lookups. Those paths remain optimization
  targets; this change does not optimize float traversal or gRPC itself.
- Pprof enables mutex and block sampling. All profile/trace run JSON is
  **diagnostic**, excluded from published latency/throughput claims.

The implementation reuses the existing read-only mapping primitive. Borrowed
rows remain inside store read locks; reload, compaction and close retire mappings
under the write lock. Append publication refreshes the mapping generation.
Public `GetVector` retains owning-copy behavior; private dot scoring returns only
the score. Existing scalar accumulation order, candidate ordering, quantization
policies, lexical hints and native GPU paths are preserved. Unsupported mapping
platforms retain `ReadAt`; `nommap` exercises that path locally. No custom
allocator, dependency patch or new unsafe-pointer primitive was introduced.

## Full-corpus compressed HNSW results

These are uninstrumented measurements of the new source. The table preserves
all first-pass results, including PQ ×8 throughput variation.
| Mode | Recall@10 | p50 ms | p95 ms | Queries/sec | RSS MB |
|---|---:|---:|---:|---:|---:|
| sq-int8 | 99.59% | 0.470 | 0.542 | 9,151 | 3,360 |
| pq-x4 | 99.58% | 1.698 | 2.068 | 4,056 | 3,425 |
| pq-x8 | 99.47% | 1.170 | 1.391 | 5,154 | 3,407 |
| pq-x16 | 98.92% | 0.823 | 0.976 | 6,896 | 3,386 |
| pq-x32 | 95.28% | 0.600 | 0.701 | 8,704 | 3,398 |
| pq-x64 | 85.71% | 0.512 | 0.583 | 8,464 | 3,394 |
| bq | 98.03% | 0.396 | 0.473 | 9,017 | 3,454 |
| bq-asym4 | 99.25% | 0.439 | 0.516 | 9,229 | 3,508 |
| bq-asym8 | 99.39% | 0.495 | 0.580 | 9,364 | 3,279 |

Raw results, environment and server logs: [scalar](mapped-scalar/),
[other eight modes](mapped-compressed/). All nine recalls match the earlier
recorded values. Graphs were independently built; no general quality gain is
claimed. RSS is decimal MB for the entire process, not encoded vector bytes.
Some historical RSS increases exceed 10% (binary/int4: about 3,180 → 3,508 MB);
this is the explicit tradeoff for resident mapped originals, not a compression
improvement. RAM-original controls are not remeasured or relabeled.

A fresh PQ ×8 pair checks an apparent concurrent-throughput regression against
historical data: [before](baseline-pq8/nornicdb-pq-x8.json) →
[repeat after](mapped-pq8-repeat/nornicdb-pq-x8.json): identical **99.47% recall**,
**1.213 → 1.146 ms p50**, **1.535 → 1.418 ms p95**, **5,629 → 6,000 queries/sec**.
The first after run produced 5,154 queries/sec; preserve both rather than select
only the faster result. Single-case construction/scheduling variation limits
claims about small differences. Fresh matched controls exist for scalar and
PQ ×8; other comparisons against the original sweep are historical.

Direct layer benchmarks use one persisted scalar graph, explicit ef=50 and
public QdrantPoint metadata. Across three trials the median mean is **178.9 µs
traversal**, **199.3 µs pipeline**, and **228.1 µs public search API**; public
allocations are approximately **27.3 KB / 86 allocations** per query. These are
means from a different process, not end-to-end p50 components. Do not subtract
these values from gRPC p50 as an exact transport measurement.
[Final direct-layer transcript](final-direct-layers.log).

## Verification and limitations

- Native focused race/coverage run: **passed, 7.986s**, covering vector-file
  lifecycle, construction leases and public quantized consumers.
  [Transcript](mapped-final-native.log).
- Portable `nommap` focused run: **passed, 1.224s**. [Transcript](mapped-portable.log).
  Mapping-specific tests skip intentionally on this backend.
- New mapped lifecycle/scoring functions reach **90–100% combined statement
  coverage** across native/portable runs; [combined profile](mapped-combined.cover).
  This is not whole-package coverage.
- Initial `make test`: **failed** in `TestQdrantRecommendKeepsProviderSnapshot`;
  93 packages passed (25 fresh, 68 cached). Fresh search **202.632s**, Qdrant
  **10.014s**, storage **118.688s**. [Original filtered log](make-test.log).
  Make filters out assertion details; no specific failure cause can be inferred.
- Exact isolated server reproduction **passed**; complete fresh server package
  **passed, 160.453s**. [Isolated](server-repro.log), [package](server-package.log).
- Complete `make test` retry **passed: 94 packages, 4 fresh, 90 cached**. Fresh
  server **161.783s**; search/Qdrant cached in this retry. No source or assertion
  changes between runs. [Retry log](make-test-retry.log). The initial failure
  remains unreproduced, not silently counted as a pass or attributed to this fix.
- `make verify` skipped per explicit maintainer instruction. CUDA/Vulkan,
  native AMD64 and fresh GPU/SciFact benchmarks remain unexecuted here.
- The legacy corpus has no paired lexical query/source text, so these timings
  do not establish lexical-hint quality gains. Existing lexical behavior remains.

## Reproduction and excluded development evidence

From the repository root, supply locally built clean before/after binaries and
cached corpus, then run the existing harness (each run in a distinct directory):

```sh
python testing/benchmarks/vector_ann/run.py --corpus bench-data/vector_ann/corpus \
  --out /tmp/hnsw-scalar-after --nornic-bin /path/to/after \
  --qdrant-bin /path/to/local-qdrant --engines nornicdb \
  --modes sq-int8 --ef-sweep ''
```

Use `pq-x4,pq-x8,pq-x16,pq-x32,pq-x64,bq,bq-asym4,bq-asym8` for the other modes.
The Qdrant binary is preflight metadata only when `--engines nornicdb` is used.
Archived profiling scripts import the same harness; add
`--nornic-env NORNICDB_PPROF_ENABLED=true` and
`--nornic-env NORNICDB_PPROF_LISTEN=127.0.0.1:19191`.
They write into the ignored local profiling directory; use a new output path
before repeating to preserve prior captures. The trace is gzip-compressed for
storage: decompress `trace-case-1/trace.out.gz` before `go tool trace`.
CPU `.pb.gz` files open directly with `go tool pprof`.

`BenchmarkANNProfileLayers` requires explicit `NORNICDB_PROFILE_DATA_DIR`,
`NORNICDB_PROFILE_DB=bench`, `NORNICDB_PROFILE_DIMS=1024` and
`NORNICDB_PROFILE_ANN_QUERIES` (little-endian float32 query matrix). Run with
`go test ./pkg/search -run '^$' -bench '^BenchmarkANNProfileLayers$' -benchtime=1s -count=3`.

The initial direct fixture omitted RPC label/beam metadata and is invalid for
comparative claims ([excluded log](mapped-direct-layers.log)); the corrected
intermediate fixture is retained separately. The first contract test compile
failed on a fixture seed type and was corrected before baseline execution
([compile log](query-contract-before.log)); this was not a reproduced product
bug. Baseline contract/microbenchmark and intermediate map-only benchmark logs
are retained; full-corpus final-source measurements lead the conclusion.
No vectors, query matrix, local datasets or binaries are committed.
