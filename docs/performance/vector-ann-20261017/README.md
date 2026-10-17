# Compressed HNSW alignment — October 17, 2026

NornicDB now supports scalar int8, non-residual HNSW-PQ at 4×/8×/16×/32×/64×,
and one-bit storage with binary or 4/8-bit scalar queries. Construction keeps
original vectors, document-anchor ordering, lexical signatures and lexical query
entry points. Qdrant collection compression policies persist, and request
ignore/rescore/oversampling controls reach the public consumers.

The full corpus shows useful compression/latency tradeoffs. Overall process RSS
and several latency comparisons still trail Qdrant; all-metric parity is an open
target. CUDA/Vulkan device execution remains for the hardware handoff.

## Source, workload and completeness

- Runtime and timed harness: `7c201376d1203a651fc625869fb98df7be746ac4`, clean,
  `go build -buildvcs=true -tags 'noui nolocalllm'`. Baseline control:
  `239b057e7c8797597b757d8a4b948c1af6034572`, clean, built in a regular local clone
  with the same tags and explicit VCS stamping.
- Later harness changes require source stamps and correct captions from actual
  rescoring parameters. They change preflight/report metadata, leaving measured
  runtime, query settings and numerical results intact. Raw JSON keeps original
  captions; generated reports display the recorded override.
- Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0; native ARM64/CGO. Qdrant 1.19.2.
- 88,155 normalized 1,024-dimensional cached Titan V2 vectors, all 1,000 queries,
  exact top-100 ground truth, measured K=10. Each curve uses 100 warm-ups, sequential
  latency and 8-client throughput on one shared gRPC channel. No AWS requests.
- **41 cases / 263 query curves**: all 15 NornicDB modes and 13 corresponding Qdrant
  modes, matched float-file controls, five paired 8× rescoring variants, and a
  RAM-originals control. Each case loads fresh data. Engines and heavy verification
  run serially. Each case constructs one graph/model; small differences between
  independent cases require repeated trials for causal performance conclusions.
- [Audit results](audit.json) verify source stamps, binary modification status,
  full population, corpus hashes, every expected mode/beam, query counts, active
  compressed coordinate sizes and CPU construction. Repeat from repository root:
  `python docs/performance/vector-ann-20261017/audit.py`.
- The cached corpus has no paired lexical source/query text. These timings do not
  establish semantic lexical-seeding gains. Public lexical/identity/anchor and
  entry-point regressions exercise that contract separately. Corpus hashes match
  the earlier recorded corpus-integrity check.

## NornicDB preset/configured results

Recall is recall@10; latency is milliseconds; RSS uses decimal MB after the final
query/readiness check. These are snapshots, with original vectors, caches and
other database state included. Load and post-load readiness are separate fields
in the raw results. Throughput includes the common Python/gRPC client cost.

| Mode | Recall@10 | p50 ms | p95 ms | Queries/s | RSS MB |
|---|---:|---:|---:|---:|---:|
| [`exact`](cpu/nornicdb-exact.json) | 1.0000 | 6.308 | 6.579 | 186 | 3,749 |
| [`hnsw-fast`](cpu/nornicdb-hnsw-fast.json) | 0.9947 | 0.811 | 0.986 | 6,322 | 3,723 |
| [`hnsw-balanced`](cpu/nornicdb-hnsw-balanced.json) | 0.9992 | 1.207 | 1.522 | 4,046 | 3,746 |
| [`hnsw-accurate`](cpu/nornicdb-hnsw-accurate.json) | 0.9997 | 2.803 | 3.532 | 1,685 | 3,760 |
| [`ivf-hnsw`](cpu/nornicdb-ivf-hnsw.json) | 0.9741 | 18.177 | 21.607 | 510 | 3,877 |
| [`ivf-pq`](cpu/nornicdb-ivf-pq.json) | 0.9773 | 5.175 | 5.707 | 955 | 3,894 |
| [`sq-int8`](cpu/nornicdb-sq-int8.json) | 0.9959 | 0.551 | 0.669 | 7,998 | 3,221 |
| [`pq-x4`](cpu/nornicdb-pq-x4.json) | 0.9958 | 1.757 | 2.135 | 3,902 | 3,202 |
| [`pq-x8`](cpu/nornicdb-pq-x8.json) | 0.9947 | 1.221 | 1.492 | 5,557 | 3,204 |
| [`pq-x16`](cpu/nornicdb-pq-x16.json) | 0.9892 | 0.904 | 1.077 | 6,030 | 3,180 |
| [`pq-x32`](cpu/nornicdb-pq-x32.json) | 0.9528 | 0.683 | 0.822 | 6,429 | 3,169 |
| [`pq-x64`](cpu/nornicdb-pq-x64.json) | 0.8571 | 0.595 | 0.730 | 8,799 | 3,222 |
| [`bq`](cpu/nornicdb-bq.json) | 0.9803 | 0.575 | 0.785 | 7,511 | 3,181 |
| [`bq-asym4`](cpu/nornicdb-bq-asym4.json) | 0.9925 | 0.644 | 0.805 | 5,424 | 3,180 |
| [`bq-asym8`](cpu/nornicdb-bq-asym8.json) | 0.9939 | 0.676 | 0.849 | 6,697 | 3,221 |

The three ordinary HNSW presets use their configured graph/beam choices.
Compressed HNSW modes use the fast graph preset and file-backed originals
(`NORNICDB_PERSIST_SEARCH_INDEXES=true`), with exact rescoring. Product/scalar
use 1× rescoring by default; binary variants use 2× in this harness. Explicit
16/32/64/128/256/512 request beams are all retained in the detailed reports.
Qdrant receives the same compression policies, graph settings and request
parameters; its original-vector storage uses its default configuration.

- [Every NornicDB curve](cpu/nornicdb.md)
- [Complete paired report and footprint data](cpu/comparison.md)
- [Every Qdrant curve](cpu/qdrant.md)

## Rescoring depth and original-vector storage

All rows below use an explicit 8× rescoring pool and the preset request beam.
A top-10 pool of 80 raises a narrower traversal beam to at least 80. The same
request parameters go to both engines. NornicDB searches one global graph;
Qdrant searches segments and merges their results, so global candidate work can
still differ. Exact rescoring improves order among discovered candidates.

| NornicDB variant | Recall@10 | p50 ms | p95 ms | Queries/s | RSS MB |
|---|---:|---:|---:|---:|---:|
| [`pq-x16+rescore8`](rescore8/nornicdb-pq-x16+rescore8.json) | 0.9990 | 2.403 | 2.825 | 2,205 | 3,274 |
| [`pq-x32+rescore8`](rescore8/nornicdb-pq-x32+rescore8.json) | 0.9989 | 2.157 | 2.554 | 2,276 | 3,220 |
| [`pq-x64+rescore8`](rescore8/nornicdb-pq-x64+rescore8.json) | 0.9922 | 1.972 | 2.304 | 2,453 | 3,187 |
| [`bq+rescore8`](rescore8/nornicdb-bq+rescore8.json) | 0.9988 | 1.776 | 2.066 | 2,683 | 3,199 |
| [`bq-asym8+rescore8`](rescore8/nornicdb-bq-asym8+rescore8.json) | 0.9991 | 1.982 | 2.322 | 2,561 | 3,226 |
| [`bq-asym8+rescore8-ram-original`](ram-original/nornicdb-bq-asym8+rescore8-ram-original.json) | 0.9990 | 0.759 | 0.915 | 8,012 | 3,738 |

The RAM control changes original-vector persistence/storage and builds a fresh
graph. Its lower latency points to original-vector I/O as a remaining cost of
large rescoring pools. Its RSS remains high. The 8× PQ64 preset improves recall
substantially over its default, while retaining less than the float presets.

- [All wider-pool NornicDB curves](rescore8/nornicdb.md)
- [Paired wider-pool results](rescore8/comparison.md)
- [RAM-originals control](ram-original/nornicdb.md)

## Coordinate storage and controls

Encoded sizes exclude graph metadata, Go object/scratch overhead, originals,
storage caches and RSS. At 1,024 dimensions, originals need 4,096 bytes/vector.
Scalar int8 needs 1,028 bytes including its per-vector scale. Binary needs 128
bytes regardless of query precision. PQ needs 1,024/512/256/128/64 code bytes at
the five ratios, plus 1,048,576 shared codebook bytes on this corpus. PQ64's total
encoded coordinates/books are 6,690,496 bytes; binary uses 11,283,840 bytes.

The current float file-backed control and scalar int8 both measured 0.9959 recall.
Scalar's 0.551 ms median versus 2.774 ms is about 5× lower latency; throughput was
7,998 versus 1,008 queries/s. RSS was approximately 3.22 versus 3.18 GB. The
compact traversal representation delivers a query benefit, while full-process
memory remains dominated by broader database state.

| Float file-backed control | Recall@10 | p50 ms | p95 ms | Queries/s | RSS MB |
|---|---:|---:|---:|---:|---:|
| [Merged source `239b057e7`](baseline/nornicdb-hnsw-fast+original-file.json) | 0.9961 | 2.816 | 3.688 | 1,025 | 3,179 |
| [Current source `7c201376d`](file-current/nornicdb-hnsw-fast+original-file.json) | 0.9959 | 2.774 | 3.572 | 1,009 | 3,180 |

The fresh controls do not establish a construction speedup from scratch pooling.
The scoped 32-query / 2,048-frontier / 1,024-D CPU candidate microbenchmark shows
about 13% lower median time and approximately 95% fewer allocated bytes. PQ query
preparation's synthetic full lookup-table benchmark went from about 223 µs and
262 KB / 2 allocations to 172 µs and zero steady-state allocations. Native ARM64
1,024-D int8 dot scoring measured approximately 48.5 ns versus 96 ns float scoring.
These microbenchmarks exclude RPC, graph traversal and original-vector I/O;
[before/after records](checks/) retain their actual workload boundaries.

## Validation and remaining boundaries

See [verification commands, failures, results and skips](verification.md).
Original-vector construction and exact reranking remain the default. The shared
training owner now uses squared distance for product codebooks, preserving
subvector magnitude; coarse IVF routing keeps cosine training. PQ books train on
activation (default HNSW sample cap 10,000) and are reused for live additions;
rebuild after substantial distribution changes when evaluating recall.

Scalar encoding uses symmetric per-vector scales. Scalar quantile clipping other
than 1, mixed named-vector compression policies, two-/1.5-bit binary storage and
quantization policy updates through UpdateCollection are explicitly unsupported.
Per-collection HNSW topology overrides remain outside this mapping. Supported
compression controls are documented in [the vector search guide](../../user-guides/vector-search.md#compressed-hnsw-coordinates).

Implementation analysis used the read-only Qdrant v1.19.2 source at
`016542aa5deb6c66380bb137badf73d54f742bde`: product ratio-to-subvector mapping in
[`quantized_vectors.rs`](https://github.com/qdrant/qdrant/blob/016542aa5deb6c66380bb137badf73d54f742bde/lib/segment/src/vector_storage/quantized/quantized_vectors.rs),
query-precision/bitplane scoring in
[`encoded_vectors_binary.rs`](https://github.com/qdrant/qdrant/blob/016542aa5deb6c66380bb137badf73d54f742bde/lib/quantization/src/encoded_vectors_binary.rs),
and per-segment rescoring/merging in its shared vector-index search and collection
segment searcher. The alignment provides the same compression families and
rescoring controls, with original-vector construction retained in NornicDB.

For mm0nst3r's machine, use the existing [native backend commands](../../../testing/benchmarks/vector_ann/README.md#review-and-gpu-handoff)
and select `VECTOR_MODES=sq-int8,pq-x16,pq-x32,pq-x64,bq,bq-asym4,bq-asym8`.
Keep the cached corpus, confirm the native `build_backend` in every HNSW result,
and compare CPU/GPU on the same machine. CUDA/Vulkan skips remain unverified.

## Default selected from these measurements

Application HNSW configuration now defaults to scalar int8, retaining the fast
graph preset and exact original-vector rescoring. Set
`NORNICDB_VECTOR_HNSW_QUANTIZATION=none` to disable compression, or select
`pq`/`binary` explicitly. Explicit collection policies (including no quantization)
still override the application default; the low-level index configuration remains
explicit. Existing checkpoints and GPU construction remain supported.

On this corpus, scalar int8 achieved 99.59% recall@10, 0.551 ms p50,
0.669 ms p95 and 7,998 queries/s. Its 3,221 MB RSS was 13.5% below the ordinary
fast-HNSW run (3,723 MB), but slightly above the file-backed float control
(about 3,180 MB). These storage configurations differ: scalar compression alone
does not establish process-memory savings when original vectors stay in RAM.
Encoded scalar coordinates occupy 90.6 MB versus 361.1 MB of raw float coordinates;
originals remain available for construction/rescoring. This is a workload-scoped
default choice, not a universal accuracy or GPU-performance guarantee.

The 18.177 ms row is IVF-HNSW, not IVF-PQ (5.175 ms). IVF-HNSW routes into
multiple clusters, searches their graphs sequentially, then merges candidates;
its missing-graph path scores the selected cluster population exactly. IVF-PQ
uses compressed product codes. The existing timings do not isolate the cost of
full-precision distances from routing, per-cluster traversal, merging or fallback.
