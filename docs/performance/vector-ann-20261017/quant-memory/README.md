# Quantization memory accounting on the pulled PR head

**Compression is reducing the live encoded index. Similar process RSS does not
show that an old float HNSW or its vectors remain live.** On this workload,
scalar → PQ ×64 reduces encoded coordinates **90.62 → 6.69 MB** and post-GC
live Go heap **790.6 → 707.4 MB**, while RSS remains around 3.4 GB.

The binary query-int4 and query-int8 modes both store **one bit per dimension**.
Their encoded size should be identical: the precision setting changes query
scoring, not the stored corpus. Their live heaps are nearly identical as well.
This is different from **PQ ×4 versus PQ ×8**, whose compression factors reduce
corpus codes differently (91.32 versus 46.18 MB including codebooks on the
recorded corpus). The current scalar storage mode is int8; binary query-int4 is
not a stored scalar-int4 mode.

## Fresh measurements

Inspected and built clean source: `92e1c44138aecac592bd421e94de67fcd5a90b9f`.
All **88,155 normalized 1024-dimensional vectors**, all **1,000 queries**, K=10,
100 warmups, existing long-lived local gRPC channel. Original vectors are
file-backed. Scalar/PQ use ef=50 and 1× rescoring; binary uses ef=50 and 2×.
No AWS/Titan access or competitor runs. Native ARM64/CGO, Apple M2 Max,
Go 1.26.6, macOS 26.5.1 (Darwin 25.5.0).

Each mode starts a fresh server and builds its own index. After sequential
recall/latency and 8-client throughput passes, the diagnostic wrapper captures
heap text, requests a heap profile with `gc=1`, captures runtime memory stats,
and runs `vmmap -summary` and `vmmap -resident`. The table uses post-GC stats.
Pprof enables mutex/block profiling: **timings are diagnostic, not replacement
latency or throughput benchmarks**. Recall is valid for the sequential queries;
concurrent recall is not separately checked. [Population/provenance audit](audit.json); repeat with
`python3 docs/performance/vector-ann-20261017/quant-memory/audit.py`.
| Mode | Encoded MB | Live Go heap MB | RSS after GC MB | Physical footprint¹ | Recall@10 |
|---|---:|---:|---:|---:|---:|
| sq-int8 | 90.62 | 790.6 | 3384 | 1.6G | 99.59% |
| bq-asym4 | 11.28 | 710.5 | 2576 | 1.4G | 99.25% |
| bq-asym8 | 11.28 | 710.1 | 3473 | 1.4G | 99.39% |
| pq-x4 | 91.32 | 809.0 | 3409 | 1.4G | 99.58% |
| pq-x64 | 6.69 | 707.4 | 3420 | 1.4G | 85.71% |

MB is decimal bytes / 1,000,000. ¹ Physical footprint is the rounded `G` value
reported by macOS `vmmap` (binary units); it is a different metric from RSS,
not a more precise replacement for it. Each is a one-case snapshot.

The two binary cases differ by about **897 MB RSS** but only **0.46 MB live Go
heap**, despite identical stored codes. Scalar → PQ ×64 saves **83.17 MB live
heap**, close to the **83.93 MB encoded-size difference**. Unrelated cache,
metadata and allocation-layout differences prevent an exact byte-for-byte
whole-heap comparison across independent cases.

All five measured recalls match their previously recorded values. PQ ×64's
85.71% preset recall is an explicit quality tradeoff; its smaller codes do not
make it a substitute for scalar's 99.59% recall. This investigation does not
remeasure the other HNSW modes, IVF-HNSW, or IVF-PQ after the pulled changes.

## Where memory actually goes

- **Live codes:** the scalar sampled heap attributes roughly 86 MiB to
  `hnswQuantizedVectors.grow`; PQ ×64 attributes roughly 5.4 MiB to codes, with
  shared codebooks accounted separately. [Scalar heap](case-1/heap-top.txt),
  [PQ ×64 heap](case-5/heap-top.txt). Pprof heap values are sampled estimates;
  `QuantizationBytes()` provides the logical encoded byte counts in the table.
- **Storage and metadata:** the PQ ×64 heap attributes about 281 MiB to
  Ristretto/Badger block allocations and 166 MiB to Badger memtable arenas.
  Node/query identity maps, graph adjacency, BM25, storage node caches and
  bookkeeping remain in all modes. They are not compressed by ANN coordinate
  quantization. `resolveBadgerOptions` selects the existing high-performance
  storage profile unless `Storage.Mode` is `low`; its caches/write buffers are
  independent of ANN compression. No storage settings were changed here.
- **Idle heap:** scalar reports **1,942 MB idle**, of which **1,155 MB is marked
  released**, after GC. PQ ×64 reports **2,023 MB idle / 1,311 MB released**.
  These are not live vector arrays. Go's Darwin runtime marks unused pages with
  `MADV_FREE_REUSABLE`; resident pages can remain counted in RSS until reclaimed.
  [Scalar runtime stats](case-1/heap-after-stats.txt),
  [PQ ×64 runtime stats](case-5/heap-after-stats.txt).
- **Mapped originals and storage files:** scalar has one 344.4 MiB virtual
  `vectors.vec` mapping with about **193 MiB resident**; PQ ×64 has about
  **188 MiB resident** in its equivalent mapping. Original floats remain on
  disk for exact rescoring; disk-backed does not mean zero resident file pages.
  SST/value-log mappings also contribute residency. Scalar's total mapped-file
  residency is about 672 MiB. The OS can reclaim clean file-backed pages.
  [Scalar OS summary](case-1/vmmap-summary.txt),
  [PQ ×64 OS summary](case-5/vmmap-summary.txt).

The global graph is not the entire database working set. On 88k vectors,
changing scalar to PQ ×64 can remove only about 84 MB of encoded coordinates;
it cannot shrink storage caches, build-time heap high-water marks or metadata
by 64×. This is why neither the earlier RSS table nor compression ratio alone
should determine a memory-constrained deployment's default.

## Ownership inspection and limits

Inspected the integrated current paths:

- `Service.addVectorLocked` writes to `vectorFileStore` **or** `vectorIndex`.
  The file-backed path does not also add an in-memory vector-index row.
- `getOrCreateHNSWIndex` supplies a canonical original-vector lookup. With a
  lookup installed, `HNSWIndex.addLocked` records `vecOff=-1` and does not
  append the original to its `vectors` matrix. The temporary construction
  mapping is closed; runtime query mappings have their own lifecycle.
- `setQuantizationLocked` publishes one replacement `h.quantized` model. PQ
  training rows are local scratch, not fields retained by the published model.
- The five cases use distinct fresh processes. An index left in a previous
  mode's process cannot explain the cross-mode RSS values.

Together, source inspection and post-GC profiles provide **no evidence of an
extra original-vector HNSW matrix in these persisted single-graph runs**. This
is not proof about every configuration or concurrent policy-transition lifetime.
RAM-original configurations intentionally retain their canonical originals.
The current IVF cluster-generation work is outside these five measured modes;
no claim is made about its complete memory ownership from these snapshots.
No product fix, allocator change, cache-size change, or default change was made.

## Evidence and reproduction

The initial five-mode process was interrupted after completing the first four
snapshots and before completing PQ ×64. [Original log](run.log) and the partial
PQ ×64 server log remain archived. That attempt is **incomplete**, not a passed
case. A separate clean PQ ×64 run completed [replacement log](finish-run.log),
[raw result](results-pq64/nornicdb-pq-x64.json). The table/audit use only its
completed `case-5` snapshot. Other results are under [results/](results/).

The [diagnostic wrapper](profile_memory.py) imports the existing benchmark
harness. From repository root, with a fresh output location in the wrapper:

```sh
go build -buildvcs=true -tags 'noui nolocalllm' -o /tmp/nornic-memory ./cmd/nornicdb
python docs/performance/vector-ann-20261017/quant-memory/profile_memory.py \
  --corpus bench-data/vector_ann/corpus --out /tmp/nornic-memory-results \
  --nornic-bin /tmp/nornic-memory --qdrant-bin /path/to/cached-qdrant \
  --engines nornicdb --modes sq-int8,pq-x4,pq-x64,bq-asym4,bq-asym8 \
  --ef-sweep '' --nornic-env NORNICDB_PPROF_ENABLED=true \
  --nornic-env NORNICDB_PPROF_LISTEN=127.0.0.1:19191
```

The wrapper's snapshot root is explicitly configured near its top; use a new
root before repeating to preserve previous evidence. Cases follow harness mode
order, not CLI order. `profile_memory_finish.py` resumes snapshot numbering at
four for this recorded interrupted run. No vectors, query matrix, datasets or
binaries are committed. Heap text and detailed `vmmap` output are gzip-compressed;
runtime stats and top allocations remain readable. Heap `.pb.gz` profiles open
directly with `go tool pprof -top -inuse_space`.

Fresh checks for this investigation: clean native build and five full-corpus
public gRPC search cases with post-GC heap/OS snapshots. No runtime source was
edited, so no new package/race/full-suite test claim is made for this head;
previous test reports retain their original source hashes. `make verify` remains
waived by the maintainer. GPU/CUDA/Vulkan, native AMD64 and SciFact were not run.
