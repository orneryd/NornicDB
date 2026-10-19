# Vector ANN completion measurements — October 16–17, 2026

These reports record the cached 88,155-vector workload used to finish #1370, #1372 and #1375. Both baseline and final directories include every measured engine/mode, source and binary provenance, effective settings, six explicit HNSW beams, recall, client timings, load/readiness time, and footprint snapshots. The comparison reports are artifacts; PR benchmark comments contain NornicDB numbers only.

## Measured source and workload

- Measured final product binary: `ce38410589d30884699cf0a61b871f51dfe3f493`, built clean with `go build -buildvcs=true -tags 'noui nolocalllm'`. Final harness: clean `4857dda3366ad19f464423bd5ec69887e89f6a94`. Later report/document commits do not alter the measured runtime or harness code.
- Final PR runtime source also includes `5d85de573289236b24d3f02b60fba72ec1c1d300`, added after timing: successful compressed retrieval inside the GPU brute-force size range avoids an unused flat copy, and compressed fallback uploads only after selection outside the pipeline lock. The measured CPU cases disable GPU, and the measured Metal case uses balanced HNSW with GPU brute force disabled; their query/construction algorithms are unchanged by this follow-up. These reports retain the actual measured revision rather than relabeling it. No GPU-compressed performance claim is made.
- Earlier integrated baseline: product `5f7e301634bd9e209987f26b4997b88488027837`, harness `f7d169831`. This is an earlier integrated correction, **not** a pre-PR or pre-Claude baseline. Intermediate/canceled timing runs are excluded.
- Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0. Native binaries; one engine/mode at a time with fresh data directories. Tests, compilation and verification were stopped before timing runs.
- 88,155 normalized 1,024-dimensional Titan V2 vectors, 1,000 cached query vectors, exact top-100 ground truth; measured K=10, 100 warm-up queries, sequential client latency and concurrent throughput using 8 RPCs on one shared channel. The corpus, queries and ground-truth hashes are recorded in every result.
- Corpus integrity: all vectors/queries are finite and normalized, all ground-truth IDs are in range, and the top-100 boundary was recomputed against every stored vector for all 1,000 queries with zero discrepancies at tolerance 1e-5. No AWS/Titan calls were made.
- This legacy corpus has **no lexical payloads**. These timings do not measure lexical construction gains. Real BM25/document-to-embedding identity, construction seeds and hybrid cluster routing are covered by public regression tests, separately from these vector-only measurements.

## Configured request logging follow-up

The maintainer-approved request log-level change at `5cfe2533f` is measured separately in the [logging follow-up](request-logging/README.md). It confirms successful requests are suppressed at info; the modest latency changes do not establish a large additional speedup.

## ARM64 scoring follow-up

A later CPU balanced-HNSW measurement at `cd5e02461` records approximately 11% lower preset p50 and 17% lower beam-512 p50. See the [source analysis, before/after results and limits](arm64-dot/README.md). The original sweep below retains its actual measured source.

## Read the results

- [Final NornicDB measurements](final/nornicdb.md)
- [Final complete workload report](final/comparison.md)
- [Earlier integrated baseline](baseline/comparison.md)
- Per-mode JSON is the authoritative input for the generated Markdown. Each tagged variant records its own settings and backend. `environment.json` describes the CPU sweep; the Metal variant is explicitly marked in its result JSON.

The preset row uses each preset's configured search beam. Explicit beams 16/32/64/128/256/512 are separate query runs on the same constructed index. Construction is fresh once per engine/mode/variant, rather than repeated trials. Small differences between independently constructed graphs or trained models are not evidence of a causal speedup or recall gain.

Load time and **post-load readiness time** are separate; readiness is not total process or ingest time. RSS and allocated disk are snapshots after the query sweep and final readiness check, **not peak memory**. The `exact` CPU mode selects the CPU brute-force strategy; its latency is not a measurement of every magnitude-sensitive or per-request `params.exact=true` path.

## Interpretation and operator choices

| NornicDB mode | Recall@10 | p50 | p95 | Queries/s | Post-load ready | RSS after queries |
|---|---:|---:|---:|---:|---:|---:|
| `exact` | 1.0000 | 6.712 ms | 8.990 ms | 175 | 4.2s | 3,728 MB |
| `hnsw-fast` | 0.9964 | 0.823 ms | 0.991 ms | 5,974 | 64.1s | 3,744 MB |
| `hnsw-balanced` | 0.9992 | 1.267 ms | 1.530 ms | 3,754 | 85.8s | 3,749 MB |
| `hnsw-accurate` | 0.9998 | 2.994 ms | 3.854 ms | 1,541 | 187.4s | 3,749 MB |
| `hnsw-balanced+metal` | 0.9992 | 1.292 ms | 1.609 ms | 3,912 | 47.6s | 3,762 MB |
| `ivf-hnsw` | 0.9744 | 18.451 ms | 21.590 ms | 449 | 60.5s | 3,973 MB |
| `ivf-hnsw+nprobe128` | 0.9993 | 80.064 ms | 84.106 ms | 92 | 59.9s | 4,228 MB |
| `ivf-pq` | 0.9773 | 5.285 ms | 5.838 ms | 942 | 401.2s | 3,462 MB |
| `ivf-pq+rerank2000` | 0.9773 | 30.109 ms | 32.896 ms | 145 | 401.3s | 4,050 MB |
| `hnsw-balanced+gomemlimit2g` | 0.9989 | 1.264 ms | 1.528 ms | 3,973 | 86.2s | 3,446 MB |

Metal construction was observed as `Metal`, with no CPU construction fallback and GPU brute force disabled. Balanced CPU and Metal construction both measured 0.9992 recall@10; their post-load readiness times were 85.8s and 47.6s respectively. Queries still selected the HNSW pipeline. This is one fresh construction per variant, not a repeated statistical comparison.

The earlier IVF-HNSW run measured 0.6399 recall@10; the final default run measured 0.9744. The corrected global candidate budget has a query-time cost: final p50 is 18.451ms versus 14.982ms earlier. The concentrated-neighbor public regression independently proves the missing-neighbor defect and its correction. Higher probing (128 lists) measured 0.9993 recall@10 at 80.064ms p50. Do not treat the higher-probe configuration as a free latency improvement or a new default.

Both fresh IVFPQ runs measured 0.9773 recall@10; rerank 200 measured 5.285ms p50 and rerank 2000 measured 30.109ms. This supports 200 on this workload, without establishing equivalence for other distributions. The optional Go memory-limit run measured 3,446MB RSS rather than the CPU balanced run's 3,749MB; it is not a 2GiB process limit. Its independently constructed graph measured 0.9989 recall@10, so memory and retrieval quality are reported together.


No GC or Badger cache defaults changed. `GOMEMLIMIT=2GiB` is an explicit optional Go soft-heap-limit experiment, not an RSS cap. IVFPQ rerank 200 and 2000 use independently trained fresh models; they are not measurements of one persisted codebook with only its query rerank changed. The higher-probe IVF run also uses a fresh clustering build. These variants expose workload tradeoffs and do not establish a universal recall guarantee.

IVF-HNSW describes the selected pipeline: small selected lists can use exact subset scoring, while larger lists use per-cluster graphs. It does not prove that every query visited a graph. The global-candidate-budget correction improves concentrated-neighbor retrieval by allowing each selected list to contribute the requested candidate budget; its additional scoring work has a measured latency cost.

## Validation and hardware handoff

The maintainer explicitly replaced the final `make verify` requirement with focused/package tests and one `make test` for this work. Both latest verifier attempts were canceled and remain incomplete; older passing verifier runs apply only to their recorded source revisions.

Full-repository validation used measured source `ce3841058`; the affected packages and native GPU boundary tests were rerun after the final `5d85de573` follow-up:

- Fresh `go test ./pkg/search ./pkg/qdrantgrpc -count=1` passed on `ce3841058` (242.662s / 2.913s). The final `5d85de573` source passed both packages fresh again (180.510s / 2.691s).
- One `make test` passed: 94 fresh package results, zero cached results and zero failures in its filtered output; logging and metrics lint gates passed. The target filters individual test/skip output and does not produce coverage. Package success does not prove unavailable external integration fixtures executed.
- Scoped search/Qdrant races passed, including GPU first publication, model-space admission, persisted exact search and both drop/recreate constructor paths. The final native GPU tests also cover successful compressed retrieval leaving the flat GPU index unallocated and canceled compressed training selecting and querying the GPU fallback. This is not a full-repository race run.
- `go vet ./pkg/search ./pkg/qdrantgrpc ./pkg/gpu ./cmd/nornicdb` passed.
- All 11 Python harness contracts and shell syntax passed. Later report/document-only changes preserve the tested code.
- Native Metal resident contracts ran locally. Linux Vulkan Go compilation passed. CUDA accelerator type checking with stubbed native bridge signatures passed; this is not native CUDA compilation or execution.

CUDA and Vulkan implementations/kernels are retained. Their native compilation, device execution and performance still require mm0nst3r's machine. Follow the [hardware commands](https://github.com/orneryd/NornicDB/blob/714b95e326cddc6bba449437b1cec542eecb8060/testing/benchmarks/vector_ann/README.md), require the requested observed backend and no CPU fallback, and treat skipped tests as unverified. Native candidate selectors retain their existing 256-candidate cap.

The separate BEIR SciFact corpus/model is not cached here, so that retrieval baseline was not rerun. Standalone golangci-lint is unavailable (exit 127); the configured logging/metrics lint gates passed. File-backed exact magnitude-sensitive queries perform durable coordinate lookups. Selective compressed filters can still return short when matching vectors are outside the probed lists; `exact=true` searches the admitted indexed population. Collection HNSW/quantization override mappings and quantization rescore controls remain outside this correction.

The final fallback fixture initially used corrupt metadata, which the normal build correctly rebuilt instead of failing; it was replaced with a canceled training attempt to exercise the existing fallback. The superseded package attempt was stopped and its failing fixture/race logs were preserved, not counted as passing. The original unused-allocation reproduction failed before the fix and now passes.

During the requested temporary-directory cleanup, fail-first, canceled and final local logs were archived to `.verify/ann-evidence-6yg0lqks/temporary-evidence.tar.gz` in the primary workspace. Raw timing logs were preserved in that archive before removing their temporary paths. Per-mode temporary server data was cleaned by the runner. Only reviewable JSON/Markdown is committed here, not source chunks, vector corpora, binaries or raw server logs.
