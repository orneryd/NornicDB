# ARM64 scoring follow-up

The ARM64 NEON dot-product change lowers full public gRPC search latency on the cached 88,155 × 1,024 corpus without lowering search beams. It does not close every performance gap or establish a universal recall guarantee. This is a focused CPU balanced-HNSW follow-up; earlier fast, accurate, IVF and Metal reports retain their original measured revisions.

## Evidence

Apple M2 Max, 32 GiB, Darwin 25.5.0, Go 1.26.6, native CGO ARM64, `noui nolocalllm`. Baseline source: `5d85de573289236b24d3f02b60fba72ec1c1d300`; optimized source: `cd5e02461285bbedca6b42454b78e93c737cd9cf`. Both binaries were built from clean source. JSON records binary hashes and embedded Go build provenance.

The same cached vectors and 1,000 queries/ground truth as [the full report](../README.md) were used, without AWS access. Search uses the public Qdrant-compatible gRPC API, K=10, CPU balanced preset, 100 warm-ups per beam, sequential client latency and 8 concurrent RPCs for throughput. Vector data was retained between restarts; logs confirm the in-memory HNSW graph was rebuilt each time. Graphs are independently constructed, so small recall differences are not a causal quality improvement. Tests and builds did not overlap these timing runs. RSS is a snapshot after queries, not peak memory.

| Beam | Source/run | Recall@10 | p50 ms | p95 ms | QPS | RSS MB |
|---|---|---:|---:|---:|---:|---:|
| preset | before | 0.9990 | 1.310 | 1.628 | 3,677 | 2,705 |
| preset | after 1 | 0.9993 | 1.163 | 1.415 | 3,972 | 2,530 |
| preset | after 2 | 0.9996 | 1.161 | 1.426 | 3,750 | 2,536 |
| 100 | before | 0.9990 | 1.321 | 1.561 | 3,647 | 2,706 |
| 100 | after 1 | 0.9993 | 1.104 | 1.299 | 3,852 | 2,530 |
| 100 | after 2 | 0.9996 | 1.144 | 1.589 | 3,888 | 2,557 |
| 512 | before | 1.0000 | 4.426 | 5.952 | 1,066 | 2,709 |
| 512 | after 1 | 1.0000 | 3.651 | 4.620 | 1,150 | 2,535 |
| 512 | after 2 | 1.0000 | 3.675 | 4.721 | 1,087 | 2,557 |

Preset p50 improved about 11%; beam-512 p50 improved about 17%. Throughput improvements are less stable (preset +2–8%, beam 512 +2–8%). This is one baseline and two optimized graph builds, not a statistical confidence interval. No memory increase was observed, but snapshot differences do not prove a causal memory reduction. Preset and explicit beam 100 are two measurements of the configured balanced beam, not different configurations.

The isolated 1,024-float dot-product benchmark changed from 243.8/244.9/244.0 ns/op to 94.78/94.08/94.41 ns/op, with 0 B/op and 0 allocs/op in every run. Command: `go test ./pkg/simd -run '^$' -bench '^BenchmarkDotProduct$/^SIMD-1024$' -benchmem -count=3 -benchtime=1s`. The normalized signed-input/block-boundary precision regression passed before and after the change; no tolerance or existing assertion was weakened.

## Source analysis

The requested read-only clone is at `~/src/qdrant`, pinned to the measured binary's v1.19.2 tag, commit `016542aa5deb6c66380bb137badf73d54f742bde`. Its [ARM64 dot scorer](https://github.com/qdrant/qdrant/blob/016542aa5deb6c66380bb137badf73d54f742bde/lib/segment/src/spaces/simple_neon.rs#L165) uses four independent fused multiply-add accumulators. NornicDB previously accumulated every four floats into one dependency chain. The change applies independent accumulators in NornicDB's existing kernel, retaining its four-wide and scalar tails. Other architectures and GPU implementations are unchanged; floating-point reduction order can differ.

Qdrant's [graph traversal](https://github.com/qdrant/qdrant/blob/016542aa5deb6c66380bb137badf73d54f742bde/lib/segment/src/index/hnsw_index/graph_layers.rs#L116) also pools visited state and batches neighbor scoring. NornicDB already pools its traversal state, but crosses the CGO boundary once per scored neighbor. A profile of the old public search path attributed 37.9% cumulative CPU to `runtime.cgocall`; that includes native arithmetic and must not be interpreted as pure call overhead. Batching is a further investigation candidate, not an implemented or proven gain.

The same profile attributed about 44% cumulative sampled CPU to synchronous standard-library request-log output. At the revisions measured in this report, success-request logging remained unchanged. The subsequent approved change is recorded in the [logging follow-up](../request-logging/README.md). This observation is specific to this logging sink/workload and is not an additive estimate alongside the scoring percentages. No logging was disabled to obtain the results above.

The measured path rebuilds and searches an in-memory HNSW graph. File-backed vector lookup allocation is not established as its bottleneck; no speculative mmap change was made. Search/construction beams, GPU support, lexical seeds and retrieval budgets were not changed.

## Reproduction and evidence

`measurement-driver.py` is the exact local driver used for these measurements, including machine-specific paths; its harness imports the existing benchmark runner. Adapt paths to a local cached corpus/data directory before repeating. It measures restarted graphs rather than the full fresh-ingest pipeline. The three published JSON files retain metrics and provenance; the original local JSON additionally retains every returned query ID. Corpus hashes/settings are in the [original corpus metadata](../final/corpus.json) and original balanced result. Raw logs/profiles remain in the ignored workspace `bench-data/vector_ann/profiling`.

Final validation is recorded in the PR result comments. CUDA/Vulkan native compilation, execution and performance still require the separate hardware setup. These CPU results do not establish GPU performance or lexical retrieval gains.

## Final validation

Checked product source `cd5e02461285bbedca6b42454b78e93c737cd9cf`, release base `f2ddd27cfae3788076778a0a0d48c827df046a3b`:

- Fresh `go test ./pkg/search ./pkg/qdrantgrpc ./pkg/simd -count=1` passed (171.733s / 3.353s / 1.512s).
- One `GOFLAGS=-count=1 make test` passed, including the logging/cardinality lint gates. 94 fresh package results, zero cached results and zero failures. Its output filters individual test/skips; this does not prove unavailable external fixtures executed.
- Native SIMD precision/package tests, non-CGO SIMD fallback tests, and focused SIMD/public lexical-construction race checks passed. This is not a full repository race run.
- `go vet ./pkg/simd ./pkg/search ./pkg/qdrantgrpc` passed. All 11 benchmark-harness tests passed fresh.
- The stacked benchmark branch includes the core commit; `git diff cd5e02461285bbedca6b42454b78e93c737cd9cf HEAD -- pkg cmd go.mod go.sum` is empty. Runtime source is identical to the tested core source; additional changes are report/evidence files.
- `make verify` was skipped at the maintainer's explicit request. No new CUDA/Vulkan device execution or SciFact measurement is claimed.

[Exact commands and retained output](checks.txt). Earlier canceled/failed verification evidence remains archived as described in the parent report.
