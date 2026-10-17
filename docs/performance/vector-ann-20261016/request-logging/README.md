# Configured gRPC request logging

The maintainer approved successful unary/streaming requests at debug and failures at warning/error, using the configured logger. Product source `5cfe2533f57e6884e7ad3db58fcb86c453e3fc2b` implements that contract. The production server passes its configured root logger, preserving its output, redaction and level policy. Standalone callers can inject `qdrantgrpc.Config.Logger`; nil uses an info-level text logger on stderr so failures remain visible.

One shared completion function selects debug for success, error for `Unknown`, `Internal`, `DataLoss` and `Unavailable`, and warning for other non-OK statuses. It checks the logger threshold before building completion attributes. Events include method, unary/stream kind, status, outcome and duration, without request payload/authentication metadata. Successful requests can be inspected with `NORNICDB_LOG_LEVEL=DEBUG`. Lifecycle/startup logs are outside this request-completion change.

## Measurement scope

Same Apple M2 Max, Darwin 25.5.0, Go 1.26.6, native ARM64/CGO `noui nolocalllm`, balanced CPU HNSW, 88,155 cached 1,024-dimensional vectors and 1,000 queries as the [ARM64 scoring follow-up](../arm64-dot/README.md). K=10; 100 warm-ups per beam; sequential client latency and concurrency-8 throughput. No tests or builds overlapped timing, and no AWS/Titan calls were made. Retained vector data is reused; the in-memory graph is rebuilt each restart. This is not a fresh-ingest benchmark or a lexical/GPU retrieval measurement.

The previous source `cd5e02461285bbedca6b42454b78e93c737cd9cf` already contains the scoring improvement. Search/construction beams, scoring code, lexical policies and GPU implementations are unchanged by this follow-up. Independently constructed graphs can differ in recall; small timing differences do not establish a causal gain. The earlier profile's approximately 44% logging CPU sample does not establish a 44% latency reduction.

## NornicDB results at default info level

| Beam | Run | Recall@10 | p50 ms | p95 ms | QPS | RSS MB |
|---|---|---:|---:|---:|---:|---:|
| preset | run 1 | 0.9988 | 1.124 | 1.308 | 4,098 | 2,736 |
| 100 | run 1 | 0.9988 | 1.161 | 1.381 | 4,087 | 2,736 |
| 512 | run 1 | 1.0000 | 3.645 | 4.414 | 1,157 | 2,736 |
| preset | run 2 | 0.9994 | 1.157 | 1.387 | 4,064 | 2,542 |
| 100 | run 2 | 0.9994 | 1.152 | 1.385 | 4,096 | 2,542 |
| 512 | run 2 | 1.0000 | 3.659 | 4.445 | 1,153 | 2,547 |

The earlier scoring-only balanced preset measured 1.161–1.163 ms p50 and 3,750–3,972 QPS with recall 0.9993–0.9996. The latest logging change measured 1.124–1.157 ms and 4,064–4,098 QPS, with recall 0.9988–0.9994. Beam-512 p50 remains approximately 3.65 ms at recall 1.0. One prior source and two new source graph builds are not enough to attribute these small differences or infer a universal retrieval/latency guarantee. No beam was lowered or recall assertion weakened. RSS snapshots are not peak memory; the first new run observed 2,736 MB, higher than its repeat's 2,542–2,547 MB. No memory reduction is claimed.

The previous driver run produced 6,401 legacy successful-request records. Both new runs produced zero gRPC request-completion records at info, as expected. Public RPC tests separately verify successful debug emission and warning/error failures; the absence of info-level successful logs is not evidence that failures disappear. Other startup/HTTP logs remain enabled. This outcome fixes configured-level behavior without disabling the logging sink.

The JSON records the clean source/binary provenance and metrics. The [measurement driver](../arm64-dot/measurement-driver.py) and cached corpus are unchanged. Original local JSON retains every result ID; raw profiles/logs remain in `bench-data/vector_ann/profiling`. Previous timing reports retain their actual measured revisions. GPU kernels/backends and lexical retrieval logic are unchanged; no new native CUDA/Vulkan or SciFact proof is claimed.

## Validation

Checked core head `5cfe2533f57e6884e7ad3db58fcb86c453e3fc2b`; checked release base `f2ddd27cfae3788076778a0a0d48c827df046a3b`.

- Public unary/streaming RPC regression tests failed against the legacy logging path, then passed after the fix. They cover info-level success suppression, debug success emission, warning/error failures and threshold suppression. A separate production-server test failed when logger injection was omitted, then passed with the actual configured logger, including authenticated success and unauthenticated warning.
- Focused `go test -race ./pkg/server ./pkg/qdrantgrpc -run 'TestQdrantGRPCUsesConfiguredRequestLogger|TestServerRequestLoggingPublicRPCs' -count=1` passed fresh (5.406s / 1.643s). This is not a full repository race run.
- Fresh full Qdrant gRPC package tests passed in 3.098s with 90.9% statement coverage. Both interceptors and the completion function reached 100%; coverage does not replace the public-path assertions above.
- One `GOFLAGS=-count=1 make test` passed: 94 fresh package results, zero cached results, zero failures. Logging and metrics lint gates passed. Its filtering hides individual test/skips, so unavailable external integrations are not claimed as executed.
- `go vet ./pkg/server ./pkg/qdrantgrpc ./cmd/nornicdb` passed. No benchmark-harness code changed; the previous 11 passing harness contracts continue to apply to identical source.
- The stacked benchmark branch contains identical runtime/test source: `git diff 5cfe2533f57e6884e7ad3db58fcb86c453e3fc2b HEAD -- pkg cmd go.mod go.sum` is empty. Runtime proof is from the core head, not a separate full invocation at the report commit.
- `make verify` skipped at the maintainer's explicit request. CUDA/Vulkan native execution and the absent SciFact fixture remain outside this proof.

[Exact commands and retained output](checks.txt). Original failing logs remain in the ignored primary workspace at `bench-data/vector_ann/profiling/logging-before.log` and `logging-wiring-before.log`; passing logs, coverage and timing data remain alongside them. No previously canceled check is counted as passing.
