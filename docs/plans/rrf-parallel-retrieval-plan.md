# Parallel RRF Retrieval Plan

## Objective

Reduce hybrid-search retrieval latency by executing BM25 and vector retrieval concurrently before Reciprocal Rank Fusion (RRF), without changing ranking, filtering, fallback, cancellation, or observability semantics.

For vector duration `Tvector`, BM25 duration `Tbm25`, and scheduling/join overhead `Toverhead`, the expected index-stage latency changes from approximately:

```text
Tsequential = Tvector + Tbm25
Tparallel   = max(Tvector, Tbm25) + Toverhead
```

The maximum useful saving is therefore bounded by the faster branch. The change will be accepted only when benchmarks show that the saving exceeds goroutine and synchronization overhead.

## Scope

The parallel region is deliberately narrow:

1. Snapshot the reranker, full-text index, options, limits, and query context.
2. Execute BM25 lookup and vector-pipeline retrieval concurrently.
3. Join both branches and handle errors.
4. Continue existing post-processing sequentially.

Decay filtering and storage-backed type/property filtering run inside branch-local adaptive-overfetch callbacks and may execute concurrently. Each branch owns its result slice and orphan map; orphan state is merged only after both branches join. RRF, MMR, reranking, result enrichment, and response assembly remain outside the parallel region.

## Non-Negotiable Constraints

- Return the same result IDs, scores, ranks, method labels, and metadata as the sequential implementation.
- Preserve existing vector-error behavior and the outer `Search` fallback sequence.
- Always join the BM25 worker before returning, including vector error and context-cancellation paths.
- Continue passing the request context to vector retrieval.
- Do not hold `Service.mu` while acquiring or waiting for `pipelineMu`.
- Do not concurrently mutate result slices, response metrics, `seenOrphans`, or service state.
- Keep the index-stage metric as launch-to-join wall time, not the sum of branch durations.
- Retain independent `VectorSearchTimeMs` and `BM25SearchTimeMs` measurements.
- Do not change the `bm25Index` interface solely for this optimization. BM25 cancellation can be considered separately if measured need justifies the broader change.
- Do not add an external concurrency dependency; one buffered result channel is sufficient.
- Use repository documentation, not repeated synthetic text, for scaling benchmarks.
- Make corpus construction deterministic and record its manifest hash so sequential and parallel samples use identical indexed content.

## Proposed Design

Run BM25 in one worker goroutine and vector retrieval on the request goroutine. This creates the required overlap with one additional goroutine per hybrid request and avoids unnecessary scheduling and allocation overhead.

Each branch owns its local output and elapsed time. The BM25 worker sends one immutable result value through a channel buffered to one element. The request goroutine performs vector-pipeline acquisition and search, receives the BM25 result unconditionally, and only then handles a vector error or starts shared post-processing.

```go
type bm25RetrievalResult struct {
	results  []indexResult
	duration time.Duration
}

indexStarted := time.Now()
bm25ResultCh := make(chan bm25RetrievalResult, 1)

go func() {
	started := time.Now()
	results := fulltextIndex.Search(query, bm25Limit)
	bm25ResultCh <- bm25RetrievalResult{
		results:  results,
		duration: time.Since(started),
	}
}()

vectorStarted := time.Now()
pipeline, vectorErr := s.getOrCreateVectorPipeline()
var vectorResults []indexResult
if vectorErr == nil {
	vectorResults, vectorErr = retrieveVectorCandidates(ctx, pipeline, embedding, opts)
}
vectorDuration := time.Since(vectorStarted)

bm25Result := <-bm25ResultCh
s.observeSearchStage(ctx, "hybrid", "index", time.Since(indexStarted))

if vectorErr != nil {
	return nil, vectorErr
}
```

The production implementation should keep existing vector conversion and search-method detection local rather than introduce an abstraction unless extraction materially improves testability.

## Checklist

### 1. Build A Reproducible Documentation Corpus Fixture

Use tracked `docs/**/*.md` files, sorted by repository-relative path. Exclude this plan file because writing its validation record would otherwise change the corpus and manifest being recorded. At plan creation, the docs tree contains 198 Markdown files, 2,515,789 bytes, and 60,096 lines. Record fresh included counts in the validation record because documentation will evolve.

- [x] Add a benchmark-only loader that discovers Markdown files beneath `docs/` from the repository root, excluding this self-referential experiment record.
- [x] Sort repository-relative paths bytewise before reading files so node creation and index insertion order are stable.
- [x] Parse Markdown into normalized content chunks without discarding headings, prose, tables, or fenced code.
- [x] Produce overlapping windows of approximately 512 UTF-8 bytes with a 256-byte stride, snapping boundaries to valid UTF-8 and nearby whitespace.
- [x] Carry the file path, nearest heading, chunk ordinal, and source text as node properties.
- [x] Use stable node IDs derived from repository-relative path plus chunk ordinal.
- [x] Derive fixed-dimension, normalized vectors deterministically from each chunk's tokens; do not call a network embedding service during benchmarks.
- [x] Derive query vectors with the same function used for document chunks.
- [x] Verify repeated fixture builds produce identical IDs, text, vectors, and a stable SHA-256 manifest.
- [x] Require at least 8,000 indexed chunks from the current full docs corpus; fail the scaling benchmark setup rather than silently benchmarking an undersized corpus.
- [x] Keep the loader and vector derivation in benchmark/test code so production behavior is unaffected.
- [x] Do not commit a generated corpus copy. The repository Markdown files are the source fixture; record the Git revision and manifest hash with results.

The content-derived embedding function is a deterministic benchmark instrument, not a replacement for production embeddings. It should hash normalized tokens into signed dimensions and L2-normalize the resulting vector. This keeps vector construction reproducible while ensuring the vector branch uses the vocabulary and distribution of real NornicDB documentation.

### 2. Establish Scaling Baselines

- [x] Record a same-process sequential reference with result cache disabled and the vector pipeline warmed.
- [x] Add a docs-corpus hybrid benchmark at geometric tiers of 1,000, 2,000, 4,000, 8,000, and all available chunks.
- [x] Build each tier from a prefix of the same stable manifest so tiers are nested and comparable.
- [ ] Report actual source-file count, source bytes, chunk count, index-build duration, and index memory or serialized size for every tier.
- [x] Add query sets for BM25-dominant, vector-dominant, and balanced retrieval using terms and passages sampled from the docs corpus.
- [ ] Include queries from several documentation areas, including architecture, operations, performance, security, and user guides.
- [ ] Exercise HNSW and CPU brute-force candidate generation where fixtures permit.
- [x] Record single-caller and concurrent throughput at 1, 8, and 16 callers for the 8,000-chunk and full-corpus tiers.
- [x] Capture at least ten repetitions with `ns/op`, `B/op`, and `allocs/op` for `benchstat` comparison.
- [x] Run sequential baselines and parallel candidates against the same corpus manifest, query set, options, and warmed-index state.

### 3. Add Red-First Regression Tests

- [x] Add a deterministic overlap test with blocking retrieval fakes that cannot complete until both branches have started.
- [x] Verify the overlap test fails against the sequential implementation before changing production code.
- [x] Add a parity test comparing the parallel result with a sequential reference for IDs, scores, ranks, method labels, and metrics.
- [x] Add a vector-error test proving BM25 is joined and the existing error is returned unchanged.
- [x] Add a canceled-context test proving vector cancellation propagates and no BM25 worker escapes the request.
- [ ] Cover nil, empty, and disabled BM25 results.
- [x] Retain `TestHybridSearch_DoesNotHoldServiceLockWhileWaitingForPipeline` as a lock-order regression.

Test synchronization must use channels or barriers rather than sleeps. If the current concrete indexes cannot be controlled deterministically, introduce the smallest package-private test seam that follows the existing functional dependency-injection patterns.

### 4. Implement Parallel Retrieval

- [x] Snapshot `reranker` and `fulltextIndex` under `Service.mu`, then release the lock before either branch starts.
- [x] Derive vector/BM25 overfetch limits, minimum similarity, and query context before launch.
- [x] Create a buffered BM25 result channel with capacity one.
- [x] Start BM25 lookup in one goroutine using branch-local results and timing.
- [x] Acquire and run the vector pipeline on the request goroutine using branch-local results and timing.
- [x] Receive the BM25 result unconditionally before any return from the parallel region.
- [x] Return the vector error after the join so outer fallback behavior remains unchanged.
- [x] Inspect the successful vector pipeline's candidate generator after the join to preserve search-method labels.
- [x] Begin decay filtering and all other shared post-processing only after both branches have joined.

### 5. Preserve Observability

- [x] Measure `VectorSearchTimeMs` inside the vector branch.
- [x] Measure `BM25SearchTimeMs` inside the BM25 branch.
- [x] Observe the hybrid `index` stage from immediately before launch through completion of both branches.
- [x] Keep `FusionTimeMs` scoped to existing fusion/post-retrieval work.
- [x] Keep `TotalTimeMs` as request wall time rather than a sum of overlapping stages.
- [x] Update comments that describe retrieval as sequential or "parallel-ish."
- [x] Avoid adding a new public metric unless existing branch and wall-time measurements prove insufficient.

### 6. Verify Correctness And Concurrency

- [x] Run focused overlap, parity, error, cancellation, and lock-order tests.
- [x] Run focused hybrid-search tests under the race detector.
- [x] Run the complete `pkg/search` test suite.
- [x] Verify all existing RRF, filtering, MMR, reranking, fallback, and method-label tests pass unchanged.
- [ ] Confirm no goroutine leak with repeated cancellation and vector-error cases.
- [ ] Run `git diff --check` and verify all new tests and benchmark artifacts are tracked and not ignored.

### 7. Verify Performance And Scaling

- [x] Repeat each before/after benchmark at least ten times under equivalent machine load and configuration.
- [x] Compare benchmark samples with `benchstat` rather than a single-run percentage.
- [ ] Confirm balanced-query index latency approaches `max(Tvector, Tbm25)` rather than their sum.
- [x] Plot or tabulate retrieval latency and throughput against indexed chunk count for every geometric tier.
- [x] Report sequential and parallel speedup at each tier rather than only at the largest corpus size.
- [x] Confirm the improvement persists as the corpus grows and identify where the measured concurrency gain stops.
- [x] Require at least 10% statistically supported latency improvement for a balanced hybrid workload.
- [x] Reject regressions greater than 5% for small or strongly imbalanced workloads.
- [ ] Reject meaningful throughput or tail-latency regression under concurrent callers.
- [x] Keep allocated bytes and allocations per operation within 5% unless a measured latency gain justifies the difference.
- [x] Confirm retrieval-quality outputs are bit-for-bit unchanged on deterministic fixtures.

## Validation Commands

```bash
# Focused behavior and lock-order tests
go test ./pkg/search -run 'Test.*RRF.*Parallel|TestHybridSearch_DoesNotHoldServiceLock' -count=1

# Concurrency validation
go test -race ./pkg/search -run 'Test.*RRF.*Parallel|TestHybridSearch_DoesNotHoldServiceLock' -count=1

# Full package validation
go test ./pkg/search -count=1

# Performance samples
go test ./pkg/search -run '^$' -bench 'SearchProfile_RRFHybrid' -benchmem -count=10

# Documentation-corpus scaling samples
go test ./pkg/search -run '^$' -bench 'SearchProfile_RRFHybrid_DocsCorpus' -benchmem -count=10

# Repository hygiene
git diff --check
git status --short
git check-ignore -v docs/plans/rrf-parallel-retrieval-plan.md
```

Run the repository's required security scan after implementation. Record an external service failure separately rather than treating it as a code-validation success.

## Risks And Mitigations

| Risk                                                      | Mitigation                                                                                                              |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Goroutine overhead exceeds the saved branch time          | Measure small and imbalanced workloads; add a threshold only if benchmarks demonstrate a reliable crossover.            |
| BM25 continues after request cancellation                 | Always join it; retain the current interface limitation explicitly and evaluate context-aware BM25 separately.          |
| Worker blocks while the caller returns                    | Use a channel buffered to one and perform an unconditional receive before every return from the parallel region.        |
| Shared-state race in post-processing                      | Keep decay, storage filtering, orphan tracking, fusion, reranking, and enrichment after the join.                       |
| Lock inversion during cold pipeline initialization        | Snapshot under `Service.mu`, release it, and preserve the established `pipelineMu` ordering.                            |
| Metrics become misleading because branch times overlap    | Keep branch durations independent and define index/total measurements as wall time.                                     |
| Ranking or fallback behavior changes during refactoring   | Use exact parity and error-path tests before measuring performance.                                                     |
| High request concurrency doubles retrieval pressure       | Benchmark concurrent throughput and tail latency at representative caller counts before rollout.                        |
| Corpus is too small to expose ANN/BM25 scaling            | Chunk all tracked docs with overlap, enforce an 8,000-node minimum, and report geometric tiers through the full corpus. |
| Documentation changes invalidate before/after comparisons | Record the Git revision and corpus manifest hash; compare only runs with identical manifests.                           |
| Synthetic vectors hide corpus characteristics             | Generate deterministic token-derived vectors from the actual chunk text and use the same derivation for queries.        |

## Rollout And Rollback

Land the optimization only after correctness, race, and benchmark gates pass. Because this is an internal scheduling change rather than a public API change, the preferred rollback is a direct revert to sequential retrieval. Add a runtime flag only if staging or production measurements show workload-dependent regressions that cannot be reproduced reliably in benchmarks.

During rollout, compare hybrid index-stage wall time, branch durations, total search latency, error rate, CPU utilization, and concurrent throughput. A tail-latency increase without a corresponding median-latency gain is a rollback signal.

## Validation Record

Executed 2026-08-28 on an Apple M3 Max (`darwin/arm64`) at source revision `cefc6809e4f6`. The stable corpus excludes this validation plan and contains 197 files, 2,504,618 bytes, and 9,882 chunks. SHA-256 manifest: `738b68252e511ccb88666c0da90941a89492039c7df2d5e55e2a08a5dbe77d26`.

### Balanced Query Scaling

Ten matched repetitions per mode, HNSW warmed, result cache disabled:

| Indexed chunks | Sequential | Parallel |  Change |
| -------------: | ---------: | -------: | ------: |
|          1,000 |   215.3 us | 162.2 us | -24.66% |
|          2,000 |   444.2 us | 343.1 us | -22.78% |
|          4,000 |   861.3 us | 614.5 us | -28.65% |
|          8,000 |   1.314 ms | 984.3 us | -25.07% |
|          9,882 |   1.563 ms | 1.080 ms | -30.89% |

All balanced-query changes are statistically significant (`p=0.000`, `n=10`). Across all corpus tiers and balanced/BM25-dominant/vector-dominant query profiles, geometric-mean latency improved 17.23%, bytes per operation increased 0.30%, and allocations per operation increased 1.35%.

At 1,000 chunks, BM25-dominant latency improved 11.67% and vector-dominant latency improved 4.92%. At 9,882 chunks, BM25-dominant latency improved 22.47% and vector-dominant latency improved 14.16%. The 2,000-chunk imbalanced changes were statistically unchanged; no query profile regressed by 5% or more.

### Concurrent Caller Scaling

Ten matched repetitions per mode using the balanced query:

| Indexed chunks | Callers | Sequential | Parallel |                Change |
| -------------: | ------: | ---------: | -------: | --------------------: |
|          8,000 |       1 |   1.330 ms | 1.026 ms |               -22.84% |
|          8,000 |       8 |   176.4 us | 139.9 us |               -20.68% |
|          8,000 |      16 |   124.4 us | 126.4 us | unchanged (`p=0.052`) |
|          9,882 |       1 |   1.449 ms | 1.000 ms |               -30.96% |
|          9,882 |       8 |   189.0 us | 139.1 us |               -26.39% |
|          9,882 |      16 |   142.0 us | 142.2 us | unchanged (`p=0.971`) |

The concurrency improvement remains statistically significant through eight callers and stops by sixteen callers for both corpus sizes. At sixteen callers, bytes per operation increased 0.31% at 8,000 chunks and 0.35% at 9,882 chunks; allocations increased 1.34% and 1.44%, respectively.

### Correctness And Concurrency

- The deterministic overlap regression failed before the production change and passes afterward.
- Exact sequential-reference parity passes on real documentation chunks for result IDs, scores, ranks, method label, and candidate counts.
- Vector-error and context-cancellation tests prove BM25 is joined before return.
- The focused parallel/lock-order suite passes under the race detector.
- The complete `pkg/search` test suite passes.
