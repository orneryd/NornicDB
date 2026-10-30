# Language-Agnostic BM25 Retrieval Quality Plan

## Objective

Improve base BM25 retrieval quality without increasing query latency, while keeping the default analyzer language agnostic. Exact lexical retrieval, optional partial-token matching, and optional language-specific analysis must remain separate behaviors.

## Measured Baseline

The official 300-query BEIR SciFact run isolates default prefix expansion as the primary ranking defect:

| Profile                               | Recall@10 | Recall@100 |  nDCG@10 |   MRR@10 |  MAP@100 |
| ------------------------------------- | --------: | ---------: | -------: | -------: | -------: |
| Current default, 32 prefix expansions |   0.74200 |    0.88422 |  0.59974 |  0.56161 |  0.55487 |
| Prefix expansion disabled             |   0.78661 |    0.87256 |  0.66343 |  0.63040 |  0.62372 |
| Absolute change                       |  +0.04461 |   -0.01167 | +0.06370 | +0.06879 | +0.06885 |

The paired-bootstrap 95% confidence interval for the nDCG@10 change is `+0.04363` to `+0.08556`. The Recall@100 interval is `-0.03000` to `+0.00667`, so the small observed depth-100 loss is not conclusive.

## Non-Negotiable Constraints

- The default analyzer uses language-neutral Unicode normalization, case folding, and exact terms.
- The default analyzer does not use English stopwords or stemming.
- Prefix expansion is opt-in partial-token matching, not base retrieval behavior.
- Query latency and allocations do not regress.
- Persisted indexes record every index-time analyzer and property-projection setting.
- All correctness fixtures, benchmarks, and small evaluation artifacts required by CI are tracked by Git.
- Large downloaded BEIR corpora and generated run files remain reproducible external artifacts rather than repository fixtures.

## Checklist

### 1. Lock Correctness Down With Failing Tests

- [x] Add a regression proving default search does not return prefix-only matches.
- [x] Preserve a regression proving explicitly enabled prefix expansion still works and remains bounded.
- [x] Add an exhaustive reference scorer used only by tests.
- [x] Compare optimized V2 IDs and scores with the exhaustive scorer on deterministic generated corpora.
- [x] Add stable document-ID tie-breaking assertions for equal scores.
- [x] Add concurrent cold-query coverage that fails under `go test -race` if query-plan caching is unsafe.
- [x] Add benchmark projection coverage proving only configured content fields are indexed.
- [x] Add language-neutral tokenizer fixtures for Latin, accented Latin, Greek, Cyrillic, Arabic, Devanagari, and CJK text.

### 2. Separate Exact Retrieval From Partial-Token Matching

- [x] Change `NORNICDB_BM25_PREFIX_MAX_EXPANSIONS` default from `32` to `0`.
- [x] Retain the environment override as explicit opt-in compatibility behavior.
- [x] Keep prefix expansion out of persisted index compatibility because it is query-time only.
- [x] Document that deployments requiring partial-token matches may enable bounded prefix expansion globally.
- [x] Do not add a final-token-only partial-match mode without a measured use case.

### 3. Repair Cache And Ordering

- [x] Make query-plan cache reads and writes race-free without holding an exclusive index lock during scoring.
- [x] Capture query-time expansion settings immutably when constructing an index so cached plans cannot observe setting changes.
- [x] Give score heaps a stable document-number tie-breaker and convert final ties to stable document-ID order.
- [x] Prove optimized pruning returns the same top-K documents and scores as exhaustive scoring.
- [x] Run focused unit tests and concurrent tests under the race detector.

### 4. Remove Benchmark Metadata Pollution

- [x] Add a general full-text property allowlist; preserve all-property indexing when no allowlist is configured.
- [x] Configure `recall-bench` to index only `title` and `text`.
- [x] Keep `beir_id` available as non-indexed result metadata.
- [x] Do not index `BEIRDocument`, `beir_id`, property names, or physical IDs in the benchmark lexical document.
- [x] Add the ordered property projection to BM25 build settings.
- [x] Force persisted BM25 rebuilds when an index-time projection or analyzer changes.
- [x] Retain concatenated `title + text` as the measured control; do not introduce field-aware scoring without a separate experiment.

### 5. Recover Depth-100 Recall Without Language Bias

- [x] Establish exact Unicode-token retrieval as the control profile.
- [ ] Ablate Unicode normalization and full case folding independently.
- [x] Do not introduce a default stopword list; stopwords are language- and domain-specific.
- [x] Do not introduce a default stemmer; optional analyzers may provide stemming explicitly.
- [ ] Evaluate language-neutral character n-grams only for scripts that lack reliable whitespace boundaries, with strict index-size and latency gates.
- [ ] Sweep `k1` and `b` offline only after document projection and tokenization are correct.
- [ ] Test index-time title weighting before considering query-time BM25F.
- [x] Reject any quality gain that requires additional unbounded query-time term expansion.

### 6. Verify Quality And Algorithmic Performance

- [x] Add in-memory search benchmarks for exact terms, prefix opt-in, cold plans, warm plans, common terms, and rare terms.
- [x] Record `ns/op`, `B/op`, and `allocs/op` over ten benchmark repetitions.
- [x] Compare results with `benchstat`; exact mode improves common-query latency by about 67% (`p=0.000`, `n=10`).
- [x] Require no increase in allocations per operation; exact mode reduces common-query allocations by about 85%.
- [x] Run the official 300-query SciFact exact profile and paired bootstrap comparison.
- [x] Require nDCG@10 `>= 0.66343` and Recall@10 `>= 0.78661`; measured `0.66345` and `0.78761`.
- [ ] Stretch target nDCG@10 `>= 0.68700` and Recall@100 `>= 0.88422`; measured `0.66345` and `0.88256`.
- [x] Run affected package tests, focused race tests, formatting, and diagnostics.
- [ ] Run the required Snyk Code scan. Repository-wide and four scoped CLI attempts returned Snyk `SNYK-0003` (HTTP 400); the dedicated scan tool was unavailable.

### 7. Verify Repository Artifacts

- [x] Use `git status --short` to enumerate every added test, benchmark, plan, and small fixture.
- [x] Use `git check-ignore -v` on every new artifact; no required source artifact is ignored.
- [x] Keep deterministic fixtures small enough for normal unit tests.
- [x] Document commands and hashes for large external corpora and generated TREC runs instead of committing them.
- [x] Run `git diff --check` before completion.

## Validation Record (2026-08-28)

### SciFact quality

Fresh title/text-only BM25 V2 run, 5,183 source documents (20,488 embedding chunks) and 300 official test queries:

| Profile                       | Recall@10 | Recall@100 |  nDCG@10 |   MRR@10 |  MAP@100 |
| ----------------------------- | --------: | ---------: | -------: | -------: | -------: |
| Historical 32-prefix baseline |   0.74200 |    0.88422 |  0.59974 |  0.56161 |  0.55487 |
| Exact Unicode title/text      |   0.78761 |    0.88256 |  0.66345 |  0.63089 |  0.62315 |
| Absolute change               |  +0.04561 |   -0.00167 | +0.06371 | +0.06928 | +0.06828 |

The paired-bootstrap nDCG@10 95% CI is `+0.04315..+0.08541`. The Recall@100 CI is `-0.02000..+0.01500`, so the small observed depth-100 change is not statistically significant.

### Query performance

Apple M3 Max, 20,000 deterministic in-memory documents, `-benchtime=250ms -count=10`:

| Path                  |  Time/op |      B/op | Allocs/op |
| --------------------- | -------: | --------: | --------: |
| Exact warm common     | 1.669 ms | 1.138 MiB |       542 |
| Exact cold common     | 1.657 ms | 1.139 MiB |       551 |
| Exact warm rare       | 2.552 us | 21.82 KiB |        11 |
| Prefix-32 warm common | 5.038 ms | 1.329 MiB |     3,602 |
| Prefix-32 cold common | 5.030 ms | 1.335 MiB |     3,618 |

Compared with Prefix-32, exact mode improves warm/cold common-query latency by `66.86%`/`67.05%`, bytes by `14.38%`/`14.69%`, and allocations by `84.95%`/`84.77%` (`p=0.000`, `n=10`).

### Reproduction and hashes

```sh
go test ./pkg/search -run 'TestFulltextIndexV2' -count=1
go test -race ./pkg/search -run 'TestFulltextIndexV2.*Concurrent' -count=1
go test ./pkg/search ./pkg/config ./cmd/recall-bench -count=1
go test ./pkg/search -run '^$' -bench '^BenchmarkBM25V2QualityPaths$' -benchmem -benchtime=250ms -count=10
bin/recall-bench run --mode bm25 --data-dir bench-data/nornic/scifact --queries bench-data/beir/scifact/queries.jsonl --manifest bench-data/runs/scifact-test-manifest.json --output bench-data/runs/scifact-bm25-language-neutral-title-text.trec
bin/recall-bench evaluate --qrels bench-data/beir/scifact/qrels/test.tsv --run bench-data/runs/scifact-bm25-language-neutral-title-text.trec --output bench-data/runs/scifact-bm25-language-neutral-title-text.metrics.json
bin/recall-bench compare --qrels bench-data/beir/scifact/qrels/test.tsv --baseline bench-data/runs/scifact-default-bm25.trec --candidate bench-data/runs/scifact-bm25-language-neutral-title-text.trec --output bench-data/runs/scifact-bm25-language-neutral-title-text.vs-prefix32.json
```

| Artifact                  | SHA-256                                                            |
| ------------------------- | ------------------------------------------------------------------ |
| SciFact corpus            | `dec31c8182f3d744c7d2c09423756fd1d17cbef75808db13ba01cc0aab4d1ac6` |
| SciFact queries           | `8ff84a7c903f722981cd8d595c022660140c51867b27608a6d4910db86080313` |
| SciFact test qrels        | `0864bb985e0ca2367ba217977e72004d549054b2b06666ed9d4825ac7c21284c` |
| Query manifest            | `71858742217160f412d3aba2cb7692af4ffa209da759a3ae775b3c2be5cac487` |
| Exact title/text TREC run | `0d687bb71030802ffe56b163dc482004ae802d4c2272b198cfe693983a795376` |
| Exact metrics JSON        | `3445426ec7bedb03267ebf3109b199ca4a031f34891ab65c432d388037b2ae53` |
| Paired comparison JSON    | `ab3bc6732541fa02577c6aeb211f65124ce6f5390fc67762d10b1ea0b4bfe698` |
| Ten-run benchmark output  | `f0b8110475f32e7ad1732a01e8764dd6a8027f680c255117992f8497fd15c296` |

The BEIR corpus and generated run/metrics files remain under ignored `bench-data/`. The plan and deterministic Go correctness/benchmark sources are not ignored and must be included with the code change.

## Implementation Order

1. Write and run the exact-vs-prefix, exhaustive parity, deterministic tie, and cache-race regressions in their failing state.
2. Disable default prefix expansion, then immediately rerun focused correctness and performance checks.
3. Repair cache synchronization and stable ordering, validating each change independently.
4. Add property projection and benchmark-only configuration, including persisted-build invalidation.
5. Run language-neutral analyzer ablations and parameter sweeps without combining variables.
6. Accept only changes that satisfy correctness, quality, latency, allocation, race, and artifact gates.

## Validation Commands

```sh
go test ./pkg/search -run 'TestFulltextIndexV2' -count=1
go test -race ./pkg/search -run 'TestFulltextIndexV2.*Concurrent' -count=1
go test ./cmd/recall-bench -count=1
go test ./pkg/search -bench 'BM25V2' -benchmem -count=10
go test ./pkg/search ./cmd/recall-bench -count=1
git status --short
git check-ignore -v <each-new-artifact>
git diff --check
```

SciFact quality runs use the reproducible protocol in `docs/performance/retrieval-recall-benchmark.md` and paired comparison through `recall-bench compare`.
