# Retrieval continuation qualification

NOR-406 targets v1.4.4. This work separates engine eligibility and continuation costs from embedding/reranking providers. It does not claim that the unavailable external application and its private 67 KB statement have been replayed.

## Reproduction and correction

A provider-free 49,000-document, two-dimensional unit-circle probe returns the first ten rows from a bounded initial pool in about 0.36 ms. It does not wait for all 469 eligible rows of the deterministic 1,000-document case. Selective subsequent pulls do perform a coverage pass. Before correction, the 49,000-document eleven-hit case measured approximately 9.0 ms and 6.35 MB per operation; the candidate measured approximately 7.4 ms and 1.91 MB, preserving eleven results and actual coverage depths.

The larger public Cypher fixture reproduces multi-second concurrent requests: 49,000 SearchSource nodes, 356 source groups, 1,024-dimensional vectors and approximately 1 KB text per source. A correlated `EXISTS` relationship predicate reduces the eligible population to 178 current sources. Sixteen concurrent filtered initial-page calls averaged about 3.5 seconds of request latency and 1.37 GB allocated per operation in the initial screening. Provider execution was controlled and negligible. These are generated engine workloads, not the private report's dataset or its 40-second screening replay.

Two causal owners are corrected:

- HNSW selection reuses the exact stage's existing complete-single-member bound before allocating per-node selection state. Hidden-member nodes, encoded aliases, custom member resolvers and unknown bounds retain expansion. The qualifying-score heap cannot need more than one score per grouped node.
- Correlated chain predicates use the existing bounded statement-plan owner. Parameterized `CALL db.retrieve($request)` installs this owner when its predicate arrives through parameters. Only value-free traversal structures are retained; property maps and dynamic syntax continue binding the current context's values. Graph paths, predicate verdicts and results are never cached by this correction.

No background producer, ranking contract, external protocol, Go API, storage payload, Badger key or WAL format is added or changed. The initial request still needs its ranked candidate pool: returning provisional discoveries before that pool is ranked would require a separate ordering decision. The measured cause here is repeated eligibility work, not complete materialization of all eventual cursor results.

## Reproduce

Run checks separately from benchmarks. Fixtures use the repository's temporary in-memory engine owner and are released by test cleanup; no dogfood database is read or modified.

```sh
go test -tags noui,nolocalllm ./pkg/search ./pkg/resultstream -count=1
go test -tags noui,nolocalllm -race ./pkg/search ./pkg/resultstream ./pkg/cypher \
  -run 'TestQuantizedContinuationSelectionBoundsSingleMemberCandidates|TestRetrievalWhereReusesChainSyntaxWithFreshStatementBindings|TestBoundChainSyntaxPlanDoesNotRetainParameterValues|TestProgressiveLookahead' -count=1
go test ./internal/cyphertext -run '^TestFoundationLexicalPopulation$' -count=1

go test -tags noui,nolocalllm ./pkg/cypher -run '^$' \
  -bench '^BenchmarkSearchSourceRetrieval$' -benchtime=16x -benchmem -timeout=30m
```

The public benchmark shares one disposable corpus per size. Its path is `size/operation/procedure/filter/concurrency`: sizes `12.5K`, `25K`, `49K`; operations `one-shot`, `first-page`, `five-pages`; procedures `retrieve`, `rretrieve`; filters `plain`, `exists`; concurrency 1 or 16. Five pages includes the initial ten and four subsequent pulls. Use anchored regex components to select one case; unanchored `retrieve` also matches `rretrieve`.

Both executor-result and search-response caches are disabled. Requests explicitly set `minRRFScore: 0` so a score cutoff does not confound population qualification. Other retrieval options use the service defaults. Query embeddings are supplied deterministically. A concurrency-safe stub reuses the canonical Voyage pass-through conversion to exercise rerank dispatch and memoization without model inference or external network traffic. The fixture is not an embedding-quality evaluation.

`ns/op` measures elapsed time divided by all operations (throughput at concurrency 16). `initial-ns/op` sums individual initial-execution durations and divides by operations (mean per-request latency). `pulls-ns/op` similarly sums subsequent pull work. Provider time and calls are reported separately. Whole-operation measurements include public execution, validation and cursor discard; phase measurements exclude those trailing controls. Every page's IDs are checked, including duplication across pages, and every issued cursor is discarded.

CPU profiles from `-cpuprofile` include corpus/index setup. Attribute request costs using the `runSearchRequest` call tree; do not describe whole-process samples as search-only. The small probe's optional search-phase profile starts after fixture construction and warm-up. Neither profile proves external provider latency.

## Source review and rejected experiments

The membership-completeness rule is shared by the candidate generator and exact grouping; no copied ownership rule remains. The statement parse uses `pipelineClausePlans` and its existing `clauseAnalysisBoundChain` slot, rather than another cache implementation. Canonical traversal and predicate evaluation remain unchanged.

Rejected experiments are retained only as evidence outside the repository:

- Replacing summed index populations with `max` is not safe: disjoint lexical/vector pools can have twice that many nodes. Node and vector-entry counts are also different domains.
- Dropping weak compressed entries without membership proof can lose a node with an unsurfaced strong sibling. The retained correction explicitly preserves those nodes and tests both directions.
- Routing the chain through the materializing correlated pipeline measured roughly 518–525 ms versus 308–310 ms in the paired 12,500-node sample, with more allocated bytes. That experiment was reverted.

Current Graphify retrieval returns HTTP 400 because HTTP is being sent to its HTTPS port; it provides no valid indexed baseline. Manual current-source review and the CI advisory checker must be reported separately. No checker rule, threshold, exclusion or hook was changed.

## Verification state

Focused public regressions fail on original production source and pass with the candidate. The search/resultstream package run passed, as did the focused integrated race checks and lexical population census. The complete paired measurements, current-head CI qualification and remaining external-workload limits are recorded on [NOR-406](https://linear.app/nornicdb/issue/NOR-406). Focused checks alone do not establish merge readiness.
