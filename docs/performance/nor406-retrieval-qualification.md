# Retrieval continuation qualification

NOR-406 targets v1.4.4. This work separates engine eligibility and continuation costs from embedding/reranking providers. It does not claim that the unavailable external application and its private 67 KB statement have been replayed.

## Reproduction and correction

A provider-free 49,000-document, two-dimensional unit-circle probe returns the first ten rows from a bounded initial pool in about 0.36 ms. It does not wait for all 469 eligible rows of the deterministic 1,000-document case. Selective subsequent pulls do perform a coverage pass. Before correction, the 49,000-document eleven-hit case measured approximately 9.0 ms and 6.35 MB per operation; the candidate measured approximately 7.4 ms and 1.91 MB, preserving eleven results and actual coverage depths.

The larger public Cypher fixture reproduces multi-second concurrent requests: 49,000 SearchSource nodes, 356 source groups, 1,024-dimensional vectors and approximately 1 KB text per source. A correlated `EXISTS` relationship predicate reduces the eligible population to 178 current sources. Sixteen concurrent filtered initial-page calls averaged about 3.5 seconds of request latency and 1.37 GB allocated per operation in the initial screening. Provider execution was controlled and negligible. These are generated engine workloads, not the private report's dataset or its 40-second screening replay.

Two causal owners are corrected:

- HNSW selection reuses the exact stage's existing complete-single-member bound before allocating per-node selection state. Hidden-member nodes, encoded aliases, custom member resolvers and unknown bounds retain expansion. The qualifying-score heap cannot need more than one score per grouped node.
- Correlated chain predicates use the existing bounded statement-plan owner. Parameterized `CALL db.retrieve($request)` installs this owner when its predicate arrives through parameters. Only value-free traversal structures are retained; property maps and dynamic syntax continue binding the current context's values. Graph paths, predicate verdicts and results are never cached by this correction.

No background producer, ranking order, wire shape, Go API, storage payload, Badger key or WAL format is added or changed. The initial request still needs its ranked candidate pool: returning provisional discoveries before that pool is ranked would require a separate ordering decision. The measured cause here is repeated eligibility work, not complete materialization of all eventual cursor results.

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

## Paired public workload results

Both complete 72-case matrices passed their result/eligibility checks, using the same generated harness against release `69115e8a2` and candidate `b7f79b193`. Each case ran 16 operations once; these are workload-scoped observations, not statistical qualification of latency differences. Provider time was measured separately with controlled, network-free inference.

| 49K workload | Release mean initial latency | Candidate mean initial latency | Allocated bytes change | Allocation count change |
| --- | ---: | ---: | ---: | ---: |
| retrieve, one-shot, EXISTS, concurrency 1 | 1,599.12 ms | 1,381.16 ms | −6.9% | −17.9% |
| retrieve, one-shot, EXISTS, concurrency 16 | 3,493.52 ms | 3,108.10 ms | −10.5% | −19.0% |
| rretrieve, one-shot, EXISTS, concurrency 1 | 1,608.59 ms | 1,392.39 ms | −6.9% | −17.9% |
| rretrieve, one-shot, EXISTS, concurrency 16 | 3,483.02 ms | 3,118.20 ms | −9.7% | −18.8% |
| retrieve, first page, EXISTS, concurrency 1 | 1,601.22 ms | 1,375.85 ms | −6.9% | −17.9% |
| retrieve, first page, EXISTS, concurrency 16 | 3,430.42 ms | 3,098.58 ms | −8.7% | −18.6% |

No case increased allocated bytes or allocation counts by more than 5%. Two unfiltered latency observations were selected for repetition before any no-regression claim: 12.5K first-page retrieve at concurrency 16 increased throughput-derived ns/op by 5.06%, and 49K five-page rretrieve at concurrency 16 increased it by 10.79%. Their allocated bytes/counts stayed approximately flat or fell. The repeated samples below overlap; this does not establish strict latency equivalence.

The boundary-page lookahead remains expensive in the measured b7f79b193 candidate. For example, release 49K filtered five-page retrieve at concurrency 1 spent about 1.616 s on initial execution and 3.969 s on subsequent pulls, allocating 5.028 GB per complete operation. Returning the last buffered page currently asks the engine to discover whether another row exists first. The operator subsequently approved returning full buffered pages immediately, with has_more including unfinished work and a final empty page permitted. The numbers above do not qualify that later correction or prove background population.

## Ready-page contract

A full buffered page now returns without expanding only to discover whether a
later row exists. `has_more` means buffered rows or unfinished retrieval may
remain; clients may receive a final empty terminal page. Requests that need
more rows still wait until the requested page is filled or retrieval finishes.
This is demand-driven population and preserves append-only ranked expansions.

Progressive serializes expansions. Retrieval runs outside the search metadata
mutex so an unrelated replay of ready rows can return during an expansion;
result deduplication, completion updates and metadata publication remain under
that mutex. No worker or detached search lifetime is introduced.

Public channel-gated search and stream tests check ready delivery during
expansion, shared insufficient pulls, exact page positions and final empty
exhaustion. The existing discard regression now requests two rows where only
one is buffered, ensuring it still exercises an actual in-flight expansion.

### Ready-page workload measurements

The same 49K filtered five-page workload ran with 16 operations per case on the
corrected production source, separately from local tests. Every case passed its
exact-ID, eligibility and cross-page duplicate assertions. CPU and allocation
profiles were captured; the CPU profile includes setup, with request attribution
filtered through `runSearchRequest`.

| Five-page workload | Release request time | Ready-page request time | Four later pulls | Allocated bytes/op | Allocation count change |
| --- | ---: | ---: | ---: | ---: | ---: |
| retrieve, concurrency1 | 5.585 s | 1.436 s | 3968.7 → 2.24 ms | 5.028 → 1.346 GB | -76.1% |
| retrieve, concurrency16 | 12.042 s | 3.167 s | 8471.3 → 2.58 ms | 4.855 → 1.270 GB | -76.4% |
| rretrieve, concurrency1 | 5.611 s | 1.445 s | 3981.4 → 2.04 ms | 5.035 → 1.353 GB | -76.1% |
| rretrieve, concurrency16 | 12.179 s | 3.160 s | 8557.9 → 2.36 ms | 4.871 → 1.274 GB | -76.3% |

Request time is summed mean initial/pull latency, not concurrent throughput
ns/op. The large reduction removes an unrequested expansion before delivering
page five. It does not prove faster complete-population work: a later request
beyond the current buffer still retrieves more. A 1K/469-hit public drain test
separately asserts complete delivery without duplicates. These generated,
single-batch observations do not qualify the private application or retained
heap equivalence. Initial correlated eligibility work still costs seconds under
concurrency and remains visible in the results.

Repeated unfiltered outliers use three 128-operation samples per binary. The
49K five-page rretrieve ranges overlap (release 3.85–4.27 ms/op, candidate
3.90–4.00 ms/op); 12.5K first-page retrieve ranges also overlap (0.567–0.638
versus 0.582–0.657 ms/op). Allocated bytes/counts are approximately flat or lower.
These do not establish statistically significant latency equivalence or a
speedup. They qualify the earlier b7f79b193 production, not the ready-page path.

## Source review and rejected experiments

The membership-completeness rule is shared by the candidate generator and exact grouping; no copied ownership rule remains. The statement parse uses `pipelineClausePlans` and its existing `clauseAnalysisBoundChain` slot, rather than another cache implementation. Canonical traversal and predicate evaluation remain unchanged.

Rejected experiments are retained only as evidence outside the repository:

- Replacing summed index populations with `max` is not safe: disjoint lexical/vector pools can have twice that many nodes. Node and vector-entry counts are also different domains.
- Dropping weak compressed entries without membership proof can lose a node with an unsurfaced strong sibling. The retained correction explicitly preserves those nodes and tests both directions.
- Routing the chain through the materializing correlated pipeline measured roughly 518–525 ms versus 308–310 ms in the paired 12,500-node sample, with more allocated bytes. That experiment was reverted.

Current Graphify retrieval returns HTTP 400 because HTTP is being sent to its HTTPS port; it provides no valid indexed baseline. Manual current-source review and the CI advisory checker must be reported separately. No checker rule, threshold, exclusion or hook was changed.

## Verification state

Focused public regressions fail on original production source and pass with the candidate. The search/resultstream package run passed, as did the focused integrated race checks and lexical population census. The complete paired measurements, current-head CI qualification and remaining external-workload limits are recorded on [NOR-406](https://linear.app/nornicdb/issue/NOR-406). Focused checks alone do not establish merge readiness.
