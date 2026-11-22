# Evidence and extraction record — NOR-406 bound-chain EXISTS predicate

Initial DeepSeek investigation record (before the parent implementation below): source inspection and fixture extraction only. **No test, build, vet, benchmark
or profile was run for this change.** The host is occupied by other jobs; the parent owns
those runs. Every number below is quoted from the ticket's appended evidence or from the
extracted fixtures' recorded runs, not produced here.

- Isolated worktree: `/private/tmp/n152-90-retrieval`, HEAD
  `18b36a85d10e775b9d5d3500ecf3482682754a8d`.
- Ticket source: `/tmp/n152-90-ticket.json` (`NOR-406`, id
  `f777bf02-dcfa-4a92-bbb8-77d56a6324f4`, updated `2026-11-21T22:26:24Z`). Its description
  contains the initial report **and the appended 1.5.2 material** read for this task: the
  `causal-controls-macos-extract.log`, `causal-controls-radeon7-linux-extract.log`,
  `positive-observed-CPU-caller-chain.txt`, and the
  `1.5.2 release-head verification` section with its command.
- Root-cause lane worktree (read-only for this task):
  `/Users/timothysweet/.codex/worktrees/nor406-v151-retrieval-cost/NornicDB`, HEAD
  `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5`.

## 1. Extracted fixtures (byte-identical)

| File in this worktree | Origin path and state | Size | SHA-256 |
|---|---|---|---|
| `pkg/cypher/retrieval_nor406_exists_cost_test.go` | same path in the root-cause lane, untracked there | 579 lines | `7041baeec1143ce7429d063808fc80bb0034d271a6b3b3be26cb2bf460829800` |
| `pkg/search/retrieval_nor406_audit_test.go` | same path in the root-cause lane, untracked there | 825 lines | `ce7ba48b95f4ae02c39066aee64bc51c47c36fbdc4a6622f4f21676a176b47ba` |
| `pkg/cypher/retrieval_screening_bench_test.go` | same path in the root-cause lane, **modified** there | 639 lines | `1d20528a5fdb209d39dfcde590911bad2dd3c03431f24fb4ba03b5f56409eba8` |

The benchmark fixture's tracked version at `d6ec7766` is
`fec48002e88aa45492f4fed4f0375124e7018caf251a0d168ca02f76347bfe55`, which is
byte-identical to this checkout's HEAD version. The lane's modification is the 26/17-line
generalization already reviewed by the parent: `newNOR406RetrievalFixture` becomes
`testing.TB`-typed and delegates to a new
`newNOR406RetrievalFixtureOnEngine(tb testing.TB, population int, engine storage.Engine)`;
`nor406BulkCreateNodes`/`nor406BulkCreateEdges` take `testing.TB`. The graph, 1024-dimension
deterministic embeddings, index build, request parameters, matrix and body logic are
unchanged, so the count fixture measures the benchmark's own public retrieval over a ledger
engine. `git status` in this worktree shows exactly those three paths and no production
file.

### Base-commit difference (source inspection, no build)

`d6ec7766` is **not** an ancestor of this worktree's HEAD; the two are 57 commits apart.
`git diff d6ec7766 HEAD` over the files this change depends on:

| File | Difference |
|---|---|
| `pkg/cypher/traversal.go` | 80 lines, all in end-seed selection and reversal (`canSeedEnd`, `reverseTraversalMatch` for fixed chains, `traverseFromEndSeeds`). The prune and `traverseChainedGraph` bodies are unchanged. |
| `pkg/search/search.go` | 12 lines adding the fine-grained-authorization view branch at `Service.Search`. `filterVerdicts` creation and the filter path are unchanged. |
| `pkg/cypher/subquery_value_expression.go`, `row_predicate_plan.go`, `retained_predicate.go`, `relationship_endpoint_projection.go`, `pkg/search/filter_verdict.go`, `filter_evaluation.go` | no difference |

So the measured counts and the reviewed owners apply to this checkout, but the extracted
test files' **compilation here is unverified** (no build was permitted). `newTestMemoryEngine`,
`config.GetParserType`/`SetParserType`, `search.EnvSearchResultCacheEnabled`,
`search.Service.SetContinuationRegistry`, `resultstream.Config{MaxStreams, MaxStreamsPerOwner,
MaxRetainedBytesPerOwner, TTL}`, `resultstream.ErrCapacity`/`ErrGoneQID`,
`Service.EmbeddingCount` and the `newNOR406Population` helpers were each found in this
checkout by reading; none was exercised.

## 2. What the extracted fixtures provide

`pkg/cypher/retrieval_nor406_exists_cost_test.go` wraps the fixture's `*storage.MemoryEngine`
in `nor406ReadLedger`, which embeds the concrete engine so every optional capability stays
as it was and only observation is added. It counts label scans, first-by-label reads,
`AllNodes`, node streams, adjacency reads by requested relationship type **and by the node
expanded from**, `GetEdgesByType`/`AllEdges`, every point body read by capability
(`GetNode`, `GetNodeWithoutEmbeddings`, `GetQueryNode`, `GetNodeProjected`,
`BatchGetNodes`, `BatchGetNodesWithoutEmbeddings`), endpoint visibility, `GetEdge` and
`GetEdgeBetween`. It runs, per parser (`nornic`, `antlr`) and population (1,000, 2,000):
the unfiltered first page (control), the filtered first page, and a one-shot request whose
limit admits the whole matched population; then asserts matched rows/identities and the
per-candidate chain shape.

`pkg/search/retrieval_nor406_audit_test.go` pins the surrounding retrieval contracts on the
existing circle fixture: global top-N before population, complete-ranked first page waiting
for the ranked phase, provider/engine work per stage, bounded two-way independent readers,
same-cursor cost, ranked-cursor capacity in stream count and retained bytes, and
cursor cancel/retry/coalesce/shutdown. It asserts current behavior; it is not a correction.

## 3. Quoted measured evidence (not re-run)

Public first page, populated Linux host, real BM25/vector/fusion, controlled reranker,
explicit query embedding, both caches off (`nor406-population` ladder from the ticket):

| population | plain retrieve/rretrieve | EXISTS retrieve/rretrieve |
|---|---|---|
| 12,500 | 6.403 / 8.197 ms | 251.851 / 238.822 ms |
| 25,000 | 9.826 / 11.289 ms | 686.572 / 699.839 ms |
| 49,000 | 12.207 / 16.440 ms | 2,223.431 / 2,201.368 ms |

1.5.2 release head (`1a10ed67f`, macOS M2 Max, 12,500 sources, first page, three measured
iterations): `db.retrieve` 4.25 ms / 1.03 MB plain versus 153.28 ms / 88.90 MB correlated
EXISTS; `db.rretrieve` 10.57 ms / 1.75 MB versus 161.85 ms / 88.55 MB. The ticket notes the
allocation profile includes fixture setup.

Read counts from the extracted fixture (nornic; antlr agrees within noise on counts):
1,000 sources first-page EXISTS 369 evaluations, 1,107 adjacency, 1,107 query-node bodies,
5 batch body reads, 0 label scans / 0 first-by-label / 0 `AllNodes` / 0 node streams;
1,000 all-admitted 3,000 / 3,000; 2,000 first-page EXISTS 2,000 evaluations, 6,000 / 6,000,
9 batch; 2,000 all-admitted 6,000 / 6,000. `seeds-by-type` at 2,000: `HAS_SOURCE` 2,000,
`HAS_VERSION` 356, `MEMBER_OF` 356.

Root-cause attribution: the parent's 262.20 s Linux public CPU profile, focused on
`StorageExecutor.*Execute`, reports `evaluateWhere` 30.77 s cumulative,
`rowSubqueryValue` 29.60 s, `boundChainCount` 29.12 s, `traverseChainedGraph` 25.38 s,
`findPaths` 22.52 s.

## 4. Source review trail (read, not executed)

| Step | Location |
|---|---|
| `db.retrieve` compiles the `where` once per statement | `pkg/cypher/retained_predicate.go:124`, called from `pkg/cypher/call_rag.go:263` |
| Row-predicate planner declines the text (braces), so the row evaluator is used | `pkg/cypher/row_predicate_plan.go:314`; `pkg/cypher/executor_mutations_where_eval.go:56` |
| EXISTS/COUNT body dispatch order | `pkg/cypher/subquery_value_expression.go:293` → `boundNodeLookupCount` (`:436`), `boundDegreeCount` (`:971`), `boundChainCount` (`:592`), then `runCorrelatedSubquery` (`:382`) |
| Bound-chain plan is retained per statement and copied per row when the row binds the end node | `subquery_value_expression.go:577`, `:640` |
| Chain traversal and per-segment contexts | `pkg/cypher/traversal.go:1711`; each segment builds a fresh `TraversalContext` with its own `nodeCache` |
| Adjacency read and endpoint hydration | `traversal.go:178` (`traversalEdges`), `:2100` (`loadTraversalEndpointNode`), `:2127` (`findPaths`) |
| Prefix-prune owner and its guard | `traversal.go:1817`, `:1835`, `:1875`, `:1941`, `:1958` |
| Where `pruneWhere` is set today (clause pipeline only) | `traversal.go:505` |
| Per-invocation candidate verdict | `pkg/search/filter_verdict.go:24`; created at `pkg/search/search.go:5202`; consulted at `pkg/search/filter_evaluation.go:46` |
| Candidate filter owner | `pkg/search/search.go:7942` (`admitsNode`), `:8007` (`filterByTypeAndProperties`) |
| Fine-grained authorization view | `pkg/search/fine_grained_search.go:13`, entered at `search.go:5134` |

## 5. Prediction derivation

Fixture structure: 356 groups; group `g`'s collection is `g % 4`; collections 0 and 1 are
listed and not archived (178 groups, 178 admitted identities); the current source of group
`g` is source `g`. Therefore after pruning `v.current_source_id = node.id` on segment 1:

- all-admitted run evaluates every source, of which `min(population, 356)` are their group's
  current source: expansions = `population + 2 × min(population, 356)`.
- 2,000-source first page evaluates the whole pool (2,000), so the same 2,712.
- 1,000-source first page evaluates 369 candidates, a subset whose composition is not
  recorded in the log, hence `369 + 2k` with `178 ≤ k ≤ 356`; `k` is measurable from the
  ledger's `AdjacencySeeds`.

No duplicated suffix remains after pruning: each level-1 passer is a distinct group's
current source with a distinct version, so `HAS_VERSION`/`MEMBER_OF` expansions equal their
distinct seed counts. That is why a group-outcome memo is neither proposed nor needed, and
why the parent's withheld approval to memoize group outcomes is respected.

## 6. Unresolved causal evidence

1. The 1.5.2 profile is a **focused share** of a mixed run (plain and EXISTS cells across
   retrieve/rretrieve and first-page modes), not per-call EXISTS attribution. `boundChainCount`
   at 29.12 s cum of 396.44 s samples does not translate into a per-call figure, and the
   profile does not say which of the evaluated candidates took how long.
2. Whether `boundChainCount` handled **every** candidate of the 1.5.2 run is not traced. The
   body has no property-map braces and no dynamic values, so the shape is admitted, and the
   profile shows the function executing; an admission counter (like the existing
   `pipelineClausePlanBuildHook` seam used by `boundChainEvaluationPlan` tests) would prove
   coverage but was not added.
3. No allocation split exists between retained path copies, endpoint node copies, row maps
   and search-side work. The 1.05 GB cumulative figure at 49,000 sources is not decomposed,
   so the allocation reduction is expected to follow expansion count but is unproven.
4. The predicted after-counts are formula-derived and unverified. They also require changing
   the extracted fixture's characterization assertions (`3 × evaluated`), which the parent's
   acceptance note says needs an approved cost contract and a fail-first regression first.
5. Prefix pruning is only exercised today through the clause pipeline
   (`pkg/cypher/chain_prefix_volatile_evaluation_test.go` shows volatile conjuncts are never
   pruned and stable ones are). No test exercises it through the bound-chain EXISTS entry.
6. Latency scaling in the 12,500 → 49,000 table has no per-cell profile, so it is not
   established that latency tracks the read counts rather than the allocation volume.
7. The advisory duplicate analysis is incomplete: Graphify returned
   `/db/nornicdbcode/tx/commit` and `/nornicdb/search` timeouts and an empty code baseline.
   Section "Reuse review" in `design.md` is a manual checkout inventory, not a passed
   checker result.
8. Private-application provider-included 3.9–6.9 s retrieval and the 16-way screening
   workload are out of scope and unreproduced; no claim about them is made here.

## 7. Exact before-run commands (not run)

All commands run from `/private/tmp/n152-90-retrieval`.

```
# 1. Test-binary build and vet of the two affected packages
go vet -tags=noui,nolocalllm ./pkg/cypher ./pkg/search
go test -tags=noui,nolocalllm -run '^$' ./pkg/cypher ./pkg/search

# 2. Baseline read counts (the cost contract instrument)
go test -tags=noui,nolocalllm ./pkg/cypher \
  -run '^TestNOR406RetrieveExistsWhereCostOwner$' -count=1 -v -p=1

# 3. Retrieval contract controls around the change
go test -tags=noui,nolocalllm ./pkg/search \
  -run 'TestNOR406CompleteRankedFirstPageWaitsForRankedPhase|TestSearchTextContinuationIDModeGroupsCompleteFilteredPopulation' \
  -count=1 -p=1
go test -tags=noui,nolocalllm ./pkg/cypher \
  -run 'TestDbRetrieveContinuationGeneratedExecutionMatrix|TestDbRetrieveContinuationIssuedDepthCompletion' \
  -count=1 -p=1

# 4. Bound-chain plan and predicate controls
go test -tags=noui,nolocalllm ./pkg/cypher \
  -run '^TestRetrievalWhereReusesChainSyntaxWithFreshStatementBindings$|^TestBoundChainSyntaxPlanDoesNotRetainParameterValues$' \
  -count=1 -p=1
go test -tags=noui,nolocalllm ./pkg/cypher \
  -run 'ChainedPatternVolatilePredicatesAreNotEvaluatedOnPrefixes|ChainedPatternEvaluatesInjectedCounterOncePerPath' \
  -count=1 -p=1

# 5. Public first-page measurement, exactly the 1.5.2 release-head command
GOMAXPROCS=2 GOGC=60 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher \
  -run '^$' \
  -bench '^BenchmarkSearchSourceRetrieval$/^12.5K$/^first-page$/^(retrieve|rretrieve)$/^(plain|exists)$/^concurrency1$' \
  -benchmem -benchtime=3x -count=1 -cpuprofile=cpu.pprof -memprofile=alloc.pprof

# 6. Optional lab profile of the causal test at 2,000 sources
GOOS=linux GOARCH=amd64 go test ./pkg/cypher \
  -run '^TestNOR406RetrieveExistsWhereCostOwner$/(nornic|antlr)/2000$' \
  -count=1 -p=1 -v \
  -cpuprofile /tmp/nor406-exists-where-cpu.out -memprofile /tmp/nor406-exists-where-mem.out
```

The 49,000-source ladder and the 16-way concurrency cells are deliberately not listed: the
parent owns benchmark scheduling, and the ticket's own acceptance forbids the 16-way
screening workload.

## 8. Explicitly not done

No production edit, no test edit beyond the three extracted files, no build, vet, test,
benchmark, profile, staging, commit, push, PR, Linear write, Plane action or Graphify
re-ingestion. No claim that the extracted files compile in this checkout. No claim that the
duplicate analysis passed.
