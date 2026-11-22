# N152-138 canonical owner map — ranked `db.rretrieve` continuation

Source-only map from commit `f77ec7706de099588ac232b32b6908860511d3da` (assigned worktree).
Every anchor below was read in this checkout. Nothing here is a fix design; the parent owns
fixture, validation, architecture and fix approval.

The graph/code-database retrieval attached to this investigation returned
`status: incomplete` (`nornicdbcode`/`nornicdocs` unreachable, `code_count: 0`). That is
**unavailable**, not "no similar owner exists". All mapping below is manual source reading.

## 1. Public consumer (Bolt → Cypher procedure)

| Owner | Anchor | Owns | Does not own |
|---|---|---|---|
| `db.rretrieve` entry | `pkg/cypher/call_rag.go:33` (`callDbRRetrieve` → `runSearchRequest(ctx, req, false, true)`) | request parsing, `useConfiguredRerank` | ranking, coverage, completion |
| Request → options | `pkg/cypher/call_rag.go:206` (`opts := search.GetAdaptiveRRFConfig(query)`), `:207` (`limit`), `:274-277` (`rerankTopK`), `:279` (`rerankMinScore`) | request fields onto `SearchOptions` | defaults (`pkg/search/search.go:504-518`: `Limit 50`, `RerankTopK 100`, `RerankMinScore 0`, `MinRRFScore 0.01`) |
| Continuation request | `pkg/cypher/call_rag.go:299-313`; `pkg/cypher/retrieval_request.go:24-37` | `qid/n/mode/groupBy/rankedLimit/maxResults/discard`, `requested()` | depth planning |
| Configured reranker gate | `pkg/cypher/call_rag.go:333` (`opts.RerankEnabled = svc.RerankerAvailable(ctx)`) | whether stage 2 runs at all | provider identity |
| Page projection | `pkg/cypher/call_rag.go:472-546` | `results`, `qid`, `has_more`, `position`, `returned`, `discovered`, `total`, `ranked_count`, `eligible_count`, `ranked_pool_exhausted`, `collection_exhausted`, `completion`, `fallback_*` | every value it copies |
| Cursor scope | `pkg/resultstream/registry.go:22-27` (`Scope{Owner,Database}`), `:150` `Start`, `:284` `Pull`, `:406` `validate` | one owner+database per stream; stream survives across the Bolt transactions that start/pull pages | rows, ordering, completion |

Each page is therefore a separate Bolt transaction on the same stream; the stream is not
transaction-scoped state, it is registry state keyed by owner+database+qid.

## 2. Ranked continuation producer (canonical stream owner)

`pkg/search/text_continuation.go`, function `startTextContinuation`.

| Unit | Anchor | Owns |
|---|---|---|
| Depth ceiling from caller | `:245-257` — `Limit<=0 → 50`; ranked mode `Limit = max(Limit, N)`; `ranked_then_id` `Limit = RankedLimit` | the **initial** depth only |
| Per-stream memo | `:259` `newRerankMemo()` | provider decisions + admitted windows for one stream |
| Step issue + retrieval | `:262-290` (`searchStep`): `execution.issue(...)`, `stepOptions.Limit = step.depth`, `SearchTextChunksWithErrorPolicy(...)` | what depth is actually retrieved |
| Stage-2 in the step | `:277-284` — `if stepOptions.RerankEnabled { ... applyContinuationRerank(...) }` | that rerank runs *after* chunk fusion, once per step |
| Initial rows | `:316` `initialResults := initial.Results[:execution.resultCount]` | initial buffer only (not deduplicated) |
| Expansion | `:331-362` `expand`: appends only ids absent from `state.seen`, then `execution.observe(..., len(state.results), nil)`, then `state.results, rows = state.results[:execution.resultCount], rows[:execution.resultCount]` | append-only row growth and one `seen` key per emitted id |
| Stream construction | `:364` `resultstream.NewProgressive(initialRows, execution.status.terminal, execution.completed.depth, expand)` | hands completion flag to the pager |
| Page metadata + hydration | `:462-500` `searchMetadataStream.Pull`; `:605-612` `searchResponseMetadata` | `ranked_count`/`discovered` = `len(state.results)`, completion strings, hydration (`pkg/search/text_catalog_continuation.go:597-660`, returns `ErrInvalidated`, never silently drops) |

Ordering, as approved: delivered order is the concatenation of admitted windows in
admission order, each window in provider order (`pkg/search/rerank_continuation.go:156-215`,
rows assembled at `:230-250`). It is **not** a global full-pool monotonic order.

## 3. Completion / exhaustion owner

`pkg/search/search_execution.go` — `continuationExecution` is the only writer of
`RetrievalExhausted`, `CandidateBudgetReached` and the completion reason.

| Unit | Anchor | Owns |
|---|---|---|
| Depth planning | `:40-65` `issue` (doubling, `nonGrowing` → population, `cap`) | requested depth |
| Completion decision | `:67-100` `observe`; `:82` `retrievalComplete := response.RetrievalExhausted && !response.CandidateBudgetReached`; `:90` terminal `eligible_population_exhausted`; `:92` `candidate_pool_exhausted` via `CandidateBudgetReached` **or** `step.depth >= step.population` | the only terminal decision |
| Output count | `:112-118` `admit` = `min(count, maxResults)`, where `count` is the *delivered unique* count | how many rows the page may emit |
| Page status | `:124-128` `continuationStatus.page(hasMore)` | `ranked_pool_exhausted`/`collection_exhausted`/`completion` strings |

Consequence worth keeping explicit: **coverage proof comes from retrieval, admission comes
from delivered rows.** `observe` is handed `len(state.results)` (post-rerank, post-dedup), so if
the rerank stage removes rows the retrieval still considers eligible, the terminal status can
be `eligible_population_exhausted` while fewer rows than the retrieved population were
emitted. That coupling is a source fact; whether it is the cause here is not established.

## 4. Window / fusion / rerank membership and offset owners

| Unit | Anchor | Owns |
|---|---|---|
| Continuation window producer | `pkg/search/rerank_continuation.go:102` `applyContinuationRerank`; `:112` `windowSize := effectiveRerankTopK(opts)`; `:113-121` `coverage = min(len(response.Results), opts.Limit)` | window boundary; `TopK` bounds **one provider window**, not the step |
| Candidate positions | `:123-133` `positions` (first index per id) | id→index inside this step's coverage |
| Retained windows | `:156-188` rebuild from `memo.acceptedWindows(query)`; un-covered ids are skipped (`:169-174`) | accepted-window stability across pulls |
| New windows | `:190-228` `fresh` = covered indices not in `admitted`, chunked by `windowSize`, `memo.acceptWindow(query, ids)` then `runRerank(...)` | first-admission membership |
| Row assembly | `:230-250` `ranked` → `response.Results`, `Returned`, `rerankExcluded` | the step's delivered rows |
| Provider invocation, validation, subset completion, filter, ordering | `pkg/search/rerank_outcome.go:82` `runRerank`; `:258` `completererankOutcome`; `:302-310` flat-verdict override; `:321` filter/limit; `:363` `selectEligibleRerankResults`; `:327` `originalrerankOutcome` | eligibility (`Filtered`, `minScore`), provider order, fallback reason |
| Memo | `pkg/search/rerank_memo.go` `acceptedWindows`/`acceptWindow`/`get`/`put`/`putUnranked` | one decision per (query,id), one admission per window |

Source-proven completeness chain **inside one step** (this is what rules out a pure
window/offset defect):

1. `coverage` covers a prefix of the step's fused list; every index in it is either in an
   accepted memo window or in `fresh` (`rerank_continuation.go:156-198`);
2. `admitted` dedupes by id across rebuild and fresh windows, so each covered id enters
   exactly one window;
3. `runRerank` returns `len(candidates)` results for every window when no minimum excludes a
   row: provider rows first, omitted candidates completed as pass-through
   (`rerank_outcome.go:312-317`), and `originalrerankOutcome` keeps `len(candidates)` because
   continuation windows pass no `resultLimit` (`rerank_continuation.go:180-186`, `:218-224`);
4. `collect` maps `OriginalRank-1` into the window's own `members` indices, so no row in a
   window can be dropped by index arithmetic;
5. emitted rows are appended once and never removed (`text_continuation.go:341-360`), and the
   pager never re-orders (`pkg/resultstream/progressive.go:47-140`).

The only ways a retrieved row can be retrieved-but-not-delivered inside one step are:
`outcome.excludedIDs` — provider `Filtered` from the local reranker's own minimum
(`pkg/search/local_rerank.go:167`) or `FinalScore < opts.RerankMinScore`
(`rerank_outcome.go:363-380`) — or the row never being in `response.Results` at all
(retrieval side).

## 5. Retrieval population owners (upstream of every rerank window)

| Unit | Anchor | Owns |
|---|---|---|
| Hybrid fusion + floor | `pkg/search/search.go:5420` `fuseRRFCounting`; `:7134-7141` `RRFScore < opts.MinRRFScore` dropped before sorting; `:5479-5483` fallback `min_rrf_score` when the page is short | which candidates exist at all |
| Hybrid exhaustion proof | `pkg/search/search.go:5421` `retrievalExhausted := vectorStats.exhausted && bm25.stats.exhausted && len(fusedResults) <= opts.Limit` | coverage proof — computed on the **post-floor** count |
| Chunk fusion | `pkg/search/chunked_search.go:145-152` (`chunkOpts.RerankEnabled=false` and chunk window `= max(chunkCandidateLimit(Limit), Limit)` under continuation), `:219`, `:331-335` (truncate to `Limit` clears `exhausted`), `:337` `RetrievalExhausted: exhausted && !budgetReached` | multi-chunk membership and exhaustion AND-ed across chunks |
| Chunk candidate window | `pkg/search/chunked_search.go:453-463` `chunkCandidateLimit = clamp(3*limit, 10, 100)` | one-shot window (raised to `Limit` for continuation) |
| One-shot stage 2 (adjacent, **not** continuation) | `pkg/search/search.go:7290-7310` `applyStage2Rerank` with `providerLimit: effectiveRerankTopK(opts)`, `resultLimit: opts.Limit`; `:7312-7317` `effectiveRerankTopK` (default 100) | single-search TopK cap only; continuation disables it in `chunkSearchOptions` |

## 6. Boundary facts that constrain the diagnosis

- Default `Limit = 50` (`search.go:504-518`), default `RerankTopK = 100`
  (`search.go:7312-7317`), default `RerankMinScore = 0`, default `MinRRFScore = 0.01`.
- The production local reranker is installed with `MinScore: 0.0`
  (`pkg/server/server.go:1535-1540`, `:1595`), so provider-side `Filtered`
  (`local_rerank.go:167`) cannot fire under defaults; `RerankMinScore` has no database
  setting binding and is request-only (`pkg/cypher/call_rag.go:279`).
- `LocalReranker.Rerank` truncates a batch to `cfg.MaxCandidates` (default 50,
  `local_rerank.go:118-124`) and returns a valid unique subset; `runRerank` completes the
  omitted candidates, so this changes provider coverage, not row count.
- `pkg/search/graph_search.go:203-224` also caps by `RerankTopK` but graph search refuses
  continuation (`pkg/cypher/call_rag.go:220-226`); it is not on this path.
