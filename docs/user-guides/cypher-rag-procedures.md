# Cypher RAG Procedures

NornicDB exposes seam-aligned Cypher procedures for in-query RAG orchestration:

- `CALL db.retrieve({query: '...', limit: 10, ...})`
- `CALL db.rretrieve({query: '...', limit: 10, ...})`
- `CALL db.rerank({query: '...', candidates: [...], rerankTopK: 50, rerankMinScore: 0.0})`
- `CALL db.index.vector.embed('...') YIELD embedding`
- `CALL db.infer({prompt: '...', max_tokens: 256, ...})`

These procedures are read-only and designed to map directly to internal contracts:

- Retrieval/rerank use existing `search.Service` + `SearchOptions`.
- Inference uses existing Heimdall manager `Generate`/`Chat` contracts.

## Procedure behavior

- `db.retrieve`
  - Uses existing hybrid search behavior.
  - Accepts explicit candidate-depth, RRF, property-filter, and fallback policy controls.
  - Supports durable START/PULL/DISCARD through the existing procedure. Continuation calls return one `page` column; ordinary calls retain the legacy result columns.
  - `failClosed: true` (alias `fail_closed`) is opt-in fail-closed retrieval: a usable numeric query embedding is required, strategy fallback including BM25-only is disabled, and supplied numeric policy values must be finite and in range (count fields such as `limit`, `candidateTarget`, and `rerankTopK` must be whole numbers; `rerankMinScore` must be finite). Embedding elements must be numeric types. It does not change ranking defaults. Absent the flag, empty embeddings still fall back to BM25.
  - Reranking is optional and follows request/config defaults.

- `db.rretrieve`
  - Shorthand retrieve path for simple usage.
  - Automatically enables rerank only when a reranker is configured and available.
  - Useful when you want one-call behavior while keeping `db.retrieve` + `db.rerank` available for explicit before/after comparisons.

- `db.rretrieveGraph`
  - `db.retrieveGraph` with the same automatic rerank as `db.rretrieve`: when a reranker is configured and available, it orders the fused pool of nodes and relationships in one call (`search_method` ends in `+rerank`; a failed rerank keeps the fused order and reports `fallback_reason: 'rerank_failed'`).
  - Takes `db.retrieveGraph`'s request and returns its rows, or with `n` / `maxResults` its page (see [Nodes and relationships in one ranking](#nodes-and-relationships-in-one-ranking)).

- `db.rerank`
  - Matches Stage-2 rerank API directly (does not run retrieval).
  - Requires caller-provided candidate rows (for example from `db.retrieve`).
  - Becomes pass-through ranking when no reranker is configured/available.
  - Use `rerankTopK` / `rerankMinScore` to tune rerank behavior.

- `db.index.vector.embed`
  - Embeds a text string using the configured embedding service for the current database.
  - Returns a vector array via `YIELD embedding`.
  - This is useful for fully manual Cypher search pipelines.

- `db.infer` caching behavior
  - The procedure itself does not cache; each call invokes the configured inference manager. Caching, when applicable, is the responsibility of the inference manager / model provider.

## Example

```cypher
CALL db.retrieve({query: 'zero-trust architecture', limit: 5}) YIELD node, score
WITH node, score
CALL db.infer({
  prompt: 'Summarize this node briefly: ' + coalesce(node.content, toString(node)),
  max_tokens: 120,
  temperature: 0.0
}) YIELD text
RETURN node, score, text
```

For deterministic retrieval policy, set candidate depth independently of
the final result limit and disable score-based or strategy fallback
explicitly. Add `failClosed: true` when a missing embedding must error
instead of falling back to BM25:

```cypher
CALL db.retrieve({
  query: 'zero-trust architecture',
  limit: 10,
  failClosed: true,
  candidateTarget: 50,
  adaptiveOverfetch: false,
  rrfK: 60,
  vectorWeight: 1.0,
  bm25Weight: 1.0,
  minRRFScore: 0.0,
  fallbackEnabled: false,
  filters: {
    lifecycle: 'active',
    generation: [3, 4],
    artifact: ['source', 'summary']
  }
})
YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered, fallback_reason
RETURN node, score, rrf_score
```

`minRRFScore` (default `0.01`) drops fused results below it. A result found by
one ranking only scores `1/(60 + rank)`, so the default keeps that ranking's
first 40. When the threshold leaves fewer results than `limit`,
`fallback_triggered` is `true` and `fallback_reason` is `min_rrf_score`; set
`minRRFScore: 0` to keep the deeper results.

Filter values use OR semantics within one property and AND semantics across
properties. Scalar and array-valued node properties are supported. Every policy
key also accepts snake_case, for example `candidate_target`, `rrf_k`,
`min_rrf_score`, `property_filters`, and `fallback_enabled`.

### Filtering with a predicate: `where`

`filters` tests property values for equality. `where` takes any Cypher
predicate on the candidate, bound as `node`, with the statement's parameters.
It is checked on every candidate before `limit`, together with `types` and
`filters`, so `limit` counts only results that pass:

```cypher
// A missing property, or a negation (current <> false is null, not true,
// when current is missing):
CALL db.retrieve({query: $q, where: 'node.current IS NULL OR node.current <> false'})

// A pattern from the candidate through the graph:
CALL db.retrieve({
  query: $q,
  where: 'EXISTS { (node)<-[:HAS_SOURCE]-(v:Version)<-[:HAS_VERSION]-(:Document)-[:MEMBER_OF]->(c:Collection)
          WHERE c.id IN $collections AND NOT c.archived AND v.current_source_id = node.id }'
})

// Vector search filtered before the top-k cut: supply the embedding.
CALL db.retrieve({query: $q, embedding: $vector, limit: 10, where: "node.state = 'current'"})
```

A search with `where` isn't served from the result cache, because what it
admits can depend on more than the request. Later pages of a paged retrieval
(`qid`) check it too. A `where` that isn't a string is an error.

When fallback changes the requested search path, `fallback_reason` contains a
stable code such as `query_embedding_failed`, `query_embedding_unavailable`,
`no_embedder`, `no_hybrid_results`, or `hybrid_search_failed`. Provider error
details are written to the warning log and are not exposed in query results.
A provider answer that isn't one non-empty vector per input, of the embedder's
dimension, is a failed embedding (`query_embedding_failed`) and is never
cached. A failed or unavailable embedding is reported over a later, benign
reason, and a chunk that wasn't searched with its embedding doesn't count in
`query_chunks_searched`.

`search_method` ends in `+rerank` only when the reranker ordered the results.
When a requested rerank didn't, the results keep their fused order,
`fallback_triggered` is `true`, and `fallback_reason` is `rerank_failed` (the
provider failed; such a result isn't cached), `rerank_scores_flat` (it scored
every candidate nearly the same) or `rerank_no_candidates` (no candidate had
content to score). `db.rerank`, which has no fallback to report, fails with
`rerank failed (<reranker>)` instead.

A positive `rerankMinScore` is an inclusive floor that applies to rerank
output and to `db.rerank` candidates. When the reranker ordered the results,
rows below it aren't returned, even when that leaves none; a candidate the
reranker didn't score is compared on its own score. `db.rerank` applies it on
every path, comparing a candidate's `score` when no rerank score is used.
Search fallbacks without rerank output (`rerank_failed`, `rerank_scores_flat`,
`rerank_no_candidates`, or no reranker configured) are returned unfiltered,
with their `fallback_reason`. The default minimum, 0, filters nothing.

A long query is split into chunks of 512 characters, and each chunk is
embedded and searched. At most `NORNICDB_SEARCH_QUERY_MAX_CHUNKS` chunks are
searched (32 by default). When a query has more, the results are for its first
chunks: `fallback_triggered` is `true` and `fallback_reason` is
`query_chunks_dropped`, unless an earlier fallback is reported. Whatever the
fallback reason, every `db.retrieve` / `db.rretrieve` row reports
`query_chunks_searched` and `query_chunks_total` (0 and 0 for a query searched
whole), as do the search metadata and a continued search's `page`. A search
that matches nothing returns no rows; to learn its coverage anyway, continue it
(`n`): a continued search always returns one `page`, with its chunk counts even
when `results` is empty. With `NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW=error`, such
a query fails instead (`search query has N chunks; at most M are searched`).

The chunks' results are fused into one ranking: `score` and `rrf_score` are
that fusion's score. A row's `vector_rank` and `bm25_rank` are the best rank
any searched chunk gave it by that method, and 0 means no searched chunk found
it that way, as for a query searched whole.

### What matched: `matches`

Every `db.retrieve` / `db.rretrieve` row has a `matches` column, a list of maps
saying what made the hit match and where it lies in the node's string
properties. A continued search's `page.results` (and their `passages`) carry
the same `matches`, as do HTTP and gRPC search hits.

- **`kind: 'chunk'`**: one of the node's embedding chunks matched the query
  vector, with its `chunk` index and `score` (similarity to the query). A vector
  hit lists its best chunk, then up to two more that reach the search's minimum
  similarity, best first. A chunk that runs across two properties is one match
  per property.
- **`kind: 'term'`**: one occurrence of a query term in a property the keyword
  index reads, with the `term` as the analyzer reads it (case-folded, NFKC). A
  keyword hit lists up to 32 occurrences: those of its rarest query terms first
  (by the keyword index's IDF, so a common word such as "the" doesn't crowd out
  the words that made the hit), each term's in the index's property order.
- **`property`, `start`, `end`**: the occurrence is `[start, end)` of that
  property's value, in Unicode code points as `substring()` counts them. They
  are null for a chunk whose place is unknown (see below).

```cypher
CALL db.retrieve({query: 'when is a late fee charged?', limit: 5})
YIELD node, matches
RETURN node.id,
       [m IN matches WHERE m.property IS NOT NULL |
        substring(node[m.property], m.start, m.end - m.start)] AS passages
```

`matches` is on by default. A caller that doesn't read it can pass
`matches: false` in the request: the column is then an empty list, and no
hit's chunks are scored or texts scanned.

Each chunk's place is recorded when the node is embedded: NornicDB finds the
chunk in the text it built from the node (labels and the included properties)
and maps it to the properties. A provider that chunks on its side (Voyage's
contextualized auto-chunking returns verbatim chunks) is matched exactly; a
chunker that trims or collapses whitespace is matched with whitespace ignored.
An embedding made before this release is placed from the chunk texts it kept
(provider chunkers keep them), so it needs no re-embedding; one without kept
chunk texts reports its chunks with null places until it is embedded again.

A text query passed to `db.index.vector.queryNodes` / `queryRelationships` is
embedded whole or not at all, since its rows (`node`, `score`) have no place to
report a partial query. It fails when it has more chunks than
`NORNICDB_SEARCH_QUERY_MAX_CHUNKS`, whatever the overflow setting, and when the
embedding of any chunk fails, is empty or has another dimension than the first
chunk's.

```cypher
CALL db.retrieve({query: 'zero-trust architecture', limit: 20}) YIELD node, score
WITH collect({id: node.id, content: coalesce(node.content, toString(node)), score: score}) AS candidates
CALL db.rerank({query: 'zero-trust architecture', candidates: candidates, rerankTopK: 20}) YIELD id, final_score
RETURN id, final_score
```

```cypher
CALL db.index.vector.embed('zero-trust architecture') YIELD embedding
CALL db.index.vector.queryNodes('doc_idx', 10, embedding) YIELD node, score
RETURN node, score
```

If you use `db.index.vector.embed()`, pass the returned `embedding` array into
`db.index.vector.queryNodes(..., embedding)` (or an inline array equivalent) for explicit pipeline control.

## Relationship search

With `NORNICDB_SEARCH_RELATIONSHIPS_ENABLED=true`, `db.retrieveRelationships`
takes the same request map as `db.retrieve` and searches relationship
properties instead of nodes. Rows have the columns
`relationship`, `start`, `end` (null when not visible), `score`, `rrf_score`,
`vector_rank`, `bm25_rank`, `search_method`, `fallback_triggered` and
`fallback_reason`; `types` filters by relationship type. It can't be
continued. See [Hybrid Search](hybrid-search.md#relationship-search).

```cypher
CALL db.retrieveRelationships({query: 'why these papers disagree', limit: 5})
YIELD relationship, start, end, score
RETURN type(relationship), relationship.reason, start.title, end.title, score
```

## Nodes and relationships in one ranking

`db.retrieveGraph` takes the same request map as `db.retrieve` and ranks
matching nodes and relationships together. The vector hits of both kinds
are ranked by similarity; the keyword hits by interleaving the node and the
relationship rankings, whose BM25 scores come from different indexes; the
two are fused by reciprocal rank, and a requested rerank orders the fused
pool in one call. `types` filters node labels, `relationshipTypes`
relationship types. Rows have `kind` (`'node'` or `'relationship'`), `node`,
`relationship`, `start`, `end`, the scores and search metadata of
`db.retrieve` (`query_chunks_searched` and `query_chunks_total` included),
and `matches`. Without relationship search it ranks nodes
alone.

```cypher
CALL db.retrieveGraph({query: 'late payment terms', limit: 10})
YIELD kind, node, relationship, start, end, score
RETURN kind, coalesce(node.title, type(relationship)), score
```

`db.rretrieveGraph` takes the same request and returns the same rows and page,
with the configured reranker ordering the fused pool when one is available, as
`db.rretrieve` reranks `db.retrieve`'s nodes.

With `n` or `maxResults` it returns one terminal page (`page.results`, each
with `kind`, `node` or `relationship` with `start` / `end`, the scores and
`matches`; `page.search_method`, `fallback_triggered`, `fallback_reason`, and
for a query split into chunks `query_chunks_searched` / `query_chunks_total`),
as `db.rretrieve`'s pages are read, and `YIELD page` works after other
clauses too. It can't be continued: `qid`, `discard`,
`mode`, `groupBy` and `rankedLimit` are errors.

In `db.retrieveGraph` and `db.retrieveRelationships`, `where` is checked on
relationship candidates too, before `limit`: the candidate is bound as
`relationship` and its endpoints as `start` and `end` (null when not visible),
with the statement's parameters and the calling row's variables. One
predicate serves both kinds: for a node candidate `relationship`, `start` and
`end` are null, for a relationship candidate `node` is null.

```cypher
CALL db.retrieveGraph({query: $q, limit: 10, where: '
  (node IS NOT NULL AND node.current <> false)
  OR (relationship IS NOT NULL AND start.id IN $ids AND end.id IN $ids)'})
YIELD kind, node, relationship
RETURN kind, coalesce(node.title, type(relationship))
```

## Durable search continuation

```cypher
CALL db.retrieve({
  query: 'sunset beach',
  mode: 'ranked_then_id',
  group_by: 'asset_id',
  limit: 500,
  ranked_limit: 5000,
  n: 50
}) YIELD page
RETURN page
```

Use `page.qid` to continue or discard the same retained population:

```cypher
CALL db.retrieve({qid: $qid, n: 50}) YIELD page RETURN page
CALL db.retrieve({qid: $qid, discard: true}) YIELD page RETURN page
```

One grouped result consumes one page slot. Its `passages` list contains the
matching child nodes in deterministic order. `id` mode permits an empty query
and performs a complete eligible storage scan. See
[Search Continuation](search-continuation.md) for the complete field and
consistency contract.

```cypher
CALL db.infer({prompt: 'Summarize: ...', temperature: 0.0}) YIELD text
RETURN text
```

## Lightweight Risk Notes

- Using LLM output as data in your own explicit downstream Cypher is supported.
- If you intentionally combine model-generated query text with dynamic execution procedures (for example, dynamic APOC execution), treat that as a high-risk pattern and review it carefully.
- Prefer parameterized query authoring and explicit mutation logic for sensitive write paths.
