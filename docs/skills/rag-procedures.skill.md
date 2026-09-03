---
name: nornicdb-rag-procedures
description: Build RAG pipelines in NornicDB using Cypher procedures — db.retrieve (hybrid vector + BM25 + RRF), db.rretrieve (retrieve + rerank), db.rerank (cross-encoder), db.infer (LLM call). Use when assembling end-to-end retrieval, reranking, or RAG flows from Cypher rather than hand-rolling vector and full-text procedures separately.
---

# RAG Procedures (Cypher API)

NornicDB exposes the entire retrieval-augmented-generation pipeline as Cypher procedures. The mental model is "Cypher all the way down": write `CALL db.<verb>({...})` with a map argument and `YIELD` the columns you want.

## The four verbs

| Procedure           | What it does                                    | Stages                                                             |
| ------------------- | ----------------------------------------------- | ------------------------------------------------------------------ |
| `db.retrieve(req)`  | Hybrid retrieval                                | vector + BM25, fused with RRF                                      |
| `db.rretrieve(req)` | Retrieve + rerank when reranker is configured   | adds cross-encoder rerank if `NORNICDB_SEARCH_RERANK_ENABLED=true` |
| `db.rerank(req)`    | Standalone rerank over user-supplied candidates | cross-encoder only                                                 |
| `db.infer(req)`     | LLM generation                                  | calls the configured inference manager                             |

All four take a single map argument. Most fields support both camelCase and snake_case; pick one and stick with it.

## `db.retrieve` — hybrid retrieval

```cypher
CALL db.retrieve({
  query:          'authentication patterns',   -- required
  limit:          10,                          -- default 50
  failClosed:     true,                        -- require embedding; disable BM25 fallback
  minSimilarity:  0.5,                         -- vector floor
  types:          ['Document', 'Memory'],      -- label filter; alias: labels
  filters:        {lifecycle: 'active'},       -- property filters
  candidateTarget: 50,                         -- branch depth, independent of limit
  rrfK:           60,
  vectorWeight:   1.0,
  bm25Weight:     1.0,
  minRRFScore:    0.0,
  fallbackEnabled: false,
  rerankEnabled:  false,                       -- explicit on/off
  rerankTopK:     100,
  rerankMinScore: 0.0,
  embedding:      $queryVector                 -- optional pre-computed; alias: queryEmbedding / query_embedding
})
YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
RETURN node.id, node.title, score, search_method
ORDER BY score DESC
```

Behavior:

- `failClosed: true` (alias `fail_closed`) requires a usable numeric query embedding and disables strategy fallback, including BM25-only search when no embedding is available. It does not change ranking defaults. Invalid or non-finite supplied policy values error. The wrapped cause distinguishes a missing embedder, provider/timeout failure, empty output, or a non-vector embedding argument.
- If `embedding` is omitted and the embedder is configured, NornicDB embeds `query` server-side. Without `failClosed`, that path still falls back to BM25 when no embedding can be produced.
- Vector and BM25 results use equal weights by default. Set `vectorWeight` and `bm25Weight` for an explicit policy.
- `candidateTarget` controls each retrieval branch independently of the final `limit`; when omitted it is derived as `max(limit * 2, 20)` up to the configured maximum.
- `filters` (aliases: `propertyFilters`, `property_filters`) uses OR within each property and AND across properties. Scalar and array-valued node properties are supported.
- `minRRFScore: 0` retains single-branch results at deeper ranks; the default is `0.01`.
- `fallbackEnabled: false` returns the empty hybrid result instead of switching to vector-only and then BM25-only retrieval.
- `search_method` reports the winning path: `rrf_hybrid`, `rrf_hybrid+rerank`, `vector_only`, or `bm25_only`.
- `fallback_triggered: true` means one strategy returned nothing and the engine fell back.

## `db.rretrieve` — retrieve + auto-rerank

Same input shape as `db.retrieve`, but reranks when the reranker is available. Use this when you want "always rerank if you can":

```cypher
CALL db.rretrieve({
  query:      'zero-trust security architecture',
  limit:      20,
  rerankTopK: 100
})
YIELD node, score, search_method
RETURN node.id, score, search_method
```

If no reranker is configured, behaves identically to `db.retrieve` (no error).

## `db.rerank` — standalone reranker

Use it when you already have candidates from another source (manual ranking, federated search, cached list) and want to apply the cross-encoder:

```cypher
CALL db.rerank({
  query:      'authentication patterns',
  candidates: [
    { id: 'doc1', content: '...', score: 0.92 },
    { id: 'doc2', content: '...', score: 0.81 }
    -- score is optional; aliases: bi_score, rrf_score
  ],
  rerankTopK: 50
})
YIELD id, content, original_rank, new_rank, bi_score, cross_score, final_score
RETURN id, final_score
ORDER BY final_score DESC
```

`db.rerank` exercises the cross-encoder when one is configured (`NORNICDB_SEARCH_RERANK_ENABLED=true` plus a provider). Without a configured reranker the procedure still succeeds and returns candidates in pass-through order — the `cross_score` and `final_score` simply equal the input `score`, and rank order is unchanged. Candidates must include a non-empty `id`; missing `content` is allowed but will hurt rerank quality when the reranker is on.

## `db.infer` — LLM generation

```cypher
CALL db.infer({
  prompt:      'Summarize: ' + $context,   -- aliases: query
  model:       'llama3:70b',               -- optional; defaults to manager config
  max_tokens:  256,
  temperature: 0.0,
  top_p:       1.0,
  top_k:       0,
  stop_tokens: ['<|eot|>']
})
YIELD text, structured, model, usage, latencyMs, finishReason
RETURN text, finishReason, latencyMs
```

Chat form (when the inference manager exposes a chat endpoint):

```cypher
CALL db.infer({
  model: 'gpt-4o-mini',
  messages: [
    { role: 'system', content: 'You are a concise assistant.' },
    { role: 'user',   content: 'List three graph DB design patterns.' }
  ],
  max_tokens: 200, temperature: 0.0
})
YIELD text, model, usage, finishReason
RETURN text, finishReason
```

Notes:

- If `text` is valid JSON, `structured` is its parsed form. Otherwise `structured` is null.
- `usage` is `{ prompt_tokens, completion_tokens, total_tokens }` when the provider returns it.
- `db.infer` requires an inference manager. If none is configured the procedure errors with `inference manager is not configured`.

## End-to-end RAG in one query

```cypher
CALL db.rretrieve({ query: $userQuestion, limit: 5 })
YIELD node, score
WITH collect(coalesce(node.content, toString(node))) AS context, $userQuestion AS q

CALL db.infer({
  prompt: 'Use only this context:\n' + apoc.text.join(context, '\n---\n')
        + '\n\nQuestion: ' + q + '\nAnswer:',
  max_tokens: 300,
  temperature: 0.0
})
YIELD text
RETURN text AS answer
```

## Argument shorthand

For one-off interactive use, `db.retrieve` accepts a string or `$param` directly and treats it as `{query: <value>}`:

```cypher
CALL db.retrieve('authentication patterns') YIELD node, score RETURN node, score
CALL db.retrieve($q) YIELD node, score RETURN node, score
```

This shortcut is not available on `db.rerank` or `db.infer`, both of which require explicit map arguments.

## When to use which

- "Just give me good results" → `db.retrieve` (or `db.rretrieve` if you want the rerank when available).
- "I already have candidates from somewhere else" → `db.rerank`.
- "I need the model's answer, not search hits" → `db.infer`.
- "I want everything in one transaction" → chain them in a single Cypher statement.

## Tuning knobs

| Knob                              | Where                                        | Effect                                                                          |
| --------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------- |
| `limit`                           | `db.retrieve` request                        | Number of results returned                                                      |
| `minSimilarity`                   | `db.retrieve` request                        | Drops vector results below this cosine score                                    |
| `types` / `labels`                | `db.retrieve` request                        | Restricts to specific labels                                                    |
| `filters` / `propertyFilters`     | `db.retrieve` request                        | Restricts by node property values                                               |
| `candidateTarget`                 | `db.retrieve` request                        | Candidate depth requested from each retrieval branch                            |
| `adaptiveOverfetch`               | `db.retrieve` request                        | Enables adaptive widening toward the candidate target                           |
| `rrfK`                            | `db.retrieve` request                        | Reciprocal-rank fusion constant                                                 |
| `vectorWeight` / `bm25Weight`     | `db.retrieve` request                        | Relative contribution of each retrieval branch                                  |
| `minRRFScore`                     | `db.retrieve` request                        | Drops fused results below this score; use `0` to retain deep single-branch hits |
| `fallbackEnabled`                 | `db.retrieve` request                        | Allows or forbids fallback after empty hybrid retrieval                         |
| `rerankTopK`                      | `db.retrieve` / `db.rretrieve` / `db.rerank` | Pool size sent to the cross-encoder                                             |
| `rerankMinScore`                  | same                                         | Floor on `final_score` after rerank                                             |
| `temperature` / `top_p` / `top_k` | `db.infer`                                   | Sampling controls; lower = more deterministic                                   |
| `max_tokens`                      | `db.infer`                                   | Length cap                                                                      |

## Failure modes worth knowing

- **Empty results.** Most often: index missing, no embedder configured, or `minSimilarity` too high. `db.retrieve` returns zero rows rather than error.
- **`query is required`.** You called `db.retrieve` with a map that doesn't have `query` (or `text`).
- **`db.rerank requires non-empty candidates`.** You passed an empty list. Make sure your candidate-gathering subquery actually produced rows before calling.
- **`inference manager is not configured`.** `db.infer` only works when the deployment has an LLM connector wired up. Check the runtime config.
- **Reranker degrades silently.** If reranking fails or is unavailable, the engine returns the pre-rerank ranking and sets `search_method` to the non-rerank path. Don't expect a hard error.

## Configuration recap

```bash
# Hybrid + adaptive RRF — no extra config; on by default
# Reranking
export NORNICDB_SEARCH_RERANK_ENABLED=true
export NORNICDB_SEARCH_RERANK_PROVIDER=local      # local | ollama | openai | http
export NORNICDB_SEARCH_RERANK_MODEL=bge-reranker-v2-m3-Q4_K_M.gguf
export NORNICDB_SEARCH_RERANK_API_URL=...         # for provider=http (Cohere, TEI, ...)
export NORNICDB_SEARCH_RERANK_API_KEY=...
```

For per-database reranking, use the canonical
`db.nornic.search.rerank.{enabled,provider,model,api.url,api.key}` settings in
the YAML `databases:` map or `PUT /admin/databases/{name}/config`. The
`NORNICDB_SEARCH_RERANK_*` variables above remain supported global environment
alternatives. Per-database reranker changes hot reload by rebuilding only that
database's search service; API keys are redacted from settings responses.

Inference (`db.infer`) provider configuration is part of NornicDB's heimdall/inference subsystem and is configured separately from search.

## See also

- `nornicdb-vector-search` — the vector and full-text indexes the retrieval procedures sit on top of.
- `nornicdb-managed-embeddings` — generating the embeddings consumed by retrieval.
- `nornicdb-knowledge-policies` — suppression and decay scoring that filter the candidates retrieval sees.
