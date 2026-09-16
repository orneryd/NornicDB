# Voyage AI

Voyage AI can be used as a managed embedding provider and as the Stage-2 reranker for hybrid search. NornicDB does not ship Voyage models; it calls the Voyage HTTP API with your API key.

Official references:

- Text embeddings: <https://docs.voyageai.com/docs/embeddings>
- Contextualized chunk embeddings: <https://docs.voyageai.com/docs/contextualized-chunk-embeddings>
- Reranking: <https://docs.voyageai.com/reference/reranker-api>

## Text Embeddings

```bash
export VOYAGE_API_KEY=pa-...
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=voyage
export NORNICDB_EMBEDDING_MODEL=voyage-4-large
export NORNICDB_EMBEDDING_DIMENSIONS=1024

nornicdb serve
```

`NORNICDB_EMBEDDING_API_URL` is optional for Voyage. When omitted, NornicDB uses `https://api.voyageai.com`.

NornicDB sends query embeddings with Voyage `input_type: query` and document embeddings with `input_type: document`. This keeps Voyage's asymmetric embedding models on the intended path without changing search callers.

## Contextualized Document Chunking

Voyage's contextualized embedding API can chunk a whole document and return chunk embeddings in one provider call. Enable it with `NORNICDB_EMBEDDING_VOYAGE_MODE=contextualized`.

```bash
export VOYAGE_API_KEY=pa-...
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=voyage
export NORNICDB_EMBEDDING_VOYAGE_MODE=contextualized
export NORNICDB_EMBEDDING_MODEL=voyage-context-4
export NORNICDB_EMBEDDING_DIMENSIONS=1024

nornicdb serve
```

The embedding worker defaults contextualized Voyage requests to the provider's maximum 32,000-token chunk size. Set `NORNICDB_EMBED_CHUNK_SIZE` (or `embedding_worker.chunk_size` in YAML) to use a smaller explicit size. The worker passes the selected size and overlap to Voyage as `chunk_size` and `chunk_overlap`. Provider chunk text is kept in embedding metadata only for provider-managed chunking responses; local deterministic chunking keeps compact metadata.

YAML:

```yaml
embedding:
  enabled: true
  provider: voyage
  api_key: "pa-..."
  model: voyage-context-4
  voyage_mode: contextualized
  dimensions: 1024

embedding_worker:
  chunk_size: 8192
  chunk_overlap: 50
```

## Reranking

Voyage reranking is configured through the existing search rerank feature flag. It uses Voyage's native `/v1/rerank` API, not the generic cross-encoder adapter.

```bash
export VOYAGE_API_KEY=pa-...
export NORNICDB_SEARCH_RERANK_ENABLED=true
export NORNICDB_SEARCH_RERANK_PROVIDER=voyage
export NORNICDB_SEARCH_RERANK_MODEL=rerank-2.5

nornicdb serve
```

`NORNICDB_SEARCH_RERANK_API_URL` is optional for Voyage and defaults to `https://api.voyageai.com`. If `NORNICDB_SEARCH_RERANK_API_KEY` is unset, NornicDB falls back to `VOYAGE_API_KEY`.

Per-database override example:

```cypher
CALL db.nornic.config.set('docs', {
  `db.nornic.embedding.provider`: 'voyage',
  `db.nornic.embedding.model`: 'voyage-context-4',
  `db.nornic.embedding.voyage.mode`: 'contextualized',
  `db.nornic.embedding.api.key`: 'pa-...',
  `db.nornic.search.rerank.enabled`: 'true',
  `db.nornic.search.rerank.provider`: 'voyage',
  `db.nornic.search.rerank.model`: 'rerank-2.5',
  `db.nornic.search.rerank.api.key`: 'pa-...'
})
```

## Inference

Voyage integration is limited to embeddings and reranking. NornicDB does not currently configure Voyage as a Heimdall/inference provider because the public Voyage docs used for this integration do not define an OpenAI-compatible chat or completions endpoint.
