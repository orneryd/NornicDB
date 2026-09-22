# Voyage AI

Voyage AI can be used as a managed embedding provider and as the Stage-2 reranker for hybrid search. NornicDB does not ship Voyage models; it calls the Voyage HTTP API with your API key.

Official references:

- Text embeddings: <https://docs.voyageai.com/docs/embeddings>
- Contextualized chunk embeddings: <https://docs.voyageai.com/docs/contextualized-chunk-embeddings>
- Multimodal embeddings: <https://docs.voyageai.com/reference/multimodal-embeddings-api>
- Reranking: <https://docs.voyageai.com/reference/reranker-api>

## Text Embeddings

```bash
export NORNICDB_EMBEDDING_API_KEY=pa-...
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=voyage
export NORNICDB_EMBEDDING_MODEL=voyage-4-large
export NORNICDB_EMBEDDING_DIMENSIONS=1024

nornicdb serve
```

`NORNICDB_EMBEDDING_API_URL` is optional for Voyage. When omitted, NornicDB uses `https://api.voyageai.com`.

NornicDB sends query embeddings with Voyage `input_type: query` and document embeddings with `input_type: document`. This keeps Voyage's asymmetric embedding models on the intended path without changing search callers.

## Contextualized Document Chunking

Voyage's contextualized embedding API can chunk a whole document and return chunk embeddings in one provider call. Enable it with `NORNICDB_EMBEDDING_MODE=contextualized`.

```bash
export NORNICDB_EMBEDDING_API_KEY=pa-...
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=voyage
export NORNICDB_EMBEDDING_MODE=contextualized
export NORNICDB_EMBEDDING_MODEL=voyage-context-4
export NORNICDB_EMBEDDING_DIMENSIONS=1024

nornicdb serve
```

The embedding worker defaults to 512-token chunks for contextualized Voyage requests and preserves any explicitly configured value, including 8,192. Documents that exceed Voyage's per-request budget are split into large consecutive segments on whitespace or sentence boundaries; every segment still uses Voyage auto-chunking with the same token-based `chunk_size`.

When overlap is not configured, NornicDB omits `chunk_overlap` so Voyage can apply its provider default. Set `NORNICDB_EMBED_CHUNK_OVERLAP=0` (or `chunk_overlap: 0` in YAML) to explicitly disable overlap. Positive values are sent unchanged. Provider chunk text and `chunker_version` are retained in managed embedding metadata.

YAML:

```yaml
embedding:
  enabled: true
  provider: voyage
  api_key: "pa-..."
  model: voyage-context-4
  mode: contextualized
  dimensions: 1024

embedding_worker:
  chunk_size: 512
  chunk_overlap: 50
```

## Multimodal Documents and Text Queries

Use a dedicated database with `NORNICDB_EMBEDDING_MODE=multimodal` and the
`voyage-multimodal-3.5` model. The background worker sends structured image and
text documents to `/v1/multimodalembeddings` with `input_type: document`.
Search text is sent to the same model and endpoint with `input_type: query`.

```bash
export NORNICDB_EMBEDDING_API_KEY=pa-...
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=voyage
export NORNICDB_EMBEDDING_MODE=multimodal
export NORNICDB_EMBEDDING_MODEL=voyage-multimodal-3.5
export NORNICDB_EMBEDDING_DIMENSIONS=1024
```

Store the ordered content parts in `_embedding_content`. The property can be a
native list of maps or a JSON string when a client cannot represent nested
property values. Text and images may be interleaved:

```json
[
  {"type":"text","text":"A diagram of the indexing pipeline"},
  {"type":"image_url","image_url":"https://example.com/diagram.png"}
]
```

For inline images, use `image_base64` with a PNG, JPEG, WEBP, or GIF data URI:

```json
[{"type":"image_base64","image_base64":"data:image/png;base64,..."}]
```

The worker batches structured documents for the same configured provider and
claims each node once across concurrent workers. Configure the provider-neutral
`NORNICDB_SEARCH_BM25_PROPERTIES` allowlist to keep structured image inputs out
of lexical indexing and rerank text, for example `title,text,description`.
Search callers can independently use `include_properties` or
`exclude_properties` to bound response properties; exclusion takes precedence
when the same key is present in both lists.

NornicDB validates the structured shape, supported data-URI media types, the
20 MB decoded inline-image limit, and Voyage's 1,000-input request limit. It
does not fetch remote images: Voyage fetches an `http` or `https` URL and
enforces pixel and token limits. Provider 4xx responses are terminal for the
node; rate limits and server failures use the configured bounded retry policy.

Managed vectors persist a provider/model-space identity. Search services only
index managed vectors for their configured database space, so contextualized
and multimodal vectors are not compared merely because their dimensions match.
Use separate logical databases when both spaces are needed. After changing a
database's mode or model, clear its prior managed embeddings and regenerate
them before searching the new space.

## Reranking

Voyage reranking is configured through the existing search rerank feature flag. It uses Voyage's native `/v1/rerank` API, not the generic cross-encoder adapter.

```bash
export NORNICDB_SEARCH_RERANK_API_KEY=pa-...
export NORNICDB_SEARCH_RERANK_ENABLED=true
export NORNICDB_SEARCH_RERANK_PROVIDER=voyage
export NORNICDB_SEARCH_RERANK_MODEL=rerank-2.5

nornicdb serve
```

`NORNICDB_SEARCH_RERANK_API_URL` is optional for Voyage and defaults to `https://api.voyageai.com`. Configure credentials with `NORNICDB_SEARCH_RERANK_API_KEY`.

For every candidate NornicDB sends the node's identifying properties
(`NORNICDB_SEARCH_RERANK_CONTEXT_PROPERTIES`, default `title,name`) followed by
the passage: for vector matches the matched chunk extended with its neighbouring
chunks, for lexical-only matches a query-centered window. Candidate content is
capped at 2048 UTF-8 bytes by default; set
`NORNICDB_SEARCH_RERANK_MAX_DOCUMENT_BYTES` to adjust the provider-independent
limit (raise it for scripts that need 2+ bytes per character, e.g. `4096` for
Cyrillic, to keep the same amount of text). Ranked continuation expansions reuse scores already obtained for the
same query and only send newly discovered candidates to Voyage.

Per-database override example:

```cypher
CALL db.nornic.config.set('docs', {
  `db.nornic.embedding.provider`: 'voyage',
  `db.nornic.embedding.model`: 'voyage-context-4',
  `db.nornic.embedding.mode`: 'contextualized',
  `db.nornic.embedding.api.key`: 'pa-...',
  `db.nornic.search.rerank.enabled`: 'true',
  `db.nornic.search.rerank.provider`: 'voyage',
  `db.nornic.search.rerank.model`: 'rerank-2.5',
  `db.nornic.search.rerank.api.key`: 'pa-...'
})
```

For a visual database, set the same keys with mode `multimodal` and model
`voyage-multimodal-3.5`. Per-database embedder reuse includes the mode, so a
text/contextualized configuration cannot alias a multimodal provider instance.

## Inference

Voyage integration is limited to embeddings and reranking. NornicDB does not currently configure Voyage as a Heimdall/inference provider because the public Voyage docs used for this integration do not define an OpenAI-compatible chat or completions endpoint.
