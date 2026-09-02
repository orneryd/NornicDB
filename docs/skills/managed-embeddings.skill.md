---
name: nornicdb-managed-embeddings
description: Use NornicDB managed embeddings via Cypher — server-side embedding generation with WITH EMBEDDING, db.index.vector.embed, embedding providers (Ollama / OpenAI / local GGUF), property include/exclude, and the managed ChunkEmbeddings storage model. Use when the API surface is Cypher and you want NornicDB to embed text for you instead of computing vectors client-side.
---

# Managed Embeddings (Cypher API)

NornicDB can generate embeddings server-side and store them on nodes for you. From the consumer's point of view this is a Cypher feature: a `WITH EMBEDDING` clause on writes, and a `db.index.vector.embed` procedure for ad-hoc embedding.

## Two storage slots, both readable

Every node has two embedding slots:

1. **`ChunkEmbeddings`** — managed by NornicDB. `[][]float32` — one entry per chunk; `ChunkEmbeddings[0]` is the primary embedding. Built by the embedding worker or by `WITH EMBEDDING`. Internal metadata (`model`, `dimensions`, `chunk_count`, `embedded_at`) is stored separately so it does not pollute `Properties`.
2. **`NamedEmbeddings`** — client-managed. `map[string][]float32`, keyed by vector name (e.g. `"default"`, `"openai-small"`). Set via `db.create.setNodeVectorProperty` or the Qdrant gRPC compatibility layer.

Vector index lookup order is `NamedEmbeddings[indexProperty] → node.Properties[indexProperty] → ChunkEmbeddings`. You usually want a vector index on a label whose nodes have managed embeddings, in which case nothing extra is needed.

## Enabling managed embeddings

```bash
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=ollama          # ollama | openai | local
export NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
export NORNICDB_EMBEDDING_API_URL=http://localhost:11434
export NORNICDB_EMBEDDING_DIMENSIONS=1024
```

YAML form:

```yaml
embedding:
  enabled: true
  provider: ollama          # ollama | openai | local
  model: mxbai-embed-large
  url: http://localhost:11434
  dimensions: 1024
```

The OpenAI provider also reads `embedding.api_key` (or `NORNICDB_EMBEDDING_API_KEY`). The local provider resolves the model file inside `NORNICDB_MODELS_DIR`.

Defaults shipped with NornicDB: `provider=local`, `model=bge-m3`, `dimensions=1024`.

### Per-database configuration

For a named database, use canonical keys in the YAML `databases:` map or
`PUT /admin/databases/{name}/config`: `db.nornic.embedding.provider`,
`db.nornic.embedding.model`, `db.nornic.embedding.api.url`,
`db.nornic.embedding.api.key`, and `db.nornic.embedding.dimensions`. Their
`NORNICDB_*` forms remain supported global environment alternatives and
alternate input names; canonical keys win collisions.

Provider, model, API URL/key, dimensions, and GPU-layer updates hot reload by
rebuilding that database's search service. `db.nornic.embedding.enabled`, cache
size, property selection, label inclusion, and warmup interval are persisted
but require a process restart. Check `restartLevel` from
`GET /admin/databases/config/keys` instead of assuming every embedding setting
is dynamic.

### Provider configuration

| Provider | Required env | Notes |
|---|---|---|
| `ollama` | `NORNICDB_EMBEDDING_API_URL=http://host:11434` | `ollama pull <model>` first; works offline |
| `openai` | `NORNICDB_EMBEDDING_API_KEY=sk-...` | Models: `text-embedding-3-small` (1536), `text-embedding-3-large` (3072) |
| `local`  | `NORNICDB_MODELS_DIR=./models` | Resolves `${NORNICDB_MODELS_DIR}/${NORNICDB_EMBEDDING_MODEL}.gguf`; set `NORNICDB_EMBEDDING_GPU_LAYERS=-1` for auto-GPU |

### Embedding text — which properties contribute

By default the embedding worker builds the text from **all node properties + node labels** (excluding internal metadata). To restrict it:

```bash
# Embed only specific properties (and labels)
export NORNICDB_EMBEDDING_PROPERTIES_INCLUDE=content,title

# Always strip these properties from embedding text
export NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE=internal_id,raw_html

# Drop labels from the embedding text entirely
export NORNICDB_EMBEDDING_INCLUDE_LABELS=false
```

YAML equivalents under `embedding_worker:` — `properties_include`, `properties_exclude`, `include_labels`. If both include and exclude are set, exclude wins per-key.

## `WITH EMBEDDING` — embed during writes

`WITH EMBEDDING` is a NornicDB extension that embeds (or re-embeds) every node touched by the preceding mutation. Append it to the end of any write statement:

```cypher
-- Single create + embed
CREATE (d:Doc {id: 'd1', content: 'hello world'})
WITH EMBEDDING
RETURN count(d) AS created

-- Multiple creates in one transaction
CREATE
  (a:Doc {id: 'a1', content: 'alpha'}),
  (b:Doc {id: 'b1', content: 'beta'})
WITH EMBEDDING
RETURN count(*) AS created

-- Update content and re-embed
MATCH (d:Doc {id: 'd1'})
SET d.content = 'updated content'
WITH EMBEDDING
RETURN d.id

-- Bulk import + embed
UNWIND $items AS item
MERGE (h:Item {id: item.id})
SET   h.text = item.text
WITH EMBEDDING
RETURN count(h) AS processed
```

Behavior:
- Runs inside the same implicit transaction as the mutation. If embedding fails, the whole write rolls back.
- Builds embedding text using the current `properties_include` / `properties_exclude` / `include_labels` config.
- Chunks long text, generates one vector per chunk, and writes them all into `ChunkEmbeddings`. The first chunk is the primary embedding.
- Stores model name, dimensions, chunk count, and timestamp in internal `EmbedMeta`.
- Errors:
  - `WITH EMBEDDING requires configured embedder` — set `NORNICDB_EMBEDDING_ENABLED=true` and pick a provider.
  - `WITH EMBEDDING requires transaction-capable storage` — running on a legacy storage engine; not common in modern deployments.

## Embedding ad-hoc text

```cypher
CALL db.index.vector.embed('your text here')
YIELD embedding
RETURN embedding
```

Use it to feed a vector into a downstream search step, or to compute query embeddings client-side without leaving Cypher:

```cypher
CALL db.index.vector.embed('zero-trust authentication patterns')
YIELD embedding
CALL db.index.vector.queryNodes('docEmbeddings', 10, embedding)
YIELD node, score
RETURN node.title, score
```

## Setting client-managed vectors

For pre-computed embeddings (e.g. from a different model), use the Neo4j-compatible procedure to write into `NamedEmbeddings`:

```cypher
CALL db.create.setNodeVectorProperty(
  'nornic:abc-123',
  'embedding',          -- this becomes the property name; index on it the same way
  [0.7, 0.2, 0.05, 0.05]
)
```

Or just SET a property containing a list of floats:

```cypher
MATCH (n:Document {id: $id})
SET n.embedding = [0.7, 0.2, 0.05, 0.05]
```

A vector index whose `property` matches will pick up either form.

## Inspecting embedding state

```cypher
-- Coverage check
MATCH (d:Doc) RETURN count(d) AS total, count(d.has_embedding) AS embedded

-- Pending background work (when embedding is async)
-- HTTP: GET /status returns .embeddings { pending, processed_total, errors }

-- Embedded-text-side tuning has no Cypher diagnostic — re-run WITH EMBEDDING to refresh
```

## Common patterns

### Re-embed after a property edit

```cypher
MATCH (d:Doc {id: $id})
SET d.summary = $newSummary
WITH EMBEDDING
RETURN d.id, d.summary
```

### Bulk re-embed after changing the embedding model

```cypher
UNWIND $batch AS row
MATCH (d:Doc {id: row.id})
SET   d.touchedAt = timestamp()
WITH EMBEDDING
RETURN count(d) AS reembedded
```

(Run in batches of e.g. 200 to keep memory usage bounded.)

### Skip embedding for some labels

There is no per-label opt-out in `WITH EMBEDDING` — every node touched by the mutation is re-embedded. Structure the statement so it only matches the labels you want to embed.

## Gotchas

- `WITH EMBEDDING` is a NornicDB extension, not standard Cypher. It will not parse on Neo4j.
- The embedder must be configured before the mutation starts; otherwise the whole transaction fails.
- Dimensions are model-specific. Switching models requires re-embedding (the search service compares vector lengths against the index).
- `NORNICDB_EMBEDDING_DIMENSIONS` is used to validate provider responses. It does not silently truncate or pad.
- The local provider resolves the model file as `${NORNICDB_MODELS_DIR}/${NORNICDB_EMBEDDING_MODEL}.gguf`. Setting only `NORNICDB_EMBEDDING_MODEL=...` without putting the matching `.gguf` in the models dir will fail at first use.
- The global embedding cache (`NORNICDB_EMBEDDING_CACHE_SIZE`, default 10000) is per-process and not persisted. A per-database `db.nornic.embedding.cache.size` override is persisted configuration but takes effect after a process restart; cached vectors themselves are still not persisted.
- If you write `n.embedding = [...]` directly **and** also rely on `WITH EMBEDDING`, the managed `ChunkEmbeddings` slot is what the vector index sees by default — your manual property may be ignored. Pick one path per label.

## See also
- `nornicdb-vector-search` — the consumer of the vectors you generate here.
- `nornicdb-rag-procedures` — `db.retrieve`, `db.rerank`, `db.infer` for end-to-end RAG.
