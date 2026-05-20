# Vector Embeddings

**Automatic embedding generation for semantic search.**

## Overview

NornicDB automatically generates vector embeddings for nodes, enabling:
- Semantic similarity search
- Hybrid search (vector + text)
- Automatic relationship inference
- Clustering and categorization

**Storage model:** NornicDB-managed embeddings are stored on nodes in `ChunkEmbeddings` (the first chunk is the main embedding). Client-managed vectors (e.g. via Qdrant gRPC) are stored in `NamedEmbeddings`.
See `docs/architecture/embedding-search.md` for details.

## Embedding Providers

| Provider | Latency | Cost | Quality |
|----------|---------|------|---------|
| Ollama (local) | 50-100ms | Free | High |
| OpenAI | 100-200ms | $$$ | Highest |
| Local GGUF | 30-80ms | Free | High |

## Configuration

### Ollama (Recommended)

```bash
# Start Ollama
ollama serve

# Pull embedding model
ollama pull mxbai-embed-large

# Configure NornicDB
export NORNICDB_EMBEDDING_ENABLED=true
export NORNICDB_EMBEDDING_PROVIDER=ollama
export NORNICDB_EMBEDDING_API_URL=http://localhost:11434
export NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
export NORNICDB_EMBEDDING_DIMENSIONS=1024
```

### OpenAI

```bash
export NORNICDB_EMBEDDING_PROVIDER=openai
export NORNICDB_EMBEDDING_API_KEY=sk-...
export NORNICDB_EMBEDDING_MODEL=text-embedding-3-small
```

### Local GGUF

```bash
export NORNICDB_EMBEDDING_PROVIDER=local
export NORNICDB_EMBEDDING_MODEL=mxbai-embed-large    # filename stem under NORNICDB_MODELS_DIR
export NORNICDB_MODELS_DIR=/models                   # directory containing the .gguf files
export NORNICDB_EMBEDDING_GPU_LAYERS=-1              # auto-detect
```

The local provider resolves the model file as `${NORNICDB_MODELS_DIR}/${NORNICDB_EMBEDDING_MODEL}.gguf`.

### Which properties are embedded

By default, the embedding worker builds text from **all node properties** and **node labels**. Managed embedding metadata is stored internally (`EmbedMeta`) to avoid property namespace pollution. You can limit this so that only specific properties are used, or exclude others.

**Use cases:**
- **Embed only one field** (e.g. `content`) so you don’t re-embed stored vectors or noisy fields.
- **Exclude internal or large fields** (e.g. `internal_id`, `raw_html`) from the text sent to the embedder.

**YAML** (in your config file under `embedding_worker`):

```yaml
embedding_worker:
  properties_include: [content]              # Only these keys (empty = all)
  properties_exclude: [internal_id, raw_html]
  include_labels: true                       # Prepend labels (default: true)
```

**Environment variables:**

```bash
# Embed only the "content" property (and labels)
export NORNICDB_EMBEDDING_PROPERTIES_INCLUDE=content

# Embed only content and title
export NORNICDB_EMBEDDING_PROPERTIES_INCLUDE=content,title

# Exclude internal fields (all other properties still embedded)
export NORNICDB_EMBEDDING_PROPERTIES_EXCLUDE=internal_id,raw_html

# Omit labels from embedding text (e.g. when using a single field)
export NORNICDB_EMBEDDING_INCLUDE_LABELS=false
```

If `properties_include` is set, only those keys are used (and exclude still applies). If only `properties_exclude` is set, all properties except those keys are used. See [Configuration Guide](../operations/configuration.md#embedding-text-which-properties-are-used) for full details.

## Automatic Embedding

### On Node Creation

When a node is created, embeddings are generated automatically:

```go
node, err := db.CreateNode(ctx, []string{"Document"}, map[string]any{
    "title":   "Machine Learning Basics",
    "content": "An introduction to ML concepts...",
})
// Embedding is generated asynchronously
```

### On Node Creation

```go
result, err := db.ExecuteCypher(ctx, `CREATE (n:KnowledgeFact {
    content: "User prefers dark mode for coding",
    title: "Preference"
}) RETURN n`, nil)
// Embedding is generated automatically from content + title
```

## Embedding Queue

Embeddings are processed asynchronously for performance:

```go
// Check queue status
status, _ := db.EmbeddingQueueStatus(ctx)
fmt.Printf("Pending: %d\n", status.Pending)
fmt.Printf("Processing: %d\n", status.Processing)
```

### Monitor Queue

```bash
curl http://localhost:7474/status | jq .embeddings
```

```json
{
  "enabled": true,
  "provider": "ollama",
  "model": "mxbai-embed-large",
  "pending": 42,
  "processed_total": 15234,
  "errors": 0
}
```

### Trigger Regeneration

```bash
# Regenerate all embeddings
curl -X POST http://localhost:7474/nornicdb/embed/trigger?regenerate=true \
  -H "Authorization: Bearer $TOKEN"
```

## Manual Embedding

### Embed Query

```go
// Generate embedding for search query
embedding, err := db.EmbedQuery(ctx, "What are the ML basics?")
if err != nil {
    return err
}

// Use for vector search
results, err := db.HybridSearch(ctx, "", embedding, nil, 10)
```

### Pre-computed Embeddings

```go
// Store node with pre-computed embedding
db.ExecuteCypher(ctx, `CREATE (n:KnowledgeFact {
    content: $content
}) RETURN n`, map[string]any{
    "content": "Important information",
})
// Pre-computed embeddings can be set via the vector index API
```

## Embedding Dimensions

| Model | Dimensions | Memory/Vector |
|-------|------------|---------------|
| mxbai-embed-large | 1024 | 4KB |
| text-embedding-3-small | 1536 | 6KB |
| text-embedding-3-large | 3072 | 12KB |

### Configuration

```bash
export NORNICDB_EMBEDDING_DIMENSIONS=1024
```

## Caching

### Embedding Cache

```bash
# Cache 10,000 embeddings in memory
export NORNICDB_EMBEDDING_CACHE_SIZE=10000
```

### Cache Behavior

- Identical text returns cached embedding
- Cache is LRU (Least Recently Used)
- Cache is not persisted across restarts

## Search with Embeddings

### Vector Search

```go
// Pure vector similarity search
results, err := db.FindSimilar(ctx, nodeID, 10)
```

### Hybrid Search

```go
// Combine vector + text search
results, err := db.HybridSearch(ctx, 
    "machine learning",    // Text query
    queryEmbedding,        // Vector query
    []string{"Document"},  // Labels
    10,                    // Limit
)
```

### RRF Fusion

Results are combined using Reciprocal Rank Fusion:

```
RRF_score = Σ 1/(k + rank_i)
```

Where `k` is typically 60.

## Indexing

### Vector Index (Auto Strategy)

Embeddings are indexed using an auto-selected strategy:

- **GPU brute-force (exact)** when GPU is enabled and `N` is within the configured threshold
- **CPU brute-force (exact)** for small datasets (low overhead)
- **HNSW (ANN)** for large datasets when brute-force is no longer viable

```go
// Indexing/search strategy is selected automatically at runtime.
// HNSW parameters (when used) are tuned for quality/speed balance:
//   M: 16
//   efConstruction: 200
//   efSearch: 50
```

### Rebuild Index

```bash
curl -X POST http://localhost:7474/nornicdb/search/rebuild \
  -H "Authorization: Bearer $TOKEN"
```

## Best Practices

### Content Preparation

```go
// Good: Combine relevant fields
content := fmt.Sprintf("%s\n%s", title, description)
memory := &Memory{Content: content}

// Bad: Too little context
memory := &Memory{Content: "yes"}
```

### Batch Processing

```go
// Process in batches for efficiency
for batch := range batches(nodes, 100) {
    db.CreateNodes(ctx, batch)
    // Wait for embeddings
    time.Sleep(time.Second)
}
```

### Monitor Quality

```go
// Check embedding coverage
result, _ := db.ExecuteCypher(ctx, `
    MATCH (n)
    WHERE n.embedding IS NOT NULL
    RETURN count(n) as with_embedding
`, nil)
```

## Troubleshooting

### Embeddings Not Generating

1. Check embedding service:
   ```bash
   curl http://localhost:11434/api/embed \
     -d '{"model":"mxbai-embed-large","input":"test"}'
   ```

2. Check queue:
   ```bash
   curl http://localhost:7474/status | jq .embeddings
   ```

3. Check logs:
   ```bash
   docker logs nornicdb | grep -i embed
   ```

### Slow Embedding

1. Use GPU acceleration
2. Increase batch size
3. Use embedding cache
4. Consider local GGUF models

## Disabling vector search per database

The per-database flag `NORNICDB_SEARCH_VECTOR_ENABLED` (default `true`) is the **strongest** memory-pressure lever — it prevents the in-memory ANN substrate from being populated at all, even when embeddings exist on disk.

### Distinct from `NORNICDB_EMBEDDING_ENABLED`

| Flag                              | Stops the embed worker? | Loads existing embeddings into RAM? | Vector queries work?   |
| --------------------------------- | :---------------------: | :---------------------------------: | :--------------------: |
| `NORNICDB_EMBEDDING_ENABLED=true`  | no                     | yes                                  | yes                    |
| `NORNICDB_EMBEDDING_ENABLED=false` | yes                    | **yes** (user-set vectors stay live)| yes                    |
| `NORNICDB_SEARCH_VECTOR_ENABLED=false` | no              | **no**                               | no (returns no results)|

`EMBEDDING_ENABLED=false` only stops automatic generation. User-set vectors written via `SET n.embedding = [...]`, `WITH EMBEDDING`, or external import are still durable in Badger and still iterated into the in-memory ANN substrate by today's search service. `SEARCH_VECTOR_ENABLED=false` is the stronger guarantee: those durable vectors are NOT iterated into RAM and no ANN strategy serves vector queries.

### Exports-only pattern

A common combination for downstream systems like Qdrant or Weaviate that keep their own vector indexes:

```yaml
memory:
  search_vector_enabled: false  # don't load embeddings into NornicDB's ANN substrate
embedding:
  enabled: true                  # do generate them
```

NornicDB writes the embeddings to durable Badger storage; an external pipeline (e.g. CDC into Qdrant) consumes them. NornicDB itself never builds HNSW / IVF / GPU brute-force structures and `db.index.vector.queryNodes` returns empty results with a WARN log.

### `db.index.vector.queryNodes` against a vector-disabled database

Returns zero rows and emits a `WARN` log line:

```
db.index.vector.queryNodes called against vector-disabled database — returning empty result
  subsystem=vector_search index_name=<index>
```

The query does **not** error — composite queries that gracefully handle empty vector results continue to succeed. Operators see the misconfiguration in logs.

## See Also

- **[Vector Search](../user-guides/vector-search.md)** - Search guide
- **[Hybrid Search](../user-guides/hybrid-search.md)** - RRF fusion
- **[GPU Acceleration](gpu-acceleration.md)** - Speed up embeddings
- **[Per-Database Search Index Flags](../operations/configuration.md#per-database-search-index-control)** — control BM25 and vector indexes per database
