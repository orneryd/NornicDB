# Vector Search Guide

**Complete guide to semantic search in NornicDB**

Last Updated: December 11, 2025

---

## Overview

NornicDB provides production-ready vector search with:

- **Automatic indexing** - All node embeddings are indexed automatically
- **Cypher integration** - `db.index.vector.queryNodes` procedure
- **String auto-embedding** - Pass text, get results (no pre-computation)
- **GPU acceleration** - 10-100x speedup with Metal/CUDA/OpenCL
- **Hybrid search** - RRF fusion of vector + BM25
- **Caching** - 450,000x speedup for repeated queries

> Important: semantic search requires embeddings. Embedding generation is disabled by default in current releases; enable it with `NORNICDB_EMBEDDING_ENABLED=true` (or `nornicdb serve --embedding-enabled`) or provide vectors yourself.

---

## How Vector Search Works

### Two Types of Indexes

NornicDB maintains **two complementary vector index systems**:

#### 1. Internal Automatic Index (Zero Configuration)

NornicDB automatically maintains an internal vector index that:

- **Indexes nodes** with managed embeddings in `node.ChunkEmbeddings` (main embedding is `ChunkEmbeddings[0]`)
- **Updates automatically** when nodes are created, updated, or deleted
- **Requires no setup** - works out of the box
- **Used by** REST API (`/nornicdb/search`) and hybrid search

```go
// This happens automatically at database startup:
db.searchService = search.NewServiceWithDimensions(storage, 1024)

// Nodes are indexed automatically via storage callbacks:
// OnNodeCreated → searchService.IndexNode(node)
// OnNodeUpdated → searchService.IndexNode(node)
// OnNodeDeleted → searchService.RemoveNode(nodeID)
```

`search.Service.IndexNode()` also indexes `node.NamedEmbeddings` (client-managed vectors, e.g. Qdrant gRPC) under IDs like `nodeID-named-{vectorName}`.

#### 2. User-Defined Cypher Indexes (Optional)

Create named indexes for specific labels/properties:

```cypher
CALL db.index.vector.createNodeIndex(
  'embeddings',      -- Your index name
  'Document',        -- Node label to filter
  'embedding',       -- Property name to search (also used as a NamedEmbeddings key if present)
  1024,              -- Vector dimensions
  'cosine'           -- Similarity: 'cosine', 'euclidean', or 'dot'
)
```

**Key insight:** User-defined indexes are **metadata only** - they specify which nodes to search and where to find embeddings. The actual embeddings come from either:

1. `node.NamedEmbeddings[index.property]` (or `"default"` when no property is configured)
2. The specified property (e.g., `node.Properties["embedding"]` when it contains a vector array)
3. `node.ChunkEmbeddings[0..N]` (best score across chunks)

### Embedding Lookup Order (Cypher `db.index.vector.queryNodes`)

When `db.index.vector.queryNodes` runs, it finds embeddings in this order:

```
1. NamedEmbeddings[index.property] (or "default")
2. node.Properties[index.property] (vector array)
3. ChunkEmbeddings[0..N]
```

This means **user-defined indexes can match managed embeddings** (via `ChunkEmbeddings`) and/or property vectors.

---

## Quick Start

### Cypher (Recommended)

```cypher
-- String query (auto-embedded)
CALL db.index.vector.queryNodes('embeddings', 10, 'machine learning tutorial')
YIELD node, score
RETURN node.title, score
ORDER BY score DESC

-- Direct vector array (Neo4j compatible)
CALL db.index.vector.queryNodes('embeddings', 10, [0.1, 0.2, 0.3, 0.4])
YIELD node, score
```

### Go API

```go
// Search for similar content
results, err := db.Search(ctx, "AI and learning algorithms", 10)
for _, result := range results {
    fmt.Printf("Found: %s (score: %.3f)\n", result.Title, result.Score)
}
```

---

## Cypher Vector Search

### `db.index.vector.queryNodes`

| Parameter    | Type                   | Description                 |
| ------------ | ---------------------- | --------------------------- |
| `indexName`  | String                 | Name of the vector index    |
| `k`          | Integer                | Number of results to return |
| `queryInput` | Array/String/Parameter | Query vector or text        |

**Query Input Types:**

```cypher
-- 1. String Query (Auto-Embedded) ✨ NORNICDB EXCLUSIVE
CALL db.index.vector.queryNodes('idx', 10, 'database performance')
YIELD node, score

-- 2. Direct Vector Array (Neo4j Compatible)
CALL db.index.vector.queryNodes('idx', 10, [0.1, 0.2, 0.3, 0.4])
YIELD node, score

-- 3. Parameter Reference
CALL db.index.vector.queryNodes('idx', 10, $queryVector)
YIELD node, score
```

## Qdrant gRPC: Text Queries (Upstream `Points.Query`)

If you have the Qdrant gRPC endpoint enabled, you can also run **text queries** using the upstream Qdrant protobuf contract (no custom protos).

**Requirements:**

- `NORNICDB_QDRANT_GRPC_ENABLED=true`
- `NORNICDB_EMBEDDING_ENABLED=true` (needed to embed the query text)

**Concept:**

- Use `qdrant.Points/Query` with `Query.nearest(VectorInput.document(Document{text: ...}))`.

See **[Qdrant gRPC Endpoint](qdrant-grpc.md)** for setup, configuration, and multi-language client examples.

### Storing Embeddings via Cypher

```cypher
-- Single property
MATCH (n:Document {id: 'doc1'})
SET n.embedding = [0.7, 0.2, 0.05, 0.05]

-- Multi-line SET with optional user metadata
MATCH (n:Document {id: 'doc1'})
SET n.embedding = [0.7, 0.2, 0.05, 0.05],
    n.embedding_dimensions = 1024,
    n.embedding_model = 'mxbai-embed-large',
    n.has_embedding = true
```

### Inline Embedding During Mutations (`WITH EMBEDDING`)

For mutations where vectors must be available immediately after the write, use `WITH EMBEDDING` on the same Cypher statement:

```cypher
CREATE (d:Doc {id: 'v1', content: 'vector ready now'})
WITH EMBEDDING
RETURN d.id
```

This performs embedding generation in the mutation transaction, so the node is queryable by vector search right away.

For additional patterns (`UNWIND`, `MERGE`, `SET`), see the Cypher guide section: `Inline Embedding on Mutations (WITH EMBEDDING)`.

### Creating Vector Indexes

```cypher
CALL db.index.vector.createNodeIndex(
  'embeddings',      -- index name
  'Document',        -- node label
  'embedding',       -- property name (also used as a NamedEmbeddings key if present)
  1024,              -- dimensions
  'cosine'           -- similarity function: 'cosine', 'euclidean', or 'dot'
)
```

> 💡 **Tip:** Managed embeddings are stored internally (`ChunkEmbeddings` + `EmbedMeta`). Even if a node has no `node.Properties["embedding"]`, Cypher/vector search can still match it via managed/internal embeddings and/or `NamedEmbeddings`.

---

## REST API (Hybrid Search)

The REST API uses NornicDB's **internal automatic index** for combined vector + BM25 search:

```bash
# Hybrid search (vector + BM25 with RRF fusion)
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "machine learning algorithms",
    "limit": 10,
    "labels": ["Document", "Memory"]
  }'
```

**Response:**

```json
{
  "status": "ok",
  "results": [
    {
      "id": "node-123",
      "title": "ML Basics",
      "score": 0.92,
      "rrf_score": 0.034,
      "vector_rank": 1,
      "bm25_rank": 3
    }
  ],
  "search_method": "hybrid",
  "metrics": {
    "vector_search_time_ms": 12,
    "bm25_search_time_ms": 8,
    "fusion_time_ms": 1
  }
}
```

---

## When to Use Each Approach

| Use Case                       | Recommended Approach                                               |
| ------------------------------ | ------------------------------------------------------------------ |
| General semantic search        | REST API `/nornicdb/search`                                        |
| Neo4j driver compatibility     | `db.index.vector.queryNodes` with user index                       |
| Filter by specific label       | User-defined index with label filter                               |
| Custom embedding property      | User-defined index with property name                              |
| Use managed embeddings         | Either (Cypher uses `ChunkEmbeddings`; HTTP uses `search.Service`) |
| Hybrid vector + keyword search | REST API (built-in RRF fusion)                                     |

---

## Go API

### Basic Search

```go
// Generate embedding
embedder, _ := embed.New(&embed.Config{
    Provider: "ollama",
    APIUrl:   "http://localhost:11434",
    Model:    "mxbai-embed-large",
})

embedding, _ := embedder.Embed(ctx, "Machine learning is awesome")

// Store node with embedding via Cypher
db.ExecuteCypher(ctx, `CREATE (n:KnowledgeFact {
    content: "Machine learning enables computers to learn from data",
    title: "ML Basics"
}) RETURN n`, nil)

// Search
results, _ := db.Search(ctx, "AI and learning algorithms", 10)
```

### Batch Embedding

```go
texts := []string{
    "Python is a programming language",
    "Go is fast and concurrent",
    "Rust provides memory safety",
}

embeddings, _ := embedder.BatchEmbed(ctx, texts)
// 2-5x faster than sequential embedding
```

### Cached Embeddings (450,000x Speedup)

```go
// Wrap any embedder with caching
cached := embed.NewCachedEmbedder(embedder, 10000) // 10K cache

// First call: ~50-200ms
emb1, _ := cached.Embed(ctx, "Hello world")

// Second call: ~111ns (450,000x faster!)
emb2, _ := cached.Embed(ctx, "Hello world")

// Check stats
stats := cached.Stats()
fmt.Printf("Cache: %.1f%% hit rate\n", stats.HitRate)
```

**Server defaults:**

```bash
nornicdb serve                        # 10K cache (~40MB)
nornicdb serve --embedding-cache 50000  # Larger cache
nornicdb serve --embedding-cache 0      # Disable
```

### Async Embedding

```go
autoEmbedder.QueueEmbed("doc-1", "Some content",
    func(nodeID string, embedding []float32, err error) {
        db.UpdateNodeEmbedding(nodeID, embedding)
    })
```

---

## GPU Acceleration

### Enable GPU

```go
gpuConfig := &gpu.Config{
    Enabled:          true,
    PreferredBackend: gpu.BackendMetal, // or CUDA, OpenCL, Vulkan
    MaxMemoryMB:      8192,
}

manager, _ := gpu.NewManager(gpuConfig)
index := gpu.NewEmbeddingIndex(manager, gpu.DefaultEmbeddingIndexConfig(1024))

// Add embeddings and sync
for _, emb := range embeddings {
    index.Add(nodeID, emb)
}
index.SyncToGPU()

// Search (10-100x faster!)
results, _ := index.Search(queryEmbedding, 10)
```

### GPU Backends

| Backend    | Platform       | Performance | Notes              |
| ---------- | -------------- | ----------- | ------------------ |
| **Metal**  | Apple Silicon  | Excellent   | Native M1/M2/M3    |
| **CUDA**   | NVIDIA         | Highest     | Requires toolkit   |
| **OpenCL** | Cross-platform | Good        | Best compatibility |
| **Vulkan** | Cross-platform | Good        | Future-proof       |

---

## Hybrid Search

Combines vector similarity with BM25 full-text search using RRF (Reciprocal Rank Fusion):

```cypher
-- Via Cypher
CALL db.index.vector.queryNodes('memories', 20, 'authentication patterns')
YIELD node, score
WHERE node.type IN ['decision', 'code'] AND score >= 0.5
RETURN node
```

```go
// Via Go API
vectorResults, _ := db.Search(ctx, "machine learning", 10)
fullTextResults, _ := db.SearchFullText(ctx, "machine learning", 10)
combined := mergeResults(vectorResults, fullTextResults)
```

---

## Performance Tuning

### Vector Strategy Selection

NornicDB chooses the fastest available vector-search strategy at runtime:

1. **K-means clustered search** (when clustering is enabled and clustered)
2. **GPU brute-force (exact)** (when GPU is enabled and the dataset is within the configured threshold)
3. **CPU brute-force (exact)** for small datasets
4. **HNSW (ANN)** for large CPU-only datasets

GPU brute-force is **exact** and typically stays competitive to much larger `N` than CPU brute-force due to massive parallelism.
Once brute-force becomes too slow (or GPU is unavailable), the pipeline switches to HNSW.

Tuning knobs:

```bash
# Use GPU brute-force when N is in this range (defaults shown)
export NORNICDB_VECTOR_GPU_BRUTE_MIN_N=5000
export NORNICDB_VECTOR_GPU_BRUTE_MAX_N=15000
```

### Compressed ANN profile (`quality=compressed`)

NornicDB also supports a compressed ANN profile for large-scale memory economics:

```bash
export NORNICDB_VECTOR_ANN_QUALITY=compressed
```

When enabled, the query path uses IVF/PQ compressed candidate generation with bounded exact reranking. If compressed prerequisites are not satisfied for a run, the service logs diagnostics and falls back safely.

High-level tradeoff (latest benchmark snapshot, averaged):

```text
Dataset size ->      1500      3000      6000      12000
HNSW latency      ~5.81us   ~5.75us   ~5.83us   ~5.64us
IVFPQ latency    ~23.0us   ~42.6us   ~38.9us   ~48.7us
```

```text
Dataset size ->        1500      3000      6000      12000
HNSW heap delta      ~1.56MiB  ~1.57MiB  ~1.57MiB  ~1.58MiB
IVFPQ heap delta     ~1.57MiB  ~2.08MiB  ~2.08MiB  ~2.08MiB
```

Use this profile when memory-scaled ANN operation is more important than lowest-latency single-query performance. For full knobs and operational tuning, see `docs/operations/configuration.md` ("Compressed ANN mode (IVFPQ)").

### Dimensions

| Dimensions | Speed    | Quality | Model Examples    |
| ---------- | -------- | ------- | ----------------- |
| 384        | Fast     | Good    | all-MiniLM-L6-v2  |
| 768        | Balanced | Better  | e5-base           |
| 1024       | Slower   | Best    | mxbai-embed-large |
| 3072       | Slowest  | Highest | OpenAI ada-002    |

### Similarity Thresholds

```go
db.Search(ctx, query, 10, 0.9) // Very similar only
db.Search(ctx, query, 10, 0.7) // Moderately similar
db.Search(ctx, query, 10, 0.0) // All results
```

### Tips

1. **Use caching** - 450,000x speedup for repeated queries
2. **Enable GPU** - 10-100x speedup for search
3. **Set thresholds** - Eliminate weak matches early
4. **Batch operations** - 2-5x faster than sequential

---

## Common Patterns

### RAG (Retrieval-Augmented Generation)

```go
// 1. Search for context
results, _ := db.Search(ctx, userQuery, 5)

// 2. Build context
context := ""
for _, r := range results {
    context += r.Content + "\n"
}

// 3. Generate with context
response := llm.Generate(userQuery, context)
```

### Semantic K-Means Clustering

```go
results, _ := db.Search(ctx, seed, 100)
clusters := groupBySimilarity(results, 0.8)
```

---

## Configuration

### Environment Variables

```bash
NORNICDB_EMBEDDING_ENABLED=true
NORNICDB_EMBEDDING_API_URL=http://localhost:8080
NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
NORNICDB_EMBEDDING_DIMENSIONS=1024
NORNICDB_EMBEDDING_CACHE_SIZE=10000
NORNICDB_KMEANS_MIN_EMBEDDINGS=1000  # Minimum embeddings before K-means clustering
```

**K-Means cluster count (auto by default):**

- The number of clusters is chosen from the dataset size when clustering runs: **K ≈ √(n/2)** (min 10, max 8192). For ~900k embeddings this yields ~670 clusters (~1350 vectors per cluster) instead of a fixed 100.
- Override with `NORNICDB_KMEANS_NUM_CLUSTERS=500` (or any positive value) to use a fixed K.

**K-Means Clustering Threshold:**

- `NORNICDB_KMEANS_MIN_EMBEDDINGS` (default: 1000): Minimum number of embeddings required before K-means clustering is triggered. Below this threshold, brute-force search is used as it's faster for small datasets.

  **Performance Scaling (Benchmarked):**
  - 2,000 embeddings: 14% faster (61ms → 65ms avg)
  - 4,500 embeddings: 26% faster (35ms → 47ms avg)
  - 10,000+ embeddings: 10-50x faster

  **Tuning:**
  - 1000 (default): Safe for most workloads, proven benefit
  - 500-1000: Latency-sensitive applications (14-26% speedup)
  - 100-500: Testing or small datasets
  - 2000+: Very large datasets, maximize speedup

### Verify Status

```bash
curl http://localhost:8080/health
# Check "embedding" section
```

---

## NornicDB vs Neo4j

| Feature                    | Neo4j GDS | NornicDB |
| -------------------------- | --------- | -------- |
| Vector array queries       | ✅        | ✅       |
| String auto-embedding      | ❌        | ✅       |
| Multi-line SET with arrays | ❌        | ✅       |
| Managed embedding storage  | ❌        | ✅       |
| Server-side embedding      | ❌        | ✅       |
| GPU acceleration           | ❌        | ✅       |
| Embedding cache            | ❌        | ✅       |

---

## Troubleshooting

| Issue              | Solution                                         |
| ------------------ | ------------------------------------------------ |
| Slow search        | Enable GPU, use caching, reduce dimensions       |
| Poor results       | Increase dimensions, lower threshold, use hybrid |
| Out of memory      | Reduce batch size, enable GPU (uses VRAM)        |
| No embedder error  | Configure embedding service or use vector arrays |
| Dimension mismatch | Ensure all embeddings use same model             |

---

## Related Docs

- [GPU K-Means](../advanced/k-means-clustering/gpu-implementation.md) - GPU clustering
- [Functions Index](../api-reference/cypher-functions/) - Vector similarity functions
- [Search Implementation](../performance/searching.md) - Hybrid search internals

---

_Last updated: December 1, 2025_
