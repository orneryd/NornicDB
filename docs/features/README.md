# Features

**Explore NornicDB's advanced features and capabilities.**

## 🚀 Core Features

### Search & Discovery
- **[Vector Search](../user-guides/vector-search.md)** - Semantic search with embeddings
- **[Hybrid Search](../user-guides/hybrid-search.md)** - RRF fusion of vector + BM25
- **[Cross-Encoder Reranking](cross-encoder-reranking.md)** - Two-stage retrieval
- **[Link Prediction](link-prediction.md)** - ML-based relationship prediction

### AI & Machine Learning
- **[MCP Integration](mcp-integration.md)** - Model Context Protocol tools
- **[Memory Decay](memory-decay.md)** - Time-based importance scoring
- **[Vector Embeddings](vector-embeddings.md)** - Automatic embedding generation
- **[GPU Acceleration](gpu-acceleration.md)** - 10-100x speedup
- **[Auto-TLP](auto-tlp.md)** - Automatic relationship inference

### Configuration
- **[Feature Flags](feature-flags.md)** - Runtime configuration
- **[APOC Functions](apoc-functions.md)** - 450+ Neo4j-compatible functions

## 📚 Feature Categories

### Search Features
NornicDB provides three complementary search methods:

1. **Vector Search** - Semantic similarity using embeddings
2. **Full-Text Search** - BM25 keyword matching  
3. **Hybrid Search** - RRF fusion combining both

[Learn more about search →](../user-guides/vector-search.md)

### AI Integration
- Automatic embedding generation
- MCP tool integration
- Memory decay simulation
- Link prediction
- Auto-TLP (automatic relationship inference)

[Learn more about AI features →](mcp-integration.md)

### Performance
- GPU acceleration (Metal, CUDA, OpenCL)
- Query caching
- Index optimization
- Parallel execution

[Learn more about performance →](../performance/)

## 🎯 Popular Features

### Vector Search
Search by meaning, not just keywords. Automatically generates embeddings for semantic search.

```cypher
// Semantic search with string query
CALL nornicdb.search.vector("machine learning algorithms", 10)
YIELD node, score
RETURN node.title, score
```

[Complete guide →](../user-guides/vector-search.md)

### GPU Acceleration
10-100x speedup for vector operations on Apple Silicon, NVIDIA, and AMD GPUs.

```yaml
gpu:
  enabled: true
  backend: metal  # or cuda, opencl
```

[Complete guide →](gpu-acceleration.md)

### Memory Decay
Simulate human memory with time-based importance scoring.

```cypher
// Get decay scores
MATCH (n)
RETURN n.title, n.decayScore
ORDER BY n.decayScore DESC
```

[Complete guide →](memory-decay.md)

## 📖 Feature Guides

- **[APOC Functions](apoc-functions.md)** - 450+ collection, text, math, graph functions
- **[Auto-TLP](auto-tlp.md)** - Automatic relationship inference
- **[Feature Flags](feature-flags.md)** - Runtime configuration
- **[Link Prediction](link-prediction.md)** - Predict missing relationships
- **[MCP Integration](mcp-integration.md)** - AI agent tools
- **[Cross-Encoder Reranking](cross-encoder-reranking.md)** - Improve search accuracy

## ⏭️ Next Steps

- **[User Guides](../user-guides/)** - Learn how to use features
- **[API Reference](../api-reference/)** - Function documentation
- **[Advanced Topics](../advanced/)** - Deep dive into internals

---

**Explore features** → **[Vector Search](../user-guides/vector-search.md)**
