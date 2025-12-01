# NornicDB Documentation

Welcome to **NornicDB** - A production-ready graph database with GPU acceleration, Neo4j compatibility, and advanced indexing.

## 🚀 Quick Links

- **[Getting Started](guides/GETTING_STARTED.md)** - Set up NornicDB in 5 minutes
- **[Chat Modes Guide](guides/CHATMODES_GUIDE.md)** - Use with AI agents (Cursor, etc.)
- **[MCP Tools Quick Reference](MCP_TOOLS_QUICKREF.md)** - 6-tool cheat sheet
- **[Architecture Overview](ARCHITECTURE.md)** - System design and components
- **[Vector Search Guide](guides/VECTOR_SEARCH.md)** - Semantic search with auto-embedding
- **[Feature Flags](FEATURE_FLAGS.md)** - Runtime configuration

## 📚 Documentation Sections

### For Users

- **[Getting Started](guides/GETTING_STARTED.md)** - Installation and first steps
- **[Vector Search Guide](guides/VECTOR_SEARCH.md)** - Semantic search with string queries
- **[Search Implementation](RRF_SEARCH_IMPLEMENTATION.md)** - How search works
- **[Complete Examples](COMPLETE_EXAMPLES.md)** - Full code examples
- **[Link Prediction](LINK_PREDICTION_INTEGRATION.md)** - ML-based relationship prediction

### For Developers

- **[Architecture](ARCHITECTURE.md)** - System design and components
- **[Neo4j Feature Audit](NEO4J_NORNICDB_FEATURE_AUDIT.md)** - 96% parity comparison
- **[Cypher Audit](CYPHER_AUDIT.md)** - Cypher compatibility
- **[Feature Flags](FEATURE_FLAGS.md)** - Runtime configuration
- **[Transaction Implementation](TRANSACTION_IMPLEMENTATION.md)** - ACID guarantees

### GPU & Performance

- **[GPU K-Means Implementation](GPU_KMEANS_IMPLEMENTATION_PLAN.md)** - GPU clustering plan
- **[K-Means Clustering](K-MEANS.md)** - K-Means for embeddings
- **[Real-Time K-Means](K-MEANS-RT.md)** - Live cluster updates
- **[Metal Atomic Fix](GPU_KMEANS_METAL_ATOMIC_SUMMARY.md)** - Apple Silicon fix

### For AI/Agent Integration

- **[Chat Modes Guide](guides/CHATMODES_GUIDE.md)** - Use NornicDB with Cursor IDE
- **[MCP Tools Quick Reference](MCP_TOOLS_QUICKREF.md)** - 6-tool cheat sheet
- **[Agent Preamble (claudette-mimir)](../../docs/agents/claudette-mimir-v2.md)** - Memory-augmented AI agent

### For DevOps

- **[Quick Start](guides/QUICK_START.md)** - Docker setup and deployment

## 🎯 Key Features

### 🧠 Graph-Powered Memory
- Semantic relationships between data
- Multi-hop graph traversal
- Automatic relationship inference
- Memory decay simulation

### 🚀 GPU Acceleration
- 10-100x speedup for vector search
- Multi-backend support (CUDA, OpenCL, Metal, Vulkan)
- Automatic CPU fallback
- Memory-optimized embeddings

### 🔍 Advanced Search
- Vector similarity search with cosine similarity
- Full-text search with BM25 scoring
- Hybrid search combining both methods
- HNSW indexing for O(log N) performance

### 🔗 Neo4j Compatible
- Bolt protocol support
- Cypher query language
- Standard Neo4j drivers work out-of-the-box
- Easy migration from Neo4j

### 🔐 Enterprise-Ready
- GDPR, HIPAA, SOC2 compliance
- Field-level encryption
- RBAC and audit logging
- ACID transactions

## 📊 Documentation Statistics

- **21 packages** fully documented
- **13,400+ lines** of GoDoc comments
- **350+ functions** with examples
- **40+ ELI12 explanations** for complex concepts
- **4.1:1 documentation-to-code ratio**

## 📋 Project Status

- **[Development Roadmap](ROADMAP_POST_TLP.md)** - Current progress and next steps
- **[Transaction Implementation](TRANSACTION_IMPLEMENTATION.md)** - ACID guarantees
- **[Local GGUF Embeddings](LOCAL_GGUF_EMBEDDING_IMPLEMENTATION.md)** - Local embedding models

## 🤝 Contributing

Found an issue or want to improve documentation? Check out our [Contributing Guide](CONTRIBUTING.md).

## 📄 License

NornicDB is MIT licensed. See [LICENSE](../LICENSE) for details.

---

**Last Updated:** December 1, 2025  
**Version:** 0.1.4  
**Docker:** `timothyswt/nornicdb-arm64-metal:latest`  
**Status:** Production Ready ✅
