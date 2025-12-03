# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-12-03

### 🎉 First Stable Release

NornicDB v1.0.0 marks the first production-ready release of the cognitive graph database.

### Added
- **Heimdall AI Assistant** - Built-in SLM for natural language database interaction
  - Bifrost chat interface in the admin UI
  - Plugin architecture for extending AI capabilities
  - Action system for executing database operations via natural language
  - BYOM (Bring Your Own Model) support for custom GGUF models
- **Comprehensive Documentation** - 40+ guides covering all features
- **Graph Traversal Guide** - Path queries and pattern matching
- **Data Import/Export Guide** - Neo4j migration and backup procedures

### Features
- Neo4j-compatible Bolt protocol and Cypher queries
- GPU-accelerated vector search (Metal/CUDA/OpenCL)
- Hybrid search with RRF fusion (vector + BM25)
- ACID transactions with WAL
- Memory decay system for AI agent memory management
- 62 Cypher functions with full documentation
- Plugin system with APOC compatibility (983 functions)
- Clustering support (Hot Standby, Raft, Multi-Region)
- GDPR/HIPAA/SOC2 compliance features

### Docker Images

#### ARM64 (Apple Silicon)

| Image | Description |
|-------|-------------|
| `timothyswt/nornicdb-arm64-metal-bge-heimdall` | **Full** - Database + Embeddings + AI Assistant |
| `timothyswt/nornicdb-arm64-metal-bge` | **Standard** - Database + BGE-M3 Embeddings |
| `timothyswt/nornicdb-arm64-metal` | **Minimal** - Core database with UI |
| `timothyswt/nornicdb-arm64-metal-headless` | **Headless** - API only, no UI |

#### AMD64 (Linux/Intel)

| Image | Description |
|-------|-------------|
| `timothyswt/nornicdb-amd64-cuda` | **GPU** - CUDA acceleration |
| `timothyswt/nornicdb-amd64-cuda-bge` | **GPU + Embeddings** - CUDA + BGE-M3 |
| `timothyswt/nornicdb-amd64-cuda-headless` | **GPU Headless** - CUDA, API only |
| `timothyswt/nornicdb-amd64-cpu` | **CPU** - No GPU required |
| `timothyswt/nornicdb-amd64-cpu-headless` | **CPU Headless** - API only |

---

## [0.1.4] - 2025-12-01

### Added
- Comprehensive documentation reorganization with 12 logical categories
- Complete user guides for Cypher queries, vector search, and transactions
- Getting started guides with Docker deployment
- API reference documentation for all 52 Cypher functions
- Feature guides for GPU acceleration, memory decay, and link prediction
- Architecture documentation for system design and plugin system
- Performance benchmarks and optimization guides
- Advanced topics: clustering, embeddings, custom functions
- Compliance guides for GDPR, HIPAA, and SOC2
- AI agent integration guides for Cursor and MCP tools
- Neo4j migration guide with 96% feature parity
- Operations guides for deployment, monitoring, and scaling
- Development guides for contributors

### Changed
- Documentation structure reorganized from flat hierarchy to logical categories
- File naming standardized to kebab-case
- All cross-references updated to new locations
- README files created for all directories

### Documentation
- 350+ functions documented with examples
- 13,400+ lines of GoDoc comments
- 40+ ELI12 explanations for complex concepts
- 4.1:1 documentation-to-code ratio

## [0.1.3] - 2025-11-25

### Added
- Complete Cypher function documentation (52 functions)
- Pool package documentation with memory management examples
- Cache package documentation with LRU and TTL examples
- Real-world examples for all public functions

### Improved
- Code documentation coverage to 100% for public APIs
- ELI12 explanations for complex algorithms
- Performance characteristics documented

## [0.1.2] - 2025-11-20

### Added
- GPU acceleration for vector search (Metal, CUDA, OpenCL)
- Automatic embedding generation with Ollama integration
- Memory decay system for time-based importance
- Link prediction with ML-based relationship inference
- Cross-encoder reranking for improved search accuracy

### Performance
- 10-100x speedup for vector operations with GPU
- Sub-millisecond queries on 1M vectors with HNSW index
- Query caching with LRU eviction

## [0.1.1] - 2025-11-15

### Added
- Hybrid search with Reciprocal Rank Fusion (RRF)
- Full-text search with BM25 scoring
- HNSW vector index for O(log N) performance
- Eval harness for search quality validation

### Fixed
- Memory leaks in query execution
- Race conditions in concurrent transactions
- Index corruption on crash recovery

## [0.1.0] - 2025-11-01

### Added
- Initial release of NornicDB
- Neo4j Bolt protocol compatibility
- Cypher query language support (96% Neo4j parity)
- ACID transactions
- Property graph model
- Badger storage engine
- In-memory engine for testing
- JWT authentication with RBAC
- Field-level encryption (AES-256-GCM)
- Audit logging for compliance
- Docker images for ARM64 and x86_64

### Features
- Vector similarity search with cosine similarity
- Automatic relationship inference
- GDPR, HIPAA, SOC2 compliance features
- REST HTTP API
- Prometheus metrics

## [Unreleased]

### Planned
- Horizontal scaling with read replicas
- Distributed transactions
- Graph algorithms (PageRank, community detection)
- Time-travel queries
- Multi-tenancy support
- GraphQL API
- WebSocket support for real-time updates

---

## Version History

- **1.0.0** (2025-12-03) - 🎉 First stable release with Heimdall AI
- **0.1.4** (2025-12-01) - Documentation reorganization
- **0.1.3** (2025-11-25) - Complete API documentation
- **0.1.2** (2025-11-20) - GPU acceleration and ML features
- **0.1.1** (2025-11-15) - Hybrid search and indexing
- **0.1.0** (2025-11-01) - Initial release

## Links

- [GitHub Repository](https://github.com/orneryd/nornicdb)
- [Documentation](https://github.com/orneryd/nornicdb/tree/main/docs)
- [Docker Hub](https://hub.docker.com/r/timothyswt/nornicdb-arm64-metal)
- [Issue Tracker](https://github.com/orneryd/nornicdb/issues)
