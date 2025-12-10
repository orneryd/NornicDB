# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
### 🎉 First Stable Release

NornicDB v1.0.0 marks the first production-ready release of the cognitive graph database.

## [1.0.0] - 2025-12-06

### Added

- **llama.cpp b7285 Update** - Major update to local embedding engine (BREAKING)
  - Updated from b4535 to b7285 (2,750 commits)
  - Fixes GGUF model compatibility (`vector::_M_range_check` errors)
  - New `llama_memory_*` API (renamed from `llama_kv_cache_*`)
  - Explicit attention types: `NON_CAUSAL` for embeddings, `CAUSAL` for generation
  - Flash attention enum (`LLAMA_FLASH_ATTN_TYPE_AUTO`)
  - Docker images now use `:7285` tag for CUDA libraries
  - Requires rebuilding all platform libraries

- **Auto-TLP (Automatic Topological Link prediction)** - Automatic relationship inference
  - Embedding similarity-based edge creation
  - Co-access pattern detection
  - Temporal proximity association
  - Transitive inference (A→B + B→C suggests A→C)
  - Feature flag: `NORNICDB_AUTO_TLP_ENABLED`

- **Heimdall QC for Auto-TLP** (Experimental) - LLM quality control for inferred edges
  - Batch review of TLP suggestions before edge creation
  - Optional augmentation mode (Heimdall can suggest additional edges)
  - Stateless one-shot calls sharing KV cache with Bifrost
  - Fail-open behavior (approve on error, log, continue)
  - Feature flags: `NORNICDB_AUTO_TLP_LLM_QC_ENABLED`, `NORNICDB_AUTO_TLP_LLM_AUGMENT_ENABLED`

- **Edge Decay System** - Automatic cleanup of unused auto-generated edges
  - Configurable decay rates and grace periods
  - Reinforcement on re-access
  - Background decay process
  - Feature flag: `NORNICDB_EDGE_DECAY_ENABLED` (enabled by default)

- **K-Means GPU Clustering** - GPU-accelerated clustering for embeddings
  - Metal shader implementation for Apple Silicon
  - Atomic operations for cluster assignment
  - Integration with semantic search
  - Feature flag: `NORNICDB_GPU_CLUSTERING_ENABLED`

- **Find Similar Inline Expansion** - UI feature for semantic search
  - Click "Find Similar" to discover related nodes
  - Real-time embedding count display
  - Integrated with vector search

- **Thread-Safe Implicit Transactions** - Concurrency fixes
  - Proper locking for implicit transactions
  - Unique constraint enforcement during concurrent writes
  - No performance impact on normal operations

### Changed

- **BM25 Full-Text Indexing** - Now indexes complete node representation
  - All properties and fields included in BM25 index
  - Better search coverage for hybrid queries

### Fixed

- WAL log size compaction and auto-compaction on startup
- Orphaned embeddings in GPU memory
- Race condition in embedding regeneration
- Query parsing for complex Cypher patterns
- Windows build configurations

### Documentation

- Auto-TLP feature documentation
- Heimdall QC proposal (RFC)
- Feature flags reference updated
- Migration notice for gob encoding change

---
### Added

- **Heimdall AI Assistant** - Built-in SLM for natural language database interaction
  - Bifrost chat interface in the admin UI
  - Plugin architecture for extending AI capabilities
  - Action system for executing database operations via natural language
  - BYOM (Bring Your Own Model) support for custom GGUF models
  - **Optional Lifecycle Hooks** for plugins:
    - `PrePromptHook` - Modify prompts before SLM processing
    - `PreExecuteHook` - Validate/modify action parameters before execution
    - `PostExecuteHook` - Post-execution logging and state updates
    - `DatabaseEventHook` - React to database operations (CRUD, queries, etc.)
  - **Autonomous Action Invocation** - Plugins can trigger SLM actions based on events
    - `HeimdallInvoker` interface for direct action invocation
    - Event accumulation patterns for intelligent triggers
    - Async fire-and-forget action execution
  - **Inline Notification System** - Proper ordering of plugin notifications with chat content
  - **Request Cancellation** - Lifecycle hooks can cancel requests with reasons
- **Security Validation Package** - Comprehensive HTTP security protection
  - CSRF token validation with injection attack prevention
  - SSRF protection blocking private IPs and cloud metadata services
  - Header injection prevention (CRLF, null bytes)
  - Protocol smuggling protection (file://, gopher://, ftp://)
  - Environment-aware middleware (dev vs production modes)
  - 19 unit tests covering 30+ attack scenarios
  - [HTTP Security Guide](security/http-security.md)
- **WAL Compaction & Auto-Snapshots** - Automatic disk space management
  - `TruncateAfterSnapshot()` - Remove WAL entries before a snapshot point
  - `EnableAutoCompaction()` - Background snapshot + truncation every 5 minutes
  - 99%+ disk savings vs unbounded WAL growth
  - 300x faster crash recovery with recent snapshots
- **Low Memory Mode** - Run NornicDB in resource-constrained environments
  - `NORNICDB_LOW_MEMORY=true` environment variable
  - `--low-memory` CLI flag
  - Reduces BadgerDB RAM from ~1GB to ~50MB (50-70% reduction)
  - Essential for Docker containers with default 2GB limits
  - [Low Memory Mode Guide](operations/low-memory-mode.md)
- **Storage & WAL Durability Improvements** - Critical fixes for data integrity
  - **Proper CRC32 checksums** - Replaced weak XOR-based checksum with CRC32-C (hardware-accelerated)
  - **Atomic WAL writes** - Length-prefixed binary format detects partial writes on crash recovery
  - **Directory fsync** - Proper durability for file creation and rename operations
  - **Batch sequence ordering** - Sequence numbers assigned at commit time (not append time)
  - **AsyncEngine flush error handling** - Failed writes stay in cache for retry (prevents data loss)
  - **Close error reporting** - Reports unflushed data and flush failures on close
  - **WAL recovery error tracking** - Detailed `ReplayResult` with applied/skipped/failed counts
- **Redo/Undo Transaction Logging** - ACID compliance for crash recovery
  - Transaction boundary markers (`OpTxBegin`, `OpTxCommit`, `OpTxAbort`)
  - Before-image storage for UPDATE and DELETE operations
  - `UndoWALEntry()` - Reverses operations using stored before-images
  - `RecoverWithTransactions()` - Rolls back incomplete transactions on recovery
  - Full ACID guarantees for multi-operation transactions
- **Configurable Durability Settings** - Balance data safety vs performance
  - `NORNICDB_STRICT_DURABILITY=true` for maximum safety (financial data)
  - `NORNICDB_WAL_SYNC_MODE=batch|immediate|none` for fine-grained control
  - `NORNICDB_WAL_SYNC_INTERVAL=100ms` for batch sync interval
  - Defaults optimized for performance, opt-in to stricter settings
  - [Durability Configuration Guide](operations/durability.md)
- **Comprehensive Documentation** - 40+ guides covering all features
- **Graph Traversal Guide** - Path queries and pattern matching
- **Data Import/Export Guide** - Neo4j migration and backup procedures

### Features

- Neo4j-compatible Bolt protocol and Cypher queries
- GPU-accelerated vector search (Metal/CUDA/OpenCL)
- Hybrid search with RRF fusion (vector + BM25)
- **Full ACID transactions** with redo/undo logging and crash recovery
- **WAL compaction with automatic snapshots** (prevents unbounded growth)
- **Atomic WAL writes** with CRC32-C checksums for data integrity
- Memory decay system for AI agent memory management
- 62 Cypher functions with full documentation
- Plugin system with APOC compatibility (983 functions)
- Clustering support (Hot Standby, Raft, Multi-Region)
- **HTTP security middleware** (CSRF/SSRF/XSS protection on all endpoints)
- GDPR/HIPAA/SOC2 compliance features

### Docker Images

#### ARM64 (Apple Silicon)

| Image                                          | Description                                     |
| ---------------------------------------------- | ----------------------------------------------- |
| `timothyswt/nornicdb-arm64-metal-bge-heimdall` | **Full** - Database + Embeddings + AI Assistant |
| `timothyswt/nornicdb-arm64-metal-bge`          | **Standard** - Database + BGE-M3 Embeddings     |
| `timothyswt/nornicdb-arm64-metal`              | **Minimal** - Core database with UI             |
| `timothyswt/nornicdb-arm64-metal-headless`     | **Headless** - API only, no UI                  |

#### AMD64 (Linux/Intel)

| Image                                     | Description                          |
| ----------------------------------------- | ------------------------------------ |
| `timothyswt/nornicdb-amd64-cuda`          | **GPU** - CUDA acceleration          |
| `timothyswt/nornicdb-amd64-cuda-bge`      | **GPU + Embeddings** - CUDA + BGE-M3 |
| `timothyswt/nornicdb-amd64-cuda-headless` | **GPU Headless** - CUDA, API only    |
| `timothyswt/nornicdb-amd64-cpu`           | **CPU** - No GPU required            |
| `timothyswt/nornicdb-amd64-cpu-headless`  | **CPU Headless** - API only          |

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

- **1.0.1** (2025-12-05) - Auto-TLP, Heimdall QC, Edge Decay, GPU K-Means
- **1.0.0** (2025-12-03) - 🎉 First stable release with Heimdall AI
- **0.1.4** (2025-12-01) - Documentation reorganization
- **0.1.3** (2025-11-25) - Complete API documentation
- **0.1.2** (2025-11-20) - GPU acceleration and ML features
- **0.1.1** (2025-11-15) - Hybrid search and indexing
- **0.1.0** (2025-11-01) - Initial build

## Links

- [GitHub Repository](https://github.com/orneryd/nornicdb)
- [Documentation](https://github.com/orneryd/nornicdb/tree/main/docs)
- [Docker Hub](https://hub.docker.com/r/timothyswt/nornicdb-arm64-metal)
- [Issue Tracker](https://github.com/orneryd/nornicdb/issues)
