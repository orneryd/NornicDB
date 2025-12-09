# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions workflows for CI/CD
- Automated Docker image building and publishing
- Binary releases for multiple platforms
- Issue and PR templates
- Migration guide for repository split

## [1.0.3] - 2025-12-09

### Added
- **Streaming Query Optimization**: `MATCH (n) RETURN n LIMIT X` queries now use streaming with early termination instead of loading all nodes into memory
  - `StreamingEngine` interface implemented in `AsyncEngine` and `WALEngine`
  - Full storage chain support: AsyncEngine → WALEngine → BadgerEngine
  - 100x+ faster LIMIT queries on large datasets (40K+ nodes)
- **O(1) Stats Lookups**: `NodeCount()` and `EdgeCount()` now return cached atomic counters
  - Eliminates O(N) full table scans for every stats/status API call
  - `COUNT(n)` fast-path optimization for simple node count queries
- **Storage Event System**: New `StorageEventNotifier` interface with 6 event callbacks
  - `OnNodeCreated`, `OnNodeUpdated`, `OnNodeDeleted`
  - `OnEdgeCreated`, `OnEdgeUpdated`, `OnEdgeDeleted`
  - Search indexes automatically synchronized via event subscriptions
  - Events fire from BadgerEngine (single source of truth)
- Comprehensive streaming unit tests for all storage engine layers

### Fixed
- **UpdateNode Upsert Count**: Fixed `UpdateNode` not incrementing node count when inserting a new node (upsert behavior)
- **WAL Test Race Condition**: Fixed flaky `auto_compaction_recoverable` test that could pick up `.tmp` snapshot files
- **SKIP+LIMIT Streaming**: Fixed streaming optimization to account for both SKIP and LIMIT values

### Changed
- Node deletion callbacks now fire from storage layer (BadgerEngine) instead of being handled separately by AsyncEngine
- Removed duplicate event handling code from AsyncEngine wrapper

## [1.0.2] - 2025-01-27

### Added
- **macOS Code Intelligence / File Indexer**: New file indexing system in the macOS menu bar app that provides semantic code search capabilities.
  - Automatically indexes source files with intelligent chunking (functions, classes, methods extracted separately)
  - **Apple Vision Integration**: PNG/image files are processed with Apple's Vision framework for:
    - OCR text extraction (reads text from screenshots, diagrams, etc.)
    - Image classification (identifies objects, scenes, activities in images)
  - Creates searchable `File` and `FileChunk` nodes linked via `HAS_CHUNK` relationships
  - Real-time file watching with automatic re-indexing on changes
  - Supports code files, markdown, images, and more
- **NornicDB Icons**: Added proper application icons for macOS app

### Changed
- **Keychain-based API Token Storage**: API tokens (Ollama, OpenAI, etc.) are now stored securely in macOS Keychain instead of plain YAML config files
- Improved default provider value handling

### Fixed
- **In-flight Node Deletion Race Condition**: Fixed a critical bug in `AsyncEngine` where nodes being flushed to disk could survive `DETACH DELETE` operations.
  - When a node was in the middle of being written (in `inFlightNodes`), delete operations would only remove it from cache
  - The flush would then complete, writing the "deleted" node back to BadgerDB
  - Now properly marks in-flight nodes for deletion so they're removed after flush completes
- **Node/Edge Count Consistency**: `NodeCount()` and `EdgeCount()` now validate that nodes can be decoded before counting, ensuring counts match what `AllNodes()` and `AllEdges()` actually return
- CUDA Dockerfile fixes for improved GPU support
- Documentation link fixes

## [1.0.1] - 2025-12-08

### Added
- macOS installer improvements: wizard-first startup, menu bar start/health wait, security tab, auto-generated JWT/encryption secrets, scrollable wizard, starting status indicator.
- Menu bar app: ensures `~/.nornicdb/config.yaml` path, shows restart progress, auto-generates secrets if empty, saves auth/encryption correctly.
- Docker ARM64 (Metal) image now builds and copies Heimdall plugin and sets `NORNICDB_HEIMDALL_PLUGINS_DIR`.
- Legacy env compatibility for Neo4j env vars (auth, transaction timeout, data dir, default db, read-only, bolt/http ports).

### Changed
- Encryption: full-database Badger encryption, salt stored at `db.salt`, rejects missing password, clearer errors on wrong password; stats report AES-256 (BadgerDB).
- Auth/JWT: server uses configured JWT secret (no hardcoded dev secret); cookie SameSite=Lax, 7d Max-Age.
- Config defaults: password `password`, embedding provider `local`; strict durability forces WAL sync immediate/interval 0.
- Tests updated and all passing (`go test ./...`).

### Fixed
- Prevent server autostart before wizard (plist created/loaded only after wizard save/start).
- Heimdall env override test; flexible boolean parsing for read-only; duration parsing for legacy env names.

## [1.0.0] - 2024-12-06

### Changed
- **BREAKING**: Repository split from `github.com/orneryd/Mimir/nornicdb` to `github.com/orneryd/NornicDB`
- **BREAKING**: Module path changed from `github.com/orneryd/mimir/nornicdb` to `github.com/orneryd/nornicdb`
- Preserved full commit history from Mimir repository
- Updated all documentation to reflect standalone repository
- Cleaned up repository structure (removed Mimir-specific files)

### Migration
See [MIGRATION.md](MIGRATION.md) for detailed migration instructions.

---

## Historical Changes (from Mimir Project)

The following changes occurred while NornicDB was part of the Mimir project. Full commit history has been preserved in this repository.

### Features Implemented (Pre-Split)
- Neo4j Bolt protocol compatibility
- Cypher query language support (MATCH, CREATE, MERGE, DELETE, WHERE, WITH, RETURN, etc.)
- BadgerDB storage backend
- In-memory storage engine for testing
- GPU-accelerated embeddings (Metal, CUDA)
- Vector search with semantic similarity
- Full-text search
- Query result caching
- Connection pooling
- Heimdall LLM integration
- Web UI (Bifrost)
- Docker images for multiple platforms
- Comprehensive test suite (90%+ coverage)
- Extensive documentation

### Performance Achievements (Pre-Split)
- 3-52x faster than Neo4j across benchmarks
- 100-500 MB memory footprint vs 1-4 GB for Neo4j
- Sub-second cold start vs 10-30s for Neo4j
- GPU-accelerated embedding generation

### Bug Fixes (Pre-Split)
- Fixed WHERE IS NOT NULL with aggregation
- Fixed relationship direction in MATCH patterns
- Fixed MERGE with ON CREATE/ON MATCH
- Fixed concurrent access issues
- Fixed memory leaks in query execution
- Fixed Bolt protocol edge cases

---

## Version History

### Release Tags
- `v1.0.0` - First standalone release (December 6, 2024)

### Pre-Split Versions
Prior to v1.0.0, NornicDB was versioned as part of the Mimir project. The commit history includes all previous development work.

---

## Migration Notes

### For Users Migrating from Mimir
If you were using NornicDB from the Mimir repository, please see [MIGRATION.md](MIGRATION.md) for detailed instructions on:
- Updating import paths
- Updating git remotes
- Updating Docker images
- Updating CI/CD pipelines

### Compatibility
- **Neo4j Compatibility**: Maintained 100%
- **API Stability**: No breaking changes to public APIs (except import paths)
- **Docker Images**: Same naming convention, new build source
- **Data Format**: Fully compatible with existing data

---

## Contributing

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) for contribution guidelines.

---

[Unreleased]: https://github.com/orneryd/NornicDB/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.1
[1.0.0]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.0
