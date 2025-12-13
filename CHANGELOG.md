# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [1.0.6] - 2025-12-12

### Added
- Timer-based K-means clustering scheduler: runs immediately on startup and then periodically (configurable interval).
- New configuration: `NORNICDB_KMEANS_CLUSTER_INTERVAL` (duration) to control the clustering interval. Default: `15m`.

### Changed
- Switched K-means clustering from an embed-queue trigger to a timer-based scheduler that skips runs when the embedding count has not changed since the last clustering.
- DB lifecycle now starts the clustering ticker on open and stops it cleanly on close.

### Fixed
- Prevent excessive K-means executions (previously fired after each embedding) that caused UI slowness and frequent re-clustering.
- Cypher `YIELD`/`RETURN` compatibility: allow `RETURN` after `YIELD` for property access (for example `CALL ... YIELD node RETURN node.id`) and ensure `ORDER BY`, `SKIP`, and `LIMIT` are correctly parsed and applied even when `RETURN` is absent.
- Improved `YIELD` parsing and `applyYieldFilter` semantics (whitespace normalization, `YIELD *` support, return projection correctness).

### Tests
- Added `pkg/cypher/yield_return_test.go` with comprehensive tests for `YIELD`/`RETURN` combinations, `ORDER BY`, `SKIP`, `LIMIT`, and `YIELD *` behaviour.
- Updated `pkg/cypher/neo4j_compat_test.go` for Neo4j compatibility cases.

### Technical Details
- Key files modified: `pkg/nornicdb/db.go`, `pkg/config/config.go`, `pkg/cypher/call.go`, `pkg/cypher/yield_return_test.go`.
- The scheduler uses `searchService.EmbeddingCount()` and a `lastClusteredEmbedCount` guard to avoid unnecessary clustering runs.

### Test Coverage
- All package tests pass; example test output excerpts:


## [1.0.5] - 2025-12-10

### Fixed
- **Critical: Node/Edge Count Returns 0 After Delete+Recreate Cycles** - `MATCH (n) RETURN count(n)` returned 0 even when nodes existed in the database
  - Atomic counters (`nodeCount`, `edgeCount`) in `BadgerEngine` became out of sync during delete+recreate cycles
  - Root cause: Nodes created via implicit transactions (MERGE, CREATE) bypass `CreateNode()` and use `UpdateNode()`
  - `UpdateNode()` checks if key exists to determine `wasInsert=true/false`, only incrementing counter when `wasInsert=true`
  - During delete+recreate, keys could still exist in BadgerDB from previous sessions, causing `wasInsert=false` for genuinely new nodes
  - The counter would increment for only 1 node, leaving `nodeCount=1` even with 234 nodes in the database
  - **Production symptom**: After deleting all nodes and re-importing 234 nodes via MERGE, `/metrics` showed `nornicdb_nodes_total 0` while `MATCH (n:Label)` returned all 234 nodes correctly
  - **Solution**: Changed `BadgerEngine.NodeCount()` and `EdgeCount()` to scan actual keys with prefix iteration instead of trusting atomic counter
  - Key-only iteration (no value loading) provides fast O(n) counting with guaranteed correctness
  - Atomic counter is updated after each scan to keep it synchronized for future calls
  - **Impact**: Count queries now always reflect reality. Embeddings worked because they scan actual data; node counts failed because they used broken counter.

- **Critical: Aggregation Query Caching Caused Stale Counts** - `MATCH (n) RETURN count(n)` was being cached, returning stale results after node creation/deletion
  - `SmartQueryCache` was caching aggregation queries (COUNT, SUM, AVG, etc.) which must always be fresh
  - Modified `pkg/cypher/executor.go` to detect aggregation via `HasAggregation` flag and skip caching entirely
  - Modified `pkg/cypher/query_info.go` to analyze queries and set `HasAggregation=true` for COUNT/SUM/AVG/MIN/MAX/COLLECT
  - Modified `pkg/cypher/cache.go` to invalidate queries with no labels when any node is created/deleted (affects `MATCH (n)` count queries)
  - **Impact**: Count queries always return fresh data, no manual cache clear needed

### Changed
- **Performance Trade-off: NodeCount() and EdgeCount() Now O(n)** - Changed from O(1) atomic counter to O(n) key scan for correctness
  - Key-only iteration is very fast (no value decoding, just prefix scan)
  - BadgerDB iterators are highly optimized for this access pattern
  - Correctness > speed for core count operations
  - Future optimization: Maintain accurate counter through all write paths

### Technical Details
- Modified `pkg/storage/badger.go`:
  - `NodeCount()` now uses `BadgerDB.View()` with key-only iterator (`PrefetchValues=false`)
  - `EdgeCount()` uses same pattern for edge prefix scan
  - Both methods sync atomic counter after scan to reduce overhead on subsequent calls
- Modified `pkg/storage/async_engine.go`:
  - Fixed `NodeCount()` calculation to include `inFlightCreates` to prevent race condition during flush
  - Prevented double-counting during flush window when nodes transition from cache to engine
- Modified `pkg/cypher/executor.go`:
  - Added aggregation detection to skip caching for COUNT/SUM/AVG/MIN/MAX queries
- Modified `pkg/cypher/query_info.go`:
  - Added `HasAggregation` field to `QueryInfo` struct
  - Updated `analyzeQuery()` to detect aggregation functions
- Modified `pkg/cypher/cache.go`:
  - Fixed `InvalidateLabels()` to also invalidate queries with no labels (e.g., `MATCH (n)`)

### Test Coverage
- All existing tests pass with updated expectations
- Modified `pkg/storage/realtime_count_test.go` to account for transient over-counting before flush
- Modified `pkg/cypher/executor_cache_test.go` to use non-aggregation queries (since aggregations aren't cached)
- Added comprehensive logging and debugging to trace count calculation flow
- Production issue validated as fixed: 234 nodes now counted correctly after delete+reimport

## [1.0.4] - 2025-12-10

### Fixed
- **Critical: Node/Edge Count Tracking During DETACH DELETE** - Edge counts became incorrect (negative, double-counted, or stale) during `DETACH DELETE` operations
  - `deleteEdgesWithPrefix()` was deleting edges but not returning count of edges actually deleted
  - `deleteNodeInTxn()` wasn't tracking edges deleted along with the node
  - `BulkDeleteNodes()` only decremented node count, not edge count for cascade-deleted edges
  - Unit tests showed counts going negative or remaining high after deletes, resetting to zero only on restart
  - Fixed by updating `deleteEdgesWithPrefix()` signature to return `(int64, []EdgeID, error)`
  - Fixed `deleteNodeInTxn()` to aggregate and return edges deleted with node
  - Fixed `BulkDeleteNodes()` to correctly decrement `edgeCount` and notify `edgeDeleted` callbacks
  - Added comprehensive tests in `pkg/storage/async_engine_delete_stats_test.go`
  - **Impact**: `/admin/stats` and Cypher `count()` queries now remain accurate during bulk delete operations

- **Critical: ORDER BY Ignored for Relationship Patterns** - `ORDER BY`, `SKIP`, and `LIMIT` clauses were completely ignored for queries with relationship patterns
  - Queries like `MATCH (p:Person)-[:WORKS_IN]->(a:Area) RETURN p.name ORDER BY p.name` returned unordered results
  - `executeMatchWithRelationships()` was returning immediately without applying post-processing clauses
  - Fixed by capturing result, applying ORDER BY/SKIP/LIMIT, then returning
  - Affects all queries with relationship traversal: `(a)-[:TYPE]->(b)`, `(a)<-[:TYPE]-(b)`, chained patterns
  - **Impact**: Fixes data integrity issues where clients relied on sorted results

- **Critical: Cartesian Product MATCH Returns Zero Rows** - Comma-separated node patterns returned empty results instead of cartesian product
  - `MATCH (p:Person), (a:Area) RETURN p.name, a.code` returned 0 rows (should return N×M combinations)
  - `executeMatch()` only parsed first pattern, ignoring subsequent comma-separated patterns
  - Fixed by detecting multiple patterns via `splitNodePatterns()` and routing to new `executeCartesianProductMatch()`
  - Now correctly generates all combinations of matched nodes
  - Supports WHERE filtering, aggregation, ORDER BY, SKIP, LIMIT on cartesian results
  - **Impact**: Critical for Northwind-style bulk insert patterns like `MATCH (s), (c) CREATE (p)-[:REL]->(c)`

- **Critical: Cartesian Product CREATE Only Creates One Relationship** - `MATCH` with multiple patterns followed by `CREATE` only created relationships for first match
  - `MATCH (p:Person), (a:Area) CREATE (p)-[:WORKS_IN]->(a)` created 1 relationship (should create 3 for 3 persons × 1 area)
  - `executeMatchCreateBlock()` was collecting only first matching node per pattern variable
  - Fixed by collecting ALL matching nodes and iterating through cartesian product combinations
  - Each CREATE now executes once per combination in the cartesian product
  - **Impact**: Fixes bulk relationship creation patterns used in data import workflows

- **UNWIND CREATE with RETURN Returns Variable Name Instead of Values** - Return clause after `UNWIND...CREATE` returned literal variable names
  - `UNWIND ['A','B','C'] AS name CREATE (n {name: name}) RETURN n.name` returned `["name","name","name"]` (should be `["A","B","C"]`)
  - `replaceVariableInQuery()` failed to handle variables inside curly braces like `{name: name}`
  - String splitting on spaces left `name}` which didn't match variable `name`
  - Fixed by properly trimming braces `{}[]()` and preserving surrounding punctuation during replacement
  - **Impact**: Fixes all UNWIND+CREATE+RETURN workflows, critical for bulk data ingestion with result tracking

### Changed
- **Cartesian Product Performance** - New `executeCartesianProductMatch()` efficiently handles multi-pattern queries
  - Builds combinations incrementally to avoid memory explosion on large datasets
  - Supports early filtering with WHERE clause before building full product
  - Properly integrates with query optimizer (ORDER BY, SKIP, LIMIT applied after filtering)

### Technical Details
- Modified `pkg/storage/badger.go`:
  - Fixed `deleteEdgesWithPrefix()` to return accurate count and edge IDs
  - Fixed `deleteNodeInTxn()` to track and return edges deleted with node
  - Fixed `BulkDeleteNodes()` to correctly decrement edge count for cascade deletes
- Modified `pkg/cypher/match.go`:
  - Added `executeCartesianProductMatch()` for comma-separated pattern handling
  - Added `executeCartesianAggregation()` for aggregation over cartesian results
  - Added `evaluateWhereForContext()` for WHERE clause evaluation on node contexts
  - Fixed `executeMatch()` to detect and route multiple patterns correctly
  - Fixed relationship pattern path to apply ORDER BY/SKIP/LIMIT before returning
- Modified `pkg/cypher/create.go`:
  - Updated `executeMatchCreateBlock()` to collect all pattern matches (not just first)
  - Added cartesian product iteration for CREATE execution
  - Now creates relationships for every combination in MATCH cartesian product
- Modified `pkg/cypher/clauses.go`:
  - Fixed `replaceVariableInQuery()` to handle variables in property maps `{key: value}`
  - Improved punctuation preservation during variable substitution

### Test Coverage
- All existing tests pass (100% backwards compatibility)
- Added `pkg/storage/async_engine_delete_stats_test.go` with comprehensive count tracking tests
- Fixed `TestWorksInRelationshipTypeAlternation` - ORDER BY now works correctly
- Fixed `TestUnwindWithCreate/UNWIND_CREATE_with_RETURN` - Returns actual values, not variable names
- Cartesian product patterns now pass all Northwind benchmark compatibility tests

## [1.0.3] - 2025-12-09

### Fixed
- **Critical: Double .gguf Extension Bug** - Model names with `.gguf` extension (e.g., `bge-m3.gguf`) were having `.gguf` appended again, resulting in `bge-m3.gguf.gguf` and "model not found" errors
  - Fixed in `pkg/heimdall/scheduler.go` - Now checks `strings.HasSuffix()` before adding extension
  - Fixed in `pkg/embed/local_gguf.go` - Same fix for embedding model resolution
  - This prevented both Heimdall AI assistant and auto-embeddings from working on macOS
- **Missing LaunchAgent Environment Variables** - macOS menu bar app's LaunchAgent plist was missing critical env vars
  - Added `NORNICDB_MODELS_DIR=/usr/local/var/nornicdb/models`
  - Added `NORNICDB_HEIMDALL_MODEL` to pass model name to Heimdall
  - Added `NORNICDB_EMBEDDING_MODEL` to pass model name to embeddings
  - Updated both plist generators in `macos/MenuBarApp/NornicDBMenuBar.swift`
- **macOS Models Path Resolution** - Added `/usr/local/var/nornicdb/models` as first candidate in Heimdall's model path resolution (was only checking Docker paths)
- **Swift YAML Config Indentation** - Fixed multi-line string indentation errors in plist generation

### Changed
- **Non-blocking Regenerate Embeddings** - `POST /nornicdb/embed/trigger?regenerate=true` now returns `202 Accepted` immediately
  - Clearing and regeneration happens asynchronously in background goroutine
  - Prevents UI from blocking for minutes during large regenerations
  - Added detailed logging for background operations
- **UI Confirmation Dialog** - Added confirmation modal before regenerating all embeddings
  - Shows warning about destructive operation
  - Displays current embedding count
  - Red warning styling to indicate danger

### Added
- **Swift YAML Parser Unit Test** - Created `macos/MenuBarApp/ConfigParserTest.swift` to verify config loading works correctly
  - Tests section extraction, boolean parsing, string parsing
  - Validates against actual `~/.nornicdb/config.yaml` file

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
 - `v1.0.6` - 2025-12-12

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

[Unreleased]: https://github.com/orneryd/NornicDB/compare/v1.0.6...HEAD
[1.0.6]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.6
[1.0.1]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.1
[1.0.0]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.0
