# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Latest Changes]

- See `docs/latest-untagged.md` for the full untagged changelog with rationale and file cites.

### Added

- **Per-database search index master switches and warming triggers.** Four new keys configure BM25 fulltext and vector ANN behavior independently per database:
  - `NORNICDB_SEARCH_BM25_ENABLED` (boolean, default `true`) — master switch for BM25 fulltext search.
  - `NORNICDB_SEARCH_BM25_WARMING` (enum: `startup`|`lazy`, default `startup`) — eager build at boot or deferred until first query.
  - `NORNICDB_SEARCH_VECTOR_ENABLED` (boolean, default `true`) — master switch for every vector search strategy (HNSW, IVF-HNSW, brute-force, GPU, Metal, Qdrant pass-through). When false, node embeddings are NOT iterated into the in-memory ANN substrate — the strongest available memory-pressure lever.
  - `NORNICDB_SEARCH_VECTOR_WARMING` (enum: `startup`|`lazy`, default `startup`).

  Defaults reproduce today's behaviour; existing deployments need no change. Configurable via env, CLI flags (`--search-bm25-enabled`, etc.), `nornicdb.yaml` global `memory:` block, and yaml `databases:` map for per-database overrides. Runtime overrides via `PUT /admin/databases/{name}/config` always win over global defaults in **both directions** (per-DB `true` enables a globally-disabled index; per-DB `false` disables a globally-enabled one). Lazy-warming is a synchronous-wait contract: the first inbound search request from any entry point (HTTP, Bolt, GraphQL, gRPC, Cypher procedures) blocks inside `Service.EnsureWarm` until the build completes; concurrent first-readers all wait on the same `sync.Once`. The build runs in the DB's long-lived context so a request that times out during the wait does NOT abort the build.

  Migration: zero. Documented in [`docs/operations/configuration.md#per-database-search-index-control`](docs/operations/configuration.md), [`docs/operations/low-memory-mode.md`](docs/operations/low-memory-mode.md), [`docs/user-guides/hybrid-search.md`](docs/user-guides/hybrid-search.md), and the openapi spec. See `docs/plans/per-database-search-index-flags-plan.md` for design context.

- **`db.index.vector.queryNodes` returns empty results with WARN log on vector-disabled databases** instead of erroring. Composite Cypher pipelines that gracefully handle empty vector results continue to succeed; operators see the misconfiguration in `subsystem=vector_search` log lines.

- **Qdrant gRPC bridge honors the per-DB vector master switch.** External Qdrant clients querying a database with `NORNICDB_SEARCH_VECTOR_ENABLED=false` see a deterministic structured error rather than a service whose ANN substrate isn't populated.

## [v1.1.1] - 2026-05-19

Patch release focused on multi-tenant MVCC isolation, MERGE concurrency correctness, and a discoverable consumer-facing documentation surface (skills, migration scripts, unified Bolt error shape). No on-disk format changes; existing `v1.1.0` databases upgrade transparently.

### Added

- **`/demo` galaxy route** — interactive 3D force-directed graph (lazy-loaded `3d-force-graph` + `three.js`) that procedurally seeds a `d3_demo` database with a Fibonacci-sphere sector layout, links sectors via gateway hyperlanes, and exposes click-two-stars-to-traverse so the deep multi-hop `shortestPath` path lights up in purple. A purple-themed HUD tracks live `shortestPath` latency only — seed and catalog calls are excluded so the bars reflect query speed, not page load. All seed Cypher pinned to the hot-path cookbook (`UnwindSimpleMergeBatch` for stars, `UnwindMultiMatchCreateBatch` for hyperlanes).
- **Cancellable traversal** — `context.Context` threaded through `executeShortestPathQuery`, `shortestPath`, `allShortestPaths`, `traverseGraph`, `traverseGraphSequential`, `traverseGraphParallel`, `traverseFromNode`, and `findPaths`. Probes `ctx.Err()` once every 256 BFS dequeues / DFS recursive entries (power-of-two mask) so client disconnects and server shutdown unwind in-flight traversals promptly instead of blocking until `ReadTimeout`/`WriteTimeout` fires. BFS funcs now return errors on cancel.
- **Server graceful shutdown wired through to in-flight requests** — `http.Server.BaseContext` is linked to a shutdown-linked parent context so `Stop()` cancels every in-flight request's `r.Context()` before `httpServer.Shutdown` waits for handlers to drain. Combined with the cancellable BFS, graceful shutdown is now deterministic — stuck traversals no longer hold the server open.
- **List-comprehension shapes in shortestPath returns** — `pathToValue` now supports `[n IN nodes(p) | n.<prop>]`, `[r IN relationships(p) | type(r)]`, and the `id`/`elementId`/`labels` variants. Previously these returned `null`.

### Changed

- **MVCC commit-sequence sharded per database; transactions pinned to one namespace.** The MVCC counter and high-water timestamp clamp moved off `BadgerEngine` (engine-global) onto a per-namespace `namespaceMVCCState` map keyed by database name. Each transaction is pinned to a single namespace at the first prefixed write (or eagerly via `BadgerTransaction.SetNamespace`); cross-namespace writes return the new sentinel `ErrCrossNamespaceTransaction`. Snapshot-isolation conflict detection compares `CommitSequence` values that share a namespace by construction, so a noisy tenant can no longer interleave version numbers with another's, and `DROP DATABASE` drops its counter alongside its keys.
- **Lifecycle prune planner consults a per-namespace safe-floor callback** — cross-namespace counters aren't comparable, so a head in namespace A is now evaluated against namespace A's oldest reader, not the global minimum across every tenant. `ReaderRegistry.OldestReaderVersionsByNamespace()` groups readers by `info.Namespace` and returns the per-group minimum.
- **Property-key dictionary persistence is out-of-band.** Counter high-water marks and pending forward/reverse entries are drained at commit time and persisted in a fresh badger transaction so they don't enter the user txn's SSI read/write set. Without this, every property-writing commit in the same namespace would touch the same propkey counter key and concurrent first-allocation commits would race on Badger's optimistic conflict check, surfacing "Transaction Conflict" instead of the genuine constraint-violation shape.
- **Bolt commit-failure wrapper unified across both commit paths.** The implicit-autocommit path at `pkg/cypher/executor.go:1978` now produces `"commit failed: ..."` (matching the explicit `BEGIN/COMMIT` path at `pkg/cypher/transaction.go:181`) instead of the legacy `"failed to commit implicit transaction: ..."`. Downstream Bolt classifiers that match on the substring no longer need to handle two shapes for the same race.
- **Variable-length pattern parser default raised** — patterns without an upper bound (`[*]`, `[*N..]`) previously capped at `MaxHops=10/100`, so `shortestPath` silently returned no rows on graphs whose diameter exceeded the cap. The parser now uses `VarLengthUnboundedMaxHops = 1<<24` so BFS terminates when the frontier is exhausted instead of when the cap fires.

### Fixed

- **MERGE on a uniquely-constrained value is now deterministically idempotent under concurrent writers.** Two writers MERGE-ing on the same `uid` would intermittently surface Badger's generic `"Transaction Conflict"` (or, after the first round of fixes, an SI `"node ... changed after transaction start"` conflict) instead of the consumer-pinned commit-time UNIQUE shape — regression: `TestMERGE_IsIdempotentUnderConcurrentRetry`, ~3 % originally and ~0.05 % after the partial fix. Root cause has two coupled parts:
  1. **Read-set leak.** `writeNodeMVCCHeadInTxn` and the archive-pre-overwrite paths read `mvccNodeHeadKey` via the user txn to carry `FloorVersion` forward; that read entered Badger's SSI set, and a peer commit on the same head key forced a generic `Transaction Conflict`. Fixed by routing those reads through fresh read transactions (`loadNodeMVCCHead` / `loadEdgeMVCCHead`).
  2. **MERGE-MATCH redirect onto a peer.** When MERGE's MATCH (a fresh read-txn lookup) found a node a peer committed between begin and our pin, the subsequent `OpUpdateNode` ran on the peer's nodeID. `validateSnapshotIsolationConflicts` then fired `checkNodeWriteConflict` (head version > readTS) → ErrConflict. **The semantic cause is the consumer-pinned commit-time UNIQUE race**, not a generic SI conflict, so the storage layer now reclassifies: when an OpUpdate hits an SI conflict on a nodeID whose pending body carries a uniquely-constrained value already mapped to that nodeID in the schema's UNIQUE-value cache, `checkNodeWriteConflict` returns a `ConstraintViolationError{Type: ConstraintUnique, Cause: ErrConflict}` and the commit path wraps it as `commit failed: constraint violation: … already exists`. The transient classifier still fires via the `Cause` chain, so retry-aware drivers see the documented retry sentinel. Verified at 50,000 stress iterations: zero non-deterministic failures.
- **Snapshot isolation broken for transactions that read before any write.** The earlier per-namespace MVCC refactor lazy-pinned `tx.readTS` to the namespace's CURRENT sequence at first prefixed read/write — which let peer commits that landed between begin and pin become visible. That broke anchored snapshot reads (`TestTransaction_Isolation`, `TestTransaction_ReadYourWritesDoesNotBreakAnchoredSnapshot`) and the SI conflict checks for concurrent CREATE on the same ID (`TestCheckNodeCreateConflict_ConcurrentWriteAfterReadTS`, `TestCheckEdgeCreateConflict_ConcurrentWriteAfterReadTS`). Fixed: `BeginTransaction` now snapshots every namespace's sequence into `tx.beginSnapshot`; lazy pin binds readTS to the begin-time entry instead of the current one. Namespaces created after begin pin to seq=0 (correctly invisible).
- **Snapshot-invisible reads no longer leak via the legacy fallback.** `GetNodeVisibleAt` / `GetEdgeVisibleAt` now return the new `ErrNotVisibleAtSnapshot` sentinel when a head exists but the caller's snapshot can't see it, distinct from `ErrNotFound` (no head at all). `getCommittedNodeLocked` falls back to a primary-key read only on the latter — the former is a hard miss. Without this, the badger-snapshot fallback would expose a peer's post-begin commit on every visibility-rejected read.
- **`discover` returned the outer RRF rank score in `similarity`, not cosine.** The `discover` MCP/Heimdall paths overwrote `similarity` with `1 / (60 + rank)` (≈ 0.0164 at rank 1, 0.0091 at rank 50). Because that score depends only on rank position, every query returned an identical top-to-bottom sequence regardless of content, making the `min_similarity` threshold useless: any caller setting it above ~0.02 silently filtered everything; any caller setting it to 0 had no filter at all. Both paths now track `bestSim` (the strongest underlying score across chunks) and surface that as `similarity` while keeping outer RRF for sort order. The BM25-only fallback in `pkg/mcp` also now reads `r.Similarity` instead of `r.Score`. Two new regression tests pin the cosine-vs-RRF distinction: `TestHandleDiscover_SimilarityIsCosineNotRRF` and `TestQueryExecutor_Discover_SimilarityIsBestScore`.
- **`shortestPath` parameter substitution** — params were being substituted _after_ parsing, so `MATCH (start:Star {starId: $startId})` matched the literal string `"$startId"` against every node, fell through to `AllNodes() × AllNodes()` BFS, and hung. Substitution now runs before parsing in both the executor and transaction dispatchers; unresolved variables now refuse rather than fall through.

### Internal

- **`ConstraintViolationError` carries a `Cause` field** with `Unwrap` so the storage layer can synthesize a unique-constraint violation from a commit-window peer race while preserving `errors.Is(err, ErrConflict)` for transient classification. `errors`-package wrappers and downstream Bolt classifiers continue to work unchanged.
- **`pkg/storage/db.go` removed.** The `*storage.DB.Update` helper embedded a `for attempt := 0; attempt < maxRetries; attempt++ { … }` retry loop in the storage layer, which violates the consumer-pinned contract: retries are the consumer's responsibility (Bolt drivers retry on the documented transient sentinels; the cypher executor does not retry). The helper had zero non-test callers and only existed to test itself. The contract is now: `engine.BeginTransaction()` → `tx.Commit()` returns the documented error; the caller decides whether to replay.
- **Cross-namespace transactions: invariant enforced, dead branches deleted.** Production never opened a transaction whose pending writes spanned multiple namespaces; the cypher executor's `transactionStorageWrapper` and the explicit-tx path both pin a single namespace at construction. The defensive multi-namespace branches in `acquireUniqueConstraintCommitLocks`, `validateNodeConstraints`, `checkUniqueConstraint`, `checkNodeKeyConstraint`, `checkTemporalConstraint`, `validateEdgeConstraints`, and `validatePolicyOnNodeLabelChange` were unreachable. All of those paths now look up the schema once from `tx.namespace` instead of re-parsing the prefix off each entity ID.
- **22 deterministic tests added** across `pkg/storage` and `pkg/storage/lifecycle` covering namespace pinning, per-namespace counters, high-water isolation, sequence saturation fallback, legacy seed migration, per-namespace prune floors, and the lost-update regression.
- **Five new consumer-error-contract regression tests** so the wire shape can't drift again: `RefreshUniqueConstraintValuesForEngine` keeps storage-prefixed IDs under `NamespacedEngine`; the five canonical optimistic-conflict messages (node, node-with-version-detail, edge, edge-alternate, adjacent-edge) plus `errors.Is(err, ErrConflict)` chain preservation; node + relationship variants of `commit failed: constraint violation: … already exists`; MERGE-loser sees the pinned wire shape and a retry succeeds with its `SET` visible; `LoadDefaults()` returns documented values for `Database.AsyncWritesEnabled=true`, `Database.PersistSearchIndexes=false`, `Auth.Enabled=false`, `Server.BoltPort=7687`, `Server.HTTPPort=7474`. Each pinned source-of-truth line gained a `// Wire contract:` comment.
- **CI server package split** — `pkg/server` tests are split during CI to use less resources.

### Documentation

- **`docs/skills/` — agent-ready skill files** for every consumer-facing surface: hot-path Cypher cookbook, Bolt client, gRPC (Qdrant + NornicSearch), Qdrant migration, Neo4j migration, knowledge policies, decay tuning, promotion policies, managed embeddings, vector & full-text search, RAG procedures.
- **Knowledge-policy user guides rewritten** with disambiguated bundle-vs-binding language: bundles are inert parameter packages; bindings carry the `FOR` target; decay is pure time math evaluated on read; `ON ACCESS` belongs to promotion only; `LAST_ACCESSED` decay only reads access metadata.
- **Documentation site consolidated to 8 top-level groups** (was 14). Every published `.md` is now in `nav:`, fixing the bug where clicking a search result loaded the content but left the sidebar empty. Canonical URLs emit on every page; URL ↔ sidebar ↔ content stay synchronized when navigating from a search result.
- **Documentation audit pass** — fixed stale env vars (`NORNICDB_NO_AUTH` → `NORNICDB_AUTH=none`, `NORNICDB_EMBEDDING_URL` → `NORNICDB_EMBEDDING_API_URL`) across development/setup, compliance/rbac, operations/deployment, troubleshooting, low-memory-mode, getting-started; corrected Material/MkDocs anchor slugs (single-dash, not double-dash) in `decay-profiles.md`, `operations/troubleshooting.md`, and twelve self-references in `release-notes-since-v1.0.11.md`; removed false `cache: true` claims for `db.infer` in `cypher-rag-procedures.md`. `mkdocs build` runs clean with zero anchor or broken-link warnings.
- **Skills aligned to verified parser/executor behavior** — knowledge-policies skill now states the actual binding-resolution tie-break (lower `Order` wins; equal-`Order` against the same target is a build error); the misleading `ALTER PROMOTION POLICY ... SET OPTIONS { ... }` example was removed (the executor only honors `enabled` on policies); `nornicdb.knowledgepolicy.resolve()` is documented as a dry-run with empty access metadata; `db.rerank` is documented as pass-through (not error) when no reranker is configured.

### Tools & Scripts

- **`scripts/migration/neo4j/{migrate.py,migrate.go,migrate.mjs}`** — runnable Neo4j → NornicDB migrations in Python, Go, and Node. Schema-first (constraints + indexes including fulltext and vector), then nodes via `UnwindSimpleMergeBatch`, then edges via `UnwindMultiMatchCreateBatch`. Keyset-paginated by `elementId`. Preserves source IDs on `_neo4j_id` and original labels on `_neo4j_labels` for an operator-driven label promotion pass.
- **`scripts/migration/qdrant/{migrate.py,migrate.go,migrate.mjs}`** — runnable Qdrant → NornicDB migrations. Python and Go use the Qdrant-compatible gRPC surface end-to-end; the Node script reads from Qdrant over REST and writes into NornicDB over Bolt (each point becomes a node with its vector and payload), since NornicDB has no Qdrant-compatible REST surface and there's no maintained JS gRPC client. Replicates collection vector configs, scrolls and upserts points in batches, verifies counts.
- All Go scripts vet and build clean against `github.com/qdrant/go-client v1.18.1` and `github.com/neo4j/neo4j-go-driver/v5 v5.28.4`.

### Technical Details

- **Range covered**: `v1.1.0..main`
- **Primary focus areas**: per-namespace MVCC isolation with begin-time snapshots, deterministic MERGE-under-contention semantics (no spurious Badger SSI conflicts for commit-time UNIQUE losers; no readTS lazy-jump that hides peer commits from anchored snapshots), discoverable consumer documentation surface (skills + migration scripts + unified Bolt error shape), and `discover` similarity-vs-RRF correctness.

## [v1.1.0] - 2026-05-14

# ⚠️ Breaking rolling upgrade changes to storage in this release. BACK UP YOUR DATA ⚠️

## Running the database requires using the `--upgrade-storage` option or in the macOS installer using the checkbox on the first run wizard, or in the tray settings panel restarting with the flag enabled. after your files are updated in place, older versions of the database will not be able to read the datafiles properly. This change is to compact the storage. after this, hopefully there shouldn't be any major overhauls to storage 🤞

### Added

- **Property pre-filter for hybrid search REST endpoint**:
  - added an optional `filters` field to `POST /nornicdb/search` that restricts the candidate set by property values _before_ top-K vector/BM25 selection, preserving recall for sparse filtered sets.
  - filter semantics: OR within a key (`{"collection": ["user_a", "user_b"]}`), AND across keys (`{"collection": ["user_a"], "type": ["text"]}`); scalar and array property values both supported.
  - filter applied in `rrfHybridSearch`, `vectorSearchOnly`, and `fullTextSearchOnly` paths.
  - primary use case: multi-tenant RAG where each node carries a `collection` array identifying which users may access it.

- **Knowledge-policy decay model support**:
  - added support for inverted Ebbinghaus-style decay curves in knowledge-policy scoring.

- **Storage migration observability**:
  - added more robust migration progress/statistics logging to improve operator visibility during upgrades.

### Changed

- **Storage serialization defaults and upgrade posture**:
  - removed Gob as an active serializer while preserving forward migration support for older data that was written using Gob.

- **Storage keying internals**:
  - introduced a per-namespace property-key dictionary as part of the storage-v2 path.

### Fixed

- **Hybrid search filter edge case**:
  - fixed empty filter-list handling so requests with empty filter lists no longer discard all results.

- **Cypher parsing and transaction conflict behavior**:
  - fixed `NOT IN` parsing behavior in the Cypher path.
  - adjusted transaction timing in conflict-sensitive paths to reduce avoidable write conflicts.

- **Knowledge-policy `ON ACCESS` arithmetic coercion**:
  - fixed runtime type coercion to align with Cypher DDL expectations.

- **Knowledge-policy `ON ACCESS` recording scope**:
  - moved `ON ACCESS` recording out of storage visibility checks to trigger only for entities actually materialized into query results.
  - routed recording through storage wrappers to preserve namespaced IDs correctly.
  - extended e2e coverage with positive and inverse test cases including accumulator contents before flush.

- **MVCC commit ordering at sequence saturation**:
  - hardened MVCC commit ordering when the global commit sequence reaches `MaxUint64`.
  - The reason for the monotonic counter is that we can actually record transactions so fast that slower processors can have negative nanos drift causing random conflicts with serial ingesiton. @1 million commits/sec it would exhaust in 584.5 million years. @1 billion/sec - 584,500 years.
  - keep sequence pinned and fall back to strictly increasing high-water timestamps rather than wrapping or failing in said years.
  - updated snapshot conflict detection to use timestamp ordering only for the saturated equal-sequence case.
  - added focused regression tests for the saturation fallback.

- **Merge-conflict error surfacing for clients**:
  - fixed unique-merge conflict handling so retryable conflict errors are surfaced and scoped correctly for Bolt clients.

- **Migration index completeness**:
  - fixed storage migration so expected indexes are created during migration.

- **Lifecycle and fast-write clock handling**:
  - fixed lifecycle and high-watermark nanosecond timing bugs affecting fast-write scenarios.

### Documentation

- Added and refreshed:
  - memory-decay and decay-profile documentation for the new inverted decay behavior and documented scenarios
  - user guidance around promotion policy and Ebbinghaus/Roynard bootstrap behavior

### Technical Details

- **Range covered**: `v1.1.0-preview-1..main`
- **Commits in range**: 29 (non-merge)
- **Repository delta**: 154 files changed, +105,380 / -1,336 lines
- **Primary focus areas**: hybrid search pre-filtering, conflict and parsing correctness, storage-v2 migration and serializer evolution, and knowledge-policy decay/scoring improvements.

## [v1.1.0-preview] - 2026-05-12

### Added

- **Cypher-native knowledge policy administration**:
  - added `CREATE`, `ALTER`, `DROP`, and `SHOW` support for knowledge-policy management so decay and promotion behavior can be administered directly through Cypher.
  - added built-in `CALL nornicdb.knowledgepolicy.*` procedures for inspecting policy state, profiles, resolution, and deindexing status.
  - added the corresponding browser-side management flow so the policy UI now works against the Cypher-backed surface instead of a separate admin API.

- **Knowledge lifecycle scoring and visibility control**:
  - added the new knowledge-policy runtime model, including shared profile resolution, access metadata indexing, scoring-before-visibility, and suppression/deindex infrastructure.
  - added Cypher functions for policy-aware scoring and diagnostics, including `decayScore`, `decay`, and `policy`.

- **End-to-end observability stack**:
  - added OpenTelemetry tracing across HTTP, Cypher planning and execution, Bolt sessions, storage operations, embeddings, and search flows.
  - added Helm chart support for observability deployment, including `ServiceMonitor`, `PodMonitor`, startup/readiness/liveness probes, and default network-policy wiring.
  - added a curated Grafana dashboard, Prometheus alert rules, and an auto-generated metrics reference covering the current exported metric catalog.
  - added pprof hooks for goroutine and mutex profiling to make production debugging easier.

- **More compact storage internals**:
  - added internal numerical IDs and a freelist-based key reclamation path to reduce storage overhead while keeping keys monotonic and reusable.

### Changed

- **Knowledge-policy operator surface**:
  - moved knowledge-policy administration away from the dedicated HTTP API surface and onto the new Cypher DDL and procedure model.
  - aligned the admin UI, runtime behavior, and diagnostics around the new suppression-anchor and decay-profile model.

- **Write-heavy query execution**:
  - expanded and tuned bulk-insert and canonical write paths so more high-volume ingestion shapes avoid unnecessary reads and stay on optimized execution paths.
  - refreshed the bulk-insert cookbook and related operator guidance to match the current fast-path behavior.

- **Management UI grid behavior**:
  - updated the browser management pages to use newer `uiGrid` capabilities for database and retention administration views.

- **Local LLM runtime bits**:
  - upgraded the bundled `llama.cpp` integration to `b9106`.

### Fixed

- **Knowledge-policy correctness and stability**:
  - fixed decay diagnostics, targeted suppression invalidation, score-argument handling, and reveal-path correctness.
  - hardened embed/reveal worker concurrency so the new knowledge lifecycle features behave predictably under load.

- **Storage metadata and schema recovery**:
  - fixed MVCC schema metadata recovery, edge-between deindexing behavior, and schema lock leak scenarios.
  - tightened uniqueness-vs-constraint-contract handling in the affected storage and Cypher paths.

- **Observability safety**:
  - fixed trace redaction and baggage filtering so credentials and other sensitive values are not emitted into spans.
  - fixed nil-pointer and span-property edge cases in the observability pipeline.

- **Cross-platform build and packaging reliability**:
  - fixed Windows and macOS build and packaging regressions that were affecting release CI and local packaging flows.

### Compatibility

- **MVCC is now opt-in by default**:
  - the default retained-version count is now `0`, which disables MVCC history unless explicitly enabled in configuration.

### Documentation

- Added and refreshed:
  - the metrics reference generated from the current observability catalog
  - Helm, dashboard, and alerting guidance for the new observability stack
  - knowledge-policy and decay diagnostics documentation aligned to the Cypher-backed administration model
  - bulk-insert cookbook material for the current optimized write paths

### Technical Details

- **Range covered**: `v1.0.45..HEAD`
- **Commits in range**: 62 (non-merge)
- **Repository delta**: 490 files changed, +65,079 / -8,480 lines
- **Primary focus areas**: knowledge-policy lifecycle management, full-stack observability, storage compaction and write-path performance, cross-platform packaging reliability, and release-facing documentation.

---

For releases prior to `v1.1.0-preview`, see [`docs/changes-before-1.1.x.md`](docs/changes-before-1.1.x.md).
