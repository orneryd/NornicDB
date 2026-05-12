# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Latest Changes]

- See `docs/latest-untagged.md` for the untagged `latest` image changelog.

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

## [v1.0.45] - 2026-05-08

### Added

- **Broader hot-path coverage for `UNWIND MATCH SET` updates**:
  - added a batched `UNWIND` + `MATCH` + `SET` hot path so more update-heavy Cypher write workloads can stay on the optimized execution path.

### Changed

- **Browser query-results grid behavior**:
  - updated the web UI to `@ornery/ui-grid-*` `1.0.5`, bringing the latest resize and scroll tracking behavior from the new grid runtime.
  - query-results selection now relies on the grid's per-column opt-out behavior for the synthetic Select column, so hidden sort and group controls follow the legacy ui-grid behavior instead of rendering as disabled header buttons.

- **Cypher write-path routing**:
  - tightened `MATCH ... SET` hot-path routing so optimized update execution only applies when uniqueness guarantees are present, reducing incorrect fast-path routing for ambiguous match shapes.

- **Maintenance and dependency refreshes**:
  - refreshed UI grid dependencies from the `1.0.4` line to `1.0.5` and regenerated the UI lockfile to match the published package graph.
  - updated package metadata and peer-dependency wiring in the UI package so installs resolve cleanly against the latest grid release.
  - folded in mainline merge-conflict cleanup that affected the release range.

### Fixed

- **Embedding worker lifecycle stability**:
  - fixed overlapping embed-worker reset and close flows by serializing worker lifecycle transitions, preventing wait-group reuse panics during concurrent embedding workloads.

- **Grid column opt-out chrome**:
  - fixed the query-results Select column so column-level sorting and grouping opt-outs now hide header controls using the grid's native per-column behavior instead of relying on CSS workarounds.

### Technical Details

- **Range covered**: `v1.0.44..HEAD`
- **Commits in range**: 9 (non-merge)
- **Repository delta**: 15 files changed, +1,322 / -582 lines
- **Primary focus areas**: query-results grid upgrades, `UNWIND MATCH SET` hot-path expansion and safety, embedding-worker lifecycle stability, and targeted UI package maintenance.

## [v1.0.44] Lithium - 2026-05-02

### Added

- **Convenience defaults for vLLM-backed local generation**:
  - added vLLM-friendly defaults in the Heimdall/OpenAI-compatible generation path so local and self-hosted inference setups require less manual configuration.

- **Relationship-head recovery fallback for storage indexes**:
  - added an edge-between head-index fallback path so relationship lookups can recover more reliably when derived index state needs repair or reconstruction.

- **Named relationship-set support in batched merge flows**:
  - added support for named relationship sets in `UNWIND` merge batches, expanding the structured relationship-merge shapes that can stay on the optimized execution path.

### Changed

- **Relationship merge and probe execution paths**:
  - optimized relationship probe planning and batched SQL relationship writes so more merge-heavy Cypher workloads avoid redundant work.
  - shared node-lookup cache locking across executor and transaction clones to reduce duplicated lookup coordination during concurrent execution.

- **Transient error classification and retry signaling**:
  - refactored transient conflict handling into the shared `pkg/errors` surface and mapped Bolt transaction conflicts onto explicit transient error semantics.
  - improved server-side failure handling so retryable conflict conditions are surfaced more consistently to clients.

- **Storage relationship-index maintenance**:
  - hardened edge-between index repair, exact relationship lookups, and streamed edge index rebuild behavior to keep relationship indexes aligned with stored graph state.

- **Documentation and dependency maintenance**:
  - refreshed the README, retention-policy guide, and hot-path query cookbook to match the current relationship hot-path behavior and operator guidance.
  - updated Go module dependencies and UI package dependencies as part of the release range.

### Fixed

- **Relationship merge property-set correctness**:
  - fixed relationship merge property updates so property sets are applied correctly across merge execution paths.

- **Relationship hot-path edge cases**:
  - fixed edge-case handling in relationship hot paths, including exact edge lookup behavior and fallback coverage for edge-between index state.

- **Bolt transient failure behavior**:
  - fixed Bolt failure handling so conflict-driven transaction failures are decoded and classified more robustly for clients.

### Tests

- Added and expanded coverage for:
  - relationship merge property handling and named relationship-set merge batches
  - relationship hot-path routing, SQL relationship writes, and probe optimization behavior
  - Bolt transient error mapping and failure-chunk decoding
  - storage edge-between index repair, streamed rebuild behavior, and exact relationship lookup regressions
  - shared-state isolation in affected regression tests

### Documentation

- Added and refreshed:
  - retention policy operator guidance
  - README release-facing behavior notes
  - hot-path cookbook coverage for the current relationship query optimizations

### Technical Details

- **Range covered**: `v1.0.43..HEAD`
- **Commits in range**: 23 (non-merge)
- **Repository delta**: 40 files changed, +2,494 / -255 lines
- **Primary focus areas**: relationship merge correctness, relationship hot-path and edge-index reliability, transient Bolt conflict handling, convenience inference defaults, and targeted documentation/dependency refreshes.

## [v1.0.43] Clockwork - 2026-04-24

### Added

- **Runtime retention and privacy control plane**:
  - added runtime retention sweep management in the database layer, including sweep budgeting, excluded-label handling, record collection, and immediate sweep triggering.
  - added admin retention endpoints for policies, legal holds, erasure requests, manual sweeps, and retention status.
  - added GDPR integration hooks so privacy and retention workflows can share the same server-side control surface.

- **Retention administration UI**:
  - added a dedicated Retention Admin page in the web UI with policy editing, legal hold management, erasure request processing, defaults loading, and sweep controls.
  - wired new client API types and routes so operators can manage retention behavior from the browser instead of only through raw admin calls.

- **Constraint and planner regression coverage**:
  - added extensive deterministic tests for constraint contract parsing, schema validation, Bolt database-manager behavior, transaction wrapper reuse, `UNWIND` merge batches, and fabric helper types.
  - added regression coverage around database-scoped rollback handling and deterministic multidatabase enforcement behavior.

- **Knowledge-layer documentation and research material**:
  - added a full knowledge-layer persistence implementation plan, expanded design material, Kalman-focused planning docs, and a new research-paper draft on persistence semantics. v1.1.0

### Changed

- **Retention configuration and operator surface**:
  - expanded runtime config, example YAML, and macOS/app UI surfaces for retention and memory-management controls.
  - made chunk overlap configurable in the macOS app and web UI chunking flow.

- **Cypher MERGE and `UNWIND MATCH` routing**:
  - routed more `UNWIND` + `MATCH`/map-merge shapes through the hot path and tightened optional-match fallback behavior.
  - started using unique-constraint and unique-index information more aggressively for merge lookup and validation paths.

- **Constraint contract parsing and validation internals**:
  - replaced older regex-heavy constraint-contract parsing with allocation-free keyword scanning helpers.
  - normalized unique-constraint cache keys and extended contract matching to cover broader label combinations and namespace cases.

- **Documentation and release content**:
  - refreshed README, operations docs, compliance docs, user-guide indexes, and the hot-path cookbook to match the current runtime behavior.
  - removed or folded obsolete planning material into the newer knowledge-layer and compliance documentation set.

- **Dependency maintenance**:
  - refreshed Go module dependencies and updated UI package dependencies as part of the release range.

### Fixed

- **Database-scoped Bolt rollback correctness**:
  - fixed Bolt explicit transaction rollback so rollback is honored against the correct selected database instead of leaking across database scope boundaries.
  - hardened transaction wrapper reuse paths and related rollback regressions in Bolt/Cypher integration tests.

- **Constraint validation correctness and safety**:
  - fixed unique lookup and validation paths to guard non-comparable values and avoid incorrect equality handling during unique checks.
  - corrected constraint-validation plumbing so unique indexes and schema checks stay aligned across storage and Cypher surfaces.

- **Local LLM generation stability**:
  - fixed CGO generation-path issues in the local llama integration, including causal-attention setup and health checks for non-unified KV caches.

- **Deterministic multidatabase enforcement tests**:
  - fixed rate-limit and enforcement test behavior to avoid timing-driven flakes and keep multidatabase coverage deterministic.

### Tests

- Added and expanded coverage for:
  - retention manager behavior, retention admin handlers, and privacy/erasure workflows
  - Bolt database-manager integration, database-scoped rollback, and transaction-wrapper reuse
  - `UNWIND`/`MATCH`/`MERGE` hot-path routing and fallback behavior
  - constraint contract parsing, validation, schema interactions, and unique lookup edge cases
  - fabric helper coverage, plan-cache behavior, and multidatabase enforcement determinism

### Documentation

- Added and refreshed:
  - GDPR compliance guidance and retention-policy operator documentation
  - knowledge-layer persistence plan and implementation details
  - Kalman behavioral signal planning notes
  - persistence-semantics research paper draft, evaluation plan, bibliography, and related-work notes

### Technical Details

- **Range covered**: `v1.0.42-hotfix..HEAD`
- **Commits in range**: 47 (non-merge)
- **Repository delta**: 64 files changed, +11,730 / -1,528 lines
- **Primary focus areas**: retention/GDPR operator workflows, Bolt rollback correctness, unique-constraint driven planner and validation improvements, deterministic regression coverage, and major knowledge-layer documentation expansion.

## [v1.0.42-hotfix] Anthem Pt. 2 - 2026-04-17

### Added

- **Explicit Bolt host binding support**:
  - added a dedicated Bolt host field so the Bolt listener can bind to the resolved server address instead of always falling back to the wildcard socket.

- **Bind-address regression coverage**:
  - added CLI/config/environment precedence tests for server bind resolution.
  - added Bolt listener tests to verify loopback binding and secure default host behavior.

### Changed

- **Bind-address resolution precedence**:
  - centralized bind-address resolution for `serve` startup so CLI flags, loaded config, and explicit protocol-specific environment variables are resolved consistently before HTTP and Bolt are started.
  - aligned startup endpoint display output with the resolved address, while still presenting `localhost` for wildcard binds in user-facing logs.

- **Dependency maintenance**:
  - updated `github.com/hybridgroup/yzma` to `v1.12.0`.
  - refreshed related Go module entries pulled in by the dependency update.
  - updated UI dev dependencies: `postcss` to `8.5.10` and `typescript` to `6.0.3`.

### Fixed

- **Bolt listener exposure**:
  - fixed the Bolt server so configured bind addresses now apply to the actual listener instead of only affecting user-facing startup messages.
  - restored secure loopback binding as the default when no explicit override is provided.

### Tests

- Added and expanded coverage for:
  - CLI `--address` precedence over config and environment sources
  - config-file and explicit environment bind-address overrides
  - Bolt listener binding to the configured host instead of wildcard interfaces

### Technical Details

- **Commit covered**: `adce4f9a9fc7b6aada07c0bfa2d737cd7a6efaca`
- **Commits in release**: 1
- **Repository delta**: 8 files changed, +194 / -26 lines
- **Primary focus areas**: Bolt bind-address correctness, secure default listener behavior, regression coverage, and targeted Go/UI dependency maintenance.

## [v1.0.42] Anthem - 2026-04-16

### Added

- **Heimdall tool coverage and documentation**:
  - exposed local chat tool routing more consistently so the OpenAI compatible endpoint can surface the user's tools from an agent into injected actions in chat flows.
  - added and refreshed Heimdall, knowledge-layer, and compliance documentation, including new guides for the Heimdall AI assistant and knowledge-layer persistence planning.

- **Regression coverage for transactional and constraint edge cases**:
  - added regression coverage for constraint handling in Bolt/Cypher paths.
  - added targeted tests for async flush race conditions and complex `UNWIND`/`MERGE` fallback behavior.

### Changed

- **Heimdall local chat behavior**:
  - hardened action parsing, tool forwarding, and local chat prompt behavior so action envelopes are handled more predictably.
  - simplified the watcher hello/search tool flow and aligned injected action naming with the current plugin surface.

- **Cypher hot paths and fallback routing**:
  - generalized hot-path execution to support broader n-ary and generic query shapes.
  - refined `UNWIND` + `MERGE` fallback routing so complex query shapes are handed off to the full executor more consistently.
  - preserved transaction-visible lookups in fallback execution paths to keep behavior aligned with in-transaction state.

- **Documentation and release content**:
  - folded older planning material into the current docs set and refreshed migration, AI-agent, and deployment-facing documentation.

### Fixed

- **MERGE cache and transaction correctness**:
  - fixed stale `MERGE` cache invalidation after implicit transaction aborts.
  - fixed async flush timing so MVCC heads do not advance while a transaction still depends on its visible state.

- **Installer and model download reliability**:
  - updated installer/download locations and added validation so failed model downloads are not accepted as valid artifacts.
  - improved macOS model download handling to verify downloaded files before treating them as usable GGUF models.

- **Heimdall local chat stability**:
  - fixed local chat template replay and unknown tool-call forwarding behavior.
  - hardened Heimdall test races and related local chat execution edge cases.

### Tests

- Added and expanded coverage for:
  - async engine flush/count race handling
  - `UNWIND`/`MERGE` fallback and optional lookup regression paths
  - constraint regressions across Bolt and Cypher execution
  - Heimdall local chat tool execution and race-sensitive paths

### Technical Details

- **Range covered**: `v1.0.41..HEAD`
- **Commits in range**: 19 (non-merge)
- **Repository delta**: 48 files changed, +5,437 / -3,007 lines
- **Primary focus areas**: Cypher fallback correctness, transaction visibility and async flush safety, Heimdall local chat/tool handling, installer download validation, and documentation consolidation.

## [v1.0.41] "Blue Orchid" - 2026-04-15

### Added

- **Graph explorer UI**:
  - added a new graph explorer tab with neighborhood visualization and node-details integration.
  - wired the browser page, node details panel, and client API helpers to support the new exploration flow.

- **MERGE and string-literal infrastructure**:
  - added shared string-literal decoding helpers so Cypher literal handling is consistent across execution paths.
  - expanded merge-chain and relationship-shape support for the fallback and hot-path planner routes.

### Changed

- **UNWIND and MERGE hot paths**:
  - generalized the staged compound `UNWIND` mutation path and tightened the merge-chain hot path so parameter handling and optional lookups behave consistently.
  - memoized the merge-chain path where the executor can safely reuse the structured shape detection.

- **Cypher executor plumbing**:
  - updated executor, pattern parsing, property evaluation, and clause handling to align the new hot-path routing logic.
  - refreshed the query-shape tests and regression coverage around compound `UNWIND`, merge chains, and literal decoding.

- **Release and dependency maintenance**:
  - updated README/version metadata and release workflows for the new build.
  - refreshed dependency pins across Go, UI, llama.cpp, Docker, and build scripts.
  - updated the hot-path performance cookbook to document the new query-shape coverage.

### Fixed

- **Fallback MERGE binding preservation**:
  - fixed fallback MERGE chain handling so bindings are preserved correctly when the structured hot path is not selected.
  - restored parameter handling in the generalized `UNWIND` merge-chain route.

- **Linker and packaging stability**:
  - fixed the native linker path used by the CUDA build flow.
  - cleaned up version bump artifacts so the packaged build reports `1.0.41` consistently.

### Tests

- Added and expanded coverage for:
  - compound `UNWIND` mutation staging and merge-chain hot-path routing
  - fallback MERGE chain bindings and optional lookup behavior
  - string-literal decoding and property evaluation helpers
  - graph explorer browser integration and node-details interactions
  - executor trace coverage and query-shape regression cases

### Technical Details

- **Range covered**: `v1.0.40..HEAD`
- **Commits in range**: 10 (non-merge)
- **Repository delta**: 50 files changed, +3,163 / -502 lines
- **Primary focus areas**: Cypher merge/UNWIND hot-path generalization, literal decoding cleanup, graph explorer UI delivery, dependency refreshes, and packaging/build fixes.

## [v1.0.40] "Kiyote" by rumspringa - 2026-04-11

### Added

- **Structured Cypher hot-path matchers**:
  - replaced several regex-based fast-path routers with scanner/helper-based matchers for compound queries, `UNWIND` routes, traversal shapes, shortest-path handling, and related query-shape detection.
  - added explicit hot-path trace coverage so the executor can report when the new structured matchers are used.

- **Parser comparison reporting**:
  - added an integrated Nornic-vs-ANTLR comparison harness that prints per-query timing ratios and an overall summary.
  - stabilized the comparison output so users and maintainers can trust the speedup numbers instead of single-run noise.

### Changed

- **Traversal and match planning**:
  - improved traversal planning for top-k seeded lookups so multi-key `ORDER BY` shapes can still use the indexed path when possible.
  - compiled common binding-row `WHERE` predicates and made traversal execution honor the active temporal viewport consistently.
  - refined MERGE routing for composite-key patterns and hardened batch deduplication so key collisions are handled predictably.

- **Cypher execution cleanup**:
  - migrated hot-path query-shape parsing away from regex capture arrays and onto shared helpers.
  - removed obsolete hot-path regex definitions after the structured matcher cutover.
  - refreshed parser-mode and query-pattern plumbing so the same user-facing queries route more consistently across executor paths.

- **Documentation and release hygiene**:
  - refreshed README and deployment notes to reflect the current product direction.
  - updated performance notes for hybrid Bolt queries.
  - refreshed dependencies as part of the release cycle.

### Fixed

- **Shortest-path parsing regression**:
  - fixed a panic in single-`MATCH` shortest-path queries by correcting the clause boundary scan used to recover the preceding `MATCH` clause.
  - added regression coverage for the exact Bolt fallback query shape that exposed the issue.

- **Query-shape stability**:
  - restored deterministic ordering and route selection for the query families that were being optimized in the hot path.
  - tightened parser and traversal tests around the new helper-based implementations.

### Tests

- Added and expanded coverage for:
  - structured compound-query matcher behavior and reject reasons
  - hot-path trace assertions for compound query routing and traversal seeding
  - shortest-path parsing, execution, and regression handling
  - `UNWIND` collect/distinct and batch merge helper paths
  - parser comparison output and performance reporting

### Technical Details

- **Range covered**: `v1.0.39..HEAD`
- **Commits in range**: 13 (non-merge)
- **Primary focus areas**: Cypher hot-path matcher migration, traversal and merge routing, parser comparison tooling, shortest-path correctness, and release documentation updates.

## [v1.0.39] "Punkrocker" - 2026-04-06

### Added

- **Block-style constraint contracts (`REQUIRE { ... }`)**:
  - added grouped schema contracts so multiple rules can be declared together on a node or relationship target.
  - block entries can mix primitive constraints already supported by the engine with runtime boolean validations.
  - added `SHOW CONSTRAINT CONTRACTS` to inspect stored contracts separately from the compiled primitive constraints they generate.

Example - node contract:

```cypher
CREATE CONSTRAINT person_contract
FOR (n:Person)
REQUIRE {
  n.id IS UNIQUE
  n.name IS NOT NULL
  n.status IN ['active', 'inactive']
  (n.tenant, n.externalId) IS NODE KEY
  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
}
```

Example - relationship contract:

```cypher
CREATE CONSTRAINT works_at_contract
FOR ()-[r:WORKS_AT]-()
REQUIRE {
  r.id IS UNIQUE
  r.startedAt IS NOT NULL
  startNode(r) <> endNode(r)
  startNode(r).tenant = endNode(r).tenant
  r.hoursPerWeek > 0
}
```

- **Relationship cardinality and endpoint policy constraints (NornicDB extensions)**:
  - added directional relationship cardinality limits using `REQUIRE MAX COUNT <n>`.
  - added endpoint policy constraints so allowed start-label/end-label combinations can be enforced for a relationship type.

Example - directional cardinality:

```cypher
CREATE CONSTRAINT max_employers
FOR ()-[r:WORKS_AT]->()
REQUIRE MAX COUNT 3
```

### Changed

- **Cypher parser compatibility and DDL coverage**:
  - expanded ANTLR parser support for Neo4j 5-style `REQUIRE` forms, parenthesized property chains in key constraints, trailing `OPTIONS`, and `WITH EMBEDDING` syntax.
  - aligned parser-mode behavior so the same user-facing query shapes work consistently in both parser paths.

- **Traversal planning and query execution**:
  - added a generic relationship `ORDER BY ... LIMIT` planner that can seed traversal from the filtered end-node side using index top-k lookups.
  - added dedicated hot-path tracing for the new traversal planner and broadened routing/coverage for the translation-query family and adjacent query shapes.

- **Release and dependency maintenance**:
  - refreshed Go, UI, and workflow dependencies.
  - updated release/readme/docs content to reflect the current feature set and release flow.

### Fixed

- **Explicit transaction final-state semantics**:
  - fixed explicit transaction handling so relationship constraints are validated against the final committed state rather than intermediate statement state.
  - fixed transaction read-your-own-write behavior for relationship reads and relationship property updates in explicit transactions.

- **Large batch write amplification**:
  - fixed `UNWIND`/bulk write paths that could fail with oversized transactions when a node was created and then updated again in the same transaction.
  - create-then-update mutations now collapse into a single pending create operation where possible.

- **Relationship query ordering and stability**:
  - fixed `ORDER BY` handling for relationship query results when ordering by aliased return expressions or node properties.
  - fixed MATCH-clause `ORDER BY` extraction so labels and relationship types such as `:Order` and `:ORDERS` no longer interfere with sorting.
  - restored deterministic ordering for Northwind-style relationship aggregation fast paths.

### Tests

- Added and expanded regression coverage for:
  - block-style constraint contracts and `SHOW CONSTRAINT CONTRACTS`
  - relationship cardinality and endpoint policy constraints
  - explicit transaction relationship final-state validation
  - batch `UNWIND` create/update collapse behavior
  - exact translation query-family parsing, routing, e2e behavior, and traversal hot-path assertions
  - relationship aggregation ordering and `ORDER BY` clause parsing around `Order`/`ORDERS` tokens

### Technical Details

- **Range covered**: `v1.0.38..HEAD`
- **Commits in range**: 19 (non-merge)
- **Repository delta**: 85 files changed, +7,997 / -2,854 lines
- **Primary focus areas**: schema contracts and relationship policy constraints, parser compatibility, explicit transaction correctness, bulk-write stability, and generic relationship traversal/query hot paths.

## [v1.0.38] "Phantom" - 2026-04-02

### Added

- **Neo4j relationship constraint parity**:
  - all constraint families now work on both nodes and relationships: uniqueness, existence (IS NOT NULL), property type (IS :: TYPE), and key constraints (IS RELATIONSHIP KEY).
  - relationship constraints use the `FOR ()-[r:TYPE]-()` target pattern, matching Neo4j 5.x syntax.
  - uniqueness and key constraints on relationships automatically create owned backing indexes that are dropped when the constraint is dropped.

Example — relationship uniqueness:

```cypher
CREATE CONSTRAINT transfer_ref_unique IF NOT EXISTS
FOR ()-[r:TRANSFERRED]-()
REQUIRE r.reference_id IS UNIQUE
```

Example — relationship existence:

```cypher
CREATE CONSTRAINT transferred_amount_exists IF NOT EXISTS
FOR ()-[r:TRANSFERRED]-()
REQUIRE r.amount IS NOT NULL
```

Example — relationship property type:

```cypher
CREATE CONSTRAINT transferred_amount_type IF NOT EXISTS
FOR ()-[r:TRANSFERRED]-()
REQUIRE r.amount IS :: FLOAT
```

Example — relationship key (composite uniqueness + existence):

```cypher
CREATE CONSTRAINT works_at_key IF NOT EXISTS
FOR ()-[r:WORKS_AT]-()
REQUIRE (r.employee_id, r.department) IS RELATIONSHIP KEY
```

- **Temporal no-overlap constraints on relationships (NornicDB extension)**:
  - extends the existing node temporal no-overlap constraint to relationships.
  - supports a composite endpoint-pair form where the last two properties are always the time range and any preceding properties form the grouping key.

Example — 3-property form (single grouping key):

```cypher
CREATE CONSTRAINT employment_temporal IF NOT EXISTS
FOR ()-[r:WORKS_AT]-()
REQUIRE (r.employee_id, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP
```

Example — 4-property form (composite grouping key):

```cypher
CREATE CONSTRAINT assignment_temporal IF NOT EXISTS
FOR ()-[r:ASSIGNED_TO]-()
REQUIRE (r.employee_id, r.project_id, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP
```

- **Domain/enum constraints (NornicDB extension)**:
  - new constraint type that restricts a property to a fixed set of allowed values.
  - works on both nodes and relationships.
  - domain constraints with different allowed-value sets on the same schema are treated as conflicting.

Example — node domain:

```cypher
CREATE CONSTRAINT person_status_domain IF NOT EXISTS
FOR (n:Person)
REQUIRE n.status IN ['active', 'inactive', 'suspended']
```

Example — relationship domain:

```cypher
CREATE CONSTRAINT works_at_role_domain IF NOT EXISTS
FOR ()-[r:WORKS_AT]-()
REQUIRE r.role IN ['engineer', 'manager', 'director']
```

- **CMEK/HSM-backed at-rest encryption**:
  - added customer-managed encryption key (CMEK) and HSM-backed at-rest encryption for the database engine.

- **macOS installer service lifecycle**:
  - added macOS packager workflow with automatic release upload.
  - separated macOS packager from Docker CD pipeline.

### Changed

- **`IF NOT EXISTS` semantics for `CREATE CONSTRAINT`**:
  - exact-duplicate `CREATE CONSTRAINT` now returns an error unless `IF NOT EXISTS` is specified, matching Neo4j behavior. Previously, duplicates were silently idempotent.

- **`SHOW INDEXES` and `SHOW CONSTRAINTS` relationship awareness**:
  - `SHOW INDEXES` now reports `entityType` (NODE or RELATIONSHIP) and `owningConstraint` for backing indexes created by relationship uniqueness/key constraints.
  - `SHOW CONSTRAINTS` now reports `entityType` as RELATIONSHIP for relationship-scoped constraints.

- **Branding and documentation**:
  - abstracted Mimir-specific references to NornicDB-focused language across docs, source comments, test files, and configuration.
  - updated system-design documentation with WITH EMBEDDING support and correct embedding defaults (8192 chunk size, bge-m3 model).
  - updated user-facing documentation for all new constraint types with syntax examples.

### Fixed

- **Embedding property namespace pollution**:
  - removed special-case routing that intercepted the `embedding` property name during SET/GET operations. The `embedding` property is now treated as a regular user property. Users can name their embedding fields anything and create vector indexes for them.

- **ORDER BY on returned node properties**:
  - stabilized ORDER BY when sorting on properties of returned nodes (e.g., `ORDER BY t.createdAt` when only `t` is returned).
  - fixed row-sorting to resolve `var.prop` against map-backed node values the same way expression evaluation does.

- **Constraint enforcement namespace filtering**:
  - fixed empty-namespace prefix filtering that caused unnamespaced edge IDs to be skipped during constraint enforcement scans.
  - fixed cross-namespace leak in transaction-level temporal constraint enforcement where pending edges from other namespaces could cause false violations.

- **Owned backing index metadata**:
  - fixed `SHOW INDEXES` to carry `entityType` and `owningConstraint` through relationship-scoped backing indexes instead of hardcoding NODE and nil.

- **macOS installer service lifecycle**:
  - fixed start/stop/restart controls to behave correctly for the macOS service.

- **Graph API hardening**:
  - hardened graph MVCC/diff handlers, aligned graph endpoint contracts, and improved validation and error mapping for the graph API.

### Security

- Updated dependencies to resolve vulnerability scan findings.
- Updated example code to pass security scanning.

### Tests

- Added and expanded regression coverage for:
  - all relationship constraint families (uniqueness, existence, property type, key) with DDL, creation-time validation, and write-path enforcement tests
  - relationship temporal no-overlap constraints (3-property and 4-property forms)
  - domain/enum constraints on nodes and relationships with conflict detection
  - `IF NOT EXISTS` idempotency semantics for all constraint types
  - embedding property namespace pollution removal
  - ORDER BY stability on returned node properties
  - CMEK/HSM encryption compliance
  - expanded unit test coverage across uncovered packages

### Technical Details

- **Range covered**: `v1.0.37..HEAD`
- **Commits in range**: 30 (non-merge)
- **Repository delta**: 180 files changed, +15,077 / -2,619 lines
- **Primary focus areas**: Neo4j relationship constraint parity, NornicDB constraint extensions (temporal no-overlap, domain/enum), CMEK encryption, embedding property namespace cleanup, ORDER BY stability, and macOS packaging.

## [v1.0.37] "The Walker" - 2026-04-01

### Added

- **Inline mutation-time embedding in Cypher (`WITH EMBEDDING`)**:
  - added support for `WITH EMBEDDING` on write statements so node mutations and managed embedding writes happen in the same transaction.
  - supports common mutation shapes including `CREATE`, `MERGE ... SET`, `MATCH ... SET`, and `UNWIND` write pipelines.

Example usage:

```cypher
CREATE (d:Doc {id: 'd1', content: 'hello world'})
WITH EMBEDDING
RETURN count(d) AS created
```

```cypher
UNWIND $rows AS row
MERGE (d:Doc {id: row.id})
SET d.content = row.content
WITH EMBEDDING
RETURN count(d) AS upserted
```

- **Shared embedding utility package**:
  - added `pkg/embeddingutil` to centralize canonical embedding text construction, metadata-key detection, invalidation, and managed embedding payload shaping.

### Changed

- **Cypher traversal and CALL-tail performance/coverage**:
  - generalized and hardened hot paths for benchmark-shaped `CALL ... YIELD` + traversal queries, `UNWIND` merge pipelines, and variable-length traversal aggregation patterns.
  - expanded E2E and benchmark coverage for real query shapes used in traversal/vector workloads.

- **Async and eventual consistency behavior**:
  - expanded async engine eventual-create eligibility and completed additional read-your-own-write (RYOW) coverage.
  - aligned async HTTP behavior and transport/docs guidance for consistency.

- **Documentation updates for user-facing query behavior**:
  - added/updated user docs for mutation-time embedding and vector search usage, including practical `WITH EMBEDDING` examples and caveats.

### Fixed

- **Cypher correctness across complex pipelines**:
  - fixed `WITH DISTINCT` projection behavior in pipeline routing.
  - fixed `CALL` argument parsing for tail function expressions.
  - fixed `YIELD ... WHERE elementId(...)` handling to preserve actual yielded entities.
  - fixed path aggregate semantics for variable-length `MATCH` and related traversal aggregate edge cases.

- **Storage/server stability under mixed write + background load**:
  - fixed scenarios where deleted nodes could reappear during index builds.
  - improved MVCC head rebuild batching and prioritized transaction hot paths over embedding worker pressure.
  - fixed auth-disabled mode write-gating behavior to avoid incorrect RBAC blocking when authentication is off.

### Tests

- Added and expanded regression coverage for:
  - `WITH EMBEDDING` mutation patterns (`CREATE`, multi-create, `MERGE/SET`, `MATCH/SET`, `UNWIND`, explicit transactions)
  - CALL-tail traversal fast paths and benchmark shape handling
  - async engine RYOW/eventual-create behavior and MVCC conflict-sensitive paths
  - embedding invalidation and shared utility behavior across Cypher/DB/worker paths

### Technical Details

- **Range covered**: `v1.0.36..HEAD`
- **Commits in range**: 26 (non-merge)
- **Repository delta**: 59 files changed, +7,156 / -726 lines
- **Primary focus areas**: inline mutation-time embeddings, Cypher traversal/call hot paths, async consistency hardening, and operational stability fixes.

## [v1.0.36] "Gold on the Cieling" - 2026-03-30

### Added

- **Deterministic token-aware text chunking infrastructure**:
  - added `pkg/textchunk` as a reusable deterministic chunk splitter driven by caller-supplied token counters instead of heuristic token estimation.
  - added exact tokenizer-backed chunking support to the local GGUF/llama embedding path so local models can split oversized text using the real model vocabulary limits.

### Changed

- **Embedding and semantic-search query chunking architecture**:
  - widened embedding/query interfaces to require provider-owned `ChunkText(...)` behavior so Cypher, HTTP search, MCP, Heimdall, gRPC, and DB-level query embedding all route chunking through the active embedder instead of a shared heuristic utility.
  - added DB query-chunk helper paths for global and per-database embedders so query chunking stays aligned with the same model configuration used for query embeddings.

- **Documentation, publishing, and quick-start guidance**:
  - refreshed docs navigation, README and deployment/search/transaction guidance, and updated quick-start instructions to better match current runtime behavior.
  - enabled and aligned GitHub Pages publishing behavior and corrected documentation link/navigation issues discovered during the doc refresh.

- **Benchmarking and build maintenance**:
  - updated benchmark assets and related build assumptions used during performance comparison work.
  - refreshed release/build housekeeping associated with the current toolchain and packaging flow.

### Fixed

- **Tokenizer overflow handling for long embedding inputs**:
  - fixed long-query and long-document embedding paths to stop relying on approximate token counting, preventing deterministic tokenizer overflows and repeated retry loops for text that exceeded the true model token cap.
  - removed the old heuristic chunker entirely and migrated affected call sites and tests to deterministic chunking behavior.

- **Bolt hot-path logging overhead**:
  - removed unconditional success-path logging from the Bolt query hot path so normal successful `RUN` execution no longer pays avoidable logging overhead.
  - preserved error-path timing/log visibility with regression coverage for the default-silent success behavior.

- **Background worker write-back observability**:
  - added auditing information for background workers that persist changes back to storage so asynchronous maintenance/update flows are easier to inspect operationally.

### Tests

- Added and updated regression coverage for:
  - deterministic chunk splitting limits, overlap behavior, and embedder-owned chunking across Cypher, DB, MCP, Heimdall, server, gRPC, Bolt, and embed-cache test doubles
  - tokenizer-aware long-query and long-document embedding behavior, including dimension-mismatch and chunk-cap branch coverage
  - Bolt hot-path logging behavior so successful executions remain silent by default while failures still log

### Technical Details

- **Range covered**: `v1.0.35..HEAD`
- **Commits in range**: 14 (non-merge)
- **Repository delta**: 71 files changed, +2,889 / -2,472 lines
- **Primary focus areas**: deterministic token-aware embedding chunking, Bolt hot-path overhead reduction, documentation/publishing updates, and release/benchmark maintenance.

## [v1.0.35] "Clean Up Before She Comes" - 2026-03-27

### Changed

- **llama.cpp roll-forward and runtime alignment**:
  - upgraded the vendored llama.cpp integration and shipped local llama artifacts to `b8547`.
  - synchronized version pins across build scripts, packaged libraries, and Docker builder images so local, CI, and container builds use the same llama baseline.

- **Container and CI dependency maintenance**:
  - rolled forward Docker build dependencies and CI image references used for CPU, CUDA, Vulkan, and Metal packaging.
  - updated the bundled Heimdall and BGE image variants so embeddings are enabled by default in the batteries-included deployment paths.

- **UI and build-tool dependency refresh**:
  - updated frontend/package dependencies and related build configuration used by the shipped UI bundle.
  - refreshed release/build assumptions tied to current toolchain behavior so maintenance packaging stays reproducible.

### Fixed

- **Bolt autocommit database executor reuse**:
  - fixed the Bolt multi-database autocommit path to reuse cached database-scoped executors instead of rebuilding a fresh Cypher executor on each `RUN`.
  - preserved auth-forwarded remote routing behavior by keeping auth-scoped database executor resolution uncached.

- **llama.cpp upgrade compatibility on macOS and Heimdall generation paths**:
  - fixed the local llama generation context initialization used by Heimdall after the llama.cpp upgrade, eliminating a macOS CGO crash during generation-model loading.
  - added generation-context normalization so native model initialization stays within safe runtime bounds after dependency roll-forwards.

- **llama build portability and packaging behavior**:
  - fixed `scripts/build-llama.sh` to remain compatible with macOS Bash 3.2.
  - updated the ARM64 Metal Heimdall image to prefer locally available models before falling back to network downloads, improving reproducibility in restricted build environments.

### Tests

- Added and updated regression coverage for:
  - Bolt database-scoped executor cache reuse for non-auth paths and cache bypass for auth-forwarded routing
  - llama generation context resolution and safe initialization behavior after the llama.cpp upgrade
  - Heimdall CGO generation-model loading on the upgraded llama runtime

### Technical Details

- **Range covered**: `v1.0.34..HEAD`
- **Commits in range**: 5 (non-merge)
- **Repository delta**: 44 files changed, +1,635 / -1,984 lines
- **Primary focus areas**: dependency roll-forwards, llama.cpp packaging/runtime compatibility, and release engineering maintenance.

## [v1.0.34] "Hello, Operator" - 2026-03-25

### Added

- **Operator MVCC lifecycle control plane**:
  - added a full admin-facing MVCC lifecycle manager with reader tracking, safe-floor computation, debt planning, fenced prune apply, pressure bands, emergency behavior, and runtime cadence changes.
  - added admin HTTP endpoints to inspect lifecycle state, pause or resume automatic work, trigger prune-now, change schedule intervals live, and inspect top debt keys.

- **End-to-end lifecycle admin UI**:
  - added a dedicated `MVCC Lifecycle` page under `Security` so operators can manage lifecycle behavior without direct API calls.
  - added confirmation-gated controls for pause, resume, prune-now, and schedule changes, plus views for pressure, debt, readers, per-namespace summaries, and short-window rollups.

- **Lifecycle architecture and operator documentation**:
  - added architecture documentation for MVCC lifecycle and background work behavior.
  - added a user guide for lifecycle API and UI workflows, including interval tuning guidance for mixed, churn-heavy, and manual-only operating modes.

### Changed

- **Storage engine lifecycle integration**:
  - propagated lifecycle support through Badger, async, WAL, namespaced, and multi-database wrappers so MVCC controls remain available across wrapped deployments.
  - wired database runtime startup to initialize lifecycle management from config and expose status through DB admin methods and Heimdall metrics.

- **Runtime lifecycle configurability**:
  - added config support for enabling lifecycle management, setting the cycle interval, bounding snapshot lifetime, capping pathological chain growth, and debouncing write-triggered embedding work.
  - lifecycle cadence can now be changed at runtime, including switching a database into manual-only mode with `0s`.

- **Operator visibility and protection signals**:
  - transaction responses now surface MVCC pressure warnings so clients can see when pinned history is building.
  - lifecycle metrics now expose pressure, debt, readers, rollups, and debt hotspots for easier diagnosis before maintenance pressure becomes an incident.

### Fixed

- **Pressure-driven snapshot handling**:
  - fixed explicit transaction/session behavior so MVCC snapshot expiration is surfaced consistently as a retryable transient lifecycle error instead of leaking inconsistent terminal behavior.
  - added audit logging for forced snapshot expiration so pressure-driven cancellations are visible in operator trails.

- **Background indexing and lifecycle coordination under write load**:
  - fixed mutation-triggered search indexing to debounce queued work instead of amplifying one background execution path per write burst.
  - fixed search-service build completion signaling to avoid duplicate-close races during concurrent build and shutdown paths.

- **MVCC churn retention behavior**:
  - fixed lifecycle prune behavior to stay bounded under repeated update/delete/recreate churn while preserving current visible heads and retained-floor semantics.

### Tests

- Added and expanded regression coverage for:
  - lifecycle manager planning, pressure hysteresis, fairness scheduling, emergency behavior, and metrics rollups
  - lifecycle delegation through storage wrappers and DB admin entry points
  - transaction admission, graceful snapshot cancellation, and hard-expiration behavior under MVCC pressure
  - server lifecycle handlers, pressure warnings, and audit logging for expired snapshots
  - search-service debounce behavior and idempotent build completion signaling
  - real-engine churn pruning bounds and tombstone compaction behavior

### Technical Details

- **Range covered**: `5ae74b0..HEAD` (from the commit that recorded the v1.0.33 changelog entry to current `main` head)
- **Commits in range**: 1 (non-merge)
- **Repository delta**: 57 files changed, +7,643 / -690 lines
- **Primary focus areas**: MVCC lifecycle control, operator visibility, runtime scheduling, pressure-aware snapshot handling, and safer background maintenance behavior.

## [v1.0.33] - 2026-03-25

### Added

- **Live-data regression coverage for `MATCH ... LIMIT` hot paths**:
  - added integration/benchmark coverage for cache-busted simple read shapes (varying `LIMIT`) on real datasets.
  - added strict wrapper-delegation tests to ensure streaming and prefix-streaming capabilities are preserved across engine wrappers.

### Changed

- **Simple `MATCH ... RETURN ... LIMIT` routing**:
  - added/expanded dedicated fast-path handling for simple node-return limit queries and alias variants.
  - added explicit hot-path tracing hooks for these shapes to make routing behavior observable in tests.

- **Wrapper capability forwarding for performance-critical interfaces**:
  - propagated streaming interfaces through multi-database size-tracking wrappers so read paths can terminate early instead of materializing full scans.
  - propagated prefix-stream and label-ID lookup interfaces through WAL wrappers to preserve optimized behavior in wrapped deployments.

- **Label-constrained `LIMIT` execution strategy**:
  - changed label-only `LIMIT` collection to prefer label-ID iteration + targeted node fetch instead of full label materialization before applying `LIMIT`.
  - this improves latency stability for both sparse-label and dense-label reads.

- **Vector query concurrency behavior**:
  - reduced lock contention in vector query specification paths under high-concurrency workloads.

### Fixed

- **`MATCH (n) RETURN n LIMIT K` latency regression in multi-database stacks**:
  - fixed a routing/capability loss where wrappers could drop optimized streaming behavior, causing full-scan-like latency even with low `LIMIT`.
  - restored millisecond-range latency for cache-busted simple-limit query shapes after warm-up.

### Tests

- Added/updated regression coverage for:
  - simple `MATCH ... LIMIT` fast-path parsing/routing and trace visibility
  - label-limited node collection strategy (`label + LIMIT`)
  - namespaced/prefix streaming delegation through async, WAL, and size-tracking wrappers
  - real-data benchmark probes for cache-busted limit-shape latency.

### Technical Details

- **Range covered**: `f7a92fc..HEAD` (from latest changelog baseline to current `main` head)
- **Commits in range**: 2 (non-merge)
- **Repository delta**: 20 files changed, +1,377 / -24 lines
- **Primary focus areas**: Cypher read hot-path latency, storage-wrapper capability propagation, and high-concurrency contention reduction.

## [v1.0.32] - 2026-03-24

### Added

- **Cross-protocol access logging by default**:
  - added Bolt and gRPC request/access logging to match HTTP visibility behavior.
  - improves operational parity and troubleshooting across all exposed protocols.

- **Hot-path query cookbook (user-facing performance guide)**:
  - added a generic, domain-neutral cookbook of optimized graph query shapes and anti-pattern guidance.
  - includes practical patterns for key lookups, relationship traversals, review/queue-style scans, batched deletes, and pagination.

- **PROFILE diagnostics for index planning decisions**:
  - expanded explain/profile metadata to show index usage status and rejection risk reasons.
  - helps operators understand why a query used (or skipped) an index.

### Changed

- **Cypher planner/executor hot paths**:
  - direct ID seek planning for parameterized predicates (`id(...) = $x`, `elementId(...) = $x`) to avoid label scans.
  - improved OR/IN index planning (`a.p1 = $x OR a.p2 = $x`, `a.prop IN $keys`) with branch-based index usage and deduplication.
  - null-normalized predicate handling for better index visibility (coalesce-style boolean filters).
  - index-backed top-N execution for `ORDER BY ... LIMIT` when eligible, avoiding full materialization/sort.
  - traversal start-node pruning/index usage improved for relationship-match shapes.
  - streaming/batched `DETACH DELETE` execution improved for bounded-memory cleanup and large delete sets.

- **`/tx/commit` execution overhead reduction**:
  - optimized single-statement implicit transaction path to reduce per-request overhead in the common case.
  - tightened cache-key determinism for parameterized query shapes to improve plan/cache reuse.

- **Embedding queue scheduling behavior**:
  - introduced debounce/throttling behavior on write-triggered embedding queue work to reduce write-path contention during active traffic.
  - pending-index refresh scans now run with less interference against foreground writes.

- **Write-path lock contention reduction**:
  - narrowed lock scope in high-write code paths so longer operations perform less work while holding shared/global locks.

### Fixed

- **Neo4j-compatible conflict error semantics at API layer**:
  - transaction conflict/deadlock-style commit failures are now surfaced as Neo4j-compatible transient errors (retryable classification) instead of syntax errors.
  - aligns client retry behavior with expected Neo4j semantics.

### Tests

- Added/updated regression coverage for:
  - parameterized ID seek planning and index-backed IN/OR-IN query shapes.
  - migration-style exact query-shape execution paths and cleanup deletes.
  - transaction error mapping for transient conflict/deadlock responses.
  - delete-streaming and routing correctness.

### Technical Details

- **Range covered**: `v1.0.31..HEAD`
- **Commits in range**: 10 (non-merge)
- **Repository delta**: 32 files changed, +3,906 / -269 lines
- **Primary focus areas**: Cypher hot-path execution, transaction semantics compatibility, write contention reduction, and operator observability.

## [v1.0.31] - 2026-03-24

### Fixed

- **Safe Unicode Cypher syntax normalization**:
  - expanded Cypher ingress normalization beyond Unicode arrows to safely normalize syntax confusables including dash variants, fullwidth structural punctuation, unusual whitespace, and selected zero-width separators
  - restricted normalization to query syntax only so string literals, comments, and backticked identifiers are preserved exactly as written
  - improved pasted-query compatibility for Neo4j-style Cypher copied from chat apps, documents, and rich-text sources.
- **Compound `MATCH ... OPTIONAL MATCH ... RETURN` result modifiers**:
  - fixed the specialized joined-row execution path to apply `ORDER BY`, `SKIP`, and `LIMIT` instead of returning storage/join order
  - restored deterministic aliased ordering for `OPTIONAL MATCH` retrieval queries and aligned the executor with Neo4j-compatible result semantics.

### Tests

- Added exact regression coverage for:
  - safe normalization of Unicode Cypher syntax confusables outside literals/comments/backticks
  - normalized `MERGE` and `OPTIONAL MATCH` execution through the public executor path
  - deterministic ordered results for mirrored-graph `OPTIONAL MATCH` retrieval queries
  - end-to-end parity for ASCII and Unicode-arrow mirrored `Section` hierarchy queries.

## [v1.0.30] - 2026-03-23

### Fixed

- **Unicode Arrow Parsing**:
  - fixed pasted Cypher relationship arrows using Unicode direction characters (`→` / `←`) by normalizing them to standard Cypher `->` / `<-` tokens before routing and parsing

## [v1.0.29] - 2026-03-23

### Added

- **Optimistic mutation metadata for async CREATE paths**:
  - added Cypher-side optimistic metadata tracking for created node/relationship IDs
  - async CREATE fast-path now records created IDs up-front for client response usage.

### Fixed

- **Cypher mutation and grouped-query compatibility for multi-entity update flows**:
  - fixed chained `MATCH ... WHERE ... MATCH ... SET ... RETURN` handling for update queries that join source/target entity sets
  - fixed multi-MATCH WHERE extraction to use the correct terminal WHERE before RETURN, preventing false `expected multiple MATCH clauses` errors
  - fixed `SET ... RETURN count(...)` aggregation semantics so update-count projections return deterministic values (`count(t)` now behaves correctly in mutation returns)
  - fixed chained MATCH normalization so queries containing `OPTIONAL MATCH` are not rewritten into incompatible required-MATCH forms
  - fixed joined-row aggregation handling for `COLLECT(...)` with non-aggregate return columns, preserving grouped-per-key result semantics
  - improved MATCH WHERE extraction boundaries in mixed clause pipelines so optional-tail clauses do not leak into WHERE parsing.
- **Correlated CALL/UNION correctness and performance**:
  - restored correct UNION subquery result behavior for correlated execution paths
  - reduced fixed overhead in correlated query routing/execution to improve hot-path latency.
- **Correlated CALL subquery write semantics (`WITH ... CREATE/MERGE/...`)**:
  - fixed correlated `CALL { WITH ... }` execution so imported node variables bind correctly across `CREATE`/`MERGE` write branches
  - fixed write-tail fallback rewriting to avoid dropping side effects for valid `MATCH ... CALL { WITH p MERGE ... }` shapes
  - fixed `WITH`-import boundary parsing so write clauses after `WITH` are preserved and executed.
- **Bolt parity for text-based vector queries**:
  - fixed Bolt database-scoped executor wiring so `db.index.vector.queryNodes(..., $text)` works when an embedder is configured
  - aligned Bolt behavior with HTTP/GraphQL execution paths for string query input.
- **Async transaction response metadata shape**:
  - surfaced optimistic metadata in transaction responses alongside receipt metadata when available.
- **Mutation stats and deduplication correctness**:
  - fixed DELETE/DETACH DELETE mutation stats under repeated OPTIONAL MATCH row expansion by deduplicating per-entity deletes
  - fixed branch regression where some SET/DELETE/CALL-IN-TRANSACTIONS paths returned nil projection values instead of expected results.
- **Mirrored graph retrieval compatibility**:
  - verified Neo4j-style mirrored `Section` save/query flows including `MERGE ... WITH ... MERGE` write shapes and `MATCH ... OPTIONAL MATCH ... RETURN n, r, p` retrieval.
- **Indexed OR-IN lookup path for key-list reads**:
  - added index-backed planning for predicates shaped like `propA IN $keys OR propB IN $keys` across alternate key fields
  - avoids full label scans for large key-list lookups and cleanup/read query patterns.

### Tests

- Added/updated regression and benchmark coverage for correlated UNION/call-subquery behavior and real-data execution profiles.
- Added regression coverage for:
  - Bolt DB-scoped executor embedder inheritance and string vector query execution
  - async CREATE fast-path optimistic ID metadata propagation.
  - correlated subquery create-or-update shape (`OPTIONAL MATCH + CALL { WITH ... UNION ... }`)
  - CALL subquery write-path regression cases (`WITH ... CREATE`, `WITH ... MERGE`)
  - delete deduplication under OPTIONAL MATCH row multiplication
  - parser/import handling for `WITH ... WHERE ...` correlated subquery clauses
  - exact `UNWIND + OPTIONAL MATCH + collect(CASE...)` key-lookup shape (per-key grouped rows and null-arm behavior)
  - `DETACH DELETE` with `WHERE elementId(...)` + `OPTIONAL MATCH` cleanup shape
  - OR-combined indexed `IN` predicate planning without scan fallback
  - mirrored Neo4j `Section` import/query shapes including chained `MERGE ... WITH ... MERGE` writes and Unicode-arrow `OPTIONAL MATCH` retrieval
  - exact mutation query shapes:
    - `MATCH ... WHERE ... OR (...) CREATE ... CREATE ... RETURN ...`
    - `MATCH ... WHERE ... OR (...) MATCH ... SET ... RETURN ...`
    - `MATCH ... WHERE elementId(...) SET ... RETURN count(...) AS updated`
  - fan-out and null-arm guard tests for OR-based creation filters.

### Technical Details

- **Range covered**: `v1.0.28..HEAD`
- **Commits in range**: 1 (non-merge)
- **Files changed in range**: 8
- **Primary focus areas**: correlated UNION subquery correctness and hot-path performance.
- **Additional staged delta (not including changelog edits)**: 9 files, +208 / -11
- **Additional staged delta (not including changelog edits)**: 17 files, +952 / -56

## [v1.0.28] - 2026-03-23

### Added

- **Vector query embedding cache for Cypher procedures**:
  - added executor-level embedding-result caching for `db.index.vector.queryNodes` / compatibility vector query paths when the query input is text
  - added in-flight de-duplication for concurrent identical embed requests so the same query text is embedded once and shared across waiters.
  - what this means: repeated semantic/vector query calls spend less time in embedding and create fewer duplicate embedding workloads under concurrent traffic.

### Changed

- **Correlated subquery execution optimizations**:
  - restored safe UNION fast paths in correlated `MATCH ... CALL { ... UNION ... } ...` execution with strict guards for write-safety and variable-dependency correctness
  - improved correlated seed extraction and batched lookup handling in subquery execution hot paths.
  - what this means: lower fixed-cost overhead for common correlated subquery/UNION shapes while preserving Neo4j-compatible semantics.
- **Query cache key normalization performance**:
  - replaced allocation-heavy whitespace normalization (`strings.Fields` join) with a single-pass compaction strategy.
  - what this means: fewer cache-key allocations and reduced GC pressure on read-heavy workloads.
- **Traversal optimization safety hardening**:
  - strengthened fallback start-node pruning behavior to fail open and preserve deterministic traversal semantics for chained/complex patterns.
  - what this means: traversal optimizations remain active without sacrificing correctness on multi-segment graph patterns.

### Fixed

- **UNION/subquery fixed-cost overhead in hot Cypher paths**:
  - reduced allocation-heavy row dedupe and subquery processing overhead for CALL/UNION shapes.
- **Correlated CALL + UNION semantics in mixed query shapes**:
  - fixed guarded fast-path routing to keep duplicate-row and chained-traversal result behavior consistent while optimized execution is enabled.

### Tests

- Added/expanded benchmark and regression coverage for:
  - correlated subquery + UNION fixed-cost cache-miss profiling
  - real-data Cypher/fabric-style e2e benchmark harnesses
  - traversal optional/WHERE pruning safety behavior
  - vector procedure caching behavior and compatibility paths.

### Technical Details

- **Range covered**: `v1.0.27..HEAD`
- **Commits in range**: 1 (non-merge)
- **Repository delta**: 13 files changed, +1,754 / -86 lines
- **Non-test surface changed**: 8 files
- **Primary focus areas**: Cypher hot-path latency, allocation/GC reduction, correlated subquery/UNION execution, and vector query embed caching.

## [v1.0.27] - 2026-03-22

### Added

- **Indexed temporal `AS OF` lookups and current-version tracking**:
  - added storage-backed temporal indexes keyed by namespace, label, temporal key, and validity window so point-in-time lookups no longer depend on full label scans
  - added current-pointer tracking for open/current temporal intervals and wired rebuild/prune maintenance through DB admin flows.
  - what this means: temporal queries scale with ordered index lookups instead of broad scans, and restore/startup flows can rebuild temporal state deterministically.
- **MVCC historical reads and retention controls**:
  - added committed node/edge version records, persisted MVCC head metadata, snapshot-visible reads, and wrapper delegation for namespaced, WAL, and async engines
  - added retention policy controls, pruning, retained-floor anchoring, and historical-read maintenance APIs.
  - what this means: NornicDB now supports explicit historical graph reads with predictable retention behavior instead of only current-state inspection.
- **Closure-based transaction helper API**:
  - added `DB.Begin`, `DB.Update`, and `DB.View` wrappers for closure-scoped transaction execution
  - exported `Transaction` as the public closure-facing transaction type.
  - what this means: callers can use transaction-scoped closures without manually juggling rollback/commit boilerplate.

### Changed

- **Storage transaction isolation model**:
  - transactions now anchor reads to a begin-time MVCC snapshot and keep point reads, label scans, and graph visibility checks pinned to that snapshot
  - commit-time validation now checks node, edge, endpoint, and adjacency races against the transaction snapshot.
  - what this means: storage transactions now provide standard Snapshot Isolation semantics rather than best-effort read-your-writes only behavior.
- **Cypher and MCP write-path stability**:
  - compound `MATCH ... CREATE` execution now reuses the standard single-clause MATCH binding path for safe query shapes, while preserving special post-filter handling for `NOT (a)-[:TYPE]->(b)` modifiers
  - MCP relationship/task mutations now retry bounded snapshot-conflict failures instead of surfacing transient storage conflicts directly.
  - what this means: common ID-targeted relationship creation queries are more reliable under load without regressing migration-style anti-relationship filters.
- **Temporal/search interaction**:
  - search indexing and rebuild flows now treat historical temporal versions as non-searchable and keep indexes current-only by default
  - temporal overlap validation now uses indexed predecessor/successor checks where supported.
  - what this means: historical state no longer pollutes current search results, and temporal writes avoid increasingly expensive validation scans.
- **Operations and configuration surface**:
  - added MVCC retention knobs to config, environment variables, and sample YAML
  - macOS installer defaults and first-run wizard presets now keep search reranking disabled unless the user explicitly chooses the advanced AI setup
  - clarified async-write consistency wording and documented retained-floor/MRS behavior.
  - what this means: operators have explicit controls and clearer expectations for history depth, pruning, and eventual-consistency modes.

### Fixed

- **Historical lookup performance cliffs**:
  - fixed sparse post-prune historical lookups by persisting a retained-floor anchor in MVCC head metadata.
- **Conflict normalization and retryability**:
  - fixed lower-level Badger conflict leakage by normalizing commit conflicts to `ErrConflict` with clearer concurrent-modification messages.
- **Compound MATCH...CREATE query-shape regressions**:
  - fixed comma-separated `MATCH (a), (b) WHERE elementId(...) ... CREATE ...` relationship creation so compound CREATE blocks reuse correct MATCH bindings
  - fixed the related regression where migration-style `AND NOT (o)-[:TRANSLATES_TO]->(t)` filters were bypassed by the single-clause fast path.
- **Graph-consistent concurrent delete behavior**:
  - fixed transaction validation so node deletes and adjacent edge changes cannot commit into a dangling-edge state across concurrent snapshots.
- **MVCC endpoint validation fallback behavior**:
  - fixed transaction edge creation/commit validation to accept readable endpoint nodes even when MVCC head metadata is temporarily missing, instead of incorrectly rejecting valid edges as dangling
  - fixed the associated transaction read fallback so missing-head recovery uses the transaction's anchored Badger snapshot rather than the live engine state, preserving Snapshot Isolation.
- **Startup/restore maintenance reliability**:
  - fixed temporal rebuild/search maintenance ordering and added explicit MVCC head rebuild/bootstrap flows for current stores.
- **Namespacing and shutdown hardening**:
  - fixed duplicate namespace prefixing in transaction and namespaced storage wrappers by making node/edge prefix helpers idempotent
  - fixed Badger `DB Closed` panics to return `ErrStorageClosed` and suppressed benign shutdown-time search indexing errors after database close/cancel.
- **HNSW runtime transition tombstone leakage**:
  - fixed HNSW result assembly to exclude tombstoned candidates that can survive in the search candidate heap after a delete during runtime strategy transition replay.
- **Plugin test isolation**:
  - fixed Heimdall plugin loader tests by resetting the global subsystem manager between test cases so plugin registrations do not leak across subtests.

### Tests

- Added/expanded regression and benchmark coverage for:
  - indexed temporal `AS OF` lookups, temporal overlap validation, rebuilds, and pruning
  - MVCC visibility, head rebuilds, pruning, retained-floor behavior, and search invariance smoke tests
  - snapshot isolation semantics including read-your-writes, repeatable label scans, write-write conflicts, edge/node delete races, snapshot-consistent edge traversal, write skew, and contention aborts
  - closure-based transaction retries and concurrent counter increments through `DB.Update()`
  - compound `MATCH ... CREATE` elementId relationship creation, migration `NOT` relationship filters, missing-MVCC-head edge creation fallback, snapshot-safe missing-head reads, shutdown hardening, namespaced prefix idempotence, plugin loader isolation, and deleted-entrypoint HNSW search filtering.

### Documentation

- Added/updated documentation for:
  - historical reads, MVCC retention, pruning guarantees, startup/recovery behavior, and retained-floor semantics
  - storage transaction isolation guarantees and feature parity language
  - temporal query usage, serialization expectations, and operational configuration examples.

### Technical Details

- **Range covered**: `v1.0.26..HEAD`
- **Commits in range**: 4 (non-merge)
- **Repository delta**: 47 files changed, +6,819 / -362 lines
- **Non-test surface changed**: 35 files
- **Primary focus areas**: indexed temporal lookups, MVCC historical storage, Snapshot Isolation correctness, and retry-friendly transaction ergonomics.

## [v1.0.26] - 2026-03-20

### Changed

- **Cypher mutation pipeline compatibility**:
  - generalized execution handling for complex mutation chains combining `UNWIND`, `MERGE`, `SET`, `OPTIONAL MATCH`, `WITH`, and `WHERE`
  - improved clause sequencing reliability for multi-stage write statements with intermediate projections.
- **Proxy/base-path runtime behavior**:
  - restored and hardened proxied UI/base-path asset loading behavior for containerized deployments
  - improved path normalization to reduce route/asset resolution drift behind reverse proxies.
- **Managed vector/query execution safeguards**:
  - tightened vector/search-related persistence and serialization guardrails for large payload handling
  - improved runtime safety defaults for high-volume decode/read paths.

### Fixed

- **Unique-key conflict handling in batched MERGE writes**:
  - fixed batched `UNWIND ... MERGE` paths to correctly reuse matching nodes during a statement, preventing false duplicate-create violations under unique constraints.
- **Mutation context propagation across chained clauses**:
  - fixed variable binding continuity so downstream relationship merges resolve correctly after intermediate `WITH` projections and optional matches.
- **Aggregate alias continuity across chained MATCH stages**:
  - fixed alias preservation in chained query stages to prevent dropped/incorrect projected values.
- **UI/path security hardening**:
  - fixed reflected path handling in UI routing flows to mitigate injection/open-redirect risk surfaces.
- **Storage path-safety hardening**:
  - fixed segment/file access validation in WAL-related paths to reduce traversal-style file access risk.
- **Dependency security updates**:
  - upgraded vulnerable transport/runtime dependencies to patched versions in security-sensitive protocol paths.

### Tests

- Added/expanded regression coverage for:
  - complex Cypher mutation shape permutations (`UNWIND` + chained `MERGE`/`OPTIONAL MATCH`/`WITH`/`WHERE`)
  - unique-constraint behavior under batched merge writes
  - UNWIND property substitution and merge-chain execution edge cases
  - proxy/base-path UI routing behavior
  - storage path validation and bounded message-pack decode limits.

### Documentation

- Added/updated policy and project documentation for patent/licensing posture clarity.

### Technical Details

- **Range covered**: `v1.0.25..HEAD`
- **Commits in range**: 14 (non-merge)
- **Repository delta**: 35 files changed, +1,760 / -91 lines
- **Non-test surface changed**: 25 files
- **Primary focus areas**: Cypher write-path correctness, constraint-safe batched merges, security hardening, and proxy deployment reliability.

## [v1.0.25] - 2026-03-20

### Changed

- **Correlated subquery execution reliability**:
  - generalized correlated `CALL { ... }` execution handling for broader valid clause combinations, including mixed `WITH`, `UNION`, and procedure-yield pipelines
  - improved execution consistency for multi-stage query pipelines that combine procedural and graph pattern clauses.
- **ID-based query execution path optimization**:
  - added direct ID-seek planning for simple `MATCH ... WHERE id(...) = ...` and `elementId(...) = ...` query shapes
  - reduced unnecessary scan behavior for high-frequency point-lookup patterns.
- **Container build base-image sourcing**:
  - updated Docker build variants to use mirrored/public base-image registries for common runtime/build dependencies
  - reduced susceptibility to upstream rate-limit failures during CI/CD image resolution.
- **Keyword-aware clause parsing consistency**:
  - migrated remaining clause-routing keyword detection paths away from raw string index checks to shared keyword helpers
  - improved robustness for mixed whitespace/newline formatting and reduced false keyword matches inside expression bodies.

### Fixed

- **`WITH` identifier substitution robustness**:
  - fixed identifier replacement behavior to avoid accidental token corruption in downstream clauses.
- **Empty-seed correlated subquery result shape**:
  - fixed no-seed correlated `CALL` paths to preserve projected column schemas rather than returning fallback/internal columns.
- **Node import projection in correlated subqueries**:
  - fixed node-variable import binding so property projections from imported variables resolve correctly in subquery bodies.
- **Canonical ID comparison normalization**:
  - fixed `id(...)` / `elementId(...)` comparison handling by normalizing canonical element-ID inputs to internal IDs before evaluation.
- **MERGE resolution under stale lookup conditions**:
  - fixed merge-path behavior to recover correctly when fast lookup candidates are stale or conflict with already-existing rows.
- **Cypher compatibility hardening**:
  - fixed edge-case compatibility issues in optional pattern matching and UNWIND map-property access paths.
- **Nested UNWIND parsing correctness**:
  - fixed double-UNWIND `AS` parsing offsets so nested list expansion queries return expected rows under complex clause chains.
- **COLLECT subquery WHERE rewrite stability**:
  - fixed COLLECT-subquery rewriting when a `WHERE` clause is already present, preventing malformed query text and empty result regressions.

### Tests

- Added regression coverage for:
  - correlated subquery execution variants (including `UNION` + procedure-yield + projection pipelines)
  - empty-seed correlated subquery schema behavior
  - node-import property projection correctness in subqueries
  - direct ID/elements-ID point-lookup planning paths
  - merge conflict/stale-lookup recovery behavior.
- Restored full `pkg/cypher` regression pass after parser/executor clause-detection updates, including complex nested UNWIND/list-expression cases and COLLECT-subquery branch coverage.

### Technical Details

- **Range covered**: `v1.0.24..HEAD`
- **Commits in range**: 10 (non-merge)
- **Repository delta**: 23 files changed, +1,768 / -291 lines
- **Non-test surface changed**: 17 files
- **Primary focus areas**: correlated subquery correctness, point-lookup performance, Cypher compatibility hardening, merge-path resiliency, and CI/CD container base-image reliability.

## [v1.0.24] - 2026-03-19

### Changed

- **CALL/YIELD pipeline execution**:
  - generalized post-`YIELD` clause handling so `MATCH`, `WITH`, `RETURN`, `ORDER BY`, `SKIP`, and `LIMIT` pipelines execute consistently after procedure calls
  - removed brittle fixed-shape assumptions and aligned handling with broader valid clause combinations.
- **Server test fixture reuse**:
  - refactored high-frequency server test paths to share fixtures through grouped subtests where isolation was not required
  - reduced repeated full server/bootstrap setup in hot test paths.
- **Test startup behavior**:
  - disabled external embedding initialization in shared server test setup to avoid unnecessary async retry work in generic tests.

### Fixed

- **CALL clause boundary parsing**:
  - fixed `YIELD` parsing to respect CALL-clause boundaries instead of matching by raw string position
  - resolved cases where downstream clauses were incorrectly parsed or skipped in multi-clause statements.
- **N-column YIELD projection correctness**:
  - fixed projection behavior so `YIELD` supports variable-width output columns without shape-specific fallbacks
  - resolved incorrect/empty result sets in valid procedure-followed query pipelines.
- **Concurrent metadata snapshot race in multi-database management**:
  - made database metadata snapshots lock-safe when listing and fetching database info
  - prevents race conditions between storage-size cache initialization and metadata reads under concurrent access.

### Tests

- Added regression coverage for:
  - procedure-call pipelines with trailing clause permutations
  - boundary-aware `YIELD` parsing in multi-clause statements
  - multi-column `YIELD` projection with downstream `MATCH`/`WITH`/`RETURN` flows.
- Added a dedicated race regression test for concurrent storage-size initialization vs database metadata listing.
- Consolidated selected multi-database and server branch tests into shared-fixture suites while preserving isolated setup for lifecycle-sensitive cases.

### Technical Details

- **Range covered**: `v1.0.23..HEAD`
- **Commits in range**: 3 (non-merge)
- **Repository delta**: 5 files changed, +806 / -108 lines
- **Primary focus areas**: procedure pipeline correctness, parser robustness, and generalized clause compatibility for Cypher execution.

## [Earlier Releases Summary] - up to v1.0.23

This condensed section summarizes user-facing progress from all releases prior to the latest five entries.

### Highlights

- **Composite/Fabric capability matured significantly**:
  - introduced and hardened multi-database/composite execution across HTTP and Bolt
  - improved `USE`/subquery planning and execution behavior
  - strengthened remote constituent connectivity and auth-aware routing.
- **Cypher compatibility and execution quality improved**:
  - expanded support for complex `CALL`/`YIELD`/`WITH`/`UNION`/`UNWIND` query pipelines
  - improved deterministic result-shape handling, aggregation behavior, and clause-boundary parsing
  - hardened DDL/index/constraint handling and compatibility edge cases.
- **Hot-path and index-driven performance improved**:
  - added index-first routing for common query shapes (`IN`, `IS NOT NULL`, top-K/ordered limits)
  - improved correlated apply/join paths and reduced allocation-heavy execution branches
  - expanded plan/query cache usage where safe.
- **Search and vector behavior became more deterministic**:
  - improved BM25/index consistency and startup/rebuild behavior
  - hardened dropped-database artifact cleanup
  - improved rerank configuration application at per-database scope.
- **Operations and UX advanced**:
  - added Browser multi-statement execution UX
  - improved database metadata visibility in UI
  - added/expanded environment-variable and topology/operator documentation
  - added stdout/stderr lifecycle controls and other operational hardening.
- **Reliability and coverage expanded across the stack**:
  - large increase in deterministic regression/integration/performance tests
  - broad hardening across storage, server, cypher, and fabric execution paths.

### Documentation

- Expanded the multi-database guide and added Fabric gap-analysis, delivery-plan, and performance-audit notes to document the new composite execution model and remaining follow-up items.
- Refreshed README badges/community links during the release range.

### Technical Details

- **Range covered**: `v1.0.16..main`
- **Commits in range**: 21 (non-merge)
- **Repository delta**: 230 files changed, +25,221 / -5,323 lines
- **Non-test surface changed**: 67 files
- **Primary focus areas**: Fabric/composite execution, remote constituent routing, transaction/protocol parity, multidatabase stats/UI, planner/executor performance.

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

[v1.0.13]: https://github.com/orneryd/NornicDB/compare/v1.0.12-hotfix...v1.0.13
[v1.0.14]: https://github.com/orneryd/NornicDB/compare/v1.0.13...v1.0.14
[v1.0.12-hotfix]: https://github.com/orneryd/NornicDB/compare/v1.0.12...v1.0.12-hotfix
[v1.0.12]: https://github.com/orneryd/NornicDB/compare/v1.0.12-preview...v1.0.12
[v1.0.12-preview]: https://github.com/orneryd/NornicDB/compare/v1.0.11...v1.0.12-preview
[v1.0.11]: https://github.com/orneryd/NornicDB/compare/v1.0.10...v1.0.11
[v1.0.10]: https://github.com/orneryd/NornicDB/compare/v1.0.9...v1.0.10
[v1.0.9]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.9
[v1.0.6]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.6
[v1.0.1]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.1
[v1.0.0]: https://github.com/orneryd/NornicDB/releases/tag/v1.0.0
