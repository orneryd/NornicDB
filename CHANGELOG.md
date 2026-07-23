# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **WHERE relationship-existence predicates now recognize bracket-less and
  undirected patterns, and bare `COUNT`/`EXISTS` subquery bodies.**
  `WHERE (n)--()`, `WHERE (n)-->()`, `WHERE (n)<--()`, and the bracketed
  undirected form `WHERE (n)-[r]-()` previously fell through to the
  relationship-pattern gate's default branch, which treats an unrecognized
  expression as `true` -- so `WHERE NOT (n)--()` matched nothing and
  `WHERE (n)--()` was always true, regardless of the graph's actual shape.
  Separately, `COUNT { (n)--() }` and `EXISTS { (n)--() }` required their
  subquery body to start with `MATCH`, so a bare (unprefixed) body always
  returned `0` / `false`. Both gates now recognize the full range of
  bracket-less and undirected existence patterns.
- **Non-DETACH `DELETE` of a node that still has relationships now errors
  instead of silently cascade-deleting its edges (behavior change).**
  `MATCH (n) DELETE n` previously called the storage engine's
  `DeleteNode` unconditionally, which cascade-deletes every adjacent edge --
  so a plain `DELETE` on a connected node quietly removed its relationships
  too, diverging from openCypher/Neo4j semantics. `DELETE` now validates that
  every node in the deletion plan has no relationships left outside the
  statement's own edge deletions *before* mutating anything (so a multi-row
  `DELETE` is judged as a whole, not partially applied), and errors with the
  same "still has relationships" wording Neo4j uses. Deleting a node together
  with its own edge in one statement (`DELETE a, r`) still works, since the
  edge being removed no longer counts as residual. Existing callers relying on
  the old silent cascade must switch to `DETACH DELETE`.
- **An `OPTIONAL MATCH`-bound relationship variable resolved to `nil` inside
  `DELETE`/`SET`/`REMOVE`'s internal match probes, even when the `OPTIONAL
  MATCH` genuinely matched.** `executeDelete`, `executeSet`, and
  `executeRemove` each build an internal
  `MATCH ... OPTIONAL MATCH (a)-[r:TYPE]->(b) RETURN <vars>` probe and execute
  it via the low-level match path instead of the dispatcher that normally
  detects `OPTIONAL MATCH`. That low-level path located the relationship
  bracket by scanning forward from the character right after the first node
  group's closing paren, an assumption the embedded `OPTIONAL MATCH (n)` text
  broke: the corrupted substring handed to the relationship-pattern parser no
  longer started with `[`, so the bound variable, type, and properties were
  silently dropped. A relationship pattern embedded after `OPTIONAL MATCH` is
  now routed through the same compound-match handler the top-level dispatcher
  already uses, so the relationship variable resolves to the real edge.
- **`REMOVE` did not support relationship variables at all.** `REMOVE r.prop`
  silently no-op'd (it only inspected `*storage.Node` values in the matched
  rows) and, independent of that gap, a `REMOVE` query returning more than one
  node variable emitted one duplicated result row per node instead of one row
  per match. `REMOVE` now removes properties from relationship variables the
  same way `SET` already does, and its `RETURN` handling builds exactly one
  row per matched row regardless of how many node/relationship variables are
  in scope. The same fix applies to a chained `SET ... REMOVE ...` clause in a
  single statement.
- **Traversal-seeded `OPTIONAL MATCH` projections are now evaluated instead of
  echoed as literal expression text.**
  A read query whose primary `MATCH` contains a relationship pattern followed
  by one or more trailing `OPTIONAL MATCH` clauses (no intervening `WITH`)
  previously resolved its `RETURN` items with a string resolver that only
  understood `var.prop` and bare variables. Every other expression came back
  as its own source text: `RETURN type(rel)` returned the literal string
  `"type(rel)"`, `coalesce(...)`/`labels(...)`/`head(...)` returned their
  source text, aggregates like `count(f)` returned the string `"count(f)"`,
  the primary MATCH's relationship variable was dropped entirely (so
  `rel.weight` returned `"rel.weight"`), chained second-level `OPTIONAL MATCH`
  clauses were silently swallowed (their bindings projected as literal text),
  and per-clause `WHERE` predicates were ignored. The path now executes the
  seed `MATCH` with relationship variables bound, left-outer joins every
  chained `OPTIONAL MATCH` clause in either direction (seeding from whichever
  endpoint is bound), evaluates projections through the real expression
  evaluator (with a fast path for plain `var.prop`/bare-variable items),
  routes aggregate projections through implicit-grouping aggregation with
  Cypher null semantics, and applies `ORDER BY`/`SKIP`/`LIMIT`. The clause
  semantics mirror Neo4j's runtime operators: a connected single-hop clause
  behaves like `OptionalExpandAll` (null seeds propagate null bindings), and
  every other shape — a disconnected pattern sharing no variable with earlier
  clauses, a single-node pattern, a multi-hop chain, or a pre-bound
  relationship variable — is evaluated with `Apply` + `Optional` semantics:
  matches extend the row, and a row with no match is preserved once with
  newly-introduced variables bound to null. No valid shape is rejected.
  Aggregation follows Neo4j's model end to end: implicit grouping by the
  non-aggregate items, identity values over empty ungrouped input, `stdev`/
  `stdevp` per Neo4j's `StdevFunction`, and `RETURN` items that contain
  aggregates inside larger expressions (e.g. `count(x) + 1`,
  `coalesce(sum(w), 0)`) are isolated and substituted exactly like Neo4j's
  `isolateAggregation` rewrite.
- **Bolt explicit transactions now bind top-level `UNWIND` rows before
  routing to mutation handlers.**
  `session.ExecuteWrite` queries shaped as `UNWIND ... MATCH ... DELETE`
  previously routed directly to the delete handler before the UNWIND variable
  was bound, returned success with zero delete counters, and left matching
  relationships intact. The same substring-based routing also intercepted
  `UNWIND ... MATCH ... SET` and `UNWIND ... MATCH ... REMOVE` in explicit
  transactions and sent them to `executeSet` / `executeRemove` without binding
  the UNWIND variable. Explicit transactions now use the same UNWIND-first
  dispatch order as autocommit for DELETE, DETACH DELETE, SET, and REMOVE, and
  aggregate per-row mutation counters (nodes/relationships created/deleted,
  properties set, labels added) into Bolt result summaries so downstream
  clients observe accurate counters.

## [v1.1.11] - 7/9/2026

### Added

- **GPU-accelerated HNSW construction on CUDA and Vulkan.**
  New `pkg/search/hnsw_build_cuda.go` and `hnsw_build_vulkan.go` add optional
  GPU build backends for HNSW graph construction, with matching stubs for
  builds without CUDA/Vulkan support. `pkg/gpu/cuda/cuda_bridge.go` gains the
  CUDA bridge functions; `pkg/gpu/vulkan/compute.go` adds a rewritten Vulkan
  compute pipeline with dedicated shaders (`hnsw_build_cosine.comp`,
  `hnsw_build_topk_rows.comp` and their compiled SPIR-V). Falls back to CPU
  build when GPU backends are unavailable. Follow-up commit tightens Vulkan
  shader dispatch/binding performance (`perf(hnsw): optimize vulkan`).
- **Adjacency snapshot-isolation API for storage-backed graph reads.**
  Added a new adjacency snapshot-isolation path across `pkg/storage` and
  `pkg/cypher` so traversal and delete-adjacent reads resolve visible edges
  against the correct MVCC view instead of relying on coarser snapshot
  behavior. This also threads through namespaced and WAL-wrapped engines and
  adds focused regression coverage in storage and server graph tests.

### Fixed

- **Large disjoint UNIQUE-constrained write batches no longer serialize on
  hash-lock collisions.**
  Replaced the fixed 256-stripe commit-lock table with an active,
  reference-counted exact-value registry. Transactions touching the same
  `(label, property, value)` still serialize through validation, Badger commit,
  and unique-cache publication, while disjoint batches commit concurrently.
  Registry-assigned ordering prevents AB-BA deadlocks, entries expire after the
  last holder or waiter, and non-reflexive values such as NaN cannot leak lock
  entries. Added real Badger commit, same-value retry, race, cleanup, and
  8-writer/100-key performance coverage.
- **Multi-MATCH relationship variable bound in a later clause was silently
  dropped.**
  `MATCH (s) WHERE s.uid IN $u MATCH (s)-[rel]->() WHERE
rel.evidence_source = $e DELETE rel` deleted zero edges instead of the
  matching set, and the same shape used as a read (`RETURN count(rel)`,
  `RETURN rel`, `RETURN rel.prop`, `elementId(rel)`) silently returned
  zero/nil for the relationship column while node columns in the same query
  resolved correctly. Root cause: `pkg/cypher/match_multi.go`'s multi-match
  binding row (`type binding map[string]*storage.Node`) can only hold node
  values, so `executeFirstMatch`/`executeChainedMatch` never stored the
  relationship a clause's pattern bound, even though the `PathResult`
  computed by the traversal already carried it. Kept `binding` itself
  unchanged (a `map[string]*storage.Node` value-typed literal is
  constructed/indexed directly by ~10 existing binding-where test files) and
  instead threaded a parallel, index-aligned `map[string]*storage.Edge` per
  row through `executeFirstMatch`, `executeChainedMatch`, a new
  `filterBindingsByWhereWithRels` (relationship-aware WHERE filtering that
  reuses the unchanged node-only WHERE compiler via a read-only
  `bindingWithRelView` property adapter), and `resolveBindingExpr`/
  `resolveBindingItem`. Also fixed a related gap where `executeMultiMatch`
  never applied SKIP/LIMIT despite a comment claiming it did. Added
  `pkg/cypher/multi_match_relationship_binding_bug_test.go`
  (`TestBug_MultiMatchRelationshipBindingLost` + `_Variations`,
  `TestMergeRelBindings`, `TestBindingWithRelView`,
  `TestResolveBindingExprUnboundVariable`). Follow-up fixes now also restore
  `MATCH ... WITH ... MATCH ... RETURN` and `... DELETE rel` pipeline shapes
  by delegating valid chained pipeline forms through the general pipeline
  executor, preserving row-bound node/relationship context across later MATCH
  clauses, and trimming trailing `ORDER BY` / `SKIP` / `LIMIT` correctly in
  pipeline RETURN projection.
- **`CREATE ... WITH ...` query routing now distinguishes missing behavior from
  invalid syntax.**
  Valid `CREATE ... WITH ... RETURN`, `CREATE ... WITH ... MATCH ... RETURN`,
  and related pipeline/fallback shapes now execute instead of failing as
  generically unsupported, while malformed tails now surface deterministic
  invalid-query errors and correctly roll back implicit single-statement
  transactions. This reuses the existing multiple-create executor as a strict
  fallback after pipeline routing and adds regression coverage for the rollback
  case.
- **Cypher map keys containing `:` now parse correctly across map-literal
  surfaces.**
  Quoted keys such as `{'key:key': 'value'}` were previously split at the
  first colon and mis-parsed in `SET`, `MERGE`, helper evaluators, APOC map
  parsing, and pipeline/map-literal call sites. Added a shared top-level
  key/value separator helper in `pkg/cypher/pattern_parser.go` and applied it
  across the affected map parsers, with parser-level and end-to-end regression
  coverage.
- **Bolt 4.x datetime/time encoding compatibility restored.**
  `pkg/bolt/packstream.go` and `pkg/bolt/server.go` now negotiate older Bolt
  datetime encodings correctly, including the Rust-driver compatibility path
  and UTC/compatibility-sensitive record emission. Added focused packstream and
  server regressions for datetime structure decoding and negotiated time
  encoding.
- **Bound relationship delete correctness hardened.**
  Unified delete projection typing so relationship delete targets are preserved
  as relationships rather than flattened into mismatched row shapes, and
  normalized dangling-edge traversal semantics so stale adjacency rows are
  skipped consistently instead of surfacing inconsistent delete behavior. This
  also adds focused delete-helper and chained-traversal regressions.
- **MVCC adjacency / visibility regressions corrected.**
  Fixed several storage-layer correctness issues in the new snapshot-adjacency
  path, including edge visibility flags being dropped while copying materialized
  edges, pruning/order bugs that could tombstone visible adjacency incorrectly,
  and namespace filtering being applied too late during unprefixing.
- **Fulltext query parser: `field:"value" AND (term)` now intersects correctly.**
  `db.index.fulltext.queryNodes` / `queryRelationships` previously tokenized the
  query with a whitespace splitter that had no notion of parenthesized groups,
  field-scoped clauses, or Lucene escape sequences. The Graphiti integration
  shape `group_id:"g" AND (<terms>)` silently discarded the parenthesized
  default-field clause — a real term and a nonsense term returned the identical
  result set, making the lexical arm of hybrid search term-blind. Replaced the
  ad-hoc tokenizer with a proper Lucene-classic recursive-descent parser
  (`pkg/cypher/fulltext_query.go`) plus per-document evaluator that supports
  the full grammar: boolean AND/OR/NOT with parens and nesting, `+`/`-`
  mandatory/prohibited clause prefixes, phrase queries and proximity
  (`"a b"~n`), fuzzy (`term~n`, Levenshtein), range queries (`[a TO b]`,
  `{a TO b}` with mixed inclusivity), boost (`^n`), wildcards (`?`, `*`
  including leading and mid-token), regex (`/re/`), and full Lucene escape
  rules (`\X` decodes to literal `X` for any X — fixes the `Cloud\Trail`
  zero-hits case). Reference implementation: Neo4j's `MultiFieldQueryParser`
  with `setAllowLeadingWildcard(true)`. Added `decodeCypherStringLiteral` in
  `pkg/cypher/call_fulltext.go` so backslash escapes survive round-trip
  through Cypher parameter substitution.
- **Post-WITH `WHERE` filtering and grouped aggregation restored.**
  A cluster of Cypher `WHERE` evaluation issues against clean graph queries:
  - Split post-`WITH WHERE` from `WITH` projections so it is applied as a
    filter instead of being absorbed into the preceding alias.
  - Preserve grouped `WITH` aggregation semantics for `OPTIONAL MATCH` rows,
    including `COUNT(c)` over null optional targets.
  - Honor inline target labels/properties in relationship pattern predicates
    such as `NOT (t)-[:R]->(:C {met:false})`.
  - Evaluate bare boolean properties in traversal `WHERE` clauses so
    `WHERE NOT c.met` returns rows where `c.met` is false.
  - Allow multiple relationship `CREATE` clauses with inline endpoint nodes
    in one statement.
    Touched: `pkg/cypher/clauses.go`, `create_pipeline_helpers.go`,
    `executor_mutations.go`, `match_rows.go`, `traversal.go` plus new
    `pkg/cypher/stats_query_test.go`.
- **DDL / CALL-tail / UNWIND / typed conversion cluster fixes.**
  - Preserve cardinality constraint parser errors for malformed
    `REQUIRE MAX COUNT` DDL.
  - Avoid compiling CALL-tail regex predicates using `=~` as equality
    comparisons.
  - Fix post-`UNWIND WHERE` boundary parsing so equality/inequality filters
    apply correctly.
  - Prefer explicit typed assignment conversions before generic reflect
    conversion.
  - Deterministic coverage tests added for schema DDL, CALL-tail predicates,
    helper branches, UNWIND filtering, and typed result assignment
    (`pkg/cypher/coverage_lift_test.go` +1163 lines).

### Performance

- **IN-list-anchored relationship traversal now index-seeds instead of
  scanning the whole label.**
  `MATCH (s:Label)-[rel]->() WHERE s.uid IN $list ...` (with or without an
  additional `AND` predicate on the bound relationship, e.g.
  `rel.evidence_source = $e`) previously fell through every start-node
  pruning branch in `executeMatchWithRelationshipsWithPath`
  (`pkg/cypher/traversal.go`) — none of them recognized an `IN [...]` /
  `IN $param` predicate — straight to `loadNodesWithTemporalViewport`, an
  O(all nodes of the label) scan, even though the equivalent node-only
  `MATCH (s:Label) WHERE s.uid IN $list RETURN s` already used the schema
  property index via `tryCollectNodesFromPropertyIndexIn(Literal)`
  (`pkg/cypher/match_index_seek.go`, already wired into `match.go`,
  `clauses.go`, and `executor_mutations.go`). Added
  `tryCollectNodesFromPropertyIndexInCompound`, which wires those existing
  index-seek helpers into the traversal start-node pruning chain —
  including when the IN-list is one conjunct of an `AND`-combined WHERE
  clause, mirroring `tryCollectNodesFromIDEqualityCompound`'s conjunct
  handling. Correctness is unaffected: `filterPathsByWhere` still
  re-evaluates the full WHERE clause after seeding, so pruning from one
  recognized conjunct can only over-fetch, never under-fetch.
  `BenchmarkInListAnchoredRelMatch` (50k-node label, 100-node target
  sublist; 5k was tried first but the algorithmic difference is inside
  measurement noise for an in-process `MemoryEngine` at that size) on this
  branch: 109,930,838 ns/op → 301,410 ns/op (~365x), 83,268,576 B/op →
  228,696 B/op (~364x), 1,388,688 allocs/op → 2,800 allocs/op (~496x).
  Added `pkg/cypher/inlist_start_node_index_seed_bug_test.go`
  (`TestBug_InListStartNodeDoesNotIndexSeed` + scan-budget and DELETE
  variants, `TestTryCollectNodesFromPropertyIndexInCompound`,
  `BenchmarkInListAnchoredRelMatch`).
- **Match/merge hot path from Graphify workloads.**
  `pkg/cypher/match_multi.go`, `merge.go`, and `pkg/storage/schema.go` gain a
  fast pattern-property index lookup so multi-pattern `MATCH`/`MERGE`
  statements hit the schema index directly instead of scanning candidates.
  New benchmark (`graphify_push_profile_bench_test.go`) and
  pattern-property regression tests (`match_pattern_property_index_test.go`)
  guard the change.
- **Bound relationship deletes now use targeted source lookup and cheaper
  snapshot adjacency reads.**
  The delete hot path now resolves bound relationship candidates using indexed
  and node-local transaction-snapshot reads instead of broader adjacency scans,
  reducing the cost of relationship delete workloads while keeping the delete
  projection contract intact.
- **Vulkan compute shaders tuned.**
  Dispatch/binding refactor in `pkg/gpu/vulkan/compute.go` following the GPU
  HNSW build feature.

### Changed

- **Dependency refresh — Go modules.**
  - `google.golang.org/api` v0.286.0 → v0.287.0
  - `google.golang.org/grpc` v1.81.1 → v1.82.0
  - `github.com/googleapis/enterprise-certificate-proxy` v0.3.16 → v0.3.17
    (indirect)
  - `google.golang.org/genproto/googleapis/rpc` bumped to 20260622175928
    (indirect)
- **Dependency refresh — UI npm packages.**
  - `@ornery/ui-grid-core` / `-react` / `-vanilla` ^1.0.6 → ^1.0.8
  - `lucide-react` ^1.22.0 → ^1.23.0
  - `neo4j-driver` ^6.1.0 → ^6.2.0
  - `react-router-dom` ^7.18.0 → ^7.18.1
  - `three` ^0.184.0 → ^0.185.0
- **Dependabot workflow versions refreshed** across `.github/workflows/`
  (`cd-llama-cpu.yml`, `cd-llama-cuda.yml`, `cd.yml`, `ci.yml`,
  `docs-pages.yml`, `release-macos.yml`).
- **Documentation.** `README.md` copy tweak. ORM/Neo4j-compatible streaming
  driver plan (`plans/`) expanded with implementation detail (+556/-189).

### Tests

- New `pkg/cypher/coverage_lift_test.go` (+1163 lines) exercising DDL,
  CALL-tail predicates, UNWIND filtering, and typed assignment.
- New `pkg/cypher/multi_match_relationship_binding_bug_test.go` and
  `pkg/cypher/inlist_start_node_index_seed_bug_test.go` covering the
  multi-MATCH relationship-binding regression, the follow-up MATCH/WITH/MATCH
  pipeline behavior, and IN-list traversal index-seeding correctness,
  scan-budget, DELETE, and benchmark variants.
- New `pkg/cypher/kalman_functions_test.go` covering the Kalman helper
  branches.
- New `pkg/cypher/fulltext_query_test.go` and
  `pkg/cypher/call_fulltext_parser_test.go` covering the full Lucene-classic
  grammar surface plus e2e regressions against the Graphiti bug repro.
- New `pkg/cypher/stats_query_test.go`, `match_pattern_property_index_test.go`,
  `graphify_push_profile_bench_test.go` guarding the WHERE-semantics and
  hot-path match/merge changes.
- `pkg/search/hnsw_build_gpu_test.go` extended for GPU-build coverage.
- New Bolt compatibility regressions in `pkg/bolt/packstream_into_test.go` and
  `pkg/bolt/server_test.go` covering older PackStream datetime structures,
  negotiated datetime emission, and Rust-driver compatibility.

### Technical Details

- **Range covered**: `v1.1.10..HEAD`
- **Commits in range**: 37
- **Repository delta**: 114 files changed, +13,740 / -2,173 lines

## [v1.1.10] - 2026-06-29

Maintenance release: dependency refresh, llama.cpp upgrade, Cypher Graphiti ingest fixes, and expanded test coverage.

### Changed

- **llama.cpp upgraded from b9644 to b9835** (191 commits, 534 files):
  - No breaking API changes: `llama_context_params`, `llama_model_params`, and `llama_batch` structs are unchanged.
  - New public API `llama_model_n_layer_nextn()` added for MTP/NextN speculative-decoding models (not used by NornicDB's embedding path).
  - No new deprecations, no embedding-path changes, no build-system changes affecting NornicDB's CMake flags.
  - Headers and build scripts (`build-llama.sh`, `build-llama-cuda.ps1`) synced to b9835.
- **Dependency refresh — Go modules** (11 direct dependencies updated to latest minor/patch):
  - `github.com/99designs/gqlgen` v0.17.90 → v0.17.93
  - `github.com/dgraph-io/badger/v4` v4.9.1 → v4.9.2
  - `github.com/ebitengine/purego` v0.10.0 → v0.10.1
  - `github.com/hashicorp/go-kms-wrapping/wrappers/azurekeyvault/v2` v2.0.14 → v2.0.15
  - `github.com/hybridgroup/yzma` v1.14.1 → v1.18.0
  - `github.com/qdrant/go-client` v1.18.2 → v1.18.3
  - `github.com/vektah/gqlparser/v2` v2.5.33 → v2.5.35
  - `golang.org/x/crypto` v0.52.0 → v0.53.0
  - `golang.org/x/net` v0.55.0 → v0.56.0
  - `golang.org/x/sync` v0.20.0 → v0.21.0
  - `google.golang.org/api` v0.283.0 → v0.286.0
  - `golang.org/x/sys`, `golang.org/x/text`, `golang.org/x/tools`, `golang.org/x/mod`, `google.golang.org/genproto/googleapis/rpc` also bumped as transitive upgrades.
- **Dependency refresh — UI npm packages** (10 packages bumped to latest minor/patch):
  - `lucide-react` ^1.17.0 → ^1.22.0
  - `neo4j-driver` ^6.0.1 → ^6.1.0
  - `react-router-dom` ^7.16.0 → ^7.18.0
  - `@tailwindcss/postcss` ^4.3.0 → ^4.3.2
  - `tailwindcss` ^4.3.0 → ^4.3.2
  - `@types/react` ^19.2.16 → ^19.2.17
  - `@vitejs/plugin-react` ^6.0.2 → ^6.0.3
  - `autoprefixer` ^10.5.0 → ^10.5.2
  - `baseline-browser-mapping` ^2.10.33 → ^2.10.40
  - `postcss` ^8.5.14 → ^8.5.16
- **GraphQL code regenerated** with gqlgen v0.17.93 to accommodate upstream `DeferredGroup.Label` and `CollectedField.Deferrable` field removals in the graphql runtime library.

### Fixed

- **Cypher Graphiti ingest correctness**:
  - `CALL`-tail projection fast path corrected so that tail subquery outputs project correctly into outer RETURN clauses.
  - Graphiti ingest-time relationship and chunk search performance improved; edge cases around exact relationship shape matching during high-frequency ingest are hardened.
- **Vector cosine fast-path routing** tightened for Graphiti query shapes using `WITH ... vector.similarity.cosine(...) AS score` patterns.

### Tests

- **Graphiti E2E scenario and fast-path assertion tests** added (`pkg/cypher/graphiti_scenario_e2e_test.go`, `pkg/cypher/graphiti_exact_shapes_e2e_test.go`):
  - Covers full ingest pipelines including relationship-batch, vector-property, and chunk-search query shapes.
  - Fast-path assertions verify that optimized execution routes are taken for expected query patterns.
- **Graphify E2E scenario tests** added (`pkg/cypher/graphify_scenario_e2e_test.go`, `pkg/cypher/graphify_exact_shapes_e2e_test.go`).
- **CALL-tail projection regression coverage** added (`pkg/cypher/coverage_call_tail_test.go`).
- **Vector cosine fast-path shape coverage** extended (`executor_match_vector_cosine_fastpath_test.go`).

### Technical Details

- **Range covered**: `v1.1.9..HEAD`
- **Commits in range**: 6 (non-merge)
- **Repository delta**: 34 files changed, +6,725 / −4,770 lines

## [v1.1.9]

### Fixed

- **Cypher count preservation**: `RETURN` clauses now preserve counts correctly when combined with aggregation and projection (hotfix backport from v1.1.8-hotfix).

## [v1.1.8-hotfix]

### Fixed

- **Cypher count preservation**: `RETURN` clauses no longer drop count values when combined with aggregation and projection.

## [v1.1.8]

### Changed

- **Cypher hot paths were tightened across execution, planning, and clause handling**:
  - structural expression evaluation now uses dedicated fast paths for common operators and property/literal forms;
  - hot candidate paths compile `WHERE` predicates earlier;
  - `CALL`-tail retrieval and composite subquery binding preservation were both corrected and optimized;
  - keyword scanning now caches results and narrows stale-index fallback behavior.
- **Vector-search execution was made more selective and more passive during ingest**:
  - query-time vector index warmups are avoided on read paths;
  - vector fast paths now use owned search-readiness gates instead of forcing full warmup semantics;
  - Graphiti ingest keeps vector reads and writes passive where appropriate;
  - exact file-store candidates are now included for property-vector node queries so dedup and ingest paths do not depend solely on HNSW recall.
- **Storage fast paths were extended for vector-heavy workloads**:
  - node properties are projected for vector fast paths;
  - current-head node body decodes are cached for MVCC reads;
  - tokenized property decode overhead was reduced;
  - async flush now blocks prefix counts while data is in flight to preserve correctness.
- **Search write-path behavior was refined for live updates and shutdown safety**:
  - unchanged vector reindexing is skipped during HNSW live writes;
  - write-side indexing now respects lazy warming;
  - background vector work stops cleanly after shutdown;
  - CPU brute-force vector search remains opt-in rather than being enabled implicitly.
- **Bolt compatibility was extended for modern temporal values**:
  - `localDateTime` PackStream encoding was added so typed temporal round-trips behave correctly with newer driver payloads.
- **Documentation and release-facing notes were refreshed**:
  - README and vector-search docs were updated alongside the execution changes;
  - configuration and environment-variable docs were aligned with the latest search and vector behavior.

### Fixed

- **Cypher vector and mutation correctness issues were resolved**:
  - relationship-bound `WITH ... CALL` vector-property paths now execute side effects and bind relationship variables correctly;
  - node mutation indexing now updates properly;
  - relationship batch match paths preserve indexed matches instead of dropping them;
  - stale indexed MATCH buckets are verified before hot-path reuse;
  - temporal values expanded from nested parameter maps now round-trip as typed Cypher literals instead of stringified timestamps.
- **Graphiti compatibility and subquery binding behavior were hardened**:
  - correlated bindings now survive composite subqueries;
  - vector reads stay passive during Graphiti ingest rather than forcing storage-scan fallbacks.
- **Search lifecycle and readiness edge cases were corrected**:
  - write-side lazy warming is honored;
  - live vector-query capability is separated from full ONLINE readiness;
  - unchanged HNSW reindex work is skipped on live writes;
  - file-backed property-vector queries now use the Cypher vector path consistently.
- **Storage and async-engine regressions were addressed**:
  - prefix-count operations no longer race against async flush state;
  - MVCC and node-body decode paths are stable under hot read/write workloads.

### Tests

- Expanded regression coverage across Cypher, search, storage, Bolt, and Graphiti-specific scenarios, including:
  - vector query shape coverage;
  - subquery and clause-boundary regressions;
  - property-vector file-store lookup behavior;
  - MVCC and async flush correctness;
  - temporal encoding round-trip verification.

### Technical Details

- **Range covered**: `v1.1.7..HEAD`
- **Commits in range**: 30 (non-merge)
- **Repository delta**: 111 files changed, +7,692 / -904 lines

## [v1.1.7]

### Changed

- **llama.cpp bundle/build refresh**
  - Updated bundled llama.cpp artifacts and synchronized build surfaces (Makefile, Windows batch scripts, shell/PowerShell build scripts, and Docker llama images) to the current pinned integration version.
- **Embedding model defaults hardened for local GGUF stability**:
  - Embedding context features now default `flash_attn` to disabled (`0`) instead of auto for safer startup across Metal/CUDA backends.
  - `NORNICDB_EMBEDDING_FLASH_ATTN=0` is treated as an explicit override (not silently ignored).
- **GPU-assisted HNSW construction path retuned for lower latency and lower allocation pressure**:
  - Build-time candidate generation now uses a Metal batched matrix/top-k primitive plus graph-beam expansion instead of per-query GPU search calls.
  - Hot-path build code now reuses graph snapshot scratch state and batch flattening buffers, avoiding repeated per-batch/per-iteration map/slice churn.
  - Default GPU graph-beam knobs were tightened to reduce candidate work without changing persistence format:
    - `NORNICDB_HNSW_BUILD_GPU_BEAM_WIDTH` default now effectively `min(candidate_k, 64)`.
    - `NORNICDB_HNSW_BUILD_GPU_BEAM_UNION_MAX` default now `4096`.
  - On the internal `BenchmarkHNSWBuildCPUVsAcceleratedCandidatesLarge1024D` reference case (8k vectors, 1024 dimensions, Apple M3 Max), observed build latency moved from ~`1.45s` CPU to ~`0.93s` Metal graph-beam (~1.55x faster), with significantly reduced GPU-build heap growth versus earlier GPU prototypes.
- **Relationship vector-search fast paths expanded**:
  - Relationship vector query specs and fast-path execution now use relationship vector indexes where applicable instead of scanning relationships and decoding vectors in the query path.
  - This reduces latency and allocation pressure for Graphiti fact-search shapes backed by relationship embeddings.
- **macOS/Homebrew release automation tightened**:
  - Intel Homebrew tarballs now build on `macos-15-intel`.
  - The macOS release workflow now invokes the Homebrew artifact script through the restored `make homebrew-artifacts` target, preventing release jobs from failing after the main macOS assets upload succeeds.

### Added

- **Homebrew distribution release plumbing**:
  - Added a Homebrew tap scaffold under `homebrew/` with formula, tap CI, formula-update workflow, and release maintenance docs.
  - Added `make homebrew-artifacts` and `scripts/build-homebrew-artifacts.sh` to publish `nornicdb-darwin-arm64.tar.gz`, `nornicdb-darwin-amd64.tar.gz`, and `SHA256SUMS` for Homebrew installs.
  - Extended the macOS release workflow to attach Homebrew tarballs and dispatch tap formula update PRs when `HOMEBREW_TAP_REPOSITORY` and `HOMEBREW_TAP_TOKEN` are configured.
- **`NORNICDB_LLAMA_VERBOSE_LOAD` generation-load diagnostics toggle**:
  - Set to `true`/`1` to expose native llama.cpp model-load diagnostics for Heimdall generation model loading.

### Fixed

- **Configuration-driven auth disabling is now honored by server startup**:
  - `auth.enabled: false` and `server.auth: none` from the loaded configuration no longer get overridden by serve startup defaults.
  - `--no-auth` remains a hard disable override, which keeps Homebrew first-run configurations that choose no authentication aligned with runtime behavior.
- **Heimdall local-model fallback error reporting now preserves both attempts**:
  - When GPU load fails and CPU fallback also fails, the returned error now includes both failure paths instead of only the final fallback error.
- **Flash-attention override semantics corrected for Heimdall and rerank paths**:
  - `NORNICDB_HEIMDALL_FLASH_ATTN=0` and `NORNICDB_RERANK_FLASH_ATTN=0` are now honored as explicit values.
- **Parameter-map property access correctness fixed across direct and `WITH` projection forms**:
  - `$map.key` and `$map['key']` now resolve to typed values in `RETURN`/expression evaluation instead of being returned as literal source text.
  - `WITH $map AS m RETURN m.key` / `m['key']` now evaluate correctly (no token corruption from scalar substitution).

## [v1.1.6] - 2026-06-12

Release focused on Neo4j/Graphiti compatibility hardening, vector-search performance correctness, and a new offline admin import/export workflow.

### Added

- **`nornicdb-admin` offline tooling** for full-database import/export workflows:
  - `database import full` with multi-file node/relationship sources, deterministic reports, structured exit codes, and fail-fast source validation.
  - Neo4j CSV package round-trip support, including schema export/import surfaces and full-schema package generation for operational migrations.
- **Operations documentation for admin workflows**, including import/export usage, package layout, and recovery behavior.

### Changed

- **Cypher DDL and clause parsing migrated further from regex matching to keyword/token scanning** across schema/index and helper paths, improving parser determinism and reducing fragile query-shape coupling.
- **Configuration/docs alignment for async writes**: YAML and environment-variable behavior is now documented and validated consistently for production operators.

### Fixed

- **Graphiti-blocking write-path correctness issues resolved end-to-end**:
  - `SET n:<labels>` + `SET n = <map>` composition no longer drops properties/labels.
  - Bulk `UNWIND ... MERGE/SET ... WITH ... CALL db.create.setNodeVectorProperty(...)` no longer silently discards row writes.
  - Matching relationship path with `db.create.setRelationshipVectorProperty(...)` no longer collapses relationship properties.
  - `MATCH + UNWIND + CREATE` with list-valued properties preserves row cardinality (no first-row-only collapse).
  - Relationship variables survive `WITH` projection correctly (`e.name`, `properties(e)`, etc. evaluate instead of returning literal expression text).
- **Bolt temporal compatibility corrected for modern Neo4j drivers**:
  - Datetime values now round-trip as typed temporal values instead of legacy/unhydrated structures or stringified Go time values.
- **Vector query compatibility and performance fast paths expanded**:
  - Fast-path routing now covers Graphiti query shapes using `WITH ... vector.similarity.cosine(...) AS score`, score filtering, and managed transaction execution paths.
  - Relationship cosine queries now route through vector-index-backed execution where applicable.
  - `CREATE VECTOR INDEX ... FOR ()-[e:TYPE]-() ON (e.prop)` is supported and integrated with fast-path execution.
  - Ascending cosine ordering correctness tightened (full score-range handling and deterministic boundary clamping).
- **Neo4j fulltext procedure compatibility improved**:
  - `db.index.fulltext.queryNodes`/`queryRelationships` now accept the optional 3rd `options` argument form used by official Neo4j tooling.
- **Neo4j relationship index DDL compatibility improved**:
  - Relationship property index syntax (`CREATE INDEX ... FOR ()-[e:TYPE]-() ON (e.prop)`) is accepted and persisted correctly.
- **Storage/schema correctness regressions fixed**:
  - Composite schema index merging now preserves all index classes deterministically.
- **Knowledge-policy gating order corrected**:
  - Reverse-decay scoring is applied before visibility suppression/on-access hooks to preserve expected scoring semantics.

### Internal

- **Bundled coverage/regression expansion** across `cypher`, `storage`, `search`, `config`, and `adminimport` to lock in the compatibility and performance fixes above.

### Technical Details

- **Range covered**: `v1.1.5..main`
- **Commits in range**: 38 (non-merge)
- **Repository delta**: 111 files changed, +11,307 / -1,880 lines
- **Notes**: transient PR-feedback and test-only commits are intentionally collapsed into the user-facing items above.

## [v1.1.5] - 2026-06-03

Post-`v1.1.4` stabilization focused on Cypher/Bolt correctness, storage resilience, and deterministic behavior under real Neo4j-driver query shapes. This range also includes broad coverage expansion; all test/coverage work is bundled under a single item below.

### Changed

- **Storage embedding persistence now shards oversized chunk payloads in Badger.** Large embedding chunks are written/read using size-aware shard keys with backward-compatible decode logic for legacy single-value chunks. This prevents value-size failures on valid large embeddings while keeping existing data readable.
- **Search query chunk scoring now uses best-match chunk score instead of average score.** This improves relevance for multi-chunk documents where one strong matching chunk should dominate ranking.
- **Version metadata updated** to reflect the current post-`v1.1.4` development state.

### Fixed

- **Cypher MATCH+CREATE error handling now surfaces storage failures instead of silent no-op results.** Label/all-node lookups and relationship-existence checks in compound MATCH...CREATE paths now return explicit errors.
- **Cypher MATCH...CREATE relationship creation regressions fixed.** The following common Neo4j-compatible patterns now execute correctly again:
  - `MATCH (a),(b) CREATE (a)-[:R]->(b)`
  - `MATCH (s) CREATE (n)-[:R]->(s)`
- **Cypher CREATE...SET clause-boundary parsing fixed.** Trailing `CREATE`/`WITH`/`RETURN` after `SET` are now parsed as clauses (not expression text), and explicit transaction routing now matches autocommit behavior for CREATE...SET execution.
- **Cypher `SET x = x + "..."` self-reference mutations fixed.** In-place property concatenation/update now writes the computed value instead of silently no-oping.
- **Cypher MATCH...CREATE projection correctness fixed.** `COUNT(expr)` in post-create return projections now evaluates against both matched and newly created bindings, preventing false zero counts.
- **Cypher MERGE return aggregation correctness fixed.** `COUNT(*)` / `COUNT(var)` in MERGE return projection paths now produce deterministic Neo4j-compatible values instead of intermittent `nil`.
- **Cypher shell parameter expression failures are now surfaced explicitly.** Expression-evaluation errors are no longer swallowed in shell parameter processing.
- **Cypher merge-chain correctness tightened.** Post-`WITH` MERGE-chain clause splitting and execution were corrected so node MERGE segments are not dropped; invalid empty merge-chain shapes now return typed `ErrInvalidMergeChainQuery` errors.
- **Cypher relationship-WHERE evaluation ordering fixed.** Relationship-pattern WHERE clauses are now evaluated before single-variable shortcut routing in multi-match paths.
- **Cypher chained MERGE relationship SET parsing fixed.** Relationship `SET` parsing now stops at the next top-level MERGE boundary in chained clauses.
- **Cypher MATCH...UNWIND...MERGE loop variable substitution fixed.** UNWIND variables are now expanded per-item (instead of treated as literal identifiers) when UNWIND appears between MATCH and MERGE.
- **Cypher/Bolt multi-hop OPTIONAL MATCH regression fixed.** `MATCH ... OPTIONAL MATCH ...` after traversal now seeds optional matches from the full initial traversal result, preventing false zero-row outcomes.
- **Bolt write counters now populate Neo4j-compatible summary stats.** PULL/DISCARD completion metadata now includes write stats so ResultSummary counters are correct.
- **Cypher RETURN-after-DETACH-DELETE projection fixed.** Non-COUNT return expressions now resolve correctly instead of returning null placeholders.
- **Storage namespaced label scans hardened.** Nil-node handling in namespaced `GetNodesByLabel` no longer panics in edge cases.
- **Storage MVCC namespace state recovery fixed after migration/reopen.** Missing namespace counters are recovered from existing heads and transaction startup now primes namespace MVCC state, preventing false snapshot conflicts on migrated/reopened stores.
- **Badger recovery behavior improved for stale/cold namespaces while keeping hot-path cost low.**
- **Search mismatched-dimension panic fixed.** Query paths now handle dimension mismatch safely instead of panicking.
- **CI architecture-related flaky float precision assertion fixed.**

### Tests

- **Bundled test/coverage work:** expanded deterministic regression and branch coverage across `cypher`, `storage`, `search`, `server`, `nornicdb`, `heimdall`, and `config`, including many new correctness-focused edge-case tests.

### Technical Details

- **Range covered**: `v1.1.4..main`
- **Commits in range**: 33 (non-merge)
- **Repository delta**: 220 files changed, +23,707 / -1,496 lines

## [v1.1.3] - 2026-05-29

Maintenance release: **llama.cpp upgraded to b9835** with configurable per-model context features, plus several storage and Bolt correctness fixes discovered through expanded test coverage. No on-disk format changes; existing databases upgrade transparently.

### Added

- **Per-model llama.cpp context feature passthrough.** Each model domain (embedding, rerank, Heimdall) now accepts env-driven llama.cpp context parameters so operators can tune models that require non-default settings (e.g. MTP-trained models, CLS pooling, rank pooling):
  - Embedding: `NORNICDB_EMBEDDING_CTX_TYPE`, `NORNICDB_EMBEDDING_POOLING_TYPE`, `NORNICDB_EMBEDDING_ATTENTION_TYPE`, `NORNICDB_EMBEDDING_FLASH_ATTN`
  - Rerank: `NORNICDB_RERANK_CTX_TYPE`, `NORNICDB_RERANK_POOLING_TYPE`, `NORNICDB_RERANK_ATTENTION_TYPE`, `NORNICDB_RERANK_FLASH_ATTN`
  - Heimdall: `NORNICDB_HEIMDALL_CTX_TYPE`, `NORNICDB_HEIMDALL_POOLING_TYPE`, `NORNICDB_HEIMDALL_ATTENTION_TYPE`, `NORNICDB_HEIMDALL_FLASH_ATTN`

### Changed

- **llama.cpp upgraded from b9106 → b9835.** Build scripts and Dockerfiles now disable the new `app/` and `tools/` targets (`-DLLAMA_BUILD_TOOLS=OFF`, `--target llama --target ggml`) to avoid link errors against `llama-server-impl`. Context creation explicitly sets `ctx_type = LLAMA_CONTEXT_TYPE_DEFAULT` to prevent struct-layout mismatches from accidentally enabling MTP on non-MTP models.

### Fixed

- **Cypher:** `$dotted.param` parsing no longer fails; the simple-where cache is correctly skipped for parameterized values.
- **Storage:** Node label index is rebuilt from bodies on engine open (#183), fixing incorrect `db.labels()` results after unclean shutdown.
- **Storage:** Label-count metadata is now namespaced per-database, preserving correctness across async write paths and startup re-indexing.
- **Bolt:** Running queries are cancelled on client disconnect and `RESET`, preventing goroutine/resource leaks.

### Tests

- Expanded unit test coverage across cypher, bolt, storage, search, server, heimdall, fabric, kms, multidb, otel, and grpc packages.

## [v1.1.2] - 2026-05-26

Headline release: **Bolt over WebSocket** lands end-to-end so browser-based Neo4j drivers connect to NornicDB without a proxy, and **per-database BM25 + vector index master switches** ship as a first-class memory and warmup-cost lever for multi-tenant deployments. Three independently reported Cypher correctness regressions (mcp-neo4j-memory) are fixed with deeply-asserted parity against Neo4j 5.x DDL and Lucene wildcard semantics. A profile-led overhaul of the shortestPath traversal stack drops latency ~400× on the demo workload. No on-disk format changes; existing `v1.1.x` databases upgrade transparently.

### Added

- **Bolt over WebSocket — browser drivers connect natively.** The Bolt port (`:7687` by default) now multiplexes four wire-level transports off one listener, sniffing the first 5 bytes of every accepted connection: `bolt://` (raw TCP, today's path), `bolt+s://` (TLS), `ws://` (WebSocket over plain TCP), `wss://` (TLS + WebSocket). The architecture mirrors Neo4j's `TransportSelectionHandler`: WebSocket frames carry the same Bolt magic + version negotiation + PackStream + chunked framing that raw TCP does, so existing drivers (Go, Java, Python, JavaScript browser, .NET) speak the same protocol on either transport. Operator-configurable knobs cover origin allowlist (default `*`), max message size (default 65 536 bytes, matching Neo4j's `MAX_WEBSOCKET_FRAME_SIZE`), ping/pong cadence (default 30 s ping / 60 s pong), pre-HELLO auth deadline, transport-sniff timeout, mTLS `ClientAuthMode` (`none`/`request`/`request_verify`/`require_verify`), `RequireTLS` (rejects every plaintext upgrade with the canonical Neo4j error), `WebSocketEnabled=false` (returns HTTP 426 on real WS upgrades while still serving the discovery probe to health checks), and operator-driven cert rotation via 5-second `tls.Config.GetCertificate` re-read with atomic-rename semantics. A plain `GET /` on the Bolt port returns a Neo4j-parity discovery response (200 OK + 5 required headers; empty body for Community parity, JSON describing the OAuth provider when `NORNICDB_AUTH_PROVIDER=oauth`). Phase-3 throughput, allocation, and round-trip benchmarks ship for all four transports; ws stays within a 5 % budget vs raw tcp and ws_tls within 0.3 % of tcp_tls.

  Auth: HELLO `scheme=bearer`/`basic` always wins. As a deliberate exception for first-party browser clients the WS upgrade reads the `nornicdb_token` cookie and `Authorization: Bearer …` header; either is honored as an "implicit bearer" when HELLO is `scheme=none`. Cookie wins on conflict; raw TCP has no HTTP layer so the implicit path is unreachable there.

  Configuration: 13 new `NORNICDB_BOLT_*` env vars (TLS cert/key/require/CA/auth-mode, WS enabled/origins/max-message/write-buffer/ping/pong, sniff/auth timeouts) plumbed through env → CLI → YAML. Documented in `docs/operations/configuration.md` (Bolt over WebSocket + TLS section), `docs/operations/environment-variables.md`, `docs/user-guides/connecting-bolt.md` (Neo4j-compatible scheme table for every official driver), and `pkg/bolt/README.md`. Metric schema migrated: `bolt_connections_active` becomes a `GaugeVec`, `bolt_connections_total` gains a closed-enum `transport` label (cardinality 3 → 12), plus new `bolt_connections_rejected_total{reason}` and `bolt_websocket_oversized_total` counters. `dashboards`/Grafana dashboards continue to work; queries that filtered only on `result` should be updated to also project `transport`.

- **NornicDB browser UI uses Bolt over WebSocket end-to-end.** The embedded admin UI swapped its HTTP `/tx/commit` Cypher transport for the official `neo4j-driver` browser build over `ws://` / `wss://`, configured automatically from the discovery response. Same-origin `nornicdb_token` cookie carries auth into every query so the UI's executeCypher path is one network round trip with no token-juggling JavaScript. Vite plugins (`neo4jBrowserChannelPlugin`, `nodeShimPlugin`) wire the driver's browser channel correctly under Vite 8 / Rolldown. The HTTP server's UI handler now serves SPA routes with trailing slashes (`/databases/`) directly instead of returning HTTP 400 — refreshing on any nested route works.

- **Per-database search index master switches and warming triggers.** Four new orthogonal keys configure BM25 fulltext and vector ANN behavior independently per database:
  - `NORNICDB_SEARCH_BM25_ENABLED` (boolean, default `true`) — master switch for BM25 fulltext search.
  - `NORNICDB_SEARCH_BM25_WARMING` (enum: `startup`|`lazy`, default `startup`) — eager build at boot or deferred until first query.
  - `NORNICDB_SEARCH_VECTOR_ENABLED` (boolean, default `true`) — master switch for every vector search strategy (HNSW, IVF-HNSW, brute-force, GPU, Metal, Qdrant pass-through). When false, node embeddings are NOT iterated into the in-memory ANN substrate — the strongest available memory-pressure lever.
  - `NORNICDB_SEARCH_VECTOR_WARMING` (enum: `startup`|`lazy`, default `startup`).

  Defaults reproduce today's behavior; existing deployments need no change. Configurable via env, CLI flags (`--search-bm25-enabled`, etc.), `nornicdb.yaml` global `memory:` block, and yaml `databases:` map for per-database overrides. Runtime overrides via `PUT /admin/databases/{name}/config` always win over global defaults in **both directions** (per-DB `true` enables a globally-disabled index; per-DB `false` disables a globally-enabled one). Lazy-warming is a synchronous-wait contract: the first inbound search request from any entry point (HTTP, Bolt, GraphQL, gRPC, Cypher procedures) blocks inside `Service.EnsureWarm` until the build completes; concurrent first-readers all wait on the same `sync.Once`. The build runs in the DB's long-lived context so a request that times out during the wait does NOT abort the build.

  Migration: zero. Documented in [`docs/operations/configuration.md#per-database-search-index-control`](docs/operations/configuration.md), [`docs/operations/low-memory-mode.md`](docs/operations/low-memory-mode.md), [`docs/user-guides/hybrid-search.md`](docs/user-guides/hybrid-search.md), and the openapi spec. See `docs/plans/per-database-search-index-flags-plan.md` for design context.

- **Lucene wildcard parity for fulltext indexes.** `db.index.fulltext.queryNodes` and `db.index.fulltext.queryRelationships` accept all three Lucene wildcard shapes:
  - `*` — `MatchAllDocsQuery`; every document in the index.
  - `*:*` — Solr-style equivalent of `*`.
  - `<prop>:*` — field-presence query; every doc that has a non-empty value for the named property.

  Each shape honors the index's declared scope (label list for nodes, relationship-type list for edges) and declared property allowlist. An undeclared field returns empty (matching Neo4j-Lucene posting-list semantics). The previous behavior — wildcard queries returning 0 rows or, conversely, returning every node regardless of label scope — is fixed.

- **Relationship-scoped fulltext indexes.** `CREATE FULLTEXT INDEX <name> [IF NOT EXISTS] FOR ()-[r:Type]-() ON EACH [r.prop1, r.prop2]` (Neo4j 5.x DDL form) is now supported. `db.index.fulltext.queryRelationships('idx', '...')` scans only relationships whose type matches the index's declared scope, instead of every edge in the graph. Persistence is forwards/backwards compatible: the new `RelationshipTypes` schema field uses `omitempty`, so old binaries reading new files see no extra key, and new binaries reading old files see an empty slice (which falls back to the legacy unscoped behavior). No on-disk schema-version bump.

- **`/cyber` demo route — cyber-physical graph visualization.** Interactive 3D visualization seeded with sectors, hyperlanes, and traversable paths against a `cyber_demo` database, exercising the same hot-path Cypher cookbook as `/demo` (UnwindSimpleMergeBatch + UnwindMultiMatchCreateBatch). Pinned for benchmark and operator-demo scenarios.

### Changed

- **`shortestPath` traversal latency cut ~400× on the demo workload (M3 Max, ~1 000 nodes / ~5 000 edges).** Profile-led cleanup spanning storage, Cypher, and UI:
  - `AsyncEngine` adds a per-node inverted index over `edgeCache` so `GetOutgoingEdges` / `GetIncomingEdges` run in O(degree) instead of O(total cached edges). The BFS-frontier full-cache scan that scaled with total seeded edges is gone.
  - `BadgerEngine` adds an edge-body cache and per-node adjacency-ID cache. BFS-style reads on a stable graph skip Badger entirely after the first visit. Cache returns shared pointers (read-only contract) so repeated hits don't pay copyEdge.
  - New `AdjacentEdgesEngine` capability fetches both directions in a single view txn; plumbed through `AsyncEngine`, `NamespacedEngine`, and `WALEngine`.
  - `NamespacedEngine.toUserEdge` / `toUserNode` drop a deep-copy branch; all `Get*Edges` callers treat results read-only and clone via `CopyNode`/`CopyEdge` before mutating.
  - Cypher `shortestPath` BFS now uses parent-pointer reconstruction instead of per-neighbor `GetNode` during traversal; one `BatchGetNodes` at the end materializes the path. Calls `GetAdjacentEdges` when the storage chain supports it.
  - Cypher `findNodeByPattern` consults `SchemaManager.PropertyIndexLookup` before falling back to a label scan (mirrors `merge.go`).

  Cumulative result on the in-process bench: warm bench 14.5 ms / 156K allocs → 36 µs / 229 allocs; latency mean ~12 ms → 874 µs; latency p99 ~26 ms → 2.2 ms.

- **Strict-typed property round-trip preserved end-to-end.** A long-standing widening regression — caller writes `[]float64` / `[]string` / `[]int64`, storage hands back `[]interface{}` on every read — is fixed. The msgpack property codec inspects array headers and decodes homogeneous arrays into their declared concrete slice types; mixed arrays still fall back to `[]interface{}`. Maps recurse the same way. The Cypher path's `substituteParams` short-circuits typed list parameters (`$rows = []float64`) so they stay as `$name` references through the parser instead of being stringified into Cypher list literals (which forced re-decode as `[]interface{}`). Threaded `ctx` through ~70 expression-evaluator functions across binding-where, case, comparison, operators, math, traversal, link-prediction, knowledge-policy, vector procs, and APOC helpers so `$param` references resolve at evaluate time inside `reduce()`, list comprehensions, `WHERE`, and every other expression context — no widening, no re-parse.

- **`db.index.vector.queryNodes` returns empty results with a WARN log on vector-disabled databases** instead of erroring or instantiating a fresh enabled service that bypasses the operator's flag. Composite Cypher pipelines that gracefully handle empty vector results continue to succeed; operators see the misconfiguration in `subsystem=vector_search` log lines.

- **Qdrant gRPC bridge honors the per-DB vector master switch.** External Qdrant clients querying a database with `NORNICDB_SEARCH_VECTOR_ENABLED=false` see a deterministic structured error rather than a service whose ANN substrate isn't populated.

### Fixed

- **`mcp-neo4j-memory` regressions — three independently reproducible Cypher correctness defects resolved.**
  1. **Map-parameter property access stored as literal text.** `WITH $entity AS entity MERGE (e:Memory {name: entity.name})` previously stored the literal string `"{name:'Alice', type:'Person'}.name"` instead of evaluating `entity.name`. The WITH-binding substitution treated `entity.<key>` as a standalone identifier and replaced just `entity`, leaving an orphaned `.name` suffix. Fixed by expanding `<ident>.<key>` into the property's Cypher literal value before the standalone-identifier replacer runs. Token boundary checks (word / underscore / dot) keep unrelated identifiers untouched. The same pattern in `UNWIND [$r] AS r MATCH (a),(b) WHERE a.name = r.source AND b.name = r.target MERGE (a)-[:REL]->(b)` now matches and creates the expected edge.

  2. **Aggregating RETURN after CALL…YIELD…WITH…WHERE returned 0 rows.** A bare `RETURN collect(...)` is required by Cypher to produce exactly one row even when the WHERE filters every input. The `MATCH-WITH-RETURN` aggregation path looked up `cr.values["entity.name"]` (a literal string keyed by alias) and silently produced an empty list when `collect(entity.name)` ran over it. New `resolveInnerForRow` evaluates each aggregate's inner expression three ways — bare alias, `alias.property` against a stored `*storage.Node`, or general expression with WITH-bound nodes as context — and applies uniformly to `count`, `sum`, and `collect`. WITH-followed-by-WHERE-followed-by-aggregating-RETURN now produces exactly one row holding the aggregation's identity value (`collect → []`, `count → 0`).

  3. **`CALL dbms.components()` reported hard-coded "1.0.0".** Wired to `pkg/buildinfo.Version()` (which loads from the embedded `VERSION` file at build time). Same fix applied to `dbms.listConfig`'s `nornicdb.version` row. `cypher-shell --version`-style probes now see the actual running binary version.

- **Cypher `SET` errors no longer silently swallowed.** A conflict-rejected `UpdateNode` / `UpdateEdge` previously looked like a successful SET to `ExecuteCypher` callers — the SET-RETURN row carried the pre-update state on disk while the executor reported success. Errors now propagate so MVCC commit conflicts surface as loud query failures instead of silent data loss. Paired with: `RebuildTemporalIndexes` + `RebuildMVCCHeads` moved from a background task into the synchronous tail of `Open()` so first-query writes can't race a startup head-rewrite that clears the entire `prefixMVCCNodeHead` range mid-commit.

- **DROP INDEX now tears down per-property vector data.** Previously `DROP INDEX <name>` only removed the schema entry, leaving per-property vector data orphaned in the in-memory `vectorIndex` / HNSW / cluster substrates. A subsequent `CREATE VECTOR INDEX` with the same name appeared to "do nothing" because the orphaned state shadowed the new one. New `search.Service.RemovePropertyVectorIndex` tears the in-memory state down; `executeDropIndex` calls it before returning so a recreate from scratch is clean.

- **WAL chunk recovery now batches snapshot restore.** `RecoverWithTransactions` and `RecoverFromWALWithResult` were calling `BulkCreateNodes` / `BulkCreateEdges` with the entire snapshot in one go, exhausting Badger's per-transaction write budget on snapshots above ~10 K nodes/edges. New `BulkCreateNodesForRecovery` / `BulkCreateEdgesForRecovery` chunk the restore into transaction-sized batches.

- **Search-flag precedence honored end-to-end.** Three independent gaps in the v1.1.1 search-flag contract caused operator-set values to be silently dropped at startup:
  1. `cmd/nornicdb/runServe` was hand-copying a subset of `cfg` fields into a fresh `nornicdb.DefaultConfig()`; the four `Search*` fields were missing from the copy block, so env+CLI values landed in `cfg` but never reached `dbConfig`. `dbConfig` is now an alias of `cfg` so any field added to `Config` flows through automatically.
  2. `nornicdb.Open` warmed search indexes in a background goroutine that raced `server.New`'s `SetDbSearchFlagsResolver`. When the resolver was nil at warmup time, default-DB warmup fell through to global defaults instead of per-DB overrides. New `Config.DeferSearchWarmup` + `db.MarkSearchWarmupReady` gate the warmup until the resolver is installed; `pkg/server` opts in.
  3. `applyEnvVars` unconditionally wrote `(true, "startup")` before checking the env var, breaking `LoadFromFile`'s precedence ladder — a YAML file setting `search_bm25_enabled: false` was silently overwritten when the env var was unset. The env path now only writes when the var is actually present.

  Operators who set `NORNICDB_SEARCH_BM25_ENABLED=false` (or the CLI / YAML equivalents) now see the flag honored from the first warmup line in the log.

- **Transactional `MATCH … MERGE` correctly routes before `CREATE`.** A regression where a `MERGE` inside a transaction containing a preceding `MATCH` was being dispatched to the `CREATE` path instead of the merge path is fixed. The dispatcher now consults `MERGE` keywords ahead of `CREATE`.

### Internal

- **38 deeply-asserted regression tests added** across `pkg/cypher` (13 in `mcp_memory_bugs_test.go` covering every shape from the bug report plus relationship-side parity), `pkg/storage` (6 in `schema_fulltext_relationship_test.go` proving forward + backward + idempotent persistence), `pkg/cypher/demo_shortest_path_bench_test.go` (latency distribution + three benchmarks), `pkg/storage/async_engine_edge_index_test.go` (edge-cache inverted index across CRUD + flush + bulk paths), `pkg/storage/async_engine_label_index_test.go` (labelIndex flush eviction, `GetNodesByLabel` cache+engine merge), and the search-flag suites (`pkg/search/index_flags_test.go` and `pkg/server/server_search_flags_test.go`).
- **CI: storage tests split into smaller test groups** so the runner doesn't exceed memory limits on shared CI hardware.
- **Bolt-side benchmarks**: `BenchmarkBolt_StreamRecords_EndToEnd_*` plus per-transport variants (`tcp`, `tcp_tls`, `ws`, `ws_tls`) ship as part of the regression suite.

### Documentation

- **`docs/user-guides/connecting-bolt.md`** — driver-by-driver connecting guide for the four Bolt-over-WebSocket transports plus driver-side aliases (`bolt+ssc://`, `neo4j://`, `neo4j+s://`, `neo4j+ssc://`). Per-driver code snippets (Java, Python, JavaScript browser, JavaScript Node, .NET, Go).
- **`docs/user-guides/graph-traversal.md`** — full Memgraph-style traversal vocabulary documented with the workload→procedure reference table: BFS / DFS via `apoc.path.expandConfig`, weighted shortest path via `apoc.algo.dijkstra` and `apoc.algo.aStar`, all-simple-paths via `apoc.algo.allSimplePaths`, neighborhood queries via `apoc.neighbors.byhop`/`tohop`, subgraph extraction via `apoc.path.subgraphNodes`, centrality (PageRank, betweenness, closeness), community detection (Louvain, label propagation, weakly connected components), and the GDS link-prediction family (`commonNeighbors`, `adamicAdar`, `jaccard`, `preferentialAttachment`, `resourceAllocation`, `predict`) plus `gds.fastRP.stream` for node embeddings.
- **`docs/plans/bolt-over-websocket-plan.md`** — full implementation plan with phasing, test coverage matrix, and Neo4j-compatibility notes for the WS transport landing.
- **`docs/plans/operator-declared-graphql-schema-plan.md`** — design plan for operator-declared GraphQL schema (read-only with relationship traversal, SDL stored in system DB, no auto-inference). Implementation deferred; plan committed as the source of truth for the future cut.

### Technical Details

- **Range covered**: `v1.1.1..main` (31 commits)
- **Primary focus areas**: Bolt-over-WebSocket transport multiplexing with full TLS / origin / mTLS / cert-rotation surface, neo4j-driver browser-build integration in the embedded UI, per-DB search index master switches with synchronous lazy-warming, mcp-neo4j-memory Cypher parity (map-param property access, fulltext label/type scope, Lucene wildcard family, post-YIELD aggregation), shortestPath traversal latency reduction via per-node edge-cache indexes and parent-pointer BFS, typed property round-trip preservation through msgpack codec + Cypher param substitution, deterministic DROP INDEX teardown of vector substrates, WAL recovery batching for large snapshots.

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
