## Acceptance conventions and dependency graph

This is an implementation checklist; artifact creation does not complete it.
All tasks remain unchecked. Group IDs refer to the [audit matrix](audit.md);
named local tests below are proposed regression IDs, not existing passing
tests. Expand each group into exact statements and expected columns, native
types, errors, ordering and effects before implementation.

Dependencies: phase 1 requires 0; 2 requires 1; 3 requires 1 and phase 2's
shared clause/scope contracts; 4 requires 1; 5 requires 1 and the Enterprise
transaction lifecycle; 6 requires 1; 7 requires 2 and relevant phase 6 types;
8 requires 1 plus #935 and the corresponding Enterprise plan phases;
9 requires all in-scope preceding phases. Independent work can proceed once
its actual prerequisites are met. No staffing or elapsed-time assumptions.

For every bug: write the exact failing regression, run and record its failure,
then fix and record its pass. Preserve the baseline successes. Mutations
require fresh-session reads, rollback, cancellation and reopen verification;
durable child batches have separate commit semantics.

## 0. Pin independent contracts and preserve the baseline

- [ ] 0.1 In [testing/cypher](../../../testing/cypher), pin 2026.09.0 Community/Enterprise artifacts, digests and driver versions alongside the existing 5.26 manifest; obtain a licensed oracle or mark Enterprise acceptance blocked. Record results by release, edition, language, parser, transport and transaction mode.
- [ ] 0.2 Expand every audit group V01-O01 into exact fixture IDs and expected outcomes, using upstream source/docs plus oracle execution; distinguish stable, preview, internal-only and out-of-scope persisted database defaults. No current-version fixture may silently run on the 5.26 oracle.
- [ ] 0.3 Capture existing unprefixed/5 query and API behavior with `TestCypher25ExistingContracts` and `TestCypher25RetainedExtensions` (R01). Inventory actually supported upstream-removed constructs, with response and real-effect fixtures; classify intentional extension differences rather than deleting APIs.
- [ ] 0.4 Turn P01, P25 and P31/P43 into failing `TestCypher25PrefixAdmission`, `TestCypher25PublicFunctionAdmission` and `TestCypher25PropertyExists` regressions; record the current rejection/zero count before any fix.
- [ ] 0.5 Publish baseline evidence in the compatibility documentation and inventory, including named read/write benchmarks and allocation/RSS measurements. Run the existing conformance/differential commands only against their declared oracle; do not treat parser agreement as independent evidence.

## 1. Carry query-selected language through the shared pipeline

- [ ] 1.1 Add validated NORNICDB_CYPHER_VERSION=5|25 configuration in [config.go](../../../pkg/config/config.go) and wire its immutable default through [executor.go](../../../pkg/cypher/executor.go), [database creation](../../../pkg/nornicdb/db.go) and [startup](../../../cmd/nornicdb/main.go). `TestCypher25ConfiguredDefault` covers absent=5, both valid values, present-empty/invalid rejection, config-file/env entry points and database/session/cache-policy executors without per-query environment reads (V01).
- [ ] 1.2 Update [statement_framing.go](../../../pkg/cypher/statement_framing.go) and [executor_entry.go](../../../pkg/cypher/executor_entry.go) to resolve prefix over configured default with source offsets before normalization/cache lookup. Make `TestCypher25PrefixAdmission` pass for both parsers, including the 2-default x 3-prefix matrix, EXPLAIN/PROFILE/options/comments and unknown-version errors before writes (V01).
- [ ] 1.3 Propagate immutable query context through bound execution, nested CALL/EXISTS/COUNT/COLLECT, streams and [multidb/routing.go](../../../pkg/multidb/routing.go). Add `TestCypher25LanguagePropagation` for USE/aliases, explicit transactions and mixed prefixed statements with correct identity/database retention, no default reselection in nested execution (V01).
- [ ] 1.4 Partition analysis, syntax, prepared, plan and result caches in [query_info.go](../../../pkg/cypher/query_info.go), [cache_key.go](../../../pkg/cypher/cache_key.go) and [cache.go](../../../pkg/cypher/cache.go). `TestCypher25LanguageCacheIsolation` must distinguish resolved 5/25 under configured defaults and prefix overrides, plus schema/security changes (V01/A01).
- [ ] 1.5 Extend [ANTLR grammar/adapter](../../../pkg/cypher/antlr) as a front end to the same operators; regenerate with `make antlr-generate`. Test invalid tails/effect-safe admission, no query-text replay and no retry as 5; preserve framing/fast-path regressions.
- [ ] 1.6 Document env-default/prefix precedence and retained extensions. Verify upgrade/reopen preserves configured behavior and default 25 enables unprefixed new syntax; no automatic default cutover, persisted database-language migration or default-language DDL (V01/R01).

## 2. Small clauses, lexical constructs and callable functions

- [ ] 2.1 Add bound LET/FILTER/FOR and RETURN/WITH ALL to [pipeline_executor.go](../../../pkg/cypher/pipeline_executor.go) and projection helpers. `TestCypher25LetFilterFor` must verify the source-research pair's exact two rows, LET scope retention, null/empty iteration and OPTIONAL MATCH filtering placement (Q01/Q03).
- [ ] 2.2 Converge static admission, evaluation and metadata in [function_catalog.go](../../../pkg/cypher/function_catalog.go), [functions_neo5.go](../../../pkg/cypher/functions_neo5.go) and [row_expression.go](../../../pkg/cypher/row_expression.go). Make the failing P25/P31 regressions pass; cover IS LABELED, cardinality and the full alias/string/null-signature inventory with `TestCypher25FunctionsE03` (E03).
- [ ] 2.3 Implement numeric-leading parameters, replace-limit and constant-import semantics; preserve existing hyperbolic/char_length controls. Add `TestCypher25ExpressionsE01` for type/arity/null/boundary cases and scalar-subquery aggregation scope (E01).
- [ ] 2.4 Reuse temporal/quantifier helpers for patterned constructors, format, allReduce, dynamic labels/types and coll functions. `TestCypher25FunctionsE02` must cover each inventoried overload, ordering, nested lists and invalid inputs; verify P41's exact syntax with the oracle before fixing it (E02).
- [ ] 2.5 Extend lexer, canonicalization and [row expressions](../../../pkg/cypher/row_expression.go) for interpolation/map comprehensions and broadened conversions. `TestCypher25ExpressionsE04` must test nested quoting/maps, parameters, duplicate keys, nulls and scope without textual replay (E04).
- [ ] 2.6 Update clause/function reference examples and signatures; measure unchanged reads and new LET/FILTER/FOR work, and rerun existing projection/function regressions in both parsers.

## 3. Whole-table composition and aggregation

- [ ] 3.1 Add typed NEXT/WHEN/ELSE/braced-UNION composition to the shared pipeline and [pipeline_call_operator.go](../../../pkg/cypher/pipeline_call_operator.go). `TestCypher25CompositionQ02` must cover output-column compatibility, branch selection and whole-table NEXT rather than per-row CALL (Q02).
- [ ] 3.2 Extend [pipeline_aggregate_stream.go](../../../pkg/cypher/pipeline_aggregate_stream.go) and [subquery_value_expression.go](../../../pkg/cypher/subquery_value_expression.go) for GROUP BY, imports and aliases. `TestCypher25GroupingQ03` must cover aggregate-after-NEXT/CALL/UNION, empty groups, nulls and invalid references (Q02/Q03).
- [ ] 3.3 Add `TestCypher25CompositionEffects`: unselected writes/procedures never execute; later failure rolls back uncommitted selected writes; fresh-session verification confirms exact effects. Preserve established read/write transitions while adding current ones (Q01/Q02).
- [ ] 3.4 Document scope/whole-table examples and benchmark multirow NEXT aggregation versus existing CALL/UNION workloads; preserve streaming where whole-table semantics do not require materialization.

## 4. Current match/path semantics

- [ ] 4.1 Extend [pattern_parser.go](../../../pkg/cypher/pattern_parser.go) and [pipeline_pattern_template.go](../../../pkg/cypher/pipeline_pattern_template.go) for explicit match modes, quantified paths and group variables. `TestCypher25MatchModesP01` must distinguish relationship reuse, cross-pattern uniqueness, bounds and null/empty group bindings (P01).
- [ ] 4.2 Add restrictive selectors and ACYCLIC to [shortest_path.go](../../../pkg/cypher/shortest_path.go) and shared traversal. `TestCypher25PathSelectorsP02` must cover tied groups, parameter counts, pre/post filters and node versus relationship uniqueness (P01/P02).
- [ ] 4.3 Retain valid simple-quantifier fast paths in [relationship_quantifier_rewrite.go](../../../pkg/cypher/relationship_quantifier_rewrite.go); replace only advanced shapes proven inadequate by failing tests. Verify authorization/cancellation during traversal and no combinatorial unbounded expansion.
- [ ] 4.4 Document selectors/modes and run sparse/dense/cyclic workload benchmarks with path counts and allocation limits; retain existing quantified/shortest-path tests.

## 5. Independent concurrent batch lifecycle

- [ ] 5.1 Extend [executor_subqueries.go](../../../pkg/cypher/executor_subqueries.go) and the CALL operator with typed concurrency/status/error/retry/DISJOINT BY modifiers, sharing the canonical transaction lifecycle. `TestCypher25BatchModifiersB01` covers validation before effects and implicit-versus-explicit transaction restrictions (B01).
- [ ] 5.2 Add bounded child execution and conflict scheduling. `TestCypher25BatchCommitOnce` verifies overlap/AUTO/NONE, retry, cancellation and exactly-once successful commits with fresh-session reads, failure injection and race checks (B01).
- [ ] 5.3 Verify retained earlier child commits after later failure, correct status rows and CDC/dual-identity attribution against the Enterprise oracle. Do not roll back durable children or duplicate their events; document partial-success behavior and benchmark contention (B01/I01).

## 6. Native typed values and persistence

- [ ] 6.1 Add VECTOR/UUID types using [value helpers](../../../pkg/cypher/value_type_names.go) and [storage/property_codec.go](../../../pkg/storage/property_codec.go). `TestCypher25TypedValuesT01T02` covers constructors, ranges, widths, dimensions, nulls, equality/order/hashing and constraints; keep existing list/string values unchanged (T01/T02).
- [ ] 6.2 Wire parameter conversion, index/cache keys, MVCC, backup and CDC representations. `TestCypher25TypedPropertyReopen` covers Memory/Badger/wrappers, reopen/restore, existing-format backward reading and failed-write rollback without mandatory existing-data migration (T01/T02).
- [ ] 6.3 Implement negotiated Bolt/HTTP/Query API value rendering in [packstream.go](../../../pkg/bolt/packstream.go) and shared server codecs. `TestCypher25TypedProtocolRoundTrip` compares supported driver values and unsupported-protocol errors; no lossy string/list fallback (T01/T02/I01).
- [ ] 6.4 Document constructors and storage/protocol boundaries; measure typed property IO, serialized size and allocations, including old-value workloads and restore compatibility.

## 7. Indexed SEARCH and open graph types

- [ ] 7.1 Add bound VECTOR/FULLTEXT SEARCH to MATCH/OPTIONAL MATCH with [existing retrieval services](../../../pkg/search), not reconstructed CALL text. `TestCypher25SearchS01` covers index kind, SCORE scope, optional null rows, filters, analyzer and pagination validation (S01).
- [ ] 7.2 Extend [schema.go](../../../pkg/storage/schema.go) index target/filter/options metadata and real option effects. `TestCypher25SearchIndexReopen` verifies multi-target/filter persistence, transaction-visible writes, rollback and #938 authorized pagination; ignored settings never report success (S01).
- [ ] 7.3 Add graph-type parsing and canonical provenance via [schema_contracts.go](../../../pkg/cypher/schema_contracts.go) and [storage enforcement](../../../pkg/storage/schema_write_checks.go). `TestCypher25GraphTypesG01` covers implied labels/endpoints, open unconstrained data, all mutation routes and conflicts with existing data (G01).
- [ ] 7.4 Add atomic SET/ADD/ALTER/DROP and SHOW/AS GRAPH/classification projections. `TestCypher25GraphTypeReopen` verifies exact recreatable schema, rollback and fresh-session/reopen state while preserving native schema extensions (G01/A01).
- [ ] 7.5 Document SEARCH/graph types and distinctions from native extensions; compare deterministic scores/errors and named SciFact recall/ranking, index memory/build time and schema-write latency without claiming identical index internals.

## 8. Current metadata, Enterprise and operational integration

- [ ] 8.1 Extend [SHOW implementations](../../../pkg/cypher/show_admin.go) and [procedure registry](../../../pkg/cypher/procedure_registry_builtin.go) for resolved-25 columns/types/nulls/currentQueryProgress and composable SHOW/TERMINATE. `TestCypher25ShowA01` compares exact shapes and privileges under env defaults and prefix overrides; resolved-5 responses and callable extensions remain available (A01/R01).
- [ ] 8.2 Extend #935 canonical role resolution for AUTH RULES, trusted OIDC attributes/native tags and administration/credential export. `TestCypher25AuthRulesA02` covers impersonation, privilege invalidation and denial effects; never treat query parameters as trusted claims (A02).
- [ ] 8.3 Extend the [Enterprise plan](../enterprise-cdc-impersonation-query-api/tasks.md) for current CDC fields and Query API transaction/timing/notification contracts. `TestCypher25CurrentServerI01` separates server API from query language and verifies transaction ownership, durable events and cross-transport identity (I01).
- [ ] 8.4 Inventory seed/reload public operations and map only implementable equivalents to native restore/plugin services. `TestCypher25OperationsO01` covers persisted effects, permissions and explicit unsupported errors for foreign formats; preserve existing supported operations (O01).
- [ ] 8.5 Document current-release deltas separately from historical 5.26 contracts. Report blocked Enterprise fixtures and retained extensions, with no fake operational success or global API removals.

## 9. Release acceptance and recurring upstream tracking

- [ ] 9.1 Extend [conformance CI](../../../.github/workflows/cypher-conformance.yml) and differential tooling with the pinned current oracle. Run both parsers over Bolt/HTTP/Query API, autocommit/explicit transactions, ordinary/impersonated identities and persistent reopen/failure cases; record each exact command and result.
- [ ] 9.2 Run `make cypher-conformance`, `make cypher-differential` and `make test-parsers` against their declared configurations, plus targeted package/race/coverage checks. Require >90% new-code coverage (core target 95%); missing Enterprise results remain blocked, not passed.
- [ ] 9.3 Compare named before/after read/write, clauses, composition, paths, batches, typed IO, SEARCH and schema workloads. Report correctness, ops/sec, p50/p95/p99, allocations/RSS and recall; investigate >5% performance or >10% memory regressions on unchanged workloads.
- [ ] 9.4 Publish exact supported/partial/missing/untested/retained-extension status in [compatibility documentation](../../../docs/neo4j-migration/cypher-compatibility.md). Verify env-default/prefix precedence and existing supported contracts before enabling 25; no automatic default cutover or broad compliance claim with untested or undisclosed differences.
- [ ] 9.5 Add a dated upstream release/source inventory and reviewed drift workflow: stable target updates require exact fixtures, owners and evidence; preview/unreleased/internal features remain a separate watchlist. Update changelog and release notes only for actually delivered behavior.
