## Architecture decision: inline the existing pipeline

The user explicitly prefers inline support in the main pipeline. Add clause
kinds, expression nodes and shared semantic helpers there; do **not** build a
second language engine, top-level dispatcher, or generic interpreter.
Keep fused fast paths where they prove the entire shape and use the same
semantics. Update the ANTLR grammar/adapter as another front end, not an oracle.
No query-string translation and re-entry to implement NEXT/FILTER/WHEN/SEARCH.

The user requires query-expressed overrides and a process default configured
as `NORNICDB_CYPHER_VERSION=5|25`, with no breaking existing queries or APIs.
Unprefixed statements use that default; unset retains 5. There is no automatic
cutover to 25 or persisted database-language migration.
Parse a small immutable language field once, pass it through existing context/
bound execution and use it only at differences. Both versions execute the same
operators. Explicit 5 is a supported language contract, not an old fallback.

## Evidence and baseline

See [audit](audit.md) and [raw probe outcomes](evidence/probe-results.jsonl).
The current prefix rejection is proven; 43 small embedded probes per parser
are not a full cross-protocol certification. Preserve existing successes and
convergence work, then fix missing/rejected/wrong-result features using failing
regressions and a pinned 2026.09.0 oracle.

## Implementation map

Paths are existing; new modules are proposals and should be added only when a
cohesive helper cannot fit the existing file-size boundary.

| Surface | Shared implementation changes |
| --- | --- |
| [statement_framing.go](../../../pkg/cypher/statement_framing.go), [executor_entry.go](../../../pkg/cypher/executor_entry.go) | Return preamble metadata rather than validate/drop version; select language before version-sensitive canonicalization, parameter parsing, compilation and cache lookup; preserve source offsets |
| [query_info.go](../../../pkg/cypher/query_info.go), [cache_key.go](../../../pkg/cypher/cache_key.go), [cache.go](../../../pkg/cypher/cache.go) | Include resolved language and semantic/schema revision in caches, alongside #935 security scope; update new-clause read/write analysis and all prepared/syntax/plan/result caches |
| [pipeline_executor.go](../../../pkg/cypher/pipeline_executor.go), [pipeline_dispatch.go](../../../pkg/cypher/pipeline_dispatch.go), [executor_query_routing.go](../../../pkg/cypher/executor_query_routing.go) | Inline clause/operator admission, typed bound composition and effect-safe rejection; no second version router |
| [CypherParser.g4](../../../pkg/cypher/antlr/CypherParser.g4), [CypherLexer.g4](../../../pkg/cypher/antlr/CypherLexer.g4), [antlr/clauses.go](../../../pkg/cypher/antlr/clauses.go) | Current grammar, tokens/contextual keywords, lexical interpolation/map comprehension and adapter to shared operators; regenerate rather than hand-edit generated Go |
| [row_expression.go](../../../pkg/cypher/row_expression.go), [functions_neo5.go](../../../pkg/cypher/functions_neo5.go), [function_catalog.go](../../../pkg/cypher/function_catalog.go), [row_extension_expression.go](../../../pkg/cypher/row_extension_expression.go) | One callable/static-validation/catalog contract, fixing registered-but-unreachable ceiling and missing functions; typed expressions for map/string constructs |
| [pipeline_aggregate_stream.go](../../../pkg/cypher/pipeline_aggregate_stream.go), [subquery_value_expression.go](../../../pkg/cypher/subquery_value_expression.go) | Explicit grouping keys, constant imports, alias mapping, empty/null semantics and aggregation across NEXT/CALL/UNION |
| [pattern_parser.go](../../../pkg/cypher/pattern_parser.go), [relationship_quantifier_rewrite.go](../../../pkg/cypher/relationship_quantifier_rewrite.go), [shortest_path.go](../../../pkg/cypher/shortest_path.go), [pipeline_pattern_template.go](../../../pkg/cypher/pipeline_pattern_template.go) | Structured match/path modes, quantified path group variables and selectors; retain simple optimized traversal but remove semantically inadequate rewrites for advanced shapes |
| [executor_subqueries.go](../../../pkg/cypher/executor_subqueries.go), [pipeline_call_operator.go](../../../pkg/cypher/pipeline_call_operator.go) | Typed batch modifier contract, bounded concurrent child execution, retry/status and resource scheduling, inherited identity/language and no accidental parent transaction |
| [value_type_names.go](../../../pkg/cypher/value_type_names.go), [value_equality.go](../../../pkg/cypher/value_equality.go), [temporal_values.go](../../../pkg/cypher/temporal_values.go), [storage/property_codec.go](../../../pkg/storage/property_codec.go) | Add real VECTOR/UUID value types and exact formatting/equality/order/hashing; propagate through parameter conversion, property codecs, constraints, indexes, caches, MVCC and CDC |
| [bolt/packstream.go](../../../pkg/bolt/packstream.go), [server/server_db.go](../../../pkg/server/server_db.go) | Negotiate pinned driver/wire value support and current diagnostics; share value codecs with planned Query API, not generic string/list coercion |
| [call_vector.go](../../../pkg/cypher/call_vector.go), [call_fulltext.go](../../../pkg/cypher/call_fulltext.go), [schema.go](../../../pkg/storage/schema.go), [search/](../../../pkg/search) | Native SEARCH operators reuse retrieval services; persist multi-target/filter index metadata and real option effects; do not rewrite clauses into CALL text |
| [schema_contracts.go](../../../pkg/cypher/schema_contracts.go), [storage/constraint_contracts.go](../../../pkg/storage/constraint_contracts.go), [storage/schema_write_checks.go](../../../pkg/storage/schema_write_checks.go), [badger_transaction_schema.go](../../../pkg/storage/badger_transaction_schema.go) | Open graph types lower to canonical schema rules with provenance/classification; reuse enforcement but distinguish native contract extensions |
| [executor_show.go](../../../pkg/cypher/executor_show.go), [show_admin.go](../../../pkg/cypher/show_admin.go), [show_schema_values.go](../../../pkg/cypher/show_schema_values.go), [procedure_registry_builtin.go](../../../pkg/cypher/procedure_registry_builtin.go) | Explicit-25 columns/types/catalogs and composable SHOW/TERMINATE; preserve existing API availability and response contracts outside opt-in language differences |
| [statement_framing.go](../../../pkg/cypher/statement_framing.go), [antlr grammar](../../../pkg/cypher/antlr) | Accept optional CYPHER 5 / CYPHER 25 preambles in one shared grammar; both parsers consume the preamble themselves so callers pass statements as written. No language default setting, config field or DDL is introduced |
| [testing/cypher/](../../../testing/cypher), [scripts/cypher-tck/](../../../scripts/cypher-tck), [cypher-conformance.yml](../../../.github/workflows/cypher-conformance.yml) | Current oracle lane plus existing 5/TCK evidence; explicit release/language/edition/protocol matrices and release-drift reporting |

## Shared execution details

### One shared grammar, optional headers

There is one grammar for Cypher 5 and Cypher 25 surface. The optional
`CYPHER 5` / `CYPHER 25` header is accepted and discarded by both parsers; no
language default setting, config field, database field or default-language DDL
is introduced, and no execution context is keyed on the version. Unprefixed
statements run through the same clause kinds and operators as prefixed ones.

The SRD parser is permissive: `LET`, `FILTER`, `FOR` and correlated unscoped
`CALL` bodies execute without a header. The ANTLR parser keeps the strict
Cypher 5.26 contract for those additive forms (it requires `CYPHER 25`) and
still rejects implicit CALL imports, but it also parses and discards the
preamble itself, so callers never strip headers before invoking `Parse` or
`Validate`. Both parsers feed the same pipeline clause kinds and operators;
ANTLR is a syntax front end, not a separate execution path.

Neo4j's own default configuration and upstream syntax removals are reference
facts, not automatic implementation requirements. Preserve existing supported
input and APIs, including retained extensions.
Report those differences honestly rather than claiming identical rejection
behavior. The source-backed examples and scope decision are recorded in
[language selection research](language-selection-research.md).

Language is not an arbitrary bundle of configurable compatibility flags.
Admission/semantic differences are named tests for the two supported versions.
Current server HTTP/protocol additions are selected by server/API contract,
not automatically hidden whenever query text says CYPHER 5.

### Clauses, expressions and scope

- LET binds without projecting away the row; FILTER filters existing rows;
  FOR shares UNWIND iteration with its own syntax/edge rules. Do not alias
  FILTER blindly to a clause-attached WHERE.
- NEXT passes a completed result table, not one row at a time. Represent
  WHEN/ELSE and braced UNION as bound compositions with declared output
  columns, branch selection and effect boundaries. Respect whole-table
  aggregation and empty inputs; only the chosen branch executes.
- GROUP BY is a projection/aggregation subclause, not an ignored suffix.
  Centralize grouping expressions, aliases, DISTINCT/ALL and implicit grouping.
  Imported outer constants must not become empty-input grouping keys.
- Map comprehension and interpolation need proper lexical nesting/escaping and
  variable scopes; update canonicalization and static analysis, not just regexes.
  Test duplicate/null/computed keys, nested constructs and error offsets.
- Function admission, argument validation, scalar/aggregate dispatch and SHOW
  FUNCTIONS must agree. The ceiling probe proves source registration is not
  sufficient. Implement every alias/overload in the release inventory through
  that shared contract, not copied APOC behavior with different null/order rules.
- Fix PROPERTY_EXISTS's silent false result first. Test all expression positions,
  nulls, absent properties and invalid entity/key types.

### Paths and batches

Use separate match-mode and path-mode fields. DIFFERENT RELATIONSHIPS applies
uniqueness across constituent patterns; REPEATABLE ELEMENTS permits reuse and
requires finite bounds where upstream requires them. ACYCLIC constrains nodes,
not merely edges. Quantified path groups retain list bindings, direction and
predicate scope. ANY/SHORTEST/k/GROUPS handle ties, parameter counts, zero-length
paths and pre/post-filtering exactly. Preserve authorization-filtered traversal
and cancellation. No arbitrary depth cap in place of declared semantics.

Concurrent CALL batches use existing transaction lifecycle, bounded workers
and independent committed results. Parse ON ERROR/RETRY/REPORT STATUS and
DISJOINT BY into metadata; do not ignore tails. AUTO needs real resource
inference; NONE disables only scheduling, not safety checks. Preserve already
committed batches on later failure, cancel active children and avoid duplicate
commits/events during retries. Share attribution/commit identity with #937.

### Values, storage and schema

VECTOR carries dimension and coordinate type, not just []float32; UUID carries
128 bits, not a string. Reuse native storage value patterns with explicit tags.
Extend equality/order/distinct/group/hash/index keys, type constraints and
serialization in one coordinated change. Test every numeric coordinate width,
overflow/NaN/infinity/null rules, dimension boundaries, UUID signed bit
conversion, property reads after restart, backups, WAL/MVCC/CDC and driver
negotiation. Older protocol limitations must match reference behavior, not a
generic string fallback. Do not require Neo4j's block-format internals.

Graph types are an open schema: unconstrained entities remain legal. Store
element-type identities, implied labels, source/target rules and provenance
alongside canonical constraints. SET/ADD/ALTER/DROP validate existing data
and commit atomically with schema publication. SHOW, SHOW AS GRAPH,
classification/enforcedLabel and recreatable statements come from that state.
Native ALLOWED/DISALLOWED and constraint blocks must not be advertised as
equivalent Neo4j graph types; reconcile only exact overlaps and document
intentional native extensions.

### SEARCH

Attach bound SEARCH to MATCH/OPTIONAL MATCH and execute native retrieval
without re-entering Cypher. Validate bound entity/index kind, input type,
filterable properties, analyzer and nonnegative integer LIMIT/SKIP/OFFSET
against the target. Preserve SCORE scope, optional null rows, security and
transaction-visible index contents. Index options must have actual behavior;
no successful acceptance of ignored quantization/filter/search settings.

Extend VectorIndex's single-label/property model to current target/filter
metadata. Reuse native HNSW/BM25 services but explicitly test score and retrieval
semantics. Do not declare exact parity merely because results look similar.
Use small exact datasets for deterministic score/error contracts and named
recall/ranking benchmarks for approximate behavior. Require native SciFact
quality preservation and #938 authorized-page integration.

## Current Enterprise plan integration and compatibility

Reuse the existing Enterprise plan's canonical auth, dual identity, native CDC
journal and Query API. Add current differences to those same implementations:

- SHOW timestamps/nulls/constraint types, composable administrative rows and
  currentQueryProgress; privilege-checked AUTH RULES, user tags/OIDC attributes
  and credentials-export capabilities via #935.
- Current CDC procedure output and typed values, including current timestamp
  when required; current Query API tx metadata/timeouts/timing/notification
  filters, authenticated transaction ownership and protocol diagnostics.
- Current catalogs/overloads: preserve existing procedure availability even
  where upstream 25 removed it, documenting retained extensions.
  Deprecated-but-supported upstream procedures remain supported with correct
  metadata. Do not equate deprecation with removal.
- Current database seed options and reloadProcedures: map native restore and
  runtime plugin mechanisms honestly; foreign Neo4j backups/JVM binaries are
  not compatible artifacts. Public operations without native equivalents
  remain explicit gaps until implemented; no fake operational success.

Replace hardcoded "only 5" admission for supported 25; replace preamble version
discard; eliminate accepted-but-ignored clause tails and wrong-value predicate
paths using failing reproductions; add current metadata when explicit 25 is
selected explicitly or through the configured default, while retaining 5
response contracts when 5 is resolved.
Upstream removals (entity RHS SET, same-clause MERGE references, indexProvider,
identifier/graph quoting, procedures/options) are compatibility-inventory
entries, not deletion targets. Preserve working native implementations.
Keep tested language differences, not "try new then old" branches.
Do not maintain separate data stores/pipelines per language.

## Delivery, risks and acceptance

Release order: oracle and language context; functions/small clauses;
composition/aggregation; path semantics and batch lifecycle; value persistence;
SEARCH/graph types; current Enterprise integration; full contract/release gate.
Independent modules may progress after shared prerequisites, but public support
claims wait for the declared acceptance scope, including documented extensions.

Before enabling explicit 25, verify existing queries/APIs and stored data remain
usable. Upgrade schema/value formats only under a tested version boundary with
backward reading of existing values; no existing data rewrite merely for
language selection. Downgrade must reject unreadable new formats or restore a
compatible backup. No automatic default cutover and no silent legacy runtime
fallback; changing the process default to 25 is an explicit operator choice.

Primary risks: hidden cross-version cache reuse, syntax normalization corrupting
new literals, NEXT per-row versus whole-table semantics, incomplete branch
rollback, combinatorial traversal, concurrent batch retry duplication, typed
value loss and schema enforcement bypass through direct writers.

Run both versions through both parser configurations and production Bolt/HTTP/
Query API modes, ordinary/impersonated sessions, memory/Badger/wrappers, fresh
sessions/reopen and failure injection. Assert errors, diagnostic structures,
columns/types/nulls, ordering, metadata, side effects and cancellation, not
only query admission. Current Enterprise oracle unavailable means blocked
acceptance, not substitute Community or the old TCK.

Measure named workloads before/after: core reads/writes, LET/FILTER/FOR,
NEXT aggregation, constrained paths, concurrent batches, typed property IO,
SEARCH and schema checks. Report ops/sec/p50/p95/p99, allocs/bytes, RSS and
recall. Investigate >5% performance or >10% memory regressions on unchanged
workloads; require >90% new-code coverage (core target 95%). Preserve the
existing allocation ratchet and benchmark baselines.

No full ISO GQL or Neo4j JVM/store/operator replication is claimed. Full
Cypher 25 language compatibility must not be claimed with missing stable
inventory entries or undisclosed retained extensions; publish scoped support
while work remains.
