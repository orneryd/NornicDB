# Changelog

All notable changes to NornicDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.4.0]

### Added

- Clear deleted entries out of the scanned ranges after a mass delete. Badger
  keeps a delete marker for every deleted node and relationship until a
  compaction it never runs on a small or idle database, and every scan steps
  over them: a 40,000-node property lookup went from 13 ms to 55 ms after
  140,000 nodes were deleted and stayed there. After 50,000 deletes, once
  deletes have stopped for 30 seconds, the engine has Badger compact them
  away (0.6 s; commits wait meanwhile), and the lookup takes 8 ms. Dropping a
  key prefix, which this and `DROP DATABASE` use, now makes commits wait
  instead of failing them with Badger's blocked-writes error (#911).
- Log classified Cypher syntax rejections at INFO with a bounded redacted
  statement shape, allowlisted statement class and stable grouping hash.
  JSON logs can be grouped into an optimization backlog without retaining
  rejected queries in memory.

### Changed

- Delete the legacy procedure dispatch switch entirely: every built-in
  procedure is now served by the registry, and unrecognized names are
  rejected at the converged router's terminal chokepoint. The five db.stats
  procedures are registered with their canonical Neo4j 5.26 signatures
  (required section argument, optional config map, canonical output columns)
  and dbms.clientConfig returns the canonical eight-column shape (#908).

- Retire the remaining sixty registry-owned text-dispatch branches: every
  procedure the built-in registry already owns (APOC algo/path/load/export,
  GDS, db.*, vector/fulltext indexes, RAG, txlog, temporal, nornicdb.*) is now
  served exclusively by the registry-first path. The legacy switch keeps only
  the twelve procedures the registry does not yet own (dbms.*, db.stats.*).
  The empty-registry fallback test pins ProcedureNotFound for all retired
  routes (#908).

- Retire the twenty APOC text-dispatch branches (algo, path, load/export,
  import) whose procedures are already owned by the built-in registry; the
  registry-first path now serves them exclusively, and the empty-registry
  fallback test pins ProcedureNotFound instead of legacy success (#908).

- Fold the last computed-row WHERE text splitter into the shared row predicate
  evaluator, so post-WITH WHERE positions evaluate with the same precedence,
  null and unrecognized-text semantics as every other WHERE owner (#908).

- Share one projected-read tail across the Async, WAL and composite engines: a
  projection-capable reader serves the read, any other engine gets a projected
  copy of its full read, and errors propagate verbatim (#521).

- Converge `apoc.neighbors.tohop` and `apoc.neighbors.byhop` on typed node
  arguments and one caller-context traversal. Preserve staged writes, support
  directed relationship alternatives and documented defaults, and match APOC
  empty-filter, hop-bucket and self-loop behavior. Remove their text fallback
  routes and obsolete argument parser. Standalone registered procedure calls
  now resolve typed parameters before formatting text for remaining adapters (#908).

- Make MATCH/procedure dispatch terminal through the shared pipeline and retire
  its private executor. Validate empty-input procedure metadata and arity,
  preserve per-row DBMS execution, and require WITH between updates and CALL
  before executing writes (#908).

- Use canonical typed invocation for procedure clauses instead of serializing
  row arguments or calling write handlers separately. Preserve node/relationship
  identity, nested values, parameters and terminal expression errors. Registered
  vector searches consume resolved inputs through shared search cores (#908).

- Route CALL-subquery compositions directly through the shared pipeline, with
  per-clause procedure/subquery classification and a shared UNION branch merger.
  Preserve correlated rows, typed distinct values, branch write statistics and
  terminal expression errors. Reject undeclared imports even on empty input and
  retire the private MATCH/CALL-subquery executor (#908).

- Make top-level UNWIND, WITH and FOREACH dispatch terminal in the shared
  pipeline. Retire their private entry owners and the duplicate UNWIND CALL
  transaction-batch runner; preserve the shared CALL operator's batch semantics.
  Selected UNWIND rewrite operators stay inside the caller's pipeline context
  and return execution errors instead of retrying. The pipeline now reports its
  own recorded expression failures, without relying on public Execute (#908).

- Retire test-only MERGE-chain projection wrappers and bespoke clause scanners.
  Move their assertions to the shared WITH planner and clause splitter, retaining
  exact node/relationship/scalar scopes, ON-action boundaries, quoted values and
  repeated WITH horizons. Require typed errors for empty projections, unknown
  variables and invalid arithmetic instead of permissive private behavior (#908).

- Route standalone and MERGE-first statements terminally through the shared
  clause pipeline. Retire private standalone, chain, multi-MERGE and segment
  executors. No-op MERGE matches no longer reindex nodes or queue embeddings;
  creation and SET mutations still notify. Preserve unbound relationship
  endpoint creation and idempotent replay in every direction, and reject
  malformed node patterns instead of clearing them into a fallback (#908).

- Route MATCH/MERGE compositions exclusively through the shared clause pipeline.
  Remove private compound MATCH/MERGE and MATCH/UNWIND/MERGE handlers and their
  orphan repeated-MATCH and window helpers. Preserve ON CREATE/ON MATCH actions,
  matched relationship bindings, row multiplicity, WITH windows and typed
  failures through public execution with exact persisted-effect checks (#908).

- Route MATCH/CREATE compositions exclusively through the shared clause pipeline
  and remove private MATCH/CREATE handlers and their orphan predicate helpers.
  Share inherited/explicit parameter preparation across public, internal and
  Fabric routes. Bound relationship predicates read the active transaction and
  propagate storage failures before further writes; WITH predicates distinguish
  relationship patterns from arithmetic. Preserve structured SET-label syntax
  errors and accept YIELD as an ANTLR symbolic name (#908, #894).

- Use shared lexical scanners across CALL, CREATE, schema and hint parsing.
  Route MATCH/CREATE/DELETE through real pipeline writes, preserve matched-row
  counts and write errors, and bound row-local WITH windows and MATCH seeds.
  Reuse immutable syntax and bounded CREATE bindings; avoid physical writes
  for cancelled unconstrained transient relationships while preserving logical
  effects, snapshot isolation, retained-edge order and recreated-ID visibility
  (#908). Historical hot-path throughput acceptance remains open.

- Route CREATE compositions and CREATE SET through the shared clause pipeline.
  Retire private multi-CREATE, CREATE/WITH/DELETE and CREATE SET dispatchers;
  preserve WITH filters, DELETE constraints, result aliases and typed errors.
  Plan adjacent CREATE clauses atomically, preserve async optimistic metadata,
  and evaluate flat CREATE/SET parameters without reconstructing query text (#908).

- Adapt eligible large Cartesian COUNT and integer SUM aggregation to workload
  size, expression complexity and available GOMAXPROCS. Dynamically claim chunks
  with worker-local accumulators and deterministic representative/group merging;
  preserve serial semantics for small, floating, DISTINCT and unsupported shapes.
  Reuse shared RETURN modifiers and avoid materializing every combination (#713).

- Borrow invocation-local native scopes for non-aggregate multi-MATCH RETURN and
  reuse cached projection admission/columns instead of allocating a scope per row.
  Shared consumers retain owned rows when ordering or wildcard projection needs
  them; preserve relationship values, typed errors and pagination (#713).

- Share MATCH/WITH/UNWIND projection, aggregation, ordering and pagination with
  the existing WITH/RETURN planners. Stream invocation-owned scopes instead of
  buffering expanded rows; preserve typed errors, scalar/list/null UNWIND,
  grouped SUM and borrowed-row ownership. Correct MATCH WHERE extraction and
  reuse folded keyword scanning without normalized statement copies. Avoid
  unused node contexts and aggregate ordering scopes; keep single string group
  keys separate from typed keys without regressing numeric grouping (#713, #728).

- Project multi-MATCH aggregates directly through shared RETURN, preserving
  exact integer SUM values above 2^53, floating SUM, grouped windows, and typed
  modifier errors. Remove redundant traversal-row conversion and lossy float
  coercion; retain integer SUM assertions alongside floating AVG controls (#713).

- Route cartesian aggregates through shared RETURN so ORDER BY, expression and
  parameter windows, LIMIT zero, and typed errors apply to aggregate results.
  Borrowed scopes preserve grouping and empty COUNT semantics while removing
  per-combination row maps. Add bitset aggregate-prefix admission and shared
  ASCII folding without allocating normalized copies of expressions (#713).

- Delegate non-aggregate cartesian RETURN, DISTINCT, ordering and expression
  pagination to the shared projector. Stream simple projections through an
  invocation-local borrowed scope; retain owned rows for window/order/wildcard
  modifiers. Preserve the supplied result buffer and stats, propagate typed
  projection/window failures, and avoid context copies for local values (#713).

- Reuse immutable OpenTelemetry span-start options without disabling tracing.
  Cache-disabled public Execute measures 35 -> 32 allocations/op and saves
  48 B/op; enabled span attributes, kinds, and concurrent parent contexts remain
  intact. Matched long-run latency is within noise (#754).

- Archive superseded versions only for other readers or retention. A write
  statement's own transaction counted as a snapshot reader, so every update
  and delete saved a copy of the old version that nobody could read. A
  transaction now stops counting once its conflicts are validated, and all
  archiving happens at commit. Updating and then deleting 40,000 nodes takes
  about 20% less time. While retention keeps history, an update archives the
  old version as an undo record (its metadata plus the properties that
  differ from the next version) instead of a complete copy; reads rebuild it
  from the next version. History for one-property updates takes about a
  quarter of the space (#911).

- Prepare shared cartesian WHERE membership indexes once per filter invocation
  instead of once per candidate combination, preserving parameter-list freshness
  between invocations and avoiding repeated content validation (#728).

- Route cartesian node-context WHERE predicates through shared typed admission
  instead of manual boolean negation and the private binding compiler. Preserve
  null truth and arithmetic failures under NOT, and admit ordinary `count`,
  `collect`, and `exists` property names as compiled scalar operands (#728).

- Read only the properties a statement uses when it scans a label. A `MATCH`
  on a label decoded every node in full, embeddings included, even when the
  rest of the statement read one or two properties. When every later clause
  only reads and the node is used only as `n.property`, the scan now reads
  just those; returning or passing on the whole node, `RETURN *`, a later
  pattern, any write or a temporal viewport keeps the full read. Grouped
  counts, sorts and filters over 40,000 nodes take 6–30% less time (#911).
  A keyword-named variable followed by a property access (`where.id`) is
  now seen as a reference by the statement checks.
- Reuse a node a transaction's label scan just read when the same statement
  writes it: `MATCH (n:L) SET …` and `DETACH DELETE` read every node again
  before writing it. Nodes with worker-sidecar embeddings, or read with decay
  filtering on, are still read again. A bulk `SET` and `DETACH DELETE` of
  40,000 nodes take about a fifth less time (#911).
- Check that a relationship's anonymous end node exists without reading it.
  `MATCH (p:Person)-[:KNOWS]->() RETURN p.id, count(*)` read every end node
  in full; storage now checks that its record key exists, or that it is
  staged, and inside an explicit transaction it uses the transaction's own
  writes and the node's version header at its snapshot. Decay filtering, a
  temporal viewport, labels or properties on the end node, or a path
  variable keep the read. That degree query over 40,000 nodes takes about a
  third less time, in and out of a transaction (#911).
- List relationships from their adjacency entries inside an explicit
  transaction too. The transaction reads each one's header from the entry when
  the relationship's version header in its snapshot is live and not newer
  than the transaction's read version, and its record otherwise; its own
  writes and deletes merge as before. The degree query over 40,000 nodes in
  a transaction takes about a quarter less time (#911).
- Store a copy of each relationship record's compact header (type, endpoints,
  timestamps, confidence, flags) in its adjacency entries, so a traversal over
  anonymous relationships (`MATCH (p)-[:KNOWS]->() …`) no longer reads every
  relationship record. Every write of the record rewrites the copy. Entries
  written by earlier versions keep working through the record; decay
  filtering keeps the full read (#911).

- Preserve tabs, newlines and carriage returns inside quoted WHERE literals and
  identifiers when normalizing clause whitespace. Binding admission shares the
  pipeline normalizer and the central quoted-text scanner (#728).

- Compile arithmetic and `size()` predicate operands through shared typed
  evaluation. Binding filters and complete WITH plans read native scopes without
  per-row parameter copies; membership indexes validate list contents once per
  invocation and preserve concurrent prepared views (#728).
- Compare seven fixed-count benchmark workloads under the Nornic parser in CI, gating
  allocation medians while reporting timing separately. Preserve raw samples
  and separate CPU profiles, resolve baseline commit hashes before checkout,
  and warm/stop the Badger cache-hit benchmark outside measurement. Include
  cache-disabled public Execute with an identical fixture on both revisions;
  exclude ANTLR from allocation checks, not parser correctness checks (#754).

- Seek a single-property uniqueness or node key constraint's own index for
  equality and IN predicates, as an index created with CREATE INDEX is used,
  inside explicit transactions too. A 17,000-node lookup inside a transaction
  drops from about 72 ms to under 0.4 ms (#875).

- Reuse immutable comparison/null evaluation handlers in compiled binding and
  row predicates, preserving unknown under negation. Skip clock scanning for
  complete cached plans and use allocation-free folded byte-prefix clock
  detection instead of per-call lowercasing (#728).

- Unify binding and WITH predicates through shared typed row evaluation, with
  invocation-local scratch and native relationship scopes. Reuse shared quote
  scanning and allocation-free operator bitsets/prefix gates; preserve null
  truth, typed errors, rollback and compiled filtering contracts (#728).

- Compare a label-less property match's string with the stored bytes during the
  node scan, skipping a non-matching node without decoding its properties, and
  reuse one property decoder per scan. A 20,000-node scan takes about half as
  long and allocates per match instead of per node (#857).

- Delegate post-SET UNWIND projection to shared UNWIND and RETURN execution,
  preserving mutation statistics, typed failures, grouping and windows (#713).
- Make projected-entity plan source links usable in the strict documentation
  build without changing the proposed design.

- Retire the traversal projection graph-evaluator fallback. Use the shared typed
  failure boundary for unresolved expressions, preserving null entity bindings
  and transactional rollback instead of projecting raw query text (#713).

- Compile CALL-tail columns from shared RETURN metadata and delegate RETURN
  splitting to the shared quote-aware comma scanner, preserving backtick names,
  escaped strings, DISTINCT columns and cached-plan immutability (#713).

- Use shared typed pagination and canonical projection columns in the simple
  MATCH/LIMIT fast path. Evaluate complete arithmetic/parameter limits without
  truncating tokens, preserving quoted aliases and streaming contracts (#713).

- Execute compiled batch WITH through the shared row projector, so all items
  read the incoming scope before aliases are published. Preserve shared scope
  pruning and propagate typed projection errors for transaction rollback (#713).

- Compile UNWIND-batch WITH assignments from shared projection metadata and
  canonical aliases. Reject aggregate, DISTINCT and modifier plans before batch
  writes so grouping and windows stay on the shared pipeline (#713).

- Compile count-only batch RETURN from shared projection metadata and canonical
  column names. Decline modifiers and unsupported projections before writes,
  retaining pagination and DISTINCT on the shared pipeline (#713).

- Make the MERGE-first annotation regression template valid Cypher by carrying
  node and row scope through WITH before its post-SET MATCH, retaining batch
  and indexed-lookup assertions under both parsers (#754).

- Compile relationship-batch RETURN fields and columns from the shared projection
  plan, preserving quoted, escaped and inferred names without admitting unsupported
  DISTINCT, aggregation or pagination into the compiled batch path (#713).

- Use Neo4j-valid quoted vector option keys in the async schema regression,
  keeping its complete admission and metadata checks valid in both parsers (#754).

- Plan async CREATE RETURN as one shared projection, preserving typed parameters,
  original column names, DISTINCT and pagination. Validate projection before
  publishing the batch so evaluation errors cannot leave created nodes (#713).

- Retire unused traversal aggregate parsing, finalization, SUM and deviation
  implementations. Preserve their regression assertions on the shared
  aggregate parser and collector, including typed localization identity (#713).

- Route plain multi-MATCH RETURN through shared projection planning, including
  parameterized/arithmetic pagination, DISTINCT, hidden sort keys and typed
  projection errors. Remove the superseded reducer and pagination tail (#713).

- Keep bound parameter-map values out of MATCH WHERE expression text. Preserve
  typed indexed lookups and full-row filtering for dynamic-key quantifiers,
  preventing source-body strings from being interpreted as arithmetic (#728).

- Share traversal aggregate RETURN planning and multi-MATCH aggregate bindings,
  preserving parameters, named paths and relationship lists. Use shared
  percentile collectors and retain exact integer SUM values above 2^53 (#713).

- Serialize compound schema admission, mutation, counters and transaction
  staging across executors sharing a schema manager. Concurrent ordinary,
  procedure-backed and constraint creators cannot publish conflicting names;
  guarded creation remains a no-op. Keep vector registration inside the
  successful mutation boundary and explicit commit actions deferred (#531).

- Share DDL index admission with all four procedure-backed vector/fulltext
  creators against their actual transaction schema view. Reject equivalent
  definitions and cross-kind/constraint name collisions before persistence
  or vector-space registration; retain legacy node-vector ProcedureCallFailed
  wrapping and native creator outputs (#531).

- Store multi-property node indexes as durable ordered RANGE definitions rather
  than colliding with an existing first-property index. Decode fulltext node
  label unions with the shared quote-aware target parser, preserving quoted
  literal pipes, and return the database-error class for missing index/constraint
  drops. Cover native profile/policy DDL, invalid options, zero phantom nodes,
  commit and rollback through public HTTP and Bolt routes in both parsers (#531).

- Reserve constraint names under the storage lock for LOOKUP index creation,
  with reference-compatible `ConstraintWithNameAlreadyExists` and guarded
  no-op behavior. Preserve LOOKUP-specific `IndexAlreadyExists` precedence
  and reject without persistence or inventory changes (#531).

- Admit expression type predicates in the strict ANTLR grammar, including
  `IS TYPED`, `IS NOT ::`, shorthand `::`, multiword types, nullable types,
  unions and nested list/array forms. Both parser modes share reference-value
  and invalid-type regressions (#838).

- Reserve shared index/constraint names before schema mutation, including native
  constraints without backing indexes. Report constraint-owned index conflicts
  with constraint schema classes. An index-side `IF NOT EXISTS` is a no-op;
  a constraint reusing an index name still fails, including guarded creation
  (#531).

- Reject unguarded duplicate ordinary/range, vector, fulltext, TEXT and POINT
  index DDL with schema diagnostic classes for equivalent definitions or
  conflicting names. Honor
  `IF NOT EXISTS` without changing stored definitions or registering another
  vector space; compare entity scope, kind, targets and ordered properties (#531).

- Converge decay bundle CREATE/ALTER option decoding: canonical case-insensitive
  keys, typed numeric zero/one, boolean forms, scope updates, and atomic rejection
  of wrong-typed values. Share validated immutable decay option application with
  the storage lifecycle (#531).

- Route compatibility vector/fulltext schema procedures through the same
  isolated transaction mutation contract as DDL. Keep vector runtime changes
  commit-local, preserve relationship fulltext scope, and expose staged native
  definitions through knowledge-policy info/profile/policy procedures. Legacy
  vector creation retains the reference ProcedureCallFailed invocation class
  when called after a data write (#530, #531).

- Stage ordinary index and constraint DDL in explicit transactions. Commit
  publishes successful definitions and backfills, rollback discards them, and
  mixed schema/data writes fail with ForbiddenDueToTransactionType. Preserve
  existing index caches and reject stale schema snapshots after concurrent
  same-database data writes (#531).

- Compile default knowledge-policy bootstrap declarations with canonical APPLY,
  ON ACCESS, WHEN APPLY PROFILE, and prefix Kalman syntax, retaining their actual
  access mutations and promotion clauses instead of silently ignoring them (#531).

- Require explicit native promotion-policy targets and reject targetless FOR
  clauses. Classify native profile/policy CREATE declarations as DDL, preserving
  undirected relationship targets. Stage native definitions in the database
  transaction, publish only persisted snapshots after commit, and discard them
  on rollback; schema-only autocommit no longer looks like an empty transaction
  (#531).

- Reject unconsumed text after native decay/promotion option and binding blocks,
  using a shared end-of-statement check that preserves one optional semicolon
  terminator for valid profile and policy statements (#531).

- Share field-specific CREATE/ALTER promotion-profile option decoding, including
  case-insensitive keys and scope support. Preserve numeric 0/1 values in numeric
  fields and reject wrongly typed updates without changing stored profiles (#531).

- Reject malformed promotion-policy clauses, unconsumed definition suffixes,
  unknown options, and non-boolean enabled values before schema mutation.
  Apply terminal ENABLE/DISABLE flags after validating SET OPTIONS. Route native
  knowledge-policy DDL consistently with either parser backend (#531).

- Replace placeholder `db.stats` rows with bounded, shared query collection,
  real invocation summaries, live graph/token/meta retrieval, canonical
  section/configuration contracts, and uncached lifecycle results (#530).

- Preserve label and relationship-type allocation order in CALL token listings,
  including LIMIT and delete/recreate behavior, with namespace-isolated persisted
  positions. Correct token procedure system flags and rich metadata while
  preserving localized descriptions (#530).

- Correct await-index defaults, ping and database/component metadata, and the
  cache-clearing procedure's `value` result column. Retain native database count
  extensions and truthful product/edition identity (#530).

- Accept empty transaction metadata maps and return void results from
  `tx.setMetaData`; support standalone autocommit calls through an implicit
  transaction and declare canonical MAP/DBMS metadata (#530).

- Return void from await/resample index procedures and validate named indexes
  with the shared IndexNotFound status; correct resample READ/system metadata
  and preserve parameterized index-name support (#530).

- Return void from vector-property setters and declare shared entity/key/vector
  argument metadata, preserving outer RETURN rows and native string-ID inputs
  without claiming a backend-specific storage representation (#530).

- Correct vector node-index creation to return void, consume evaluated arguments,
  and declare SCHEMA/deprecation metadata. Supply rich vector query metadata and
  the analyzer `stopwords` column, retaining native scoring, plugin inventory,
  and documented extension fields (#530).

- Return typed nonpersisted schema-visualization entities with label metadata,
  virtual relationship endpoints, and marginal label combinations. Include
  standalone indexes and native constraint statements without fabricated store
  IDs or duplicate owned backing indexes (#530).

- Supply dbms.listConfig filtering and eight shared result fields from the
  database-scoped settings resolver, preserving secret redaction and native
  inventory while replacing invented transport state with unknown values (#530).

- Populate dbms.listConnections from immutable live Bolt snapshots, including
  server addresses, user agents, acceptance timestamps, user visibility, and
  disconnect cleanup; share the instance inventory with HTTP queries (#530).

- Persist native database/server UUIDs and expose them through SHOW DATABASES,
  Bolt, and HTTP. Replace fixed db.info/dbms.info names and creation dates with
  actual selected/system database metadata; preserve legacy timestamps and
  read-only startup behavior (#530).

- Render SHOW PROCEDURES argument/return descriptions, optional defaults, and
  status flags from canonical procedure metadata. Match pinned Neo4j fulltext
  node/relationship query signatures, field metadata, and system flags, and
  SHOW FUNCTIONS argument descriptions for both range overloads (#530).

- Consolidate all 22 open Dependabot updates into one dependency refresh:

| PR | Dependency | Previous | Updated |
| --- | --- | --- | --- |
| #787 | lucide-react | 1.37.0 | 1.48.0 |
| #788 | github.com/googleapis/gax-go/v2 | 2.23.0 | 2.26.2 |
| #789 | docker/setup-buildx-action | 4.3.0 | 4.4.1 |
| #790 | react-router-dom | 7.18.3 | 7.18.4 |
| #791 | github.com/prometheus/client_model | 0.6.2 | 0.6.3 |
| #792 | github.com/neo4j/neo4j-go-driver/v5 | 5.28.4 | 5.28.5 |
| #793 | autoprefixer | 10.5.4 | 10.6.1 |
| #794 | docker/build-push-action | 7.3.0 | 7.4.0 |
| #795 | github.com/hybridgroup/yzma | 1.27.0 | 1.28.0 |
| #796 | github.com/hashicorp/go-kms-wrapping/wrappers/azurekeyvault/v2 | 2.0.15 | 2.0.16 |
| #797 | @vitejs/plugin-react | 6.0.4 | 6.1.1 |
| #798 | github.com/ebitengine/purego | 0.10.2 | 0.11.1 |
| #799 | docker/setup-qemu-action | 4.2.0 | 4.4.0 |
| #800 | go.opentelemetry.io/otel/exporters/prometheus | 0.66.0 | 0.68.0 |
| #801 | react-dom / @types/react-dom | 19.2.8 / 19.2.5 | 19.3.0 / 19.3.0 |
| #802 | github.com/hashicorp/go-kms-wrapping/v2 | 2.0.24 | 2.0.26 |
| #803 | github.com/vektah/gqlparser/v2 | 2.5.36 | 2.5.58 |
| #804 | postcss | 8.5.26 | 8.5.28 |
| #805 | golang.org/x/net | 0.58.0 | 0.59.0 |
| #806 | react / @types/react | 19.2.8 / 19.2.18 | 19.3.0 / 19.3.0 |
| #807 | baseline-browser-mapping | 2.11.20 | 2.11.26 |
| #808 | three | 0.185.1 | 0.186.1 |

  Include the corresponding golang.org/x/{crypto,sync,sys,text}, Google IAM
  and genproto companion updates. Regenerate Go/npm lockfiles together and
  retain exact SHA pins for the Docker actions.

- Read only the properties a statement uses when it scans a label. A `MATCH`
  on a label decoded every node in full, embeddings included, even when the
  rest of the statement read one or two properties. When every later clause
  only reads and the node is used only as `n.property`, the scan now reads
  just those; returning or passing on the whole node, `RETURN *`, a later
  pattern, any write or a temporal viewport keeps the full read. Grouped
  counts, sorts and filters over 40,000 nodes take 6–30% less time (#911).
  A keyword-named variable followed by a property access (`where.id`) is
  now seen as a reference by the statement checks.

- Keep a node's full-text document when it is re-indexed with the same
  searchable text, for example when only its embedding changes: the text is
  no longer removed, analyzed twice and added back. Re-indexing such a node
  takes about a sixth of the time. The default searchable text lists a node's
  other properties in key order, so the same node always yields the same text
  (#911).

### Fixed

- Compare explicit HTTP differential failures after commit, where Neo4j can
  defer connected-node DELETE validation. Roll back open reference transactions
  before failed assertions can leave locks behind and cause later reset timeouts.

- Version the label-index ready marker for the v3 index scheme. Upgraded
  stores with the legacy `{1}` marker rerun the existing label-index backfill
  instead of treating missing current-scheme entries as ready. Empty stores
  synchronously mark the current scheme without launching a backfill.

- Record the committed version, not a transaction's uncommitted state, as
  history when a transaction updates a node and then deletes it. With history
  retention on, reading that version returned values that were never
  committed (#911).

- Give every explicit HTTP transaction its own ID. IDs were the current time
  in nanoseconds, so two BEGINs in the same clock tick (seen on Windows) got
  the same commit URL; one client's statements and commit reached the other's
  transaction, and the other got `TransactionNotFound` or `no active
  transaction`. IDs now count up from the server's start time. Erasure
  requests and edge provenance records, which also used the clock as their
  ID, get random IDs (#915).

- Complete strict ANTLR admission for DISALLOWED policy constraints, constraint
  blocks/inventory, label expressions, relationship quantifiers, escaped
  backticks, bare index properties and supported database/user administration.
  Preserve IS NULL/type precedence and reproducibly vet-clean parser generation.
  Correct invalid phase-boundary fixtures without dropping state assertions;
  preserve existing correlated RETURN rather than appending a second one. Isolate
  concurrent tracing assertions by owned trace IDs while requiring every
  parent/operator span (#713, #728, #754).

- Compare an integer and a float by their exact values everywhere (WHERE,
  RETURN, ORDER BY, CASE, with or without an index), as Neo4j compares
  stored values: `9007199254740993 > 9007199254740992.0`. The most negative
  integer divided by -1 wraps to itself instead of failing, and negating it
  is Neo4j's long overflow instead of null. `size()` of a stored number,
  boolean, temporal value or point is Neo4j's TypeError naming the value
  (`got: Long(1)`); a statically typed argument keeps the SyntaxError (#893).

- Accept a WHERE inside a node or relationship pattern, as Neo4j 5 does:
  `(a:Q WHERE a.id > 2)`, `-[r:R WHERE r.w > 1]->`. It filters as if it were
  in the clause's WHERE; on a quantified relationship (`-[r WHERE …]->{1,2}`)
  it applies to each relationship. It is Neo4j's SyntaxError on a `*`
  variable-length relationship and in CREATE or MERGE. `WHERE (true)` is a
  parenthesised literal, not a node pattern (#878).

- Read a `*` or `..` inside a backticked relationship type or a quoted
  property value as part of it, not as a variable length (#879).

- Match a quantified relationship (`-[:R]->{1,3}`, `-->+`, `<-[r]-*`) as the
  variable-length relationship with those bounds instead of a single
  relationship; a type expression on it applies to each relationship. A
  quantifier on a variable-length relationship, in CREATE or MERGE, or in a
  pattern predicate or comprehension is a SyntaxError, as in Neo4j (#864).

- Read `WITH *, items` and `RETURN *, items` as Neo4j does instead of failing
  with "could not evaluate expression: *": the * stands for every variable in
  scope, in name order, except one an item redefines, followed by the items.
  `RETURN DISTINCT *` is accepted, `WITH DISTINCT *` keeps one row per
  distinct set of variables instead of every row, and a `RETURN *` without
  rows lists the variables in scope. The error for `RETURN *` with no
  variables in scope is localized (#883).

- Give a list predicate's condition the path variables in a traversal's
  WHERE: `all(i IN range(0, size(r)-1) WHERE r[i].w < 3)` and
  `nodes(p)[i]` were null there, so every row was dropped. A variable-length
  relationship variable is its list of relationships wherever a path context
  is turned into row values (#882).

- Run a variable-length OPTIONAL MATCH through the pipeline: the single-hop
  OPTIONAL MATCH handler bound the relationship variable to one relationship,
  so `size(r)` failed with a type mismatch.

- Accept a list index whose type is one of several, such as `r[i + 1]`;
  Neo4j checks it when it runs.

- Accept `where`, `optional`, `union`, `call`, `as` and `distinct` as
  variable names, as Neo4j does: in node patterns, WITH, UNWIND and RETURN
  aliases, ORDER BY, WHERE and expressions. A keyword-named variable can be
  a projection's first or last item before a clause (`WITH where WHERE …`,
  `WITH x, optional MATCH …`, `RETURN by ORDER BY by`), and `distinct` is a
  variable wherever Neo4j reads it as one (`RETURN distinct ORDER BY
  distinct`, `RETURN distinct.id`) and the keyword wherever the rest can be
  an expression (`RETURN DISTINCT skip - 1`, `count(distinct)` has no
  argument). One DISTINCT rule now serves every projection and aggregate
  (#894).

- Re-embed a node whose content changes while the embed worker is embedding
  it. The worker's writeback now lands only while the node still has the
  properties and labels it embedded; before, it stored the old content's
  embedding as the new content's (after a Cypher SET, or an update keeping
  the node's update time) or dropped the node from the pending queue so it
  was never embedded again. Embeddings written before this fix can be wrong
  for nodes edited while they were being embedded, and those nodes can't be
  told apart afterwards: re-embed the database to be sure (#889).

- Run `shortestPath` / `allShortestPaths` MATCH clauses as a pipeline step:
  endpoint patterns and WHERE select the pairs, LIMIT, ORDER BY, aggregation
  and WITH apply to the path rows, and path predicates find the shortest path
  that satisfies them. Common start/end nodes, minimum lengths above 1,
  relationship properties and unbound endpoints in expressions raise Neo4j's
  errors (#863).

- Read `b:Label` in the WHERE of a MATCH that joins a bound variable as a
  label test instead of text (#876).

- Keep an index and a uniqueness constraint on the same label and property
  exclusive, as Neo4j does: creating either over the other fails with Neo4j's
  code and message, and DROP INDEX can't drop the index a constraint owns
  (#884).

- Evaluate the documented decay functions `decayScore()`, `decay()` and
  `policy()` in statements instead of rejecting them as unknown functions, and
  score every decay call of a statement at the statement's time (#871, #866).

- Read one statement instant for every temporal clock call in WHERE, as in
  RETURN: `WHERE datetime() = datetime()` keeps every row (#872).

- Count only the requested relationship type in the asynchronous engine's
  endpoint-label counts (`EdgeCountByStartLabel`, `EdgeCountByEndLabel`).
  Pending relationships of every type were added to them before a flush, and a
  pending change of another type's relationship cancelled the requested type's
  count (#868).

- Label and relationship-type names are case-sensitive, as in Neo4j:
  `:Person`, `:person` and `:PERSON` are three labels (#862). Storage
  lower-cased them in the label index, the relationship-type index, the
  relationship-between set and heads, the label and type counts and the
  temporal index, so counts added every spelling (`count(:Person)` was 2 with
  one `:Person` and one `:person` node) and `:R` and `:r` between the same
  nodes shared one lookup entry. Reads, transaction overlays and the
  asynchronous engine compare names exactly, Cypher's relationship-type and
  label filters (APOC path algorithms, FastRP, count fast paths, index
  hints) do too, and unnamed constraints and indexes on labels or properties
  that differ only in case get distinct generated names. The V2-to-V3
  upgrade, gated behind `--upgrade-storage`, rebuilds the label,
  relationship-type and relationship-between indexes from the stored nodes
  and relationships, moves deindex catalogs and tombstones to the new keys,
  and has the counts and temporal index rebuilt at startup. Original Badger
  relationship prefixes are preserved; policy state uses the existing
  AccessMetaStore metadata namespace rather than additional key families.

- Label expressions (`n:A|B`, `n:A&B`, `n:!A`, `n:%`, groups, and GQL's
  `n IS A`) in MATCH patterns, relationship patterns (`[r:!R]`, `[:R&S]`),
  WHERE and RETURN, pattern predicates, pattern comprehensions and
  EXISTS / COUNT / COLLECT bodies return Neo4j's rows; they matched nothing
  before. A statement's label expressions are rewritten once, before it is
  routed, into the label forms and WHERE predicates every route evaluates.
  CREATE and MERGE accept `&` and `IS`, and the forms Neo4j rejects (colons
  mixed with expression symbols, `R|:S` with a variable, `[:R:S]`, type
  expressions on variable-length relationships, `n IS NOT A`, other label
  expressions in CREATE and MERGE) are SyntaxErrors with Neo4j's messages
  (#860).

- Stop reallocating the search result cache on every write. Each created,
  updated or deleted node invalidated it by allocating a fresh map sized for
  the whole cache; it now returns at once when the cache is empty and clears
  in place otherwise. A 100,000-node CREATE is about 20% faster, a bulk SET
  or DETACH DELETE about 14% (#849).

- Seek the label indexes for an equality combined with a disjunction of labels
  on a node pattern without labels, `MATCH (n) WHERE n.id = $id AND (n:A OR
  n:B)` (also `n:A|B` and `IN` lists), when every label in the disjunction has
  an index on the property, as Neo4j does with one index seek per label. It
  scanned every node per row: graphify's incremental-sync stale-node delete
  took 0.2 s per id on 40,000 nodes (#858).

- Stream a label-less property match inside a transaction, which includes every
  write statement, from the transaction's snapshot with only the matched
  properties decoded, instead of decoding every node in full. Writes such as
  graphify's `MATCH (a {id: $src}), (b {id: $tgt}) MERGE …` and reads in
  explicit transactions now cost what an auto-commit read does (#824).
- Make a label-less property scan cheaper per node and use it for more
  statements. The scan decodes a node's projected properties through one
  decoder per scan: key tokens are resolved once per database and a rejected
  node allocates no map, reader or ID (6 → 2 allocations per scanned node).
  `MATCH (a) WHERE a.id = $id` and relationship patterns that start from a
  label-less node with properties, such as graphify's stale-edge delete
  `MATCH (a {id: row.src})-[r]->(b {id: row.tgt})`, now take the projected
  scan instead of decoding every node (#857).

- Route a top-level UNION before the auto-commit async CREATE fast paths: on a
  server, a UNION whose first branch is a CREATE ran that branch for the whole
  statement. Mismatched branch columns wrote the node and returned the rest of
  the text as a column name, and `CREATE … FINISH UNION …` wrote an extra
  unlabeled node. Both now behave as in Neo4j on every route (#781).

- Storage range scans read only their own keys. Reverse scans (Badger ignores
  the prefix bound in reverse) and forward scans without a prefix bound read
  past their range over deleted keys; after DROP DATABASE, every relationship
  read of a node in another database walked the dropped database's deleted
  adjacency keys, and DETACH DELETE of 100,000 nodes went from 6 s to 36
  minutes. Every range scan is now forward and bounded. Lookups that want the
  greatest keys first (an MVCC version at or before a read version, temporal
  history as of a time) find the range's two lowest keys forward and seek in
  reverse only above them, so Badger's read-ahead stays in the range (#850).

- Read a label's nodes in one database only. The label index is shared by
  every database, and a label scan loaded and decoded the label's nodes in all
  of them before the database filter dropped the others: a 1,000-node scan
  took 190 times longer with 100,000 nodes of the label in another database.
  Label reads now skip other databases' index entries before reading their
  nodes, on every engine layer and inside transactions (#851).

- Commit statements of any size atomically. A statement whose writes exceed
  one Badger batch (about 15% of the memtable) is written as several hidden
  batches under one reserved run of commit timestamps and becomes visible all
  at once; readers never wait, conflicts are checked across the whole
  transaction, a failed or crash-interrupted large commit is rolled back (on
  the next open, for a crash), and stores written by earlier releases open
  unchanged. A statement may also introduce any number of new property
  names: their records are written in as many batches as they need, and
  staging them is no longer quadratic (#703).
- Commit the engine's bulk node and relationship creates and deletes (used by
  recovery, the admin importer, replication, the async engine's flush and
  the Qdrant API) as one
  transaction: they succeed at any size or write nothing, instead of failing
  with "Txn is too big" or, for embedding chunks, writing them in separate
  commits. They share the transaction's checks and bookkeeping, so bulk-created
  nodes are queued for embedding like created ones, and recovery no longer
  splits restore batches. A transaction now rejects a relationship ID that is
  already committed instead of overwriting it, and reports a missing endpoint
  as not found. Loading a database's version state on a closed engine returns
  "storage closed" instead of crashing (#703).
- Complete `DROP DATABASE` for databases of any size: the namespace drop no
  longer fails with "This transaction has been discarded" after flushing a
  full write batch, which left the database listed and partly deleted (#819).
- Use a property index for counting and range lookups:
  `MATCH (p:Person {id: $id}) RETURN count(p)`, `WHERE p.id IN $ids RETURN
  count(p)` and `WHERE p.id < 200` read the index instead of scanning the
  label. Row reads and counts share one index seed selection (#820).
- Read stored values in place during storage scans instead of starting a
  goroutine per item to prefetch them: snapshot reads of earlier node
  versions, snapshot adjacency and label scans at a version are 4-40x
  faster, and
  short lookups such as relationship `MERGE` no longer wake a thread per
  item (#703).
- Restore write-path performance: convert query parameters into row values
  once per query instead of once per expression evaluation (bare `UNWIND
  $rows CREATE` and relationship `MERGE` batches were quadratic), validate
  streamed aggregation rows once with one cached projection parse, check a
  `CREATE CONSTRAINT ... IF NOT EXISTS` before snapshotting or persisting the
  schema, let a pipeline `MERGE` that scanned the label and found no node
  create without scanning it again, and skip the text checks a repeated
  statement already passed (#823).
- Store spatial points as properties: `point(…)` returns a POINT value
  (cartesian, cartesian-3d, wgs-84, wgs-84-3d) instead of a map that storage
  rejected with "unsupported type Map". Points (and lists of points of one
  CRS) are stored and read back, sent over Bolt as Point2D / Point3D and over
  HTTP in Neo4j's form, and support Neo4j's construction rules and errors,
  fields, `point.distance` (on Neo4j's Earth radius), `point.withinBBox`,
  equality, DISTINCT and ORDER BY; they can be indexed with a POINT index
  (#817).
- Order values of different types as Neo4j does: points and temporal values
  sort after lists and before strings, booleans and numbers (#837).
- Seed `MATCH (n:Label {prop: $value}) OPTIONAL MATCH ...` from the property
  index instead of streaming the whole label, and stop collecting that seed
  when no fast path needs it. An indexed string, boolean or number that no
  node holds now answers from the index in MATCH, OPTIONAL MATCH and MERGE
  instead of falling back to a label or full node scan (#821).
- Return the same rows from an equality on an indexed property as without the
  index: `WHERE p.name = toUpper($name)`, `WHERE p.d = date(...)` and other
  computed values are evaluated before the index lookup instead of being
  looked up as their own text, and list values are filed in property indexes.
  A uniqueness constraint now rejects a duplicate list, as in Neo4j (#844).
- Compare node-pattern properties with Cypher equality in `MATCH`, `OPTIONAL
  MATCH` and `MERGE`: `(n {n: 1.0})` matches a stored `1`, `(n {tags: [1, 2]})`
  a stored integer list and `(n {d: date(...)})` a stored date, in auto-commit
  and explicit transactions. `MERGE` with such values matched nothing and
  created a duplicate node (#846).
- Scan a property match without a label (`MATCH (n {id: $id})`) at a fraction
  of the per-node cost: only the pattern's properties are decoded, a node that
  fails them is skipped before the rest of it is decoded, and the whole node is
  read only for a match. Inside a transaction the node scan this uses listed no
  nodes at all; it now reads the transaction's view of every node (#824).
- Collect query statistics from database start, as Neo4j 5.26 does:
  `db.stats.status()` reports `collecting` until `db.stats.stop('QUERIES')`,
  and `db.stats.clear('QUERIES')` answers `false`, "Collected data cannot be
  cleared while collecting." while collection runs. Recorded invocations no
  longer allocate a map per query; collection adds no measurable time per
  statement (#530).
- Fix `go generate ./pkg/localization`: the procedure-metadata generator no
  longer requires every procedure to be registered by a literal
  `registerBuiltInProcedure` call; a test checks that every metadata entry is
  registered with its localized description in the live registry (#530).
- Delete an entity once when a transaction deletes it more than once: a
  repeated delete of a node or relationship, or of a relationship its node
  already took, changes nothing, so statement statistics and stored counts
  match Neo4j (`count(r)` returned -2 after deleting two relationships
  twice) (#827).
- Accept SKIP and LIMIT expressions that read no row variable: properties of
  a map parameter (`SKIP $p.n`, `LIMIT size($p.vals)`), map literals and
  variables the expression binds itself (list comprehensions, `reduce`), as
  Neo4j does (#829).
- Write zero seconds in `toString()` of times, local times, date-times and
  local date-times (`12:34:00`, not `12:34`), as Neo4j does. The value's own
  text, used in HTTP results, still leaves zero seconds out, as in Neo4j (#818).
- Evaluate type predicate expressions, as Neo4j does: `x IS :: TYPE`,
  `x IS NOT :: TYPE`, `x :: TYPE` and `x IS [NOT] TYPED TYPE`, with every
  type and synonym, `NOT NULL`, `LIST<T>`, unions, `ANY`, `NOTHING`, `NULL`
  and `PROPERTY VALUE`. Previously every form failed with "could not evaluate
  expression" (#838).
- Remove documents from the full-text (BM25) index in time linear in their
  number: a removal counts its postings dead instead of copying each shared
  term's posting list, and a list is compacted once half of it is dead.
  Updating or deleting N nodes that share a property value no longer takes
  time quadratic in N (#826).
- Seed `MATCH (n:Label {prop: $value}) OPTIONAL MATCH ...` from the property
  index instead of streaming the whole label, and stop collecting that seed
  when no fast path needs it. An indexed string, boolean or number that no
  node holds now answers from the index in MATCH, OPTIONAL MATCH and MERGE
  instead of falling back to a label or full node scan (#821).
- Answer label counts through the async write buffer from the stored per-label
  counters plus the unflushed writes instead of reading every node of the label.
  `MATCH (n:Label) RETURN count(n)` and the coverage check of label-less
  property lookups no longer grow with the label (#843).
- Preserve locally bound iterators in nested list predicates, including
  same-kind `all`, `any`, `none`, and `single` calls (#774, #775).
- Route `CREATE TEXT INDEX` and `CREATE POINT INDEX` through schema execution
  instead of async node batch creation. Preserve typed index metadata in Bolt
  and HTTP auto-commit and explicit transactions (#775).
- Reject statically invalid LIST/MAP/scalar subscripts with compile-time
  SyntaxError, including known parameter types, while retaining runtime errors
  for dynamically typed FLOAT indexes (#698).
- Validate FINISH/UNION columns before writes, preserve unit-CALL graph effects,
  and enforce explicit CALL import scopes, including empty imports
  (#743, #781, #782, #783).
- Validate CYPHER versions/options and repeated execution-mode prefixes before
  execution. Preserve EXPLAIN/PROFILE plans through composite dispatch and
  return HTTP PROFILE plan envelopes (#744, #784).
- Match repeated composite-constraint diagnostics and compile-time SHOW
  predicate type validation (#785, #786).
- Complete label-less indexed property reads across other labels/unlabeled
  nodes, accept parameters named `as`, and bind comma-MATCH path/shortestPath
  variables correctly (#814, #815, #816).
- Expand the forced ANTLR grammar for multipart and expression subqueries,
  scoped CALL/UNION, CALL pagination, map projections, dynamic labels,
  schema/administration commands, keyword identifiers, signed numeric literals,
  multiline strings, and post-index property access. Preserve syntax rejection
  and odd/even logical negation (#739).
- Require `NORNICDB_RUN_PERFORMANCE_TESTS=1` for Bolt throughput and Cypher
  timing/profile workloads, and always disable them under race/short runs.
  Keep COUNT correctness assertions unconditional (#715).
- Resolve the 13 remaining reported diagnostic mismatches in #657: validate
  graph-property subscripts, arithmetic function argument types, function
  arity, undefined function-expression variables, scalar quantifier inputs,
  and expression-subquery outer-name shadowing before execution. Reject
  impossible property access before missing-parameter errors and return
  `ProcedureCallFailed` for missing vector indexes without managed embeddings.
  Preserve the managed-embedding fallback, valid subquery identity imports,
  optional function arguments, and legal map/list access. Add pinned Neo4j
  5.26.30 differential regressions for Bolt and HTTP transaction modes.
- Preserve null-valued node pattern predicates instead of dropping them.
  Null property-map MATCH reads and mutations match nothing; null MERGE
  properties are rejected before writes. Keep CREATE null-property omission
  separate from matching semantics (#810).
- Reject malformed and oversized transaction HTTP bodies; abort explicit
  sessions on invalid execute/commit bodies. Match Neo4j's required statement
  list, optional empty-body and first-document framing rules: ignore trailing
  documents/garbage without executing them, but drain the size-limited reader
  before writes so oversized suffixes remain rejected (#776, #812).
- Preserve indexed explicit-transaction read-your-writes for equality, IN,
  pattern-property and ordered reads, including mixed committed/pending rows
  and MATCH mutations. Property-index lookups made inside a transaction merge
  the transaction's own node writes, so they stay index lookups: a read by an
  indexed property costs the same in a transaction as in auto-commit, and
  MERGE on an indexed property finds a node the transaction created. Ordered
  and not-null index scans read the index while the transaction has written
  no nodes, and the shared transactional scan once it has (#809).
- `UNWIND … MATCH (n:Label) WHERE <predicate>` on a property without an index
  tests the predicate per candidate node without building a row for it, and a
  comparison with a string literal is parsed once per predicate text rather
  than once per node (an unindexed read of 2,000 ids against 2,000 nodes:
  11.0 s to 3.6 s in auto-commit, 7.9 s to 0.8 s in an explicit transaction).
- `MATCH (n:Label) WHERE n.k IN $list` no longer tests every candidate node
  against the text of the whole list. On an indexed property the list's values
  are looked up and the nodes returned as they are; without an index the list
  is parsed once into its values (5,000 ids against 5,000 nodes: 30 s to 0.2 s
  with an index, 0.6 s without).
- `MATCH p = (n:Label)`, a named path of one node, returned no rows (and
  `MATCH p = (n:Label {k: v}) RETURN n.k` failed to evaluate): the path
  assignment was parsed as part of the node pattern.
- Preserve each outer row exactly once after successful unit CALL subqueries,
  even when inner MATCH or WITH filters remove all rows. Keep returning CALL
  joins and transactional batches on the shared pipeline (#771).
- Extend shared node-product streaming across successive MATCH and row-local
  WITH clauses (#728, #777). Evaluate computed properties through the shared row
  evaluator and reject incomplete arithmetic before writes (#514, #778). Preserve
  Unicode lowercase expansion (#698), recursively serialize entity temporal
  properties over HTTP (#668, #779), and reject system graph reads consistently
  (#738). Apply outer composite projections through the main Cypher pipeline
  without leaking inner columns; encode constituent-aware Bolt entity IDs
  recursively (#745, #648, #780).
- Replace mutation RETURN/WITH projectors and transactional CALL text batching
  with shared typed pipeline operators. Preserve empty CALL schemas, counters,
  UNION exports, embedding options, and partial runtime-error rows while rolling
  failed writes back. Keep quoted Fabric continuations on the canonical pipeline.
  Preserve nonfinite typed write values, Neo4j math diagnostics and rounding,
  MERGE conflict retry safety, and large-integer indexed ordering.
- Preserve outer rows after scoped transactional CALL unit subqueries and
  admit explicit transactions when MATCH or UNWIND supplies zero inputs (#648).
  Accept unbound bare endpoints in whole-pattern relationship MERGE (#640).
  Feed unfiltered comma-separated node products into the shared incremental
  WITH/RETURN aggregate collector rather than retaining every binding (#728).
- Stream direct UNWIND range inputs through shared row-local WITH projections
  and predicates into shared incremental WITH/RETURN aggregation (#772),
  avoiding eager range validation and both filtered and unfiltered aggregate
  OOMs. Keep scalar aggregate state per group instead of retaining input rows;
  collect, DISTINCT, and percentiles retain their necessary values.
  Remove legacy UNWIND replay, aggregation,
  and collect evaluators. Preserve typed mutation/procedure arguments, ordered
  MERGE/SET writes, and row-producing Fabric prefixes in the shared pipeline.
- Fix server concurrency races (#770): atomically publish executor loggers and
  startup timestamps, keep UI base paths local to each router, and drain admitted
  Badger helper transactions through their commit tails before releasing engine
  state during Close. Add fail-before concurrency and shutdown regressions.
- Preserve local composite transaction commit/rollback after a Bolt statement's
  context ends (#683). Reject graph writes targeting `system` with SemanticError
  before cross-database transaction admission; return HTTP 200 for missing USE
  targets without changing missing endpoint database errors (#738). Serialize
  transaction HTTP temporal values as ISO text, including nested values (#668).
- Share scalar string and conversion function contracts across row and generic
  evaluation, including null propagation, Unicode uppercase expansion, FLOAT
  string formatting, and temporal text (#698). Reject malformed expressions and
  undefined variables in function-result property access (#514, #657). Exclude
  nested property functions from pattern-variable scope discovery.
- Reject semicolon-chained statements and mixed RETURN/FINISH UNION branches
  before graph writes (#743, #744). Conflicting EXPLAIN/PROFILE modes report
  compile-time ArgumentError, including through HTTP transactions; Bolt PROFILE
  summaries publish only the profile key. Regenerate the shared ANTLR grammar
  to accept scoped CALL after UNWIND (#739).

- Admit composite node uniqueness constraints with atomic validation and
  namespace-aware enforcement for direct and explicit-transaction writes.
  Reject malformed composite references and obsolete ON/ASSERT constraint
  syntax; preserve typed duplicate-schema and constraint-creation errors (#531).
- Admit TEXT and POINT index DDL through the schema route and persist their
  typed definitions for SHOW and drop/recreate operations. Query execution
  retains its existing scan fallback (#531, #530).
- Accept the legacy variable-length MVCC version-key layout
  (`[prefix][string ID][0x00][version]`) alongside the fixed-width layout
  during the V2→V3 edge-adjacency migration and in the runtime version
  scans. Stores that predate the fixed-width key rewrite previously failed
  startup with "migration v2→v3 failed: repair archived edge adjacency:
  invalid mvcc edge version key: len=61".

- Preserve evaluated map-literal order in transaction HTTP rows and metadata,
  including aliases and nested collections; snapshot ordering before caching.
  Restore remote HTTP node/relationship reads through standard row+graph results
  and one metadata-aware decoder, including identities, labels and endpoints.
- Match computed arithmetic filters combined with known boolean AND/OR operands;
  reject numeric-only WHERE arithmetic statically and non-boolean comprehension
  predicates at runtime. Remove generic comprehension's default-true fallback.
- Validate FOREACH mutation bodies before row evaluation through the shared
  parser and recursive mutation validation, rejecting trailing garbage and empty
  assignments even when MATCH produces no rows.
- Align transaction HTTP entity rows with Neo4j: return properties in rows and
  identities in metadata, including nested collections and paths. Ordinary maps
  are no longer inferred to be entities from their field names; the previous
  entity-envelope format is removed.
- Match standalone computed arithmetic WHERE filtering on the pinned Neo4j
  default lookup-index schema while retaining TypeError for non-boolean logical
  operands. Restore default lookup indexes in the differential reset harness;
  compare labels as sets only where their order is explicitly unspecified.
- Preserve compound FOREACH updates, MERGE action bindings, and outer CALL
  variables through shared clause execution (#640, #648). CALL projections
  now reuse pipeline RETURN, including wildcard columns and integer sums.
- Validate undefined MERGE property variables and empty WITH projections;
  evaluate CREATE/MERGE property list comprehensions (#514).
- Distinguish statically invalid WHERE predicates (SyntaxError) from dynamic
  non-boolean values (TypeError), using procedure output types for YIELD;
  reject filtering importing-WITH clauses and transactional CALL inside an
  explicit transaction with Neo4j's error code (#728, #648).
- Correct IVF/PQ score reconstruction to add the raw centroid used during
  residual training, preventing centroid-norm bias before exact rescoring
  (#446). Controlled service recall improves; the issue's external dataset
  recall remains unverified.
- Deliver EXPLAIN/PROFILE query plans to clients (#744): Bolt PULL SUCCESS
  metadata now carries `plan` (EXPLAIN) and `profile` with runtime counters
  (PROFILE), and HTTP transaction results carry `plan`/`profile` in the
  Neo4j JSON shape (`operatorType`, `identifiers`, `args` with
  `EstimatedRows`, `children`; profile adds `rows`/`dbHits`). The Go driver
  reads both via `summary.Plan()` and `summary.Profile()`.
- Stop PROFILE's inner execution from serving and populating the result
  cache: a cached result object was mutated with plan metadata and leaked it
  into ordinary cached reads of the same query.
- Fix `OPTIONAL MATCH p = shortestPath(...)` returning a fabricated
  `{result: null}` row with the wrong columns instead of projecting the
  path. Clause-only and anchored forms (`MATCH (a) OPTIONAL MATCH
  p = shortestPath((a)-...->(c))`) now run the shared shortestPath BFS with
  left-outer-join semantics: one row per seed, real values when a path
  exists, nulls when none, and errors propagate instead of being swallowed.
- Allow `shortestPath(...)` / `allShortestPaths(...)` in value position
  (`RETURN length(shortestPath((a)-[:R*]->(b)))`) instead of rejecting the
  statement as an illegal projected pattern expression. Value forms reuse
  the same BFS machinery as clause forms and project per row.
- Return null (not a fabricated `0`) for `length(null)` / `length()` of an
  unbound path in a non-matching OPTIONAL MATCH.
- Isolate one-statement transaction scripts (`BEGIN … COMMIT/ROLLBACK`) on a
  private executor instead of the shared per-database executor. A client
  statement could previously open the script's transaction on the executor
  other clients' auto-commit statements run on, letting a low-privilege
  caller crash the process mid-run and cause unrelated clients' acknowledged
  writes to fail or be silently lost.
- Reject bare `BEGIN`/`COMMIT`/`ROLLBACK` statements on cached per-database
  executors (the embedded `DB.Cypher`/`DB.ExecuteCypher` base executor and
  the HTTP and Bolt autocommit executor caches) with the same
  `Neo.ClientError.Statement.SyntaxError` a client statement gets. A bare
  `BEGIN` on an unmarked context previously opened a transaction on the
  shared executor, after which every concurrent auto-commit statement and
  one-statement script ran inside that caller's transaction. Embedded
  callers that want explicit transactions create their own session executor
  (`cypher.NewStorageExecutor(db.GetStorage())`); HTTP/Bolt protocol
  transaction owners keep their per-session executors.
- Treat pattern comprehensions (`[(n)-->(m) | …]`) and `COUNT { }`/`EXISTS { }`
  pattern subqueries as graph access for the composite-root guard. They
  previously slipped past the per-database authorization and read, counted,
  and existence-tested data in composite constituents the caller was
  explicitly denied.
- Scope `:param`/`:params` shell parameters per authenticated caller instead
  of storing them on the shared per-database executor, so one client's
  parameter values can no longer be read, cleared, or overwritten by another
  client of the same database.
- Classify and route `CREATE OR REPLACE DATABASE` as an admin command: it
  previously skipped the admin check and silently did nothing; it now
  requires admin permission and creates or replaces the database.
- Evaluate every arithmetic operator in CREATE/MERGE property maps and list
  items. `CREATE (n:T {a: 2 * 3})` previously stored the expression's own text
  `'2 * 3'` as the property value (silent wrong data) because only `+` and `/`
  were routed through the evaluator; `*`, `-`, `^`, `%` and unary minus now
  evaluate to their values, null operands omit the property, and a runtime
  failure (e.g. `1 / 0`) fails the statement.
- Decode a quoted property value only when it is exactly one quoted literal:
  `{s: 'a' + 'b'}` previously decoded to `"a' + 'b"` instead of evaluating
  the concatenation.
- Reject non-boolean, non-null WHERE results with Neo4j's `TypeError` across
  row, binding, path, multi-node, and WITH predicate routes; null remains
  unknown and does not retain a row.
- Preserve CALL-subquery result columns through `RETURN *` and supported
  trailing `WITH`, `UNWIND`, and `MATCH` clauses, including empty results and
  write counters.
- Accept `SHOW ... YIELD ... WHERE ... RETURN ...` in ANTLR parser mode.
- Avoid Cartesian expansion for supported comma-MATCH property equality
  joins, while leaving computed predicates on the general evaluation path.
- Reject forms Neo4j rejects that the validator previously accepted: the
  `NOT IN` operator (write `NOT x IN [list]`), trailing or leading commas in
  list literals (`[1, 2,]`, `[, 1]`), adjacent string literals (`'a''b'` —
  Cypher's string escape is a backslash, not a doubled quote), and a
  statement whose last clause is `UNWIND` with nothing after it.
- Accept Neo4j 5 statement preambles (`CYPHER [version] [option=value …]`
  groups, any number, on every route) and run the statement they precede.
- Support the `FINISH` clause terminator (Neo4j 5.19+): a statement ending in
  `FINISH` runs and returns no rows, including on each UNION branch and in
  `CALL { }` bodies; `FINISH` after RETURN/WITH/YIELD stays a SyntaxError.
- Reject `EXPLAIN PROFILE` / `PROFILE EXPLAIN` as a SyntaxError, as Neo4j
  does.
- Enforce Neo4j's array property rule on CREATE/MERGE/SET writes: a list
  property whose elements don't share one primitive or temporal kind, or
  that contains null, now fails with a TypeError and nothing is stored; an
  int/float mix is stored as floats (`[1, 2.5]` stores `[1.0, 2.5]`).
- Truncate logged query shapes at a rune boundary so redaction/log seams can
  never emit invalid UTF-8.
- Unify scheduled and explicit MVCC pruning on per-key transactions, preserving
  active snapshots and avoiding conflicts with normal writes. Scheduled pruning
  now keeps exactly `MaxVersionsPerKey` closed versions; its next cycle may
  remove one extra version retained by the previous lifecycle planner.
- Skip prune-floor lookups for MVCC nodes and relationships with no persisted
  floor, restoring point-read and label-scan allocations without changing
  visibility of pruned history after restart or backup restore.
- Fully remove namespace-owned indexes, dictionaries, adjacency, MVCC history,
  heads, and prune floors on database drop; count every Badger key family in
  storage byte metrics.
- Remove Cypher comments before redacting rejection and slow-query log
  records, preventing comment text from exposing secrets in query logs.
- Gate the V2-to-V3 storage upgrade behind `--upgrade-storage` and restore
  missing versioned adjacency for edges written by older bulk-create paths.
  Current relationship traversal and retained pre-update/pre-delete snapshots
  are repaired on upgrade; pruned history cannot be reconstructed. Edge body
  encoding remains V2.
- Make `BadgerEngine.Close` wait for in-flight durable writes before releasing
  engine state. An explicit transaction whose Badger commit had already
  returned could have its post-commit tail (label counts, MVCC sequence, ID
  counters, caches, callbacks) torn by a concurrent `Close`, panicking on the
  released `db` handle and reporting a durable commit to the Bolt client as
  `transaction commit panicked: ... nil pointer dereference`; a non-transactional
  `CreateNode` in the same window panicked on a released cache map. Writes now
  hold a read barrier that `Close` takes for write, and a commit that arrives
  after `Close` has finished fails with `ErrStorageClosed` without touching
  Badger. Fixes #499.
- Floor the numeric ID dictionary counters at the highest numID the durable
  forward maps hold when the engine opens. The counter high-water mark is
  persisted in its own transaction after the user transaction commits, so a
  crash, kill, or engine Close racing that commit tail left committed nodes and
  edges above the persisted counter; the next allocation then reissued a live
  numID and two entities shared one compact key in every numID-keyed index
  (adjacency, label, edge-between, MVCC heads). Observed as `MATCH (a)-[r]->(b)`
  returning each relationship a second time under an unrelated start node after
  a restart mid-commit.
- Seed node MATCH candidates from a property index when the WHERE clause is a
  conjunction containing an equality on an indexed property (e.g.
  `WHERE n.repo_id = $r AND n.evidence_source = 'x' AND n.generation_id <> $g`),
  on both the single-clause and WITH dispatch paths. Previously every such query
  hydrated the whole label even with a usable index present; the residual
  predicates still filter in the executor, so results are unchanged. A conjunctive
  existence probe over a 50k-node label drops from ~27.7ms/op to ~8.7us/op
  (~3177x), ~2001x fewer bytes and ~1109x fewer allocs per op. Fixes #490.
- Track embedding claims as in-flight work across provider calls, batch
  bisection, retry backoff, and persistence. Embed stats now keep `running`
  true, expose `in_flight`, and include claims in `pending_nodes`, preventing
  completion polling from returning before vectors are searchable.
- Buffer large MessagePack search-index snapshots and atomically replace files
  only after a successful encode and fsync. The durable storage clean-shutdown
  marker is now recorded before potentially slow search persistence, so an
  orchestrator timeout cannot force an unrelated storage-index rebuild.
- Reuse persisted BM25 indexes across stemmer plugin binary rebuilds when the
  tokenizer, stemmer algorithm, plugin API, source version, indexed properties,
  schema, and index format are unchanged.
- Bound decoded MVCC node bodies by retained bytes, keep separately stored
  embeddings out of the cache, and rehydrate them only for full-node reads.
  Search responses now use an O(1) LRU with a retained-byte budget as well as
  the existing entry limit, preventing document-sized results from pinning
  gigabytes of heap.
- Treat the IVF/PQ exact-rescore setting as a candidate floor independent of
  the requested result limit, and raise its default to 2,000 candidates. This
  preserves high recall before final truncation while still allowing deeper
  caller-requested searches.
- Skip periodic WAL snapshots when no mutation has arrived since the previous
  compaction, and stream snapshot nodes without loading separately stored
  embeddings. The snapshot check interval is now configurable through
  `NORNICDB_WAL_SNAPSHOT_INTERVAL` or `database.wal_snapshot_interval`.
- Resolve search type/label candidate filters from compact in-memory metadata,
  including the legacy string `type` property, before reading any document
  properties. Type-only filtering no longer decodes candidate nodes, and
  combined filters only read properties for candidates that pass the type.
- Use the transaction's pinned label and relationship-type indexes for
  snapshot reads, preserving read-your-writes and Neo4j snapshot semantics
  while avoiding database-wide node/edge decoding in explicit transactions
  and writing statements.
- Record a one-use clean-shutdown boundary after flushing acknowledged writes,
  allowing clean restarts to skip temporal-index and MVCC-head reconstruction.
  Multi-database byte accounting is now initialized only when a byte limit or
  explicit size query needs it, eliminating its unconditional startup scan and
  avoiding a deferred scan on ordinary writes.
- Feed the already-computed BM25 result prefix into HNSW as multiple layer-zero
  entry points, and preserve compact high-IDF rank/topic signatures during graph
  construction for deterministic diversity tie-breaking. The integration is
  always active when lexical data exists and performs no per-node BM25 queries.
- Build HNSW graphs with the diversity heuristic used by the reference
  algorithm and reserve `2*M` links on layer zero. Persisted graphs use a new
  topology version so indexes built with the recall-losing layout are rebuilt.
- Create the configured file-backed vector store during the first live vector
  write, including migration of any vectors already accepted in memory, so an
  initially empty database does not retain its entire bulk load in RAM.
- Apply HNSW additions, updates, and removals synchronously at every index size,
  removing the arbitrary live-update cutoff and deferred-rebuild thresholds.
  Mutations concurrent with a rebuild are replayed before its atomic swap so
  the replacement graph cannot drop newly indexed vectors.
- Remove the arbitrary global MessagePack decode ceiling from database-owned
  storage and search snapshots. Validate BM25 reloads after memory-bounded
  vector warmup so a failed load cannot be reported as success or overwrite a
  valid large snapshot with an empty index.
- Bind managed search continuation streams to their canonical database even
  when Bolt/Cypher omits `USE` and HTTP omits `database`, allowing signed qids
  to move between protocol adapters for the same authenticated or anonymous
  principal.
- Widen HNSW traversal independently of returned candidate depth, select the
  best matching chunk per owning node from that beam, and continue adaptive
  expansion until the approximate index is genuinely exhausted. This restores
  recall and requested result counts on heavily chunked corpora without
  rebuilding persisted graphs.
- Remove shared per-label count keys from explicit transactions' optimistic
  conflict sets. Transactions now accumulate label deltas locally and publish
  derived counts in commit order, so independent same-label creates and label
  changes commit concurrently without false `Transaction.Outdated` failures.
- Pack contextualized document batches into multiple Voyage requests when the
  worker batch exceeds the provider byte or input-count budget, preserving
  document order instead of retrying one locally rejected batch forever.
  Unclassified batch-local errors are now isolated while explicitly transient
  provider outages remain single requests, and batch failures are logged.
- Search a wider ANN candidate pool before collapsing chunk vectors to nodes,
  and stop storing chunk 0 twice under both the node ID and a chunk suffix.
  HNSW and compressed IVF/PQ now preserve substantially more of the exact
  node-ranking candidate set on heavily chunked corpora.
- Keep compressed IVF/PQ indexes current between rebuilds with an exact live
  mutation overlay and removal tombstones, persist that overlay atomically with
  the compressed bundle, reject it when the vector-store generation differs,
  probe an adaptive fraction of coarse lists by default, and search a bounded
  exact overflow set for vectors poorly represented by trained centroids.
- Preserve labeled-count fast paths inside explicit transactions, falling back
  to transaction-visible counting only after a node mutation is staged.
- Preserve structured multimodal batching through cache and tracing wrappers,
  preventing one provider request per image in production wrapper stacks.
- Keep transient embedding failures pending across retry exhaustion and
  restarts, apply provider-wide exponential cooldowns (including Voyage
  `Retry-After`), and avoid recursively bisecting provider-wide outages.
  Terminal failures remain parked and can be listed or explicitly requeued.
- Preserve batched embedding-free node reads through Namespaced, Async, and WAL
  storage wrappers so search filters do not decode or copy stored vectors.
- Open explicit transactions no longer retain the async flush lock. Transaction
  admission now flushes acknowledged writes and opens the MVCC snapshot under
  one short boundary, preventing concurrent `BEGIN` and count-query stalls.
- Reserve pending embedding nodes in memory across provider calls so concurrent
  workers cannot submit the same structured document twice. Batch multimodal
  documents for providers that support structured batch requests.
- Add per-search `include_properties` and `exclude_properties` response
  projections, including durable continuation pages and cache isolation;
  exclusions take precedence. Keep lexical/rerank selection independently
  controlled by `NORNICDB_SEARCH_BM25_PROPERTIES`.
- Bound Stage-2 rerank content per candidate, prefer the winning managed
  embedding passage, and use a query-centered window for lexical-only matches.
  Ranked continuations now reuse prior rerank scores instead of resubmitting
  the growing candidate prefix on every depth expansion.
- Batch pending documents across embedding-provider requests, pace requests
  instead of individual nodes, isolate rejected inputs, and park permanent
  failures without blocking the queue. Voyage contextualized
  embeddings now split oversized documents into bounded, boundary-aligned
  segments that retain provider auto-chunking; default contextualized chunks
  to 512 tokens while preserving explicit sizes; and distinguish omitted
  overlap from explicit zero.
- Match Neo4j Unicode code-point semantics for string length, slicing, and
  indexing; order computed RETURN expressions before pagination; and preserve
  bindings through `UNWIND … CREATE … SET … WITH … MATCH … CREATE` pipelines.
- Compute BM25 IDF lazily from current corpus statistics instead of refreshing
  the entire term dictionary on every node mutation, and rebuild the sorted
  prefix lexicon lazily instead of shifting it for every new term.
- Share compound top-level `UNWIND` routing between autocommit and explicit
  Bolt transactions so bindings survive multi-clause mutation pipelines.
- Reduce default in-memory vector retention by normalizing unit embeddings in
  one private copy and making HNSW reuse that immutable storage. Preserve sparse
  raw copies only where non-unit dot/euclidean semantics require them, and
  compact redundant raw data while loading legacy snapshots.
- Isolate packaged Snowball runtimes so multiple language plugins can coexist,
  quarantine unrelated broken plugins, and document Snowball v3.1.1 with the
  current `-P` compiler syntax.
- Restore Neo4j-compatible row expression and compound-clause semantics for
  chained `WITH`, `UNWIND`, `EXISTS`, `CREATE`, `MERGE`, and `SET` queries;
  missing properties now return `null`, and acknowledged async writes are
  visible when a subsequent explicit transaction begins.
- Avoid decoding chunk embeddings while applying search candidate filters and
  use a read-only mapped vector lease during file-backed HNSW construction,
  eliminating per-neighbor reads and allocations without changing the public
  owning-vector accessor.
- Log query-embedding fallback at warning level and expose a stable,
  sanitized `fallback_reason` through HTTP, native gRPC, Cypher/Bolt, MCP,
  Heimdall, and durable continuation responses.

### Added

- Restore managed Voyage multimodal embeddings for mixed text and URL/base64
  image documents and text queries. Keep Voyage schemas and limits isolated in
  the provider package, persist model-space identity, and prevent per-database
  search indexes from mixing incompatible managed spaces with equal dimensions.
- Expose llama.cpp lazy tensor loading for local embedding, reranking, and
  Heimdall models through domain-specific `*_LAZY_MODE` settings.

### Changed

- Upgrade the embedded llama.cpp library and Docker library images from the
  `b10411` nightly to the latest stable release, `v0.4.1`; align the Windows
  yzma bindings with its v0.4.1 ABI; and update local GGUF examples to the
  current non-deprecated C API.
- Add `orca` as an optional OpenAI-compatible managed-embedding and Heimdall
  provider, using the existing generic API URL, API key, model, and dimension
  settings.

## [v1.3.3] - 9/15/2026

### Added

- Publish a design RFC for pluggable Snowball stemming in BM25 indexes.
- Add signed, authorization-bound search continuation across HTTP, native gRPC,
  Cypher, and Bolt metadata, including progressive ranked retrieval, complete
  ID populations, deterministic grouped passages, bounded per-owner registry
  admission, and page-only hydration.
- Add continuation lifecycle metrics, bounded cursor admission controls, and
  protocol-specific status mappings so operators can distinguish disabled,
  saturated, expired, invalid, and wrong-scope continuation cursors.

### Fixed

- Preserve process-wide cursor lifecycle metrics and gauges when additional
  database search services attach to the shared continuation registry.
- Prevent stateful `db.retrieve` continuation START/PULL/DISCARD calls from
  using the ordinary Cypher result cache, so cursor ownership, discard,
  expiry, and invalidation checks are always enforced.
- Align no-auth anonymous HTTP and Bolt principals so a durable continuation
  cursor started through one protocol can be resumed through the other when
  authentication is disabled.
- Preserve continuation result display metadata, including canonical type,
  title, description, content preview, grouped gRPC child evidence, and
  explicit `max_results` completion labels.
- Prevent ranked continuation from treating short approximate, filtered, or
  fused batches as exhaustion. Propagate retrieval exhaustion evidence, deepen
  chunk candidates beyond one-shot limits, remove the arbitrary
  5,000-candidate engine ceiling, and report caller-selected budget limits
  explicitly instead of silently ending the stream.
- Stop ranked and `ranked_then_id` continuation when an ANN or hybrid producer
  reaches a stable bounded candidate pool, avoiding repeated equivalent
  retrieval work, integer-overflow depth expansion, and TTL-delayed failures.
- Preserve Stage-2 rerank tails and candidate-budget boundaries so reranker
  top-k limits do not silently drop otherwise eligible search results or mark
  non-exhausted collections as fully exhausted.
- Use property indexes for scalar expression-valued lookup keys, including
  forms such as `MATCH (n:Doc {id: $i + 1})`, `WHERE n.id = row.offset + 1`,
  and UNWIND-driven relationship creation that binds both endpoints by
  expression. These shapes now avoid full label scans when the expression
  evaluates to an indexable scalar.
- Bound database-manager startup memory by scanning only leaked system-record ID prefixes and streaming node/edge size reconciliation. In a cold 2,000-node persistent-store benchmark, cleanup fell from about 3.23 ms and 7.81 MB per operation to 25 us and 3.5 KB; reconciliation allocations fell about 5% without changing serialized-size accounting.
- Preserve exact cosine-vector fast-path semantics for inline node properties, filtered top-k queries, exact LIMIT results, and WITH projections ordered before RETURN.
- Skip empty k-means clusters during vector routing and load legacy vector files without query metadata when storage is empty.
- Invalidate local and Fabric query-cache entries after direct, asynchronous, replicated, edge, and bulk-prefix graph mutations while keeping Badger label counts coherent.
- Deduplicate concurrent property-free relationship MERGE operations by deriving a shared deterministic relationship identity across managed transactions.
- Execute CREATE clauses that follow one or more MERGE clauses, including multiple and comma-separated CREATE patterns, without misclassifying ON CREATE SET.
- Apply every sort key before indexed pagination, including complete primary-key ties and filtered candidates beyond the initial index window.
- Restore row bindings across mixed relationship/node MATCH products, chained and multi-hop OPTIONAL MATCH, MATCH-UNWIND-MATCH-MERGE pipelines, and null property projections.
- Expand incoming bound-end patterns in their declared direction across OPTIONAL MATCH horizons, pattern comprehensions, and COUNT subqueries while preserving node, relationship, and scalar bindings.
- Correct DISTINCT aggregation for nodes, relationships, and scalars across chained MATCH clauses, including nested expressions such as `size(collect(DISTINCT ...))`.
- Apply WHERE after an aggregating WITH in chained MATCH pipelines, preserve subsequent WITH expressions and RETURN ordering, and accelerate bound-start expansion from about 80.7 ms to 0.97 ms per benchmark operation.
- Bind every intermediate node in bound-anchor multi-hop MATCH patterns in either traversal direction, reject conflicting prior bindings, and parse each chained relationship pattern once per clause; benchmark median latency fell about 19%, bytes about 24%, and allocations about 29%.
- Honor explicitly supplied embedding CLI flags in the loaded configuration; omitted flags preserve environment/YAML values.
- Pass embedding GPU-layer choices through local model initialization and distinguish CPU-only `0` from automatic `-1` when reusing embedders.
- Preserve async node update classification across flush cleanup, retain pending-create counts after failed first writes, and report persistence-lookup failures.
- Release Badger-backed in-memory engine resources and background server graphs
  on close/stop paths used by production and tests, reducing retained memory
  from completed database/server lifecycles.
- Restore persisted vector-store reloads on Windows while preserving committed-tail recovery and append semantics.
- Repair persisted HNSW warmup, localized model-path, and vector-store error fixtures for Windows and Linux.
- Restore documentation-site builds after dependency and configuration changes.
- Complete the CLI localization catalogs for the Voyage embedding mode flag and
  its provider/API key help text.
- Match Neo4j Bolt result streaming for explicit transactions with zero-based `qid` values, independently resumable concurrent statements, latest-statement fallback, bounded `DISCARD`, and invalid-stream failure/reset behavior. Focused Apple M3 Max benchmarks reduced streaming-option parsing from about 125 ns, 339 B, and 3 allocations per operation to 13 ns with zero allocations; autocommit stream-state handling fell from about 19 ns, 48 B, and 1 allocation to 7 ns with zero allocations.
- Keep Bolt connections open after delivered retryable commit conflicts, mapping
  Badger write conflicts to the existing transient transaction status without
  forcing the client driver to reconnect before retrying.
- Unify textual search across HTTP, native gRPC, Cypher retrieval, MCP discover, and Heimdall discovery. Long queries now embed and search each chunk independently, rank the combined candidates with one deterministic outer RRF implementation, and never average chunk embeddings; explicit caller-provided vectors remain single-vector searches. On Apple M3 Max, the focused 8-chunk/800-candidate fusion benchmark improved from about 55.6 us, 79.7 KB, and 294 allocations per operation to 24.4 us, 43.0 KB, and 8 allocations, increasing throughput from about 18.0k to 41.0k operations per second; the single-chunk path remains allocation-free at about 6 ns.

## [v1.3.2] - 9/11/2026

### Security

- **Upgraded gRPC-Go to `v1.83.2`.** This includes the upstream HTTP/2
  transport fix that rejects requests missing both `:authority` and `Host`,
  preventing the crafted-request denial of service affecting xDS-enabled
  gRPC-Go servers.

- **Hardened authenticated reverse-proxy deployments.** Forwarded scheme,
  host, prefix, and client-address metadata is now accepted only from explicit
  `NORNICDB_HTTP_TRUSTED_PROXIES` IP/CIDR entries. Authenticated public HTTP can
  run behind a trusted TLS terminator, native HTTP TLS settings now reach the
  listener, and documented YAML HTTPS settings are decoded correctly.

- **Bolt disablement now removes the listener.**
  `NORNICDB_BOLT_ENABLED=false` no longer suppresses only the public-listener
  security check while still starting Bolt. Disabled Bolt is also omitted from
  discovery and startup endpoint output. Authenticated public Bolt listeners
  must enable and require native TLS with a certificate and key.

- **Added native TLS and mTLS for the shared Qdrant/Nornic gRPC listener.**
  Authenticated public gRPC endpoints now accept direct TLS with rotating
  server certificates and optional verified client certificates. Startup
  rejects incomplete TLS material, invalid client-auth modes, and public
  authenticated plaintext listeners.

- **Fixed OAuth callback identity and credential exposure.** OAuth callback
  JWTs now retain the internal user subject required by authenticated profile
  and API requests, including repeat logins for existing users. Public user
  responses no longer expose upstream OAuth access or refresh tokens stored in
  account metadata.

### Changed

- **Selective exact cosine queries now stream projected candidates.** Badger
  and namespaced storage support a projected label stream that preserves
  knowledge-policy decay visibility. Filtered cosine queries apply eligible
  pre-score predicates before scoring and retain only the exact top-k
  candidates, reducing the local 2,008-candidate regression fixture from about
  1.78 ms/op to 1.27 ms/op and from 28.2k to 24.3k allocations/op.

### Fixed

- **Container ingress settings now reach NornicDB unchanged.** Shipped Docker
  Compose variants forward auth, CORS, trusted HTTP proxies, native HTTPS, Bolt
  TLS/mTLS and WebSocket controls, Qdrant gRPC listener and request limits, and
  GraphQL tracing. Runtime images no longer shadow `NORNICDB_*` settings with
  generated CLI flags, configured listener ports are published consistently,
  and health checks follow the effective address, base path, HTTP/HTTPS mode,
  and port. GraphQL continues to share the hardened HTTP ingress rather than
  exposing a separate listener.

- **Persistent vector indexes retain query metadata after restart.** File-backed
  vector sidecars now persist node labels and named, property, and chunk vector
  associations. Restarted services can resolve property-vector queries without
  rebuilding from storage; legacy sidecars without this metadata rebuild once.

- **Filtered cosine query forms now preserve exact Cypher semantics.** Selective
  pre-`WITH` predicates and inline property patterns evaluate the complete
  candidate population when a bounded vector shortlist cannot prove exactness,
  then apply score ordering and `LIMIT`. Direct `RETURN` cosine queries now
  also honor `ORDER BY` on the computed score alias. Unfiltered, complete-small,
  and pre-warmup live-ingestion paths retain indexed execution.

## [v1.3.1] - 9/8/2026

### Security

- **Fixed a GraphQL arbitrary-Cypher authorization bypass.** `Query.cypher`
  previously allowed authenticated read-only users to execute data-changing
  statements because write checks depended on the GraphQL operation type.
  Top-level and nested Cypher execution now enforce canonical read, write,
  schema, and admin requirements against the caller's effective access for the
  selected database. Database aliases and `USE`/Fabric targets are resolved
  against the request's database scope and authorized before storage routing.
  Reported by Sevban Dönmez (`jankesec`).

### Added

- **Editable knowledge-policy control plane.** Decay bindings and promotion
  policies now support atomic `ALTER ... FOR ... APPLY { ... }` replacement.
  Knowledge-policy catalog procedures expose canonical `Apply` bodies for
  lossless editing. The admin UI now provides inline ui-grid text and numeric
  editors, constrained selects, and enablement toggles for policy targets,
  apply directives, and profile settings, with visual validation before
  submission. Saves are serialized, accessible popup notifications report the
  result, and authoritative policy state is reloaded after both successful and
  failed updates.

### Changed

- **Performance documentation now uses current, workload-scoped evidence.**
  Cross-system claims reference the reproducible Northwind comparison and BEIR
  SciFact retrieval evaluation, report correctness alongside latency,
  throughput, resource use, and retrieval quality, and distinguish measured
  configurations from general product claims.

### Fixed

- **Relationship updates cannot overwrite peer writes published after validation.**
  Native conflict detection remains enabled in high-performance mode, and
  committed edge targets now join the native transaction's conflict-read set.
  A late peer update or deletion causes the stale writer to return the existing
  transient conflict error instead of losing newer properties or resurrecting
  a deleted relationship.

- **Transaction snapshot reads no longer admit unpublished MVCC reservations.**
  A peer could reserve a sequence, let a reader enumerate an existing edge,
  then publish its deletion at that same sequence. Snapshot lookups now share
  one pinned read-only Badger transaction, and write admission compares physical
  publication revisions as well as logical versions. The conflicting write
  keeps the existing transient error contract without serializing transactions.

## [v1.3.0] - 9/5/2026

### Security

- **Fixed an authorization bypass in the Neo4j-compatible HTTP transaction
  API.** An authenticated user with read-only database access could previously
  execute data-changing compound Cypher statements because HTTP authorization
  classified statements by their leading keyword. Autocommit and explicit
  transaction open, execute, and commit paths now use the canonical Cypher
  permission analysis used by Bolt. Registered procedure modes and nested
  statements also inherit the caller's read, write, schema, and admin
  permissions. Operators should upgrade to `v1.3.0`. Reported by Sevban Dönmez
  (`jankesec`).
- **Updated Browserslist to `4.28.8`.** The UI dependency is pinned to the
  patched release rather than accepting an older transitive resolution.

### Added

- **Added opt-in `failClosed` for `db.retrieve`.** `failClosed: true`
  (alias `fail_closed`) requires a usable numeric query embedding and
  disables strategy fallback, including BM25-only search, without changing
  ranking defaults. Callers that need deterministic RRF or candidate depth
  still pass those fields explicitly. In fail-closed mode, non-finite,
  out-of-range, or non-integral count policy values error (including
  `rerankTopK` / `rerankMinScore`), supplied embedding elements must be
  numeric types, and embedder failures wrap
  the underlying cause so operators can distinguish a disabled embedder,
  timeout, empty output, or a string passed as `embedding`. Without the
  flag, empty embeddings still fall back to BM25. `fallbackEnabled: false`
  also no longer silently switches empty-embedding requests to BM25-only
  search.

- **Added production localization across user-facing commands, protocols,
  errors, and runtime logs.** Immutable embedded `en-US`, `es-ES`, and `en-XA`
  pseudo-locale catalogs support BCP 47 language negotiation from CLI options,
  `NORNICDB_LANGUAGE`, YAML, operating-system preferences, request context,
  HTTP `Accept-Language`, gRPC metadata, and Bolt session metadata. Localized
  boundaries now cover the NornicDB and admin CLIs, HTTP and GraphQL services,
  MCP JSON-RPC, Bolt, Nornic and Qdrant gRPC, Heimdall, authentication, Cypher,
  storage, search, replication, multi-database operations, retention, and core
  database errors. Typed descriptors preserve wrapped causes, sentinels,
  protocol status codes, exit codes, and machine-readable configuration keys.
  Stable structured event IDs keep logs queryable independently of rendered
  prose, while missing packs or entries fall back deterministically to English
  with bounded warnings. Catalog validation checks canonical language tags,
  duplicate and unknown IDs, plural forms, and template placeholders in CI
  without requiring localization inventory CSV files at runtime.

### Changed

- **Made database-scoped settings explicit, canonical, and operationally
  verifiable without changing unrestricted defaults.** One typed registry now
  drives validation, resolution, admin metadata, and bounded `SHOW SETTING[S]`
  output. Settings that overlap Neo4j use the matching Neo4j namespace;
  NornicDB extensions use `db.nornic.*`. Canonical dotted keys and their
  `NORNICDB_*` alternatives are both accepted in YAML and API input, normalized
  before persistence, and resolved in built-in, global YAML/environment,
  explicit process/CLI, then per-database override order. Every dynamic setting
  names a concrete applicator: embedding, search-index, and reranker changes
  rebuild that database's search service, while search-result cache capacity
  and TTL mutate the live cache in place. Other settings persist immediately,
  retain the current effective value, and report `pendingRestart` until the next
  process start; no unsupported database-only restart level is advertised.
  Restart persistence is covered across an actual Badger close/reopen, and the
  registry rejects settings that claim hot reload without an implementation.
  Badger low-memory mode and existing durability controls now reach their
  storage constructors.
- **Automatic WAL compaction now writes atomic, checksummed streaming
  snapshots.** Recovery detects and incrementally reads the framed format while
  retaining compatibility with existing JSON snapshots.
- **Hardened multi-database isolation and deployment defaults.** Manager-owned
  storage/query limits cover protocol and background inference paths, MCP and
  Bolt enforce database scope/admission, and startup rejects wildcard CORS,
  public plaintext listeners, and empty credentials when authentication is
  enabled, regardless of environment. The documented initial `admin` / `password`
  credentials remain supported for local bootstrap and emit a warning until
  changed. Explicit no-auth startup remains supported in every environment.
  Container images and maintained Compose examples retain their
  authentication-disabled compatibility default, and the entrypoint emits a
  high-severity structured event whenever it is selected.
- **BM25 V2 now defaults to exact, language-neutral Unicode retrieval.** Text
  is normalized with NFKC and Unicode case folding without default English
  stopwords or stemming. Bounded prefix matching remains available through
  `NORNICDB_BM25_PREFIX_MAX_EXPANSIONS`, but is disabled by default. Query-plan
  caching is race-safe, equal scores use stable document-ID ordering, and
  configured BM25 property projections are recorded with the analyzer in
  persisted build settings so incompatible indexes rebuild automatically.
  `NORNICDB_SEARCH_BM25_PROPERTIES` provides the property allowlist, while the
  BEIR benchmark now indexes only `title` and `text`. On the official 300-query
  SciFact run, nDCG@10 improved from `0.59974` to `0.66345` and Recall@10 from
  `0.74200` to `0.78761`. Ten-run in-memory benchmarks measured about 67% lower
  common-query latency and 85% fewer allocations than the previous 32-prefix
  default.
- **Vector search storage now uses fixed-stride file-backed records.** Direct
  ordinal offsets replace variable-length record scans, with atomic metadata
  checkpoints, uncommitted-tail recovery, ordinal-based compaction, and batched
  candidate scoring. Vector storage selection supports `auto`, `memory`, and
  `disk`, with independent BM25, vector, and metadata byte ceilings enforced
  during writes and startup index builds.
- **Hybrid retrieval now parallelizes reciprocal-rank fusion and uses bounded
  adaptive IVF-PQ overfetch.** Deterministic policy controls cover candidate
  depth, RRF weights, score thresholds, fallback behavior, and property
  filters without changing default ranking behavior.
- **Refreshed supported build and application dependencies.** Container builds
  use Go `1.27.1`; UI packages, Go modules, and associated lock files were
  updated, including UI Grid `5.0.1`, Lucide React `1.37.0`, and React Router
  DOM `7.18.3`.

### Fixed

- **Local browser authentication now works over loopback HTTP without weakening
  HTTPS deployments.** Session cookies remain `HttpOnly` and `SameSite=Lax`;
  the `Secure` attribute is enabled for direct TLS and TLS-terminating proxies
  that report `X-Forwarded-Proto: https`, and omitted for direct HTTP such as
  `http://localhost`.
- **Initial administrator bootstrap is idempotent and preserves changed
  credentials.** The documented default password is accepted only for initial
  creation with a warning, credentials are stored as salted bcrypt hashes, and
  subsequent startups never overwrite a password changed through the console.
- **Corruption recovery now uses bounded memory.** Snapshot records stream into
  a fresh Badger database in fixed-size batches and WAL entries replay
  incrementally. Failed recovery preserves source data, records a recovery
  manifest, and does not reopen a partial rebuild.
- **Headless mode now disables the complete browser-only HTTP surface.** The
  GraphQL Playground is no longer registered in headless mode, while the
  authenticated GraphQL API and other core APIs remain available.

## [v1.2.3] - 8/20/2026

### Added

- **Heimdall can now use LiteLLM as a chat provider.** This adds LiteLLM
  provider configuration and incorporates review feedback for reliable
  provider initialization and request handling.
- **Added a reproducible BEIR retrieval-recall benchmark with recorded SciFact
  results.** Using the official 300 test qrels and `bge-m3:latest` at 1,024
  dimensions:

  | Retrieval profile                        | Recall@100 | nDCG@10 |
  | ---------------------------------------- | ---------: | ------: |
  | BM25 V2                                  |    0.88422 | 0.59974 |
  | Accurate HNSW, equal RRF weights         |    0.93767 | 0.65534 |
  | Exact CPU brute force, equal RRF weights |    0.94433 | 0.67932 |

  Native BGE-M3 reranking over the same candidates was measured:

  | Profile                            | Recall@100 |  nDCG@10 |   MRR@10 |  MAP@100 |
  | ---------------------------------- | ---------: | -------: | -------: | -------: |
  | Exact equal RRF, no reranker       |    0.93563 |  0.67043 |  0.64404 |  0.63486 |
  | Exact equal RRF with native BGE-M3 |    0.93563 |  0.72292 |  0.69447 |  0.69215 |
  | Absolute change                    |    0.00000 | +0.05248 | +0.05042 | +0.05729 |

  Reranking preserves Recall@100 because it reorders the same candidates.
  These are configuration-specific measurements, not leaderboard claims.

### Changed

- **Updated llama.cpp to b10411.** The llama integration now also accepts
  `LLAMA_LOAD_MODE_MMAP` for configuring model loading.
- **Restored configurable local and remote reranking.** GGUF rerankers support
  rank pooling and classifier output dimensions, query-document pairs use model
  templates or separator tokens, invalid non-scalar outputs are rejected, and
  pooling, attention, context, flash-attention, and provider settings remain
  configurable across local, Ollama, OpenAI, and HTTP providers.
- **Improved hybrid retrieval recall and oversized Ollama embedding handling.**
  Hybrid retrieval uses equal RRF weights across query lengths, and oversized
  Ollama embedding input is chunked through the shared chunker before provider
  evaluation.

### Fixed

- **Bolt explicit transactions now enforce the client-configured lifetime and
  own one cleanup attempt on every terminal path through the database-manager or
  per-connection `TransactionalExecutor` path.** Per-connection executors come
  from `NewWithDatabaseManager` or a `SessionExecutorFactory` that returns a
  distinct `TransactionalExecutor` for every connection. A directly supplied
  `TransactionalExecutor` remains supported only with `MaxConnections: 1` and
  is quarantined after any cleanup failure or uncertain commit failure.
  Multi-connection servers now reject `BEGIN` for a shared raw executor;
  integrations must migrate to a factory. Plain `QueryExecutor` servers retain their
  documented per-`RUN` auto-commit behavior. `BEGIN` validates
  `tx_timeout` as a PackStream long or `null` before allocating a storage
  transaction. Matching Neo4j 5.26, missing, `null`, zero, and negative values
  disable the client deadline, while huge positive values saturate instead of
  failing. A positive lifetime starts when validated `BEGIN` reaches backend
  allocation, so slow allocation consumes the deadline; storage acceptance
  still receives `BEGIN` success before timeout failure on the next operation. Expiry
  cancels an active `RUN`; expiry during an admitted deferred result flush is
  likewise handed to that operation. The session lifecycle then rolls back
  once after the operation releases transaction ownership, before responding,
  without polling an adapter lock or overlapping custom executors. Cleanup uses
  an uncancelled five-second
  request context, while a backend that ignores context remains synchronously
  owned instead of being abandoned. It
  returns Neo4j's transaction-timeout status instead of allowing a later
  `COMMIT`. `RESET`, `ROLLBACK`, `GOODBYE`, and connection loss use the same
  exactly-once rollback arbitration, including persistent Badger/WAL-backed
  transactions. Successful cleanup releases storage ownership; cleanup errors,
  panics, and uncertain commit outcomes fail the connection closed rather than
  claiming release or allowing unsafe reuse. Timeout responses wait for owned
  cleanup, deferred explicit `PULL`/`DISCARD` flush errors require `RESET`, and
  every terminal path discards pending result/flush state so only
  `CommitTransaction` owns commit durability.
- **Empty Cypher deletes no longer perform unnecessary commit work.**
- **Heimdall watcher execution again acquires its pre-execution mutex.**
- **`WITH`-attached Cypher predicates now evaluate label tests and function
  calls correctly.** Label conjunctions, boolean combinations, function calls,
  and null predicates are evaluated against the bound row rather than silently
  passing through or resolving function operands incorrectly.
- **Cypher now preserves bindings across aggregate, traversal, and subquery
  paths.** Correlated relationship matches retain rows for `collect(DISTINCT
...)`; list subscripts preserve node bindings through a subsequent `WITH`;
  relationship list properties work with `IN`; compound multi-hop patterns use
  previously bound anchors; and correlated `EXISTS` / `NOT EXISTS` evaluate
  inner predicates with their outer bindings.

## [v1.2.2] - 8/6/2026

### Security

- **Bolt now authorizes registered procedures by their declared mode.**
  A caller with only `read` permission could previously invoke a `WRITE`
  procedure when its mutation was supplied dynamically, because Bolt classified
  only outer-query keywords. `WRITE`, `SCHEMA`, `ADMIN`, and `DBMS` procedures
  now require their corresponding entitlements before execution. Dynamic nested
  statements inherit the invoking caller's permissions, closing the same bypass
  for parameterized APOC execution in both autocommit and explicit transactions.
- **Bolt rejects malformed PackStream lists before allocating their declared
  size.** `LIST8`, `LIST16`, and `LIST32` headers are validated against the
  remaining payload, preventing unauthenticated peers from using an oversized
  list declaration during `HELLO` decoding to request an attacker-controlled
  memory allocation.
- **Updated AWS SDK dependencies for published security advisories.**

### Fixed

- **Relationship `MERGE` now includes properties from the pattern in its
  identity.**
  NornicDB previously matched relationships by start node, end node, and type
  only. Two assertions such as `[:BUILT_FROM {scope_id: ...}]` between the
  same nodes therefore collapsed into one relationship, and deleting one
  scope could remove the other scope's evidence. Plain and `UNWIND` relationship
  merges now match the complete property pattern. Concurrent writers of the
  same property identity use one deterministic storage key and converge after
  the existing transient-conflict retry, while different identities remain
  independent.
- **Remote relationship `MERGE` now converges on the deterministic edge ID.**
  Remote storage previously translated edge creation to Cypher `CREATE`, which
  could insert duplicate relationships when property-aware `MERGE` retries
  targeted the same generated identity. ID-bearing remote edges now use an
  identity-bearing relationship `MERGE`; legacy empty-ID edge creation retains
  its existing `CREATE` behavior.
- **Empty property maps in relationship `MERGE` patterns are accepted.**
  `MERGE (a)-[:TYPE {}]->(b)` now has the same behavior as an unqualified
  relationship pattern instead of failing parser validation.
- **Bolt's committed-write cache now invalidates from the transaction's
  authoritative operation count.** This prevents a query result cached before
  a committed transaction from being reused after that transaction has changed
  graph state, including writes executed through paths that are not reliably
  identified by outer-query keyword scanning.

## [v1.2.1] - 7/31/2026

### Fixed

- **Cypher `WHERE ... LIMIT` now preserves predicate semantics on non-streaming
  storage wrappers.**
  The MATCH fast path could mark a compilable predicate as already evaluated,
  then fall back to a non-streaming node scan that never applied it. This made
  predicates such as `n.content CONTAINS "safe to delete"` return unrelated
  rows when a `LIMIT` was present. The fallback now applies the full WHERE
  predicate before limiting results, restoring Neo4j's exact-substring
  semantics for multi-word `CONTAINS` and preventing unsafe false-positive
  matches from reaching downstream mutations.
- **MCP `discover` results now use one bounded relevance scale and are ordered
  by the returned `similarity` value.**
  Cross-chunk RRF previously controlled result order while `similarity` exposed
  a mixture of cosine and raw BM25 values, so an exact lexical match could carry
  a score such as `49.09` below unrelated `0.x` results. Vector-backed hits now
  retain their cosine similarity through stage-2 reranking, lexical-only scores
  are monotonically normalized to `[0,1]`, and final results are sorted by that
  same client-visible value. The `min_similarity` tool schema now documents and
  applies the normalized relevance contract.
- **New property-key dictionary entries are now durable before nodes and
  relationships can reference them.**
  Property-key metadata was previously persisted in a best-effort transaction
  after the entity commit, allowing a cleanly acknowledged write to leave an
  unreopenable store if that metadata write failed. New dictionary tokens are
  now persisted before entity records commit, persistence errors abort the
  write, and failed allocations are staged again on retry. This prevents
  startup failures such as `property key id N not in dictionary` after storing
  a previously unseen property name.
- **Automatic snapshot and WAL recovery now works when the data directory is a
  container bind-mount root.**
  Linux rejects attempts to rename a mount point with `EBUSY`, which previously
  trapped standard `-v nornicdb-data:/data` deployments in a restart loop after
  recovery replay had already succeeded. Auto-recovery now falls back to moving
  the corrupted store's children into a hidden forensic directory within the
  mount, then rebuilds the recovered Badger store at the original mount root.
- **OAuth browser flows now reject unsafe callback URLs and issue secure
  authentication cookies.**
  The local OAuth provider validates callback URLs both before rendering
  consent and before redirecting, rejecting external hosts, non-HTTP(S)
  schemes, embedded user information, and fragments. Browser authentication
  cookies, including sessions created through Basic authentication, are now
  marked `Secure`, and the OAuth test harness registers callback URLs
  explicitly instead of accepting arbitrary redirect destinations.
- **Persistence operations are now confined to their configured filesystem
  roots.**
  Search metadata and indexes, vector stores, build snapshots, Badger and JSON
  backups, and APOC exports now use rooted filesystem capabilities instead of
  resolving caller-influenced paths and then opening them by absolute path.
  Non-canonical and traversal paths are rejected at the operation boundary,
  database names must be a single path component before search artifacts are
  created or removed, and atomic replacement and cleanup remain inside the
  configured root.
- **Untrusted sizes and integer conversions can no longer wrap into unsafe
  allocations.**
  Added checked conversion and allocation helpers across Cypher execution,
  traversal, FastRP, search and vector persistence, storage, temporal indexes,
  GraphQL, GPU backends, and protocol adapters. User-controlled `LIMIT` and
  dimension values are no longer used as unchecked capacity hints, serialized
  vector sizes are validated before allocation, and lossy GPU/HNSW conversions
  now fail instead of truncating or wrapping.
- **Credentials and other sensitive values are no longer exposed through
  routine logs or command output.**
  Authentication, OAuth, Bolt, storage, search, replication, Heimdall, and the
  macOS menu-bar paths now omit or redact passwords, API keys, JWTs, query
  parameters, record contents, and other secret-bearing values. CLI startup
  output also avoids echoing credentials supplied through flags or connection
  strings.
- **Security-sensitive cache and composite-key hashes now use keyed SHA-256.**
  Authentication cache keys and storage composite-key indexes no longer rely
  on weaker non-cryptographic hashing, reducing collision and
  hash-manipulation risk while preserving deterministic lookup within a
  running process.
- **Swagger UI bootstrap values are now rendered through safe structured
  encoding.**
  Configured OpenAPI and OAuth values are no longer interpolated directly into
  executable HTML/JavaScript, preventing crafted configuration values from
  breaking out of their intended context.
- **The macOS file indexer now treats ignore rules as glob patterns without
  compiling caller-provided regular expressions.**
  Direct glob matching removes regex-injection and pathological-expression
  behavior while preserving the intended ignore-file semantics; related
  menu-bar logging also no longer prints JWT or API-key details.
- **GitHub Actions workflows now use least-privilege token permissions.**
  Default workflow permissions are read-only, with cache write access granted
  only to the image-build jobs that require it.
- **APOC remote URL loads are now denied by default, and internal Cypher rewrites now reuse the shared escaped-literal path.**
  `apoc.load.json`, `apoc.load.jsonArray`, `apoc.load.csv`, and
  `apoc.import.json` no longer issue arbitrary outbound HTTP(S) fetches unless
  operators explicitly enable `allow_remote_url_access` or
  `NORNICDB_APOC_SECURITY_ALLOW_REMOTE_URL_ACCESS`, and explicitly allow the
  destination host. The remote fetch path now uses a hardened HTTP client with
  redirects disabled and rejects hosts that resolve to loopback, private,
  link-local, multicast, or unspecified addresses. Separately, the
  `WITH`/subquery/CALL rewrite paths now route string and map literal rendering
  through the shared escaped Cypher literal helpers instead of ad hoc quoting,
  closing several executable query interpolation sinks.
- **Updating a relationship a peer transaction committed after this
  transaction began is now a retryable transient conflict instead of a hard
  "not found" error.**
  Inside an explicit transaction, `MERGE` resolves an existing relationship
  through a latest-committed lookup, but the property update read the same
  edge through the transaction's begin-time snapshot. When a peer session
  MERGEd the same relationship with different property values and committed
  after the transaction began, the edge was found but not updatable, and
  `MERGE ... SET` (both the plain and `UNWIND` batch forms) failed with
  "not found", surfaced over Bolt as the non-retryable
  `Neo.ClientError.Statement.SyntaxError`. Neo4j succeeds on this exact
  interleaving (its `MERGE` blocks on the relationship lock, re-reads, and
  applies the `SET`), so drivers' managed-transaction retry never engages on
  the NornicDB error. The storage layer now classifies a snapshot-invisible
  but live edge as the existing conflict shape
  (`conflict: edge <id> changed after transaction start` →
  `Neo.TransientError.Transaction.Outdated`) already used for the same race
  at commit time, so managed transactions (`session.ExecuteWrite`) retry on
  a fresh snapshot and converge to one relationship with the retrying
  writer's values. The reclassification is tombstone-aware: an edge the peer
  actually deleted still reports "not found", and snapshot-isolated reads
  are unchanged.

## [v1.2.0] - 7/27/2026

### Changed

- **APOC local file permissions now use separate import and export toggles.**
  Added distinct APOC security controls for local file reads and writes so
  operators can allow import/load procedures without implicitly allowing export
  sinks, or vice versa. `allow_file_access` remains supported as a legacy
  shorthand that enables both directions, while the preferred configuration is
  `allow_import_file_access` plus `allow_export_file_access`. The APOC config
  docs, environment-variable reference, and sample YAML now reflect the split
  settings and the Neo4j-style `file_access_root` behavior.
- **The UI dependency graph now pins a patched React Router core release.**
  The UI now overrides the transitive `react-router` dependency to the patched
  `8.3.0` line while `react-router-dom` remains on its current published
  release. This clears the known vulnerable router core without requiring a
  breaking application-level routing refactor.

### Fixed

- **Authentication is now enabled by default, and omitted YAML auth settings no
  longer downgrade the secure default.**
  New default deployments now require authentication unless it is explicitly
  disabled. Configuration loading also preserves the secure default when
  `auth.enabled` is omitted from YAML, while still honoring explicit `true` and
  `false` values and the final CLI override path.

- **APOC local file reads now honor explicit security gates and a rooted
  import/export directory policy.**
  `apoc.load.json`, `apoc.load.jsonArray`, `apoc.load.csv`, and
  `apoc.import.json` previously accepted caller-controlled local file paths and
  opened them directly. Local APOC reads are now denied by default, validate
  `file:` URL shape, and when `file_access_root` is configured, normalize and
  rebase local file paths under that root to prevent absolute-path and
  traversal-based file disclosure.
- **APOC export procedures no longer write directly to arbitrary caller-supplied
  paths.**
  `apoc.export.json.all`, `apoc.export.json.query`, `apoc.export.csv.all`, and
  `apoc.export.csv.query` now route local file destinations through the same
  validated path-resolution policy as APOC imports. This closes arbitrary local
  file write and traversal paths by requiring explicit export file access and,
  when configured, constraining writes to the configured `file_access_root`.
- **APOC file-access root configuration now loads independently of the legacy
  combined file-access flag.**
  `NORNICDB_APOC_SECURITY_FILE_ACCESS_ROOT` was previously only honored when
  the combined `ALLOW_FILE_ACCESS` env var was also set. The root path now
  loads independently from environment variables and YAML, so deployments can
  preconfigure a restricted local-file root without relying on legacy flag
  ordering.

## [v1.1.12] - 7/26/2026

### Added

- **Knowledge-policy architecture documentation.**
  Added end-to-end scoring-pipeline and visibility-layer guides covering policy
  evaluation, score composition, temporal decay, access decisions, caching,
  observability, and integration with NornicDB's storage and query layers.
- **Remote-provider-only Heimdall plugin builds.**
  Heimdall plugins that use remote providers such as OpenAI or Ollama can now be
  built without the local GGUF runtime by using the `nolocalllm` build tag. The
  built-in watcher plugin exposes this through
  `make plugin-heimdall-watcher-remote` or
  `make plugin-heimdall-watcher NOLOCALLLM=1`, with matching deployment and Go
  plugin ABI guidance in the documentation.

### Changed

- **Updated supported runtimes and dependencies.**
  Upgraded the bundled llama.cpp runtime from `b9835` to `b10069`, refreshed Go
  dependencies including BadgerDB `v4.9.4`, Prometheus client `v1.24.0`, gRPC
  `v1.82.1`, and `golang.org/x` modules, and updated UI dependencies including
  React `19.2.8`, Lucide React `1.26.0`, Three.js `0.185.1`, Tailwind CSS
  `4.3.3`, and TypeScript `7.0.2`. Build scripts and container images now use
  the matching runtime versions.

### Fixed

- **Explicit `auth.enabled` values in YAML configuration now take precedence.**
  An `auth.enabled: false` setting was previously indistinguishable from an
  omitted value and could leave authentication enabled by defaults or other
  configuration sources. Explicit `true` and `false` values are now both
  honored, while the `--no-auth` startup flag remains the final override.
- **Heimdall native tool-calling follow-up requests now preserve empty assistant
  content.** OpenAI-compatible and Ollama request payloads now serialize
  `content: ""` for assistant messages containing tool calls instead of
  omitting the field, preventing second-round HTTP 400 responses from providers
  that require it. Plugin load failures caused by a mismatched Go package build
  now also report the host toolchain and build settings that must match.
- **Large disjoint UNIQUE-constrained write batches no longer serialize on
  hash-lock collisions.**
  Replaced the fixed 256-stripe commit-lock table with an active,
  reference-counted exact-value registry. Transactions touching the same
  `(label, property, value)` still serialize through validation, Badger commit,
  and unique-cache publication, while disjoint batches commit concurrently.
  Registry-assigned ordering prevents AB-BA deadlocks, entries expire after the
  last holder or waiter, and non-reflexive values such as NaN cannot leak lock
  entries.

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
  statement's own edge deletions _before_ mutating anything (so a multi-row
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
