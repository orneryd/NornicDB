## Context

This is a coordinated implementation plan, not implemented functionality.
The [proposal](proposal.md) defines the version and scope; the
[reference contract](reference-contract.md) separates documented facts from
required differential observations. Shared identity and commit semantics make
these requests interdependent.

### Existing code and concrete changes

Paths are relative to the repository root; proposed new files are labeled new.

| Existing surface | Observed behavior | Planned change |
| --- | --- | --- |
| [auth/privileges.go](../../../pkg/auth/privileges.go), [roles.go](../../../pkg/auth/roles.go), [allowlist.go](../../../pkg/auth/allowlist.go) | Per-role database booleans; Resolve falls back to RolePermissions | Consume #935's canonical scoped privilege records/evaluator, including target-user IMPERSONATE and procedure EXECUTE/BOOSTED; delete the boolean/global fallback path |
| [auth/auth.go](../../../pkg/auth/auth.go), [auth_adapter.go](../../../pkg/bolt/auth_adapter.go) | User directory, stable principal IDs, disabled users, auth cache | Resolve an impersonated user without their password; validate authenticated principal's IMPERSONATE before creating execution context; invalidate identity/authorization caches |
| [session_messages.go](../../../pkg/bolt/session_messages.go) | RUN checks authResult; BEGIN saves extras; ROUTE ignores payload; identity cache only keys on authResult | Parse versioned extras, resolve effective identity before authorization/database selection, pin it to tx/result stream, and clear on all terminal paths |
| [transaction_executor_adapter.go](../../../pkg/bolt/transaction_executor_adapter.go) | BeginTransaction discards its metadata argument | Pass typed trusted transaction attributes through the existing lifecycle to storage |
| [show_admin.go](../../../pkg/cypher/show_admin.go), [executor.go](../../../pkg/cypher/executor.go) | RequestIdentity has one user; separate authenticated principal is used for ownership | Separate authenticated/executing identities and safe commit attributes; keep authenticated ownership distinct from effective authorization |
| [authorization.go](../../../pkg/cypher/authorization.go), [procedure_registry.go](../../../pkg/cypher/procedure_registry.go), [procedure_registry_builtin.go](../../../pkg/cypher/procedure_registry_builtin.go) | Coarse read/write/schema/admin and procedure mode checks | Canonical statement/procedure authorization, including special boosted CDC read; register actual signatures, modes, output fields and composition |
| [server_router.go](../../../pkg/server/server_router.go), [server_db.go](../../../pkg/server/server_db.go) | `/db/` prechecks PermRead; router supports tx/cluster, no query/v2; old statements-array envelope | Route Query API separately, authenticate before target resolution without requiring caller graph READ; new v2 request/response adapters share execution/session core |
| [txsession/manager.go](../../../pkg/txsession/manager.go) | Explicit sessions have database, owner, executor, TTL; default TTL is 30s | Pin effective security context, distinguish protocol ownership, implement oracle-confirmed Query API TTL (documented 60s), serialize operations and terminal cleanup |
| [call_compat.go](../../../pkg/cypher/call_compat.go) | tx.setMetaData reaches BadgerTransaction.SetMetadata | Keep one metadata mutation contract; deep-copy inputs; propagate to active transaction listing and final CDC envelope |
| [executor_show.go](../../../pkg/cypher/executor_show.go), [executor_query_routing.go](../../../pkg/cypher/executor_query_routing.go) | CREATE emits a name row; SHOW options is {}; ALTER expects SET LIMIT | Extract option DDL handling, preserve non-conflicting extensions, replace touched standard DDL results/errors with 5.26 contracts |
| [multidb/manager.go](../../../pkg/multidb/manager.go), [metadata.go](../../../pkg/multidb/metadata.go), [routing.go](../../../pkg/multidb/routing.go) | Persisted DatabaseInfo and UUIDs; namespace/alias routing | Persist validated enrichment option and lifecycle state; expose manager contract to Cypher and storage; fence option transitions against commits |
| [badger_transaction.go](../../../pkg/storage/badger_transaction.go), [badger_mvcc.go](../../../pkg/storage/badger_mvcc.go) | Commit has physical operations/net states; cascade op only keeps edge IDs | Reuse net-state reduction, retain deleted edge bodies and endpoint snapshots, stage CDC before publication, not as a post-commit hook |
| [badger_commit_writer.go](../../../pkg/storage/badger_commit_writer.go), [badger_managed.go](../../../pkg/storage/badger_managed.go) | One logical commit can span invisible batches; oracle publishes contiguous finished timestamps | Make CDC records/head part of the same writer, undo/recovery, and publication boundary; do not equate physical batch timestamps with user tx IDs |
| [badger.go](../../../pkg/storage/badger.go) | MVCC sequences reserved before commit and persisted separately; prefix 0x25 is used | Allocate CDC logical order independently; reserve unused 0x26 after collision check; do not expose MVCC reservation sequence as CDC order |
| [badger_nodes.go](../../../pkg/storage/badger_nodes.go), [badger_edges.go](../../../pkg/storage/badger_edges.go), [badger_bulk.go](../../../pkg/storage/badger_bulk.go) | Six individual graph writes can commit directly; bulk creates use transactions | Route capture-enabled writes through shared logical finalization; cover every bulk/delete/update variant and prevent nested capture |
| [key_families.go](../../../pkg/storage/key_families.go), [badger_backup.go](../../../pkg/storage/badger_backup.go), [bytes_metrics.go](../../../pkg/storage/bytes_metrics.go) | Explicit family inventory and namespace ownership logic | Register event/control records for accounting, backup, restore, drop, prefix deletion and recovery |
| [wal_engine.go](../../../pkg/storage/wal_engine.go), [wal.go](../../../pkg/storage/wal.go) | WAL/receipts are not a complete before/after change journal | Keep recovery/receipt responsibilities; no CDC reads, cursor, or retention fallback to WAL/MVCC history |
| [run-differential.sh](../../../scripts/cypher-tck/run-differential.sh), [differential tests](../../../testing/cypher/differential/differential_test.go) | Community image, auth disabled, generic comparison allows unordered rows | Separate authenticated Enterprise lane and strict protocol/CDC comparator; retain existing Community TCK lane |

## Goals and non-goals

Match the pinned APIs, response shapes, errors, authorization, attribution,
transaction boundaries, durability, and lifecycle. Use the same execution and
storage semantics for Bolt, old HTTP, and Query API. All implementation tasks
remain unchecked until their tests and persistent effects are verified.

Not in scope: Neo4j store/log binary formats, Kafka/connectors, Aura operations,
Cypher 25-only features, changing decay policy, or rebuilding the execution
router. Native search and remote/Fabric paths are in scope only where effective
identity or graph writes cross them. #935 owns its broader privilege work;
this change cannot claim its acceptance without that dependency.

## Decisions

### 1. One security context, two identities

Extend the existing request-identity contract using an auth-owned immutable
security context (proposed `pkg/auth/security_context.go`) rather than a second
context per transport. It contains authenticated principal, executing principal,
effective roles/privilege revision, target home database, and impersonation state.
Request-supplied user metadata cannot populate trusted fields.

Resolve in order: authenticate caller; validate the requested target's wire type;
evaluate caller IMPERSONATE with DENY precedence; resolve/check target; build the
target's complete security context; resolve home/explicit database and ACCESS;
authorize and execute. Oracle tests fix precise lookup/error precedence and
revocation timing. Do not union caller roles with target roles. Impersonation
also prohibits updating administration commands, even when the target is admin.

Bolt's connection authResult remains the authenticated principal. BEGIN pins
execution identity; RUN inside that transaction cannot replace it. Autocommit
identity lasts through PULL/DISCARD/commit, not just handleRun: a streamed
auto-commit read (#939) keeps running until its last PULL, with a helper
goroutine answering PULLs, so the identity must travel in the statement's
context rather than be read from the session while it streams. ROUTE uses the
target home database/access without changing the connection principal.
RESET, timeout, failed BEGIN, rollback, disconnect, LOGOFF/LOGON and pool reuse
must not leak a target to the next request. Avoid closures reading mutable
session authResult after resolution.

Replace all coarse prechecks on affected routes with #935's evaluator; simply
changing a context value after the old precheck is insufficient. Cache keys
include effective policy identity/revision and database; ownership-sensitive
artifacts also bind authenticated owner. Target changes and revocation invalidate
authorization-dependent entries. Apply to USE, aliases, subqueries, APOC/search,
result continuations and link prediction through #935's shared enforcement.
Remote execution must carry a validated delegation understood by the receiver;
never forward only the service account credential and drop the target. Until
a receiver supports that contract, reject before remote effects, not silently
execute as caller. Full remote support is a release gate where that route exists.

### 2. Canonical privileges and a deliberate cutover

IMPERSONATE is a DBMS action with named-user or wildcard scope. Use #935's
GRANT/DENY records, role membership including PUBLIC, immutable handling, revoke
forms, persistence, SHOW and AS COMMANDS renderer. Do not bolt new string
permissions onto the existing Read/Write booleans.

Ship a versioned, one-time migration of existing role/allowlist/privilege data
into the canonical representation. Produce a dry-run mapping and require
operator resolution for entries with no unambiguous equivalent. Preserve user
identifiers and credential storage; do not silently turn viewer/custom roles
into a more privileged built-in. After migration, remove dual reads and the
global permission fallback. Migrate UI/access-management consumers and either
make their native API operate on canonical records without loss or retire
lossy matrix endpoints with an explicit error. No endpoint may overwrite fine
grained rules using the old boolean matrix.

### 3. Shared transaction attributes, not protocol dictionaries in storage

Introduce a storage-level value type (proposed `pkg/storage/transaction_attributes.go`)
for trusted users, database/server identity, connection protocol/client/server,
start time and user tx metadata. Auth resolves it; adapters attach it at
transaction creation; storage owns a copy. Storage does not import auth,
Cypher, or HTTP. Commit time and transaction order are assigned by storage.

Wire explicit Bolt adapter BEGIN, autocommit extras, Cypher implicit transaction
creation, old HTTP sessions, Query API sessions and CALL IN TRANSACTIONS children.
`tx.setMetaData` uses the same storage setter with oracle-tested semantics;
one commit's events all use its final metadata snapshot. Deep-copy nested values.
User changes to metadata cannot spoof authenticated/executing user or timestamps.

Separate delivery into ordinary/system attribution (tasks 2a) and canonical
impersonation integration (2b). Ordinary adapters already know the authenticated
principal and set authenticated=executing; internal writers use explicit system
origin. The transport-neutral dual-user attribute type, metadata setter,
transaction propagation and ordinary cleanup tests need no #935 evaluator.
Only target resolution/impersonated attribution and corresponding SHOW/lifecycle
tests wait on canonical security. Do not make CDC storage/event work transitively
depend on full RBAC merely because it consumes transaction attributes.

Expose attributes from active transactions to SHOW and query/audit logging.
Keep both users internally even if 5.26 SHOW exposes only a formatted username;
render precisely the oracle's columns/values. Keep repository structured logging
and redaction, with a compatible query-log attribution projection; do not invent
a fake Neo4j log file or leak credentials. Internal writers receive an explicit
system-origin context; absent user identity is not silently attributed to admin.

### 4. Query API v2 is a protocol adapter, not an old HTTP wrapper

Add focused files under `pkg/server`: `query_api.go`, `query_api_values.go`,
`query_api_transactions.go` and matching tests (new). Extract only reusable
execution/session helpers from the already-large server_db.go.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/db/{database}/query/v2` | One implicit statement |
| POST | `/db/{database}/query/v2/tx` | Begin, optionally execute |
| POST | `/db/{database}/query/v2/tx/{id}` | Execute or keep alive |
| POST | `/db/{database}/query/v2/tx/{id}/commit` | Optional final statement and commit |
| DELETE | `/db/{database}/query/v2/tx/{id}` | Rollback |

Request fields for oracle confirmation: statement, parameters, includeCounters,
accessMode (Read/Write), bookmarks and impersonatedUser. Use single-statement
request bodies, not `statements: []`. Respect omitted/empty/null distinctions per
endpoint and independently negotiate Accept and Content-Type for application/json
and application/vnd.neo4j.query.v1.2. Enforce limits and preserve int64 values.
Unknown fields and unsupported media types follow the pinned server, not an
invented strictness policy or an unconditional ignore.

Return `data.fields` and row-ordered `data.values`; conditionally return counters,
notifications, query plan/profile, bookmarks, transaction id/expires and errors
exactly as 5.26 does. Success is generally 202 and auth failure 401; freeze
parse/media/authorization/not-found statuses separately before implementation.
Do not copy newer queryType/timing/txMetadata/maxExecutionTime/notification-filter
features into the pinned surface.

Use native typed values from Cypher/storage and existing conversion helpers;
add codecs where v2 differs from old HTTP. Cover all primitive/collection,
temporal/spatial and graph/path forms, special floats and typed parameter
rejection. Never serialize unsupported values with fmt.Sprint or reuse old
HTTP `row/meta/graph` wrappers. Streaming must preserve the reference's late
error envelope and commit boundary; cancellation before commit rolls back,
while a delivery error after durable commit must not pretend it rolled back.

Use txsession.Manager with a protocol-scoped session key, authenticated owner,
database, and pinned execution context. Oracle-confirm the 60-second idle
timeout and update expiry on keepalive. Determine continuation reauthentication
and target-field semantics from QAPI-03/04; do not let continuation rebind the
target or let one HTTP protocol access another's transaction by ID. Preserve
safe serialization for overlapping requests. No synthetic cluster-affinity
header on a standalone server.

### 5. Commit identity, CDC storage and atomic publication

Do not use tx.ID strings, wall-clock HTTP bookmarks, WAL offsets, or reserved
MVCC sequence numbers as a published CDC transaction ID.

Add a namespace-scoped logical commit coordinator (new
`pkg/storage/change_capture.go`) for capture-enabled graph commits. Take its
gate after the commit's unique-key commit locks (#961, #964's
`acquireUniqueConstraintCommitLocks`) and constraint validation, and hold
it only while assigning a monotonically increasing integer logical txId,
staging graph changes, events and durable head, and publishing through
commitWriter. A commit can wait on another transaction's unique-key lock until
that transaction commits; a gate taken before those locks would let A hold the
gate while waiting for B's key and B wait for the gate to commit, a cycle the
key-lock deadlock detector cannot see. Gaps are allowed; visible commits cannot
reorder and abandoned reservations cannot block readers. Use one acquisition per
logical commit, not per physical batch or nested direct write. Establish and
test an acquisition/release ledger against unique-key locks, write barriers,
count locks, physical large-commit gates, schema commits, close, retention and
option transitions before wiring the gate. "After key locks" is not a blanket
claim that every publication lock can simply be taken last. Preserve existing
count-before-large-commit ordering, establish the coordinator's exact placement
relative to those locks, and never reacquire unique keys while holding it.
Controlled-schedule tests must force the reported A/B wait and verify progress
and release after small/large commit failure, cancellation and recovery.

The existing managed commitOracle remains the authority for physical
visibility/recovery; this coordinator is only logical per-database ordering.
No new global mutex serializes unrelated databases. OFF mode need not retain
event bodies, but the shared durable commit-position service for bookmarks must
still describe real published commits on those databases.
Reuse the existing commitWriter/publication/durability machinery for positions
rather than adding a separately synced head transaction merely for bookmarks.
Position tracking cannot be assumed free: benchmark the position-only OFF
implementation against the pre-position baseline before CDC is layered on,
including head writes, fsyncs, allocations, RSS and same/independent-database
contention. OFF bypasses event construction/serialization, not durable position
correctness; retain the performance/memory budgets below.

Reserve 0x26 only after checking the family registry at implementation time.
Use length-delimited namespace plus record kind; event keys include capture
epoch, logical txId and seq. Control records hold epoch, published head, earliest
boundary and segment metadata. Store native values with explicit record version.
Include this family in namespace ownership, backup, accounting and drop; never
reassign retired prefixes.

CDC writes MUST use commitWriter/batchWriter, including its large-commit
rollback/recovery. Write failure aborts the graph transaction. No post-commit
best-effort outbox append, replay-based event construction, or WAL fallback.
Durable acknowledgement must not precede the durability boundary required by
the pinned contract; review SetImplicit's skipped Sync and transport flush paths.
After crash, recover graph/event/head together and seed logical order from
durable state, not process counters.

### 6. Capture net changes with stable event semantics

Share newCommitStates' first-before/final-after reduction with MVCC. Do not
independently interpret mutation query text. Enhance cascade deletion to retain
relationship bodies and endpoint labels/keys; deduplicate self-loops and edges
deleted explicitly and through DETACH. Preserve before states through all
physical-operation compaction and deferred-edge paths.

Build events for the oracle's net changes; create-then-delete and update-revert
cases require explicit fixtures. Emit category order: node create, relationship
create, node update, relationship update, node delete, relationship delete.
Use oracle-defined within-category ordering, not Go map iteration or statement
order. Assign seq once after netting/ordering.

Node events: eventType, elementId, operation, complete top-level labels,
label-to-list-of-key-maps keys, and state.before/state.after (labels/properties).
Relationship events: eventType, elementId, operation, type, list-of-key-maps keys,
start/end (elementId/labels/keys), and property-only before/after.
Creates have null before and full after; deletes full before and null after
in both modes. FULL updates contain full states; DIFF contains changed
labels/properties, including removals with the oracle's exact missing/null form.
Freeze top-level label/key/endpoint before/after choice in CDC-03/04.

Resolve key constraints, including equivalent uniqueness plus existence
constraints, against the schema and entity states belonging to the commit.
Do not query today's graph when serving historical events. Keep Cypher values
typed through storage, Bolt PackStream and both HTTP encodings.

Route every capture-enabled write through this boundary: implicit async CREATE
(both batching and eventual routes), explicit transactions, CALL IN TRANSACTIONS
batches, six direct engine CRUD paths, bulk methods, namespace/WAL/async/
storage-size wrappers, API-native mutations and background workers.
Internal index/cache/embedding-only maintenance is not a user graph event;
background changes to user-visible graph properties/labels/edges are captured
under explicit system origin. Do not let bulk/API writes cross namespaces in
one logical per-database commit. Reject incapable engines before effects;
remove the affected direct-execution capability fallback.

### 7. Database options and procedure authorization

Persist txLogEnrichment with DatabaseInfo and expose it via the existing manager
interface. Observed on the reference ([CDC-01 evidence](evidence/cdc-01-database-options.txt)):
values are case-insensitive and stored upper-case; an unset option is absent
from `options` (`{}`), not `OFF`, and REMOVE OPTION returns to that state.
Add shared lexical parsing for CREATE OPTIONS, ALTER SET OPTION and
REMOVE OPTION; consume all tokens and support parameter expressions allowed
by 5.26. System/composite/alias legality and explicit admin transaction behavior
are oracle-defined. Replace CREATE's current name-row response where it
conflicts; SHOW must report actual options with correct default/YIELD columns.

Option transitions have a linearization point against transaction finalization:
no half-DIFF/half-FULL transaction, no write lost between enabling and admission.
Determine whether in-flight transactions capture the begin/commit mode from
CDC-01, then enforce that boundary. OFF increments a durable capture epoch,
invalidating all prior cursors even after re-enable; DIFF/FULL preserves history
and records each transaction's actual capture mode.
Because manager metadata is in system storage while capture state belongs to a
data namespace, implement a durable transition intent plus startup completion
under the namespace fence; do not rely on two unrelated successful writes.

Implement three procedures in the existing registry, with typed invocation and
normal YIELD/WHERE/RETURN support (new `pkg/cypher/call_cdc.go`). Use a narrow
storage read capability passed through wrappers, not storage downcasts scattered
across Cypher. Current/earliest need ACCESS and ordinary EXECUTE.
Query needs ACCESS, EXECUTE and EXECUTE BOOSTED (including DENY/glob semantics)
and then returns all captured changes irrespective of graph read filters.
Do not require the admin role name or run the query through the target's
filtered entity reader after successful boosted authorization.

Separate implementation readiness from public enablement. Core options,
events, selectors, scans and retention use internal fixtures and ordinary
attributes without waiting on full #935. Public option DDL requires canonical
database-administration privileges; current/earliest require canonical
ACCESS/EXECUTE; query also requires BOOSTED. These are the shared persisted
privilege/evaluator subset (1a), not a temporary policy engine. Keep each public
procedure unregistered until its authorization tests pass; no admin-name or
coarse-read fallback. Full graph/impersonation acceptance remains gated on
1b/2b, but does not block core implementation or ordinary-user subset tests.

### 8. Cursors, selectors and bounded scans

Return opaque versioned database/epoch-bound cursor strings. Distinguish a
transaction-end current boundary, earliest boundary before first retained event,
and an event position; an event resume is exclusive only of that event and
earlier events, not the rest of its transaction. Cursor parsing validates
encoding, version, database UUID, epoch, retention floor and future positions
with oracle-mapped errors. Restore/drop-recreate cannot reuse old cursors.

Implement namespace/epoch/txId/seq range iteration with a stable published upper
bound, context cancellation and bounded batches. Procedures currently return
materialized slices; add a streaming result contract to the registry and CALL
operator rather than assuming transport streaming makes the source lazy.
Wire demand/stop through YIELD/WHERE/RETURN to consuming Bolt/HTTP adapters.
#939's lazy Bolt work in #968 was OPEN at review on 2026-10-08; reuse that
infrastructure once available, but explicitly extend CALL sources and LIMIT
instead of treating plain RETURN support as sufficient.

Instrument event visits, peak buffered rows/bytes and iterator/snapshot closure.
For a nonblocking unfiltered LIMIT 1, seek to the cursor without visiting
earlier events and visit at most 1+B events, where B is a fixed documented total
prefetch budget across iterator and producer, independent of history size.
With selectors/WHERE, the first match at position k may require k+B visits;
no-match may consume the published window while keeping buffering bounded.
ORDER BY and aggregation are blocking operators and may consume that window;
test correct results and report their materialization cost separately rather
than promising constant scan work/memory for every composition.

Stop upstream iteration once LIMIT is satisfied within the fixed prefetch
budget. Release iterators, snapshots and producer tasks on exhaustion, early
termination, errors, PULL/DISCARD/RESET, cancellation and HTTP disconnect.
Protect active scans from concurrent pruning using the managed snapshot model;
do not pin readers beyond consumer lifetime.

Compile and validate selectors once per invocation. OR across selectors, AND
across all fields, deduplicate matches by event identity. Support e/n/r,
operation, changesTo, elementId, labels, key, type, start/end, executingUser,
authenticatedUser and txMetadata with exact null/type/unknown-field errors.
Endpoint selectors allow node identity/keys/labels but not operation/changesTo.
Matching uses captured before/after/key information, not live storage.

### 9. Retention, restart, backup and bookmark replacement

Map `db.tx_log.rotation.retention_policy` to the native logical change journal,
default **2 days 2G**. Support pinned grammar: keep_all/true, keep_none/false,
files, size, txs/entries, hours/days and valid combined space restrictions.
Use logical segments of complete transactions, persisted byte/count/time
metadata, configured rotation and a safe checkpoint/pruning boundary. Preserve
at least the latest nonempty segment; do not substitute per-event TTL deletion.
Measure bytes in Nornic's native journal representation, not pretend they equal
Neo4j's store-format bytes. The retention-policy and cursor behavior is the
compatibility surface, not identical physical disk footprint.

Wire configuration validation/startup/dynamic setting and SHOW SETTINGS paths
through existing config facilities; provide the 5.26 checkpoint trigger/settings
surface necessary to exercise retention. Keep this independent of WAL compaction
and MVCC history pruning. Retention only removes whole eligible transactions;
update floor atomically with pruning, preserve active readers, expose errors.

Restart retains mode, epoch, head and floor. Backups include CDC/control records;
restore to a new logical database invalidates source cursors as Neo4j does.
Use existing storage format/version gates to prevent an older writer opening a
CDC-enabled store and silently missing events; enabling new persisted semantics
must not be labeled downgrade-safe merely because 0x26 was unused.

Replace server_db.generateBookmark and session_messages' process-counter/
placeholder fallback with a shared durable database commit-position capability.
Reuse existing receipt plumbing only where it proves committed state. Bolt,
old HTTP and Query API consume the same token semantics: wait for referenced
published state with cancellation/deadline, validate scope and map errors.
Remove `nornicdb:tx:auto` acceptance and wall-clock `FB:nornicdb` generation.
CDC cursor and causal bookmark are distinct types, never interchangeable.
No fake success or immediate future-token rejection in place of causal waiting.

## Delivery, migration and risks

Follow the task DAG, not an unconditional series of independent feature PRs.
Oracle and shared privilege/identity contracts precede public enablement.
Wire CDC storage before admitting txLogEnrichment=DIFF/FULL. Query API is not
complete until its lifecycle, codecs and impersonation pass, not just routing.

Before cutover: back up the store; report role mappings and new storage format;
drain active transactions/async writes; migrate canonical privileges; deploy
one behavior; obtain new bookmarks/cursors as documented. A failed migration
leaves startup blocked with an actionable error, not a legacy runtime fallback.
Rollback to older binaries requires restoring a compatible pre-migration backup.
No pre-enable history is synthesized from MVCC/WAL.

Main risks: lock inversion and serialized same-database commit throughput,
multi-batch crash atomicity, large FULL event size, stale authorization caches,
cross-transport transaction ownership, ambiguous evolving docs, and exposing
all graph changes through an insufficiently protected procedure.

Measure OFF/DIFF/FULL, authenticated/impersonated, Bolt/old HTTP/v2 separately:
single creates, batched UNWIND, mixed node/edge updates/deletes, large commits,
selector scans, resume and many independent databases. Report ops/sec, p50/p95/p99,
allocs/bytes per op, peak RSS, bytes per captured event, fsync cost and recovery
time on named hardware. OFF mode must not regress >5% latency/throughput or
>10% memory without explicit reviewed justification; captured modes have
measured overhead, not an invented "free" target. Require bounded-memory scans,
race tests, >90% new-code coverage (core paths target 95%), and fresh-session/
reopen evidence alongside strict differential results.
