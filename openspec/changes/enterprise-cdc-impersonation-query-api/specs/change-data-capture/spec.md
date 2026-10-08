## Purpose

Provide Neo4j Enterprise 5.26.30 CDC over NornicDB's native atomic commit
journal. This is a versioned Neo4j addition, not an openCypher feature or an
API over NornicDB's existing WAL/MVCC history.

## ADDED Requirements

### Requirement: Persistent database enrichment option

The system SHALL support `CREATE DATABASE ... OPTIONS {txLogEnrichment: ...}`,
`ALTER DATABASE ... SET OPTION txLogEnrichment ...`, and `REMOVE OPTION
txLogEnrichment`, with OFF default and DIFF/FULL capture modes. DDL legality,
parameter handling, errors, counters, response rows and SHOW DATABASES
projections SHALL match 5.26.30. Option changes SHALL persist and serialize
against capture admission/finalization without partial transaction modes.

#### Scenario: Create and inspect an enabled database

- **WHEN** a permitted administrator creates a database with FULL enrichment and lists it after restart
- **THEN** SHOW DATABASES reports the persisted option and DDL result shape matches the reference rather than a Nornic-only name row (`CDC-01`)

#### Scenario: Option spelling and the unset state

- **WHEN** an option value is given in lower or mixed case, or the option is never set or is removed
- **THEN** the stored and shown value is upper-case, and an unset or removed option is absent from `options` (`{}`) rather than shown as `OFF` (`CDC-01`)

#### Scenario: Invalid mode or invalid database kind

- **WHEN** an option value has the wrong type/value or targets a disallowed system/composite/alias form
- **THEN** the reference error occurs and no database or option state changes (`CDC-01`)

#### Scenario: OFF breaks continuity

- **WHEN** CDC is disabled by SET OFF or REMOVE OPTION and later re-enabled
- **THEN** all prior-epoch cursors remain invalid, no disabled-interval history is synthesized, and the new capture boundary is durable (`CDC-01`, `CDC-08`)

### Requirement: Atomic committed change capture

Every committed user-visible graph change on a capture-enabled database SHALL
be captured exactly once per net changed entity according to the reference.
Graph state, event records and published capture head SHALL share one logical
atomic commit, including managed multi-batch commits. Rollbacks, failed
constraints, canceled uncommitted operations and net-zero cases SHALL emit
no events where the reference emits none.

#### Scenario: Commit fails after writing an invisible batch

- **WHEN** a fault interrupts a large commit before publication
- **THEN** recovery exposes neither its graph changes nor its events/head, and a later successful commit remains readable (`CDC-09`)

#### Scenario: Different API and storage write paths

- **WHEN** equivalent writes use Bolt, old HTTP, Query API, explicit/implicit transactions, bulk calls or capture-enabled direct-engine wrappers
- **THEN** each path obeys the same logical transaction/event contract and no asynchronous batch merges unrelated client transactions (`CDC-09`)

#### Scenario: Capture capability is unavailable

- **WHEN** enabling capture or executing a capture-required write encounters an incapable engine or wrapper
- **THEN** it reports an explicit error before effects and does not fall back to uncaptured direct execution (`CDC-09`)

### Requirement: Native event envelope

Query rows SHALL have `id`, `txId`, `seq`, `metadata` and `event` in the pinned
order and types. Metadata SHALL include the oracle-confirmed 5.26 identity,
capture mode, connection, server/database, start/commit time and txMetadata
fields without speculative later-version additions.

#### Scenario: Inspect a committed transaction envelope

- **WHEN** a caller queries multiple events from one committed transaction
- **THEN** columns and native types match 5.26, each event has its own id/seq, and all events share txId and transaction metadata (`CDC-03`)

### Requirement: Entity event schema and net state

Node events SHALL expose eventType n, elementId, operation c/u/d, labels,
label-key maps and before/after labels/properties. Relationship events SHALL
expose eventType r, elementId, operation, type, relationship keys, start/end
identity/labels/keys and before/after properties. State and key snapshots
SHALL belong to the committed change, not the graph at query time.

#### Scenario: FULL and DIFF updates

- **WHEN** a transaction adds/removes labels and properties and changes an existing property
- **THEN** FULL includes complete before/after states while DIFF includes precisely the reference's changed values, labels and missing/null representation (`CDC-03`)

#### Scenario: Create and delete in both capture modes

- **WHEN** entities are created or deleted in DIFF or FULL
- **THEN** creates have null before/full after and deletes full before/null after, including detached relationship bodies and endpoint information (`CDC-03`, `CDC-04`)

#### Scenario: Key constraints change after capture

- **WHEN** key constraints, or equivalent uniqueness-plus-existence constraints, exist for a captured change and are later altered
- **THEN** historical events retain their original node/relationship/endpoint key snapshots and new constraints do not retrofit older events (`CDC-03`)

### Requirement: Ordered logical transactions and exclusive cursor resume

Visible transaction IDs SHALL be database-local, ordered by logical publication,
and unique with seq; gaps SHALL be permitted. Events SHALL use the reference
category ordering and within-category contract rather than statement order or
map iteration. Opaque cursors SHALL encode a database/epoch-bound position and
be validated against published and retained state.

#### Scenario: A transaction mixes entity operations

- **WHEN** one transaction creates, updates and deletes both nodes and relationships in an interleaved statement order
- **THEN** CDC emits node creates, relationship creates, node updates, relationship updates, node deletes and relationship deletes with unique ordered seq values (`CDC-04`)

#### Scenario: Resume from the middle of a transaction

- **WHEN** a consumer saves an event cursor before the final event in its transaction
- **THEN** querying that cursor returns the remaining events after it without duplicates or skipping the entire transaction (`CDC-05`)

#### Scenario: Current and earliest boundaries differ

- **WHEN** a consumer queries from current or earliest
- **THEN** current excludes the transaction it represents, while earliest permits retrieval of the first retained event (`CDC-02`, `CDC-05`)

#### Scenario: Cursor is invalid for this history

- **WHEN** the cursor is malformed, foreign-database, future, expired, old-epoch or invalidated by restore/drop-recreate
- **THEN** the reference error is returned rather than an empty successful result or fallback to current/earliest (`CDC-05`, `CDC-08`)

### Requirement: Procedure signatures and composition

The registry SHALL expose db.cdc.earliest(), db.cdc.current() and
db.cdc.query(from, selectors) with 5.26 signatures/defaults/output metadata.
Current and earliest SHALL return the pinned single id column. Query SHALL
default omitted from to the reference's current boundary and selectors to an
empty list, and SHALL distinguish explicit null from omission as the oracle
does. Procedures SHALL compose with YIELD/WHERE/RETURN/ORDER BY/LIMIT through
shared execution semantics.

#### Scenario: Poll with a default start position

- **WHEN** a caller invokes db.cdc.query with omitted/default from
- **THEN** it uses the current boundary rather than scanning all history, and optional/default/null argument behavior matches 5.26 (`CDC-02`)

#### Scenario: Get the oldest available event

- **WHEN** a caller pipes earliest into query and returns LIMIT 1
- **THEN** it gets the first retained event with bounded-memory iteration and no new CDC event caused by the read (`CDC-02`, `CDC-08`)

### Requirement: Complete selector semantics

Selectors SHALL implement OR across the list and AND within each selector,
supporting e/n/r, operation, changesTo, elementId, labels, key, type, start,
end, executingUser, authenticatedUser and txMetadata. They SHALL match captured
state with the oracle's before/after rules, not live graph properties. Invalid
types/fields/shapes SHALL report the pinned error rather than being ignored.

#### Scenario: Match all changed properties with metadata

- **WHEN** a selector requests changesTo [a,b] and a txMetadata subset
- **THEN** it matches only events satisfying both changed-property conditions and the specified metadata entries; two separate selectors instead use OR (`CDC-06`)

#### Scenario: Relationship endpoint selector

- **WHEN** a start/end selector specifies node keys and labels
- **THEN** captured endpoint state is used; operation/changesTo or a non-node select value in that nested selector fails as in the reference (`CDC-06`)

#### Scenario: One event matches multiple selectors

- **WHEN** overlapping selectors all match one captured change
- **THEN** the result contains that change once, in its original order and with unchanged seq (`CDC-06`)

### Requirement: Explicit privileged access to all events

Current/earliest SHALL require database ACCESS and ordinary procedure EXECUTE.
Query SHALL require ACCESS, EXECUTE and EXECUTE BOOSTED using #935's canonical
grants, denies and procedure patterns. After successful authorization it SHALL
return the full matching change stream regardless of caller graph READ/TRAVERSE
restrictions. Impersonation SHALL evaluate these privileges on the target.

#### Scenario: Reader lacks boosted execution

- **WHEN** a user can access a database and execute procedures but lacks required boosted permission
- **THEN** current/earliest may succeed but query fails with the reference error (`CDC-07`)

#### Scenario: Authorized reader cannot traverse an affected entity

- **WHEN** the user has all CDC query privileges but cannot traverse a changed node
- **THEN** query still returns that node's matching event, rather than filtering it as an ordinary graph read (`CDC-07`)

### Requirement: Native journal retention and history lifecycle

Retention SHALL expose the 5.26 `db.tx_log.rotation.retention_policy` grammar,
default `2 days 2G`, valid dynamic changes and observable setting errors. It
SHALL operate on native logical journal segments, retain complete transactions
and the newest nonempty segment, and prune only at a safe checkpoint boundary.
It SHALL remain independent of WAL compaction and MVCC pruning.

#### Scenario: Consumer falls beyond retained history

- **WHEN** a safe prune advances the retained floor past a consumer cursor
- **THEN** the cursor fails with the reference retention error and earliest provides the new valid boundary, persisted across reopen (`CDC-08`)

#### Scenario: Pruning overlaps a query

- **WHEN** an active bounded CDC scan overlaps retention and concurrent commits
- **THEN** it sees a consistent permitted history window without partial transactions, missing middle rows or use-after-close errors (`CDC-08`)

#### Scenario: Backup, restore and namespace removal

- **WHEN** a database is backed up, restored as a new logical database or dropped
- **THEN** CDC/control records participate in that lifecycle and storage accounting, source cursors cannot access restored history, and other namespaces are unaffected (`CDC-08`, `CDC-09`)
