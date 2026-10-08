## Purpose

Implement Neo4j Enterprise 5.26.30 Query API v2 as a native NornicDB HTTP
adapter using shared security, Cypher and transaction semantics. The older
transaction HTTP API remains independently supported, not a fallback.

## ADDED Requirements

### Requirement: Versioned Query API endpoints

The server SHALL implement POST `/db/{database}/query/v2`, POST
`/db/{database}/query/v2/tx`, POST `/db/{database}/query/v2/tx/{id}`,
POST `/db/{database}/query/v2/tx/{id}/commit` and DELETE
`/db/{database}/query/v2/tx/{id}`. Method/path validation, database resolution,
authentication and request body behavior SHALL match 5.26.30.

#### Scenario: Single-statement implicit request

- **WHEN** a client posts a valid statement and parameters to query/v2
- **THEN** it executes as one implicit transaction and returns the Query API envelope, not the old statements-array API response (`QAPI-01`)

#### Scenario: Unsupported method or malformed request

- **WHEN** a client uses a wrong method/path, malformed body, unsupported media type or invalid field value
- **THEN** the reference status, headers, error code/message and absence of side effects are reproduced (`QAPI-01`)

### Requirement: Pinned request and response schema

Request fields, defaults, optionality and unknown-field treatment SHALL match
the pinned server, including statement, parameters, includeCounters, accessMode,
bookmarks and impersonatedUser. Successful data SHALL use data.fields and
data.values. Optional counters, notifications, plans/profiles, bookmarks,
transaction and errors SHALL have exact 5.26 shapes, types and omission rules.
Later-version request and response additions SHALL NOT be mixed in.

#### Scenario: Query returns rows and counters

- **WHEN** a client requests counters for a mutating query returning data
- **THEN** the reference counters and field-ordered value rows are returned only in the appropriate response sections and bookmarks represent committed state (`QAPI-01`, `QAPI-05`)

#### Scenario: Notification or execution plan is returned

- **WHEN** a reference-supported query emits a notification or EXPLAIN/PROFILE output
- **THEN** its metadata is encoded in the pinned Query API shape rather than old HTTP or raw executor metadata (`QAPI-01`)

#### Scenario: Version-specific fields are absent

- **WHEN** a request succeeds on the 5.26 compatibility surface
- **THEN** newer queryType/result timing fields are not added merely because current documentation shows them, and newer request-field behavior follows the actual 5.26 unknown-field contract (`QAPI-01`)

### Requirement: Plain and typed JSON value fidelity

The server SHALL independently negotiate request Content-Type and response
Accept for application/json and application/vnd.neo4j.query.v1.2. Plain and
typed codecs SHALL preserve the pinned semantics for nulls, booleans, integers,
floats, strings, bytes, lists, maps, temporal/spatial values and graph entities/
paths. Typed values SHALL use the appropriate `$type` and `_value` structures.
Invalid typed parameters SHALL fail explicitly with the reference error.

#### Scenario: Typed input with plain output

- **WHEN** typed parameters include an integer beyond JavaScript's safe integer range and a datetime while Accept requests plain JSON
- **THEN** execution receives the exact native values and output follows plain JSON semantics without an intermediate float64 conversion (`QAPI-02`)

#### Scenario: Nested CDC result in typed JSON

- **WHEN** a client requests db.cdc.query output with the typed media type
- **THEN** nested maps/lists, txId/seq, timestamps, properties and null states retain their native types and exact typed wrappers (`QAPI-02`, `QAPI-05`)

#### Scenario: Graph and unusual scalar values

- **WHEN** a query returns nodes, relationships, paths, byte arrays, points and special float values
- **THEN** plain and typed responses match the respective reference encodings rather than stringifying unsupported values (`QAPI-02`)

### Requirement: Explicit transaction lifecycle and isolation

Explicit transactions SHALL use a database-bound, authenticated-owner-bound,
execution-context-bound session. Open/continue/keepalive/commit/rollback, idle
expiry, optional final statements, concurrency and terminal errors SHALL match
the pinned server. Response transaction metadata SHALL appear only when the
reference retains an open transaction. Old HTTP and Query API transaction
identifiers SHALL not bypass each other's ownership or protocol checks.

#### Scenario: Keepalive and final commit

- **WHEN** a client opens a transaction, keeps it alive with an allowed empty request and commits with a final statement
- **THEN** expiry updates as specified, both statements persist atomically, and the commit response omits open-transaction metadata as in the reference (`QAPI-03`)

#### Scenario: Error after an earlier write

- **WHEN** a statement fails inside an explicit transaction after an earlier successful mutation
- **THEN** follow-up behavior and response errors match the reference, and no terminal commit can persist invalid partial work (`QAPI-03`)

#### Scenario: Timeout or overlapping requests

- **WHEN** a transaction expires or concurrent continue/commit/rollback requests address the same session
- **THEN** operations are serialized according to the lifecycle contract, cleanup occurs exactly once, and no orphan storage transaction or partial write remains (`QAPI-03`)

### Requirement: Authentication and effective authorization

Every request SHALL use supported Basic/Bearer or configured no-auth behavior
and return Query API error envelopes rather than generic native API errors.
Impersonation SHALL use the common target-only resolver and be pinned across
the explicit session. A route-level permission check SHALL not require the
service account's graph READ before its permitted target can be resolved.

#### Scenario: Caller may impersonate but may not read the target graph

- **WHEN** a caller with appropriate IMPERSONATE requests a query as a target who can read that graph
- **THEN** target authorization determines the result, rather than an earlier caller-read rejection (`QAPI-04`)

#### Scenario: Missing or incorrect authentication

- **WHEN** authentication is enabled and credentials are absent or invalid
- **THEN** the oracle's HTTP status, headers, errors and existing-transaction consequences are reproduced without executing the submitted statement (`QAPI-03`)

### Requirement: Causal and durable query completion

Query API SHALL use the shared durable bookmark capability and canonical
transaction commit/cancel boundaries. A successful committed write SHALL be
visible in a fresh session and after reopen, and SHALL produce the appropriate
CDC events when enabled. Streaming delivery failure SHALL not misreport the
outcome of an already durable commit.

#### Scenario: Reuse a commit bookmark

- **WHEN** a committed Query API write's bookmark is supplied to a subsequent operation over another supported transport
- **THEN** the operation observes at least that commit and tokens cannot bypass database scope or authorization (`QAPI-05`)

#### Scenario: Disconnect before or after commit

- **WHEN** the client disconnects before commit or after the commit is durable
- **THEN** the first case rolls back uncommitted work and the second remains committed, with storage state and CDC agreeing in both cases (`QAPI-03`, `CDC-09`)
