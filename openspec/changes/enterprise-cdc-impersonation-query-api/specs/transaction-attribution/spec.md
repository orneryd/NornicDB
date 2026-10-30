## Purpose

Preserve trustworthy request-to-commit identity and metadata across NornicDB's
transports, transaction modes and storage wrappers. Neo4j-facing representations
are pinned to 5.26.30; internal/system writers are explicitly Nornic extensions.

## ADDED Requirements

### Requirement: Trusted dual identity

Every external transaction SHALL retain authenticated and executing principals
separately. Trusted user, connection, database and server attribution SHALL be
derived from authenticated server context, not user transaction metadata.
Ordinary transactions SHALL have equal authenticated/executing user names.
Internal writers SHALL carry an explicit system-origin identity rather than
silently borrowing an administrator or a previous session's identity.

#### Scenario: Transaction metadata tries to spoof attribution

- **WHEN** a client supplies tx metadata keys named authenticatedUser, executingUser or txCommitTime
- **THEN** those keys remain ordinary nested txMetadata values and do not alter trusted fields (`ATTR-01`, `CDC-03`)

#### Scenario: Impersonated transaction commits

- **WHEN** a service account commits while executing as Alice
- **THEN** all events from that commit retain the service account as authenticatedUser and Alice as executingUser, with one trusted connection/database/server context (`ATTR-02`, `IMP-04`)

### Requirement: One metadata lifecycle

Bolt BEGIN and autocommit `tx_metadata`, `tx.setMetaData`, and supported HTTP
metadata sources SHALL reach the actual transaction. The system SHALL implement
the pinned server's metadata type/size validation and setter semantics, deep
copy mutable inputs, and prevent leakage between transactions. All events from
one commit SHALL contain the same final transaction metadata.

#### Scenario: BEGIN metadata reaches commit without a setter call

- **WHEN** BEGIN supplies a nested tx_metadata map and subsequent statements mutate the graph
- **THEN** active introspection and committed events contain that map even though tx.setMetaData was not called (`ATTR-01`)

#### Scenario: Metadata is changed within an explicit transaction

- **WHEN** the client calls tx.setMetaData after an earlier write and before a later write
- **THEN** the setter's replacement/merge behavior matches 5.26 and every committed event uses the final metadata snapshot (`ATTR-01`)

#### Scenario: Independent batched subtransactions

- **WHEN** CALL IN TRANSACTIONS commits multiple child batches
- **THEN** each successful child has its own transaction identity/start/commit times and correctly inherited attribution, while rolled-back children emit nothing (`ATTR-02`, `CDC-09`)

### Requirement: Consistent introspection and logging

SHOW CURRENT USER, SHOW TRANSACTIONS and query-log attribution SHALL reflect
the pinned server's authenticated/executing identity semantics, projection
columns and value formatting. Both identities SHALL remain available internally
even when an API exposes a single combined field. Authorization of these views
SHALL use the canonical privilege model.

#### Scenario: Observer inspects an idle impersonated transaction

- **WHEN** an authorized observer lists an open impersonated transaction between statements
- **THEN** its username representation, metadata and connection fields match the reference, without speculative extra user columns (`IMP-04`, `ATTR-01`)

#### Scenario: Query logging under impersonation

- **WHEN** query logging is enabled for an impersonated operation
- **THEN** the recorded attribution identifies both principals according to the reference semantics and existing secret redaction remains enforced (`IMP-04`)

### Requirement: Durable published commit identity

The system SHALL distinguish logical transaction identity from MVCC reservations,
physical batches, WAL offsets and transport-generated identifiers. Acknowledged
commits SHALL have a durable per-database position consistent with graph and
event visibility. Failed or abandoned reservations SHALL not yield events or
prevent later published commits from being read.

#### Scenario: Concurrent reservations complete out of order

- **WHEN** two same-database writes reserve work and one is delayed or fails
- **THEN** readers never observe CDC order that later acquires an earlier committed event, and current/head never advances past unpublished logical work (`CDC-05`, `CDC-09`)

#### Scenario: A logical commit crosses many Badger batches

- **WHEN** a transaction exceeds one physical batch
- **THEN** all graph mutations, events and the published head become visible atomically as one logical transaction and remain consistent after reopen (`CDC-09`)

#### Scenario: Capture is off but a commit returns a bookmark

- **WHEN** a graph write commits with enrichment OFF
- **THEN** its durable position survives reopen without constructing CDC event bodies, and the position-only implementation has recorded before/after latency, throughput, allocations, memory, head-write and fsync measurements before CDC is layered on (`CDC-09`, `QAPI-05`)

### Requirement: Shared causal bookmark semantics

Bolt, old HTTP transactions and Query API SHALL produce and consume bookmarks
backed by published durable database state, with oracle-compatible scope,
validation, waiting and errors. CDC cursors and bookmarks SHALL remain distinct.
Synthetic wall-clock tokens, process-counter fallback and legacy placeholder
acceptance SHALL be removed from affected surfaces.

#### Scenario: Commit on one transport and read on another

- **WHEN** a Query API commit returns a bookmark used by a later Bolt or HTTP operation
- **THEN** the later operation cannot execute before the represented state is published, including after restart (`QAPI-05`)

#### Scenario: Referenced commit is not yet visible

- **WHEN** a valid bookmark refers to state the server has not reached
- **THEN** the operation waits subject to cancellation/deadline and returns the reference timeout/error if unmet, rather than a fabricated success or immediate process-counter comparison failure (`QAPI-05`)
