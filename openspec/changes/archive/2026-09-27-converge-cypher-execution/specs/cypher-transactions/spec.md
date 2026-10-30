## Purpose

Preserve atomic mutations, snapshot isolation and durable graph visibility
across transaction modes and production storage wrappers.

## ADDED Requirements

### Requirement: Atomic failure and explicit lifecycle

Failed autocommit statements SHALL leave no persistent partial mutations.
Terminal execution errors in an explicit transaction SHALL prevent COMMIT from
persisting that transaction's partial writes. Explicit transaction lifecycle
SHALL start only through a client transaction request; child queries SHALL use
the active parent context. COMMIT and ROLLBACK SHALL not implicitly begin a new
transaction.

#### Scenario: Error after an earlier explicit write

- **WHEN** a transaction creates a node and then encounters an evaluation error
- **THEN** a subsequent COMMIT cannot persist that node and protocol recovery leaves the connection usable

#### Scenario: Autocommit mutation fails mid-statement

- **WHEN** a multi-row write encounters a constraint or evaluation error
- **THEN** a fresh session observes none of the failed statement's partial writes

### Requirement: Stable snapshot with own writes

An explicit transaction SHALL read from a consistent snapshot plus its own
pending mutations. Later external commits SHALL not leak into that snapshot.
Caches and streaming optimizations SHALL preserve this visibility contract.

#### Scenario: Concurrent update after snapshot creation

- **WHEN** another transaction commits an update after a reader's snapshot starts
- **THEN** the reader retains the earlier visible value and still sees its own pending writes

#### Scenario: Cached read after own mutation

- **WHEN** a transaction reads a pattern, creates a matching node and repeats the read
- **THEN** the repeated read includes its pending node and does not reuse an incompatible cached result

### Requirement: Consistent committed graph visibility

Successfully committed node and relationship writes SHALL become observable
through every supported live and transactional read interface and remain so
after reopen. Background embedding updates SHALL not temporarily remove
committed graph membership. Namespaces and mutation constraints SHALL remain
enforced through individual and batch interfaces.

#### Scenario: Autocommit creates nodes and their relationship

- **WHEN** one statement creates two nodes and an edge between them
- **THEN** a subsequent explicit transaction can bind the edge, and the edge remains visible after database reopen

#### Scenario: Embedding flush overlaps a label scan

- **WHEN** embeddings are updated for existing nodes without creating or deleting nodes
- **THEN** live label scans and counts retain the committed membership throughout the update

### Requirement: Preserve supported database DDL behavior

Supported ALTER DATABASE and ALTER COMPOSITE DATABASE statements SHALL remain
available in the supported explicit-transaction context with tested completion
and failure behavior. This change SHALL preserve decay and promotion policy
behavior.

#### Scenario: Existing database ALTER inside a transaction

- **WHEN** a supported database ALTER is executed between BEGIN and COMMIT
- **THEN** it is not refused solely because the transaction is explicit, and its committed or rolled-back effects match the tested statement contract
