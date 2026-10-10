# Spec delta: write-path routing

Candidate requirements the chosen path must satisfy. These are recorded now so
the decision (G1 or G2) is checked against observable behavior, not style.

## ADDED Requirements

### Requirement: Single transactional route for writes

Every Cypher write statement SHALL execute through exactly one route with
statement-level atomicity, regardless of query shape, constraint coverage, or
durability mode.

#### Scenario: constrained and unconstrained labels take the same path

- **WHEN** a `CREATE` statement targets a label with a uniqueness constraint
- **THEN** it executes the same admission and commit code as an identical
  statement on an unconstrained label, and a constraint violation rolls back
  the statement.

#### Scenario: no shape-based routing remains

- **WHEN** a write statement matches a previously "async-eligible" shape
- **THEN** no keyword/schema scan selects a different executor or drops
  statement atomicity.

#### Scenario: mixed statements stay atomic

- **WHEN** one statement contains several write clauses and any clause fails
- **THEN** no row from that statement is visible after the error.

### Requirement: No buffered-visibility write layer without an atomic contract

A write may be buffered or batched before durability only if every subsequent
read, constraint check, and receipt observes it as an atomic, committed unit.

#### Scenario: no write is visible before commit

- **WHEN** a cached/batched write is staged
- **THEN** no snapshot opened before its commit can observe any of its rows.

#### Scenario: eventual-mode acknowledgment is explicit

- **WHEN** an eventual-durability mode exists
- **THEN** it is declared in API documentation with its loss window, and never
  applies to constrained or explicit-transaction writes.

### Requirement: Measured write-path changes

Every hot-path change to the write route SHALL be accompanied by a
workload-scoped before/after measurement (latency, throughput, allocations)
and a CPU profile, per the performance protocol.

#### Scenario: tuning is reversible and evidence-backed

- **WHEN** a Badger or WAL option is changed
- **THEN** the change is a single revertible experiment with recorded
  before/after numbers and no regression on the ingest matrix.
