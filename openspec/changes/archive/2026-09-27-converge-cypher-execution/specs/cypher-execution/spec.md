## Purpose

Make complete Cypher statements behave consistently across every execution
route without losing bindings, clauses or transaction context. Queries that
cannot be handled return proper errors instead of alternate execution.

## ADDED Requirements

### Requirement: Complete statement execution

Every successful execution SHALL account for the entire statement or explicitly
bounded child fragment. A supported clause sequence SHALL execute in language
order and preserve its required rows, columns, scope and mutation effects.
Unrecognized syntax SHALL NOT be discarded after a recognized prefix.

#### Scenario: UNWIND followed by optional matching

- **WHEN** UNWIND binds keys and OPTIONAL MATCH looks up an absent key
- **THEN** the optional variables are bound to null and all subsequent WITH, WHERE, aggregation and RETURN clauses execute

#### Scenario: Trailing malformed syntax after a write prefix

- **WHEN** a statement has a recognized CREATE prefix and an invalid trailing clause
- **THEN** it fails without committing partial mutations or reporting prefix-only success

### Requirement: Unhandled valid syntax converges to the shared pipeline

A query with valid syntax that no route can handle SHALL be rejected with a
proper unsupported or syntax error by the converged execution pipeline. It
SHALL NOT succeed silently, dispatch to an alternate text executor, or replay.
Whenever such a query is found (issue, report or TCK gap), a regression test
SHALL be added recording the expected error and the absence of observable
effects.

#### Scenario: Valid but unhandled statement

- **WHEN** a syntactically valid statement cannot be handled by any route
- **THEN** execution returns a proper unsupported error, no rows are emitted and no mutations are committed

#### Scenario: Regression coverage for a found unhandled shape

- **WHEN** a new valid-but-unhandled query shape is discovered
- **THEN** a regression test records the expected error class and effect-free execution for every transaction mode

### Requirement: Terminal errors for uncovered shapes

An uncovered shape or parsing rejection SHALL return a proper unsupported or
syntax error of the same class Neo4j reports for that statement. There SHALL be
no alternate execution, parser replay or retry. Runtime, authorization,
constraint, cancellation and storage failures SHALL terminate execution without
parser replay.

#### Scenario: Unhandled query shape

- **WHEN** a query cannot be handled and has produced no observable effects
- **THEN** execution returns a proper unsupported or syntax error and the statement is not executed by another route

#### Scenario: Failure after execution begins

- **WHEN** a runtime error occurs after writes or emitted rows
- **THEN** execution terminates, pending writes roll back, and another parser does not replay the statement

### Requirement: Bound internal composition

Child query execution SHALL preserve explicitly imported bindings, typed
parameters, cancellation, authorization, database selection and parent
transaction lifecycle. Values SHALL NOT become executable query source through
substitution. Child execution SHALL NOT independently commit the parent.

#### Scenario: Correlated subquery retains outer row

- **WHEN** MATCH binds a company and CALL imports it to compute an employee count
- **THEN** the outer company remains available with the correct per-company count after CALL

#### Scenario: Parameter contains Cypher punctuation

- **WHEN** a parameter contains quotes, keywords, braces or comment markers
- **THEN** it remains a value and cannot alter the clause structure or selected database
