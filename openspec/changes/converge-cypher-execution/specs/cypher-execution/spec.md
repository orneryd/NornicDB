## Purpose

Make complete Cypher statements behave consistently across optimized and
fallback execution without losing bindings, clauses or transaction context.

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

### Requirement: Safe fallback and terminal failures

An uncovered shape or parsing rejection SHALL be eligible for fallback only
before observable effects. Runtime, authorization, constraint, cancellation and
storage failures SHALL terminate execution without parser replay. A supported
fallback query SHALL satisfy the same observable behavior as an optimized route.

#### Scenario: Effect-free shape miss

- **WHEN** the fast route cannot handle a valid query and has produced no observable effects
- **THEN** the fallback executes it once with the same bindings and transaction context

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
