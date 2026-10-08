## ADDED Requirements

### Requirement: Native VECTOR and UUID fidelity

VECTOR and UUID SHALL be native typed values with current constructors,
functions, comparison, type predicates and property constraints. Their type,
precision and identity SHALL survive storage, cache/index keys, MVCC, CDC,
backup/reopen and supported protocol round trips without implicit string/list
substitution.

#### Scenario: Typed properties survive restart

- **WHEN** VECTOR and UUID properties are written and the database restarts
- **THEN** valueType, equality, ordering and returned values retain the original types and exact reference-compatible values (`T01`, `T02`)

#### Scenario: Vector coordinate and dimension boundaries

- **WHEN** constructors receive boundary dimensions, numeric widths, invalid coordinates or nulls
- **THEN** conversion/range rules and errors match the reference before any invalid property is committed (`T01`)

### Requirement: Native indexed SEARCH

MATCH and OPTIONAL MATCH SHALL support VECTOR and FULLTEXT SEARCH using native
retrieval services, with correct index kind/targets, filter metadata, score
binding, analyzer, pagination and argument validation. Accepted index options
SHALL have real behavior. Result visibility SHALL use the effective graph
security and transaction context.

#### Scenario: Optional search has no matching candidates

- **WHEN** OPTIONAL MATCH SEARCH finds no authorized result
- **THEN** row preservation and null score/entity bindings match the reference, not an empty mandatory-MATCH result (`S01`)

#### Scenario: Index filtering and score projection

- **WHEN** SEARCH queries a declared multi-target index with filterable properties and SCORE AS
- **THEN** allowed predicates, score scope, LIMIT/SKIP/OFFSET and wrong-index errors match the declared contract (`S01`)

#### Scenario: Approximate retrieval is measured

- **WHEN** native index algorithms differ from Neo4j internals
- **THEN** deterministic score/type/error fixtures and named recall/ranking measurements accompany any compatibility claim; differences are not hidden by dropping scores or rows (`S01`)

### Requirement: Persistent open graph types

SET/ADD/ALTER/DROP/SHOW CURRENT GRAPH TYPE and AS GRAPH SHALL operate on
canonical persisted schema rules. Implied labels, relationship endpoint
requirements and graph-type provenance/classification SHALL be enforced across
all mutation routes while leaving unrelated entities unconstrained.

#### Scenario: Open schema permits unrelated data

- **WHEN** a graph type constrains Person while a transaction creates an unrelated label
- **THEN** that creation remains legal unless another applicable rule forbids it, and constrained Person writes obey the graph type (`G01`)

#### Scenario: Schema change conflicts with stored data

- **WHEN** an element-type change would invalidate existing entities
- **THEN** validation and failure/rollback match the reference, with no partial published schema and consistent state after reopen (`G01`)

#### Scenario: Introspection preserves graph-type identity

- **WHEN** a graph type is listed as text, graph or SHOW CONSTRAINTS rows
- **THEN** implied labels, endpoint labels, classification and recreatable statements describe the same persisted rules (`G01`, `A01`)
