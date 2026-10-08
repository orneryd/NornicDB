## ADDED Requirements

### Requirement: Current clauses compose inline

The shared pipeline SHALL support LET, FILTER, FOR, RETURN/WITH ALL, GROUP BY
and allowed read/write transitions with current Neo4j scope, grouping and
null/empty behavior. Every clause SHALL be consumed and validated; an
unrecognized tail SHALL never become successful partial execution.

#### Scenario: Bind and filter without losing scope

- **WHEN** a statement uses LET followed by FILTER and FOR
- **THEN** binding retention, row expansion and filtering match the reference without rewriting the statement to another query (`Q01`, `Q03`)

#### Scenario: Explicit grouping

- **WHEN** RETURN or WITH supplies GROUP BY expressions and aggregate aliases
- **THEN** grouping, output shape and invalid-reference errors match the reference, including empty inputs (`Q03`)

### Requirement: Table-level composed queries

NEXT, WHEN/ELSE and braced UNION compositions SHALL use bound pipeline
structures with reference output-column and scope rules. NEXT SHALL pass the
whole result table. Only selected conditional branches SHALL execute; ordinary
statement failures SHALL roll back all of that statement's uncommitted writes.

#### Scenario: Aggregate after NEXT

- **WHEN** a multirow result passes through NEXT into aggregation inside CALL or UNION
- **THEN** aggregation observes the correct complete input table rather than one independent invocation per row (`Q02`)

#### Scenario: Unselected branch contains a write

- **WHEN** a conditional chooses a different branch
- **THEN** the unselected write has no graph, procedure or external side effect, and later failure rolls back selected uncommitted changes (`Q02`)

### Requirement: Current expressions and function contracts

Numeric-leading parameters, interpolation, map comprehensions and all stable
function additions/overloads in the audit inventory SHALL share correct
parsing, static validation, evaluation and catalog signatures. Null, type,
ordering, scoping and error behavior SHALL match the pinned release.

#### Scenario: Property existence is true for stored properties

- **WHEN** two nodes both have non-null x and a query filters with PROPERTY_EXISTS(n, 'x')
- **THEN** the count is two, matching IS NOT NULL rather than the current silent zero (`E03`, probes P31/P43)

#### Scenario: Registered function is publicly callable

- **WHEN** a supported function such as ceiling is listed or registered
- **THEN** public execution in every expression position accepts its valid signature and rejects invalid arguments consistently (`E03`, probe P25)

#### Scenario: Nested interpolation and map comprehension

- **WHEN** these expressions contain nested maps, strings, parameters and computed keys
- **THEN** lexical boundaries, escaping, duplicate-key handling and value conversion match the reference (`E04`)

### Requirement: Current match and path semantics

DIFFERENT RELATIONSHIPS, REPEATABLE ELEMENTS, quantified paths/group variables,
ACYCLIC and restrictive path selectors SHALL share native traversal with
reference uniqueness, bounds, ties, predicate scope and parameter rules.
Authorization and cancellation SHALL apply during expansion.

#### Scenario: Repeated relationships versus acyclic nodes

- **WHEN** a cyclic fixture is queried using different match/path modes
- **THEN** each mode admits exactly the reference's paths, including cross-pattern uniqueness and finite-bound restrictions (`P01`, `P02`)

#### Scenario: Select shortest groups with a parameter

- **WHEN** SHORTEST or ANY uses a parameter count and tied path lengths
- **THEN** group/cardinality and pre/post-filter semantics match the reference rather than a generic shortestPath substitution (`P01`)

### Requirement: Independent concurrent batch lifecycle

CALL IN TRANSACTIONS SHALL support current concurrency, status, error/retry and
DISJOINT BY modifiers through the existing lifecycle. Each successful child
commit SHALL occur once, retain attribution and remain committed when the
reference preserves it after a later failure. Cancellation SHALL stop active
children without inventing rollback of durable commits.

#### Scenario: Later batch fails after earlier commits

- **WHEN** a concurrent transactional subquery encounters a configured error/retry policy
- **THEN** statuses, retained commits, retries and CDC events match the reference with no duplicate successful child commit (`B01`)

#### Scenario: Overlapping disjoint resources

- **WHEN** batches share resources under DISJOINT BY expressions or AUTO
- **THEN** scheduling honors the inferred/declared conflicts while unrelated work remains bounded and concurrent (`B01`)
