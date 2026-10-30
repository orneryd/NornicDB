## Purpose

Define consistent typed values, scope, expressions and aggregation across
every Cypher position where the same language construct is valid.

## ADDED Requirements

### Requirement: Uniform expression meaning

The same expression and bindings SHALL yield the same typed value in RETURN,
WITH, predicates, assignment, property maps, sorting and aggregate arguments
where valid. Undefined variables or unsupported expressions SHALL produce
typed errors rather than expression source text or an implicit null result.
Bound null and missing properties SHALL follow the pinned language semantics.

#### Scenario: Scoped relationship arithmetic

- **WHEN** a relationship with numeric n is updated using SET r.n = r.n + 1
- **THEN** its new property is the numeric sum in both transaction modes and remains numeric on fresh-session readback

#### Scenario: Undefined expression

- **WHEN** an expression refers to an undefined variable
- **THEN** execution returns a semantic error and cannot persist the variable name as a string

#### Scenario: Optional null predicate

- **WHEN** OPTIONAL MATCH leaves t bound to null
- **THEN** t IS NULL evaluates to true, and a WHERE predicate yielding null does not retain the row

### Requirement: Recursive typed access

Property access, list subscripts/slices, map projections and supported temporal
components SHALL operate recursively on values, including function results and
values carried through WITH. Storage round trips SHALL preserve the types needed
for those operations.

#### Scenario: Access through function and map values

- **WHEN** a query reads startNode(r).id or a nested map m.b.c
- **THEN** it returns the resolved property value rather than the expression's source text

#### Scenario: List index inside MATCH

- **WHEN** UNWIND binds a list p and MATCH uses an inline property value p[0]
- **THEN** matching uses the evaluated list element for each bound row

### Requirement: Correct projection grouping and ordering

Projection SHALL evaluate against its input scope before publishing output
aliases. Aggregates SHALL operate over the correct groups before scalar
expressions enclosing them are evaluated. ORDER BY SHALL apply all legal sort
keys and directions before SKIP/LIMIT, with the pinned null-order semantics.

#### Scenario: Alias shadows its input variable

- **WHEN** a company node c passes through WITH and RETURN evaluates c.name AS c
- **THEN** the output c contains its name rather than the original node

#### Scenario: Aggregate nested in scalar expression

- **WHEN** RETURN evaluates round(avg(i.price) * 100) / 100
- **THEN** avg consumes all rows in the group before the surrounding arithmetic and round operations execute

#### Scenario: Hidden sort key and secondary key

- **WHEN** RETURN projects a name and ORDER BY uses an allowed nonprojected age plus a secondary name key
- **THEN** both keys control the result and LIMIT selects from that ordered result

### Requirement: Atomic evaluated assignment

Assignments SHALL evaluate values in their legal binding scope, including
nested SET += map entries and MERGE branch assignments. Setting a property to
null SHALL remove that property. Evaluation failure SHALL abort pending changes.

#### Scenario: Map update references a bound value

- **WHEN** SET n += {name: r.name} updates a node
- **THEN** the stored property is the value of r.name, and setting it to null removes it from keys(n)
