## Purpose

Preserve measured Cypher performance while exposing rejected unhandled
workloads as opportunities for additional verified streaming optimizations.

## ADDED Requirements

### Requirement: Streaming eligible reads

Eligible snapshot-visible reads with a limiting result count SHALL stop after
satisfying that count without unconditional materialization of the full label.
Required sorting, grouping and mutation effects SHALL still complete before a
limit can eliminate work. Projection SHALL preserve all properties needed by
filters, ordering and visibility checks.

#### Scenario: Simple projected LIMIT read in an explicit transaction

- **WHEN** a label read returns ten names without blocking clauses
- **THEN** candidate visit evidence demonstrates early termination while results match the snapshot-visible reference

#### Scenario: Ordering prevents early scan termination

- **WHEN** a query must sort candidates before applying LIMIT
- **THEN** execution returns the correct sorted limit and does not truncate candidates before ordering is satisfied

### Requirement: Measured optimization acceptance

Hot-path changes SHALL include repeated before/after latency, throughput and
allocation measurements on a named equivalent workload with correctness checks.
Regressions above 5% latency or 10% memory SHALL require an explicit documented
justification. Existing wrong or no-op behavior SHALL not count as a valid
performance baseline for a corrected query.

#### Scenario: Remove a duplicate executor

- **WHEN** a redundant handler is replaced by shared execution
- **THEN** the change includes conformance evidence and workload-specific performance comparison before removal is accepted

### Requirement: Informational rejection reporting

Queries that end in a proper unsupported or syntax error SHALL produce a
bounded, redacted report of query shapes and reasons suitable for an
optimization backlog. Returning the proper error alone SHALL not fail
correctness CI. Reports SHALL exclude raw parameter values and sensitive query
literals and distinguish rejection from execution failures.

#### Scenario: Valid query is not yet supported

- **WHEN** a query cannot be handled
- **THEN** execution returns a proper unsupported or syntax error and the report records an optimization opportunity

#### Scenario: Report contains sensitive parameters

- **WHEN** an executed query includes confidential strings in parameters
- **THEN** the report contains only the normalized shape and permitted aggregate measurements
