## Purpose

Provide reproducible evidence that Cypher behavior conforms to the pinned
language contract and remains correct across supported transaction modes.

## ADDED Requirements

### Requirement: Complete reproducible conformance runs

The conformance suite SHALL enumerate every scenario and example in its pinned
official TCK corpus and execute each against a fresh graph over Bolt in
autocommit and explicit-transaction modes. The report SHALL identify the source
revision, server revision, configuration and each scenario's outcome.

#### Scenario: Execute a mutation scenario in both modes

- **WHEN** a TCK scenario mutates its initial graph
- **THEN** each mode starts from an independent copy of that initial graph
- **AND** the report includes result, completion error and observable side-effect checks for both modes

#### Scenario: Unrecognized test step

- **WHEN** a corpus step has no implemented binding
- **THEN** the run fails as a harness error and cannot count the scenario as passed or silently skipped

### Requirement: Exact regression baseline

CI SHALL fail on newly failing cases, changed failure signatures, missing cases,
unexpectedly passing expected-failure entries, crashes, timeouts and harness
errors. Existing gaps SHALL be recorded individually with a reason and linked
issue or local gap ID. Passing an unsupported feature through an error response
SHALL NOT count as conformance to a scenario requiring a successful result.

#### Scenario: Previously passing case regresses

- **WHEN** a change produces a wrong result in an explicit-transaction case that previously passed
- **THEN** CI fails even if its autocommit counterpart passes and total pass count increases

#### Scenario: Existing gap is fixed

- **WHEN** an expected-failure case passes in all required variants
- **THEN** its stale baseline entry must be removed before CI passes

### Requirement: Independent result and mutation verification

Assertions SHALL preserve meaningful types, row multiplicity, required ordering,
graph identity relationships and observable side effects. Scoped issue
reproductions SHALL remain regression tests even without matching upstream
scenarios. Comparator and fixture errors SHALL be distinguishable from query
implementation errors.

#### Scenario: Wrong numeric type or duplicate count

- **WHEN** actual output substitutes a string for a number or drops duplicate rows
- **THEN** comparison fails even if string formatting or unique row sets appear equal

#### Scenario: Statement return hides a missing write

- **WHEN** a write returns the expected value but a fresh session cannot read the expected committed graph
- **THEN** the regression fails and the issue is not closure-ready
