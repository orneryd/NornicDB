## ADDED Requirements

### Requirement: One shared grammar with optional headers

Cypher 5 and Cypher 25 surface SHALL execute through the same shared pipeline,
clause kinds and operators. The `CYPHER 5` / `CYPHER 25` header SHALL be
optional, accepted and discarded by both parsers, and callers SHALL pass
statements as written without stripping the preamble. No language default,
configuration setting, persisted database field or default-language DDL SHALL
be introduced. Unsupported 25 behavior SHALL NOT retry through 5 or an
independently interpreted query-text path.

#### Scenario: Explicit header is accepted

- **WHEN** `CYPHER 25 RETURN 1 AS v` executes through either parser and any supported transport
- **THEN** it returns integer 1 rather than the current version ArgumentError (`V01`, probe P01)

#### Scenario: Headerless additive syntax

- **WHEN** `FOR x IN [1,2,3] LET scaled = x * 10 FILTER scaled > 10 RETURN x, scaled` executes through the SRD parser
- **THEN** it returns (2, 20) and (3, 30) without a header; the same statement with `CYPHER 25` also succeeds on both parsers (`Q01`, probe P03)

#### Scenario: Existing syntax remains supported

- **WHEN** an existing supported statement uses a construct upstream removed in 25
- **THEN** NornicDB retains its working implementation, including under explicit 25, and records the upstream difference instead of introducing a breaking rejection (`R01`)

### Requirement: Parser agreement within one grammar

Both parsers SHALL accept the optional preamble and feed the same pipeline
operators. The SRD parser MAY accept additive clauses and correlated unscoped
CALL bodies without a header; the ANTLR parser SHALL keep the strict Cypher
5.26 admission for those forms. Divergent admission between the two front ends
SHALL be explicit, tested and documented, never a silent execution difference.

#### Scenario: ANTLR requires the header for additive clauses

- **WHEN** `FOR x IN [1] RETURN x` is validated by the ANTLR parser without a header
- **THEN** it fails with the documented strict error, while `CYPHER 25 FOR x IN [1] RETURN x` succeeds (`V01`)

#### Scenario: Implicit CALL import remains a tested difference

- **WHEN** a correlated unscoped CALL body reads an outer variable without importing it
- **THEN** the SRD parser executes it and the ANTLR parser rejects it with the Cypher 5.26 contract; both behaviors are pinned by tests (`R01`)
