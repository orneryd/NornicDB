## ADDED Requirements

### Requirement: One pipeline with explicit language semantics

CYPHER 5 and CYPHER 25 SHALL execute through the same shared pipeline and
semantic helpers. The selected language SHALL remain available to admission,
evaluation, catalogs and response rendering. Unsupported 25 behavior SHALL
NOT retry through 5 or an independently interpreted query-text path.

#### Scenario: Explicit 25 reaches execution

- **WHEN** `CYPHER 25 RETURN 1 AS v` executes through either parser and any supported transport
- **THEN** it returns integer 1 rather than the current version ArgumentError (`V01`, probe P01)

#### Scenario: Existing syntax remains supported

- **WHEN** an existing supported statement uses a construct upstream removed in 25
- **THEN** NornicDB retains its working implementation, including under explicit 25, and records the upstream difference instead of introducing a breaking rejection (`R01`)

### Requirement: Query prefix overrides configured process default

An explicit CYPHER 5 / CYPHER 25 prefix SHALL override the process default
configured by `NORNICDB_CYPHER_VERSION=5|25`. Unprefixed queries SHALL use the
configured version, including its new syntax; an absent setting SHALL retain
5. No persisted database defaults, language migration or default-language DDL
SHALL be introduced.

#### Scenario: Configured default and query overrides

- **WHEN** each default 5/25 is tested with unprefixed, explicit 5 and explicit 25 queries
- **THEN** unprefixed queries use the configured version and each explicit prefix wins; FOR/LET/FILTER works unprefixed under default 25 (`V01`)

#### Scenario: Existing queries survive upgrade and reopen

- **WHEN** an existing database is upgraded and reopened
- **THEN** existing unprefixed queries retain their configured-language contracts, explicit 5 remains 5 regardless of the process default, and no persisted language migration occurs (`V01`, `R01`)

#### Scenario: Routing preserves the query prefix

- **WHEN** a prefixed query resolves USE or an alias through supported routing
- **THEN** the selected query language is retained without introducing database/alias defaults or changing authorization (`V01`)

### Requirement: Validate language configuration before execution

Invalid or present-empty NORNICDB_CYPHER_VERSION settings SHALL surface
configuration errors before serving requests, not silently fall back.
Configuration SHALL be resolved outside the per-query hot path and passed
consistently to all executor construction paths.

#### Scenario: Invalid configured version

- **WHEN** NORNICDB_CYPHER_VERSION is present with an empty value or anything other than 5 or 25
- **THEN** startup/configuration validation reports the setting and prevents request serving without silently selecting another version (`V01`)

### Requirement: Language-safe preparation and caches

Preparation SHALL select language before version-sensitive normalization and
semantic validation. Cached analysis, validation, plans and results SHALL be
partitioned by resolved query language and relevant schema/security revision.
Unprefixed statements SHALL resolve language before cache lookup.
Bound subqueries and streams SHALL retain the required context.

#### Scenario: Same text in two language contexts

- **WHEN** the same query body runs under both configured defaults and explicit CYPHER 5 / CYPHER 25 overrides
- **THEN** a cache cannot reuse incompatible admission, output types or result values (`V01`, `A01`)

#### Scenario: Explicit transaction changes statement language

- **WHEN** statements inside one explicit transaction request different language prefixes
- **THEN** admission and context propagation match the pinned oracle without losing transaction identity or reusing another statement's language (`V01`)
