## ADDED Requirements

### Requirement: Versioned public metadata and diagnostics

Current opt-in SHOW/procedure/function outputs, deprecations and errors SHALL
match the selected language and pinned server/API contract except documented
retained extensions. Unprefixed queries SHALL receive their resolved language's
metadata. Server-version Query API/Bolt additions SHALL not be selected solely
by Cypher language. Native plan internals SHALL remain truthful, not simulated
Neo4j operators.

#### Scenario: SHOW uses current types and composition

- **WHEN** 25 SHOW TRANSACTIONS or composable SHOW executes
- **THEN** timestamp/null/current-query fields and clause composition match current Neo4j rather than fixed 5.26 strings and empty values (`A01`)

### Requirement: Preserve supported APIs across language selection

Existing supported queries/APIs SHALL remain available; queries resolving to
5 SHALL retain their 5 response contracts. Upstream removals SHALL be recorded
as retained extensions where implemented, not imposed as breaking deletions.

#### Scenario: Deprecated is not removed

- **WHEN** a caller uses a current deprecated-but-supported procedure or a procedure removed in 25
- **THEN** the former remains callable with correct metadata and any existing implementation of the latter remains available as a documented NornicDB extension (`R01`, `I01`)

### Requirement: Current Enterprise surface shares canonical implementation

Current authorization rules, administration, CDC and Query API changes SHALL
extend #935 and the existing Enterprise plan's shared implementations. Trusted
attributes, credential-export privileges, dual identity and graph visibility
SHALL not be bypassed by new language or introspection features.

#### Scenario: Auth rules change effective roles

- **WHEN** validated OIDC attributes or native user tags satisfy a configured auth rule
- **THEN** canonical role resolution and invalidation follow the reference, including during impersonation, without treating arbitrary query parameters as trusted claims (`A02`)

#### Scenario: Current CDC and Query API fields

- **WHEN** the current server contract requires newer CDC output or Query API transaction/notification metadata
- **THEN** the shared adapters expose the correct current shape while historical oracle fixtures remain separately identified (`I01`)

### Requirement: Release-scoped independent conformance evidence

The project SHALL maintain a pinned 2026.09.0 reference inventory and differential
tests for each stable public item in the audit matrix, with separate language,
edition, protocol and transaction dimensions. Missing/untested behavior SHALL
not count as passed. Existing 5.26 and openCypher evidence SHALL remain separate.

#### Scenario: Oracle cannot run

- **WHEN** the current licensed Enterprise reference is unavailable
- **THEN** affected acceptance remains explicitly blocked rather than passing against Community, embedded parser agreement or documentation alone

#### Scenario: A probe returns a plausible result

- **WHEN** a query returns without error
- **THEN** acceptance still checks columns, native types, nulls, ordering, diagnostic/error shape and persistent side effects rather than mere admission

### Requirement: Track stable and future development separately

An upstream-release inventory SHALL record sources, release/edition, stable or
preview state, affected modules, fixture IDs and verified status. New stable
releases SHALL trigger an explicit reviewed target update. Preview/announced
features and vendor-internal/plugin features SHALL not silently enter the
stable conformance claim.

#### Scenario: Preview feature becomes stable

- **WHEN** a feature such as graph types moves from preview to GA
- **THEN** the inventory updates its classification and schedules missing stable acceptance work rather than leaving it excluded indefinitely

#### Scenario: Publish compatibility status

- **WHEN** a release report is generated
- **THEN** it identifies the exact target, retained extensions and supported/partial/missing/untested cases without claiming full Cypher 25 or ISO GQL compliance from a selected probe set
