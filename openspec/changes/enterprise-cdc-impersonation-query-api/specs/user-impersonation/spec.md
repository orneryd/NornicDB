## Purpose

Provide the Neo4j Enterprise 5.26.30 impersonation contract using one canonical
NornicDB security context. These are versioned Neo4j additions, not openCypher
core requirements. Fixture IDs refer to the [reference matrix](../../reference-contract.md).

## ADDED Requirements

### Requirement: Canonical impersonation privileges

The system SHALL support Neo4j 5.26 GRANT, DENY, and REVOKE [GRANT | DENY]
IMPERSONATE syntax, including named users, wildcard/omitted targets, multiple
roles, allowed parameters/quoting and immutable forms. It SHALL persist these
in the same privilege model as #935, apply DENY precedence, and return the
pinned server's SHOW PRIVILEGES and AS COMMANDS columns, rows, commands,
errors and update counters.

#### Scenario: A named deny overrides a wildcard grant

- **WHEN** a caller has IMPERSONATE (*) through one role and DENY IMPERSONATE (alice) through another
- **THEN** impersonating Alice fails before execution while an otherwise valid, permitted Bob request succeeds, and SHOW preserves both records (`IMP-01`, `IMP-02`)

#### Scenario: Revoke one effect without removing the other

- **WHEN** an administrator executes REVOKE GRANT on a target with both granted and denied records
- **THEN** only the granted record is removed, and restart preserves the remaining denied record and its SHOW representation (`IMP-01`)

### Requirement: Complete target security context

An authorized impersonated operation SHALL run with the target's complete
security context, including home database, ACCESS, graph privileges, procedure
permissions and policy revision. It SHALL NOT inherit a union of authenticated
and target permissions. Updating administration commands SHALL remain forbidden
under impersonation even when the target has administrative privileges.
Invalid, disallowed, nonexistent or unavailable target cases SHALL match the
pinned server's errors and precedence.

#### Scenario: A service account impersonates a restricted reader

- **WHEN** a service account with write/admin rights impersonates a target whose #935 policy hides a node and denies writes
- **THEN** patterns, paths, aggregates, procedures and caches expose only the target's graph and a denied write leaves no committed change (`IMP-04`, `IMP-06`)

#### Scenario: The target is an administrator

- **WHEN** a permitted caller impersonates an administrator and requests an updating administration command
- **THEN** the command fails with the reference error and makes no administrative side effect, while permitted SHOW commands remain usable (`IMP-04`)

#### Scenario: Target changes after a previous operation

- **WHEN** target roles or caller impersonation privileges change before the next impersonation request
- **THEN** the next request observes the oracle-defined authorization state rather than a stale cached grant, and active-transaction behavior follows the captured revocation contract (`IMP-05`)

### Requirement: Versioned Bolt impersonation

Bolt 4.4+ BEGIN, autocommit RUN and ROUTE SHALL support `imp_user`, with omitted
or null meaning no impersonation. Resolution SHALL precede effective database
selection and authorization. Explicit transaction identity SHALL remain pinned;
unexpected RUN extras inside it SHALL follow the oracle's validation behavior
without changing that identity. Pre-4.4 requests SHALL follow their own
versioned message contract, not reinterpret fields using 4.4 layouts.

#### Scenario: Route to a target home database

- **WHEN** an authorized ROUTE request supplies imp_user and omits a database
- **THEN** the response names/routes the target's home database as in 5.26, enforcing target access without changing connection authentication (`IMP-03`)

#### Scenario: Explicit transaction retains its target

- **WHEN** BEGIN selects Alice and later RUN metadata attempts to select Bob
- **THEN** no statement runs as Bob and the reference error or ignored-extra behavior is reproduced, with the transaction state remaining oracle-compatible (`IMP-03`)

#### Scenario: Invalid impersonation in a pipelined request

- **WHEN** a target fails validation or authorization before a pipelined write
- **THEN** no write executes and FAILURE, subsequent IGNORED messages and RESET recovery match the negotiated Bolt state machine (`IMP-02`, `IMP-03`)

### Requirement: Query API impersonation and ownership

Query API v2 SHALL accept `impersonatedUser` where supported by 5.26 and use
the same resolver as Bolt. A transaction SHALL retain both its authenticated
owner and executing target. Continuation, keepalive, commit and rollback SHALL
apply oracle-confirmed authentication/target-field rules without allowing
ownership transfer or target rebinding.

#### Scenario: Impersonated HTTP transaction continues

- **WHEN** a service account opens a Query API transaction for Alice and continues it using the reference-supported continuation form
- **THEN** it still executes with Alice's rights and records service-account/Alice attribution through commit (`QAPI-04`)

#### Scenario: Another caller guesses the transaction identifier

- **WHEN** a different authenticated owner attempts to continue or commit that transaction
- **THEN** no operation executes through the owner's session and status, body and lifecycle follow the pinned server's contract (`QAPI-03`, `QAPI-04`)

### Requirement: No identity leakage across execution surfaces

The system SHALL retain effective identity through result streaming, USE,
aliases, subqueries and local/remote composite execution. Terminal cleanup and
connection reuse SHALL not leak it to the next operation. Authorization-sensitive
caches and continuations SHALL be partitioned or invalidated by effective
security context; authenticated ownership SHALL remain distinct.

#### Scenario: Pooled connection alternates identities

- **WHEN** one connection executes as Alice, then Bob, then with no target after commit, rollback, RESET or timeout
- **THEN** each operation sees only its own authorized graph and no result, permission closure or default database is reused from the previous target (`IMP-05`, `IMP-06`)

#### Scenario: Remote constituent cannot preserve delegation

- **WHEN** an impersonated query would cross a remote boundary incapable of validating the effective identity
- **THEN** it fails before remote effects rather than executing under the service account alone; supporting that deployed route remains an implementation acceptance gate (`IMP-06`)

### Requirement: Replace conflicting authorization behavior

The canonical privilege model SHALL supersede conflicting role booleans,
allowlist/global fallbacks and coarse prechecks on affected paths. Migration
SHALL be explicit, persistent, and fail with an actionable diagnostic on
ambiguous mappings. No legacy evaluator, lossy matrix overwrite, compatibility
toggle or silent execution-as-authenticated-user fallback SHALL remain.

#### Scenario: Old privilege data requires migration

- **WHEN** an existing installation is upgraded
- **THEN** operators receive a deterministic mapping report, accepted records migrate once, unresolved records prevent cutover, and subsequent authorization reads only canonical records (`IMP-01`, `IMP-06`)