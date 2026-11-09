## ADDED Requirements

### Requirement: Complete canonical operation classification

All commands, procedures, functions and transport operations SHALL have an
explicit reviewed classification evaluated through the canonical FGAC authority.
An unclassified operation SHALL be denied rather than inferred to be read-only.

#### Scenario: A callable is installed at runtime
- **WHEN** an authenticated principal invokes a newly installed callable without
  a reviewed operation class
- **THEN** execution is denied before its protected effects occur
- **AND** adding a valid class still requires the principal's corresponding grant
  and applies explicit denies through the canonical evaluator.

#### Scenario: Another transport or nested execution reaches the same operation
- **WHEN** the same principal, database and policy reach an operation through
  another supported transport, parser, alias or nested execution path
- **THEN** the same class/effects and privilege decision apply
- **AND** no legacy check or default read classification bypasses that decision.

### Requirement: FGAC visibility applies throughout execution and commit

The #935 effective-graph contract SHALL apply through every supported public
consumer, including path selectors and operations carrying WRITE_RECORD effects.
Transactions SHALL undergo the requested commit-time visibility validation under
the contract pinned from PD's requirement and the retained failing evidence.

#### Scenario: A restricted principal executes a path selector
- **WHEN** a collection/graph-restricted principal executes a supported path
  selector using either supported parser
- **THEN** it observes only the permitted graph and preserves the supported query
  result/error contract without redispatch violating the internal-text boundary.

#### Scenario: A transaction reaches commit
- **WHEN** a restricted transaction reaches the commit boundary
- **THEN** the canonical visibility check required by the pinned commit contract
  runs before protected effects become durable
- **AND** a denied commit preserves the specified atomic rollback outcome,
  verified from a fresh session.

### Requirement: HTTP denial precedes disclosure and initialization

An authenticated principal without access to a database SHALL NOT learn its
existence through the protected HTTP operation or initialize its services before
being denied.

#### Scenario: Existing and nonexistent inaccessible database names
- **WHEN** the principal sends the equivalent protected HTTP request for either
  an existing inaccessible database or a nonexistent inaccessible name
- **THEN** neither response discloses which case exists
- **AND** denied requests do not initialize the database or its services.

### Requirement: Explicit transaction and callable privilege classes

Own-transaction termination SHALL use its supported Neo4j transaction-management
contract rather than an unrelated general-read gate. Index procedures and APOC
export SHALL have explicit reviewed classes in the canonical inventory.

#### Scenario: A principal terminates its own transaction
- **WHEN** a principal permitted by the supported Neo4j ownership contract ends
  its own transaction but lacks general graph-read permission
- **THEN** absence of that unrelated permission does not prevent termination
- **AND** ending another principal's transaction still requires its applicable
  management permission.

#### Scenario: An index procedure or APOC export is denied
- **WHEN** a principal lacks the operation's reviewed required privileges or an
  applicable deny takes precedence
- **THEN** the canonical evaluator rejects the operation before protected effects
- **AND** positive and negative public-path tests establish its actual class,
  scope and side effects rather than treating it as a default read.

### Requirement: FGAC replaces the legacy RBAC authority in 1.5.0

The planned 1.5.0 FGAC release SHALL replace the legacy RBAC model and scattered
checks through the canonical privilege owner, with a verified approved migration
for existing records. The old and new models SHALL NOT remain competing policy
authorities after cutover.

#### Scenario: Existing RBAC records are migrated for cutover
- **WHEN** the combined FGAC release migrates existing users, roles and privilege
  records under the approved migration contract
- **THEN** the canonical FGAC records/evaluator become the single authority
- **AND** restart and public-path tests verify the approved effective permissions
  before the legacy model/checks are removed.

### Requirement: Integrated family acceptance

The F-PRIV root-cause correction SHALL land with FGAC only after a passing review
of the complete integrated family. The historical inventory and unresolved
reviews SHALL remain tracked until reconciled on current develop.

#### Scenario: One stacked PR passes checks while family findings remain
- **WHEN** #1352 or another component passes individual checks but retained
  family findings or the refreshed operation inventory remain unresolved
- **THEN** F-PRIV remains blocked under Rule 73
- **AND** #1352 does not merge independently as the completed permissions fix.
