## ADDED Requirements

### Requirement: One workflow owner across public adapters

The system SHALL expose the version-1 API in `api.md` through one database-scoped
coordination service, typed APOC procedures/functions and opt-in Heimdall MCP
actions. It SHALL preserve existing authentication, database pinning, procedure
modes, collision handling and plugin lifecycle behavior. It SHALL work without
local inference and SHALL identify this namespace as a Nornic extension.

#### Scenario: Adapter parity without local inference

- **WHEN** an authorized client creates a task and acquires resources through
  APOC while the local inference harness is disabled
- **THEN** MCP observes the same task, owner and generation from a fresh session
- **AND** no local model is loaded and no second coordination registry exists.

#### Scenario: Incomplete plugin registration

- **WHEN** a required external action cannot register because its name collides
- **THEN** the first registered handler remains intact and the collision is logged
- **AND** the swarm capability report identifies the missing required operation
  rather than advertising a complete workflow.

### Requirement: Authenticated session ownership

The system SHALL bind ownership to project, T3 environment/thread, provider
session and epoch using trusted context. Display IDs SHALL NOT grant authority.

#### Scenario: Thread resumes with a new provider session

- **WHEN** a thread resumes after its prior provider session expired
- **THEN** it receives a new epoch and must reconcile its task and reacquire claims
- **AND** old renew, release and action requests cannot affect the new owner.

#### Scenario: Agent supplies another thread's identity

- **WHEN** an agent passes another owner's ID in tool arguments
- **THEN** the authenticated scope remains authoritative and unauthorized mutation
  is rejected before changing any claim or evidence.

### Requirement: Autonomous lifecycle with evidence gates

The system SHALL drive the lifecycle in `design.md` through review-ready PR
preparation without mandatory intermediate human approval. It SHALL require
phase-specific evidence and stop autonomous authority before merge/tag/deploy.

#### Scenario: Complete autonomous development

- **WHEN** a task has sufficient scope and its investigation, plan, implementation,
  tests, documentation, performance assessment and independent review succeed
- **THEN** the harness progresses automatically and opens a non-draft PR against
  the configured release branch with the exact validated head
- **AND** the task is ready for maintainer review without merging it.

#### Scenario: Agent declares a missing test passed

- **WHEN** a model submits a narrative that a required test passed without a
  trusted execution receipt for the candidate inputs
- **THEN** the obligation remains unmet and `workflow.next` names the missing check.

#### Scenario: Review changes invalidate evidence

- **WHEN** a review fix changes inputs covered by prior validation
- **THEN** affected evidence is invalidated and those checks run again before
  readiness; unchanged-input reuse is labeled and justified by policy.

#### Scenario: No relevant performance workload

- **WHEN** a documentation-only task meets the policy's not-applicable conditions
- **THEN** a reasoned performance assessment satisfies that obligation
- **AND** no fabricated benchmark or forced unrelated optimization is required.

#### Scenario: Budget or toolchain blocks completion

- **WHEN** a required check cannot run or the tuning budget is exhausted before
  its acceptance target is met
- **THEN** the task is explicitly blocked with preserved evidence and next steps
- **AND** a checkpoint draft PR is not reported as review-ready completion.

### Requirement: Atomic resource ownership

The system SHALL acquire, replace and transfer explicit resource sets atomically
with durable serialization across conflicting resources and all public paths.
Reads SHALL remain available; dependent-code risks SHALL be warnings unless
their exact resources are included in an exclusive claim.

#### Scenario: Concurrent overlapping file and symbol claims

- **WHEN** two sessions simultaneously request a file and a contained symbol
- **THEN** exactly one acquisition succeeds
- **AND** the loser receives the owner's task, reason, T3 route and intersecting resources.

#### Scenario: Failed multi-resource expansion

- **WHEN** replacing a claim would overlap another owner's resource
- **THEN** no new partial ownership is granted and the original claim is unchanged
- **AND** guidance avoids indefinite hold-and-wait between conflicting tasks.

#### Scenario: Rename and graph region

- **WHEN** an agent requests a rename or bounded graph-region edit
- **THEN** old/new paths or the materialized region members are reserved together
- **AND** a stale region generation or ambiguous symbol mapping requires refresh
  or explicit file ownership before editing.

### Requirement: Expiry, fencing and recovery

The system SHALL use expiring leases and durable generations. Release or expiry
SHALL NOT allow overlapping active execution before the old executor is quiescent
or its write authority has been revoked.

#### Scenario: Expired agent wakes after reassignment

- **WHEN** an old session submits a patch after ownership has moved
- **THEN** the old generation is rejected and cannot mutate the replacement's state.

#### Scenario: Shell remains alive past lease loss

- **WHEN** a tool process or descendant remains active as its lease expires
- **THEN** new writes are stopped/revoked by the runner and the claim remains
  draining until quiescence is confirmed
- **AND** the lease timeout alone does not authorize a competing writer.

#### Scenario: Lost execution response

- **WHEN** an action begins and its response is lost
- **THEN** retrying its stable ID returns the recorded state
- **AND** an unknown shell outcome is reconciled rather than executed twice.

#### Scenario: Server restart

- **WHEN** Nornic restarts after a durable claim acknowledgement
- **THEN** ownership and generations recover from storage before new admission
- **AND** unresolved in-flight effects remain visible until runner reconciliation.

### Requirement: Freeze releases ownership and preserves pending work

The system SHALL atomically record an immutable change and release edit claims
after work is complete. Rework SHALL create a new generation and reacquire claims.

#### Scenario: Frozen change waits for review

- **WHEN** validated work is frozen with no running mutations
- **THEN** its edit claims are released in the same transaction
- **AND** another claimant sees its pending change/commit without being blocked
  solely for the duration of review.

#### Scenario: Review requests another edit

- **WHEN** a reviewer finds an issue in the frozen change
- **THEN** rework preserves the prior change and findings, refreshes scope and
  acquires current claims before any new source write.

### Requirement: Isolated worktrees and compatible dependency sharing

The harness SHALL default to separate linked worktrees and reuse only compatible
dependencies with enforced write isolation. Shared-source exceptions SHALL be
explicitly enumerated and serialized as specified in `code-intelligence.md`.

#### Scenario: Two workers share dependency artifacts

- **WHEN** two worktrees have matching dependency/toolchain fingerprints
- **THEN** they may reference the same protected dependency artifacts
- **AND** their source, test databases, ports and mutable outputs remain isolated.

#### Scenario: A worker changes its lockfile

- **WHEN** a worker needs a different dependency fingerprint
- **THEN** the provisioner creates a compatible installation from shared caches
- **AND** other workers' linked dependency targets are not mutated.

#### Scenario: Editing child inherits a checkout

- **WHEN** a T3 launch/delegation path would inherit a writable parent checkout
- **THEN** the harness binds a distinct worktree before enabling editing, or
  refuses that coding launch until isolation can be established.

### Requirement: One active-release code corpus

The system SHALL maintain one canonical code graph and vector corpus per
configured project release target, shared by all workers. Worktree delta and
coordination records SHALL NOT be persistently embedded.

#### Scenario: Additional worktrees start

- **WHEN** multiple agents start work against an unchanged release commit
- **THEN** no additional full ingestion, vector index, database or model instance
  is created and canonical embedding job count remains unchanged.

#### Scenario: Canonical release changes one symbol

- **WHEN** the configured release ref advances with one content-policy-relevant
  symbol change
- **THEN** one refresh owner updates the shared corpus and only affected content
  is queued for embedding; worker branches cannot replace that corpus.

#### Scenario: Refresh partially fails

- **WHEN** an incremental import fails after writing some batches
- **THEN** graph-dependent readiness remains failed/not-ready rather than claiming
  a consistent old or new snapshot
- **AND** retry converges through the existing importer without a duplicate full graph.

#### Scenario: Worktree deletes or changes a canonical symbol

- **WHEN** a task queries context for code it changed or deleted locally
- **THEN** its delta masks the canonical body and returns current source references
- **AND** unembedded/new or incomplete context is labeled explicitly.

#### Scenario: Main and worker hooks fire

- **WHEN** main or a worker branch updates while the project tracks release/v1.4.4
- **THEN** those hooks cannot overwrite the configured release corpus
- **AND** an update to the configured release is coalesced into one refresh job.

### Requirement: Local resource admission and measurement

The harness SHALL budget extraction, embedding, builds and tuning with one host
resource policy, bounded queues and backpressure. It SHALL preserve capacity for
coordination and SHALL report measured performance rather than invented targets.

#### Scenario: Embedding pressure

- **WHEN** host memory pressure makes another batch unsafe under configured limits
- **THEN** new embedding/heavy-job admission pauses while claims and receipts can
  still renew/release, and no per-agent embedding fallback is started.

#### Scenario: Comparable benchmark run

- **WHEN** performance tuning runs a baseline/candidate comparison
- **THEN** both use the declared workload/configuration and record background load,
  correctness, timing, allocations/memory and relevant I/O
- **AND** unmet performance acceptance blocks readiness without weakening tests.

### Requirement: Reuse T3 messaging with durable coordination references

The system SHALL route coordination messages through authenticated T3 operations
using owner identity and stable event IDs. It SHALL retain small delivery state
without replicating transcripts or treating a message as an ownership transfer.

#### Scenario: Conflicting agents coordinate

- **WHEN** a blocked agent asks the current owner about an interface change
- **THEN** the message references the task/claim generation and is delivered to
  the owning T3 thread under T3 permissions
- **AND** any handoff occurs only through the claim acceptance transaction.

#### Scenario: T3 response or session is lost

- **WHEN** delivery outcome is uncertain or the sender session is replaced
- **THEN** the router reconciles the T3 receipt and deduplicates by event ID
- **AND** it does not claim exactly-once LLM handling or create a shadow chat service.

### Requirement: Enforce before effects for both providers

Codex and Copilot CLI SHALL each pass complete enabled-tool coverage tests before
being advertised as strict-capable. Hook/approval configuration alone SHALL NOT
be claimed as a complete execution boundary.

#### Scenario: Agent bypasses the intended edit tool

- **WHEN** an agent attempts an unclaimed write through shell, stdin, a formatter,
  generator, MCP tool or delegated worker
- **THEN** the managed execution boundary prevents the effect before modification
- **AND** returns the missing prerequisite or scope requirement.

#### Scenario: Missing hook or unsupported provider path

- **WHEN** a hook is untrusted/disabled or an enabled tool path lacks enforcement
- **THEN** the provider is not writable in strict mode until configured and tested
- **AND** full-access mode does not silently disable swarm prerequisites.

### Requirement: Atomic durable commands and truthful protocol outcomes

Mutation procedures SHALL follow the standalone autocommit contract, reject
unsupported invocation contexts before effects, and persist command state and
receipts atomically. Read-only functions SHALL remain pure.

#### Scenario: Mutation inside explicit transaction

- **WHEN** a client calls a swarm mutation inside BEGIN/COMMIT or a row-driven subquery
- **THEN** it receives an unsupported-transaction error before any side effect
- **AND** fresh-session reads find no claim, task mutation or success receipt.

#### Scenario: Storage failure or idempotency collision

- **WHEN** persistence fails before command commit, or an existing request ID is
  reused with different input
- **THEN** no partial claim/phase/event state appears and no usable permit is returned.

#### Scenario: Bounded results

- **WHEN** context/event/list results exceed configured output bounds
- **THEN** the result has a cursor or explicit incomplete indication
- **AND** no gate mistakes truncated evidence for complete conflict/impact analysis.

### Requirement: Review-ready PR reflects actual remote state

The system SHALL reconcile Git/GitHub effects by immutable revision and stable
intent IDs and SHALL preserve the boundary between ready for review and merged.

#### Scenario: PR creation response is lost

- **WHEN** GitHub creates the requested PR but its response is lost
- **THEN** the adapter looks up the intended branch/PR and records its receipt
- **AND** it does not blindly create duplicate PRs or report unverified readiness.

#### Scenario: Remote head changes after validation

- **WHEN** the PR head no longer matches the frozen validated revision
- **THEN** readiness is invalidated and the required checks/review are reconciled
- **AND** no automatic merge, tag or deployment follows.

### Requirement: Linear drives scoped development intake

The harness SHALL bind one execution outcome to a verified Linear issue and SHALL
use its current criteria, dependencies and scope under the existing NOR workflow.
Human assignee, tracker delegate and T3 execution identity SHALL remain distinct.

#### Scenario: Eligible issue begins development

- **WHEN** an eligible Todo issue has current criteria, satisfied dependencies and
  approved release scope
- **THEN** the harness binds the issue and confirms Dev In Progress in Linear
  before authorizing its first source mutation
- **AND** investigate, plan, claims and action prerequisites still apply.

#### Scenario: Human changes criteria or cancels work

- **WHEN** a refreshed issue changes acceptance criteria or becomes Canceled or Duplicate
- **THEN** affected plans/evidence/readiness are invalidated or execution is drained
- **AND** no new source mutation proceeds using the obsolete issue snapshot.

#### Scenario: Tracker becomes unavailable

- **WHEN** the harness cannot confirm a required current tracker snapshot or transition
- **THEN** new source writes, phase advancement and review readiness are blocked
- **AND** bounded running checks may finish while claims drain safely.

### Requirement: Linear progress is reconciled from evidence

The tracker adapter SHALL project the existing status and Delivery mapping from
verified evidence, preserve human edits and use bounded idempotent reconciliation.
It SHALL NOT invent a Review state or equate release verification with shipment.

#### Scenario: Validated PR becomes ready for review

- **WHEN** the exact release PR revision satisfies validation, documentation,
  performance and independent review obligations
- **THEN** the adapter derives the applicable Test Complete and Delivery update
  and records non-closing develop/release PR links on the same outcome
- **AND** it does not report Merged or Released without their external evidence.

#### Scenario: Human edit races with a tracker update

- **WHEN** read-before/write/readback detects conflicting human changes
- **THEN** the adapter records a conflict and preserves the human-owned fields
- **AND** it does not pretend Linear supplies an unverified atomic compare-and-swap.

#### Scenario: Tracker write response is lost

- **WHEN** a progress update has an unknown delivery result
- **THEN** the adapter reconciles the current issue and stable update intent
  before retrying
- **AND** it avoids duplicate comments, unrelated label removal and private
  discussion publication.
