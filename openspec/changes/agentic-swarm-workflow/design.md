# Design and lifecycle

## 1. Owners and integration boundaries

One database-scoped workflow service owns all swarm state transitions. Proposed
home: `pkg/swarm`, with narrow host-supplied storage, clock, identity and policy
interfaces. Do not import `pkg/nornicdb` back into that service or create package
cycles. The host constructs one instance per configured database and injects the
same service into both adapters; two plugins must not create separate registries.

| Component | Canonical responsibility |
| --- | --- |
| Nornic workflow service | Tasks, evidence references, phase rules, claims, permits, pending changes, idempotent receipts |
| APOC plugin adapter | Typed Cypher arguments, modes, metadata and result projection |
| Heimdall swarm plugin | Explicit external MCP action schemas and the same service calls |
| Harness adapter/execution gateway | Trusted session binding, worktree provisioning, action interception, constrained process execution and evidence capture |
| Existing code-intelligence importer | Release-commit extraction, incremental content updates and repository graph ownership |
| Existing embedding worker | Shared model lifetime, queues and vector generation for canonical code only |
| T3 | Threads, provider sessions, delegation, conversation delivery and thread UI |
| Git/GitHub | Commits, branches, pull requests and maintainer merge authority |

The harness owns filesystem/process effects; APOC does not execute arbitrary
shell commands. The database records receipts for effects that occur elsewhere.
Direct Bolt/HTTP callers receive the same authorization checks as MCP callers.
An LLM-provided `agentId` is a display hint, never proof of ownership.

## 2. Identity and graph model

The owner key is `(database, projectId, environmentId, threadId, sessionEpoch)`.
Store `providerInstanceId`, native provider session ID, current T3 run ID and
task assignment alongside it. A new provider session receives a new epoch;
the same thread title or ID alone cannot revive old leases. Parent/child lineage
does not grant children the parent's claims. Native workers lacking separately
attested identity cannot independently edit in strict mode.

Store coordination records with dedicated `Swarm*` labels in the same configured
database, separate from importer-owned code labels. Required entities:

| Entity | Required state |
| --- | --- |
| Project | Stable repo identity, active release ref, policy version, database and T3 environment bindings |
| Session | Trusted identity binding, epoch, capabilities, last renewal and revocation |
| Task | Acceptance criteria, dependencies, phase, status, assignee, state version and budget |
| Plan | Versioned scope, affected resources, validation/docs/performance obligations and rationale |
| Workspace | Worktree path/branch, immutable base, dependency manifest, executor binding |
| Claim | Owner, task, resource set, reason, generation, lease expiry and state |
| Action | Exact tool/arguments digest, permitted scope, generation, execution state and receipt |
| Evidence | Type, artifact reference/hash, source revision, producing action and outcome |
| Change | Frozen commit/tree/diff digest, affected resources, base, pending PR and supersession |
| Notification | Small routing envelope, event ID, T3 delivery reference and retry state |
| IndexState | Desired/indexed commit, generation, refresh status, structural/vector readiness |

Use relations for task dependencies, ownership, affected code, evidence and
supersession. Resource keys remain valid without a current graph node. Never
store embeddings, transcript copies, credentials or full command logs on
coordination nodes. Enforce this exclusion before creating the first such node.

## 3. State machine

`phase` and `status` are separate. Status is `active`, `blocked`, `paused`,
`ready_for_review`, `cancelled` or `closed`. A blocked task retains its last phase,
typed reason and next actionable step; it does not pretend to have progressed.

| Phase | Entry prerequisites | Exit evidence / next phase |
| --- | --- | --- |
| intake | Project and authenticated session | Concrete task, acceptance criteria, target ref and dependencies → investigate |
| investigate | Bound read-only workspace; resource budget | Source references, baseline revision and impact analysis → plan |
| plan | Investigation evidence | Versioned implementation/spec plan, test plan, docs obligations, performance workload or justified not-applicable decision → implement |
| implement | Satisfied task dependencies, workspace readiness, current claims and execution capability | Actual patch and required regression tests → validate |
| validate | Exact tree/diff identity and runnable test plan | Required checks pass with fresh evidence → document; failed code checks → implement |
| document | Validated behavior | Public API/examples/changelog or justified documentation assessment → optimize |
| optimize | Workload and baseline defined, or recorded not-applicable assessment | Measured changes meet acceptance criteria; any code edit returns to validate → freeze |
| freeze | All obligations valid for current tree; no in-flight mutating actions | Atomic frozen-change record plus edit-claim release → review |
| review | Immutable change available, no edit claims required | Independent agent findings resolved → prepare_pr; actionable findings → rework |
| rework | Recorded findings and refreshed baseline | Reacquire affected claims and create a new plan/change generation → implement |
| prepare_pr | Current review/validation/docs/performance evidence | Publish branch, create/update PR against configured target, record GitHub receipt → ready |
| ready | PR head matches frozen revision and all required evidence | `ready_for_review`; maintainer may later merge or close |
| closed | Verified external merge/closure/cancellation receipt | Retain evidence; release remaining ownership; eligible workspace cleanup |

The harness advances phases automatically through `workflow.advance`. There is
no mandatory human plan approval. Human clarification is needed only when scope
or acceptance meaning cannot be derived from the task. Runtime prerequisites
are deterministic rules; an LLM saying "tests passed" cannot satisfy them.

Documentation edits after tests must be classified against the test manifest.
Tests may be reused only when their declared inputs remain byte-identical and
the policy permits reuse. Label reused results as reused. Any behavioral change,
dependency change, rebase or review fix invalidates affected evidence. Re-run
required checks on the final candidate; a prior branch's success is not proof.

Performance tuning is bounded by task budgets. Never optimize indefinitely or
invent a gain. For documentation-only or otherwise irrelevant changes, record a
reasoned not-applicable result; the policy defines when that classification is
valid. Existing failures and missing toolchains remain visible blockers to
readiness, not silent exceptions. A draft PR can preserve blocked work without
marking it ready. Merge, deployment and tagging are not actions in this profile.

## 4. Claims and conflicts

Claims express exclusive editing intent, not read locks. Reads remain available.
Canonical identity includes stable repository ID and integration target, not
the local worktree path. Separate feature branches targeting the same release
therefore still coordinate.

Resource kinds: `task`, `file`, `symbol`, `directory`, `repository`, and explicit
`graph_node`/`graph_region` selections. File creation claims the intended path;
rename claims old and new paths; deletion claims the removed path. Directory
overlap is segment-aware (`pkg/a` does not cover `pkg/abc`). Repository claims
cover all source paths. Graph regions are bounded, materialized resource lists
at a recorded generation, not live transitive predicates.

File claims cover contained symbols. Symbol editing is allowed only if the
executor verifies the patch is confined to the resolved symbol span in the
exact source hash. Otherwise acquire the containing file. Shared imports,
file-level declarations and formatters normally need a file claim. Renames and
unresolved/stale symbol identities fall back explicitly to file ownership.
Dependency paths produce actionable warnings, not transitive exclusive locks.

Acquire/replace a set atomically, all or nothing. Serialize overlap decisions
per `(database, repo, integrationTarget)` with a durable transaction guard; the
guard must be written so snapshot-isolation write skew cannot admit overlapping
sets. A local mutex alone is not a durable or clustered correctness argument.
Use the existing authoritative writer/transaction route. Active-active replicas
that cannot provide this serialization are not a supported lease authority.

Keep the critical section short: normalize resources before admission, check
the indexed active claims, persist claim/generation/receipt/event together, and
commit durably before acknowledging. Do not embed, traverse dependencies, call
T3 or wait for a competing agent inside the transaction. Failed acquisition
returns the owner's routing identity, task, reason, expiry and intersecting
resources. Optional fair queueing uses non-blocking durable wait tickets; the
initial implementation may use bounded backoff and event wakeups instead.

No hold-and-wait for expanded scopes: a failed replacement leaves the original
claim unchanged and returns a handoff/replan action; the scheduler must not let
two tasks wait forever while each retains the other's needed resource. Release
and retry the complete desired set, or negotiate an explicit handoff.

## 5. Lease lifetime and actual execution

Default proposed lease: 120 seconds; one session renewal every 30 seconds,
batched across that session's claims. Configuration bounds are in `api.md`.
Expiry is checked during acquisition/authorization, not only by cleanup.
Every ownership change increments a durable generation. Old renew/release/action
requests cannot affect a newer owner. A transfer requires explicit acceptance
by the destination and quiescence of the source; messages alone do not transfer.

Execution admission binds a one-use permit to exact arguments, session epoch,
claim generation, workspace, plan version and source hashes. Permit lifetime is
at most the remaining lease. The trusted gateway consumes it before any effects.
It retains a durable action state and records outcome after execution. A lost
response is reconciled by action ID; never blindly replay a shell command.

A pre-tool check alone cannot stop a long-running or interactive process after
lease loss. The runner must constrain all descendant writes to granted source
paths and declared private output paths, monitor lease/deadline, and stop or
revoke write access before releasing its execution fence. Until quiescence is
confirmed, the claim is `draining` and conflicting work remains blocked even if
its lease has expired. A disconnected machine can keep editing its private
worktree, but cannot submit those effects as authorized. Resume requires a new
session, reconciliation and new claims. Shared-workspace recovery requires
confirmed process termination/revocation; a timeout alone is insufficient.

For finite mediated patches, consume the permit, verify current hashes, and
apply under the local workspace fence. Scope changes invalidate permits.
The action gate fails closed on missing authority/readiness. Existing reads,
diagnostics and coordination remain available. A provider with incomplete write
coverage must not be advertised as strict-capable.

## 6. Freeze, review, handoff and external effects

`change.freeze` checks no active writes, creates an immutable pending change and
releases edit claims in one database transaction. Pending changes remain visible
in impact/conflict responses but do not keep exclusive ownership through review.
An agent acquiring those resources receives the pending commit/PR and rebase
warning. New changes do not overwrite earlier evidence; they supersede it.

Pause/cancel records the reason, cancels admission, drains actions, preserves
unsubmitted patch references and then releases claims. Do not delete dirty
worktrees. Normal T3 end-of-turn is not task completion or claim release.
Completed review rounds are separate T3 delegated tasks; do not try to reopen
a completed reviewer via a message. A human's review fixes use the same rework
loop and can run autonomously to another review-ready revision.

Git branch publication and PR creation are external effects: prepare an intent
with a stable key, execute through the trusted adapter, then record the remote
head/PR receipt. If the response is lost, query the exact branch/PR before retry.
Do not claim distributed atomicity between GitHub and Nornic. A mismatched PR
head is not ready. Merge/closure observations update local lifecycle facts;
they do not authorize a merge. Keep a review-ready worktree until retention and
dirty-state checks allow explicit cleanup.

## 7. Compatibility, startup and rollout

Install the optional swarm service and adapters explicitly. All mutation
procedures use `WRITE`, reads use `READ`, and configuration uses admin checks.
Pure helper functions do not read mutable state or cause side effects.
`SHOW PROCEDURES`/`SHOW FUNCTIONS` must expose truthful signatures. Existing
APOC names/behavior remain unchanged. No semantic claim is made for Neo4j.

The initial control-plane transaction contract is standalone autocommit calls:
mutation procedures are rejected inside caller-managed explicit transactions,
multi-statement batches and row-driven/subquery invocation before any effect.
The host must expose and enforce this context; do not guess from query strings.
This avoids reporting a grant that is later rolled back by an outer transaction.
Reads use normal transaction snapshots for display only; authorization always
checks current authoritative state. MCP mutations use the same internal atomic
command path. Future composable transaction support needs a separate explicit
contract, not an autonomous inner commit masquerading as rollback-safe work.

Service startup verifies storage, schemas, embedding exclusions and required
adapter registration before advertising capabilities. Unload stops admission,
drains active calls and removes public tools through the existing lifecycle.
Persisted claims survive server restart and are reconciled before new ownership.
An in-memory backend is suitable for isolated tests, not durable production claims.

No runtime behavior is changed in this spec PR. Implementation is incremental,
but the feature is not marked complete until both requested providers pass the
full lifecycle and enforcement tests. Use supported T3/provider extension points;
any unavoidable third-party source patch needs its own explicit maintainer scope.
