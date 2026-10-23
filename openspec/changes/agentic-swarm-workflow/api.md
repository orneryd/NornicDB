# T3 workflow MCP API contract, version 1

The full catalog below is the target contract. T3 owns one typed MCP registry;
there are no new Cypher procedures or Heimdall plugins. Listing an operation here
does not mean it exists. The experiment's actual schemas and capability report
are in the pinned T3 patch and documented in the local setup guide.

## 1. Calling convention

The tool `nornic_workflow` accepts one JSON object with an `operation` field and
returns one structured envelope, including for list operations:

```text
nornic_workflow({operation: "<group>.<operation>", ...request})
  -> {ok, code, data, version, requestId}
```

Success is `ok=true, code="OK"`. Expected contention, readiness failures and
phase denials return `ok=false` with structured data and actionable `next`.
Invalid argument types, unauthorized calls, unsupported transaction context,
internal failures and failed durable commits are query/protocol errors, not
successful empty results. Error messages are localized through existing owners;
stable machine codes are not localized. MCP returns the same structured envelope
and marks protocol/service errors as errors. No error returns a usable permit.

Common request fields:

| Field | Type / requirement |
| --- | --- |
| `apiVersion` | Integer, required, exactly `1` |
| `projectId` | Nonempty string, required except capabilities and project configuration |
| `requestId` | UUID/string, required for every mutation; optional on reads |
| `expectedVersion` | Integer, required when changing an existing task/plan/workspace/change/config record |
| `taskId` | String, required for task-scoped operations below |
| `cursor`, `limit` | Optional list pagination; limit 1–200, default 50 |

Identity, database scope, receipt signer and effective privileges come from
trusted invocation context. The request cannot choose another principal by
supplying a T3 ID. The transport's database selector is authoritative; any
conflicting database in the request is rejected. Results are never cached across
users or reused as fresh authorization. `version` is the affected aggregate's
version; reads return the observed version, and capabilities returns registry
version. List results contain `items`, `nextCursor`, `truncated` and an opaque
snapshot/high-water marker. Bound each response by both item count and bytes.

Mutations use idempotency key `(database, principal session epoch, operation,
requestId)` and a canonical input digest. Same key/input returns the original
receipt; same key/different input returns `IDEMPOTENCY_MISMATCH`. Replay does not
renew an old lease or make an expired permit executable. Retain receipts while
their task/session can resume and for the configured terminal retention period.
After pruning, an expired session remains invalid, preventing replay as new work.

Lists/search never imply exhaustive coverage when truncated. Return a continuation
or `incomplete` reason. An incomplete check cannot establish absence of conflicts.
Conflict detection itself must examine the complete indexed active-overlap set.

## 2. Shared schemas

`Resource`:

```json
{
  "kind": "symbol",
  "repoId": "orneryd/NornicDB",
  "integrationTarget": "refs/heads/release/v1.4.4",
  "path": "pkg/storage/engine.go",
  "symbolId": "language-qualified-stable-symbol-id",
  "sourceHash": "sha256:...",
  "graphGeneration": 12
}
```

`kind` is one of `task|file|symbol|directory|repository|graph_node|graph_region`.
Repository and target must match the project. `path` is required for source
resources, normalized repository-relative without `..`, absolute paths or NUL.
`symbolId` is required for symbols. Graph selectors use stable importer identity,
never transient numeric node IDs; regions include a bounded explicit member list
and generation. Task resources specify `targetTaskId`. Case handling follows the
registered filesystem and Git path rules; reject ambiguous aliases. Preserve
rename mappings and inspect real paths at execution to prevent dependency links
from turning a private path into another worktree's writable source.

`Owner` contains `environmentId`, `threadId`, `providerInstanceId`, `sessionEpoch`,
`runId`, `taskId`, `displayName` and a scoped T3 thread link. `Conflict` adds
`claimId`, `generation`, `resources`, `reason`, `expiresAt`, `state` and pending
change references. Cross-project data requires explicit read authorization.

`Evidence` contains `kind`, `artifactUri`, `sha256`, `producerActionId`,
`baseCommit`, `headCommit` or `treeHash`, `diffHash`, `inputManifestHash`,
`command`, `exitCode`, `startedAt`, `finishedAt`, `outcome`, `summary`, and
`reusedFrom` if applicable. Kinds are investigation, plan, test, docs, benchmark,
review, workspace, patch, PR and external lifecycle receipt. Command/exit fields
are required for executable checks. Outcomes are `passed|failed|cancelled|skipped|
not_applicable|reused`. Only the trusted producer can attest execution results;
agents may attach claims/narrative but these do not satisfy executable checks.
Artifact references must resolve within project-authorized storage. Bound inline
summaries; logs and large patches stay in Git/local artifact storage.

`NextAction` contains `operation`, `requiredInputs`, `reasonCode`, `reason`,
`blockingIds` and `retryAfterMs` when meaningful. This is guidance, not an
authorization token. It must describe missing prerequisites without prompting
for already-granted project authorization.

`Policy` defines allowed task types, phase evidence rules, resource limits,
worktree mode, integration ref, embedding settings and automation boundary.
Policy updates are versioned and invalidate affected permits. Task plans cannot
weaken policy. A readiness check produces obligation IDs and evidence bindings,
not just a Boolean.

## 3. Procedure inventory

In the following tables, `R` means a read operation, `W` means a mutating operation.
All require authenticated T3 invocation and workflow authorization. Additional roles are:
`agent` (assigned task/session), `runner` (trusted execution/evidence adapter),
`indexer` (canonical importer), `router` (T3 delivery adapter),
`tracker` (Linear integration adapter), `maintainer`
(project configuration/recovery), and `reviewer` (assigned review session).
The coordinator may schedule tasks but cannot forge runner receipts.

### Discovery, project and sessions

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `capabilities` | R / authenticated | None | API/schema version, supported operations, provider evidence, strict-enforcement readiness, limits; no secrets |
| `project.configure` | W / maintainer | `repoId`, `targetRef`, `codeDatabase`, `t3EnvironmentId`, `policy`; expectedVersion=0 on create | `project`, `version`; validates single corpus binding and returns initialization obligations |
| `project.get` | R / member | None | Project policy, configured target, current index and resource status |
| `session.open` | W / runner | Trusted transport binding, `providerCapabilities`, `workspaceId` if bound | `sessionId`, `sessionEpoch`, `renewAfterMs`; native/T3 identities resolved from attestation |
| `session.heartbeat` | W / runner | `sessionId`, `sessionEpoch`, `activeActionIds` | `expiresAt`, renewed claim IDs/generations and individual rejection reasons; never revives expired ownership |
| `session.close` | W / runner | `sessionId`, `sessionEpoch`, `reason` | Revoked admission, released claims or draining action IDs; ordinary turn completion must not call this |

Project configuration cannot switch a target while active mutating tasks exist.
Pause/drain them first, then explicitly retarget and invalidate old baselines.
No worker can configure a different code DB to bypass the one-corpus contract.

### Tasks, plans and lifecycle guidance

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `task.create` | W / agent/coordinator | `title`, `objective`, `acceptanceCriteria`, `taskType`, `budget` | Task at intake; each criterion has a stable ID |
| `task.get` | R / member | `taskId` | Phase/status, versions, plan/evidence/claims, dependencies and next actions |
| `task.list` | R / member | Optional status/phase/assignee filters | Paginated tasks scoped to project |
| `task.assign` | W / coordinator | `taskId`, attested destination session | Assignment only; does not transfer active claims; rejects until source quiesces |
| `task.dependencies` | W / assignee/coordinator | `taskId`, replacement list of prerequisite task IDs and required outcomes | Atomically validated acyclic dependency set; missing/failed dependency blocks advancement |
| `plan.put` | W / assignee | `taskId`, scope/resources, steps, acceptance mapping, validation/docs/performance plans | Immutable plan version and invalidated evidence/permit IDs; never marks steps complete by declaration |
| `workflow.next` | R / assignee | `taskId` | Current phase, unmet obligations and legal next operations; no mutation or hidden lease acquisition |
| `workflow.advance` | W / assignee | `taskId`, `toPhase`, evidence IDs, planVersion | Atomic prerequisite evaluation and legal transition, or detailed missing obligations; cannot skip phases |
| `task.pause` | W / assignee/coordinator | `taskId`, `reason`, patch/checkpoint references | Paused status; drain/release plan, no deletion of work |
| `task.resume` | W / assignee/coordinator | `taskId`, refreshed workspace/base | Revalidated phase and missing obligations; no resurrection of old permits |
| `task.cancel` | W / assignee/coordinator | `taskId`, `reason` | Cancelled, effects drained and claims released or explicitly draining |
| `task.observeClosure` | W / runner | `taskId`, verified GitHub merge/closure receipt | Closed state and cleanup eligibility; never performs merge/tag/deployment |

`workflow.advance` may move only along the edges in `design.md`. Freeze, rework
and PR readiness are specialized transitions performed by the operations below;
the generic transition cannot bypass their atomic side effects. No arbitrary
`setStatus("done")` API exists. Tasks are reusable after review findings through
rework, not by overwriting their evidence/history.

### Linear intake and progress projection

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `tracker.configure` | W / maintainer | Linear team/project binding, verified state/label mappings, field-ownership and assignment policy, related-link mode | Versioned adapter profile and capability/readiness checks; never creates statuses implicitly |
| `tracker.bind` | W / tracker | `taskId`, verified issue ID/snapshot, criteria hash, project/milestone, assignee and dependency/PR mappings | Unique outcome binding; paired develop/release PRs stay on one outcome |
| `tracker.refresh` | W / tracker | Binding ID, fresh external snapshot/version and verified source references | Current criteria/status/delivery state, invalidated plan/evidence/permits and sync obligations |
| `tracker.planUpdate` | W / assignee/coordinator | `taskId`, desired lifecycle event, evidence IDs, expected external version | Derived allowed field delta and intent ID, or unmet prerequisites; arbitrary status overrides rejected |
| `tracker.pending` | R / tracker | Cursor, optional binding ID | Bounded pending update intents for the external adapter |
| `tracker.recordUpdate` | W / tracker | Intent ID, attempt ID, API result plus read-back snapshot or explicit failure/unknown outcome | Confirmed/conflict/failed/unknown receipt; no duplicate comment or false completion |
| `tracker.get` | R / member | `taskId` or binding ID | Current Linear mapping, last refresh, pending deltas, evidence provenance and conflicts |

These operations reuse the same envelope/idempotency/authorization rules.
`tracker.refresh` updates bounded local facts; the adapter reads Linear first.
`tracker.recordUpdate` records external effects and must not pretend Nornic and
Linear share a transaction. `workflow.next` exposes tracker/criteria readiness;
new source work, phase transitions and PR readiness require a reconciled current
binding in the Linear-enabled profile. Live status names, label semantics,
human-assignee rules and failure handling are specified in [linear.md](linear.md).
Store API credentials privately outside graph records and model arguments.

### Workspace and code context

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `workspace.prepare` | W / assignee | `taskId`, `baseCommit`, dependency manifest, mode (default isolated) | Workspace intent and stable operation ID; actual filesystem action belongs to runner |
| `workspace.record` | W / runner | Workspace intent ID, attested path/branch/base, dependency fingerprints, isolation test receipt | Ready workspace or failed preparation receipt; cannot bind two editing sessions accidentally |
| `workspace.retire` | W / runner | `workspaceId`, clean/frozen check, retention decision, deletion receipt if removed | Retired or retained-with-reason; dirty/unsubmitted work preserved |
| `code.status` | R / member | None | Desired/indexed commit, graph generation, refresh/embedding state, lag and active refresh owner |
| `code.refresh` | W / coordinator/indexer | `targetCommit`, reason | Coalesced job ID for configured release ref only; never accepts a worker branch as canonical |
| `code.refreshBegin` | W / indexer | Job ID, expected index generation, immutable target commit, resource reservation | Exclusive refresh token; changes status before any importer writes |
| `code.refreshFinish` | W / indexer | Refresh token, structural result/counts/hash and error details if failed | Verified structural generation or failed state; vector readiness recorded separately |
| `code.context` | R / member | `taskId`, resources, `baseCommit`, optional query | Bounded source/impact references with canonical revision, overlays, freshness and completeness |
| `code.overlay` | W / runner | `taskId`, workspace ID, base/head/tree hashes, changed/deleted paths and extracted changed-symbol metadata | Replaces task delta manifest only; no vectors and no full repository upload |

The existing importer remains the only code-node writer. `code.refreshBegin`
and `Finish` wrap that owner with scheduling/readiness; they do not introduce
another importer. Only a trusted indexer can attest its result. The code-context
reader filters shadowed/deleted canonical symbols and labels unembedded matches.

### Ownership and handoffs

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `claim.acquire` | W / assignee | `taskId`, planVersion, resources, reason, optional ttlSeconds | `acquired`, claim ID, generation, expiry; all-or-nothing overlap check |
| `claim.inspect` | R / member | Resources or claim IDs | Active/draining owners and nonexclusive pending changes; complete overlap status |
| `claim.replace` | W / owner | Claim ID/generation, full desired resources, reason | Atomic scope replacement and new generation; failure preserves original set |
| `claim.release` | W / owner | Claim ID/generation, reason | Released or draining; stale release cannot affect replacement ownership |
| `claim.handoff` | W / owner | Claim ID/generation, destination session/task, reason | Handoff offer ID, destination and expiry; source retains ownership until accepted |
| `claim.accept` | W / destination | Offer ID, expected source generation, destination planVersion | Atomic transfer after source quiescence and destination prerequisites; fresh generation |

`session.heartbeat` is the sole renewal path; do not perform a write per file.
Handoff offers expire with their source claim and do not reserve a second copy.
Cancel/reject an offer by source release/replacement or expiry. A direct owner
release does not satisfy freeze/completion evidence requirements.

### Execution and evidence

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `action.authorize` | W / runner | Task/workspace/plan IDs, tool name, canonical argument digest, source/output resources, source hashes, maximum duration | One-use permit ID, expiry and constrained scope, or denied next actions |
| `action.begin` | W / runner | Permit ID, operation ID, observed hashes, runner capability proof | Consumed permit and execution fence; duplicate returns current action state, never starts another execution |
| `action.finish` | W / same runner | Action ID, outcome, actual changed-resource/hash manifest, process quiescence, evidence references | Execution receipt and updated overlay/evidence invalidations; unknown effect stays unresolved |
| `action.get` | R / task member/runner | Action ID | Durable admission/execution/outcome state for reconciliation after disconnect |
| `evidence.record` | W / authorized producer | `taskId`, complete Evidence object | Evidence ID, validity and obligation mapping; failed/skipped checks retained |
| `evidence.list` | R / member | `taskId`, optional kind/current-only filter | Paginated evidence with invalidation causes and source versions |

Action classes are read, source-write, private-build/test-output, dependency
provision, Git commit/rebase, branch publish and PR mutation. Reads may avoid
durable permits after ordinary auth; their source references still carry hashes.
Source-write requires claims. Builds run with source read-only unless explicit
claimed writes are part of the task. Dependency provisioning is runner-owned.
Publish/PR actions are admitted only after the phase-specific evidence check.
Merge/tag/deploy is denied in this profile. Interactive stdin remains within the
original process fence and scope; it cannot create an unrestricted shell escape.

### Frozen changes, review and PR preparation

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `change.freeze` | W / runner for assignee | Task ID, planVersion, base/head/tree/diff hashes, affected resources and valid evidence IDs | Immutable change ID and released claims in one commit; requires no running writes |
| `review.request` | W / coordinator/assignee | Change ID, reviewer assignment, brief and criteria | Review intent/round ID for a new T3 delegated task; implementer cannot attest independent review |
| `review.record` | W / assigned reviewer | Round/change IDs, reviewed hashes, findings, verdict, T3 result reference | Review evidence; findings have stable IDs/severity/resource and required resolution |
| `change.rework` | W / assignee/coordinator | Change ID, finding IDs or new scope reason, refreshed base | New work generation linked to prior change; claims must be reacquired before edits |
| `pr.prepare` | W / assignee | Change ID, title/body, target ref, branch, evidence IDs | Publication intent and exact review bundle, no GitHub side effect |
| `pr.record` | W / runner | Publication intent ID, GitHub PR URL/number, remote head/base, draft status, request receipt | Linked PR and readiness decision; mismatched head/evidence stays blocked/draft |

`pr.record` can finalize `ready_for_review` only when the PR is non-draft and all
obligations are valid. Publishing a draft after a known blocker is allowed as a
checkpoint but does not complete the task. An external new commit or force update
invalidates readiness until reconciled. Never force-push another worker's branch.

### Communication and observability

| Procedure suffix | Mode / role | Additional required inputs | `data` on success and semantics |
| --- | --- | --- | --- |
| `signal.send` | W / task member | Target owner/thread, kind, task/claim/change reference, bounded body | Durable event ID and pending delivery; no ownership transfer |
| `signal.pending` | R / router | Cursor, optional destination | Authorized undelivered/retryable envelopes for T3 transport |
| `signal.recordDelivery` | W / router | Event ID, attempt ID, T3 command/message reference or explicit error | Accepted/failed/unknown delivery state; acceptance is not proof the recipient acted |
| `events.read` | R / member | Cursor, optional task filter | Bounded coordination events; no transcript streaming or whole-graph polling |
| `status` | R / member | Optional task ID | Phase/claim/index/resource/provider readiness and blocked-work summary |

Signal kinds: conflict, question, answer, handoff_offer, handoff_accepted,
claim_released, dependency_ready, review_requested, review_result and task_blocked.
Large discussion stays in T3. Outbox state stores only the necessary envelope
until delivery/retention; the T3 message reference is the durable conversation
locator. T3's thread permission rules remain authoritative. No fallback private
chat bus is silently created when T3 is unavailable.

## 4. Pure functions

| Signature | Behavior |
| --- | --- |
| `resourceKey(resource) -> string` | Internal canonical identity serialization after syntactic validation; no graph/clock/identity access |
| `resourcesOverlap(left, right) -> boolean` | Internal deterministic structural overlap of normalized resource descriptors; unresolved symbol containment requires explicit path/member data and otherwise errors |

These helpers are for planning/display. They cannot establish current ownership,
authorize writes, or substitute for the authoritative claim transaction.

## 5. Errors and limits

Stable expected-denial codes:

| Code | Required response/action |
| --- | --- |
| `CLAIM_CONFLICT` | Conflicting Owner/reason/resources, pending changes, retry or coordinate |
| `CLAIM_EXPIRED`, `STALE_GENERATION`, `SESSION_REVOKED` | Current authoritative state; reopen/reacquire, never silently renew |
| `ACTION_DRAINING`, `ACTION_UNRESOLVED` | Action IDs; reconcile execution before releasing/retrying |
| `PHASE_PREREQUISITE`, `DEPENDENCY_BLOCKED` | Exact unmet obligation/dependency IDs |
| `VERSION_CONFLICT`, `BASE_CHANGED`, `SOURCE_CHANGED` | Expected/current hashes or versions; refresh and invalidate affected evidence |
| `INDEX_NOT_READY`, `INDEX_STALE`, `INDEX_INCOMPLETE` | Desired/indexed commit, status, refresh job or incomplete reason |
| `RESOURCE_PRESSURE`, `BUDGET_EXHAUSTED` | Resource/budget and queued/paused state; bounded retry, no hidden workload duplication |
| `PROVIDER_UNSUPPORTED`, `ENFORCEMENT_UNAVAILABLE` | Failed capability and required integration/test, not advisory success |
| `T3_UNAVAILABLE`, `DELIVERY_UNKNOWN` | Durable event ID and retry/reconciliation state |
| `TRACKER_UNAVAILABLE`, `TRACKER_CONFLICT`, `CRITERIA_CHANGED`, `TRACKER_MAPPING_INVALID` | Binding/intent ID, observed state and required reconciliation; no silent scope/status replacement |
| `EVIDENCE_INVALID`, `REVIEW_REQUIRED`, `PR_HEAD_MISMATCH` | Invalid obligations, expected change and actual remote head |
| `IDEMPOTENCY_MISMATCH` | Reused key and operation; no effect |

Protocol/query errors use namespaced swarm categories for invalid request,
unauthorized, unsupported transaction, persistence failure and internal error,
mapped through T3 MCP protocol errors. Do not claim an invented
Neo4j status code is standard. Partial commit/cancellation outcomes report known
receipt IDs; callers reconcile after transport loss rather than infer rollback.

Proposed initial configurable bounds, returned by capabilities:

- TTL default 120 s, accepted 30–600 s; heartbeat 30 s by default and strictly
  below half the configured TTL. Permit default 10 s, bounded by lease expiry.
- At most 256 explicit resources per atomic claim. A larger change uses an
  explicit directory/repository claim; never split and pretend acquisition was
  atomic. Canonical paths and reasons are bounded strings (4 KiB each).
- 200 list rows and 256 KiB serialized response maximum; 16 KiB signal body;
  1 MiB delta metadata maximum per batch. Large manifests use paginated,
  hash-verified staging and publish a complete manifest atomically.
- 30-day terminal coordination receipt retention by default, configurable.
  Never prune live claims, unresolved actions, pending changes or required
  evidence. Referenced Git artifacts have separately reported retention.
- Resource policy and workload-derived budgets are defined in
  [code-intelligence.md](code-intelligence.md); reaching a bound returns an
  explicit incomplete/queued outcome, never silent truncation of correctness.

## 6. Example agent loop

```json
{"operation":"task.create","requestId":"create-1","taskId":"fix-1","title":"Correct the arithmetic bug","acceptanceCriteria":["square(3)=9"]}
```

```json
{"operation":"plan.put","requestId":"plan-1","taskId":"fix-1","expectedVersion":1,"resources":["calc.py"],"steps":["Replace addition with multiplication"],"validation":["python -m unittest"]}
```

Next acquire the planned resources, read the file and its SHA256, and pass that
hash plus the live claim generation to file.write. Fetch the current task version
before workflow.validate. The duplicate receipt is not independent test evidence.
The target full lifecycle freezes immutable changes, independently reviews them
and prepares a PR only after all obligations pass.

## 7. Invocation and persistence rules

T3 resolves trusted identity before dispatch. Server-owned credentials remain
outside model arguments. Tool availability and authorization are separate;
runner/indexer/tracker-only operations require their own scoped admission.

Each workflow mutation uses one version-checked durable command path. Database
persistence failures do not return a grant. External filesystem, provider and
tracker effects use persisted intents and read-back receipts; the service does
not claim atomicity across those systems. Pending effects fence conflicting
admission until reconciled. A caller cannot wrap a workflow operation inside a
Cypher transaction: the operation is an authenticated T3 MCP call, not a database
procedure. Fresh-session tests must verify persisted outcomes through the public
T3 boundary. Internal helpers do not grant authority.