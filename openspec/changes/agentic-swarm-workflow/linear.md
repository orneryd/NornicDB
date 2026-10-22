# Linear-driven development workflow

## 1. Inspected workspace and boundary

The connected `NOR` / `Nornicdb` team and its **v1.4.4 — Stabilization & release**
project were inspected on 2026-10-22. The project already distinguishes outcome
tracking, develop/release evidence and final shipment. This proposal uses that
workflow; it does not create new team statuses or replace it with a generic
"Review" column. The planning PR does not mutate Linear.

The inspected project has three milestones:

| Existing milestone | Harness meaning |
| --- | --- |
| Scope confirmed | An outcome and its release inclusion are explicitly established |
| Release-branch fixes verified | Required behavior/checks are evidenced on the intended release candidate |
| Release approved & shipped | Maintainer release decision plus verified main/tag/publication evidence |

The initial work register is a historical snapshot; live issues and current
source evidence are authoritative. Existing internal work uses non-closing PR
links, with separate develop and release counterparts. Branch-specific status
automation is not established by the inspected configuration. Preserve that
boundary and qualify the new adapter rather than assuming GitHub integration
already provides these transitions.

Team/project/status/label/user UUIDs are resolved through the authenticated
adapter at installation and kept in private configuration. This public spec
records names and behavioral mappings, not internal discussion, user emails,
private evidence excerpts or exported workspace data.

## 2. Sources of truth

| Fact | Authoritative owner |
| --- | --- |
| Outcome, acceptance criteria, priority, scope, project/milestone, human assignee, issue dependencies | Linear issue/project, with human edits preserved |
| Current execution phase, claims, agent session, permits and evidence receipts | Nornic workflow service |
| Conversation and executing agent identity | T3 thread/provider session |
| Actual code, branch, PR head/base/merge and release publication | Git/GitHub |
| Linear status/Delivery update proposed by the harness | Evidence-derived projection, applied through one tracker adapter and reconciled with the live issue |

There is one internal task aggregate per Linear outcome and repository/target
binding, with execution attempts and contributing PRs underneath it. Develop and
release PRs for the same outcome must not become duplicate independent issues.
Multiple tasks may be explicit child tasks when the issue's acceptance scope is
decomposed; child completion does not automatically complete the parent.

Import the current issue description, criteria, dependencies, assignee, project,
milestone, labels, attachments and external update/version marker. Extract stable
criterion IDs into the task plan and record a hash of the exact source text.
An agent can propose a clarification; it cannot invent missing acceptance meaning
or silently narrow the Linear outcome to match an easy implementation.

Linear labels/status are planning evidence, not execution authorization. An
imported Test Complete issue does not by itself mint passed test receipts.
Existing author/maintainer-reported validation must retain its source and checked
revision and be accepted under explicit evidence policy; never relabel it as a
fresh independent run. When proof is missing, show the discrepancy without
blindly downgrading every existing issue on first sync.

## 3. Exact status mapping

These are the observed team names/types. Resolve by UUID after validating names;
do not rely on display order or collapse all `started` states into one meaning.

| Existing Linear status | Type | Harness projection rule |
| --- | --- | --- |
| Backlog | backlog | Unscheduled outcome; no worker automatically takes it unless project policy includes it |
| Todo | unstarted | Eligible scoped work; investigate/plan can occur here before source mutation |
| Dev In Progress | started | Implementation or code-changing rework is active, including required documentation/optimization edits |
| Dev Complete | started | Planned source/docs changes are complete for a candidate; required validation has not yet started or must be rerun |
| Test in Progress | started | Required validation is running, incomplete, timed out or awaiting a usable environment; failed checks needing code route to Dev In Progress |
| Test Complete | started | Required scoped candidate checks are valid for the current revision, with review readiness separately reported |
| Merged | started | Actual intended release counterpart is merged; validation/shipment remain separate facts |
| Released | completed | Verified release inclusion, main/release workflow, tag and agreed publication satisfy the release gate |
| Canceled | canceled | Explicit outcome cancellation; pause/drain agents and preserve work |
| Duplicate | duplicate | Explicitly linked duplicate of another outcome; stop duplicate execution and preserve the canonical issue link |

There is no observed Review status. Independent agent review and a PR ready for
human review generally remain **Test Complete** if the acceptance checks still
hold. A review finding requiring code returns to **Dev In Progress**; a missing
test returns to **Test in Progress**. A PR's draft flag alone decides none of
these development/testing states.

The phase engine may revisit validation after documentation or tuning changes.
The projection uses the actual current obligation set, not a one-way ordinal
rank: code work → Dev In Progress; source complete → Dev Complete; checking →
Test in Progress; valid required checks → Test Complete. Meaningful transitions
are recorded; transient scheduling events do not spam comments.

Merged is a delivery fact and is not overwritten by a later validation status.
Post-merge validation problems use Merged plus Blocked/Delivery evidence and a
linked corrective task. Reverted release inclusion is reconciled as a changed
delivery fact, with an explicit reason; do not pretend it remains merged into
the active candidate. Released is never inferred from a PR merge or closed issue.

## 4. Existing labels and delivery rules

The **Delivery** group is single-select:

| Observed label | Evidence needed |
| --- | --- |
| Develop only | Implementation evidence exists, but no verified release delivery |
| Release PR open | Actual linked PR targets the intended release or its verified dependency stack and remains open |
| Release merged; validation pending | The intended release counterpart is merged, but exact candidate validation is incomplete |
| Release verified | Required behavior/checks are valid on the intended release candidate; does not mean shipped |

Select at most one child label and preserve unrelated labels. For the same
outcome, prefer release evidence over a still-open develop counterpart. Release
verified supersedes the weaker delivery labels only when its candidate proof
remains applicable. New candidate inputs may require validation again.
Do not apply the Delivery group itself as a label.

**Blocked** records a concrete blocker and next action without inventing a new
status. **Needs decision** is for missing scope/acceptance/ownership decisions;
ordinary claim contention or compute queueing is not automatically a request
for human intervention. Remove only harness-owned blocker annotations after
their cause is resolved; do not erase a maintainer's unrelated blocker.

The **Source** group contains GitHub and Internal. These indicate origin, not
privacy controls. Existing intake is one-way and PR-comment linkbacks are
disabled according to project notes; the adapter must verify effective routing
before writes rather than assuming a label prevents publication. Do not copy
private Linear discussion into public PRs, synced intake threads or Linear
Reviews. Publish an approved public summary/evidence bundle separately.

## 5. Concrete harness loop

For an eligible Todo issue in the release project:

1. **Bind the outcome.** Resolve its immutable issue ID and current snapshot;
   reuse the existing Nornic task if bound. Read acceptance criteria, milestone,
   dependencies and both develop/release PR links. Record the target commit/ref.
2. **Resolve people and agents.** Keep the responsible Linear human assignee.
   Bind a verified T3 environment/thread/session as the executing agent. If
   GitHub and Linear assignees differ, use a configured verified account mapping
   and explicit assignment policy; don't guess from display names or PR authors.
3. **Investigate and plan.** `workflow.next` returns the issue criteria, missing
   obligations, relevant shared-code references and allowed next operations.
   Save the plan with a criterion-to-test/docs/benchmark mapping. Insufficient
   acceptance meaning becomes Needs decision with the precise question.
4. **Enter development.** Provision a separate worktree, reserve compute and
   acquire claims. Prepare/confirm Dev In Progress through the tracker adapter
   before the first source mutation. The LLM receives conflict owners/T3 routes
   and can coordinate instead of editing a claimed resource.
5. **Finish and verify.** Record the source-complete candidate as Dev Complete;
   then run required checks under Test in Progress. Report exact head/base,
   commands, outcomes, skips, source and limitations. Code corrections return
   to Dev In Progress and reacquire any released claims.
6. **Document and tune.** Complete docs/examples and applicable performance work
   autonomously; any changed validation inputs invalidate earlier receipts.
   Re-enter testing until current acceptance evidence is sufficient.
7. **Freeze and review.** Release claims atomically with the frozen change. With
   required checks complete, project Test Complete. Run independent agent review;
   findings produce a new work generation and the appropriate earlier state.
8. **Prepare the PR.** Create/update the release-target PR and non-closing Related
   linkage. Record the actual remote head and compact evidence summary in the
   internal issue. Test Complete plus the review-ready receipt identifies the
   maintainer handoff; it does not mean Merged or Released.
9. **Observe later delivery.** After a maintainer merges, reconcile actual release
   inclusion to Merged and the correct Delivery label. Only verified shipment
   evidence can later project Released. The agent never performs merge/tag/deploy
   under this autonomous development profile.

Example `workflow.next` payload (synthetic IDs, observed status names):

```json
{
  "phase": "validate",
  "tracker": {
    "provider": "linear",
    "issueId": "configured-issue-uuid",
    "status": "Test in Progress",
    "criteriaHash": "sha256:current-criteria",
    "syncState": "confirmed"
  },
  "obligations": [
    {"id": "AC-1", "state": "passed", "evidenceId": "ev-21"},
    {"id": "AC-2", "state": "missing", "requiredCheck": "lifecycle race regression"}
  ],
  "next": [{
    "operation": "action.authorize",
    "reasonCode": "PHASE_PREREQUISITE",
    "requiredInputs": ["taskId", "workspaceId", "tool", "argumentDigest"]
  }]
}
```

## 6. Assignment and dependency semantics

Keep three identities separate: Linear responsible person, optional Linear
delegate integration, and T3 executing session. A T3 thread ID is not a Linear
user ID. Populate the delegate field only for a real installed Linear agent
identity; otherwise expose executing-thread links in the approved internal
progress section. Do not assign all agent work to the integration's account.

The operator can configure GitHub-to-Linear account mappings. Preserve an
existing explicit assignee; any mismatch that needs a decision is visible.
Missing mapping blocks reassignment, not unrelated read-only investigation.
Never mass-assign issues just because a PR is authored by someone.

Linear blocks/blocked-by relations are authoritative declared dependencies;
Git branch stacks are separately observed code dependencies. Reconcile both.
Textual stack references become proposed relations until verified. A sibling
develop PR does not block an already delivered release counterpart. Changes to
acceptance criteria or prerequisites invalidate affected plans/permits and return
the agent to the earliest required phase without discarding completed artifacts.

## 7. Tracker adapter and synchronization

Add a `tracker` role and the seven procedures in `api.md`. The adapter performs
external Linear effects, while Nornic stores bounded snapshots, update intents
and receipts. Agent-facing tools request an outcome/status update; they cannot
write arbitrary status IDs or self-certify completion.

Before intake, phase changes and PR readiness, refresh the live issue. Use
webhooks if supported and configured, plus one bounded incremental reconciliation
loop (default 60 seconds while active) for missed events. Validate webhook
authenticity where applicable. Use cursors, rate-limit backoff and coalescing;
never rescan all Linear issues on each tool call.

Each write intent includes issue ID, observed external version/update marker,
intended field delta, task/plan/change version, evidence IDs and an idempotency
key. Only one adapter worker writes harness projections for a given issue.
Re-read before writing; preserve unrelated description/labels/assignee fields.
Use an API conditional mutation if supported. Without server-side compare-and-set,
preflight reads are not atomic: read back afterward, detect conflicting edits,
surface them and reconcile to the latest human-owned decision. Do not claim
lost-update prevention solely from timestamps. Do not automatically retry a
conflicting full-description overwrite.

Write compact progress into an explicitly owned internal section or one
updateable progress record with stable ID. Keep acceptance criteria human-owned.
On uncertain response, read the issue/progress record before repeating; never
duplicate comments blindly. Separate `planned`, `sent`, `confirmed`, `failed`,
`conflict` and `unknown` sync states. An API response accepted by the transport
is not confirmed until the relevant remote state is observed.

If Linear is unavailable, keep reads/diagnostics and already-running bounded
checks available; do not newly start source mutation, change phase or claim PR
readiness on a stale intake snapshot. Drain/release claims when a phase is
blocked by synchronization. Preserve the exact failure and resume automatically
after successful reconciliation, subject to current scope and evidence.

Manual cancellation/duplicate marking stops admission and drains execution.
A manual assignee/criteria/project change triggers reconciliation; a status drag
alone does not forge evidence or silently rewrite code scope. Manual reopening
creates a new work generation. External merge/ship observers may update delivery
facts after the execution task closes, but cannot authorize the external action.

## 8. Installation and rollout

Suggested project profile (names resolved to verified private IDs at setup):

```yaml
tracker:
  provider: linear
  team: NOR
  project: v1.4.4 — Stabilization & release
  target_ref: refs/heads/release/v1.4.4
  intake_states: [Todo]
  completion_boundary: ready_for_review
  pr_link_mode: related
  create_statuses: false
  reconciliation_seconds: 60
  preserve_human_assignee: true
  publish_private_discussion: false
```

This is proposed adapter configuration, not a currently accepted Nornic config
block. Verify API permissions, active status/label IDs, mappings and integration
routing before enabling mutations. Discover unknown/renamed states explicitly;
do not guess a substitute. Do not auto-enroll every Backlog issue or invent a
release date. The release project has a scoped feature-exception policy; binding
this design does not silently approve shipping all swarm implementation in v1.4.4.

Start with a read-only mapping preview, then a dedicated fixture issue and the
two-provider lifecycle test. Acceptance includes manual edits, provider restarts,
Linear outage, duplicate webhooks, lost mutation responses, criterion changes,
paired PRs, exact-head evidence and release-only completion. No new status or
live issue changes are performed by this planning PR.
