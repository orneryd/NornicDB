# Upstream T3, Codex and Copilot integration

## 1. Verified integration inputs and limits

The source inventory in [sources.md](sources.md) pins inspected T3 documentation.
It establishes provider-session-scoped MCP credentials, durable T3 thread IDs,
thread messaging, worktree-aware launch, Codex app-server integration and generic
ACP providers. It does **not** establish a universal T3 pre-execution interceptor
or prove the T3 workflow extension can prevent every native tool write.

Codex is a native T3 provider. Copilot CLI offers ACP and can be configured as a
T3 local ACP command (`copilot --acp --stdio`) or a compatible registry entry
when available. Copilot CLI can use its supported account or BYOK configuration.
VS Code Copilot is a separate runtime; installing an MCP server there alone
does not make it a strict T3-controlled worker. Do not treat an API key as a
provider adapter, execution hook or T3 identity credential.

Use the provider's supported authentication flow; keep model keys in the provider
credential store/environment, never graph properties, plans or PR bodies.
Record exact T3, Codex, Copilot, adapter and OS versions in capability evidence.
Authentication success and model availability are separate from enforcement.

## 2. Proposed installation model

This repository supplies a pinned, user-authorized local T3 extension patch and
setup scripts. T3 hosts the workflow service, trusted execution gateway and
workspace manager. Existing NornicDB APIs store coordination in `t3workflow` and
the release corpus in `nornicdbcode`. No custom NornicDB plugin is installed.

The integration resolves T3 invocation scope inside T3's authenticated MCP
service, without trusting model-supplied IDs. The internal workflow owner calls
NornicDB using server-owned configuration; T3 bearer tokens are not forwarded as
database identity and no undocumented attestation endpoint is needed.
Do not scrape transcripts, rely on titles, or invent an undocumented identity
endpoint. If the pinned upstream API cannot attest this binding, record the
exact upstream extension needed as an implementation dependency; strict T3
integration is not complete until it is supported and tested.

The gateway keeps runner/indexer/router credentials outside agent-readable files
and tool arguments. Agent-facing MCP calls can request plans and claims, while
only the gateway can attest actual execution and validation. Hooks return
model-visible next steps; they do not expose authority-bearing credentials.

## 3. Codex adapter

Use supported session and pre/post-tool hooks for lifecycle context, phase
checks and outcome collection. Pin and test the exact hook schemas. Current
official documentation includes shell/unified-exec, patch and MCP tool coverage,
but also notes exceptions and that interactive stdin does not rerun the initial
pre-tool hook. Hooks alone are not a complete isolation boundary.

Strict profile requirements:

- Prove hooks are enabled/trusted before starting a writable task. A missing,
  disabled or changed-untrusted hook makes the profile unready.
- Intercept every enabled local mutation tool, not just `apply_patch`. Bind
  unified-exec sessions, stdin and descendants to the original execution scope.
- Combine hooks with a gateway/sandbox that limits source writes to claimed
  paths and makes other worktrees and shared dependencies read-only.
- Reject native delegated workers unless their identity, tool paths and write
  boundary are covered; use T3-owned isolated workers instead.
- An approval-mode change cannot remove workflow prerequisites. Ordinary Codex
  approval requests can be auto-satisfied only within an already authorized,
  currently permitted action; broad approval is not a lease.
- Fail hook/gateway outages into blocked mutation, with a useful recovery step.
  Provider-native handling of malformed hook output must be tested rather than
  assumed to fail closed.

If the pinned provider cannot constrain a native tool path, remove that tool
from the strict profile using supported configuration and supply an equivalent
gateway operation. If neither supported interception nor restriction can cover
it, mark the version unsupported for strict coding. An advisory-only demo is
not completion of the user's requested prerequisite enforcement.

## 4. Copilot adapter

Run Copilot CLI through upstream T3's ACP mechanism, with supported hooks and
tool filtering. Verify the CLI's actual hook behavior in ACP mode. GitHub's SDK
also documents pre-tool hooks, but SDK hook availability is not proof the same
callback is exposed through ACP. Use a supported wrapper/SDK adapter only when
it preserves T3 identity and provider protocol correctly; do not claim they are
interchangeable implementations.

Apply the same execution gateway, workspace limits, claim/phase rules, child
identity and evidence receipts as Codex. ACP permission callbacks alone are
insufficient: T3 can auto-approve them, and agents may execute tools themselves.
Do not infer full tool coverage from activity events received after execution.
Use the CLI's advertised tool inventory and supported exclusions to establish
the complete enabled surface. Test native subagents explicitly.

VS Code remains a useful manual review/client surface. A future VS Code worker
adapter can implement the same contract, but is not counted as the requested
T3 Copilot provider. Manual edits are detected through source hashes and require
reconciliation; this feature does not claim to police an unrestricted human OS
account outside the managed worker boundary.

## 5. Phase-driving loop

For each task the harness:

For a Linear-bound task, first import/reconcile the existing outcome and its
criteria through the tracker adapter. Apply [linear.md](linear.md)'s observed
status mapping after each evidence-backed transition; preserve the assignee and
use non-closing PR links. `workflow.next` carries these tracker obligations into
the same execution loop, rather than starting a separate competing workflow.

1. Binds the authenticated provider session and provisions an isolated workspace
   from a pinned release commit before enabling write tools.
2. Calls `workflow.next`, retrieves bounded code context and exposes the current
   obligations to the LLM. It does not inject all API schemas/history every turn.
3. Lets the LLM investigate and propose a plan; records artifacts through the
   typed API. Evidence required by the phase comes from actual tool receipts.
4. Reserves local compute and atomically acquires the needed claims. Conflicts
   expose the owning T3 thread, task and reason; coordinate or choose other work.
5. Before each mutation, obtains/consumes a permit and executes within the source
   and output constraints. Captures outcomes and invalidates affected evidence.
6. Advances automatically through validation, documentation and measured tuning.
   A failing check routes back to implementation; missing capacity queues work.
7. Freezes the exact revision and releases edit ownership; starts an independent
   review with only the brief, criteria, change references and required context.
8. Reacquires ownership for review findings, repeats affected checks and review,
   then prepares/publishes the PR and verifies its actual head before readiness.

When a provider reaches context/usage limits, checkpoint task/plan/patch state,
drain writes and release claims. A replacement session resumes from recorded
facts with a new epoch and fresh claims. It never treats compaction text as
authority or skips evidence because a previous agent summarized it as done.

## 6. T3 delegation and workspace binding

T3's documented `create_threads` inherits the current workspace; it is not the
default launcher for coding workers in this feature. `t3_thread_launch` supports
an explicit worktree strategy/base. The harness must bind the correct workspace
before a writable turn starts. A prompt saying "create a worktree" is insufficient.

The documented `delegate_task` input does not itself expose a worktree strategy.
Use it for read-only research/review, or only for coding when the upstream adapter
proves an isolated child binding. Otherwise launch a separate isolated ordinary
thread and record its task lineage in Nornic. Do not silently inherit the parent's
checkout for an editing child. A new review round uses a new delegated task and
stable retry ID for that round.

Track one logical launch intent. T3's documented ordinary thread launch has no
retry key; after a lost launch response reconcile by the adapter's recorded
thread binding before retrying. If a unique binding cannot be proven, mark the
launch unresolved and do not start a second writable worker.

## 7. Communication protocol

Use T3 as the delivery and conversation owner. Return conflict owner fields and
`t3-thread://v1/<threadId>` links resolved within the recorded environment.
Notification envelope:

```json
{
  "protocolVersion": 1,
  "eventId": "evt-unique",
  "kind": "conflict",
  "projectId": "project-nornic",
  "taskId": "task-42",
  "claimId": "claim-17",
  "generation": 3,
  "from": {"environmentId": "env-1", "threadId": "thread-a"},
  "to": {"environmentId": "env-1", "threadId": "thread-b"},
  "body": "I need to change this caller. Will the new signature preserve null handling?"
}
```

The router uses `t3_thread_send` with a stable `clientRequestId` derived from the
event ID and a policy-appropriate queue/steer mode. It retains the T3 receipt.
T3 retry IDs are provider-session-scoped, so after session replacement reconcile
the delivery before retrying. A recipient deduplicates by event ID; delivery is
at-least-once with idempotent handling, not exactly-once human/LLM processing.
Out-of-order messages include the claim/change generation so agents can ignore
obsolete advice after reading current state.

Respect T3's runtime-mode ceilings, active-run and environment permissions. If
the caller cannot send to an owner, return an actionable coordination block or
route through an authorized coordinator; do not invent privileges. An expired
thread/session may be unreachable: use claim expiry/reassignment, not a new
shadow conversation identity. A message saying "take over" is not a handoff;
ownership changes only through `claim.accept` after quiescence.

T3 outage: record bounded pending notification state, pause work that needs an
answer, and retry through one router after recovery. Unrelated tasks may proceed.
Do not duplicate full transcripts in Nornic or poll all threads on every tool call.

## 8. Provider capability qualification

The capability report records pass/fail evidence for identity, isolated launch,
file edits, shell commands, interactive stdin, formatters/generators, dependency
installs, Git writes, MCP mutations, native/T3 delegation, cancellation, lease
loss, restarts, hook trust and evidence capture. Test each provider and supported
OS separately. Upgrading any execution component invalidates its qualification
until the affected suite passes. Unknown does not mean supported.

Supported extensions/configuration are the first implementation route. If a
missing upstream feature proves unavoidable, the milestone must name the exact
upstream change and maintenance boundary for review before implementation.
This spec does not authorize modifying upstream T3 or provider source.
