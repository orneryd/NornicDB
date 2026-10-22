# Agentic coding and swarm coordination

## Why

Codex and Copilot workers need to coordinate before editing overlapping code,
complete the development workflow without repeated human intervention, and leave
a verified PR ready for maintainer review. A shared release-branch code graph
must make this possible on local hardware without embedding each worktree.

## Approved product contract

- Integrate with upstream `pingdotgg/t3code`; target Codex and Copilot CLI.
  VS Code Copilot is a separate client, not implicitly a T3 provider.
- Default to separate Git worktrees. Reuse compatible dependencies from the
  original tooling checkout or shared cache without making source trees writable
  through dependency links. Enumerate exceptional shared-workspace cases.
- Work autonomously through investigation, specification, implementation,
  deterministic tests, documentation, measured performance tuning, agent review,
  rework, and creation of a review-ready PR. Merge, tagging and deployment are
  outside this autonomy boundary.
- Enforce phase prerequisites and current ownership before mutation, including
  command tools and delegated workers. Prompt instructions alone are insufficient.
- Release edit claims when a revision is frozen for review; retain pending-change
  visibility. Reacquire claims for rework. Expire abandoned ownership safely.
- Reuse T3 identity and messaging. Nornic owns coordination facts, not a second
  conversation system.
- Integrate the existing NOR Linear release project: acceptance-driven intake,
  its actual development/test statuses, verified assignee mapping, separate
  develop/release PR evidence and non-closing links. Preserve human-owned scope.
- Maintain one canonical code corpus for the active release branch in one
  configured Nornic database. No full worktree ingestion, per-agent database,
  per-agent vector index, or per-agent embedding model instance.
- APOC and Heimdall/MCP adapters expose one shared workflow service, usable with
  local inference disabled and without the `localllm` dependency.

## What changes

This PR specifies the feature; **it delivers no runtime implementation**.
The proposed extension namespace is `apoc.swarm.*`. Mutations are procedures;
pure resource-key helpers are functions. The contract includes identity,
projects, tasks, plans, evidence, workspaces, code-index readiness, claims,
execution permits, handoffs, T3 notifications, review and PR receipts.

Read in this order:

1. [Design and lifecycle](design.md)
2. [Complete API contract](api.md)
3. [T3, Codex and Copilot integration](harness.md)
4. [Concrete Linear workflow integration](linear.md)
5. [Shared code graph and local resource policy](code-intelligence.md)
6. [Observable requirements](specs/swarm-workflow/spec.md)
7. [Acceptance and performance verification](verification.md)
8. [Implementation milestones](tasks.md)
9. [Inspected source inventory](sources.md)

## Capabilities

### New capabilities

- `swarm-workflow`: durable, phase-aware coordination for agentic coding with
  isolated workspaces and a single shared release-code corpus.

### Modified capabilities

None of the existing OpenSpec capability definitions are replaced. Implementation
extends existing plugin registration, authentication, code-intelligence ingestion
and embedding policy owners only where this contract requires it.

## Status, base and dependencies

Planning base: `release/v1.4.4@ba8d6d13b48452ec4e38d826869ad63a64063848`.
No issue ID was supplied. This is a Nornic extension, not a Neo4j APOC parity
claim or openCypher language change. The Cypher convergence plan is relevant
only for shared procedure dispatch, authorization, cancellation and transaction
semantics; no new parsing/execution route is proposed.

Already present on the inspected release base:

- Typed plugin procedure registration and procedure modes.
- Heimdall opt-in external MCP actions, FIFO collision handling, and plugin-only
  startup independently of local inference.
- Repository-scoped, content-hash-based code ingestion and existing embedding
  policies. The local update hooks currently track `main`, not this proposal's
  configurable active release ref.

Still to implement: every swarm API and workflow below, provider adapters,
execution enforcement, Linear intake/projection, single-writer release refresh,
unembedded worktree deltas,
and their tests. T3 and provider behavior must be version-qualified; inspected
documentation is not an end-to-end enforcement test.

## Non-goals

No T3 fork, new chat service, automatic merge/release, default binary model
bundling, replacement of T3 persistence, unbounded graph locking, or wholesale
refactor of existing APOC locks. Existing `apoc.lock.*` is not a durable agent
lease service. This planning PR changes no third-party source or runtime defaults.
