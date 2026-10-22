# Inspected sources and compatibility boundary

Inspected for this proposal on 2026-10-22 (America/Phoenix). These references
establish existing integration surfaces; proposed swarm APIs and strict provider
support are not implemented or verified by those references.

## NornicDB release snapshot

Commit: `ba8d6d13b48452ec4e38d826869ad63a64063848`.

| Source | Observed fact / design consequence |
| --- | --- |
| [`AGENTS.md`](../../../AGENTS.md), [`CONTRIBUTING.md`](../../../CONTRIBUTING.md) | Follow release routing, deterministic public-contract evidence and local verification reporting |
| [Standing principles](../../../.agents/standing-design-principles.md) | One canonical owner; use supported third-party integration; do not silently patch upstream |
| [`pkg/nornicdb/plugins.go`](../../../pkg/nornicdb/plugins.go) | Plugin function/procedure declarations and loading already exist |
| [`pkg/nornicdb/plugin_procedure_registration.go`](../../../pkg/nornicdb/plugin_procedure_registration.go) | Context-aware typed procedure handlers and mode/signature registration are available |
| [`pkg/cypher/procedure_registry.go`](../../../pkg/cypher/procedure_registry.go) | Canonical procedure metadata/dispatch; swarm must extend this path |
| [`pkg/heimdall/plugin.go`](../../../pkg/heimdall/plugin.go), [`external_tools.go`](../../../pkg/heimdall/external_tools.go) | Explicit external actions and registration order exist |
| [`pkg/mcp/plugin_tools.go`](../../../pkg/mcp/plugin_tools.go) | Shared native/plugin FIFO registry and unregister behavior exist |
| [External-tool OpenSpec](../heimdall-external-mcp-tools/design.md) | Plugin-only lifecycle/local-inference separation is on the inspected base |
| [`apoc/lock/lock.go`](../../../apoc/lock/lock.go) | Process-local graph locks are not durable session leases; leave their unrelated behavior unchanged |
| [Code-intelligence guide](../../../docs/user-guides/graphify-local.md) | Existing repository/hash-based importer, commit markers, partial-batch caveat and main-ref hooks |
| [`scripts/code_intelligence.py`](../../../scripts/code_intelligence.py), [`graphify_local.py`](../../../scripts/graphify_local.py) | Existing owner to extend for active-release selection and refresh admission |
| [`pkg/config/dbconfig/settings.go`](../../../pkg/config/dbconfig/settings.go), [`pkg/config/config.go`](../../../pkg/config/config.go) | Existing embedding label exclusions and process-restart semantics |
| [Dynamic-memory proposal](../dynamic-memory-soft-limits/design.md) | Related policy design, not evidence of implemented process-wide memory protection |

## Upstream T3 snapshot

Commit: `2c8bdf6177159d8fcd12a897d7c9311f6eb93237` in `pingdotgg/t3code`.

- [Orchestrator MCP server](https://github.com/pingdotgg/t3code/blob/2c8bdf6177159d8fcd12a897d7c9311f6eb93237/docs/orchestration-v2/orchestrator-mcp-server.md):
  authenticated environment/thread/provider-session scope; thread IDs, messaging,
  delegation, worktree-aware launch and retry/permission constraints.
- [Codex provider](https://github.com/pingdotgg/t3code/blob/2c8bdf6177159d8fcd12a897d7c9311f6eb93237/docs/user/providers-codex.md):
  native Codex setup and account/session behavior.
- [ACP providers](https://github.com/pingdotgg/t3code/blob/2c8bdf6177159d8fcd12a897d7c9311f6eb93237/docs/user/providers-acp.md):
  local ACP commands, agent-owned credentials/tools and permission handling.

These documents do not prove that third-party MCP services can validate T3's
internal session token, or that every provider action passes a blocking T3 hook.
The identity and execution qualification milestone must establish supported
end-to-end mechanisms before strict integration can ship.

## Provider documentation

- [Copilot CLI ACP server](https://docs.github.com/en/copilot/reference/copilot-cli-reference/acp-server):
  ACP launch, tool filters and supported BYOK configuration. ACP is documented as
  public preview; pin and test the installed version.
- [Copilot SDK pre-tool hook](https://github.com/github/copilot-sdk/blob/main/docs/hooks/pre-tool-use.md):
  an SDK hook surface; do not assume identical exposure through ACP.
- [Codex hooks](https://learn.chatgpt.com/docs/hooks): lifecycle/tool hooks, trust
  requirements and coverage limitations, including interactive execution.
- [Codex app-server](https://learn.chatgpt.com/docs/app-server): session/turn and
  approval integration; approval callbacks are not a universal write boundary.

Documentation URLs may evolve. Record installed versions, schemas and actual
qualification results in implementation evidence. This PR uses no model API
credentials and runs no live provider sessions.

## Connected Linear workflow inspection

Read-only inspection on 2026-10-22 covered the NOR / Nornicdb team,
**v1.4.4 — Stabilization & release** project, live issue states, label groups,
project milestones, work register and current release-work examples.
[linear.md](linear.md) records the sanitized mapping and its limitations.

Observed states are Backlog, Todo, Dev In Progress, Dev Complete, Test in Progress,
Test Complete, Merged, Released, Canceled and Duplicate. Delivery labels separate
develop-only work, an open release PR, merged pending validation and release
verification. Neither Test Complete nor Release verified proves shipment.

Live issues and current commit evidence supersede historical register snapshots.
Existing reported validation is contextual evidence, not a test rerun by this PR.
Branch-specific GitHub status automation and supported agent delegation remain
installation qualifications. No issue content, private discussion, account IDs
or credentials are reproduced; no Linear objects were changed.
