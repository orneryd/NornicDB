# Design

ActionFunc owns the opt-in flag. SubsystemManager records successful action
registration order, sorting each action map and rejecting final-name collisions.
The MCP server owns the combined tool registry and synchronizes registration,
discovery, and handler lookup. Server initialization bridges public actions to
that registry after plugin loading. Handlers retain initialized inference access
and receive the current request's context and RBAC data.

Built-in MCP definitions remain first. All Heimdall actions remain available for
internal inference. No default plugin action changes visibility. Plugin database
operations continue through existing authorization-aware routers. No query or
inference hot path optimization is claimed; no performance comparison is available.


The local generator implementations now live in `pkg/heimdall/local`. The server
imports this optional backend; plugin authors import `pkg/heimdall`, whose
transitive dependencies exclude localllm. This removes the nolocalllm-dependent
plugin package hash rather than asking users to match a local-inference tag.
Go toolchain, architecture, CGO and other shared dependency settings still match.
Server startup supplies a plugin-only context when the harness is disabled or
model initialization fails. It has a no-op Bifrost bridge and a live action invoker
without a generator; prompt methods truthfully report that inference is absent.
Plugin initialization/start and external tool registration use one shared path.

The subsystem manager owns action removal and notifies MCP consumers outside its
lock. Each loaded plugin records only its successful action registrations, so a
rollback cannot remove a collision winner owned by another plugin. Shutdown clears
FIFO state and retires export-removal hooks before plugin shutdown callbacks.
The MCP registry removes plugin tools from both discovery and invocation, preserving
native tools. Standalone MCP Stop snapshots its HTTP server under its mutex and
drains requests after releasing that mutex.
Server-owned plugin cleanup runs after HTTP draining, retains the database router
until plugin Stop/Shutdown complete, and serializes against asynchronous startup.
