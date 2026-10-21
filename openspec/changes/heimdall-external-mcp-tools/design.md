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
