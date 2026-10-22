## ADDED Requirements

### Requirement: Explicit public exposure
WHEN an action sets External true THEN MCP discovery and invocation expose it
with its description and schema, or the existing default schema if absent.
WHEN an action omits External THEN it remains available internally and is absent
from the external MCP surface.

### Requirement: FIFO collision ownership
WHEN two registrations produce the same name THEN registration logs the failure
and retains the first definition and handler. Built-in MCP tools register first;
plugin actions follow successful registration order with sorted keys per plugin.

### Requirement: Existing execution context
WHEN an external action executes THEN its arguments, request context and RBAC
fields reach its handler, and its initialized HeimdallInvoker remains usable for
inference through the configured provider, including llama.cpp.


### Requirement: Plugins independent of the local inference backend
WHEN a plugin imports the Heimdall contract THEN its dependency graph excludes
localllm and it can compile without native inference libraries. The same plugin
can load in compatible hosts with or without the nolocalllm tag.

### Requirement: Plugin-only installation
WHEN configured plugin directories exist and Heimdall is disabled THEN the
server initializes/starts plugins and exposes their external tools without
loading a model or creating the Bifrost chat handler. Direct action/database
access remains available; prompt inference reports unavailable. A failed model
initialization does not prevent this plugin-only path.

### Requirement: Plugin export lifecycle
WHEN plugin Start fails THEN its registration/actions are removed and Shutdown
cleans up the initialized plugin before MCP publication. WHEN the manager shuts
down THEN owned external tools disappear from discovery and future calls, FIFO
order resets, and native/collision-winning tools owned elsewhere remain intact.
WHEN the server stops THEN its initialized plugins receive Stop and Shutdown
exactly once, including with the harness disabled.

### Requirement: Standalone MCP draining
WHEN MCP Stop drains an in-flight list or call request THEN registry operations
can finish without waiting for the shutdown owner's mutex.
