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
