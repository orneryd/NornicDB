# Public MCP tools from Heimdall plugins

Add an opt-in `external: true` action contract to the existing Heimdall plugin
surface on release/v1.4.4. Publish those actions beside native MCP tools, retaining
FIFO registration and logging collisions without replacing the first tool.
Default examples remain internal. No issue ID was supplied; this work does not
change Cypher convergence semantics.
