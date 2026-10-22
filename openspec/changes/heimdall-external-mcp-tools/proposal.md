# Public MCP tools from Heimdall plugins

Add an opt-in `external: true` action contract to the existing Heimdall plugin
surface on release/v1.4.4. Publish those actions beside native MCP tools, retaining
FIFO registration and logging collisions without replacing the first tool.
Default examples remain internal. No issue ID was supplied.

The user also requested fixing the known base failure in
`TestListInputHTTPRepresentations` in this PR. Port the reference-backed
`apoc.map.fromPairs` correction from release commit `ed8c8831`, with the
shared pinned value/error oracle and embedded, HTTP and Bolt regressions.
This includes native constructor coercion and signature checks; unrelated
traversal and vector-catalog changes from that release commit remain outside scope.
