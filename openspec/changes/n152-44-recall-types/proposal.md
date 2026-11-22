# Filter MCP recall on every requested type (N152-44 / NOR-100)

## Required outcome

MCP recall must return the union of every supplied node type label before applying the limit, instead of filtering on `nodeTypes[0]`. The advertised discover depth description must state the observed hop semantics: depth 1 publishes no related nodes, depth 2 one hop and depth 3 two hops.

## Scope

Correct the canonical `handleRecall` type predicate to reuse `containsLabel` across all requested labels. Preserve the existing case-sensitive label comparison, argument coercion, direct-ID behavior, database scope and eligibility-before-limit order. Clarify the discover depth prose in the tools/list schema without changing its 1-3 bounds, default or traversal behavior. The copied public-path regression covers both parsers over JSON-RPC, REST and default-database-pinned REST; no new parser or malformed-input policy is introduced.

## Evidence

The reviewed fixture has two Memory nodes, one Task, one Decision and one unrequested Noise node. Before the fix, 24 leaf cases (2 parsers × 3 transports × 4 requests) failed identically: `[Memory,Task]` returned 2 instead of 3, `[Memory,Task,Decision]` and the reversed order returned only the first requested type, and `[Bogus,Memory]` returned 0 instead of 2. Single/empty/absent/unknown-only controls, limit eligibility, node IDs, Noise exclusion, depth clamps and schema bounds passed before and after. After the fix the same 24 cases pass under both parsers and all three transports, and the advertised description reports `1 publishes no related nodes, 2 publishes one hop, and 3 publishes two hops`.

The fixture targets the default database; the pinned route covers the default-database pin only. Depth coverage uses the keyword fixture. No full-suite, race, stress or performance qualification is included in this change, and the duplicate-checker evidence is limited by the incomplete code-graph retrieval recorded in the task review.
