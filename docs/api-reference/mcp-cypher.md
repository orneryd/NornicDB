# Native Cypher over MCP

The `cypher` tool runs a native Cypher statement with the authenticated caller's
database and statement permissions. Traversal, APOC, GDS and retrieval procedures
use the same native executor as other query transports.

Send `tools/call` to `/mcp` (JSON-RPC), or send the tool request directly to
`/mcp/tools/call`:

```json
{
  "name": "cypher",
  "arguments": {
    "database": "nornic",
    "query": "MATCH (n:Document) WHERE n.topic = $topic RETURN elementId(n) AS id, n.content AS content",
    "parameters": {"topic": "graphs"},
    "max_rows": 100
  }
}
```

`query` is required. `parameters` is an optional object; nested maps/lists remain
bound values. JSON integers retain their precision and native INTEGER type.
`database` uses the existing MCP selection rules, including configured defaults
and authorized aliases. A database-pinned URL such as `/mcp/nornic` overrides the
payload's database. A query's native `USE` and nested statements still undergo
database authorization. Read/write/schema/admin and procedure permissions are
decided by the native executor; the tool has no mode override.

Success includes the same JSON object in text `content` and `structuredContent`:

```json
{
  "columns": ["id", "content"],
  "rows": [["document-id", "Full selected content"]],
  "stats": null,
  "metadata": null,
  "truncation": {
    "truncated": false,
    "reasons": [],
    "returned_rows": 1,
    "max_rows": 100,
    "max_bytes": 1048576
  }
}
```

`stats` contains the native counters when present. `metadata` retains native
plans, mutation receipts and retrieval outcomes when present. Rows preserve all
selected values using their native JSON representation, including identifiers,
properties, scores, evidence, passages and method/ranking information.

`max_rows` defaults to 100 and accepts integers from 1 through 1000. The result
JSON is also bounded to 1 MiB. Bounds omit whole rows, preserving complete selected
values; `truncation.reasons` reports `max_rows`, `max_bytes`, and/or
`database_max_results` when the native database result limit cut rows. A row too
large to fit is omitted with `max_bytes`, even if that leaves no returned rows.
Oversized columns/metadata and non-JSON values are errors. These are output
bounds: execution still uses native query, memory, rate and concurrency limits.
Writes finish under native transaction semantics before output is bounded;
truncated output is not a reason to retry a write.

An execution or serialization failure returns MCP `isError: true` and its error
text in `content`. Empty successful query results have `rows: []` and
`isError` absent. Request cancellation reaches native execution and its providers;
database execution limits continue to apply. Disconnecting the HTTP request
cancels its work. The tool does not introduce a cancellation or cursor registry.

## Native retrieval and continuation

All retrieval options belong to the native procedure. For example:

```json
{
  "name": "cypher",
  "arguments": {
    "query": "CALL db.retrieve($request)",
    "parameters": {
      "request": {
        "query": "graph search",
        "labels": ["Document"],
        "where": "node.allowed = $allowed",
        "failClosed": true,
        "n": 10
      },
      "allowed": true
    }
  }
}
```

The native `where` predicate uses statement parameters and checks eligibility
before the limit, including later continuation pages. `db.rretrieve`,
`db.rerank`, `db.retrieveRelationships` and `db.retrieveGraph` are callable the
same way with their own options and outcomes. Native failClosed errors remain
MCP errors, including unavailable or failed query embeddings.

When a native operation returns a `qid`, call that operation with
`parameters: {"request": {"qid": "returned-qid", "n": 10}}` to continue, or
`{"request": {"qid": "returned-qid", "discard": true}}` to release it. The
native principal and database own the continuation. Page fields, evidence,
scores and outcomes pass through intact. Relationship retrieval does not support
continuation; graph retrieval keeps its native terminal-page restrictions.

There is no generic Cypher cursor. `max_rows` counts Cypher rows, including a
native page as one row; the byte bound also applies to that page. An MCP-truncated
row/page cannot be recovered using a native `qid` that has already advanced.
Use smaller native `n` values or a smaller query projection to keep pages within
the output bounds. Truncation never implies resumability.
