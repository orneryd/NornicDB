# Native Cypher over MCP

The `cypher` tool runs a native Cypher statement with the authenticated caller's
database and statement permissions. Traversal, APOC, GDS and retrieval procedures
use the same native executor as other query transports.

Native Cypher requires an authenticated principal, database scope, permission
resolver and configured database-scoped executor on every call, including calls
made in process. It is unavailable with authentication disabled. The standalone
MCP HTTP surfaces (`Start`, `RegisterRoutes`, `ServeHTTP`) leave it disabled;
they have no native Cypher authentication model. A host integration may enable
`NativeCypherEnabled` only with those authenticated request dependencies. Missing
context is an MCP error and never selects an anonymous/default administrator
executor. The authenticated product integration supplies these dependencies.

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
bound values. Input JSON integer lexemes retain their precision and native
INTEGER type on the server. Output integers are JSON numbers: clients that decode
all numbers as IEEE-754 doubles can lose integers outside their exact range.
Select `toString(value)` when such a client needs an exact large integer string.
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
selected values using the native JSON scalar codec, including identifiers,
properties, scores, evidence, passages and method/ranking information.
Temporal/duration values use native strings and byte arrays use signed integer
arrays, recursively inside maps, lists and entity properties.

`max_rows` defaults to 100 and accepts integers from 1 through 1000. The result
complete serialized MCP HTTP response is bounded to 1 MiB, including the
JSON-RPC/direct envelope, text escaping and `structuredContent` duplication.
Bounds omit whole rows, preserving complete selected
values; `truncation.reasons` reports `max_rows`, `max_bytes`, and/or
`database_max_results` when the native database result limit cut rows. A row too
large to fit is omitted with `max_bytes`, even if that leaves no returned rows.
Oversized columns/metadata and non-JSON values are errors. Long error messages
carry an explicit truncation suffix to keep error responses within the same cap.
Final rows go through the existing native result stream as they are produced;
bounded requests bypass the native result cache. Plain reads stop once their
output budget is full. Writes and procedure calls finish and validate their
remaining effects while excess output is discarded. Native operations requiring
materialization also use the existing transaction-memory accounting with a
16 MiB request ceiling, or a stricter configured database limit; oversized lists,
sorts and other tracked intermediate state fail as native memory-limit errors.
Query timeout, rate, concurrency and configured memory limits still apply.
Writes retain native transaction semantics;
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
