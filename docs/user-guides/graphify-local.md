# Local Graphify Code Graph

Graphify indexes the repository with local AST parsing and stores the queryable
graph in `graphify-out/` (ignored by Git). The graph is also imported into the
local NornicDB server through its Neo4j Bolt endpoint at `127.0.0.1:7687`.
The VS Code workspace has an ignored `.vscode/mcp.json` pointing Copilot Chat
at the local Graphify graph; it does not require database credentials.

Install the official Graphify tool with its optional Neo4j, streaming JSON,
and MCP dependencies:

```sh
brew install uv
uv tool install --with neo4j --with ijson 'graphifyy[mcp]'
```

From the repository root, build and query the code graph without an LLM or
external service:

```sh
"$(uv tool dir --bin)/graphify" extract . --code-only --no-cluster
"$(uv tool dir --bin)/graphify" query "which query routers call the same helpers?"
"$(uv tool dir --bin)/graphify" path "StorageExecutor" "Execute"
```

To refresh the graph after changing code, run `graphify update . --no-cluster`
(using `"$(uv tool dir --bin)/graphify"` if it is not on `PATH`). Then push the
updated graph to NornicDB. The import is idempotent and accepts `--batch-size`:

```sh
"$(uv tool dir)/graphifyy/bin/python" \
  scripts/graphify_local.py --graph graphify-out/graph.json
```

The importer targets `bolt://localhost:7687` (the Bolt port; `7474` is the
HTTP/UI port) with `admin` / `password` by default (override with `--uri`,
`--user`, `--password`, or `NEO4J_PASSWORD`). The graph is imported into its
own `nornicdbcode` database by default (`--database` to change), created
automatically when missing. Every symbol node also receives a full,
untruncated `body` property: the complete source span re-read from the
repository files, including the comment block directly above the symbol and
all comments within it. There are no caps on body size or on the number of
nodes and edges imported — the whole codebase is ingested regardless of size.
NornicDB's managed embedding worker includes every string property in the
embedding text, so ingested bodies are automatically embedded and searchable
through the vector search APIs.

Re-running the importer is an incremental sync, not a re-import:

- new nodes and edges are created;
- existing nodes are rewritten **only when their content hash changes**
  (`props_hash`), so unchanged nodes are never touched and the embedding
  worker has nothing to re-embed on repeat runs; edges are re-written in
  place by MERGE (they carry no embeddings, so this is cheap);
- stale nodes and edges that disappeared from the graph are deleted, scoped
  to importer-managed labels so unrelated data in the database is never
  touched;
- transient MVCC conflicts with the embedding worker are retried with
  bounded exponential backoff.

The web UI's "upload graphify artifact" dialog applies the same semantics:
choose an existing database name and check "update existing database" to sync
it in place (create/update/delete) instead of creating a new one.

Browse the result at the `/graphify` route of the web UI: it loads the tree
rooted at `cmd/nornicdb/main.go` `main()` at a configurable depth (default 3),
walks the call neighborhood outward, re-roots from any clicked symbol, runs
hybrid RRF search from the search box, and spawns vector-similar symbols as a
pink "similar" arm off any clicked node.

The importer matches Graphify's node IDs, labels, relationship types, and
scalar properties, including implicit edge endpoints. It reads the `links`
array emitted by Graphify >=0.9.69 (`edges` fallback for older graphs), creates
per-label `id` indexes before the first write so MERGE lookups never scan, and
groups writes into bounded `UNWIND` batches (default 2000) because Graphify's
stock Neo4j exporter sends one Bolt query per node and edge. Re-running
`MERGE` updates existing records. Only code is indexed: docs and PDFs are
skipped, so no LLM/API key is required.