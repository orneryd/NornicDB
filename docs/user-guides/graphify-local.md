# Local Graphify Code Graph

Graphify indexes a repository with local AST parsing. The graph is loaded into a
NornicDB database, where every function body is embedded and searchable, and the
`/graphify` page of the web UI browses it as a force graph. The VS Code workspace
has an ignored `.vscode/mcp.json` pointing Copilot Chat at the local Graphify
graph; it does not require database credentials.

Install Graphify with its optional Neo4j and streaming-JSON dependencies:

```sh
brew install uv
uv tool install --with neo4j --with ijson 'graphifyy[mcp]'
```

## Load the graph

From the repository root, extract the graph (no LLM or external service) and load
it into the local server:

```sh
"$(uv tool dir --bin)/graphify" extract . --code-only --no-cluster
"$(uv tool dir)/graphifyy/bin/python" scripts/graphify_local.py --graph graphify-out/graph.json
```

Extract from a clean checkout when you can: Graphify indexes every file that is not
ignored by `.gitignore`, so untracked build output such as `ui/dist` would end up in
the graph.

With no options the importer uses the HTTP API at `http://localhost:7474` (`7687` is
the Bolt port; pass a `bolt://` URI to use it), `admin` / `password`, and the
database `nornicdbcode`, which is created when missing. Override with `--uri`,
`--user`, `--password`, `--database`, or `NORNICDB_URI` / `NORNICDB_USER` /
`NORNICDB_PASSWORD` / `NORNICDB_DATABASE`. The repository name stamped on the nodes
comes from the `origin` remote (`--repo`), and the commit and branch from `HEAD`.

This is the same importer the Soraban/code-intelligence GitHub Action runs on every
push (`scripts/graphify_ingest.py` there), with this repository's defaults. Keep the
two in step.

## What is stored

- **Bodies.** Every symbol node gets a full, untruncated `body`: the complete source
  span re-read from the repository files, including the comment block directly above
  the symbol and all comments within it. There are no caps on body size or on the
  number of nodes and edges: the whole codebase is ingested regardless of size.
  NornicDB's managed embedding worker includes every string property in the embedding
  text, so ingested bodies are embedded automatically.
- **`symbol_kind`.** `function`, `class`, `file`, `external` (a symbol the code only
  references, such as a library function, so it has no source file) or `other`.
  Graphify flags callables with private markers for some languages but not Go, so a
  label ending in `()` counts as a function as well.
- **`repo`.** Nodes are keyed by `(id, repo)`.
- **The main entry point.** One node per repository also carries the label `:Main`
  (`cmd/nornicdb/main.go` `main()` by default). Name another with `--main`: a node id,
  `path/to/file:symbol`, or a symbol name that is unique in the repo (methods have a
  leading dot, as in `.run()`). It must be a function or class defined in the code;
  an ambiguous or unknown name fails before anything is written. With `--main ""` the
  default is the non-test function with the most connected nodes, which tends to be a
  utility hub rather than an entry point. `--main none` skips it.
- **`(:CodeRepository {name})`** records the last ingested commit, the counts and the
  main entry. `--check --commit <sha>` reports whether an ingest is needed.

## Re-running is an incremental sync

- new nodes and edges are created;
- existing nodes are rewritten **only when their content hash changes**
  (`props_hash`), so unchanged nodes are never touched and the embedding worker has
  nothing to re-embed on repeat runs. A node's hash includes its source location, so
  inserting lines near the top of a file rewrites the symbols below it. Edges are
  re-written in place by MERGE (they carry no embeddings, so this is cheap);
- stale nodes and edges that disappeared from the graph are deleted, but only nodes
  this importer wrote in this repo. Hand-made nodes and other repositories' data are
  never touched, and a run that would delete more than `--max-delete-fraction` (50%)
  of the repo's graph fails instead, which usually means a truncated extraction;
- transient MVCC conflicts with the embedding worker are retried with bounded
  exponential backoff, and over HTTP each request is split to stay under the server's
  10 MB request limit.

The web UI's "upload graphify artifact" dialog applies the same create/update/delete
semantics to a graph you upload, and `--out-graph enriched.json` writes a copy of the
graph with the bodies embedded for that dialog. The dialog does not set `:Main` or
`symbol_kind`.

## Browse it

Open the `/graphify` route of the web UI. It starts from the `:Main` node (or, when a
database has none, the function with the most connected nodes), walks its call
neighborhood at a configurable depth (default 3), re-roots from any clicked symbol,
runs hybrid RRF search from the search box, and spawns vector-similar symbols as a
pink "similar" arm off any clicked node.

## Tests

```sh
python -m unittest scripts.test_graphify_local scripts.test_graphify_main
```
