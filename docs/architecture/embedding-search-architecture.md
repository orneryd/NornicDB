# Embedding Search Architecture

**Current embedding storage model and search execution paths in NornicDB.**

This document is architecture-focused (what exists today in code), and links out to user-facing guides for “how to use it”.

## Related docs

- Entry point: `docs/architecture/embedding-search.md`
- Flow diagrams: `docs/architecture/embedding-search-flow-diagrams.md`
- Examples: `docs/architecture/embedding-search-examples.md`
- Vector search usage: `docs/user-guides/vector-search.md`
- Qdrant gRPC usage: `docs/user-guides/qdrant-grpc.md`

## Data model: where embeddings live

Embeddings are stored on `storage.Node` in struct fields (not in `node.Properties`):

- `Node.ChunkEmbeddings [][]float32`
  - Used for NornicDB-managed embeddings and chunked document embeddings
  - Convention: `ChunkEmbeddings[0]` is the “main” node embedding
  - Additional chunks are stored at `[1..N]` for long documents
- `Node.NamedEmbeddings map[string][]float32`
  - Used for client-managed vectors keyed by name (notably Qdrant gRPC vectors)
  - Convention: the unnamed Qdrant vector is stored under key `"default"`

Property vectors still exist (and are useful for Cypher compatibility):

- `node.Properties[propertyKey]` may contain a vector array (e.g. `n.embedding = [..]`)

## Two vector-search execution paths

NornicDB has two distinct vector-search entrypoints that share the same core service,
but intentionally preserve different semantics.

### 1) Indexed search (`search.Service.Search` / `VectorSearchCandidates`)

Used by:

- HTTP `/nornicdb/search` (hybrid search / RRF)
- Qdrant gRPC search endpoints (compatibility layer)
- MCP flows that call `DB.HybridSearch()`

Implementation:

- Core: `pkg/search/search.go`
- Indexing: `(*search.Service).IndexNode()`
- **Result cache:** `Search()` results are cached by query + options (LRU, TTL 5m); cache is invalidated on `IndexNode`/`RemoveNode`. All call paths (HTTP, Cypher, MCP) share this cache so repeated identical searches are fast.

Index entry IDs:

- Chunk embeddings: `ChunkEmbeddings[0]` is indexed under the node ID itself (the main entry); each later chunk gets its own entry.
- Each named embedding and each vector-shaped property gets its own entry.
- Entry keys are built and decoded only by `pkg/search/vector_entry_key.go`. The encoding is injective: an entry key always names exactly one node, whatever characters the node ID, vector name or property key contain, so a node's entries can never collide with another node's. Search decodes each result's owner once, when it collapses entries to nodes; nothing parses IDs.
- Persisted vector stores and ANN graphs record the key format. A store saved with an older format is rebuilt from storage on startup.

This means **NamedEmbeddings are indexed** (they are not “storage-scan only”).

### 2) Cypher vector procedure (`db.index.vector.queryNodes`)

Used by:

- Cypher procedure `CALL db.index.vector.queryNodes(...)`

Implementation:

- Cypher layer: `pkg/cypher/call_vector.go` (`callDbIndexVectorQueryNodes`)
- Core: `pkg/search/vector_query_spec.go` (`(*search.Service).VectorQueryNodes`)

Important behavior:

- Uses Cypher vector index metadata (label/property/similarity) but executes through `search.Service`
- Today, `VectorQueryNodes` is intentionally scan-based to preserve Cypher semantics (embedding precedence + filters)

Embedding selection order (per node):

1. `node.NamedEmbeddings[index.property]` (or `"default"` when the index has no property)
2. `node.Properties[index.property]` if it contains a vector array
3. `node.ChunkEmbeddings[0..N]`

This path exists for Neo4j compatibility, but it is inherently O(N) over nodes in storage.
The design keeps this complexity isolated behind `search.Service`, so future optimizations can be made
without changing the Cypher interface.

## Qdrant gRPC mapping (collections and points)

The Qdrant gRPC layer stores points as nodes:

- Qdrant **collection** → NornicDB **database** (namespace).
  - Storage isolation is done via `multidb.DatabaseManager` + `storage.NamespacedEngine`.
  - The Qdrant gRPC layer opens a namespaced engine for every request: `dbManager.GetStorage(collectionName)`.
  - Collection metadata is persisted inside that database as a required node:
    - Node ID: `_collection_meta`
    - Label: `_CollectionMeta`
    - Properties: `{dimensions: int, distance: int32(qdrant.Distance), schema_version: 1}`
  - Implementation: `pkg/qdrantgrpc/collection_store.go`
- Qdrant **point** → a node inside the collection/database namespace:
  - Node ID: `qdrant:point:<raw-id>` (collection name is not embedded; namespace already scopes it)
  - Labels: `QdrantPoint`, `Point`
  - Payload → `node.Properties` (with internal `_qdrant_*` keys stripped from API output)
  - Vectors → `Node.NamedEmbeddings` (single unnamed vector stored as `"default"`)
  - Implementation: `pkg/qdrantgrpc/points_service.go`

Qdrant vectors do not overwrite NornicDB-managed embeddings because managed embeddings are stored in `ChunkEmbeddings`.

### Drop behavior (why DROP DATABASE is fast)

Because collections are databases, deleting a collection is a database drop:

- `DROP DATABASE <name>` / `DatabaseManager.DropDatabase(name)` deletes the namespace prefix `<name>:` via `storage.Engine.DeleteByPrefix`.
- For Badger-backed engines, `DeleteByPrefix` is optimized to use Badger's prefix drop for db-scoped keyspaces and targeted cleanup for secondary indexes.

## Why this split matters

Separating `NamedEmbeddings` and `ChunkEmbeddings` enables:

- **No clobbering**: managed embeddings and client vectors can coexist on a node
- **Clear ownership**: Qdrant gRPC owns `NamedEmbeddings`, embedding pipeline owns `ChunkEmbeddings`
- **Flexible query semantics**:
  - Cypher can stay Neo4j-compatible via property vectors and scans
  - HTTP/gRPC can stay fast via the indexed `search.Service` path
