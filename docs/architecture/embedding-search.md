# Embedding Search

**How NornicDB stores and searches embeddings across HTTP, Cypher, and Qdrant gRPC.**

This section documents the *current* embedding data model and the two vector-search execution paths that exist today:

- **`search.Service` (indexed)**: Used by the HTTP `/nornicdb/search` endpoint and the Qdrant gRPC compatibility layer.
- **Cypher `db.index.vector.queryNodes` (scan)**: Neo4j-compatible procedure that currently scans nodes in storage and computes similarity in-process.

## Key Concepts

### Two embedding fields, two purposes

- **`Node.ChunkEmbeddings`** (`[][]float32`): NornicDB-managed embeddings and chunked document embeddings (the first chunk is the “main” node embedding).
- **`Node.NamedEmbeddings`** (`map[string][]float32`): Client-managed vectors (notably Qdrant vectors via gRPC), keyed by vector name (`"default"` for unnamed).

These fields are intentionally separate so “managed embeddings” and “client vectors” don’t overwrite each other.

### Indexing behavior (search.Service)

`search.Service.IndexNode()` indexes *both* embedding types:

- Each named embedding gets its own vector entry
- Chunk embeddings are indexed under `nodeID` (main) plus one entry per later chunk. Entry keys come from `pkg/search/vector_entry_key.go` and always name exactly one node.

## Documentation

- **Architecture**: `docs/architecture/embedding-search-architecture.md`
- **Flow diagrams (Mermaid)**: `docs/architecture/embedding-search-flow-diagrams.md`
- **Examples**: `docs/architecture/embedding-search-examples.md`

## Related docs

- Vector search usage: `docs/user-guides/vector-search.md`
- Qdrant gRPC usage: `docs/user-guides/qdrant-grpc.md`
- Embedding generation: `docs/features/vector-embeddings.md`
- Qdrant gRPC internals: `pkg/qdrantgrpc/README.md`

