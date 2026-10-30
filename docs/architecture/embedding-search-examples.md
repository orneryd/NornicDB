# Embedding Search Examples

**Examples that show how embedding search works end-to-end, and which embedding field each API uses.**

For full end-user docs, see:

- Vector search guide: `docs/user-guides/vector-search.md`
- Qdrant gRPC guide: `docs/user-guides/qdrant-grpc.md`

## 1) HTTP hybrid search (search.Service)

The HTTP search endpoint runs hybrid search (vector + BM25 with RRF fusion):

```bash
curl -X POST http://localhost:7474/nornicdb/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "machine learning algorithms",
    "limit": 10
  }'
```

Notes:

- Query text is embedded server-side (when embeddings are enabled).
- Results come from the indexed search path in `pkg/search/search.go`.

## 2) Cypher vector search (db.index.vector.queryNodes)

Cypher’s Neo4j-compatible vector procedure currently scans nodes in storage and computes similarity:

- Implementation: `pkg/cypher/call_vector.go`
- Storage access: `storage.Engine.AllNodes()`

### String query (server-side embedding)

```cypher
CALL db.index.vector.queryNodes('embeddings', 10, 'machine learning algorithms')
YIELD node, score
RETURN node, score
ORDER BY score DESC
```

### Vector array query (client-side embedding)

```cypher
CALL db.index.vector.queryNodes('embeddings', 10, [0.1, 0.2, 0.3, 0.4])
YIELD node, score
RETURN node, score
ORDER BY score DESC
```

### Property vectors vs. internal embeddings

`db.index.vector.queryNodes` chooses embeddings in this order:

1. `node.NamedEmbeddings[index.property]` (or `"default"` when no property is configured)
2. `node.Properties[index.property]` (if it contains a vector array)
3. `node.ChunkEmbeddings[0..N]` (best score across chunks)

That means **Cypher can always work with property vectors**, even if you never use `NamedEmbeddings`.

## 3) Qdrant gRPC named vectors (NamedEmbeddings)

The Qdrant gRPC endpoint stores vectors in `Node.NamedEmbeddings`:

- Unnamed vector → `NamedEmbeddings["default"]`
- Named vectors → `NamedEmbeddings[name]`

Example (Python `qdrant-client`, gRPC):

```python
from qdrant_client import QdrantClient
from qdrant_client.http import models as m

client = QdrantClient(host="127.0.0.1", grpc_port=6334, prefer_grpc=True)

client.create_collection(
    collection_name="docs",
    vectors_config={
        "title": m.VectorParams(size=128, distance=m.Distance.COSINE),
        "content": m.VectorParams(size=128, distance=m.Distance.COSINE),
    },
)

client.upsert(
    collection_name="docs",
    points=[
        m.PointStruct(
            id="doc1",
            vector={"title": [1.0] * 128, "content": [0.2] * 128},
            payload={"kind": "article"},
        )
    ],
)

results = client.search(
    collection_name="docs",
    query_vector=("title", [1.0] * 128),
    limit=5,
)
print([r.id for r in results])
```

## 4) Graph + vector together (Cypher)

Vector similarity plus graph traversal is the “hybrid” capability you don’t get in a pure vector DB:

```cypher
CALL db.index.vector.queryNodes('embeddings', 10, 'neural networks')
YIELD node, score
MATCH (node)-[:CITES]->(cited:Paper)
RETURN node, cited, score
ORDER BY score DESC
```

## 5) Mapping summary

- **Managed embeddings / chunked docs** → `Node.ChunkEmbeddings`
- **Qdrant vectors / named vectors** → `Node.NamedEmbeddings`
- **Cypher property vectors** → `node.Properties[index.property]`

