# Qdrant gRPC Endpoint

NornicDB can expose a **Qdrant-compatible gRPC API** so you can use **existing Qdrant SDKs** (multi-language) against NornicDB without rewriting your client code.

This endpoint is intended for:

- high-throughput **vector ingest** (upsert/delete/update vectors)
- low-latency **vector search** (SearchPoints / Query / Scroll / Count)
- “Qdrant client compatibility” integrations (migration / parity testing / drop-in usage)

> NornicDB does **not** implement the Qdrant REST API. This feature is **gRPC only** (default port `6334`).

---

## Enable the endpoint

### Environment variables

```bash
export NORNICDB_QDRANT_GRPC_ENABLED=true
export NORNICDB_QDRANT_GRPC_LISTEN_ADDR=":6334"   # optional (default :6334)
export NORNICDB_QDRANT_GRPC_TLS_ENABLED=true
export NORNICDB_QDRANT_GRPC_TLS_CERT=/tls/grpc.crt
export NORNICDB_QDRANT_GRPC_TLS_KEY=/tls/grpc.key
```

### YAML config

```yaml
features:
  qdrant_grpc_enabled: true
  qdrant_grpc_listen_addr: ":6334"
  qdrant_grpc_max_vector_dim: 4096
  qdrant_grpc_max_batch_points: 1000
  qdrant_grpc_max_top_k: 1000
  qdrant_grpc_tls_enabled: true
  qdrant_grpc_tls_cert: "/tls/grpc.crt"
  qdrant_grpc_tls_key: "/tls/grpc.key"
```

### Docker / Compose

Expose the gRPC port:

```yaml
ports:
  - "7474:7474" # HTTP
  - "7687:7687" # Bolt
  - "6334:6334" # Qdrant gRPC
environment:
  - NORNICDB_QDRANT_GRPC_ENABLED=${NORNICDB_QDRANT_GRPC_ENABLED:-false}
  - NORNICDB_QDRANT_GRPC_LISTEN_ADDR=${NORNICDB_QDRANT_GRPC_LISTEN_ADDR:-0.0.0.0:6334}
  - NORNICDB_QDRANT_GRPC_TLS_ENABLED=${NORNICDB_QDRANT_GRPC_TLS_ENABLED:-false}
  - NORNICDB_QDRANT_GRPC_TLS_CERT=${NORNICDB_QDRANT_GRPC_TLS_CERT:-}
  - NORNICDB_QDRANT_GRPC_TLS_KEY=${NORNICDB_QDRANT_GRPC_TLS_KEY:-}
  - NORNICDB_QDRANT_GRPC_TLS_CLIENT_CA=${NORNICDB_QDRANT_GRPC_TLS_CLIENT_CA:-}
  - NORNICDB_QDRANT_GRPC_TLS_CLIENT_AUTH_MODE=${NORNICDB_QDRANT_GRPC_TLS_CLIENT_AUTH_MODE:-none}
```

The shipped Compose files also forward the vector-dimension, batch-size, and
top-k limits and TLS/mTLS settings. Mount certificate files into the container
when using native TLS. For secure deployment topologies, see
[Qdrant gRPC reverse proxying](../operations/reverse-proxy.md#qdrant-grpc).

## TLS, mTLS, and application authentication

Authenticated public listeners require native TLS. Clients must trust the CA
that issued `NORNICDB_QDRANT_GRPC_TLS_CERT` and connect using TLS on port 6334.
To require client certificates as well, set:

```bash
export NORNICDB_QDRANT_GRPC_TLS_CLIENT_CA=/tls/client-ca.crt
export NORNICDB_QDRANT_GRPC_TLS_CLIENT_AUTH_MODE=require_verify
```

Supported client-auth modes are `none`, `request`, `request_verify`, and
`require_verify`. The verifying modes require a client CA. With NornicDB
authentication enabled, transport authentication is additive: clients must
still send `authorization: Basic ...`, `authorization: Bearer ...`, or
`x-api-key` metadata. NornicDB then applies the configured gRPC RBAC policy.

Plaintext gRPC remains supported on loopback for a same-host TLS proxy. It is
rejected on a public interface when application authentication is enabled.

---

## Embedding ownership (critical)

NornicDB can run in two modes:

1) **NornicDB-managed embeddings** (`NORNICDB_EMBEDDING_ENABLED=true`)
- NornicDB owns embedding generation and storage.
- Qdrant “vector mutation” RPCs (e.g. Upsert/UpdateVectors/DeleteVectors) may be rejected with `FailedPrecondition` as a policy choice (to avoid multiple external writers).
- Qdrant **text queries** via `Points.Query` are supported (NornicDB embeds the query).

2) **Client-managed vectors** (`NORNICDB_EMBEDDING_ENABLED=false`)
- Your Qdrant client owns vectors and can upsert/update/delete vectors normally.
- Best fit when you want to use Qdrant SDKs “as-is”.

Note: Qdrant vectors are stored in `Node.NamedEmbeddings`, while NornicDB-managed embeddings are stored in `Node.ChunkEmbeddings`, so the two embedding systems do not overwrite each other.
For the internal details, see `docs/architecture/embedding-search.md`.

If you want to use Qdrant SDKs for ingestion, set:

```bash
export NORNICDB_EMBEDDING_ENABLED=false
```

---

## Collection & point mapping

At a high level:

- Qdrant **collection** → a NornicDB **database** (namespace).
  - The Qdrant gRPC layer uses `DatabaseManager.GetStorage(collectionName)`; all point data lives under the namespace prefix `collectionName:`.
  - A collection is considered “Qdrant-managed” if it contains `_collection_meta` (created by `CreateCollection`).
- Qdrant **point** → a NornicDB node inside the collection/database namespace:
  - Node ID: `qdrant:point:<raw-id>` (does not include the collection name)
  - Labels: `QdrantPoint`, `Point`
  - Payload: mapped to `node.Properties` (with internal `_qdrant_*` keys removed from responses)
  - Vector(s): stored in `Node.NamedEmbeddings` and indexed for vector search

NornicDB also supports **named vectors** in Qdrant:

- Qdrant named vectors → `node.NamedEmbeddings[vectorName]`
- This enables multiple independent embedding fields per point (e.g. `title` and `content`).

### Vector search uses the database's index

A Qdrant `Search` runs on the collection database's search index, the same index and query as Cypher's
`db.index.vector.queryNodes` on that database:

- **ANN strategy:** the configured strategy applies, `NORNICDB_VECTOR_ANN_QUALITY` included (HNSW presets, `compressed` IVF-PQ, k-means routing,
  brute force), with the same exact rescoring.
- **`hnsw_ef`:** `params.hnsw_ef` sets the HNSW beam width for that request on HNSW-based strategies. IVF-PQ, k-means routing and
  brute force don't search a graph and ignore it.
- **Payload filters:** filters are applied while the search widens, so a selective filter still returns its top matches.
- **Scores:** Qdrant's raw similarities: cosine and dot as-is, Euclid as `1 / (1 + distance)`.
- **Restart:** points stay searchable across a restart, since the index is rebuilt from the stored vectors.
- **Vector search disabled:** a database with vector search disabled returns no hits, as it does for Cypher.

### Cypher access (collection = database)

Because collections are databases, you can query points with Cypher:

```cypher
USE my_vectors
MATCH (p:QdrantPoint)
RETURN count(p)
```

Dropping a collection is the same as dropping a database:

```cypher
DROP DATABASE `my_vectors`
```

---

## Quick check: is the gRPC endpoint reachable?

If you have `grpcurl` installed:

```bash
grpcurl -plaintext 127.0.0.1:6334 grpc.health.v1.Health/Check
```

---

## Client examples (multi-language)

### Python (official `qdrant-client`, gRPC)

```bash
pip install qdrant-client
```

```python
from qdrant_client import QdrantClient
from qdrant_client.http import models as m

client = QdrantClient(host="127.0.0.1", grpc_port=6334, prefer_grpc=True)

client.create_collection(
    collection_name="my_vectors",
    vectors_config=m.VectorParams(size=128, distance=m.Distance.COSINE),
)

client.upsert(
    collection_name="my_vectors",
    points=[
        m.PointStruct(id="p1", vector=[1.0] * 128, payload={"tag": "first"}),
        m.PointStruct(id="p2", vector=[0.5] * 128, payload={"tag": "second"}),
    ],
)

results = client.search(
    collection_name="my_vectors",
    query_vector=[1.0] * 128,
    limit=10,
    with_payload=True,
)

print([r.id for r in results])
```

Named vectors:

```python
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
            vector={"title": [1.0]*128, "content": [0.2]*128},
            payload={"kind": "article"},
        )
    ],
)

results = client.search(
    collection_name="docs",
    query_vector=("title", [1.0]*128),
    limit=5,
)
```

### Go (direct gRPC using upstream Qdrant protos)

```go
package main

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	qdrant "github.com/qdrant/go-client/qdrant"
)

func main() {
	ctx := context.Background()

	conn, err := grpc.Dial("127.0.0.1:6334", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	points := qdrant.NewPointsClient(conn)

	resp, err := points.Search(ctx, &qdrant.SearchPoints{
		CollectionName: "my_vectors",
		Vector:         make([]float32, 128),
		Limit:          10,
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = resp
}
```

### Rust (gRPC, conceptually)

Use the official Qdrant client that supports gRPC (or generated gRPC stubs) and point it at `127.0.0.1:6334`.

If you prefer a “known good” reference, see:

- `scripts/qdrantgrpc_e2e_python.py` (Python SDK flow)
- `scripts/qdrantgrpc_e2e_client.go` (Go gRPC flow)

---

## Text queries (Qdrant `Points.Query` with `Document.text`)

If NornicDB-managed embeddings are enabled (`NORNICDB_EMBEDDING_ENABLED=true`), you can send a text query using the upstream Qdrant protobuf contract:

- `Points/Query` with `Query.nearest(VectorInput.document(Document{text: ...}))`

This is useful when you want “Qdrant-shaped” inference queries without custom APIs.

---

## Compatibility + supported RPCs

See:

- [`pkg/qdrantgrpc/README.md`](https://github.com/orneryd/nornicdb/blob/main/pkg/qdrantgrpc/README.md) for implementation details and examples
- [`pkg/qdrantgrpc/COMPAT.md`](https://github.com/orneryd/nornicdb/blob/main/pkg/qdrantgrpc/COMPAT.md) for the supported RPC matrix

---

## Troubleshooting

### gRPC not listening

- Confirm `NORNICDB_QDRANT_GRPC_ENABLED=true`
- Confirm port `6334` is exposed (Docker) and not already in use.

### `FailedPrecondition` on Upsert / UpdateVectors / DeleteVectors

This typically means NornicDB-managed embeddings are enabled. If you want the client to own vectors:

```bash
export NORNICDB_EMBEDDING_ENABLED=false
```
