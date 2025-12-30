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
```

### YAML config

```yaml
features:
  qdrant_grpc_enabled: true
  qdrant_grpc_listen_addr: ":6334"
  qdrant_grpc_max_vector_dim: 4096
  qdrant_grpc_max_batch_points: 1000
  qdrant_grpc_max_top_k: 1000
```

### Docker / Compose

Expose the gRPC port:

```yaml
ports:
  - "7474:7474" # HTTP
  - "7687:7687" # Bolt
  - "6334:6334" # Qdrant gRPC
environment:
  - NORNICDB_QDRANT_GRPC_ENABLED=true
  - NORNICDB_QDRANT_GRPC_LISTEN_ADDR=:6334
```

---

## Embedding ownership (critical)

NornicDB can run in two modes:

1) **NornicDB-managed embeddings** (`NORNICDB_EMBEDDING_ENABLED=true`)
- NornicDB owns embedding generation and storage.
- Qdrant “vector mutation” RPCs (e.g. Upsert/UpdateVectors/DeleteVectors) may be rejected with `FailedPrecondition` to avoid conflicting sources of truth.
- Qdrant **text queries** via `Points.Query` are supported (NornicDB embeds the query).

2) **Client-managed vectors** (`NORNICDB_EMBEDDING_ENABLED=false`)
- Your Qdrant client owns vectors and can upsert/update/delete vectors normally.
- Best fit when you want to use Qdrant SDKs “as-is”.

If you want to use Qdrant SDKs for ingestion, set:

```bash
export NORNICDB_EMBEDDING_ENABLED=false
```

---

## Collection & point mapping

At a high level:

- Qdrant **collection** → a named vector space/index in NornicDB (managed by the Qdrant gRPC compatibility layer)
- Qdrant **point** → a NornicDB node carrying:
  - ID (string/uuid/number mapped to NornicDB node ID)
  - payload (mapped to node properties)
  - vector(s) (stored as embeddings, indexed for vector search)

NornicDB also supports **named vectors** in Qdrant:

- Qdrant named vectors → `node.NamedEmbeddings[vectorName]`
- This enables multiple independent embedding fields per point (e.g. `title` and `content`).

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

- `pkg/qdrantgrpc/README.md` for implementation details and examples
- `pkg/qdrantgrpc/COMPAT.md` for the supported RPC matrix

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

