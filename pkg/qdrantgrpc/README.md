# Qdrant gRPC Compatibility Layer

This package provides a **Qdrant-compatible gRPC API** for NornicDB, enabling existing Qdrant SDKs to connect without modification.

## Overview

NornicDB implements the [Qdrant gRPC API](https://qdrant.tech/documentation/interfaces/#grpc-api) (pinned to v1.16.x) to enable:

- **Multi-language SDK reuse**: Python, Go, Rust, JavaScript, and other Qdrant clients work out of the box
- **Zero-migration vector search**: Existing applications can switch to NornicDB without code changes
- **High-performance protocol**: Direct protobuf (no JSON) for minimal latency
- **Unified indexing**: Points added via Qdrant gRPC are searchable via `/nornicdb/search` and vice versa

## Feature Flag

The Qdrant gRPC endpoint is **disabled by default** and must be explicitly enabled:

### Environment Variable

```bash
export NORNICDB_QDRANT_GRPC_ENABLED=true
export NORNICDB_QDRANT_GRPC_LISTEN_ADDR=":6334"  # optional, default is :6334
```

### Embedding Ownership (Important)

NornicDB can run in two modes:

- **NornicDB-managed embeddings** (`NORNICDB_EMBEDDING_ENABLED=true`): Qdrant vector mutation RPCs (`Upsert`, `UpdateVectors`, `DeleteVectors`) return `FailedPrecondition` to avoid conflicting sources of truth.
- **Client-managed vectors via Qdrant gRPC** (`NORNICDB_EMBEDDING_ENABLED=false`): Qdrant clients can fully manage stored vectors/embeddings via gRPC (recommended when you are using Qdrant SDKs as-is).

### Configuration

```yaml
features:
  qdrant_grpc_enabled: true
  qdrant_grpc_listen_addr: ":6334"
  qdrant_grpc_max_vector_dim: 4096
  qdrant_grpc_max_batch_points: 1000
  qdrant_grpc_max_top_k: 1000
```

## Supported Features

NornicDB exposes a single gRPC surface: the **official upstream Qdrant gRPC contract** (package `qdrant`), so real Qdrant SDKs (Python `qdrant-client`, etc.) work without modification.

Implemented for SDK compatibility (and covered by `scripts/qdrantgrpc_e2e_python.sh`):

#### Collections Service

| RPC | Status | Notes |
|-----|--------|-------|
| `Create` | ✅ | Single-vector and named-vector configs |
| `Get` | ✅ | Returns minimal-but-valid `CollectionInfo` with defaults filled for SDK parsing |
| `List` | ✅ | |
| `Delete` | ✅ | Deletes collection metadata and points |
| `Update` | ✅ | Acknowledges existence (NornicDB manages params) |
| `CollectionExists` | ✅ | |

#### Points Service (core)

| RPC | Status | Notes |
|-----|--------|-------|
| `Upsert` | ✅ | Dense vectors + named vectors |
| `Get` | ✅ | With payload/vectors selectors |
| `Delete` | ✅ | By ID list or filter |
| `Count` | ✅ | |
| `Search` | ✅ | Score threshold + vector_name supported |
| `Query` / `QueryBatch` | ✅ | Supports `VectorInput` (Dense/Id) and `Document` when embeddings are enabled |
| `Scroll` | ✅ | |
| `SetPayload` / `OverwritePayload` / `DeletePayload` / `ClearPayload` | ✅ | |
| `UpdateVectors` / `DeleteVectors` | ✅ | Subject to embedding ownership flag |

Other upstream Qdrant RPCs are currently `UNIMPLEMENTED` and can be added as needed.

## Text Queries via Upstream Qdrant API (`Points.Query`)

NornicDB supports Qdrant’s upstream “inference-shaped” query inputs by implementing:

- `qdrant.Points/Query`
- `qdrant.Points/QueryBatch`

This allows clients to send a **document/text query** (rather than a numeric vector) using the upstream protobuf contract:

- `VectorInput.Document` (`Document.text`)

### Requirements

- `NORNICDB_QDRANT_GRPC_ENABLED=true`
- `NORNICDB_EMBEDDING_ENABLED=true` (so NornicDB can embed the query text). If embeddings are disabled, `VectorInput.Document` returns `FailedPrecondition`.

### Go Example (direct gRPC call)

```go
conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
defer conn.Close()

points := qdrant.NewPointsClient(conn)

resp, err := points.Query(ctx, &qdrant.QueryPoints{
    CollectionName: "my_collection",
    Limit:          10,
    Query: &qdrant.Query{
        Variant: &qdrant.Query_Nearest{
            Nearest: &qdrant.VectorInput{
                Variant: &qdrant.VectorInput_Document{
                    Document: &qdrant.Document{Text: "database performance"},
                },
            },
        },
    },
    WithPayload: &qdrant.WithPayloadSelector{
        SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: true},
    },
})
if err != nil {
    // handle error
}
_ = resp
```

## Quick Start

### Production Server Setup

For production use, provide the `search.Service` to enable unified vector indexing:

```go
package main

import (
    "log"
    
    "github.com/orneryd/nornicdb/pkg/qdrantgrpc"
    "github.com/orneryd/nornicdb/pkg/search"
    "github.com/orneryd/nornicdb/pkg/storage"
)

func main() {
    // Create persistent storage (Badger for production)
    store, err := storage.NewBadgerEngine("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()
    
    // Create search service for unified indexing
    searchSvc := search.NewService(store)
    defer searchSvc.Close()
    
    // Build indexes from existing data
    if err := searchSvc.BuildIndexes(ctx); err != nil {
        log.Fatal(err)
    }
    
    // Configure server
    config := qdrantgrpc.DefaultConfig()
    config.ListenAddr = ":6334"
    
    // Create server with persistent registry and search service
    server, registry, err := qdrantgrpc.NewServerWithPersistentRegistry(config, store, searchSvc)
    if err != nil {
        log.Fatal(err)
    }
    defer registry.Close()
    
    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    defer server.Stop()
    
    log.Printf("Qdrant gRPC server listening on %s", server.Addr())
    
    // Keep running...
    select {}
}
```

### Python Client Example

```python
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

# Connect to NornicDB's Qdrant-compatible endpoint
client = QdrantClient(host="localhost", port=6334, grpc=True)

# Create a collection
client.create_collection(
    collection_name="my_vectors",
    vectors_config=VectorParams(size=1024, distance=Distance.COSINE)
)

# Insert vectors
client.upsert(
    collection_name="my_vectors",
    points=[
        PointStruct(
            id="doc-1",
            vector=[0.1] * 1024,
            payload={"title": "Document 1", "category": "tech"}
        ),
        PointStruct(
            id="doc-2",
            vector=[0.2] * 1024,
            payload={"title": "Document 2", "category": "science"}
        ),
    ]
)

# Search
results = client.search(
    collection_name="my_vectors",
    query_vector=[0.15] * 1024,
    limit=10,
    with_payload=True
)

# Scroll through all points
scroll_results = client.scroll(
    collection_name="my_vectors",
    limit=100,
    with_payload=True
)

# Update payload
client.set_payload(
    collection_name="my_vectors",
    points=["doc-1"],
    payload={"updated": True}
)

# Recommend similar points
recommendations = client.recommend(
    collection_name="my_vectors",
    positive=["doc-1"],
    negative=["doc-2"],
    limit=5
)
```

### Go Client Example

```go
package main

import (
    "context"
    "log"
    
    qdrant "github.com/qdrant/go-client/qdrant"
)

func main() {
    client, err := qdrant.NewClient(&qdrant.Config{
        Host: "localhost",
        Port: 6334,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    ctx := context.Background()
    
    // Create collection
    err = client.CreateCollection(ctx, &qdrant.CreateCollection{
        CollectionName: "my_vectors",
        VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
            Size:     1024,
            Distance: qdrant.Distance_Cosine,
        }),
    })
    
    // Upsert points
    _, err = client.Upsert(ctx, &qdrant.UpsertPoints{
        CollectionName: "my_vectors",
        Points: []*qdrant.PointStruct{
            {
                Id:      qdrant.NewIDNum(1),
                Vectors: qdrant.NewVectors(0.1, 0.2, 0.3 /* ... */),
                Payload: qdrant.NewValueMap(map[string]any{
                    "title": "Document 1",
                }),
            },
        },
    })
    
    // Search
    results, err := client.Search(ctx, &qdrant.SearchPoints{
        CollectionName: "my_vectors",
        Vector:         []float32{0.1, 0.2, 0.3 /* ... */},
        Limit:          10,
        WithPayload:    qdrant.NewWithPayload(true),
    })
    
    // Update vectors
    _, err = client.UpdateVectors(ctx, &qdrant.UpdatePointVectors{
        CollectionName: "my_vectors",
        Points: []*qdrant.PointVectors{
            {
                Id:      qdrant.NewIDNum(1),
                Vectors: qdrant.NewVectors(0.5, 0.5, 0.5 /* ... */),
            },
        },
    })
}
```

## Configuration

| Option | Default | Description | Env Variable |
|--------|---------|-------------|--------------|
| `QdrantGRPCEnabled` | `false` | Enable the Qdrant gRPC server | `NORNICDB_QDRANT_GRPC_ENABLED` |
| `QdrantGRPCListenAddr` | `:6334` | gRPC listen address | `NORNICDB_QDRANT_GRPC_LISTEN_ADDR` |
| `QdrantGRPCMaxVectorDim` | `4096` | Maximum vector dimension | `NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM` |
| `QdrantGRPCMaxBatchPoints` | `1000` | Max points per upsert | `NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS` |
| `QdrantGRPCMaxTopK` | `1000` | Max search results | `NORNICDB_QDRANT_GRPC_MAX_TOP_K` |

## Architecture

### Thin Translation Layer

The Qdrant gRPC package is a **thin translation layer** that maps Qdrant RPCs to NornicDB internals:

```
┌─────────────────────────────────────────────────────────────┐
│                     Qdrant SDK                               │
│              (Python, Go, Rust, etc.)                        │
└─────────────────────────────────────────────────────────────┘
                           │ gRPC (protobuf)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              pkg/qdrantgrpc (TRANSLATION LAYER)              │
│                                                              │
│   ┌─────────────────┐  ┌──────────────┐  ┌────────────────┐ │
│   │ CollectionsService│ │PointsService │ │ HealthService  │ │
│   │  (6 RPCs)        │ │  (16 RPCs)    │ │  (1 RPC)       │ │
│   └────────┬─────────┘ └──────┬────────┘ └────────────────┘ │
│            │                  │                              │
│            │  Type conversion │  Type conversion             │
│            ▼                  ▼                              │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     NornicDB Core                            │
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐  │
│  │ storage.Engine│  │ search.Service│  │ SchemaManager   │  │
│  │ (Nodes/Edges) │  │ (Vector Index)│  │ (Field Indexes) │  │
│  └───────────────┘  └───────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Data Model Mapping

| Qdrant Concept | NornicDB Equivalent |
|----------------|---------------------|
| Collection | Metadata node (`_QdrantCollection` label) |
| Point | Node with `QdrantPoint` + collection labels |
| PointId | NodeID: `qdrant:{collection}:{id}` |
| Payload | Node.Properties |
| Vector(s) | `Node.ChunkEmbeddings` (named vectors preserved via internal name→index mapping) |
| Named Vectors | Node.ChunkEmbeddings[N] (future) |
| Filter | In-memory property filter |

### Key Benefits

1. **Single Source of Truth**: Points are stored as standard NornicDB nodes
2. **Cross-Endpoint Search**: Points added via Qdrant are searchable via `/nornicdb/search`
3. **Cypher Integration**: Points can be queried via Cypher MATCH patterns
4. **Unified Indexing**: `search.Service` maintains one vector index for all data

## Distance Metrics

| Qdrant Distance | NornicDB Implementation |
|-----------------|-------------------------|
| `COSINE` | Dot product on normalized vectors |
| `DOT` | Dot product |
| `EUCLID` | Euclidean distance |

## Performance Considerations

### Hot Path Optimizations

- **No JSON**: Pure protobuf encoding/decoding
- **Batch operations**: Bulk node creation
- **Direct storage access**: Bypasses Cypher query layer for CRUD
- **Connection pooling**: gRPC keepalive tuning

### Limits for Safety

All limits are enforced to prevent OOM conditions:

- Batch sizes capped to prevent large memory allocations
- Payload sizes limited per point
- Search result limits enforced

## Testing

### End-to-End (Core Server Integration)

This verifies the gRPC endpoint is correctly wired into the core server behind feature flags:

```bash
./scripts/qdrantgrpc_e2e.sh
```

```bash
# Run unit tests
go test ./pkg/qdrantgrpc/... -v

# Run with coverage
go test ./pkg/qdrantgrpc/... -coverprofile=coverage.out
go tool cover -html=coverage.out

# Current coverage: 73.9%
```

## Implementation Tracking

See [COMPAT.md](./COMPAT.md) for detailed implementation status.

## License

Same license as NornicDB.
