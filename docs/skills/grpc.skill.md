---
name: nornicdb-grpc
description: Drive NornicDB over gRPC — the Qdrant-compatible surface (Collections, Points, Snapshots) plus the additive NornicSearch service. Use when ingesting via Qdrant SDKs, migrating from Qdrant, or running hybrid text+vector search from a non-Bolt client. Covers connection, RPC catalog, collection→database mapping, point→node mapping, limits, auth, and minimum-viable client examples.
---

# NornicDB gRPC (Qdrant + NornicSearch)

NornicDB exposes two gRPC services on the same listener:

1. **Qdrant compatibility** — full set of `qdrant.Collections`, `qdrant.Points`, and `qdrant.Snapshots` services. Existing Qdrant SDKs work without modification.
2. **`NornicSearch`** — one additive RPC, `SearchText`, that returns hybrid (vector + BM25 + RRF) results.

The same client connection talks to both; pick whichever matches the operation.

## Connection

| Setting             | Value                                                                                                                                          |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Listen address      | `:6334` (`NORNICDB_QDRANT_GRPC_LISTEN_ADDR`)                                                                                                   |
| Default port (host) | `6334`                                                                                                                                         |
| Auth                | Same `Auth.Enabled` flag as Bolt. When off, gRPC is open. When on, basic auth or bearer JWT in the gRPC metadata.                              |
| TLS                 | Native TLS/mTLS is available and required for authenticated public listeners. Plaintext is suitable only for loopback or isolated development. |

Enable the gRPC server (off by default):

```bash
export NORNICDB_QDRANT_GRPC_ENABLED=true
export NORNICDB_QDRANT_GRPC_TLS_ENABLED=true
export NORNICDB_QDRANT_GRPC_TLS_CERT=/tls/grpc.crt
export NORNICDB_QDRANT_GRPC_TLS_KEY=/tls/grpc.key
```

YAML:

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

## Limits (defaults)

| Limit                             | Default | Override                                |
| --------------------------------- | ------- | --------------------------------------- |
| Max vector dimension              | 4096    | `NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM`   |
| Max points per Upsert batch       | 1000    | `NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS` |
| Max top-K per Search              | 1000    | `NORNICDB_QDRANT_GRPC_MAX_TOP_K`        |
| Max payload bytes per point       | 1 MB    | (not configurable)                      |
| Max filter clauses per request    | 100     | (not configurable)                      |
| Request timeout                   | 30 s    | (not configurable)                      |
| Max gRPC message size (recv/send) | 64 MB   | (not configurable)                      |

## Data model mapping

| Qdrant concept | NornicDB equivalent                  | Storage detail                                                                            |
| -------------- | ------------------------------------ | ----------------------------------------------------------------------------------------- |
| Collection     | Database (namespace)                 | `DatabaseManager.GetStorage(collectionName)`; all point keys live under `collectionName:` |
| Point          | Node                                 | Node ID is `qdrant:point:<rawID>`. Labels include `QdrantPoint`, `Point`.                 |
| Point payload  | `node.Properties`                    | Internal `_qdrant_*` keys are stripped from outbound responses.                           |
| Single vector  | `node.NamedEmbeddings["default"]`    |                                                                                           |
| Named vectors  | `node.NamedEmbeddings[<vectorName>]` | Each named vector is independently searchable.                                            |

A NornicDB-managed collection is identifiable by the presence of a `_collection_meta` metadata node inside the database.

## Qdrant RPCs implemented

### `qdrant.Collections`

- `Create` — single-vector and named-vector configs supported
- `Get` — minimal-but-valid `CollectionInfo`
- `List` — list all collections
- `Delete` — drops collection metadata and all its points
- `Update` — no-op (validates existence; NornicDB manages params)
- `CollectionExists` — fast existence check

### `qdrant.Points`

- `Upsert` — dense and named vectors
- `Get` — with payload/vector selectors
- `Delete` — by ID list or by filter
- `Count`
- `Search` — supports `score_threshold` and `vector_name`
- `SearchBatch`
- `Query` — supports `VectorInput` (dense, ID) and `Document` (server-side embedding) when `NORNICDB_EMBEDDING_ENABLED=true`
- `QueryBatch`
- `Scroll`
- `SetPayload`, `OverwritePayload`, `DeletePayload`, `ClearPayload`
- `UpdateVectors`, `DeleteVectors`
- `Recommend`, `RecommendBatch`
- `SearchGroups`
- `CreateFieldIndex`, `DeleteFieldIndex`

### `qdrant.Snapshots`

- `Create`, `List`, `Delete` (per collection)
- `CreateFull`, `ListFull`, `DeleteFull` (database-wide)

For the full Qdrant proto compatibility matrix and divergence notes, see `pkg/qdrantgrpc/COMPAT.md`.

## NornicSearch RPC (additive)

```protobuf
service NornicSearch {
  rpc SearchText(SearchTextRequest) returns (SearchTextResponse);
}

message SearchTextRequest {
  string database = 1;
  string query = 2;
  uint32 limit = 3;
  repeated string labels = 4;
  optional float min_similarity = 5;
  string qid = 6;
  uint32 n = 7;
  bool discard = 8;
  optional uint64 max_results = 9;
  string mode = 10;           // ranked | ranked_then_id | id
  string group_by = 11;
  optional uint64 ranked_limit = 12;
  repeated string include_properties = 13;
  repeated string exclude_properties = 14;
}

message SearchTextResponse {
  string search_method = 1;
  repeated SearchHit hits = 2;     // includes phase, group_key, and passages
  bool fallback_triggered = 3;
  string message = 4;
  double time_seconds = 5;
  string qid = 6;
  bool has_more = 7;
  uint64 position = 8;
  uint32 returned = 9;
  optional uint64 total = 10;
  google.protobuf.Timestamp expires_at = 11;
  bool released = 12;
  string mode = 13;
  int64 ranked_count = 14;
  optional int64 eligible_count = 15;
  bool ranked_pool_exhausted = 16;
  bool collection_exhausted = 17;
  string completion = 18;
  string fallback_reason = 19;
}
```

`include_properties` and `exclude_properties` project the property maps in
returned hits without affecting filtering or ranking. Exclusion wins, and a
continuation retains its initial projection across pulls.

`ranked_pool_exhausted` is true only when the ranked branch is known to be
fully exhausted. `completion="eligible_population_exhausted"` means the stream
emitted the complete eligible population, including a `ranked` stream that
naturally exhausted an exact producer. `completion="candidate_pool_exhausted"`
means a configured candidate boundary, such as rerank top-K, stopped ranked
expansion without proving full ranked exhaustion. `completion="max_results_reached"`
means the caller's explicit result ceiling stopped the stream before full
collection exhaustion.

`SearchText` runs the same hybrid pipeline as the `db.retrieve` Cypher procedure: vector + BM25, fused with RRF, with adaptive weights based on query length. If the requested search path changes, it sets `fallback_triggered=true` and returns a stable `fallback_reason` code. Provider diagnostics remain in warning logs rather than caller-visible fields.

Supplying `n` or another continuation field starts a durable stream. Reuse the
returned `qid` with `n` to pull another page, or with `discard=true` to release
it. `limit` is the initial ranked depth; `n` is the page size. See
[Search Continuation](../user-guides/search-continuation.md) for mode, grouping,
expiry, and cross-protocol rules.

## Minimum-viable clients

### Python (`qdrant-client`, prefer gRPC)

```python
from qdrant_client import QdrantClient
from qdrant_client.http import models as m

client = QdrantClient(host="127.0.0.1", grpc_port=6334, prefer_grpc=True)

client.create_collection(
    collection_name="docs",
    vectors_config=m.VectorParams(size=1024, distance=m.Distance.COSINE),
)

client.upsert(
    collection_name="docs",
    points=[
        m.PointStruct(id="d1", vector=[0.1] * 1024, payload={"title": "hello"}),
    ],
)

hits = client.search(
    collection_name="docs",
    query_vector=[0.1] * 1024,
    limit=10,
    with_payload=True,
)
```

### Go (`github.com/qdrant/go-client v1.18.1`)

```go
import (
    "context"
    "github.com/qdrant/go-client/qdrant"
)

client, _ := qdrant.NewClient(&qdrant.Config{
    Host:   "localhost",
    Port:   6334,
    UseTLS: false,
})

collections, _ := client.ListCollections(ctx)

scroll, _ := client.Scroll(ctx, &qdrant.ScrollPoints{
    CollectionName: "docs",
    Limit:          qdrant.PtrOf(uint32(100)),
    WithPayload:    qdrant.NewWithPayload(true),
    WithVectors:    qdrant.NewWithVectors(true),
})
for _, p := range scroll {
    _ = p
}
```

### Node (`@qdrant/js-client-rest` — note: REST, not gRPC)

The Node ecosystem does not have a maintained gRPC Qdrant client; the official `@qdrant/js-client-rest` talks to the REST surface. NornicDB exposes Qdrant compatibility on gRPC only — for Node/TypeScript clients, drive NornicDB through Bolt (`neo4j-driver`) instead, or use the Cypher `db.index.vector.queryNodes` / `db.retrieve` procedures.

If you do have an existing Node app on `@qdrant/js-client-rest`, you'll need to either run a separate gRPC client (e.g. via `@grpc/grpc-js` against the Qdrant `.proto`) or migrate that path to Bolt.

## Auth

`Auth.Enabled` (the same flag that controls Bolt) gates gRPC too. When enabled:

- **Basic auth** — pass `authorization: Basic <base64(user:pass)>` in the gRPC metadata.
- **Bearer JWT** — pass `authorization: Bearer <token>`.
- **Per-collection RBAC** — optional, configured via the `qdrant_grpc_rbac` YAML block, mapping `<Service>/<Method>` to a permission tier (`read`, `write`, `create`, `delete`, `admin`).

Native TLS is configured with `NORNICDB_QDRANT_GRPC_TLS_ENABLED`,
`NORNICDB_QDRANT_GRPC_TLS_CERT`, and `NORNICDB_QDRANT_GRPC_TLS_KEY`. Optional
mTLS uses `NORNICDB_QDRANT_GRPC_TLS_CLIENT_CA` and
`NORNICDB_QDRANT_GRPC_TLS_CLIENT_AUTH_MODE=require_verify`. TLS/mTLS does not
replace Basic, Bearer, or API-key metadata; both layers are enforced.

When `Auth.Enabled=false`, the gRPC endpoint is open — local-development default.

## When to pick gRPC vs Bolt

| Use case                                                 | Pick                                                                 |
| -------------------------------------------------------- | -------------------------------------------------------------------- |
| Existing Qdrant SDK code, vector-only workload           | gRPC (Qdrant compat)                                                 |
| Existing Neo4j SDK code, graph + vector                  | Bolt + Cypher                                                        |
| Non-Bolt language (e.g. Rust, C++) needing hybrid search | gRPC + `NornicSearch.SearchText`                                     |
| Migrating from Qdrant                                    | gRPC; see [`qdrant-migration.skill.md`](qdrant-migration.skill.md)   |
| Migrating from Neo4j                                     | Bolt; see [`neo4j-migration.skill.md`](neo4j-migration.skill.md)     |
| Mixed ingestion (graph + vector)                         | Bolt; gRPC's collection-as-database model is per-collection isolated |

## See also

- [`qdrant-migration.skill.md`](qdrant-migration.skill.md) — end-to-end Qdrant → NornicDB migration.
- [`neo4j-migration.skill.md`](neo4j-migration.skill.md) — Neo4j → NornicDB via Bolt.
- [`bolt-client.skill.md`](bolt-client.skill.md) — the Bolt surface and retry classifier.
- [`user-guides/qdrant-grpc.md`](../user-guides/qdrant-grpc.md) — long-form documentation including the full proto compatibility matrix.
- [`user-guides/nornic-search-grpc.md`](../user-guides/nornic-search-grpc.md) — additive `NornicSearch` proto setup.
