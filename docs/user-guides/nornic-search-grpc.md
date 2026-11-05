# NornicSearch gRPC With Existing Qdrant Drivers

Use existing Qdrant gRPC drivers unchanged, and add NornicDB's `NornicSearch` proto as an extra client when you want `SearchText`.

You do **not** need to fork Qdrant drivers.

## When You Need This

- You already use Qdrant gRPC APIs (`Points.Search`, `Points.Query`, etc.).
- You also want Nornic-native `NornicSearch/SearchText`.
- You want both over the same endpoint (`:6334` by default).

## Prerequisites

- `NORNICDB_QDRANT_GRPC_ENABLED=true`
- Port `6334` exposed/reachable
- Native TLS enabled for authenticated public listeners, or a loopback-only
  plaintext hop behind an HTTP/2-capable TLS proxy
- Existing Qdrant gRPC client in your app (unchanged)
- Add generated client code from `pkg/nornicgrpc/proto/nornicdb_search.proto`

## Architecture (Side-by-Side Clients)

- **Qdrant client**: talks to Qdrant-compatible services
- **Nornic client**: talks to `NornicSearch` service
- **Same gRPC channel**: both clients can share one connection to `127.0.0.1:6334`

## Go Example (Recommended)

1. Keep your current Qdrant client/protos.

2. Generate Go stubs for Nornic proto:

```bash
protoc \
  --go_out=. \
  --go_opt=module=github.com/orneryd/nornicdb \
  --go-grpc_out=. \
  --go-grpc_opt=module=github.com/orneryd/nornicdb \
  pkg/nornicgrpc/proto/nornicdb_search.proto
```

3. Create both clients on the same connection:

```go
transportCredentials, err := credentials.NewClientTLSFromFile("ca.crt", "db.example.com")
if err != nil {
    log.Fatal(err)
}
conn, err := grpc.NewClient("db.example.com:6334", grpc.WithTransportCredentials(transportCredentials))
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

qdrantClient := qdrant.NewPointsClient(conn)             // Existing Qdrant driver usage
nornicClient := nornicpb.NewNornicSearchClient(conn)     // Additive Nornic client

// Existing Qdrant-compatible vector search
_, err = qdrantClient.Search(ctx, &qdrant.SearchPoints{
    CollectionName: "my_vectors",
    Vector:         make([]float32, 128),
    Limit:          10,
})
if err != nil {
    log.Fatal(err)
}

// Nornic-native text search
resp, err := nornicClient.SearchText(ctx, &nornicpb.SearchTextRequest{
    Query:             "machine learning",
    Limit:             10,
    IncludeProperties: []string{"title", "summary"},
    ExcludeProperties: []string{"raw_payload"},
})
if err != nil {
    log.Fatal(err)
}
_ = resp
```

## Durable continuation

Start a continued search by setting `N`. The returned qid is opaque and must be
sent unchanged on later calls:

```go
page, err := nornicClient.SearchText(ctx, &nornicpb.SearchTextRequest{
    Database: "nornic",
    Query: "sunset beach",
    Limit: 500,
    N: 50,
    Mode: "ranked_then_id",
    GroupBy: "asset_id",
    RankedLimit: proto.Uint64(5000),
    IncludeProperties: []string{"title", "description", "asset_id"},
    ExcludeProperties: []string{"raw_payload"},
})
if err != nil {
    log.Fatal(err)
}

next, err := nornicClient.SearchText(ctx, &nornicpb.SearchTextRequest{
    Qid: page.Qid,
    N: 50,
})
if err != nil {
    log.Fatal(err)
}

_, err = nornicClient.SearchText(ctx, &nornicpb.SearchTextRequest{
    Qid: next.Qid,
    Discard: true,
})
```

Import `google.golang.org/protobuf/proto` for `proto.Uint64`. Grouped hits use
one page slot and expose child matches through `SearchHit.passages`; each child
passage is also a `SearchHit`, so hit metadata has the same shape at both
levels. See [Search Continuation](search-continuation.md) for all modes and
metadata.

If a pull returns a cancellation or deadline-exceeded error while producing its
page, retry the same `Qid` with a fresh context to resume at the same position
before its fixed expiry. Other producer errors retire the qid. Failed delivery
of an already produced page also retires it; see
[Stream lifetime](search-continuation.md#stream-lifetime) for that distinction.

`include_properties` limits the returned property map and
`exclude_properties` removes keys from it; exclusion wins when a key appears
in both. These fields only project responses—they do not change filters,
BM25/rerank inputs, or ranking. A continuation retains the projection supplied
when it starts, so pull requests only need the qid and page size.

Import `google.golang.org/grpc/credentials` for this TLS configuration. Use
`insecure.NewCredentials()` only for local development or a loopback-only hop
behind a same-host TLS proxy. For mTLS, construct `credentials.NewTLS` with a
client certificate and trusted root pool.

## Existing Qdrant Text Query (No Nornic Proto Needed)

If you only need text query with existing Qdrant drivers, use `Points.Query` with `Document.text`:

```python
from qdrant_client import QdrantClient, models

client = QdrantClient(host="127.0.0.1", grpc_port=6334, prefer_grpc=True)
client.query_points(
    collection_name="my_vectors",
    query=models.Document(text="machine learning"),
    limit=10,
)
```

## Which Path Should I Use?

- Use **Qdrant `Points.Query` + `Document.text`** when you want pure Qdrant-driver semantics.
- Use **`NornicSearch/SearchText`** when you want Nornic-native typed search RPC behavior.
- Use both together if your app needs both compatibility and native features.
