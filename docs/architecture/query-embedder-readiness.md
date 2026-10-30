# Query embedder readiness

Query executors and protocol adapters share one `cypher.QueryEmbedderBinding`. A provider published by `SetEmbedder` becomes visible to later requests on existing and new connections. Before readiness, keyword fallback is allowed. Publishing a provider changes the cache generation, so startup fallbacks and vectors from a previous provider cannot serve later requests.

A request captures one provider, including an unavailable provider, for its entire lifetime. In-flight requests finish using that snapshot; the next request sees a replacement. This avoids mixing chunking, vector dimensions, or cached vectors from different providers.

## Scoped Cypher executors

Bind a scoped executor during construction, before serving requests. `ShareEmbedderFrom` shares future publications; copying `GetEmbedder()` shares only the currently loaded provider.

```go
base := db.GetCypherExecutor()
scoped := cypher.NewStorageExecutor(db.GetStorage())
scoped.ShareEmbedderFrom(base)
// Publishing on base also reaches scoped and executor clones.
base.SetEmbedder(provider) // provider implements cypher.QueryEmbedder
```

Protocol adapters that need direct access use `base.QueryEmbedderBinding()`. The zero value of `cypher.QueryEmbedderBinding` is unavailable; `Load()` returns nil. `Store(provider)` publishes a provider and resets its caches; `Store(nil)` makes later requests unavailable. The binding retains no registry or history of old providers.

## Native text searches

Capture a per-database provider once when a text request begins, then use its methods for chunking and embedding:

```go
request := db.QueryEmbeddingForDB("documents")
chunks, vectors, err := request.EmbedQueryChunks(ctx, "find related release notes")
if err != nil {
    return err
}
// Score each vector against this database, retaining the best chunk score.
_ = chunks
_ = vectors
```

The returned `nornicdb.QueryEmbeddingSnapshot` also provides `ChunkQuery` and `EmbedQuery`. Per-database model configuration and dimension validation retain their existing behavior. ID and numeric-vector searches do not need a text provider and should not resolve one.

For adapters using the global convenience methods, pass one snapshot context through every operation:

```go
requestCtx := db.WithQueryEmbeddingSnapshot(ctx)
chunks, err := db.ChunkQuery(requestCtx, query)
if err != nil {
    return err
}
vector, err := db.EmbedQuery(requestCtx, query)
_ = chunks
_ = vector
return err
```

Keep that context within the request. A nested call on the same database preserves its outer snapshot, including nil during startup.
