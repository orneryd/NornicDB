# gRPC vs Bolt Benchmark Harness

Benchmarks end-to-end vector search performance over the same NornicDB instance using:

- **Qdrant gRPC**: `qdrant.Points/Search` (using a named vector to force brute-force matching)
- **Bolt**: Cypher `CALL db.index.vector.queryNodes(...)` over Bolt protocol

This harness:

1. Starts NornicDB **once** on random ports.
2. Loads a deterministic synthetic dataset.
3. Runs both clients with identical concurrency and duration.
4. Prints ops/sec + latency percentiles + a simple histogram.
5. Appends a CSV row to `testing/benchmarks/grpc_vs_bolt/results.csv`.

## Usage

```bash
go run ./testing/benchmarks/grpc_vs_bolt \
  -points 20000 \
  -dim 128 \
  -k 10 \
  -concurrency 32 \
  -seconds 10 \
  -warmup-seconds 2
```

## Notes / Caveats

- The Cypher executor has an internal read-query result cache. The harness avoids
  “benchmarking the cache” by adding a changing `$nonce` parameter to the Bolt
  query so each request is a cache miss (but still reuses the same query text so
  plan/analyzer caches can still help).
- The **Bolt** path uses `CALL db.index.vector.queryNodes(...)`, which currently scans nodes and computes cosine similarity in-process.
- The **gRPC** path uses a **named vector** (`vector_name="v"`) so it also takes the brute-force code path (for a fairer “protocol overhead” comparison).
- For a “best-case gRPC” benchmark (ANN/HNSW), change the gRPC query to omit `vector_name` and load default vectors; that will use `search.Service` when available.
