# NornicDB vs Qdrant (Local) Benchmark Harness

Compares **NornicDB’s Qdrant-compatible gRPC endpoint** against a **local Qdrant** instance using the *same upstream Qdrant gRPC contract* (`qdrant.Points/Search`).

This harness:

1. Starts NornicDB once on random ports (gRPC enabled).
2. Loads the same dataset into:
   - NornicDB (via its Qdrant-compatible gRPC endpoint)
   - Qdrant (via its own gRPC endpoint)
3. Runs identical search workloads with identical concurrency.
4. Prints ops/sec + latency stats and writes CSV for regression tracking.

## Requirements

Start Qdrant with **both** REST and gRPC ports exposed:

```bash
docker run -d --name qdrant-vector-db \
  -p 6333:6333 -p 6334:6334 \
  -v qdrant_data:/qdrant/storage \
  qdrant/qdrant:latest
```

## Run

```bash
go run ./testing/benchmarks/nornic_vs_qdrant \
  -qdrant-grpc-addr 127.0.0.1:6334 \
  -points 20000 -dim 128 -k 10 \
  -concurrency 32 -seconds 10 -warmup-seconds 2 \
  -load-timeout-seconds 600
```

## “Real sanitized data”

You can supply a JSONL dataset to avoid synthetic vectors:

```bash
go run ./testing/benchmarks/nornic_vs_qdrant \
  -dataset /path/to/sanitized.jsonl \
  -dim 1536 \
  -qdrant-grpc-addr 127.0.0.1:6334
```

JSONL format: one JSON object per line:

```json
{"id":"doc-1","vector":[0.1,0.2,0.3],"payload":{"tag":"a","i":1}}
```
