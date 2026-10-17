# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`c5ef90e3825d2871f7df772a293b09237c00c2a8`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## IVF (k-means routing, IVF-HNSW) [no-fallback]

- **Strategy:** IVF-HNSW (per-cluster HNSW)
- **Load:** 12.7 s (6,963 vectors/s); **index ready:** 54.1 s; **disk:** 1,238 MB; **RSS:** 3,534 MB after load, 3,767 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9930 | 0.9737 | 2.452 ms | 2.363 ms | 2.691 ms | 5.635 ms | 738 |
