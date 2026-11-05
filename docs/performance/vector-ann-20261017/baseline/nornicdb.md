# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`239b057e7c8797597b757d8a4b948c1af6034572`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## HNSW fast (M=16, efConstruction=100, efSearch=50) [original-file]

- **Strategy:** HNSW
- **Load:** 13.0 s (6,800 vectors/s); **index ready:** 35.4 s; **disk:** 1,629 MB; **RSS:** 3,179 MB after load, 3,179 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9961 | 2.906 ms | 2.816 ms | 3.688 ms | 4.953 ms | 1,025 |
| 16 | 0.9830 | 0.9712 | 1.532 ms | 1.486 ms | 1.967 ms | 2.319 ms | 1,976 |
| 32 | 0.9950 | 0.9920 | 2.156 ms | 2.087 ms | 2.747 ms | 3.389 ms | 1,355 |
| 64 | 0.9970 | 0.9974 | 3.383 ms | 3.288 ms | 4.299 ms | 5.050 ms | 870 |
| 128 | 1.0000 | 0.9992 | 5.591 ms | 5.556 ms | 7.153 ms | 8.806 ms | 505 |
| 256 | 1.0000 | 0.9998 | 9.671 ms | 9.554 ms | 12.808 ms | 16.750 ms | 289 |
| 512 | 1.0000 | 1.0000 | 16.827 ms | 16.669 ms | 21.693 ms | 27.407 ms | 165 |
