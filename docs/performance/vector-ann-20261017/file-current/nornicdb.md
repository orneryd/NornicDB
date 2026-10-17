# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`7c201376d1203a651fc625869fb98df7be746ac4`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## HNSW fast (M=16, efConstruction=100, efSearch=50) [original-file]

- **Strategy:** HNSW
- **Load:** 13.0 s (6,763 vectors/s); **index ready:** 35.3 s; **disk:** 1,631 MB; **RSS:** 3,180 MB after load, 3,180 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9959 | 2.850 ms | 2.774 ms | 3.572 ms | 5.039 ms | 1,009 |
| 16 | 0.9840 | 0.9724 | 1.499 ms | 1.440 ms | 1.911 ms | 2.408 ms | 2,047 |
| 32 | 0.9960 | 0.9930 | 2.141 ms | 2.063 ms | 2.720 ms | 3.889 ms | 1,219 |
| 64 | 0.9970 | 0.9972 | 3.395 ms | 3.302 ms | 4.385 ms | 8.228 ms | 846 |
| 128 | 1.0000 | 0.9992 | 5.563 ms | 5.451 ms | 7.204 ms | 9.949 ms | 494 |
| 256 | 1.0000 | 0.9998 | 9.599 ms | 9.465 ms | 12.570 ms | 16.616 ms | 283 |
| 512 | 1.0000 | 1.0000 | 16.685 ms | 16.532 ms | 22.060 ms | 26.245 ms | 160 |
