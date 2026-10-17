# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`7c201376d1203a651fc625869fb98df7be746ac4`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## HNSW + binary storage, int8 query (rescore, 8x oversampling) [rescore8-ram-original]

- **Strategy:** HNSW binary query int8 (11283840 encoded bytes)
- **Load:** 12.5 s (7,075 vectors/s); **index ready:** 49.1 s; **disk:** 1,240 MB; **RSS:** 3,738 MB after load, 3,738 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9990 | 0.784 ms | 0.759 ms | 0.915 ms | 1.129 ms | 8,012 |
| 16 | 1.0000 | 0.9990 | 0.760 ms | 0.742 ms | 0.866 ms | 0.947 ms | 6,613 |
| 32 | 1.0000 | 0.9990 | 0.756 ms | 0.748 ms | 0.904 ms | 0.986 ms | 6,103 |
| 64 | 1.0000 | 0.9990 | 0.759 ms | 0.742 ms | 0.888 ms | 1.047 ms | 7,866 |
| 128 | 1.0000 | 0.9990 | 0.763 ms | 0.760 ms | 0.876 ms | 0.943 ms | 6,558 |
| 256 | 1.0000 | 0.9997 | 1.019 ms | 1.002 ms | 1.207 ms | 1.430 ms | 5,380 |
| 512 | 1.0000 | 0.9999 | 1.512 ms | 1.483 ms | 1.890 ms | 2.146 ms | 4,356 |
