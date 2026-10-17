# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`7c201376d1203a651fc625869fb98df7be746ac4`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## HNSW + product quantization x16 (rescore) [rescore8]

- **Strategy:** HNSW PQ x16 (23616256 encoded bytes)
- **Load:** 13.0 s (6,775 vectors/s); **index ready:** 40.5 s; **disk:** 1,630 MB; **RSS:** 3,274 MB after load, 3,274 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9990 | 2.443 ms | 2.403 ms | 2.825 ms | 3.089 ms | 2,205 |
| 16 | 1.0000 | 0.9990 | 2.398 ms | 2.385 ms | 2.752 ms | 2.961 ms | 2,173 |
| 32 | 1.0000 | 0.9990 | 2.469 ms | 2.449 ms | 2.938 ms | 3.229 ms | 2,069 |
| 64 | 1.0000 | 0.9990 | 2.429 ms | 2.407 ms | 2.816 ms | 2.993 ms | 2,160 |
| 128 | 1.0000 | 0.9991 | 2.440 ms | 2.432 ms | 2.799 ms | 2.969 ms | 2,193 |
| 256 | 1.0000 | 0.9995 | 2.921 ms | 2.906 ms | 3.338 ms | 3.517 ms | 1,831 |
| 512 | 1.0000 | 1.0000 | 3.728 ms | 3.731 ms | 4.241 ms | 4.476 ms | 1,509 |

## HNSW + binary quantization (rescore, 8x oversampling) [rescore8]

- **Strategy:** HNSW binary (11283840 encoded bytes)
- **Load:** 12.9 s (6,841 vectors/s); **index ready:** 35.4 s; **disk:** 1,629 MB; **RSS:** 3,199 MB after load, 3,199 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9988 | 1.796 ms | 1.776 ms | 2.066 ms | 2.245 ms | 2,683 |
| 16 | 1.0000 | 0.9988 | 1.737 ms | 1.729 ms | 2.032 ms | 2.144 ms | 2,715 |
| 32 | 1.0000 | 0.9988 | 1.742 ms | 1.735 ms | 2.026 ms | 2.145 ms | 2,649 |
| 64 | 1.0000 | 0.9988 | 1.752 ms | 1.736 ms | 2.041 ms | 2.173 ms | 2,629 |
| 128 | 1.0000 | 0.9985 | 1.764 ms | 1.760 ms | 2.048 ms | 2.209 ms | 2,731 |
| 256 | 1.0000 | 0.9990 | 1.902 ms | 1.893 ms | 2.193 ms | 2.347 ms | 2,512 |
| 512 | 1.0000 | 0.9992 | 2.175 ms | 2.168 ms | 2.482 ms | 2.621 ms | 2,278 |

## HNSW + binary storage, int8 query (rescore, 8x oversampling) [rescore8]

- **Strategy:** HNSW binary query int8 (11283840 encoded bytes)
- **Load:** 13.0 s (6,793 vectors/s); **index ready:** 35.3 s; **disk:** 1,630 MB; **RSS:** 3,225 MB after load, 3,226 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9991 | 2.013 ms | 1.982 ms | 2.322 ms | 2.612 ms | 2,561 |
| 16 | 0.9990 | 0.9991 | 1.965 ms | 1.953 ms | 2.269 ms | 2.388 ms | 2,432 |
| 32 | 0.9990 | 0.9991 | 1.955 ms | 1.948 ms | 2.230 ms | 2.385 ms | 2,557 |
| 64 | 0.9990 | 0.9991 | 1.956 ms | 1.947 ms | 2.254 ms | 2.374 ms | 2,566 |
| 128 | 0.9990 | 0.9992 | 2.004 ms | 1.987 ms | 2.330 ms | 2.477 ms | 2,557 |
| 256 | 1.0000 | 0.9996 | 2.277 ms | 2.257 ms | 2.575 ms | 2.784 ms | 2,318 |
| 512 | 1.0000 | 0.9998 | 2.802 ms | 2.791 ms | 3.251 ms | 3.462 ms | 1,964 |

## HNSW + product quantization x32 (rescore) [rescore8]

- **Strategy:** HNSW PQ x32 (12332416 encoded bytes)
- **Load:** 12.9 s (6,813 vectors/s); **index ready:** 38.6 s; **disk:** 1,629 MB; **RSS:** 3,220 MB after load, 3,220 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9989 | 2.185 ms | 2.157 ms | 2.554 ms | 2.840 ms | 2,276 |
| 16 | 1.0000 | 0.9989 | 2.123 ms | 2.118 ms | 2.435 ms | 2.567 ms | 2,267 |
| 32 | 1.0000 | 0.9989 | 2.100 ms | 2.086 ms | 2.410 ms | 2.545 ms | 2,266 |
| 64 | 1.0000 | 0.9989 | 2.105 ms | 2.092 ms | 2.401 ms | 2.600 ms | 2,278 |
| 128 | 1.0000 | 0.9990 | 2.141 ms | 2.124 ms | 2.462 ms | 2.624 ms | 2,264 |
| 256 | 1.0000 | 0.9997 | 2.425 ms | 2.420 ms | 2.750 ms | 2.926 ms | 1,936 |
| 512 | 1.0000 | 0.9999 | 2.941 ms | 2.932 ms | 3.358 ms | 3.617 ms | 1,818 |

## HNSW + product quantization x64 (rescore) [rescore8]

- **Strategy:** HNSW PQ x64 (6690496 encoded bytes)
- **Load:** 13.1 s (6,706 vectors/s); **index ready:** 38.5 s; **disk:** 1,629 MB; **RSS:** 3,187 MB after load, 3,187 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9922 | 1.992 ms | 1.972 ms | 2.304 ms | 2.609 ms | 2,453 |
| 16 | 0.9960 | 0.9922 | 1.981 ms | 1.957 ms | 2.302 ms | 2.435 ms | 2,518 |
| 32 | 0.9960 | 0.9922 | 2.042 ms | 1.968 ms | 2.383 ms | 3.857 ms | 2,487 |
| 64 | 0.9960 | 0.9922 | 1.967 ms | 1.960 ms | 2.312 ms | 2.493 ms | 2,475 |
| 128 | 0.9960 | 0.9928 | 1.958 ms | 1.938 ms | 2.282 ms | 2.462 ms | 2,438 |
| 256 | 0.9990 | 0.9946 | 2.173 ms | 2.161 ms | 2.536 ms | 2.648 ms | 2,290 |
| 512 | 0.9990 | 0.9950 | 2.472 ms | 2.455 ms | 2.883 ms | 3.042 ms | 2,114 |
