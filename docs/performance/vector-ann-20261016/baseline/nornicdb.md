# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`5f7e301634bd9e209987f26b4997b88488027837`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Exact (brute force)

- **Strategy:** CPU brute-force
- **Load:** 12.5 s (7,074 vectors/s); **index ready:** 4.2 s; **disk:** 1,238 MB; **RSS:** 3,410 MB after load, 3,411 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 7.048 ms | 6.764 ms | 8.883 ms | 10.704 ms | 177 |

## HNSW fast (M=16, efConstruction=100, efSearch=50)

- **Strategy:** HNSW
- **Load:** 12.6 s (6,983 vectors/s); **index ready:** 62.3 s; **disk:** 1,237 MB; **RSS:** 3,742 MB after load, 3,742 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9959 | 0.846 ms | 0.805 ms | 0.965 ms | 1.379 ms | 6,014 |
| 16 | 0.9850 | 0.9713 | 0.510 ms | 0.489 ms | 0.580 ms | 0.657 ms | 8,572 |
| 32 | 0.9940 | 0.9907 | 0.677 ms | 0.649 ms | 0.757 ms | 0.861 ms | 7,458 |
| 64 | 0.9970 | 0.9973 | 0.976 ms | 0.924 ms | 1.132 ms | 4.097 ms | 4,575 |
| 128 | 0.9990 | 0.9991 | 1.477 ms | 1.433 ms | 1.765 ms | 4.534 ms | 3,527 |
| 256 | 1.0000 | 0.9998 | 2.419 ms | 2.362 ms | 2.997 ms | 5.150 ms | 2,051 |
| 512 | 1.0000 | 1.0000 | 4.232 ms | 4.126 ms | 5.614 ms | 7.954 ms | 1,203 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100)

- **Strategy:** HNSW
- **Load:** 12.7 s (6,922 vectors/s); **index ready:** 87.0 s; **disk:** 1,238 MB; **RSS:** 3,735 MB after load, 3,735 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9980 | 0.9985 | 1.318 ms | 1.269 ms | 1.552 ms | 4.186 ms | 3,731 |
| 16 | 0.9860 | 0.9735 | 0.555 ms | 0.531 ms | 0.653 ms | 0.761 ms | 7,133 |
| 32 | 0.9950 | 0.9917 | 0.705 ms | 0.678 ms | 0.811 ms | 0.940 ms | 7,292 |
| 64 | 0.9970 | 0.9973 | 1.015 ms | 0.985 ms | 1.170 ms | 1.463 ms | 4,902 |
| 128 | 0.9980 | 0.9988 | 1.582 ms | 1.512 ms | 2.016 ms | 4.604 ms | 2,999 |
| 256 | 1.0000 | 0.9996 | 2.563 ms | 2.495 ms | 3.377 ms | 4.794 ms | 2,000 |
| 512 | 1.0000 | 1.0000 | 4.197 ms | 4.117 ms | 5.394 ms | 7.358 ms | 1,137 |

## HNSW accurate (M=32, efConstruction=400, efSearch=200)

- **Strategy:** HNSW
- **Load:** 12.8 s (6,889 vectors/s); **index ready:** 191.7 s; **disk:** 1,238 MB; **RSS:** 3,744 MB after load, 3,744 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9999 | 3.378 ms | 3.297 ms | 4.584 ms | 6.027 ms | 1,514 |
| 16 | 0.9950 | 0.9845 | 0.684 ms | 0.658 ms | 0.809 ms | 0.935 ms | 7,225 |
| 32 | 0.9990 | 0.9956 | 0.944 ms | 0.912 ms | 1.123 ms | 1.306 ms | 5,391 |
| 64 | 1.0000 | 0.9994 | 1.494 ms | 1.443 ms | 1.882 ms | 4.370 ms | 3,551 |
| 128 | 1.0000 | 0.9997 | 2.246 ms | 2.182 ms | 2.798 ms | 5.507 ms | 2,151 |
| 256 | 1.0000 | 1.0000 | 3.737 ms | 3.675 ms | 4.945 ms | 6.920 ms | 1,243 |
| 512 | 1.0000 | 1.0000 | 6.120 ms | 6.071 ms | 7.936 ms | 9.559 ms | 764 |

## IVF (k-means routing, IVF-HNSW)

- **Strategy:** IVF-HNSW (per-cluster HNSW)
- **Load:** 13.0 s (6,782 vectors/s); **index ready:** 60.4 s; **disk:** 1,237 MB; **RSS:** 3,589 MB after load, 3,962 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9940 | 0.6399 | 11.801 ms | 14.982 ms | 18.347 ms | 19.483 ms | 609 |

## IVF-PQ (compressed)

- **Strategy:** IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=6m39.634324083s]
- **Load:** 13.0 s (6,799 vectors/s); **index ready:** 403.9 s; **disk:** 1,388 MB; **RSS:** 3,910 MB after load, 3,911 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9920 | 0.9773 | 5.492 ms | 5.393 ms | 6.067 ms | 8.749 ms | 944 |
