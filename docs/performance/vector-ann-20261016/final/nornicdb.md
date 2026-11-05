# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`ce38410589d30884699cf0a61b871f51dfe3f493`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Exact (brute force)

- **Strategy:** CPU brute-force
- **Load:** 12.5 s (7,052 vectors/s); **index ready:** 4.2 s; **disk:** 1,238 MB; **RSS:** 3,728 MB after load, 3,728 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 6.953 ms | 6.712 ms | 8.990 ms | 10.317 ms | 175 |

## HNSW fast (M=16, efConstruction=100, efSearch=50)

- **Strategy:** HNSW
- **Load:** 12.5 s (7,047 vectors/s); **index ready:** 64.1 s; **disk:** 1,239 MB; **RSS:** 3,744 MB after load, 3,744 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9964 | 0.864 ms | 0.823 ms | 0.991 ms | 1.563 ms | 5,974 |
| 16 | 0.9900 | 0.9731 | 0.495 ms | 0.472 ms | 0.559 ms | 0.655 ms | 8,576 |
| 32 | 0.9960 | 0.9909 | 0.654 ms | 0.625 ms | 0.742 ms | 0.858 ms | 6,814 |
| 64 | 0.9990 | 0.9974 | 0.944 ms | 0.909 ms | 1.101 ms | 1.278 ms | 4,553 |
| 128 | 1.0000 | 0.9990 | 1.447 ms | 1.406 ms | 1.699 ms | 4.356 ms | 3,334 |
| 256 | 1.0000 | 0.9996 | 2.332 ms | 2.276 ms | 2.844 ms | 5.637 ms | 1,963 |
| 512 | 1.0000 | 1.0000 | 3.923 ms | 3.889 ms | 4.894 ms | 6.688 ms | 1,164 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100)

- **Strategy:** HNSW
- **Load:** 12.8 s (6,910 vectors/s); **index ready:** 85.8 s; **disk:** 1,238 MB; **RSS:** 3,749 MB after load, 3,749 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9992 | 1.312 ms | 1.267 ms | 1.530 ms | 4.222 ms | 3,754 |
| 16 | 0.9890 | 0.9783 | 0.535 ms | 0.510 ms | 0.613 ms | 0.681 ms | 7,742 |
| 32 | 0.9970 | 0.9928 | 0.684 ms | 0.654 ms | 0.770 ms | 0.882 ms | 7,306 |
| 64 | 0.9990 | 0.9981 | 0.977 ms | 0.942 ms | 1.123 ms | 1.441 ms | 4,942 |
| 128 | 1.0000 | 0.9992 | 1.538 ms | 1.486 ms | 1.787 ms | 4.780 ms | 3,241 |
| 256 | 1.0000 | 0.9999 | 2.459 ms | 2.403 ms | 3.008 ms | 5.407 ms | 1,962 |
| 512 | 1.0000 | 1.0000 | 4.138 ms | 4.076 ms | 5.090 ms | 6.974 ms | 1,119 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100) [gomemlimit2g]

- **Strategy:** HNSW
- **Load:** 12.6 s (7,005 vectors/s); **index ready:** 86.2 s; **disk:** 1,237 MB; **RSS:** 3,446 MB after load, 3,446 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9989 | 1.311 ms | 1.264 ms | 1.528 ms | 4.356 ms | 3,973 |
| 16 | 0.9920 | 0.9727 | 0.547 ms | 0.511 ms | 0.639 ms | 0.942 ms | 8,739 |
| 32 | 0.9980 | 0.9915 | 0.680 ms | 0.655 ms | 0.766 ms | 0.877 ms | 6,867 |
| 64 | 1.0000 | 0.9976 | 1.011 ms | 0.974 ms | 1.163 ms | 2.326 ms | 5,079 |
| 128 | 1.0000 | 0.9991 | 1.527 ms | 1.481 ms | 1.786 ms | 4.388 ms | 3,248 |
| 256 | 1.0000 | 0.9997 | 2.436 ms | 2.383 ms | 2.920 ms | 5.069 ms | 1,834 |
| 512 | 1.0000 | 1.0000 | 4.147 ms | 4.081 ms | 5.213 ms | 7.603 ms | 1,152 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100) [metal]

- **Strategy:** HNSW
- **Load:** 12.7 s (6,966 vectors/s); **index ready:** 47.6 s; **disk:** 1,239 MB; **RSS:** 4,391 MB after load, 3,762 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9980 | 0.9992 | 1.342 ms | 1.292 ms | 1.609 ms | 4.240 ms | 3,912 |
| 16 | 0.9870 | 0.9721 | 0.530 ms | 0.503 ms | 0.598 ms | 0.692 ms | 7,650 |
| 32 | 0.9970 | 0.9932 | 0.686 ms | 0.661 ms | 0.777 ms | 0.903 ms | 7,192 |
| 64 | 0.9980 | 0.9988 | 1.113 ms | 1.016 ms | 1.663 ms | 4.031 ms | 5,203 |
| 128 | 0.9990 | 0.9996 | 1.546 ms | 1.503 ms | 1.812 ms | 4.575 ms | 3,434 |
| 256 | 1.0000 | 0.9999 | 2.479 ms | 2.433 ms | 2.963 ms | 5.392 ms | 1,955 |
| 512 | 1.0000 | 0.9999 | 4.179 ms | 4.144 ms | 5.189 ms | 6.906 ms | 1,188 |

## HNSW accurate (M=32, efConstruction=400, efSearch=200)

- **Strategy:** HNSW
- **Load:** 12.7 s (6,963 vectors/s); **index ready:** 187.4 s; **disk:** 1,239 MB; **RSS:** 3,749 MB after load, 3,749 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9998 | 3.037 ms | 2.994 ms | 3.854 ms | 5.463 ms | 1,541 |
| 16 | 0.9940 | 0.9820 | 0.673 ms | 0.636 ms | 0.793 ms | 1.172 ms | 7,254 |
| 32 | 1.0000 | 0.9961 | 0.898 ms | 0.867 ms | 1.057 ms | 1.255 ms | 5,384 |
| 64 | 1.0000 | 0.9992 | 1.407 ms | 1.368 ms | 1.723 ms | 4.340 ms | 3,422 |
| 128 | 1.0000 | 0.9997 | 2.327 ms | 2.230 ms | 3.228 ms | 4.709 ms | 2,188 |
| 256 | 1.0000 | 0.9998 | 3.683 ms | 3.631 ms | 4.636 ms | 6.630 ms | 1,284 |
| 512 | 1.0000 | 1.0000 | 6.031 ms | 5.962 ms | 7.643 ms | 9.074 ms | 772 |

## IVF (k-means routing, IVF-HNSW)

- **Strategy:** IVF-HNSW (per-cluster HNSW)
- **Load:** 13.2 s (6,692 vectors/s); **index ready:** 60.5 s; **disk:** 1,237 MB; **RSS:** 3,540 MB after load, 3,973 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9920 | 0.9744 | 15.943 ms | 18.451 ms | 21.590 ms | 23.152 ms | 449 |

## IVF (k-means routing, IVF-HNSW) [nprobe128]

- **Strategy:** IVF-HNSW (per-cluster HNSW)
- **Load:** 12.6 s (7,001 vectors/s); **index ready:** 59.9 s; **disk:** 1,237 MB; **RSS:** 3,686 MB after load, 4,228 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9993 | 80.434 ms | 80.064 ms | 84.106 ms | 87.892 ms | 92 |

## IVF-PQ (compressed)

- **Strategy:** IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=6m36.994280334s]
- **Load:** 12.9 s (6,819 vectors/s); **index ready:** 401.2 s; **disk:** 1,388 MB; **RSS:** 3,455 MB after load, 3,462 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9920 | 0.9773 | 5.345 ms | 5.285 ms | 5.838 ms | 7.623 ms | 942 |

## IVF-PQ (compressed) [rerank2000]

- **Strategy:** IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=6m36.935648291s]
- **Load:** 12.8 s (6,870 vectors/s); **index ready:** 401.3 s; **disk:** 1,388 MB; **RSS:** 4,049 MB after load, 4,050 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9920 | 0.9773 | 30.510 ms | 30.109 ms | 32.896 ms | 36.259 ms | 145 |
