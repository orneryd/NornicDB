# Vector ANN benchmark: NornicDB vs Qdrant

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`ce38410589d30884699cf0a61b871f51dfe3f493`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Summary

Each mode at its preset beam (explicit on Qdrant; configured preset on NornicDB). Missing cells are unmeasured, not passing runs.

| Mode | NornicDB recall@10 | NornicDB p50 | NornicDB p95 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant p95 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Exact (brute force) | 1.0000 | 6.712 ms | 8.990 ms | 175 | 1.0000 | 4.075 ms | 5.028 ms | 356 |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | 0.9964 | 0.823 ms | 0.991 ms | 5,974 | 0.9966 | 0.625 ms | 0.735 ms | 3,615 |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | 0.9992 | 1.267 ms | 1.530 ms | 3,754 | 0.9997 | 0.963 ms | 1.103 ms | 2,053 |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) [gomemlimit2g] | 0.9989 | 1.264 ms | 1.528 ms | 3,973 | N/A | N/A | N/A | N/A |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) [metal] | 0.9992 | 1.292 ms | 1.609 ms | 3,912 | N/A | N/A | N/A | N/A |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | 0.9998 | 2.994 ms | 3.854 ms | 1,541 | 1.0000 | 1.972 ms | 2.314 ms | 912 |
| IVF (k-means routing, IVF-HNSW) | 0.9744 | 18.451 ms | 21.590 ms | 449 | N/A | N/A | N/A | N/A |
| IVF (k-means routing, IVF-HNSW) [nprobe128] | 0.9993 | 80.064 ms | 84.106 ms | 92 | N/A | N/A | N/A | N/A |
| IVF-PQ (compressed) | 0.9773 | 5.285 ms | 5.838 ms | 942 | N/A | N/A | N/A | N/A |
| IVF-PQ (compressed) [rerank2000] | 0.9773 | 30.109 ms | 32.896 ms | 145 | N/A | N/A | N/A | N/A |
| HNSW + scalar int8 quantization (rescore) | N/A | N/A | N/A | N/A | 0.9972 | 0.349 ms | 0.393 ms | 6,720 |
| HNSW + product quantization x16 (rescore) | N/A | N/A | N/A | N/A | 0.9944 | 0.560 ms | 0.630 ms | 4,556 |
| HNSW + binary quantization (rescore, 2x oversampling) | N/A | N/A | N/A | N/A | 0.9952 | 0.288 ms | 0.326 ms | 7,403 |

## `hnsw_ef` sweeps

The same Search beam on both engines. The baseline uses each named preset's efSearch (explicitly sent on Qdrant), raised to the result count on NornicDB.

### HNSW fast (M=16, efConstruction=100, efSearch=50)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9964 | 0.823 ms | 5,974 | 0.9966 | 0.625 ms | 3,615 |
| 16 | 0.9731 | 0.472 ms | 8,576 | 0.9817 | 0.428 ms | 5,830 |
| 32 | 0.9909 | 0.625 ms | 6,814 | 0.9931 | 0.499 ms | 4,337 |
| 64 | 0.9974 | 0.909 ms | 4,553 | 0.9971 | 0.702 ms | 3,051 |
| 128 | 0.9990 | 1.406 ms | 3,334 | 0.9990 | 0.985 ms | 1,912 |
| 256 | 0.9996 | 2.276 ms | 1,963 | 0.9998 | 1.512 ms | 1,214 |
| 512 | 1.0000 | 3.889 ms | 1,164 | 0.9998 | 2.324 ms | 815 |

### HNSW balanced (M=16, efConstruction=200, efSearch=100)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9992 | 1.267 ms | 3,754 | 0.9997 | 0.963 ms | 2,053 |
| 16 | 0.9783 | 0.510 ms | 7,742 | 0.9856 | 0.447 ms | 5,716 |
| 32 | 0.9928 | 0.654 ms | 7,306 | 0.9946 | 0.511 ms | 4,414 |
| 64 | 0.9981 | 0.942 ms | 4,942 | 0.9982 | 0.759 ms | 2,975 |
| 128 | 0.9992 | 1.486 ms | 3,241 | 0.9999 | 1.084 ms | 1,893 |
| 256 | 0.9999 | 2.403 ms | 1,962 | 1.0000 | 1.706 ms | 1,185 |
| 512 | 1.0000 | 4.076 ms | 1,119 | 1.0000 | 2.719 ms | 770 |

### HNSW balanced (M=16, efConstruction=200, efSearch=100) [gomemlimit2g]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9989 | 1.264 ms | 3,973 | N/A | N/A | N/A |
| 16 | 0.9727 | 0.511 ms | 8,739 | N/A | N/A | N/A |
| 32 | 0.9915 | 0.655 ms | 6,867 | N/A | N/A | N/A |
| 64 | 0.9976 | 0.974 ms | 5,079 | N/A | N/A | N/A |
| 128 | 0.9991 | 1.481 ms | 3,248 | N/A | N/A | N/A |
| 256 | 0.9997 | 2.383 ms | 1,834 | N/A | N/A | N/A |
| 512 | 1.0000 | 4.081 ms | 1,152 | N/A | N/A | N/A |

### HNSW balanced (M=16, efConstruction=200, efSearch=100) [metal]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9992 | 1.292 ms | 3,912 | N/A | N/A | N/A |
| 16 | 0.9721 | 0.503 ms | 7,650 | N/A | N/A | N/A |
| 32 | 0.9932 | 0.661 ms | 7,192 | N/A | N/A | N/A |
| 64 | 0.9988 | 1.016 ms | 5,203 | N/A | N/A | N/A |
| 128 | 0.9996 | 1.503 ms | 3,434 | N/A | N/A | N/A |
| 256 | 0.9999 | 2.433 ms | 1,955 | N/A | N/A | N/A |
| 512 | 0.9999 | 4.144 ms | 1,188 | N/A | N/A | N/A |

### HNSW accurate (M=32, efConstruction=400, efSearch=200)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9998 | 2.994 ms | 1,541 | 1.0000 | 1.972 ms | 912 |
| 16 | 0.9820 | 0.636 ms | 7,254 | 0.9928 | 0.527 ms | 4,254 |
| 32 | 0.9961 | 0.867 ms | 5,384 | 0.9975 | 0.671 ms | 3,399 |
| 64 | 0.9992 | 1.368 ms | 3,422 | 0.9997 | 0.981 ms | 2,159 |
| 128 | 0.9997 | 2.230 ms | 2,188 | 1.0000 | 1.500 ms | 1,355 |
| 256 | 0.9998 | 3.631 ms | 1,284 | 1.0000 | 2.313 ms | 853 |
| 512 | 1.0000 | 5.962 ms | 772 | 1.0000 | 3.569 ms | 554 |

### HNSW + scalar int8 quantization (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | N/A | N/A | N/A | 0.9972 | 0.349 ms | 6,720 |
| 16 | N/A | N/A | N/A | 0.9807 | 0.276 ms | 7,140 |
| 32 | N/A | N/A | N/A | 0.9935 | 0.298 ms | 7,443 |
| 64 | N/A | N/A | N/A | 0.9977 | 0.333 ms | 6,440 |
| 128 | N/A | N/A | N/A | 0.9994 | 0.409 ms | 5,382 |
| 256 | N/A | N/A | N/A | 0.9998 | 0.558 ms | 3,902 |
| 512 | N/A | N/A | N/A | 1.0000 | 0.817 ms | 2,563 |

### HNSW + product quantization x16 (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | N/A | N/A | N/A | 0.9944 | 0.560 ms | 4,556 |
| 16 | N/A | N/A | N/A | 0.9795 | 0.448 ms | 5,540 |
| 32 | N/A | N/A | N/A | 0.9903 | 0.526 ms | 4,571 |
| 64 | N/A | N/A | N/A | 0.9962 | 0.580 ms | 4,540 |
| 128 | N/A | N/A | N/A | 0.9965 | 0.673 ms | 3,914 |
| 256 | N/A | N/A | N/A | 0.9972 | 0.859 ms | 3,177 |
| 512 | N/A | N/A | N/A | 0.9973 | 1.168 ms | 2,398 |

### HNSW + binary quantization (rescore, 2x oversampling)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | N/A | N/A | N/A | 0.9952 | 0.288 ms | 7,403 |
| 16 | N/A | N/A | N/A | 0.9825 | 0.272 ms | 7,221 |
| 32 | N/A | N/A | N/A | 0.9915 | 0.279 ms | 7,271 |
| 64 | N/A | N/A | N/A | 0.9962 | 0.300 ms | 7,140 |
| 128 | N/A | N/A | N/A | 0.9975 | 0.330 ms | 6,782 |
| 256 | N/A | N/A | N/A | 0.9984 | 0.391 ms | 5,385 |
| 512 | N/A | N/A | N/A | 0.9985 | 0.500 ms | 4,435 |

## Build and footprint

| Mode | Engine | Strategy | Load | Load rate | Index ready | Disk | RSS after queries |
|---|---|---|---:|---:|---:|---:|---:|
| Exact (brute force) | NornicDB | CPU brute-force | 12.5 s | 7,052 vec/s | 4.2 s | 1,238 MB | 3,728 MB |
| Exact (brute force) | Qdrant | exact search (exact=true) | 5.3 s | 16,519 vec/s | 1.0 s | 436 MB | 901 MB |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | NornicDB | HNSW | 12.5 s | 7,047 vec/s | 64.1 s | 1,239 MB | 3,744 MB |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | Qdrant | HNSW m=16 ef_construct=100 | 5.7 s | 15,534 vec/s | 4.0 s | 440 MB | 540 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | NornicDB | HNSW | 12.8 s | 6,910 vec/s | 85.8 s | 1,238 MB | 3,749 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | Qdrant | HNSW m=16 ef_construct=200 | 5.5 s | 16,045 vec/s | 10.1 s | 442 MB | 542 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) [gomemlimit2g] | NornicDB | HNSW | 12.6 s | 7,005 vec/s | 86.2 s | 1,237 MB | 3,446 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) [metal] | NornicDB | HNSW | 12.7 s | 6,966 vec/s | 47.6 s | 1,239 MB | 3,762 MB |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | NornicDB | HNSW | 12.7 s | 6,963 vec/s | 187.4 s | 1,239 MB | 3,749 MB |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | Qdrant | HNSW m=32 ef_construct=400 | 5.7 s | 15,355 vec/s | 20.2 s | 443 MB | 545 MB |
| IVF (k-means routing, IVF-HNSW) | NornicDB | IVF-HNSW (per-cluster HNSW) | 13.2 s | 6,692 vec/s | 60.5 s | 1,237 MB | 3,973 MB |
| IVF (k-means routing, IVF-HNSW) [nprobe128] | NornicDB | IVF-HNSW (per-cluster HNSW) | 12.6 s | 7,001 vec/s | 59.9 s | 1,237 MB | 4,228 MB |
| IVF-PQ (compressed) | NornicDB | IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=6m36.994280334s] | 12.9 s | 6,819 vec/s | 401.2 s | 1,388 MB | 3,462 MB |
| IVF-PQ (compressed) [rerank2000] | NornicDB | IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=6m36.935648291s] | 12.8 s | 6,870 vec/s | 401.3 s | 1,388 MB | 4,050 MB |
| HNSW + scalar int8 quantization (rescore) | Qdrant | HNSW m=16 ef_construct=100 + scalar quantization | 5.2 s | 16,966 vec/s | 2.0 s | 531 MB | 660 MB |
| HNSW + product quantization x16 (rescore) | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.3 s | 20,691 vec/s | 30.3 s | 480 MB | 574 MB |
| HNSW + binary quantization (rescore, 2x oversampling) | Qdrant | HNSW m=16 ef_construct=100 + binary quantization | 6.0 s | 14,709 vec/s | 1.0 s | 452 MB | 594 MB |

## Notes

- **Strategy:** NornicDB's observed pipeline strategy is checked against the requested mode. Qdrant's collection configuration and request parameters are checked; its internal per-query planner is not observed. Qdrant uses its minimum supported full-scan threshold (10 KB), so small populations may still use exact scanning.
- **Index ready**: NornicDB's search index built and its first search answered; Qdrant's collection green with every vector indexed (`indexing_threshold` is 1 KB for ANN modes; exact mode has HNSW disabled).
- **Disk** counts allocated blocks and **RSS** is a process snapshot after all queries and a final readiness check; neither is a peak measurement.
- NornicDB's quantization is IVF-PQ (`compressed`); Qdrant quantizes on top of HNSW. The modes are listed separately, not as equivalents.
