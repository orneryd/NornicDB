# Vector ANN benchmark: NornicDB vs Qdrant

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`5f7e301634bd9e209987f26b4997b88488027837`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Summary

Each mode at its preset beam (explicit on Qdrant; configured preset on NornicDB). Missing cells are unmeasured, not passing runs.

| Mode | NornicDB recall@10 | NornicDB p50 | NornicDB p95 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant p95 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Exact (brute force) | 1.0000 | 6.764 ms | 8.883 ms | 177 | 1.0000 | 3.797 ms | 4.042 ms | 376 |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | 0.9959 | 0.805 ms | 0.965 ms | 6,014 | 0.9975 | 0.573 ms | 0.667 ms | 3,645 |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | 0.9985 | 1.269 ms | 1.552 ms | 3,731 | 0.9994 | 0.927 ms | 1.070 ms | 2,257 |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | 0.9999 | 3.297 ms | 4.584 ms | 1,514 | 1.0000 | 1.990 ms | 2.351 ms | 1,026 |
| IVF (k-means routing, IVF-HNSW) | 0.6399 | 14.982 ms | 18.347 ms | 609 | N/A | N/A | N/A | N/A |
| IVF-PQ (compressed) | 0.9773 | 5.393 ms | 6.067 ms | 944 | N/A | N/A | N/A | N/A |
| HNSW + scalar int8 quantization (rescore) | N/A | N/A | N/A | N/A | 0.9960 | 0.303 ms | 0.342 ms | 6,976 |
| HNSW + product quantization x16 (rescore) | N/A | N/A | N/A | N/A | 0.9950 | 0.522 ms | 0.603 ms | 4,805 |
| HNSW + binary quantization (rescore, 2x oversampling) | N/A | N/A | N/A | N/A | 0.9944 | 0.282 ms | 0.320 ms | 7,625 |

## `hnsw_ef` sweeps

The same Search beam on both engines. The baseline uses each named preset's efSearch (explicitly sent on Qdrant), raised to the result count on NornicDB.

### HNSW fast (M=16, efConstruction=100, efSearch=50)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9959 | 0.805 ms | 6,014 | 0.9975 | 0.573 ms | 3,645 |
| 16 | 0.9713 | 0.489 ms | 8,572 | 0.9821 | 0.416 ms | 6,075 |
| 32 | 0.9907 | 0.649 ms | 7,458 | 0.9940 | 0.516 ms | 4,763 |
| 64 | 0.9973 | 0.924 ms | 4,575 | 0.9982 | 0.688 ms | 3,284 |
| 128 | 0.9991 | 1.433 ms | 3,527 | 0.9997 | 0.983 ms | 2,079 |
| 256 | 0.9998 | 2.362 ms | 2,051 | 1.0000 | 1.483 ms | 1,298 |
| 512 | 1.0000 | 4.126 ms | 1,203 | 1.0000 | 2.373 ms | 815 |

### HNSW balanced (M=16, efConstruction=200, efSearch=100)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9985 | 1.269 ms | 3,731 | 0.9994 | 0.927 ms | 2,257 |
| 16 | 0.9735 | 0.531 ms | 7,133 | 0.9841 | 0.440 ms | 5,928 |
| 32 | 0.9917 | 0.678 ms | 7,292 | 0.9957 | 0.561 ms | 4,314 |
| 64 | 0.9973 | 0.985 ms | 4,902 | 0.9989 | 0.719 ms | 2,980 |
| 128 | 0.9988 | 1.512 ms | 2,999 | 0.9996 | 1.100 ms | 1,934 |
| 256 | 0.9996 | 2.495 ms | 2,000 | 0.9999 | 1.684 ms | 1,202 |
| 512 | 1.0000 | 4.117 ms | 1,137 | 0.9999 | 2.733 ms | 763 |

### HNSW accurate (M=32, efConstruction=400, efSearch=200)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9999 | 3.297 ms | 1,514 | 1.0000 | 1.990 ms | 1,026 |
| 16 | 0.9845 | 0.658 ms | 7,225 | 0.9906 | 0.493 ms | 4,377 |
| 32 | 0.9956 | 0.912 ms | 5,391 | 0.9978 | 0.685 ms | 3,408 |
| 64 | 0.9994 | 1.443 ms | 3,551 | 0.9996 | 0.994 ms | 2,181 |
| 128 | 0.9997 | 2.182 ms | 2,151 | 0.9999 | 1.525 ms | 1,332 |
| 256 | 1.0000 | 3.675 ms | 1,243 | 1.0000 | 2.351 ms | 876 |
| 512 | 1.0000 | 6.071 ms | 764 | 1.0000 | 3.635 ms | 575 |

### HNSW + scalar int8 quantization (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | N/A | N/A | N/A | 0.9960 | 0.303 ms | 6,976 |
| 16 | N/A | N/A | N/A | 0.9842 | 0.266 ms | 7,284 |
| 32 | N/A | N/A | N/A | 0.9945 | 0.285 ms | 7,389 |
| 64 | N/A | N/A | N/A | 0.9972 | 0.319 ms | 6,658 |
| 128 | N/A | N/A | N/A | 0.9993 | 0.406 ms | 5,089 |
| 256 | N/A | N/A | N/A | 0.9997 | 0.580 ms | 4,482 |
| 512 | N/A | N/A | N/A | 0.9999 | 0.796 ms | 2,634 |

### HNSW + product quantization x16 (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | N/A | N/A | N/A | 0.9950 | 0.522 ms | 4,805 |
| 16 | N/A | N/A | N/A | 0.9808 | 0.501 ms | 5,548 |
| 32 | N/A | N/A | N/A | 0.9913 | 0.537 ms | 4,439 |
| 64 | N/A | N/A | N/A | 0.9956 | 0.587 ms | 4,517 |
| 128 | N/A | N/A | N/A | 0.9974 | 0.667 ms | 3,847 |
| 256 | N/A | N/A | N/A | 0.9977 | 0.870 ms | 3,151 |
| 512 | N/A | N/A | N/A | 0.9978 | 1.172 ms | 2,473 |

### HNSW + binary quantization (rescore, 2x oversampling)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | N/A | N/A | N/A | 0.9944 | 0.282 ms | 7,625 |
| 16 | N/A | N/A | N/A | 0.9823 | 0.269 ms | 7,376 |
| 32 | N/A | N/A | N/A | 0.9893 | 0.268 ms | 7,529 |
| 64 | N/A | N/A | N/A | 0.9954 | 0.290 ms | 7,422 |
| 128 | N/A | N/A | N/A | 0.9978 | 0.321 ms | 7,030 |
| 256 | N/A | N/A | N/A | 0.9987 | 0.381 ms | 6,392 |
| 512 | N/A | N/A | N/A | 0.9989 | 0.496 ms | 4,686 |

## Build and footprint

| Mode | Engine | Strategy | Load | Load rate | Index ready | Disk | RSS after queries |
|---|---|---|---:|---:|---:|---:|---:|
| Exact (brute force) | NornicDB | CPU brute-force | 12.5 s | 7,074 vec/s | 4.2 s | 1,238 MB | 3,411 MB |
| Exact (brute force) | Qdrant | exact search (exact=true) | 5.3 s | 16,778 vec/s | 1.0 s | 436 MB | 903 MB |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | NornicDB | HNSW | 12.6 s | 6,983 vec/s | 62.3 s | 1,237 MB | 3,742 MB |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | Qdrant | HNSW m=16 ef_construct=100 | 5.3 s | 16,507 vec/s | 4.0 s | 440 MB | 543 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | NornicDB | HNSW | 12.7 s | 6,922 vec/s | 87.0 s | 1,238 MB | 3,735 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | Qdrant | HNSW m=16 ef_construct=200 | 5.2 s | 16,874 vec/s | 9.1 s | 442 MB | 542 MB |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | NornicDB | HNSW | 12.8 s | 6,889 vec/s | 191.7 s | 1,238 MB | 3,744 MB |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | Qdrant | HNSW m=32 ef_construct=400 | 5.3 s | 16,478 vec/s | 20.2 s | 443 MB | 544 MB |
| IVF (k-means routing, IVF-HNSW) | NornicDB | IVF-HNSW (per-cluster HNSW) | 13.0 s | 6,782 vec/s | 60.4 s | 1,237 MB | 3,962 MB |
| IVF-PQ (compressed) | NornicDB | IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=6m39.634324083s] | 13.0 s | 6,799 vec/s | 403.9 s | 1,388 MB | 3,911 MB |
| HNSW + scalar int8 quantization (rescore) | Qdrant | HNSW m=16 ef_construct=100 + scalar quantization | 5.0 s | 17,686 vec/s | 1.0 s | 531 MB | 673 MB |
| HNSW + product quantization x16 (rescore) | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.5 s | 19,754 vec/s | 28.2 s | 480 MB | 577 MB |
| HNSW + binary quantization (rescore, 2x oversampling) | Qdrant | HNSW m=16 ef_construct=100 + binary quantization | 5.6 s | 15,661 vec/s | 1.0 s | 1,071 MB | 1,433 MB |

## Notes

- **Strategy:** NornicDB's observed pipeline strategy is checked against the requested mode. Qdrant's collection configuration and request parameters are checked; its internal per-query planner is not observed. Qdrant uses its minimum supported full-scan threshold (10 KB), so small populations may still use exact scanning.
- **Index ready**: NornicDB's search index built and its first search answered; Qdrant's collection green with every vector indexed (`indexing_threshold` is 1 KB for ANN modes; exact mode has HNSW disabled).
- **Disk** counts allocated blocks and **RSS** is a process snapshot after all queries and a final readiness check; neither is a peak measurement.
- NornicDB's quantization is IVF-PQ (`compressed`); Qdrant quantizes on top of HNSW. The modes are listed separately, not as equivalents.
