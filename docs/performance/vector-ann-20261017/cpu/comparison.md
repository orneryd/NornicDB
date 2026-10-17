# Vector ANN benchmark: NornicDB vs Qdrant

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`7c201376d1203a651fc625869fb98df7be746ac4`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Summary

Each mode at its preset beam (explicit on Qdrant; configured preset on NornicDB). Missing cells are unmeasured, not passing runs.

| Mode | NornicDB recall@10 | NornicDB p50 | NornicDB p95 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant p95 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Exact (brute force) | 1.0000 | 6.308 ms | 6.579 ms | 186 | 1.0000 | 3.870 ms | 5.000 ms | 369 |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | 0.9947 | 0.811 ms | 0.986 ms | 6,322 | 0.9977 | 0.580 ms | 0.714 ms | 3,318 |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | 0.9992 | 1.207 ms | 1.522 ms | 4,046 | 0.9993 | 0.931 ms | 1.070 ms | 1,976 |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | 0.9997 | 2.803 ms | 3.532 ms | 1,685 | 1.0000 | 2.030 ms | 2.395 ms | 1,009 |
| IVF (k-means routing, IVF-HNSW) | 0.9741 | 18.177 ms | 21.607 ms | 510 | N/A | N/A | N/A | N/A |
| IVF-PQ (compressed) | 0.9773 | 5.175 ms | 5.707 ms | 955 | N/A | N/A | N/A | N/A |
| HNSW + scalar int8 quantization (rescore) | 0.9959 | 0.551 ms | 0.669 ms | 7,998 | 0.9966 | 0.349 ms | 0.458 ms | 7,156 |
| HNSW + product quantization x16 (rescore) | 0.9892 | 0.904 ms | 1.077 ms | 6,030 | 0.9936 | 0.558 ms | 0.621 ms | 4,783 |
| HNSW + binary quantization (rescore, 2x oversampling) | 0.9803 | 0.575 ms | 0.785 ms | 7,511 | 0.9944 | 0.279 ms | 0.316 ms | 7,444 |
| HNSW + binary storage, int4 query (rescore, 2x oversampling) | 0.9925 | 0.644 ms | 0.805 ms | 5,424 | 0.9964 | 0.286 ms | 0.334 ms | 7,529 |
| HNSW + binary storage, int8 query (rescore, 2x oversampling) | 0.9939 | 0.676 ms | 0.849 ms | 6,697 | 0.9971 | 0.296 ms | 0.342 ms | 7,341 |
| HNSW + product quantization x4 (rescore) | 0.9958 | 1.757 ms | 2.135 ms | 3,902 | 0.9908 | 1.285 ms | 1.517 ms | 1,799 |
| HNSW + product quantization x8 (rescore) | 0.9947 | 1.221 ms | 1.492 ms | 5,557 | 0.9896 | 0.826 ms | 0.927 ms | 2,987 |
| HNSW + product quantization x32 (rescore) | 0.9528 | 0.683 ms | 0.822 ms | 6,429 | 0.9874 | 0.439 ms | 0.506 ms | 5,816 |
| HNSW + product quantization x64 (rescore) | 0.8571 | 0.595 ms | 0.730 ms | 8,799 | 0.9136 | 0.381 ms | 0.444 ms | 6,874 |

## `hnsw_ef` sweeps

The same Search beam on both engines. The baseline uses each named preset's efSearch (explicitly sent on Qdrant), raised to the result count on NornicDB.

### HNSW fast (M=16, efConstruction=100, efSearch=50)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9947 | 0.811 ms | 6,322 | 0.9977 | 0.580 ms | 3,318 |
| 16 | 0.9705 | 0.486 ms | 8,920 | 0.9831 | 0.417 ms | 6,064 |
| 32 | 0.9909 | 0.631 ms | 7,919 | 0.9940 | 0.517 ms | 3,737 |
| 64 | 0.9958 | 0.902 ms | 4,594 | 0.9988 | 0.674 ms | 3,301 |
| 128 | 0.9988 | 1.370 ms | 3,691 | 0.9998 | 0.984 ms | 2,049 |
| 256 | 0.9998 | 2.231 ms | 2,214 | 1.0000 | 1.510 ms | 1,214 |
| 512 | 1.0000 | 3.811 ms | 1,274 | 1.0000 | 2.311 ms | 812 |

### HNSW balanced (M=16, efConstruction=200, efSearch=100)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9992 | 1.207 ms | 4,046 | 0.9993 | 0.931 ms | 1,976 |
| 16 | 0.9728 | 0.490 ms | 8,615 | 0.9834 | 0.434 ms | 4,318 |
| 32 | 0.9927 | 0.666 ms | 7,612 | 0.9942 | 0.549 ms | 4,419 |
| 64 | 0.9984 | 0.936 ms | 5,198 | 0.9987 | 0.723 ms | 2,813 |
| 128 | 0.9992 | 1.416 ms | 3,252 | 0.9996 | 1.098 ms | 1,901 |
| 256 | 0.9998 | 2.252 ms | 1,976 | 0.9999 | 1.688 ms | 1,187 |
| 512 | 1.0000 | 3.777 ms | 1,137 | 0.9999 | 2.687 ms | 747 |

### HNSW accurate (M=32, efConstruction=400, efSearch=200)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9997 | 2.803 ms | 1,685 | 1.0000 | 2.030 ms | 1,009 |
| 16 | 0.9837 | 0.612 ms | 7,388 | 0.9923 | 0.532 ms | 4,284 |
| 32 | 0.9965 | 0.857 ms | 5,564 | 0.9984 | 0.676 ms | 3,367 |
| 64 | 0.9991 | 1.291 ms | 3,840 | 0.9994 | 0.976 ms | 2,088 |
| 128 | 0.9994 | 2.088 ms | 2,258 | 0.9998 | 1.511 ms | 1,328 |
| 256 | 0.9999 | 3.395 ms | 1,360 | 1.0000 | 2.343 ms | 861 |
| 512 | 1.0000 | 5.662 ms | 813 | 1.0000 | 3.611 ms | 565 |

### HNSW + scalar int8 quantization (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9959 | 0.551 ms | 7,998 | 0.9966 | 0.349 ms | 7,156 |
| 16 | 0.9725 | 0.412 ms | 7,818 | 0.9840 | 0.272 ms | 7,419 |
| 32 | 0.9931 | 0.475 ms | 8,935 | 0.9941 | 0.294 ms | 6,118 |
| 64 | 0.9972 | 0.583 ms | 8,270 | 0.9977 | 0.329 ms | 6,909 |
| 128 | 0.9992 | 0.786 ms | 6,314 | 0.9992 | 0.401 ms | 5,951 |
| 256 | 0.9998 | 1.139 ms | 5,513 | 0.9997 | 0.567 ms | 3,926 |
| 512 | 1.0000 | 1.751 ms | 3,743 | 0.9999 | 0.799 ms | 2,698 |

### HNSW + product quantization x16 (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9892 | 0.904 ms | 6,030 | 0.9936 | 0.558 ms | 4,783 |
| 16 | 0.9700 | 0.734 ms | 7,765 | 0.9783 | 0.467 ms | 5,421 |
| 32 | 0.9859 | 0.810 ms | 7,662 | 0.9906 | 0.507 ms | 5,103 |
| 64 | 0.9914 | 0.939 ms | 6,767 | 0.9947 | 0.580 ms | 4,527 |
| 128 | 0.9928 | 1.220 ms | 5,230 | 0.9960 | 0.683 ms | 3,711 |
| 256 | 0.9932 | 1.636 ms | 4,178 | 0.9966 | 0.849 ms | 2,982 |
| 512 | 0.9936 | 2.375 ms | 3,039 | 0.9967 | 1.213 ms | 2,329 |

### HNSW + binary quantization (rescore, 2x oversampling)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9803 | 0.575 ms | 7,511 | 0.9944 | 0.279 ms | 7,444 |
| 16 | 0.9732 | 0.517 ms | 7,603 | 0.9796 | 0.269 ms | 6,970 |
| 32 | 0.9741 | 0.492 ms | 7,010 | 0.9889 | 0.275 ms | 7,426 |
| 64 | 0.9805 | 0.552 ms | 8,778 | 0.9954 | 0.289 ms | 7,160 |
| 128 | 0.9822 | 0.647 ms | 7,560 | 0.9982 | 0.318 ms | 6,739 |
| 256 | 0.9822 | 0.768 ms | 7,100 | 0.9989 | 0.379 ms | 6,210 |
| 512 | 0.9828 | 0.974 ms | 5,774 | 0.9989 | 0.546 ms | 3,723 |

### HNSW + binary storage, int4 query (rescore, 2x oversampling)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9925 | 0.644 ms | 5,424 | 0.9964 | 0.286 ms | 7,529 |
| 16 | 0.9884 | 0.563 ms | 8,569 | 0.9865 | 0.276 ms | 7,204 |
| 32 | 0.9886 | 0.579 ms | 8,199 | 0.9932 | 0.275 ms | 7,310 |
| 64 | 0.9937 | 0.646 ms | 7,846 | 0.9977 | 0.292 ms | 7,470 |
| 128 | 0.9952 | 0.737 ms | 6,901 | 0.9989 | 0.323 ms | 6,872 |
| 256 | 0.9955 | 0.891 ms | 5,961 | 0.9997 | 0.387 ms | 6,060 |
| 512 | 0.9959 | 1.235 ms | 4,886 | 0.9998 | 0.529 ms | 3,902 |

### HNSW + binary storage, int8 query (rescore, 2x oversampling)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9939 | 0.676 ms | 6,697 | 0.9971 | 0.296 ms | 7,341 |
| 16 | 0.9879 | 0.614 ms | 7,391 | 0.9862 | 0.280 ms | 7,601 |
| 32 | 0.9887 | 0.622 ms | 8,432 | 0.9932 | 0.280 ms | 7,207 |
| 64 | 0.9948 | 0.729 ms | 6,560 | 0.9978 | 0.299 ms | 7,158 |
| 128 | 0.9967 | 0.862 ms | 6,898 | 0.9997 | 0.339 ms | 6,836 |
| 256 | 0.9969 | 1.110 ms | 5,494 | 1.0000 | 0.412 ms | 5,745 |
| 512 | 0.9971 | 1.508 ms | 4,219 | 1.0000 | 0.625 ms | 4,094 |

### HNSW + product quantization x4 (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9958 | 1.757 ms | 3,902 | 0.9908 | 1.285 ms | 1,799 |
| 16 | 0.9736 | 1.299 ms | 4,983 | 0.9730 | 1.017 ms | 2,196 |
| 32 | 0.9929 | 1.552 ms | 4,108 | 0.9870 | 1.136 ms | 1,897 |
| 64 | 0.9972 | 1.911 ms | 3,569 | 0.9929 | 1.357 ms | 1,791 |
| 128 | 0.9992 | 2.761 ms | 2,598 | 0.9955 | 1.746 ms | 1,432 |
| 256 | 0.9998 | 4.249 ms | 1,679 | 0.9963 | 2.458 ms | 1,118 |
| 512 | 1.0000 | 6.904 ms | 1,105 | 0.9964 | 3.841 ms | 868 |

### HNSW + product quantization x8 (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9947 | 1.221 ms | 5,557 | 0.9896 | 0.826 ms | 2,987 |
| 16 | 0.9726 | 0.934 ms | 6,471 | 0.9673 | 0.701 ms | 3,318 |
| 32 | 0.9920 | 1.039 ms | 6,144 | 0.9852 | 0.766 ms | 3,262 |
| 64 | 0.9968 | 1.379 ms | 5,162 | 0.9906 | 0.842 ms | 2,925 |
| 128 | 0.9990 | 1.728 ms | 4,110 | 0.9918 | 1.047 ms | 2,380 |
| 256 | 0.9994 | 2.485 ms | 2,776 | 0.9920 | 1.392 ms | 1,832 |
| 512 | 0.9996 | 3.912 ms | 1,859 | 0.9921 | 2.013 ms | 1,366 |

### HNSW + product quantization x32 (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9528 | 0.683 ms | 6,429 | 0.9874 | 0.439 ms | 5,816 |
| 16 | 0.9378 | 0.571 ms | 8,100 | 0.9709 | 0.389 ms | 6,620 |
| 32 | 0.9508 | 0.620 ms | 8,579 | 0.9843 | 0.374 ms | 6,281 |
| 64 | 0.9539 | 0.699 ms | 4,859 | 0.9880 | 0.399 ms | 6,047 |
| 128 | 0.9553 | 0.856 ms | 7,253 | 0.9891 | 0.500 ms | 4,757 |
| 256 | 0.9556 | 1.123 ms | 5,691 | 0.9903 | 0.612 ms | 4,024 |
| 512 | 0.9555 | 1.554 ms | 4,144 | 0.9904 | 0.773 ms | 3,352 |

### HNSW + product quantization x64 (rescore)

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.8571 | 0.595 ms | 8,799 | 0.9136 | 0.381 ms | 6,874 |
| 16 | 0.8435 | 0.515 ms | 8,474 | 0.8899 | 0.318 ms | 7,208 |
| 32 | 0.8535 | 0.542 ms | 7,311 | 0.9088 | 0.328 ms | 6,826 |
| 64 | 0.8576 | 0.620 ms | 8,331 | 0.9155 | 0.356 ms | 6,561 |
| 128 | 0.8590 | 0.679 ms | 6,909 | 0.9191 | 0.382 ms | 6,205 |
| 256 | 0.8592 | 0.844 ms | 7,456 | 0.9202 | 0.482 ms | 5,237 |
| 512 | 0.8595 | 1.128 ms | 5,267 | 0.9208 | 0.631 ms | 3,742 |

## Build and footprint

| Mode | Engine | Strategy | Load | Load rate | Index ready | Disk | RSS after queries |
|---|---|---|---:|---:|---:|---:|---:|
| Exact (brute force) | NornicDB | CPU brute-force | 12.6 s | 6,970 vec/s | 4.2 s | 1,213 MB | 3,749 MB |
| Exact (brute force) | Qdrant | exact search (exact=true) | 5.3 s | 16,623 vec/s | 1.0 s | 437 MB | 902 MB |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | NornicDB | HNSW | 12.8 s | 6,872 vec/s | 49.4 s | 1,238 MB | 3,723 MB |
| HNSW fast (M=16, efConstruction=100, efSearch=50) | Qdrant | HNSW m=16 ef_construct=100 | 5.4 s | 16,259 vec/s | 4.1 s | 440 MB | 539 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | NornicDB | HNSW | 12.7 s | 6,930 vec/s | 71.1 s | 1,239 MB | 3,746 MB |
| HNSW balanced (M=16, efConstruction=200, efSearch=100) | Qdrant | HNSW m=16 ef_construct=200 | 5.2 s | 16,820 vec/s | 11.1 s | 442 MB | 538 MB |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | NornicDB | HNSW | 12.7 s | 6,957 vec/s | 144.8 s | 1,238 MB | 3,760 MB |
| HNSW accurate (M=32, efConstruction=400, efSearch=200) | Qdrant | HNSW m=32 ef_construct=400 | 5.3 s | 16,540 vec/s | 20.2 s | 443 MB | 542 MB |
| IVF (k-means routing, IVF-HNSW) | NornicDB | IVF-HNSW (per-cluster HNSW) | 13.0 s | 6,770 vec/s | 54.4 s | 1,238 MB | 3,877 MB |
| IVF-PQ (compressed) | NornicDB | IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=1m42.6493685s] | 12.9 s | 6,815 vec/s | 106.9 s | 1,651 MB | 3,894 MB |
| HNSW + scalar int8 quantization (rescore) | NornicDB | HNSW scalar int8 (90623340 encoded bytes) | 13.0 s | 6,804 vec/s | 36.8 s | 1,631 MB | 3,221 MB |
| HNSW + scalar int8 quantization (rescore) | Qdrant | HNSW m=16 ef_construct=100 + scalar quantization | 5.0 s | 17,642 vec/s | 2.0 s | 531 MB | 675 MB |
| HNSW + product quantization x16 (rescore) | NornicDB | HNSW PQ x16 (23616256 encoded bytes) | 13.2 s | 6,679 vec/s | 40.5 s | 1,632 MB | 3,180 MB |
| HNSW + product quantization x16 (rescore) | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.3 s | 20,422 vec/s | 28.3 s | 480 MB | 579 MB |
| HNSW + binary quantization (rescore, 2x oversampling) | NornicDB | HNSW binary (11283840 encoded bytes) | 13.0 s | 6,758 vec/s | 35.4 s | 1,629 MB | 3,181 MB |
| HNSW + binary quantization (rescore, 2x oversampling) | Qdrant | HNSW m=16 ef_construct=100 + binary quantization | 5.7 s | 15,345 vec/s | 1.0 s | 452 MB | 595 MB |
| HNSW + binary storage, int4 query (rescore, 2x oversampling) | NornicDB | HNSW binary query int4 (11283840 encoded bytes) | 13.1 s | 6,744 vec/s | 35.9 s | 1,629 MB | 3,180 MB |
| HNSW + binary storage, int4 query (rescore, 2x oversampling) | Qdrant | HNSW m=16 ef_construct=100 + binary quantization | 5.6 s | 15,711 vec/s | 1.0 s | 454 MB | 597 MB |
| HNSW + binary storage, int8 query (rescore, 2x oversampling) | NornicDB | HNSW binary query int8 (11283840 encoded bytes) | 12.9 s | 6,816 vec/s | 35.6 s | 1,629 MB | 3,221 MB |
| HNSW + binary storage, int8 query (rescore, 2x oversampling) | Qdrant | HNSW m=16 ef_construct=100 + binary quantization | 5.8 s | 15,247 vec/s | 1.0 s | 453 MB | 594 MB |
| HNSW + product quantization x4 (rescore) | NornicDB | HNSW PQ x4 (91319296 encoded bytes) | 13.1 s | 6,726 vec/s | 44.7 s | 1,631 MB | 3,202 MB |
| HNSW + product quantization x4 (rescore) | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.2 s | 20,842 vec/s | 79.8 s | 546 MB | 650 MB |
| HNSW + product quantization x8 (rescore) | NornicDB | HNSW PQ x8 (46183936 encoded bytes) | 12.9 s | 6,809 vec/s | 42.5 s | 1,630 MB | 3,204 MB |
| HNSW + product quantization x8 (rescore) | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.4 s | 20,216 vec/s | 55.6 s | 503 MB | 600 MB |
| HNSW + product quantization x32 (rescore) | NornicDB | HNSW PQ x32 (12332416 encoded bytes) | 13.0 s | 6,757 vec/s | 38.8 s | 1,630 MB | 3,169 MB |
| HNSW + product quantization x32 (rescore) | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.3 s | 20,399 vec/s | 23.2 s | 470 MB | 567 MB |
| HNSW + product quantization x64 (rescore) | NornicDB | HNSW PQ x64 (6690496 encoded bytes) | 13.0 s | 6,805 vec/s | 38.4 s | 1,630 MB | 3,222 MB |
| HNSW + product quantization x64 (rescore) | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.4 s | 20,020 vec/s | 16.2 s | 464 MB | 557 MB |

## Notes

- **Strategy:** NornicDB's observed pipeline strategy is checked against the requested mode. Qdrant's collection configuration and request parameters are checked; its internal per-query planner is not observed. Qdrant uses its minimum supported full-scan threshold (10 KB), so small populations may still use exact scanning.
- **Index ready**: NornicDB's search index built and its first search answered; Qdrant's collection green with every vector indexed (`indexing_threshold` is 1 KB for ANN modes; exact mode has HNSW disabled).
- **Disk** counts allocated blocks and **RSS** is a process snapshot after all queries and a final readiness check; neither is a peak measurement.
- NornicDB's quantization is IVF-PQ (`compressed`); Qdrant quantizes on top of HNSW. The modes are listed separately, not as equivalents.
