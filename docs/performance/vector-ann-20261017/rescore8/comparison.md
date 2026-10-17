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
| HNSW + product quantization x16 (rescore) [rescore8] | 0.9990 | 2.403 ms | 2.825 ms | 2,205 | 0.9988 | 0.651 ms | 0.761 ms | 4,009 |
| HNSW + binary quantization (rescore, 8x oversampling) [rescore8] | 0.9988 | 1.776 ms | 2.066 ms | 2,683 | 0.9980 | 0.364 ms | 0.447 ms | 5,960 |
| HNSW + binary storage, int8 query (rescore, 8x oversampling) [rescore8] | 0.9991 | 1.982 ms | 2.322 ms | 2,561 | 0.9990 | 0.382 ms | 0.479 ms | 5,908 |
| HNSW + product quantization x32 (rescore) [rescore8] | 0.9989 | 2.157 ms | 2.554 ms | 2,276 | 0.9978 | 0.505 ms | 0.593 ms | 5,217 |
| HNSW + product quantization x64 (rescore) [rescore8] | 0.9922 | 1.972 ms | 2.304 ms | 2,453 | 0.9909 | 0.411 ms | 0.537 ms | 6,077 |

## `hnsw_ef` sweeps

The same Search beam on both engines. The baseline uses each named preset's efSearch (explicitly sent on Qdrant), raised to the result count on NornicDB.

### HNSW + product quantization x16 (rescore) [rescore8]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 2.403 ms | 2,205 | 0.9988 | 0.651 ms | 4,009 |
| 16 | 0.9990 | 2.385 ms | 2,173 | 0.9988 | 0.645 ms | 3,975 |
| 32 | 0.9990 | 2.449 ms | 2,069 | 0.9988 | 0.644 ms | 4,034 |
| 64 | 0.9990 | 2.407 ms | 2,160 | 0.9988 | 0.647 ms | 3,935 |
| 128 | 0.9991 | 2.432 ms | 2,193 | 0.9993 | 0.714 ms | 3,577 |
| 256 | 0.9995 | 2.906 ms | 1,831 | 0.9999 | 0.904 ms | 2,500 |
| 512 | 1.0000 | 3.731 ms | 1,509 | 1.0000 | 1.231 ms | 2,277 |

### HNSW + binary quantization (rescore, 8x oversampling) [rescore8]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9988 | 1.776 ms | 2,683 | 0.9980 | 0.364 ms | 5,960 |
| 16 | 0.9988 | 1.729 ms | 2,715 | 0.9980 | 0.326 ms | 5,641 |
| 32 | 0.9988 | 1.735 ms | 2,649 | 0.9980 | 0.324 ms | 6,609 |
| 64 | 0.9988 | 1.736 ms | 2,629 | 0.9980 | 0.328 ms | 6,790 |
| 128 | 0.9985 | 1.760 ms | 2,731 | 0.9989 | 0.352 ms | 6,415 |
| 256 | 0.9990 | 1.893 ms | 2,512 | 0.9999 | 0.419 ms | 4,387 |
| 512 | 0.9992 | 2.168 ms | 2,278 | 0.9999 | 0.571 ms | 3,778 |

### HNSW + binary storage, int8 query (rescore, 8x oversampling) [rescore8]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9991 | 1.982 ms | 2,561 | 0.9990 | 0.382 ms | 5,908 |
| 16 | 0.9991 | 1.953 ms | 2,432 | 0.9990 | 0.351 ms | 6,885 |
| 32 | 0.9991 | 1.948 ms | 2,557 | 0.9990 | 0.330 ms | 6,637 |
| 64 | 0.9991 | 1.947 ms | 2,566 | 0.9990 | 0.332 ms | 5,841 |
| 128 | 0.9992 | 1.987 ms | 2,557 | 0.9995 | 0.362 ms | 6,471 |
| 256 | 0.9996 | 2.257 ms | 2,318 | 0.9998 | 0.449 ms | 3,823 |
| 512 | 0.9998 | 2.791 ms | 1,964 | 0.9999 | 0.599 ms | 4,021 |

### HNSW + product quantization x32 (rescore) [rescore8]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9989 | 2.157 ms | 2,276 | 0.9978 | 0.505 ms | 5,217 |
| 16 | 0.9989 | 2.118 ms | 2,267 | 0.9978 | 0.500 ms | 5,106 |
| 32 | 0.9989 | 2.086 ms | 2,266 | 0.9978 | 0.464 ms | 5,196 |
| 64 | 0.9989 | 2.092 ms | 2,278 | 0.9978 | 0.497 ms | 5,079 |
| 128 | 0.9990 | 2.124 ms | 2,264 | 0.9986 | 0.540 ms | 3,821 |
| 256 | 0.9997 | 2.420 ms | 1,936 | 0.9995 | 0.647 ms | 3,951 |
| 512 | 0.9999 | 2.932 ms | 1,818 | 1.0000 | 0.816 ms | 3,062 |

### HNSW + product quantization x64 (rescore) [rescore8]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9922 | 1.972 ms | 2,453 | 0.9909 | 0.411 ms | 6,077 |
| 16 | 0.9922 | 1.957 ms | 2,518 | 0.9909 | 0.404 ms | 5,109 |
| 32 | 0.9922 | 1.968 ms | 2,487 | 0.9909 | 0.439 ms | 4,738 |
| 64 | 0.9922 | 1.960 ms | 2,475 | 0.9909 | 0.444 ms | 5,996 |
| 128 | 0.9928 | 1.938 ms | 2,438 | 0.9940 | 0.466 ms | 5,442 |
| 256 | 0.9946 | 2.161 ms | 2,290 | 0.9966 | 0.524 ms | 4,678 |
| 512 | 0.9950 | 2.455 ms | 2,114 | 0.9974 | 0.676 ms | 3,809 |

## Build and footprint

| Mode | Engine | Strategy | Load | Load rate | Index ready | Disk | RSS after queries |
|---|---|---|---:|---:|---:|---:|---:|
| HNSW + product quantization x16 (rescore) [rescore8] | NornicDB | HNSW PQ x16 (23616256 encoded bytes) | 13.0 s | 6,775 vec/s | 40.5 s | 1,630 MB | 3,274 MB |
| HNSW + product quantization x16 (rescore) [rescore8] | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.4 s | 20,189 vec/s | 28.3 s | 480 MB | 577 MB |
| HNSW + binary quantization (rescore, 8x oversampling) [rescore8] | NornicDB | HNSW binary (11283840 encoded bytes) | 12.9 s | 6,841 vec/s | 35.4 s | 1,629 MB | 3,199 MB |
| HNSW + binary quantization (rescore, 8x oversampling) [rescore8] | Qdrant | HNSW m=16 ef_construct=100 + binary quantization | 5.9 s | 15,061 vec/s | 1.0 s | 453 MB | 594 MB |
| HNSW + binary storage, int8 query (rescore, 8x oversampling) [rescore8] | NornicDB | HNSW binary query int8 (11283840 encoded bytes) | 13.0 s | 6,793 vec/s | 35.3 s | 1,630 MB | 3,226 MB |
| HNSW + binary storage, int8 query (rescore, 8x oversampling) [rescore8] | Qdrant | HNSW m=16 ef_construct=100 + binary quantization | 5.8 s | 15,301 vec/s | 1.0 s | 454 MB | 595 MB |
| HNSW + product quantization x32 (rescore) [rescore8] | NornicDB | HNSW PQ x32 (12332416 encoded bytes) | 12.9 s | 6,813 vec/s | 38.6 s | 1,629 MB | 3,220 MB |
| HNSW + product quantization x32 (rescore) [rescore8] | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.4 s | 20,166 vec/s | 23.2 s | 470 MB | 568 MB |
| HNSW + product quantization x64 (rescore) [rescore8] | NornicDB | HNSW PQ x64 (6690496 encoded bytes) | 13.1 s | 6,706 vec/s | 38.5 s | 1,629 MB | 3,187 MB |
| HNSW + product quantization x64 (rescore) [rescore8] | Qdrant | HNSW m=16 ef_construct=100 + product quantization | 4.4 s | 20,073 vec/s | 16.2 s | 463 MB | 557 MB |

## Notes

- **Strategy:** NornicDB's observed pipeline strategy is checked against the requested mode. Qdrant's collection configuration and request parameters are checked; its internal per-query planner is not observed. Qdrant uses its minimum supported full-scan threshold (10 KB), so small populations may still use exact scanning.
- **Index ready**: NornicDB's search index built and its first search answered; Qdrant's collection green with every vector indexed (`indexing_threshold` is 1 KB for ANN modes; exact mode has HNSW disabled).
- **Disk** counts allocated blocks and **RSS** is a process snapshot after all queries and a final readiness check; neither is a peak measurement.
- NornicDB's quantization is IVF-PQ (`compressed`); Qdrant quantizes on top of HNSW. The modes are listed separately, not as equivalents.
