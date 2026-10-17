# Vector ANN benchmark: NornicDB vs Qdrant

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`239b057e7c8797597b757d8a4b948c1af6034572`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Summary

Each mode at its preset beam (explicit on Qdrant; configured preset on NornicDB). Missing cells are unmeasured, not passing runs.

| Mode | NornicDB recall@10 | NornicDB p50 | NornicDB p95 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant p95 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| HNSW fast (M=16, efConstruction=100, efSearch=50) [original-file] | 0.9961 | 2.816 ms | 3.688 ms | 1,025 | N/A | N/A | N/A | N/A |

## `hnsw_ef` sweeps

The same Search beam on both engines. The baseline uses each named preset's efSearch (explicitly sent on Qdrant), raised to the result count on NornicDB.

### HNSW fast (M=16, efConstruction=100, efSearch=50) [original-file]

| hnsw_ef | NornicDB recall@10 | NornicDB p50 | NornicDB QPS | Qdrant recall@10 | Qdrant p50 | Qdrant QPS |
|---|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9961 | 2.816 ms | 1,025 | N/A | N/A | N/A |
| 16 | 0.9712 | 1.486 ms | 1,976 | N/A | N/A | N/A |
| 32 | 0.9920 | 2.087 ms | 1,355 | N/A | N/A | N/A |
| 64 | 0.9974 | 3.288 ms | 870 | N/A | N/A | N/A |
| 128 | 0.9992 | 5.556 ms | 505 | N/A | N/A | N/A |
| 256 | 0.9998 | 9.554 ms | 289 | N/A | N/A | N/A |
| 512 | 1.0000 | 16.669 ms | 165 | N/A | N/A | N/A |

## Build and footprint

| Mode | Engine | Strategy | Load | Load rate | Index ready | Disk | RSS after queries |
|---|---|---|---:|---:|---:|---:|---:|
| HNSW fast (M=16, efConstruction=100, efSearch=50) [original-file] | NornicDB | HNSW | 13.0 s | 6,800 vec/s | 35.4 s | 1,629 MB | 3,179 MB |

## Notes

- **Strategy:** NornicDB's observed pipeline strategy is checked against the requested mode. Qdrant's collection configuration and request parameters are checked; its internal per-query planner is not observed. Qdrant uses its minimum supported full-scan threshold (10 KB), so small populations may still use exact scanning.
- **Index ready**: NornicDB's search index built and its first search answered; Qdrant's collection green with every vector indexed (`indexing_threshold` is 1 KB for ANN modes; exact mode has HNSW disabled).
- **Disk** counts allocated blocks and **RSS** is a process snapshot after all queries and a final readiness check; neither is a peak measurement.
- NornicDB's quantization is IVF-PQ (`compressed`); Qdrant quantizes on top of HNSW. The modes are listed separately, not as equivalents.
