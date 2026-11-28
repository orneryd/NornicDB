# Experimental layout, projected false — Mixed Corpus Report

## Configuration

- Source: `b0332923ff6112ebde1a94959a50ea400c25f805`; `storage: isolated`, `layout: experimental`, `projected: false`.
- Graph: 97,440 Northwind nodes and 312,050 relationships.
- Documents: 272 frozen Markdown files, replicated 16 times to 4,352 records. Each adds body text, independent 4 KiB byte noise, 256 numeric metrics, and small indexed fields.
- Corpus SHA-256: `2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`.
- Parser: nornic; query cache off; graph-only indexing; 8 clients, 4,000 measured calls and 64 warmups per phase.
- Random narrow reads choose one of seven nonempty subsets of key, path, status and exclude all large properties.
- Random wide reads choose one of seven nonempty subsets of body, noise, metrics and exclude the small properties from results. Identity lookup still uses key.
- Requests choose records and property subsets deterministically; each mode uses the same sequence.

## Query latency and throughput

| Phase | P50 ms | P95 ms | P99 ms | Mean ms | Calls/s | Peak server RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| `document_projection` | 0.203 | 0.377 | 0.612 | 0.229 | 33918.1 | 2278.5 |
| `northwind_projection` | 0.224 | 0.390 | 0.703 | 0.252 | 31192.4 | 2333.5 |
| `mixed_projection` | 0.213 | 0.359 | 0.549 | 0.236 | 31514.0 | 2388.0 |
| `document_batch_projection` | 0.407 | 0.658 | 0.976 | 0.446 | 16834.3 | 2486.6 |
| `random_narrow` | 0.186 | 0.344 | 0.610 | 0.209 | 37302.0 | 2488.5 |
| `random_wide` | 0.222 | 0.567 | 1.552 | 0.284 | 22681.8 | 2502.1 |
| `document_full_control` | 0.259 | 0.669 | 1.518 | 0.331 | 17722.0 | 2507.5 |
| `document_full_warm_control` | 0.248 | 0.637 | 1.460 | 0.315 | 18714.7 | 2516.2 |

## Correctness

- Northwind: 14 query fingerprints, all stable: True.
- Mixed: `seeded 4352 documents 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Mixed: `verified all fields against source corpus: 4352 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Per-phase SHA-256 fingerprints are stored in the linked JSON receipts and checked against the other three modes.

## Storage and memory

- Isolated store after Northwind: 243.2 MiB allocated (`du`).
- Isolated store after adding documents and running mixed workload: 431.6 MiB allocated (`du`).
- Server RSS shown above is process memory sampled during each phase, not resident size attributable only to the current query.

## Raw receipts

- [Northwind report](nornicdb.md) and [Northwind JSON](nornicdb.results.json).
- [Catalog options](catalog.json), [document seed](mixed-seed.log), and [whole-corpus verification](mixed-verify.log).
- [document_projection](document_projection.json).
- [northwind_projection](northwind_projection.json).
- [mixed_projection](mixed_projection.json).
- [document_batch_projection](document_batch_projection.json).
- [random_narrow](random_narrow.json).
- [random_wide](random_wide.json).
- [document_full_control](document_full_control.json).
- [document_full_warm_control](document_full_warm_control.json).

## Limits

- One sequential run per mode on this Mac; phase order is fixed and caches may warm across phases.
- Query result cache was disabled, but filesystem and database caches were not flushed.
- Energy sampling was unavailable; no energy claim is made.
