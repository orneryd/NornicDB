# Experimental layout, projected true — Mixed Corpus Report

## Configuration

- Source: `b0332923ff6112ebde1a94959a50ea400c25f805`; `storage: isolated`, `layout: experimental`, `projected: true`.
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
| `document_projection` | 0.211 | 0.441 | 0.963 | 0.250 | 31292.9 | 1679.3 |
| `northwind_projection` | 0.258 | 0.659 | 1.755 | 0.338 | 22954.6 | 1646.8 |
| `mixed_projection` | 0.217 | 0.457 | 1.110 | 0.258 | 29361.4 | 1469.9 |
| `document_batch_projection` | 0.491 | 1.526 | 2.598 | 0.652 | 11276.6 | 1617.6 |
| `random_narrow` | 0.206 | 0.606 | 1.284 | 0.266 | 29279.4 | 1650.6 |
| `random_wide` | 0.254 | 1.942 | 4.829 | 0.507 | 13145.8 | 1649.9 |
| `document_full_control` | 0.283 | 2.035 | 4.389 | 0.537 | 11497.0 | 1393.2 |
| `document_full_warm_control` | 0.289 | 2.555 | 4.841 | 0.687 | 9003.9 | 1204.1 |

## Correctness

- Northwind: 14 query fingerprints, all stable: True.
- Mixed: `seeded 4352 documents 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Mixed: `verified all fields against source corpus: 4352 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Per-phase SHA-256 fingerprints are stored in the linked JSON receipts and checked against the other three modes.

## Storage and memory

- Isolated store after Northwind: 249.1 MiB allocated (`du`).
- Isolated store after adding documents and running mixed workload: 438.3 MiB allocated (`du`).
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
