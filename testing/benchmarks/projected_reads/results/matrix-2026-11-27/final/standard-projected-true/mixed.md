# Standard layout, projected true — Mixed Corpus Report

## Configuration

- Source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`; `storage: isolated`, `layout: standard`, `projected: true`.
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
| `document_projection` | 0.205 | 0.363 | 0.633 | 0.234 | 33211.9 | 2264.8 |
| `northwind_projection` | 0.246 | 0.423 | 0.673 | 0.272 | 28871.2 | 2333.2 |
| `mixed_projection` | 0.210 | 0.346 | 0.601 | 0.233 | 32539.1 | 2382.1 |
| `document_batch_projection` | 0.462 | 0.934 | 1.765 | 0.536 | 13993.9 | 2587.2 |
| `random_narrow` | 0.193 | 0.377 | 0.774 | 0.221 | 35098.6 | 2631.4 |
| `random_wide` | 0.217 | 0.515 | 1.255 | 0.271 | 22925.3 | 2708.1 |
| `document_full_control` | 0.272 | 0.678 | 1.707 | 0.347 | 17030.4 | 2739.5 |
| `document_full_warm_control` | 0.251 | 0.633 | 1.388 | 0.318 | 18306.1 | 2744.2 |

## Correctness

- Northwind: 14 query fingerprints, all stable: True.
- Mixed: `seeded 4352 documents 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Mixed: `verified all fields against source corpus: 4352 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Per-phase SHA-256 fingerprints are stored in the linked JSON receipts and checked against the other three modes.

## Storage and memory

- Isolated store after Northwind: 303.7 MiB allocated (`du`).
- Isolated store after adding documents and running mixed workload: 474.8 MiB allocated (`du`).
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
