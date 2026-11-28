# Experimental layout, projected true — Mixed Corpus Report

## Configuration

- Source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`; `storage: isolated`, `layout: experimental`, `projected: true`.
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
| `document_projection` | 0.200 | 0.362 | 0.707 | 0.230 | 34043.8 | 2593.8 |
| `northwind_projection` | 0.240 | 0.400 | 0.629 | 0.262 | 29866.3 | 2596.8 |
| `mixed_projection` | 0.208 | 0.366 | 0.645 | 0.231 | 33079.6 | 2599.1 |
| `document_batch_projection` | 0.397 | 0.668 | 1.121 | 0.438 | 17100.8 | 2609.8 |
| `random_narrow` | 0.190 | 0.344 | 0.587 | 0.213 | 36745.7 | 2611.8 |
| `random_wide` | 0.211 | 0.498 | 1.121 | 0.259 | 24486.9 | 2615.8 |
| `document_full_control` | 0.246 | 0.615 | 1.330 | 0.307 | 19092.4 | 2620.7 |
| `document_full_warm_control` | 0.246 | 0.629 | 1.303 | 0.308 | 19163.1 | 2629.2 |

## Correctness

- Northwind: 14 query fingerprints, all stable: True.
- Mixed: `seeded 4352 documents 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Mixed: `verified all fields against source corpus: 4352 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Per-phase SHA-256 fingerprints are stored in the linked JSON receipts and checked against the other three modes.

## Storage and memory

- Isolated store after Northwind: 256.1 MiB allocated (`du`).
- Isolated store after adding documents and running mixed workload: 445.2 MiB allocated (`du`).
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
