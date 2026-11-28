# Experimental layout, projected false — Mixed Corpus Report

## Configuration

- Source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`; `storage: isolated`, `layout: experimental`, `projected: false`.
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
| `document_projection` | 0.205 | 0.354 | 0.650 | 0.231 | 33638.6 | 2505.7 |
| `northwind_projection` | 0.225 | 0.393 | 0.648 | 0.251 | 31072.3 | 2507.8 |
| `mixed_projection` | 0.210 | 0.370 | 0.653 | 0.237 | 31493.4 | 2511.8 |
| `document_batch_projection` | 0.398 | 0.660 | 1.155 | 0.437 | 17209.1 | 2520.7 |
| `random_narrow` | 0.197 | 0.370 | 0.696 | 0.233 | 33521.6 | 2524.1 |
| `random_wide` | 0.221 | 0.556 | 1.454 | 0.286 | 22337.8 | 2528.0 |
| `document_full_control` | 0.248 | 0.624 | 1.390 | 0.312 | 18367.1 | 2532.9 |
| `document_full_warm_control` | 0.254 | 0.644 | 1.448 | 0.321 | 18247.6 | 2540.3 |

## Correctness

- Northwind: 14 query fingerprints, all stable: True.
- Mixed: `seeded 4352 documents 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Mixed: `verified all fields against source corpus: 4352 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Per-phase SHA-256 fingerprints are stored in the linked JSON receipts and checked against the other three modes.

## Storage and memory

- Isolated store after Northwind: 237.5 MiB allocated (`du`).
- Isolated store after adding documents and running mixed workload: 425.7 MiB allocated (`du`).
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
