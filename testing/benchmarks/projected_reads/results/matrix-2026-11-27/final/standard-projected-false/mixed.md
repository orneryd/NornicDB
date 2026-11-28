# Standard layout, projected false — Mixed Corpus Report

## Configuration

- Source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`; `storage: isolated`, `layout: standard`, `projected: false`.
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
| `document_projection` | 0.203 | 0.360 | 0.858 | 0.230 | 34041.4 | 1896.6 |
| `northwind_projection` | 0.252 | 0.930 | 1.831 | 0.356 | 21781.1 | 1858.6 |
| `mixed_projection` | 0.241 | 0.812 | 1.967 | 0.336 | 22333.1 | 1751.6 |
| `document_batch_projection` | 0.480 | 1.978 | 2.655 | 0.667 | 11244.9 | 1899.5 |
| `random_narrow` | 0.214 | 0.731 | 1.996 | 0.303 | 25625.7 | 1942.6 |
| `random_wide` | 0.251 | 1.606 | 2.978 | 0.417 | 15963.0 | 2030.4 |
| `document_full_control` | 0.279 | 2.123 | 10.428 | 0.688 | 8913.5 | 2119.6 |
| `document_full_warm_control` | 0.333 | 1.736 | 4.896 | 0.586 | 10578.1 | 2109.1 |

## Correctness

- Northwind: 14 query fingerprints, all stable: True.
- Mixed: `seeded 4352 documents 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Mixed: `verified all fields against source corpus: 4352 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Per-phase SHA-256 fingerprints are stored in the linked JSON receipts and checked against the other three modes.

## Storage and memory

- Isolated store after Northwind: 294.9 MiB allocated (`du`).
- Isolated store after adding documents and running mixed workload: 465.3 MiB allocated (`du`).
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
