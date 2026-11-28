# Standard layout, projected false — Mixed Corpus Report

## Configuration

- Source: `b0332923ff6112ebde1a94959a50ea400c25f805`; `storage: isolated`, `layout: standard`, `projected: false`.
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
| `document_projection` | 0.223 | 0.413 | 0.884 | 0.260 | 29985.8 | 1904.9 |
| `northwind_projection` | 0.249 | 0.482 | 0.788 | 0.284 | 27319.9 | 1964.6 |
| `mixed_projection` | 0.233 | 0.500 | 0.976 | 0.277 | 27062.1 | 2002.7 |
| `document_batch_projection` | 0.521 | 1.359 | 2.536 | 0.656 | 11258.1 | 2121.3 |
| `random_narrow` | 0.274 | 0.845 | 1.812 | 0.415 | 18364.3 | 1990.2 |
| `random_wide` | 0.271 | 1.277 | 2.493 | 0.420 | 15444.1 | 2069.3 |
| `document_full_control` | 0.340 | 2.044 | 3.502 | 0.586 | 10596.7 | 2146.2 |
| `document_full_warm_control` | 0.330 | 2.462 | 4.776 | 0.645 | 9546.9 | 2091.8 |

## Correctness

- Northwind: 14 query fingerprints, all stable: True.
- Mixed: `seeded 4352 documents 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Mixed: `verified all fields against source corpus: 4352 2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`
- Per-phase SHA-256 fingerprints are stored in the linked JSON receipts and checked against the other three modes.

## Storage and memory

- Isolated store after Northwind: 303.4 MiB allocated (`du`).
- Isolated store after adding documents and running mixed workload: 473.8 MiB allocated (`du`).
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
