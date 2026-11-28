# Northwind and mixed-corpus 2×2 matrix

## Provenance

- NornicDB PR #1826 head: `b0332923ff6112ebde1a94959a50ea400c25f805`.
- Host: `Darwin louise.local 25.5.0 Darwin Kernel Version 25.5.0: Mon Apr 27 20:39:09 PDT 2026; root:xnu-12377.121.6~2/RELEASE_ARM64_T6020 arm64`; `go version go1.27.4 darwin/arm64`.
- Four fresh isolated stores, same seed, same query and document corpus, one sequential run per cell.
- `layout` and `projected` were explicitly set and checked against `SHOW DATABASES` options.
- Northwind uses 30 measured iterations plus 5 warmups for each of 14 queries. Mixed phases use 4,000 calls, 8 clients, and 64 warmups each.
- Graph-only mode; Nornic parser; query cache off; asynchronous writes on; `GOGC=400`; `GOMEMLIMIT=8GiB`.
- All 14 Northwind result hashes and eight mixed-phase result hashes matched across the four modes; whole document corpus verified per mode.

## Northwind results

| Layout | Projected | Seed s | Query-loop ops/s | Mean query ms | Isolated disk MiB | Reports |
|---|:---:|---:|---:|---:|---:|---|
| standard | false | 12.49 | 0.33 | 2601.14 | 303.4 | [Northwind](standard-projected-false/nornicdb.md) · [Mixed](standard-projected-false/mixed.md) |
| experimental | false | 13.49 | 0.31 | 2726.62 | 243.2 | [Northwind](experimental-projected-false/nornicdb.md) · [Mixed](experimental-projected-false/mixed.md) |
| standard | true | 11.03 | 0.30 | 2873.87 | 309.2 | [Northwind](standard-projected-true/nornicdb.md) · [Mixed](standard-projected-true/mixed.md) |
| experimental | true | 12.32 | 0.28 | 3073.74 | 249.1 | [Northwind](experimental-projected-true/nornicdb.md) · [Mixed](experimental-projected-true/mixed.md) |

Northwind aggregate throughput weights each of the 14 queries equally by call count; use the per-query reports to assess your actual query mix.

## Mixed-corpus results

| Layout | Projected | Random narrow p95 ms | Random narrow p99 ms | Random narrow calls/s | Random wide p95 ms | Random wide p99 ms | Random wide calls/s | Mixed disk MiB |
|---|:---:|---:|---:|---:|---:|---:|---:|---:|
| standard | false | 0.845 | 1.812 | 18364.3 | 1.277 | 2.493 | 15444.1 | 473.8 |
| experimental | false | 0.344 | 0.610 | 37302.0 | 0.567 | 1.552 | 22681.8 | 431.6 |
| standard | true | 0.502 | 1.091 | 29623.5 | 0.808 | 1.559 | 19336.1 | 480.3 |
| experimental | true | 0.606 | 1.284 | 29279.4 | 1.942 | 4.829 | 13145.8 | 438.3 |

## Best under each circumstance

- **Northwind aggregate throughput:** `standard-projected-false` (0.331).
- **Random narrow p95:** `experimental-projected-false` (0.344).
- **Random wide p95:** `experimental-projected-false` (0.567).
- **Mixed-query p95:** `experimental-projected-false` (0.359).
- **Northwind store disk:** `experimental-projected-false` (243.2 MiB).
- **Mixed store disk:** `experimental-projected-false` (431.6 MiB).

These are workload-specific winners from one run, not universal defaults. Security, reliability and compatibility still govern store selection. The `projected` option is an opt-in immutable store setting.

## Measurement limits

- One sequential run per cell; no confidence interval or claims of significance. Run alternating repeats before selecting a production default from small differences.
- Northwind p99 is the high sample of 30 measured calls; inspect absolute tails in each full report.
- No power samples or cold-device-cache trial; the Northwind report marks unavailable power/memory sections accordingly.
- Mixed phases run in the stated order on the same store, so later phases benefit from earlier cache fills.
