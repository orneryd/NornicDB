# Northwind and mixed-corpus 2×2 matrix

## Provenance

- NornicDB PR #1826 head: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`.
- Host: `Darwin louise.local 25.5.0 Darwin Kernel Version 25.5.0: Mon Apr 27 20:39:09 PDT 2026; root:xnu-12377.121.6~2/RELEASE_ARM64_T6020 arm64`; `go version go1.27.4 darwin/arm64`.
- Four fresh isolated stores, same seed, same query and document corpus, one sequential run per cell.
- `layout` and `projected` were explicitly set and checked against `SHOW DATABASES` options.
- Northwind uses 30 measured iterations plus 5 warmups for each of 14 queries. Mixed phases use 4,000 calls, 8 clients, and 64 warmups each.
- Graph-only mode; Nornic parser; query cache off; asynchronous writes on; `GOGC=400`; `GOMEMLIMIT=8GiB`.
- All 14 Northwind result hashes and eight mixed-phase result hashes matched across the four modes; whole document corpus verified per mode.

## Northwind results

| Layout | Projected | Seed s | Query-loop ops/s | Mean query ms | Isolated disk MiB | Reports |
|---|:---:|---:|---:|---:|---:|---|
| standard | false | 12.47 | 0.37 | 2298.58 | 294.9 | [Northwind](standard-projected-false/nornicdb.md) · [Mixed](standard-projected-false/mixed.md) |
| experimental | false | 10.59 | 0.38 | 2247.85 | 237.5 | [Northwind](experimental-projected-false/nornicdb.md) · [Mixed](experimental-projected-false/mixed.md) |
| standard | true | 11.63 | 0.36 | 2343.05 | 303.7 | [Northwind](standard-projected-true/nornicdb.md) · [Mixed](standard-projected-true/mixed.md) |
| experimental | true | 11.63 | 0.33 | 2577.13 | 256.1 | [Northwind](experimental-projected-true/nornicdb.md) · [Mixed](experimental-projected-true/mixed.md) |

Northwind aggregate throughput weights each of the 14 queries equally by call count; use the per-query reports to assess your actual query mix.

## Mixed-corpus results

| Layout | Projected | Random narrow p95 ms | Random narrow p99 ms | Random narrow calls/s | Random wide p95 ms | Random wide p99 ms | Random wide calls/s | Mixed disk MiB |
|---|:---:|---:|---:|---:|---:|---:|---:|---:|
| standard | false | 0.731 | 1.996 | 25625.7 | 1.606 | 2.978 | 15963.0 | 465.3 |
| experimental | false | 0.370 | 0.696 | 33521.6 | 0.556 | 1.454 | 22337.8 | 425.7 |
| standard | true | 0.377 | 0.774 | 35098.6 | 0.515 | 1.255 | 22925.3 | 474.8 |
| experimental | true | 0.344 | 0.587 | 36745.7 | 0.498 | 1.121 | 24486.9 | 445.2 |

## Best under each circumstance

- **Northwind aggregate throughput:** `experimental-projected-false` (0.380).
- **Random narrow p95:** `experimental-projected-true` (0.344).
- **Random wide p95:** `experimental-projected-true` (0.498).
- **Mixed-query p95:** `standard-projected-true` (0.346).
- **Northwind store disk:** `experimental-projected-false` (237.5 MiB).
- **Mixed store disk:** `experimental-projected-false` (425.7 MiB).

These are workload-specific winners from one run, not universal defaults. Security, reliability and compatibility still govern store selection. The `projected` option is an opt-in immutable store setting.

## Choosing a store setting

- Keep `standard, projected: false` for existing stores and the established
  default behavior. It requires no data migration.
- `experimental, projected: false` had the highest Northwind aggregate and
  smallest disk footprint in this run. Its on-disk layout is independently
  opt-in; the small Northwind advantage over standard is not a default-change
  recommendation from one pass.
- Enable `projected: true` on a **new** isolated store when queries routinely
  select a few properties from nodes with large unrelated values. In the
  matched encrypted 2 MiB point-read benchmark, the narrow path requested
  457 logical value-log bytes and took about 6.6 µs versus about 2.1 MiB and
  314 µs for the ordinary full-value path. This is logical requested data,
  not a claim about physical device I/O. Small writes and metadata cost more.
- This mixed corpus has 4 KiB noise and Markdown bodies. The final projected
  modes had good random narrow and wide tails in this run, while the
  experimental nonprojected mode used the least disk. Use the linked phase
  tables and your actual payload sizes to choose.

The [baseline-to-final comparison](comparison.md) gives every query and phase.
The [alternating control audit](paired-control.md) checks the apparent
nonprojected slowdowns from the one-pass matrix; category revenue did not
reproduce as a regression, and wide-read tail spikes occurred on both builds.

## Measurement limits

- One sequential run per cell; no confidence interval or claims of significance. Run alternating repeats before selecting a production default from small differences.
- Northwind p99 is the high sample of 30 measured calls; inspect absolute tails in each full report.
- No power samples or cold-device-cache trial; the Northwind report marks unavailable power/memory sections accordingly.
- Mixed phases run in the stated order on the same store, so later phases benefit from earlier cache fills.
