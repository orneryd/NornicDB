# NornicDB vs Neo4j — Northwind Benchmark Comparison

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations per query: **10** (after 2 warmup iterations)

## Summary

| Metric | NornicDB | Neo4j | Delta | Ratio |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.23 | 98.64 | -99.8% | 432.38× |
| Overall throughput (ops/sec) | 17.70 | 7.76 | +128.1% | 2.28× |
| Seed duration (ms) | 6,795.99 | 5,750.07 | +18.2% | 0.85× |
| Avg CPU power (mW) | 8,923.47 | 8,608.14 | +3.7% | 0.96× |
| Avg GPU power (mW) | 39.93 | 95.37 | -58.1% | 2.39× |
| Avg package power (mW) | 8,963.41 | 8,703.51 | +3.0% | 0.97× |
| Energy during benchmark (J) | 118.00 | 264.87 | -55.5% | 2.24× |
| Benchmark wall-clock (s) | 14.00 | 31.63 | -55.7% | 2.26× |
| Peak memory used (bytes) | 32.5 GiB | 33.4 GiB | -2.8% | 1.03× |
| Raw data files (bytes) | 139,575,296 | 53,207,040 | +162.3% | 0.38× |

_Delta = (NornicDB − Neo4j) / Neo4j. Ratio compares Neo4j to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to Neo4j for throughput (higher is better)._

## Per-Query Latency

| Query | NornicDB mean (ms) | Neo4j mean (ms) | Delta | NornicDB P95 | Neo4j P95 | NornicDB ops/s | Neo4j ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.23 | 9.34 | -97.6% | 0.26 | 12.82 | 4,022.12 | 106.74 |
| `customer_category_distinct_orders` | 0.25 | 226.67 | -99.9% | 0.27 | 259.13 | 3,946.72 | 4.41 |
| `optional_match_orders_count` | 0.20 | 72.58 | -99.7% | 0.24 | 84.98 | 4,467.36 | 13.77 |
| `revenue_by_product` | 0.24 | 85.96 | -99.7% | 0.33 | 93.62 | 4,170.79 | 11.63 |

## Correctness

**Seed verification.** Post-seed counts reported by each database (via `MATCH (n:Label) RETURN count(n)` and equivalent edge queries).

| Entity | NornicDB | Neo4j | Match |
|---|---:|---:|:---:|
| Category | 96 | 96 | ✅ |
| Supplier | 144 | 144 | ✅ |
| Customer | 1,200 | 1,200 | ✅ |
| Product | 48,000 | 48,000 | ✅ |
| Order | 48,000 | 48,000 | ✅ |
| PART_OF | 48,000 | 48,000 | ✅ |
| SUPPLIES | 48,000 | 48,000 | ✅ |
| PURCHASED | 48,000 | 48,000 | ✅ |
| ORDERS | 168,050 | 168,050 | ✅ |

**Per-query result fingerprints.** Each engine runs the query on the first (warmup) iteration, canonicalises the full result set, and hashes it with SHA-256. A matching row_count + matching hash means the engines returned the same data.

| Query | NornicDB rows | Neo4j rows | NornicDB hash | Neo4j hash | Match |
|---|---:|---:|---|---|:---:|
| `products_per_category` | 96 | 96 | `91e9f1f06368…` | `91e9f1f06368…` | ✅ |
| `customer_category_distinct_orders` | 10 | 10 | `5da36214d516…` | `5da36214d516…` | ✅ |
| `optional_match_orders_count` | 100 | 100 | `8950fcdaab16…` | `8950fcdaab16…` | ✅ |
| `revenue_by_product` | 10 | 10 | `60b64c678f4c…` | `60b64c678f4c…` | ✅ |

**Intra-run stability.** Every iteration of each query re-fingerprints its result set; a mismatch within a single engine's run is flagged below.

- No intra-run mismatches on either engine.

✅ **All correctness checks passed** — both engines seeded identically and returned identical result sets (by row count and canonical SHA-256 fingerprint) for every benchmark query.

## Storage

Raw data files only (preallocated scratch, WAL, and indexes excluded from the headline):

| Bucket | NornicDB | Neo4j |
|---|---:|---:|
| **Raw data** | 133.1 MiB (139,575,296 B) | 50.7 MiB (53,207,040 B) |
| Indexes / stats | 0 B (0 B) | 7.5 MiB (7,823,360 B) |
| Write-ahead logs | 260.0 KiB (266,240 B) | 144.4 MiB (151,363,584 B) |
| Metadata | 8.0 KiB (8,192 B) | 1.1 MiB (1,191,936 B) |
| _Scratch (excluded)_ | 1.0 MiB (1,048,576 B) | 4.0 KiB (4,096 B) |
| _Unclassified_ | 0 B (0 B) | 0 B (0 B) |
| Total `du` | 134.4 MiB | 203.7 MiB |

- **Raw data ratio:** 2.62× Neo4j (larger)
- Full-dir ratio (includes scratch/WAL): 0.66× Neo4j

## Power

| | NornicDB | Neo4j |
|---|---:|---:|
| Samples | 13 | 30 |
| Duration (s) | 13.16 | 30.43 |
| CPU avg (mW) | 8,923.5 | 8,608.1 |
| GPU avg (mW) | 39.9 | 95.4 |
| Package avg (mW) | 8,963.4 | 8,703.5 |
| Energy (J) | 118.00 | 264.87 |

## Memory Pressure

System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).

| | NornicDB | Neo4j |
|---|---:|---:|
| Samples | 16 | 34 |
| Avg used (active+wired+compressor) | 31.8 GiB | 32.5 GiB |
| Peak used | 32.5 GiB | 33.4 GiB |
| Avg free | 13.1 GiB | 12.0 GiB |
| Min free | 12.3 GiB | 10.9 GiB |
| Avg compressed (logical) | 17.1 GiB | 17.1 GiB |
| Peak compressed | 17.1 GiB | 17.1 GiB |

## Notes

- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.
- Both databases were freshly initialized before each run; Neo4j was stopped during the NornicDB run, and vice versa, to isolate measurements.
- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.
- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). Neo4j raw data = `neostore*store.db*` (record stores). Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and Neo4j empty `*.id` allocation files — are excluded because their size is fixed/preallocated and does not scale with the dataset.