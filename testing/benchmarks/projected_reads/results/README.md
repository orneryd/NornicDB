# Default projected-read comparison

Release `a492f2acd` → PR runtime `9653bb773`. No feature flag or experimental format is used. Both builds read fresh copies of identical ordinary-format fixtures. Query-result caching is disabled; normal storage/parser/OS caches remain.

At eight clients, mixed-point p95 is **0.727 → 0.655 ms** (-10.0%), and throughput is **13,461 → 14,300 queries/s** (+6.2%).

### Mixed corpus: eight clients

| Workload | Release p95 ms | PR p95 ms | Release p99 ms | PR p99 ms |
| --- | --- | --- | --- | --- |
| Document point | 0.780 | 0.718 | 1.108 | 1.174 |
| Product point | 0.859 | 0.782 | 2.039 | 1.539 |
| 50/50 mixed point | 0.727 | 0.655 | 1.011 | 0.887 |
| Batch: 16 documents | 1.626 | 1.563 | 5.527 | 10.215 |
| Widen to large fields | 0.712 | 0.878 | 1.241 | 1.710 |
| Repeat large fields | 0.792 | 0.716 | 1.311 | 1.176 |

Four alternating trials, 8,000 measured calls per phase and 64 warm-ups. Values are medians of per-run statistics, not pooled percentiles. Batch p99 is worse despite lower p95; this is not a universal latency improvement.

### Sustained mixed concurrency

| Clients | Queries/s: release → PR | p95 ms: release → PR | p99 ms: release → PR | Peak RSS MiB: release → PR |
| --- | --- | --- | --- | --- |
| 8 | 12,698 → 12,990 | 0.810 → 0.748 | 2.020 → 1.603 | 2,850 → 2,420 |
| 16 | 12,833 → 13,531 | 2.198 → 1.979 | 21.559 → 21.921 | 2,868 → 2,418 |
| 32 | 13,163 → 13,848 | 5.125 → 4.910 | 25.773 → 26.832 | 2,869 → 2,427 |

120,000 measured calls per run, 8,192 warm-ups and two alternating trials. Peak RSS is sampled from the server every 10 ms. Throughput includes client fingerprint work and shares the four-CPU quota; query latency excludes fingerprint hashing.

The equal process-cold large-field control improves p95 from **0.908 to 0.882 ms** at eight clients. The first large-field phase in the mixed sequence instead pays for widening previously narrow images; the release had already cached those fields. The repeated large-field phase follows that fill. These controls return body/noise/metrics properties, not whole entities. Process-cold does not mean cold device caches.

### Northwind: all 14 queries

| Query | Release mean ms | PR mean ms | Change |
| --- | --- | --- | --- |
| `products_per_category` | 1,185.61 | 993.03 | -16.2% |
| `customer_category_distinct_orders` | 8,860.91 | 8,183.04 | -7.7% |
| `optional_match_orders_count` | 7,508.89 | 7,292.23 | -2.9% |
| `revenue_by_product` | 6,323.68 | 5,348.91 | -15.4% |
| `products_by_supplier` | 1,149.41 | 1,025.76 | -10.8% |
| `orders_by_customer` | 1,205.09 | 974.01 | -19.2% |
| `revenue_by_category` | 7,164.70 | 6,350.29 | -11.4% |
| `revenue_by_supplier` | 6,906.32 | 6,372.56 | -7.7% |
| `revenue_by_customer` | 7,405.82 | 6,491.28 | -12.3% |
| `order_line_sales_by_country` | 6,158.02 | 5,241.46 | -14.9% |
| `low_stock_products` | 428.66 | 439.39 | +2.5% |
| `products_in_category` | 4.26 | 4.57 | +7.3% |
| `order_line_quantity_distribution` | 6,762.89 | 5,458.11 | -19.3% |
| `customer_order_details` | 1.66 | 1.78 | +7.2% |
| **Overall mean** | **4,361.85** | **3,869.74** | **-11.3%** |

Two alternating-order trials, three measured calls per query per trial and one reference/warm-up call. These six observations per query do not establish stable tail percentiles. All row counts and canonical result fingerprints match.

Northwind median sampled peak RSS: **4,148 → 4,222 MiB**.

### Memory profiles

| Separate profiling pass | Allocated MiB: release → PR | Change | Retained heap MiB: release → PR | Change |
| --- | --- | --- | --- | --- |
| [Mixed: 20,000 calls + 64 warm-ups, 8 clients](mixed-memory/) | 634.9 → 553.6 | -12.8% | 524.3 → 496.7 | -5.3% |
| [Northwind: reference + 1 measured call/query](northwind-memory/) | 34,907.5 → 34,817.5 | -0.3% | 535.7 → 539.0 | +0.6% |

Allocated bytes are cumulative sampled allocations, not live RAM. Heap snapshots follow two explicit GCs. Northwind allocations are essentially unchanged and its sampled retained heap is slightly higher; no Northwind memory reduction is established. Profiling-pass latency fields are excluded from timing tables. Raw `.pb.gz` snapshots and allocation/heap top reports are linked in the dataset folders above.

### Single-client results

| Workload | Mean ms: release → PR | p95 ms: release → PR | p99 ms: release → PR | Queries/s: release → PR |
| --- | --- | --- | --- | --- |
| Document point | 0.412 → 0.401 | 0.484 → 0.470 | 0.529 → 0.526 | 2,401 → 2,467 |
| Product point | 0.434 → 0.421 | 0.471 → 0.463 | 0.506 → 0.493 | 2,278 → 2,346 |
| 50/50 mixed point | 0.396 → 0.398 | 0.450 → 0.447 | 0.494 → 0.489 | 2,491 → 2,479 |
| Batch: 16 documents | 0.605 → 0.582 | 0.704 → 0.664 | 0.860 → 0.882 | 1,582 → 1,641 |
| Widen to large fields | 0.418 → 0.449 | 0.461 → 0.522 | 0.503 → 0.584 | 1,968 → 1,848 |
| Repeat large fields | 0.422 → 0.421 | 0.478 → 0.465 | 0.524 → 0.509 | 1,948 → 1,955 |

### Method and reproduction

- Go 1.27.3, Linux/amd64, `noui,nolocalllm`, four CPUs, 10 GiB container memory, `GOGC=400`, Go soft limit 8 GiB. No builds, tests or pprof processes overlapped timed queries.
- The mixed fixture has 97,440 Northwind nodes and 312,050 relationships, plus 4,352 document records. The pure Northwind fixture omits document records.
- Both source copies use the same table layout: combined fixture in L6; pure Northwind in four L0 tables. No data-format changes or storage-size gains are claimed.
- Frozen documentation comes from `ddc59dc20:docs` with CRLF line endings: 272 files × 16 replicas. Corpus SHA-256: `2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`. Both binaries verified all stored document fields against that source.
- Document identities, text, status, 4 KiB independent byte noise and numeric lists are deterministic. The [file manifest](corpus-manifest.json), [binary provenance](provenance.json) and [fixture receipt](fixture-receipt.json) identify the inputs.
- The format-compatible Badger fork changes only its integration note relative to upstream v4.9.6. No fork-runtime optimization is included in this comparison.

See the [harness instructions](../README.md). Each dataset folder records the full command settings in `metadata.json` and contains every paired result receipt. `verify/` contains whole-corpus verification output. Regenerate this report with `python3 testing/benchmarks/projected_reads/summarize.py`.

Inspect a memory capture locally after downloading its folder:

```sh
go tool pprof -top -sample_index=alloc_space -base mixed-memory/old-8-1-alloc-before.pb.gz mixed-memory/old-8-1-alloc-after.pb.gz
go tool pprof -top -sample_index=inuse_space mixed-memory/new-8-1-heap-after.pb.gz
```
