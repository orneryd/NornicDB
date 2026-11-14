# NornicDB Parser Modes — Northwind Query Latency (default vs ANTLR)

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations/query: **30** (**5 warmup**)

## NornicDB Parser Modes: default vs ANTLR

Query latency and throughput of the two `NORNICDB_PARSER` modes over the same seeded Northwind graph and the same workload. Each mode ran serialized from a freshly wiped data directory. Seeding, storage, power and memory are not compared here.

| Metric | NornicDB (default) | NornicDB (ANTLR) | Delta | ANTLR slower by |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.39 | 0.38 | -1.6% | 0.98× |
| End-to-end query-loop throughput (ops/sec) | 8.25 | 8.03 | -2.7% | 1.03× |
| Query-latency-only aggregate throughput (ops/sec) | 2,580.90 | 2,621.79 | +1.6% | 0.98× |
| Query-loop duration (s) | 50.892 | 52.297 | +2.8% | 1.03× |

_Delta = (ANTLR − default) / default. "ANTLR slower by" is ANTLR ÷ default for latency and default ÷ ANTLR for throughput, so values above 1.00× mean ANTLR mode is slower._

### Per-query latency

| Query | Mean default (ms) | Mean ANTLR (ms) | Mean × | Median default (ms) | Median ANTLR (ms) | P95 default (ms) | P95 ANTLR (ms) | P99 default (ms) | P99 ANTLR (ms) | Ops/sec default | Ops/sec ANTLR | Same result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `products_per_category` | 0.43 | 0.42 | 0.98× | 0.41 | 0.41 | 0.51 | 0.47 | 0.56 | 0.56 | 2,042.74 | 2,108.12 | ✅ |
| `customer_category_distinct_orders` | 0.37 | 0.35 | 0.96× | 0.36 | 0.34 | 0.41 | 0.39 | 0.45 | 0.39 | 2,635.10 | 2,752.02 | ✅ |
| `optional_match_orders_count` | 0.46 | 0.40 | 0.87× | 0.45 | 0.40 | 0.51 | 0.42 | 0.72 | 0.45 | 1,926.79 | 2,215.04 | ✅ |
| `revenue_by_product` | 0.34 | 0.34 | 1.02× | 0.33 | 0.34 | 0.35 | 0.37 | 0.38 | 0.38 | 2,888.31 | 2,826.70 | ✅ |
| `products_by_supplier` | 0.34 | 0.35 | 1.02× | 0.34 | 0.35 | 0.37 | 0.37 | 0.40 | 0.40 | 2,781.10 | 2,732.83 | ✅ |
| `orders_by_customer` | 0.34 | 0.34 | 0.99× | 0.34 | 0.34 | 0.38 | 0.36 | 0.39 | 0.36 | 2,785.46 | 2,824.22 | ✅ |
| `revenue_by_category` | 0.41 | 0.40 | 0.98× | 0.40 | 0.40 | 0.47 | 0.43 | 0.48 | 0.44 | 2,142.80 | 2,182.65 | ✅ |
| `revenue_by_supplier` | 0.36 | 0.35 | 0.99× | 0.36 | 0.35 | 0.39 | 0.38 | 0.40 | 0.39 | 2,655.07 | 2,684.61 | ✅ |
| `revenue_by_customer` | 0.37 | 0.36 | 0.98× | 0.36 | 0.36 | 0.40 | 0.38 | 0.56 | 0.40 | 2,582.98 | 2,622.62 | ✅ |
| `order_line_sales_by_country` | 0.36 | 0.35 | 0.98× | 0.35 | 0.35 | 0.39 | 0.39 | 0.40 | 0.39 | 2,690.01 | 2,738.93 | ✅ |
| `low_stock_products` | 0.43 | 0.43 | 1.00× | 0.43 | 0.42 | 0.46 | 0.47 | 0.47 | 0.49 | 2,017.48 | 2,014.52 | ✅ |
| `products_in_category` | 0.42 | 0.43 | 1.02× | 0.42 | 0.43 | 0.45 | 0.46 | 0.45 | 0.48 | 2,037.76 | 1,994.96 | ✅ |
| `order_line_quantity_distribution` | 0.33 | 0.34 | 1.03× | 0.33 | 0.34 | 0.35 | 0.36 | 0.38 | 0.37 | 2,906.37 | 2,819.82 | ✅ |
| `customer_order_details` | 0.46 | 0.46 | 0.99× | 0.44 | 0.46 | 0.58 | 0.50 | 0.68 | 0.51 | 1,808.15 | 1,831.03 | ✅ |

_Mean × is ANTLR mean ÷ default mean. "Same result" compares each query's row count and SHA-256 result fingerprint across the two modes._

Both modes returned identical results for all 14 queries.

