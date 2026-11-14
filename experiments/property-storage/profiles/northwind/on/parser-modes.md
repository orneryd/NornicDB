# NornicDB Parser Modes — Northwind Query Latency (default vs ANTLR)

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations/query: **30** (**5 warmup**)

## NornicDB Parser Modes: default vs ANTLR

Query latency and throughput of the two `NORNICDB_PARSER` modes over the same seeded Northwind graph and the same workload. Each mode ran serialized from a freshly wiped data directory. Seeding, storage, power and memory are not compared here.

| Metric | NornicDB (default) | NornicDB (ANTLR) | Delta | ANTLR slower by |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.39 | 0.41 | +4.8% | 1.05× |
| End-to-end query-loop throughput (ops/sec) | 8.02 | 7.72 | -3.8% | 1.04× |
| Query-latency-only aggregate throughput (ops/sec) | 2,548.79 | 2,432.32 | -4.6% | 1.05× |
| Query-loop duration (s) | 52.346 | 54.415 | +4.0% | 1.04× |

_Delta = (ANTLR − default) / default. "ANTLR slower by" is ANTLR ÷ default for latency and default ÷ ANTLR for throughput, so values above 1.00× mean ANTLR mode is slower._

### Per-query latency

| Query | Mean default (ms) | Mean ANTLR (ms) | Mean × | Median default (ms) | Median ANTLR (ms) | P95 default (ms) | P95 ANTLR (ms) | P99 default (ms) | P99 ANTLR (ms) | Ops/sec default | Ops/sec ANTLR | Same result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `products_per_category` | 0.39 | 0.42 | 1.06× | 0.39 | 0.41 | 0.41 | 0.47 | 0.43 | 0.47 | 2,247.23 | 2,137.93 | ✅ |
| `customer_category_distinct_orders` | 0.34 | 0.36 | 1.08× | 0.34 | 0.37 | 0.37 | 0.40 | 0.38 | 0.40 | 2,873.59 | 2,661.67 | ✅ |
| `optional_match_orders_count` | 0.42 | 0.46 | 1.10× | 0.42 | 0.43 | 0.46 | 0.64 | 0.47 | 0.68 | 2,134.20 | 1,886.21 | ✅ |
| `revenue_by_product` | 0.34 | 0.32 | 0.94× | 0.34 | 0.31 | 0.36 | 0.41 | 0.37 | 0.43 | 2,881.94 | 3,002.95 | ✅ |
| `products_by_supplier` | 0.36 | 0.39 | 1.08× | 0.36 | 0.37 | 0.39 | 0.50 | 0.39 | 0.53 | 2,661.55 | 2,440.27 | ✅ |
| `orders_by_customer` | 0.37 | 0.37 | 1.00× | 0.37 | 0.35 | 0.40 | 0.47 | 0.41 | 0.58 | 2,603.50 | 2,599.34 | ✅ |
| `revenue_by_category` | 0.45 | 0.41 | 0.91× | 0.45 | 0.40 | 0.49 | 0.45 | 0.49 | 0.47 | 1,939.69 | 2,118.17 | ✅ |
| `revenue_by_supplier` | 0.44 | 0.38 | 0.85× | 0.44 | 0.36 | 0.51 | 0.45 | 0.54 | 0.47 | 2,141.78 | 2,505.83 | ✅ |
| `revenue_by_customer` | 0.36 | 0.36 | 0.99× | 0.36 | 0.36 | 0.40 | 0.40 | 0.41 | 0.42 | 2,592.50 | 2,609.60 | ✅ |
| `order_line_sales_by_country` | 0.35 | 0.39 | 1.09× | 0.35 | 0.37 | 0.39 | 0.48 | 0.39 | 0.50 | 2,663.94 | 2,481.60 | ✅ |
| `low_stock_products` | 0.44 | 0.57 | 1.30× | 0.43 | 0.56 | 0.49 | 0.70 | 0.50 | 0.72 | 2,002.05 | 1,490.23 | ✅ |
| `products_in_category` | 0.44 | 0.43 | 0.99× | 0.44 | 0.42 | 0.47 | 0.48 | 0.49 | 0.55 | 1,985.25 | 1,975.06 | ✅ |
| `order_line_quantity_distribution` | 0.35 | 0.38 | 1.09× | 0.35 | 0.38 | 0.38 | 0.46 | 0.39 | 0.47 | 2,752.50 | 2,508.61 | ✅ |
| `customer_order_details` | 0.45 | 0.52 | 1.17× | 0.45 | 0.50 | 0.48 | 0.70 | 0.49 | 0.75 | 1,873.67 | 1,594.05 | ✅ |

_Mean × is ANTLR mean ÷ default mean. "Same result" compares each query's row count and SHA-256 result fingerprint across the two modes._

Both modes returned identical results for all 14 queries.

