# NornicDB Parser Modes — Northwind Query Latency (default vs ANTLR)

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations/query: **30** (**5 warmup**)

## NornicDB Parser Modes: default vs ANTLR

Query latency and throughput of the two `NORNICDB_PARSER` modes over the same seeded Northwind graph and the same workload. Each mode ran serialized from a freshly wiped data directory. Seeding, storage, power and memory are not compared here.

| Metric | NornicDB (default) | NornicDB (ANTLR) | Delta | ANTLR slower by |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.13 | 0.17 | +30.1% | 1.30× |
| End-to-end query-loop throughput (ops/sec) | 20.22 | 21.08 | +4.3% | 0.96× |
| Query-latency-only aggregate throughput (ops/sec) | 7,684.15 | 5,908.17 | -23.1% | 1.30× |
| Query-loop duration (s) | 20.773 | 19.923 | -4.1% | 0.96× |

_Delta = (ANTLR − default) / default. "ANTLR slower by" is ANTLR ÷ default for latency and default ÷ ANTLR for throughput, so values above 1.00× mean ANTLR mode is slower._

### Per-query latency

| Query | Mean default (ms) | Mean ANTLR (ms) | Mean × | Median default (ms) | Median ANTLR (ms) | P95 default (ms) | P95 ANTLR (ms) | P99 default (ms) | P99 ANTLR (ms) | Ops/sec default | Ops/sec ANTLR | Same result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| `products_per_category` | 0.15 | 0.18 | 1.18× | 0.15 | 0.18 | 0.18 | 0.21 | 0.18 | 0.23 | 5,874.77 | 5,020.33 | ✅ |
| `customer_category_distinct_orders` | 0.11 | 0.18 | 1.59× | 0.11 | 0.17 | 0.13 | 0.22 | 0.14 | 0.29 | 8,668.22 | 5,488.64 | ✅ |
| `optional_match_orders_count` | 0.15 | 0.19 | 1.30× | 0.14 | 0.18 | 0.18 | 0.23 | 0.18 | 0.25 | 6,072.82 | 4,807.92 | ✅ |
| `revenue_by_product` | 0.09 | 0.16 | 1.69× | 0.09 | 0.15 | 0.12 | 0.18 | 0.18 | 0.20 | 10,394.26 | 6,224.71 | ✅ |
| `products_by_supplier` | 0.11 | 0.14 | 1.28× | 0.11 | 0.14 | 0.12 | 0.16 | 0.14 | 0.17 | 8,585.94 | 6,700.67 | ✅ |
| `orders_by_customer` | 0.12 | 0.13 | 1.10× | 0.12 | 0.13 | 0.14 | 0.15 | 0.15 | 0.15 | 7,890.93 | 7,268.54 | ✅ |
| `revenue_by_category` | 0.14 | 0.20 | 1.44× | 0.14 | 0.20 | 0.16 | 0.24 | 0.17 | 0.25 | 6,100.71 | 4,367.55 | ✅ |
| `revenue_by_supplier` | 0.12 | 0.16 | 1.32× | 0.12 | 0.16 | 0.15 | 0.19 | 0.16 | 0.20 | 7,782.44 | 5,985.98 | ✅ |
| `revenue_by_customer` | 0.12 | 0.16 | 1.29× | 0.12 | 0.15 | 0.15 | 0.18 | 0.16 | 0.18 | 7,810.47 | 6,132.30 | ✅ |
| `order_line_sales_by_country` | 0.11 | 0.14 | 1.27× | 0.10 | 0.14 | 0.17 | 0.17 | 0.18 | 0.18 | 8,599.68 | 6,835.79 | ✅ |
| `low_stock_products` | 0.18 | 0.20 | 1.10× | 0.17 | 0.19 | 0.21 | 0.22 | 0.32 | 0.24 | 4,864.08 | 4,488.30 | ✅ |
| `products_in_category` | 0.16 | 0.19 | 1.19× | 0.16 | 0.19 | 0.17 | 0.20 | 0.18 | 0.21 | 5,420.75 | 4,635.35 | ✅ |
| `order_line_quantity_distribution` | 0.10 | 0.14 | 1.43× | 0.09 | 0.13 | 0.12 | 0.17 | 0.13 | 0.17 | 9,835.53 | 7,048.87 | ✅ |
| `customer_order_details` | 0.16 | 0.21 | 1.30× | 0.16 | 0.21 | 0.17 | 0.22 | 0.19 | 0.23 | 5,211.80 | 4,188.51 | ✅ |

_Mean × is ANTLR mean ÷ default mean. "Same result" compares each query's row count and SHA-256 result fingerprint across the two modes._

Both modes returned identical results for all 14 queries.

