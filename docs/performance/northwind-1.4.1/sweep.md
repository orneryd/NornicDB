# Northwind Benchmark Sweep — All Engines

Every engine below ran the **same deterministic Northwind corpus** in strict isolation: fresh store per run, its own powermetrics + vmstat sampling window, and its own on-disk measurement. Same query corpus, iterations, warmup, batch size, and parallelism.

## Summary

| Engine | Mean latency (ms) | Query ops/s (latency) | End-to-end ops/s | Seed (ms) | Seed nodes/s | Power (W avg) | Energy (J) | Disk (MiB) | Wall (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **NornicDB** | 0.13 | 7,684.1 | 20.2 | 5,388.93 | 18,081.5 | 9.5 | 259.94 | 149.6 | 28.5 |
| **NornicDB (ANTLR parser)** | 0.17 | 5,908.2 | 21.1 | 5,354.84 | 18,196.6 | 9.2 | 244.88 | 149.5 | 27.4 |
| **Neo4j** | 42.00 | 23.8 | 19.6 | 5,390.93 | 18,074.8 | 5.9 | 257.77 | 203.8 | 44.4 |
| **FalkorDB** | 41.50 | 24.1 | 20.6 | 9,320.88 | 10,453.9 | 6.4 | 191.42 | 14.1 | 30.6 |
| **Memgraph** | 53.25 | 18.8 | 16.0 | 1,603.15 | 60,780.2 | 7.5 | 224.25 | 532.8 | 30.4 |
| **LadybugDB** | 15.58 | 64.2 | 54.9 | 4,944.22 | 19,707.9 | 13.0 | 159.00 | 25.1 | 13.0 |

## Per-Query Latency (mean ms)

| Query | NornicDB | NornicDB (ANTLR parser) | Neo4j | FalkorDB | Memgraph | LadybugDB |
|---|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.15 | 0.18 | 6.33 | 7.77 | 9.94 | 0.88 |
| `customer_category_distinct_orders` | 0.11 | 0.18 | 172.48 | 167.69 | 226.41 | 58.22 |
| `optional_match_orders_count` | 0.15 | 0.19 | 53.05 | 73.29 | 88.75 | 10.22 |
| `revenue_by_product` | 0.09 | 0.16 | 67.65 | 72.60 | 80.61 | 5.08 |
| `products_by_supplier` | 0.11 | 0.14 | 6.87 | 4.94 | 8.80 | 0.77 |
| `orders_by_customer` | 0.12 | 0.13 | 6.74 | 5.93 | 9.66 | 0.98 |
| `revenue_by_category` | 0.14 | 0.20 | 63.18 | 53.82 | 71.88 | 34.45 |
| `revenue_by_supplier` | 0.12 | 0.16 | 60.92 | 56.08 | 72.60 | 35.09 |
| `revenue_by_customer` | 0.12 | 0.16 | 69.97 | 53.10 | 99.63 | 61.75 |
| `order_line_sales_by_country` | 0.11 | 0.14 | 48.79 | 41.13 | 41.53 | 2.05 |
| `low_stock_products` | 0.18 | 0.20 | 7.57 | 2.22 | 8.79 | 2.33 |
| `products_in_category` | 0.16 | 0.19 | 0.95 | 0.68 | 0.85 | 2.22 |
| `order_line_quantity_distribution` | 0.10 | 0.14 | 22.58 | 40.96 | 25.35 | 1.51 |
| `customer_order_details` | 0.16 | 0.21 | 0.95 | 0.76 | 0.65 | 2.63 |

## Seed Counts Cross-Check

Same seed data, verified by each engine's own `count(...)` queries.

| Entity | NornicDB | NornicDB (ANTLR parser) | Neo4j | FalkorDB | Memgraph | LadybugDB |
|---|---:|---:|---:|---:|---:|---:|
| Category | 96 | 96 | 96 | 96 | 96 | 96 |
| Supplier | 144 | 144 | 144 | 144 | 144 | 144 |
| Customer | 1,200 | 1,200 | 1,200 | 1,200 | 1,200 | 1,200 |
| Product | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 |
| Order | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 |
| PART_OF edges | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 |
| SUPPLIES edges | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 |
| PURCHASED edges | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 | 48,000 |
| ORDERS edges | 168,050 | 168,050 | 168,050 | 168,050 | 168,050 | 168,050 |

## Query Result Cross-Check

Row counts must agree across engines (same deterministic dataset). Hashes may legitimately differ when engines order results differently; disagreements in row count or intra-run stability are flagged.

| Query | NornicDB rows | NornicDB (ANTLR parser) rows | Neo4j rows | FalkorDB rows | Memgraph rows | LadybugDB rows | Agreement |
|---|---:|---:|---:|---:|---:|---:||:---:|
| `products_per_category` | 96 | 96 | 96 | 96 | 96 | 96 | ✅ |
| `customer_category_distinct_orders` | 10 | 10 | 10 | 10 | 10 | 10 | ✅ |
| `optional_match_orders_count` | 100 | 100 | 100 | 100 | 100 | 100 | ✅ |
| `revenue_by_product` | 10 | 10 | 10 | 10 | 10 | 10 | ✅ |
| `products_by_supplier` | 25 | 25 | 25 | 25 | 25 | 25 | ✅ |
| `orders_by_customer` | 25 | 25 | 25 | 25 | 25 | 25 | ✅ |
| `revenue_by_category` | 96 | 96 | 96 | 96 | 96 | 96 | ✅ |
| `revenue_by_supplier` | 25 | 25 | 25 | 25 | 25 | 25 | ✅ |
| `revenue_by_customer` | 25 | 25 | 25 | 25 | 25 | 25 | ✅ |
| `order_line_sales_by_country` | 15 | 15 | 15 | 15 | 15 | 15 | ✅ |
| `low_stock_products` | 100 | 100 | 100 | 100 | 100 | 100 | ✅ |
| `products_in_category` | 100 | 100 | 100 | 100 | 100 | 100 | ✅ |
| `order_line_quantity_distribution` | 25 | 25 | 25 | 25 | 25 | 25 | ✅ |
| `customer_order_details` | 100 | 100 | 100 | 100 | 100 | 100 | ✅ |

> Result hashes per engine are in each per-engine report; this table checks cross-engine row-count agreement and intra-run stability.

