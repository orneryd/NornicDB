# Northwind Benchmark Sweep — All Engines

Every engine below ran the **same deterministic Northwind corpus** in strict isolation: fresh store per run, its own powermetrics + vmstat sampling window, and its own on-disk measurement. Same query corpus, iterations, warmup, batch size, and parallelism.

## Summary

| Engine | Mean latency (ms) | Query ops/s (latency) | End-to-end ops/s | Seed (ms) | Seed nodes/s | Power (W avg) | Energy (J) | Disk (MiB) | Wall (s) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **NornicDB** | 0.39 | 2,548.8 | 8.0 | 16,801.81 | 5,799.4 | 0.0 | 0.00 | 241.8 | 72.2 |
| **NornicDB (ANTLR parser)** | 0.41 | 2,432.3 | 7.7 | 16,964.49 | 5,743.8 | 0.0 | 0.00 | 241.7 | 73.4 |

## Per-Query Latency (mean ms)

| Query | NornicDB | NornicDB (ANTLR parser) |
|---|---:|---:|
| `products_per_category` | 0.39 | 0.42 |
| `customer_category_distinct_orders` | 0.34 | 0.36 |
| `optional_match_orders_count` | 0.42 | 0.46 |
| `revenue_by_product` | 0.34 | 0.32 |
| `products_by_supplier` | 0.36 | 0.39 |
| `orders_by_customer` | 0.37 | 0.37 |
| `revenue_by_category` | 0.45 | 0.41 |
| `revenue_by_supplier` | 0.44 | 0.38 |
| `revenue_by_customer` | 0.36 | 0.36 |
| `order_line_sales_by_country` | 0.35 | 0.39 |
| `low_stock_products` | 0.44 | 0.57 |
| `products_in_category` | 0.44 | 0.43 |
| `order_line_quantity_distribution` | 0.35 | 0.38 |
| `customer_order_details` | 0.45 | 0.52 |

## Seed Counts Cross-Check

Same seed data, verified by each engine's own `count(...)` queries.

| Entity | NornicDB | NornicDB (ANTLR parser) |
|---|---:|---:|
| Category | 96 | 96 |
| Supplier | 144 | 144 |
| Customer | 1,200 | 1,200 |
| Product | 48,000 | 48,000 |
| Order | 48,000 | 48,000 |
| PART_OF edges | 48,000 | 48,000 |
| SUPPLIES edges | 48,000 | 48,000 |
| PURCHASED edges | 48,000 | 48,000 |
| ORDERS edges | 168,050 | 168,050 |

## Query Result Cross-Check

Row counts must agree across engines (same deterministic dataset). Hashes may legitimately differ when engines order results differently; disagreements in row count or intra-run stability are flagged.

| Query | NornicDB rows | NornicDB (ANTLR parser) rows | Agreement |
|---|---:|---:||:---:|
| `products_per_category` | 96 | 96 | ✅ |
| `customer_category_distinct_orders` | 10 | 10 | ✅ |
| `optional_match_orders_count` | 100 | 100 | ✅ |
| `revenue_by_product` | 10 | 10 | ✅ |
| `products_by_supplier` | 25 | 25 | ✅ |
| `orders_by_customer` | 25 | 25 | ✅ |
| `revenue_by_category` | 96 | 96 | ✅ |
| `revenue_by_supplier` | 25 | 25 | ✅ |
| `revenue_by_customer` | 25 | 25 | ✅ |
| `order_line_sales_by_country` | 15 | 15 | ✅ |
| `low_stock_products` | 100 | 100 | ✅ |
| `products_in_category` | 100 | 100 | ✅ |
| `order_line_quantity_distribution` | 25 | 25 | ✅ |
| `customer_order_details` | 100 | 100 | ✅ |

> Result hashes per engine are in each per-engine report; this table checks cross-engine row-count agreement and intra-run stability.

