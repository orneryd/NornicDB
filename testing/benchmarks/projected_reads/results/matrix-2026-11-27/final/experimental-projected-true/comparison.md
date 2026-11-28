# experimental-projected-true: final versus baseline

- Baseline source: `b0332923ff6112ebde1a94959a50ea400c25f805`.
- Final source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`.
- Full reports: [baseline Northwind](../../baseline/experimental-projected-true/nornicdb.md) · [final Northwind](nornicdb.md) · [baseline mixed](../../baseline/experimental-projected-true/mixed.md) · [final mixed](mixed.md).

## Northwind query latency

| Query | Baseline mean ms | Final mean ms | Change | Baseline p95 ms | Final p95 ms | Result |
|---|---:|---:|---:|---:|---:|---|
| `products_per_category` | 826.33 | 730.20 | -11.6% | 1052.37 | 925.52 | same |
| `customer_category_distinct_orders` | 6864.20 | 7436.68 | +8.3% | 8174.87 | 8668.59 | same |
| `optional_match_orders_count` | 5882.82 | 5605.00 | -4.7% | 7147.77 | 7294.36 | same |
| `revenue_by_product` | 3692.36 | 2849.49 | -22.8% | 4319.56 | 3761.63 | same |
| `products_by_supplier` | 871.05 | 536.45 | -38.4% | 1023.56 | 642.50 | same |
| `orders_by_customer` | 488.01 | 388.30 | -20.4% | 630.86 | 413.77 | same |
| `revenue_by_category` | 5244.20 | 4532.48 | -13.6% | 6501.36 | 6243.40 | same |
| `revenue_by_supplier` | 5692.15 | 4314.57 | -24.2% | 6662.16 | 6189.50 | same |
| `revenue_by_customer` | 5504.25 | 3928.04 | -28.6% | 6081.16 | 5734.26 | same |
| `order_line_sales_by_country` | 3027.75 | 2430.52 | -19.7% | 3398.42 | 3522.77 | same |
| `low_stock_products` | 750.05 | 256.68 | -65.8% | 879.69 | 292.30 | same |
| `products_in_category` | 3.89 | 2.66 | -31.6% | 5.34 | 2.87 | same |
| `order_line_quantity_distribution` | 4183.43 | 3067.30 | -26.7% | 4783.88 | 4302.96 | same |
| `customer_order_details` | 1.93 | 1.40 | -27.3% | 2.73 | 1.59 | same |

## Mixed-corpus phase latency

| Phase | Baseline p50 ms | Final p50 ms | Baseline p95 ms | Final p95 ms | Change p95 | Baseline p99 ms | Final p99 ms | Hash |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `document_projection` | 0.211 | 0.200 | 0.441 | 0.362 | -17.8% | 0.963 | 0.707 | same |
| `northwind_projection` | 0.258 | 0.240 | 0.659 | 0.400 | -39.2% | 1.755 | 0.629 | same |
| `mixed_projection` | 0.217 | 0.208 | 0.457 | 0.366 | -19.9% | 1.110 | 0.645 | same |
| `document_batch_projection` | 0.491 | 0.397 | 1.526 | 0.668 | -56.2% | 2.598 | 1.121 | same |
| `random_narrow` | 0.206 | 0.190 | 0.606 | 0.344 | -43.2% | 1.284 | 0.587 | same |
| `random_wide` | 0.254 | 0.211 | 1.942 | 0.498 | -74.4% | 4.829 | 1.121 | same |
| `document_full_control` | 0.283 | 0.246 | 2.035 | 0.615 | -69.8% | 4.389 | 1.330 | same |
| `document_full_warm_control` | 0.289 | 0.246 | 2.555 | 0.629 | -75.4% | 4.841 | 1.303 | same |
