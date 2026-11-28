# standard-projected-true: final versus baseline

- Baseline source: `b0332923ff6112ebde1a94959a50ea400c25f805`.
- Final source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`.
- Full reports: [baseline Northwind](../../baseline/standard-projected-true/nornicdb.md) · [final Northwind](nornicdb.md) · [baseline mixed](../../baseline/standard-projected-true/mixed.md) · [final mixed](mixed.md).

## Northwind query latency

| Query | Baseline mean ms | Final mean ms | Change | Baseline p95 ms | Final p95 ms | Result |
|---|---:|---:|---:|---:|---:|---|
| `products_per_category` | 750.50 | 595.79 | -20.6% | 1016.56 | 786.06 | same |
| `customer_category_distinct_orders` | 6830.16 | 5136.59 | -24.8% | 8281.75 | 6936.36 | same |
| `optional_match_orders_count` | 6455.93 | 5025.75 | -22.2% | 7528.91 | 6815.81 | same |
| `revenue_by_product` | 3271.86 | 2633.64 | -19.5% | 3989.24 | 2909.37 | same |
| `products_by_supplier` | 761.43 | 642.26 | -15.7% | 1004.29 | 1003.15 | same |
| `orders_by_customer` | 438.77 | 396.72 | -9.6% | 649.44 | 441.62 | same |
| `revenue_by_category` | 4020.83 | 4320.44 | +7.5% | 5641.25 | 6298.06 | same |
| `revenue_by_supplier` | 4807.40 | 4258.01 | -11.4% | 6580.41 | 5476.29 | same |
| `revenue_by_customer` | 5386.18 | 4061.70 | -24.6% | 6193.50 | 5855.34 | same |
| `order_line_sales_by_country` | 2820.20 | 2349.95 | -16.7% | 3322.57 | 3092.26 | same |
| `low_stock_products` | 737.71 | 246.18 | -66.6% | 915.22 | 275.56 | same |
| `products_in_category` | 3.18 | 2.89 | -9.1% | 3.49 | 3.21 | same |
| `order_line_quantity_distribution` | 3948.38 | 3131.45 | -20.7% | 4828.55 | 4498.55 | same |
| `customer_order_details` | 1.59 | 1.39 | -12.1% | 2.08 | 1.53 | same |

## Mixed-corpus phase latency

| Phase | Baseline p50 ms | Final p50 ms | Baseline p95 ms | Final p95 ms | Change p95 | Baseline p99 ms | Final p99 ms | Hash |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `document_projection` | 0.216 | 0.205 | 0.532 | 0.363 | -31.8% | 1.883 | 0.633 | same |
| `northwind_projection` | 0.326 | 0.246 | 0.888 | 0.423 | -52.3% | 1.593 | 0.673 | same |
| `mixed_projection` | 0.240 | 0.210 | 0.612 | 0.346 | -43.5% | 1.351 | 0.601 | same |
| `document_batch_projection` | 0.524 | 0.462 | 1.493 | 0.934 | -37.4% | 2.802 | 1.765 | same |
| `random_narrow` | 0.215 | 0.193 | 0.502 | 0.377 | -25.0% | 1.091 | 0.774 | same |
| `random_wide` | 0.253 | 0.217 | 0.808 | 0.515 | -36.3% | 1.559 | 1.255 | same |
| `document_full_control` | 0.291 | 0.272 | 0.890 | 0.678 | -23.8% | 2.461 | 1.707 | same |
| `document_full_warm_control` | 0.289 | 0.251 | 0.913 | 0.633 | -30.6% | 2.060 | 1.388 | same |
