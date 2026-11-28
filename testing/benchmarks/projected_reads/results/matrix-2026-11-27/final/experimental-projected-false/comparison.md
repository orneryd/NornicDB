# experimental-projected-false: final versus baseline

- Baseline source: `b0332923ff6112ebde1a94959a50ea400c25f805`.
- Final source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`.
- Full reports: [baseline Northwind](../../baseline/experimental-projected-false/nornicdb.md) · [final Northwind](nornicdb.md) · [baseline mixed](../../baseline/experimental-projected-false/mixed.md) · [final mixed](mixed.md).

## Northwind query latency

| Query | Baseline mean ms | Final mean ms | Change | Baseline p95 ms | Final p95 ms | Result |
|---|---:|---:|---:|---:|---:|---|
| `products_per_category` | 740.44 | 664.47 | -10.3% | 1049.66 | 938.18 | same |
| `customer_category_distinct_orders` | 6733.23 | 4956.15 | -26.4% | 8373.10 | 6754.88 | same |
| `optional_match_orders_count` | 5742.78 | 4637.41 | -19.2% | 7550.65 | 6303.91 | same |
| `revenue_by_product` | 3092.12 | 2690.26 | -13.0% | 3878.68 | 3730.05 | same |
| `products_by_supplier` | 711.09 | 489.22 | -31.2% | 1084.96 | 507.56 | same |
| `orders_by_customer` | 471.04 | 456.29 | -3.1% | 642.14 | 669.55 | same |
| `revenue_by_category` | 4105.55 | 3785.30 | -7.8% | 6196.18 | 5321.42 | same |
| `revenue_by_supplier` | 4660.11 | 4038.12 | -13.3% | 6148.59 | 5210.17 | same |
| `revenue_by_customer` | 4836.67 | 4096.61 | -15.3% | 5968.81 | 5593.78 | same |
| `order_line_sales_by_country` | 2893.90 | 2410.42 | -16.7% | 3435.54 | 3359.42 | same |
| `low_stock_products` | 310.79 | 264.72 | -14.8% | 531.12 | 338.59 | same |
| `products_in_category` | 4.00 | 2.72 | -32.2% | 7.52 | 2.77 | same |
| `order_line_quantity_distribution` | 3869.37 | 2976.40 | -23.1% | 4331.26 | 4131.61 | same |
| `customer_order_details` | 1.56 | 1.82 | +16.8% | 1.80 | 2.58 | same |

## Mixed-corpus phase latency

| Phase | Baseline p50 ms | Final p50 ms | Baseline p95 ms | Final p95 ms | Change p95 | Baseline p99 ms | Final p99 ms | Hash |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `document_projection` | 0.203 | 0.205 | 0.377 | 0.354 | -5.9% | 0.612 | 0.650 | same |
| `northwind_projection` | 0.224 | 0.225 | 0.390 | 0.393 | +0.8% | 0.703 | 0.648 | same |
| `mixed_projection` | 0.213 | 0.210 | 0.359 | 0.370 | +3.0% | 0.549 | 0.653 | same |
| `document_batch_projection` | 0.407 | 0.398 | 0.658 | 0.660 | +0.3% | 0.976 | 1.155 | same |
| `random_narrow` | 0.186 | 0.197 | 0.344 | 0.370 | +7.7% | 0.610 | 0.696 | same |
| `random_wide` | 0.222 | 0.221 | 0.567 | 0.556 | -1.8% | 1.552 | 1.454 | same |
| `document_full_control` | 0.259 | 0.248 | 0.669 | 0.624 | -6.7% | 1.518 | 1.390 | same |
| `document_full_warm_control` | 0.248 | 0.254 | 0.637 | 0.644 | +1.1% | 1.460 | 1.448 | same |
