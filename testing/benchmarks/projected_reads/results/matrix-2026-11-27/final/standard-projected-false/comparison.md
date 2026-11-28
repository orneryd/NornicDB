# standard-projected-false: final versus baseline

- Baseline source: `b0332923ff6112ebde1a94959a50ea400c25f805`.
- Final source: `b5666cd157caa2dfd7fd8e4345b12e9b2e0f9387`.
- Full reports: [baseline Northwind](../../baseline/standard-projected-false/nornicdb.md) · [final Northwind](nornicdb.md) · [baseline mixed](../../baseline/standard-projected-false/mixed.md) · [final mixed](mixed.md).

## Northwind query latency

| Query | Baseline mean ms | Final mean ms | Change | Baseline p95 ms | Final p95 ms | Result |
|---|---:|---:|---:|---:|---:|---|
| `products_per_category` | 696.61 | 722.15 | +3.7% | 1087.13 | 1014.62 | same |
| `customer_category_distinct_orders` | 6474.87 | 4917.52 | -24.1% | 8023.85 | 6623.69 | same |
| `optional_match_orders_count` | 5791.03 | 4730.15 | -18.3% | 7231.96 | 6361.00 | same |
| `revenue_by_product` | 2998.84 | 2856.13 | -4.8% | 3740.92 | 3650.33 | same |
| `products_by_supplier` | 601.29 | 530.54 | -11.8% | 1000.46 | 622.61 | same |
| `orders_by_customer` | 425.56 | 390.90 | -8.1% | 580.46 | 421.80 | same |
| `revenue_by_category` | 4296.89 | 5210.81 | +21.3% | 6132.48 | 6384.79 | same |
| `revenue_by_supplier` | 4535.44 | 4081.93 | -10.0% | 6385.05 | 5853.95 | same |
| `revenue_by_customer` | 4340.56 | 3512.01 | -19.1% | 5723.95 | 4731.69 | same |
| `order_line_sales_by_country` | 2433.63 | 2117.68 | -13.0% | 3509.47 | 3025.38 | same |
| `low_stock_products` | 240.09 | 323.69 | +34.8% | 257.13 | 407.83 | same |
| `products_in_category` | 2.68 | 3.22 | +20.3% | 2.87 | 3.41 | same |
| `order_line_quantity_distribution` | 3576.88 | 2781.97 | -22.2% | 4600.44 | 3908.20 | same |
| `customer_order_details` | 1.61 | 1.35 | -16.3% | 2.10 | 1.61 | same |

## Mixed-corpus phase latency

| Phase | Baseline p50 ms | Final p50 ms | Baseline p95 ms | Final p95 ms | Change p95 | Baseline p99 ms | Final p99 ms | Hash |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `document_projection` | 0.223 | 0.203 | 0.413 | 0.360 | -12.8% | 0.884 | 0.858 | same |
| `northwind_projection` | 0.249 | 0.252 | 0.482 | 0.930 | +92.9% | 0.788 | 1.831 | same |
| `mixed_projection` | 0.233 | 0.241 | 0.500 | 0.812 | +62.5% | 0.976 | 1.967 | same |
| `document_batch_projection` | 0.521 | 0.480 | 1.359 | 1.978 | +45.6% | 2.536 | 2.655 | same |
| `random_narrow` | 0.274 | 0.214 | 0.845 | 0.731 | -13.5% | 1.812 | 1.996 | same |
| `random_wide` | 0.271 | 0.251 | 1.277 | 1.606 | +25.8% | 2.493 | 2.978 | same |
| `document_full_control` | 0.340 | 0.279 | 2.044 | 2.123 | +3.9% | 3.502 | 10.428 | same |
| `document_full_warm_control` | 0.330 | 0.333 | 2.462 | 1.736 | -29.5% | 4.776 | 4.896 | same |
