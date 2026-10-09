# NornicDB vs Neo4j — Northwind Benchmark Comparison

- Products seeded: **48,000**, Orders seeded: **48,000**
- Query workloads: **14** NornicDB / **14** Neo4j
- Iterations/query: **30** NornicDB (**5 warmup**) / **30** Neo4j (**5 warmup**)
- Seed batches / parallel sessions: **500 / 4** NornicDB; **500 / 4** Neo4j

## Summary

| Metric | NornicDB | Neo4j | Delta | Ratio |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.13 | 42.00 | -99.7% | 322.76× |
| End-to-end query-loop throughput (ops/sec) | 20.22 | 19.57 | +3.3% | 1.03× |
| Query-latency-only aggregate throughput (ops/sec) | 7,684.15 | 23.81 | +32175.6% | 322.76× |
| Query-loop duration (s) | 20.773 | 21.458 | -3.2% | 1.03× |
| Seed duration (ms) | 5,388.93 | 5,390.93 | -0.0% | 1.00× |
| Wipe duration (ms) | 1.16 | 100.14 | -98.8% | 86.70× |
| Index setup duration (ms) | 2.96 | 492.42 | -99.4% | 166.41× |
| Ingestion duration (ms) | 5,384.81 | 4,798.36 | +12.2% | 0.89× |
| Ingestion nodes/sec | 18,095.36 | 20,306.93 | -10.9% | 0.89× |
| Ingestion relationships/sec | 57,950.10 | 65,032.61 | -10.9% | 0.89× |
| Avg CPU power (mW) | 9,245.25 | 5,859.92 | +57.8% | 0.63× |
| Avg GPU power (mW) | 205.31 | 1.23 | +16613.5% | 0.01× |
| Avg package power (mW) | 9,450.56 | 5,861.15 | +61.2% | 0.62× |
| Energy during benchmark (J) | 259.94 | 257.77 | +0.8% | 0.99× |
| Benchmark wall-clock (s) | 28.48 | 44.37 | -35.8% | 1.56× |
| Peak memory used (bytes) | 24.2 GiB | 23.5 GiB | +2.7% | 0.97× |
| Raw data files (bytes) | 155,422,720 | 53,207,040 | +192.1% | 0.34× |

_Delta = (NornicDB − Neo4j) / Neo4j. Ratio compares Neo4j to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to Neo4j for throughput (higher is better)._
_End-to-end query-loop throughput divides measured operations by the full suite window, including warmups and per-query setup; the query-latency-only rate excludes both._

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

## Full Query Suite

Each workload is reported independently with all recorded latency percentiles, range, sample count, and per-query rate.

### `products_per_category`

Product counts grouped by category, with a full result sort.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.15 | 0.15 | 0.18 | 0.18 | 0.14 | 0.18 | 0.01 | 5,874.77 | 96 |
| Neo4j | 30 | 6.33 | 6.25 | 7.35 | 8.65 | 5.62 | 9.10 | 0.68 | 157.49 | 96 |
| NornicDB (ANTLR) | 30 | 0.18 | 0.18 | 0.21 | 0.23 | 0.14 | 0.23 | 0.02 | 5,020.33 | 96 |

Mean-latency ratio (Neo4j / NornicDB): **41.62×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.18×**.

<details><summary>Cypher</summary>

```cypher
MATCH (c:Category)<-[:PART_OF]-(p:Product)
			RETURN c.categoryName AS categoryName, count(p) AS productCount
			ORDER BY productCount DESC
```

</details>

### `customer_category_distinct_orders`

Four-hop customer-to-category traversal with distinct-order aggregation.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.11 | 0.11 | 0.13 | 0.14 | 0.10 | 0.14 | 0.01 | 8,668.22 | 10 |
| Neo4j | 30 | 172.48 | 171.01 | 182.50 | 192.73 | 165.32 | 196.69 | 7.12 | 5.80 | 10 |
| NornicDB (ANTLR) | 30 | 0.18 | 0.17 | 0.22 | 0.29 | 0.15 | 0.32 | 0.03 | 5,488.64 | 10 |

Mean-latency ratio (Neo4j / NornicDB): **1546.46×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.59×**.

<details><summary>Cypher</summary>

```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)
			RETURN c.companyName AS companyName, cat.categoryName AS categoryName, count(DISTINCT o) AS orders
			ORDER BY orders DESC, companyName ASC, categoryName ASC
			LIMIT 10
```

</details>

### `optional_match_orders_count`

Optional product-to-order traversal with zero-match preservation and top-100 sorting.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.15 | 0.14 | 0.18 | 0.18 | 0.13 | 0.18 | 0.01 | 6,072.82 | 100 |
| Neo4j | 30 | 53.05 | 52.90 | 54.88 | 55.75 | 51.66 | 56.09 | 0.99 | 18.84 | 100 |
| NornicDB (ANTLR) | 30 | 0.19 | 0.18 | 0.23 | 0.25 | 0.16 | 0.26 | 0.03 | 4,807.92 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **364.67×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.30×**.

<details><summary>Cypher</summary>

```cypher
MATCH (p:Product)
			OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
			RETURN p.productName AS productName, count(o) AS orderCount
			ORDER BY orderCount DESC, productName ASC
			LIMIT 100
```

</details>

### `revenue_by_product`

Relationship-property arithmetic and revenue aggregation grouped by product.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.09 | 0.09 | 0.12 | 0.18 | 0.07 | 0.20 | 0.03 | 10,394.26 | 10 |
| Neo4j | 30 | 67.65 | 67.10 | 70.47 | 71.28 | 65.36 | 71.49 | 1.51 | 14.78 | 10 |
| NornicDB (ANTLR) | 30 | 0.16 | 0.15 | 0.18 | 0.20 | 0.13 | 0.20 | 0.02 | 6,224.71 | 10 |

Mean-latency ratio (Neo4j / NornicDB): **728.49×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.69×**.

<details><summary>Cypher</summary>

```cypher
MATCH (p:Product)<-[r:ORDERS]-(:Order)
			WITH p, sum(p.unitPrice * r.quantity) AS revenue
			RETURN p.productName AS productName, revenue
			ORDER BY revenue DESC, productName ASC
			LIMIT 10
```

</details>

### `products_by_supplier`

Supplier-to-product traversal with top-N aggregation and deterministic ties.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.11 | 0.11 | 0.12 | 0.14 | 0.09 | 0.15 | 0.01 | 8,585.94 | 25 |
| Neo4j | 30 | 6.87 | 6.52 | 8.75 | 9.82 | 5.87 | 9.99 | 1.03 | 145.33 | 25 |
| NornicDB (ANTLR) | 30 | 0.14 | 0.14 | 0.16 | 0.17 | 0.13 | 0.18 | 0.01 | 6,700.67 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **61.93×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.28×**.

<details><summary>Cypher</summary>

```cypher
MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)
			RETURN s.companyName AS supplier, count(p) AS products
			ORDER BY products DESC, supplier ASC
			LIMIT 25
```

</details>

### `orders_by_customer`

Customer-to-order traversal grouped into a top-25 order-count ranking.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.12 | 0.12 | 0.14 | 0.15 | 0.10 | 0.16 | 0.01 | 7,890.93 | 25 |
| Neo4j | 30 | 6.74 | 6.66 | 6.89 | 8.39 | 6.46 | 8.99 | 0.44 | 148.13 | 25 |
| NornicDB (ANTLR) | 30 | 0.13 | 0.13 | 0.15 | 0.15 | 0.12 | 0.16 | 0.01 | 7,268.54 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **55.85×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.10×**.

<details><summary>Cypher</summary>

```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
			RETURN c.companyName AS customer, count(o) AS orders
			ORDER BY orders DESC, customer ASC
			LIMIT 25
```

</details>

### `revenue_by_category`

Three-hop category revenue aggregation from order-line quantities and product prices.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.14 | 0.14 | 0.16 | 0.17 | 0.12 | 0.17 | 0.01 | 6,100.71 | 96 |
| Neo4j | 30 | 63.18 | 62.36 | 68.46 | 69.24 | 60.74 | 69.48 | 2.37 | 15.82 | 96 |
| NornicDB (ANTLR) | 30 | 0.20 | 0.20 | 0.24 | 0.25 | 0.18 | 0.25 | 0.02 | 4,367.55 | 96 |

Mean-latency ratio (Neo4j / NornicDB): **444.62×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.44×**.

<details><summary>Cypher</summary>

```cypher
MATCH (c:Category)<-[:PART_OF]-(p:Product)<-[r:ORDERS]-(:Order)
			RETURN c.categoryName AS category, sum(p.unitPrice * r.quantity) AS revenue
			ORDER BY revenue DESC, category ASC
```

</details>

### `revenue_by_supplier`

Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.12 | 0.12 | 0.15 | 0.16 | 0.10 | 0.16 | 0.02 | 7,782.44 | 25 |
| Neo4j | 30 | 60.92 | 60.68 | 62.69 | 63.42 | 59.30 | 63.68 | 0.96 | 16.41 | 25 |
| NornicDB (ANTLR) | 30 | 0.16 | 0.16 | 0.19 | 0.20 | 0.14 | 0.21 | 0.02 | 5,985.98 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **501.64×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.32×**.

<details><summary>Cypher</summary>

```cypher
MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)<-[r:ORDERS]-(:Order)
			RETURN s.companyName AS supplier, sum(p.unitPrice * r.quantity) AS revenue
			ORDER BY revenue DESC, supplier ASC
			LIMIT 25
```

</details>

### `revenue_by_customer`

Customer-order-product traversal with relationship-property revenue aggregation.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.12 | 0.12 | 0.15 | 0.16 | 0.09 | 0.16 | 0.02 | 7,810.47 | 25 |
| Neo4j | 30 | 69.97 | 69.60 | 72.59 | 73.82 | 68.21 | 74.29 | 1.48 | 14.29 | 25 |
| NornicDB (ANTLR) | 30 | 0.16 | 0.15 | 0.18 | 0.18 | 0.14 | 0.18 | 0.01 | 6,132.30 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **573.54×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.29×**.

<details><summary>Cypher</summary>

```cypher
MATCH (c:Customer)-[:PURCHASED]->(:Order)-[r:ORDERS]->(p:Product)
			RETURN c.companyName AS customer, sum(p.unitPrice * r.quantity) AS revenue
			ORDER BY revenue DESC, customer ASC
			LIMIT 25
```

</details>

### `order_line_sales_by_country`

Order-line scan grouped by shipping country with line-count and unit aggregation.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.11 | 0.10 | 0.17 | 0.18 | 0.09 | 0.19 | 0.03 | 8,599.68 | 15 |
| Neo4j | 30 | 48.79 | 48.51 | 50.78 | 51.67 | 48.05 | 51.69 | 0.85 | 20.49 | 15 |
| NornicDB (ANTLR) | 30 | 0.14 | 0.14 | 0.17 | 0.18 | 0.12 | 0.19 | 0.02 | 6,835.79 | 15 |

Mean-latency ratio (Neo4j / NornicDB): **437.32×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.27×**.

<details><summary>Cypher</summary>

```cypher
MATCH (o:Order)-[r:ORDERS]->(:Product)
			RETURN o.shipCountry AS country, count(r) AS orderLines, sum(r.quantity) AS units
			ORDER BY orderLines DESC, country ASC
```

</details>

### `low_stock_products`

Selective numeric property filter followed by a stable top-100 product sort.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.18 | 0.17 | 0.21 | 0.32 | 0.15 | 0.36 | 0.04 | 4,864.08 | 100 |
| Neo4j | 30 | 7.57 | 7.47 | 7.79 | 8.97 | 7.32 | 9.45 | 0.37 | 131.51 | 100 |
| NornicDB (ANTLR) | 30 | 0.20 | 0.19 | 0.22 | 0.24 | 0.18 | 0.24 | 0.01 | 4,488.30 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **42.49×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.10×**.

<details><summary>Cypher</summary>

```cypher
MATCH (p:Product)
			WHERE p.unitsInStock < 25
			RETURN p.productName AS product, p.unitsInStock AS unitsInStock, p.unitPrice AS unitPrice
			ORDER BY unitsInStock ASC, product ASC
			LIMIT 100
```

</details>

### `products_in_category`

Selective category lookup and adjacent product traversal with a top-100 result.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.16 | 0.16 | 0.17 | 0.18 | 0.14 | 0.18 | 0.01 | 5,420.75 | 100 |
| Neo4j | 30 | 0.95 | 0.84 | 1.07 | 2.58 | 0.79 | 3.19 | 0.43 | 1,015.91 | 100 |
| NornicDB (ANTLR) | 30 | 0.19 | 0.19 | 0.20 | 0.21 | 0.17 | 0.21 | 0.01 | 4,635.35 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **6.06×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.19×**.

<details><summary>Cypher</summary>

```cypher
MATCH (c:Category {categoryID: 7})<-[:PART_OF]-(p:Product)
			RETURN p.productName AS product, p.unitPrice AS unitPrice, p.unitsInStock AS unitsInStock
			ORDER BY product ASC
			LIMIT 100
```

</details>

### `order_line_quantity_distribution`

Full relationship-property scan grouped by line quantity.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.10 | 0.09 | 0.12 | 0.13 | 0.09 | 0.13 | 0.01 | 9,835.53 | 25 |
| Neo4j | 30 | 22.58 | 22.05 | 25.98 | 26.36 | 21.55 | 26.40 | 1.29 | 44.26 | 25 |
| NornicDB (ANTLR) | 30 | 0.14 | 0.13 | 0.17 | 0.17 | 0.11 | 0.17 | 0.02 | 7,048.87 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **235.33×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.43×**.

<details><summary>Cypher</summary>

```cypher
MATCH ()-[r:ORDERS]->()
			RETURN r.quantity AS quantity, count(r) AS lineCount
			ORDER BY quantity ASC
```

</details>

### `customer_order_details`

Selective customer lookup followed by order-line expansion and computed row projection.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.16 | 0.16 | 0.17 | 0.19 | 0.14 | 0.20 | 0.01 | 5,211.80 | 100 |
| Neo4j | 30 | 0.95 | 0.83 | 1.22 | 2.85 | 0.67 | 3.50 | 0.51 | 1,018.17 | 100 |
| NornicDB (ANTLR) | 30 | 0.21 | 0.21 | 0.22 | 0.23 | 0.19 | 0.23 | 0.01 | 4,188.51 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **5.94×**.

Mean-latency ratio (NornicDB ANTLR / default): **1.30×**.

<details><summary>Cypher</summary>

```cypher
MATCH (c:Customer {customerID: 42})-[:PURCHASED]->(o:Order)-[r:ORDERS]->(p:Product)
			RETURN o.orderID AS orderID, p.productName AS product, r.quantity AS quantity,
			       p.unitPrice * r.quantity AS extendedPrice
			ORDER BY orderID ASC, product ASC
			LIMIT 100
```

</details>

## Correctness

**Seed verification.** Post-seed counts reported by each database (via `MATCH (n:Label) RETURN count(n)` and equivalent edge queries).

| Entity | NornicDB | Neo4j | Match |
|---|---:|---:|:---:|
| Category | 96 | 96 | ✅ |
| Supplier | 144 | 144 | ✅ |
| Customer | 1,200 | 1,200 | ✅ |
| Product | 48,000 | 48,000 | ✅ |
| Order | 48,000 | 48,000 | ✅ |
| PART_OF | 48,000 | 48,000 | ✅ |
| SUPPLIES | 48,000 | 48,000 | ✅ |
| PURCHASED | 48,000 | 48,000 | ✅ |
| ORDERS | 168,050 | 168,050 | ✅ |

**Per-query result fingerprints.** Each engine runs the query on the first (warmup) iteration, canonicalises the full result set, and hashes it with SHA-256. A matching row_count + matching hash means the engines returned the same data.

| Query | NornicDB rows | Neo4j rows | NornicDB hash | Neo4j hash | Match |
|---|---:|---:|---|---|:---:|
| `products_per_category` | 96 | 96 | `91e9f1f06368…` | `91e9f1f06368…` | ✅ |
| `customer_category_distinct_orders` | 10 | 10 | `5da36214d516…` | `5da36214d516…` | ✅ |
| `optional_match_orders_count` | 100 | 100 | `8950fcdaab16…` | `8950fcdaab16…` | ✅ |
| `revenue_by_product` | 10 | 10 | `60b64c678f4c…` | `60b64c678f4c…` | ✅ |
| `products_by_supplier` | 25 | 25 | `af1e9b5d1d66…` | `af1e9b5d1d66…` | ✅ |
| `orders_by_customer` | 25 | 25 | `ecff10cfcfa9…` | `ecff10cfcfa9…` | ✅ |
| `revenue_by_category` | 96 | 96 | `23ba39858bf7…` | `23ba39858bf7…` | ✅ |
| `revenue_by_supplier` | 25 | 25 | `41900ee05a8f…` | `41900ee05a8f…` | ✅ |
| `revenue_by_customer` | 25 | 25 | `639a28655928…` | `639a28655928…` | ✅ |
| `order_line_sales_by_country` | 15 | 15 | `a86030b2ba5e…` | `a86030b2ba5e…` | ✅ |
| `low_stock_products` | 100 | 100 | `a8f2f994d920…` | `a8f2f994d920…` | ✅ |
| `products_in_category` | 100 | 100 | `e207262bf51e…` | `e207262bf51e…` | ✅ |
| `order_line_quantity_distribution` | 25 | 25 | `e239e5f47878…` | `e239e5f47878…` | ✅ |
| `customer_order_details` | 100 | 100 | `9d6ef178ee44…` | `9d6ef178ee44…` | ✅ |

**Intra-run stability.** Every iteration of each query re-fingerprints its result set; a mismatch within a single engine's run is flagged below.

- No intra-run mismatches on either engine.

✅ **All correctness checks passed** — both engines seeded identically and returned identical result sets (by row count and canonical SHA-256 fingerprint) for every benchmark query.

## Storage

Raw data files only (preallocated scratch, WAL, and indexes excluded from the headline):

| Bucket | NornicDB | Neo4j |
|---|---:|---:|
| **Raw data** | 148.2 MiB (155,422,720 B) | 50.7 MiB (53,207,040 B) |
| Indexes / stats | 0 B (0 B) | 7.5 MiB (7,913,472 B) |
| Write-ahead logs | 260.0 KiB (266,240 B) | 144.4 MiB (151,400,448 B) |
| Metadata | 8.0 KiB (8,192 B) | 1.1 MiB (1,191,936 B) |
| _Scratch (excluded)_ | 1.0 MiB (1,048,576 B) | 4.0 KiB (4,096 B) |
| _Unclassified_ | 0 B (0 B) | 0 B (0 B) |
| Total `du` | 149.6 MiB | 203.8 MiB |

- **Raw data ratio:** 2.92× Neo4j (larger)
- Full-dir ratio (includes scratch/WAL): 0.73× Neo4j

## Power

| | NornicDB | Neo4j |
|---|---:|---:|
| Samples | 27 | 43 |
| Duration (s) | 27.51 | 43.98 |
| CPU avg (mW) | 9,245.2 | 5,859.9 |
| GPU avg (mW) | 205.3 | 1.2 |
| Package avg (mW) | 9,450.6 | 5,861.1 |
| Energy (J) | 259.94 | 257.77 |

## Memory Pressure

System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).

| | NornicDB | Neo4j |
|---|---:|---:|
| Samples | 31 | 47 |
| Avg used (active+wired+compressor) | 23.4 GiB | 23.3 GiB |
| Peak used | 24.2 GiB | 23.5 GiB |
| Avg free | 7.3 GiB | 7.2 GiB |
| Min free | 6.4 GiB | 6.6 GiB |
| Avg compressed (logical) | 7.2 GiB | 7.2 GiB |
| Peak compressed | 7.2 GiB | 7.2 GiB |

## Notes

- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.
- Both databases were freshly initialized before each run; Neo4j was stopped during the NornicDB run, and vice versa, to isolate measurements.
- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.
- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). Neo4j raw data = `neostore*store.db*` (record stores). Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and Neo4j empty `*.id` allocation files — are excluded because their size is fixed/preallocated and does not scale with the dataset.