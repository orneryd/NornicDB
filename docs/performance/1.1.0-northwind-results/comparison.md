# NornicDB vs Neo4j — Northwind Benchmark Comparison

- Products seeded: **48,000**, Orders seeded: **48,000**
- Query workloads: **14** NornicDB / **14** Neo4j
- Iterations/query: **30** NornicDB (**5 warmup**) / **30** Neo4j (**5 warmup**)
- Seed batches / parallel sessions: **500 / 4** NornicDB; **500 / 4** Neo4j

## Summary

| Metric | NornicDB | Neo4j | Delta | Ratio |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.14 | 52.02 | -99.7% | 369.76× |
| End-to-end query-loop throughput (ops/sec) | 17.59 | 15.83 | +11.1% | 1.11× |
| Query-latency-only aggregate throughput (ops/sec) | 7,107.92 | 19.22 | +36876.5% | 369.76× |
| Query-loop duration (s) | 23.875 | 26.531 | -10.0% | 1.11× |
| Seed duration (ms) | 7,662.40 | 6,588.26 | +16.3% | 0.86× |
| Wipe duration (ms) | 2.23 | 125.66 | -98.2% | 56.45× |
| Index setup duration (ms) | 2.55 | 547.96 | -99.5% | 215.22× |
| Ingestion duration (ms) | 7,657.61 | 5,914.62 | +29.5% | 0.77× |
| Ingestion nodes/sec | 12,724.59 | 16,474.42 | -22.8% | 0.77× |
| Ingestion relationships/sec | 40,750.29 | 52,759.07 | -22.8% | 0.77× |
| Avg CPU power (mW) | 9,122.39 | 6,390.96 | +42.7% | 0.70× |
| Avg GPU power (mW) | 57.07 | 9.72 | +486.9% | 0.17× |
| Avg package power (mW) | 9,179.46 | 6,400.69 | +43.4% | 0.70× |
| Energy during benchmark (J) | 305.60 | 329.89 | -7.4% | 1.08× |
| Benchmark wall-clock (s) | 34.37 | 53.00 | -35.1% | 1.54× |
| Peak memory used (bytes) | 20.0 GiB | 19.9 GiB | +0.4% | 1.00× |
| Raw data files (bytes) | 149,749,760 | 53,207,040 | +181.4% | 0.36× |

_Delta = (NornicDB − Neo4j) / Neo4j. Ratio compares Neo4j to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to Neo4j for throughput (higher is better)._
_End-to-end query-loop throughput divides measured operations by the full suite window, including warmups and per-query setup; the query-latency-only rate excludes both._

## Full Query Suite

Each workload is reported independently with all recorded latency percentiles, range, sample count, and per-query rate.

### `products_per_category`

Product counts grouped by category, with a full result sort.

| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NornicDB | 30 | 0.16 | 0.16 | 0.17 | 0.17 | 0.14 | 0.17 | 0.01 | 5,561.78 | 96 |
| Neo4j | 30 | 7.99 | 7.79 | 9.19 | 10.10 | 7.18 | 10.45 | 0.68 | 124.68 | 96 |

Mean-latency ratio (Neo4j / NornicDB): **50.71×**.

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
| NornicDB | 30 | 0.14 | 0.14 | 0.17 | 0.18 | 0.10 | 0.18 | 0.02 | 7,112.31 | 10 |
| Neo4j | 30 | 207.03 | 199.20 | 252.24 | 277.04 | 194.14 | 278.10 | 20.20 | 4.83 | 10 |

Mean-latency ratio (Neo4j / NornicDB): **1511.19×**.

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
| NornicDB | 30 | 0.18 | 0.18 | 0.19 | 0.19 | 0.16 | 0.20 | 0.01 | 4,984.39 | 100 |
| Neo4j | 30 | 67.21 | 66.75 | 70.01 | 72.07 | 66.11 | 72.57 | 1.43 | 14.87 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **383.41×**.

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
| NornicDB | 30 | 0.13 | 0.13 | 0.15 | 0.17 | 0.11 | 0.18 | 0.02 | 7,189.86 | 10 |
| Neo4j | 30 | 85.34 | 85.47 | 87.19 | 87.38 | 83.41 | 87.45 | 1.23 | 11.72 | 10 |

Mean-latency ratio (Neo4j / NornicDB): **635.27×**.

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
| NornicDB | 30 | 0.12 | 0.13 | 0.14 | 0.14 | 0.10 | 0.14 | 0.01 | 7,600.95 | 25 |
| Neo4j | 30 | 8.42 | 8.11 | 8.99 | 10.92 | 7.83 | 11.67 | 0.72 | 118.65 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **67.59×**.

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
| NornicDB | 30 | 0.13 | 0.13 | 0.15 | 0.15 | 0.09 | 0.15 | 0.01 | 7,378.79 | 25 |
| Neo4j | 30 | 9.09 | 8.97 | 9.43 | 11.21 | 8.79 | 11.89 | 0.55 | 109.93 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **70.39×**.

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
| NornicDB | 30 | 0.18 | 0.18 | 0.20 | 0.20 | 0.15 | 0.20 | 0.02 | 4,789.24 | 96 |
| Neo4j | 30 | 79.65 | 76.72 | 95.89 | 96.95 | 72.43 | 97.36 | 8.10 | 12.55 | 96 |

Mean-latency ratio (Neo4j / NornicDB): **450.41×**.

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
| NornicDB | 30 | 0.13 | 0.13 | 0.16 | 0.18 | 0.11 | 0.18 | 0.02 | 7,037.57 | 25 |
| Neo4j | 30 | 73.28 | 73.00 | 76.73 | 77.33 | 71.42 | 77.57 | 1.61 | 13.64 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **551.09×**.

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
| NornicDB | 30 | 0.14 | 0.14 | 0.17 | 0.17 | 0.10 | 0.17 | 0.02 | 6,823.74 | 25 |
| Neo4j | 30 | 81.38 | 81.16 | 83.88 | 85.13 | 79.53 | 85.54 | 1.37 | 12.29 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **591.45×**.

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
| NornicDB | 30 | 0.08 | 0.07 | 0.09 | 0.10 | 0.07 | 0.11 | 0.01 | 12,062.73 | 15 |
| Neo4j | 30 | 66.52 | 66.02 | 68.53 | 69.52 | 65.56 | 69.69 | 1.04 | 15.03 | 15 |

Mean-latency ratio (Neo4j / NornicDB): **871.41×**.

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
| NornicDB | 30 | 0.15 | 0.15 | 0.17 | 0.17 | 0.13 | 0.18 | 0.01 | 5,532.25 | 100 |
| Neo4j | 30 | 9.87 | 9.71 | 10.53 | 11.94 | 9.58 | 12.39 | 0.53 | 100.96 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **66.01×**.

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
| NornicDB | 30 | 0.15 | 0.15 | 0.18 | 0.19 | 0.13 | 0.20 | 0.02 | 5,292.99 | 100 |
| Neo4j | 30 | 1.28 | 1.18 | 1.34 | 3.04 | 1.09 | 3.74 | 0.47 | 757.92 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **8.34×**.

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
| NornicDB | 30 | 0.12 | 0.12 | 0.15 | 0.16 | 0.10 | 0.16 | 0.02 | 7,694.86 | 25 |
| Neo4j | 30 | 30.11 | 29.77 | 31.61 | 31.68 | 29.35 | 31.71 | 0.73 | 33.20 | 25 |

Mean-latency ratio (Neo4j / NornicDB): **244.98×**.

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
| NornicDB | 30 | 0.16 | 0.16 | 0.17 | 0.17 | 0.15 | 0.17 | 0.01 | 4,901.79 | 100 |
| Neo4j | 30 | 1.14 | 1.03 | 1.30 | 3.14 | 0.86 | 3.88 | 0.53 | 841.78 | 100 |

Mean-latency ratio (Neo4j / NornicDB): **7.02×**.

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
| **Raw data** | 142.8 MiB (149,749,760 B) | 50.7 MiB (53,207,040 B) |
| Indexes / stats | 0 B (0 B) | 7.6 MiB (7,970,816 B) |
| Write-ahead logs | 260.0 KiB (266,240 B) | 144.6 MiB (151,674,880 B) |
| Metadata | 8.0 KiB (8,192 B) | 1.1 MiB (1,191,936 B) |
| _Scratch (excluded)_ | 1.0 MiB (1,048,576 B) | 4.0 KiB (4,096 B) |
| _Unclassified_ | 0 B (0 B) | 0 B (0 B) |
| Total `du` | 144.1 MiB | 204.1 MiB |

- **Raw data ratio:** 2.81× Neo4j (larger)
- Full-dir ratio (includes scratch/WAL): 0.71× Neo4j

## Power

| | NornicDB | Neo4j |
|---|---:|---:|
| Samples | 33 | 51 |
| Duration (s) | 33.29 | 51.54 |
| CPU avg (mW) | 9,122.4 | 6,391.0 |
| GPU avg (mW) | 57.1 | 9.7 |
| Package avg (mW) | 9,179.5 | 6,400.7 |
| Energy (J) | 305.60 | 329.89 |

## Memory Pressure

System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).

| | NornicDB | Neo4j |
|---|---:|---:|
| Samples | 37 | 55 |
| Avg used (active+wired+compressor) | 19.7 GiB | 19.6 GiB |
| Peak used | 20.0 GiB | 19.9 GiB |
| Avg free | 438.3 MiB | 565.8 MiB |
| Min free | 60.5 MiB | 52.4 MiB |
| Avg compressed (logical) | 20.6 GiB | 20.6 GiB |
| Peak compressed | 20.6 GiB | 20.6 GiB |

## Notes

- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.
- Both databases were freshly initialized before each run; Neo4j was stopped during the NornicDB run, and vice versa, to isolate measurements.
- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.
- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). Neo4j raw data = `neostore*store.db*` (record stores). Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and Neo4j empty `*.id` allocation files — are excluded because their size is fixed/preallocated and does not scale with the dataset.