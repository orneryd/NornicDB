# NornicDB — Northwind Benchmark Report

**Run:** `2026-11-27T22:53:34.19464-07:00` → `2026-11-27T23:14:42.004091-07:00`
**Endpoint:** `bolt://127.0.0.1:27687` (database `matrix`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **11,625.04 ms**
- Wipe duration: **3.10 ms**
- Index setup duration: **8.95 ms**
- Ingestion duration (row generation and writes): **11,612.96 ms**
- Ingestion nodes/sec: **8,390.62**
- Ingestion relationships/sec: **26,870.84**
- Seed batch size: **2,000 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 730.20 | 720.35 | 925.52 | 1,157.22 | 506.54 | 1,251.66 | 153.01 | 1.37 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 7,436.68 | 7,683.05 | 8,668.59 | 8,924.31 | 5,122.28 | 8,978.41 | 982.84 | 0.13 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 5,605.00 | 5,805.03 | 7,294.36 | 7,514.10 | 3,964.92 | 7,545.29 | 1,106.87 | 0.18 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 2,849.49 | 2,629.21 | 3,761.63 | 3,992.77 | 2,306.08 | 4,085.51 | 523.35 | 0.35 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 536.45 | 516.05 | 642.50 | 670.76 | 488.04 | 679.57 | 51.88 | 1.86 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 388.30 | 383.21 | 413.77 | 464.17 | 369.48 | 483.73 | 21.40 | 2.58 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 4,532.48 | 3,963.20 | 6,243.40 | 6,382.97 | 3,442.25 | 6,439.84 | 1,086.15 | 0.22 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 4,314.57 | 3,902.06 | 6,189.50 | 6,355.18 | 3,410.32 | 6,391.05 | 935.93 | 0.23 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 3,928.04 | 3,477.56 | 5,734.26 | 6,134.71 | 3,226.17 | 6,235.68 | 892.48 | 0.25 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 2,430.52 | 2,213.10 | 3,522.77 | 3,599.50 | 1,929.59 | 3,628.45 | 515.32 | 0.41 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 256.68 | 249.09 | 292.30 | 370.90 | 236.43 | 402.60 | 30.95 | 3.89 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 2.66 | 2.63 | 2.87 | 3.01 | 2.58 | 3.03 | 0.10 | 370.13 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 3,067.30 | 2,821.72 | 4,302.96 | 4,410.32 | 2,546.95 | 4,441.18 | 576.03 | 0.33 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 1.40 | 1.37 | 1.59 | 1.81 | 1.24 | 1.89 | 0.13 | 680.46 |

- **Overall mean latency:** 2,577.13 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 0.33 ops/sec
- **Query-latency-only aggregate throughput:** 0.39 ops/sec
- **Query-loop duration:** 1,256.076 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 1,267.842 s

## Correctness

Seed counts (from the database's own `count(...)` queries):

| Entity | Count |
|---|---:|
| Category | 96 |
| Supplier | 144 |
| Customer | 1,200 |
| Product | 48,000 |
| Order | 48,000 |
| PART_OF edges | 48,000 |
| SUPPLIES edges | 48,000 |
| PURCHASED edges | 48,000 |
| ORDERS edges | 168,050 |

Per-query result fingerprints (SHA-256 over canonicalised rows):

| Query | Rows | Hash | Stable across iterations |
|---|---:|---|:---:|
| `products_per_category` | 96 | `91e9f1f063680a6d…` | ✅ |
| `customer_category_distinct_orders` | 10 | `5da36214d5163220…` | ✅ |
| `optional_match_orders_count` | 100 | `8950fcdaab16eaeb…` | ✅ |
| `revenue_by_product` | 10 | `60b64c678f4c01fd…` | ✅ |
| `products_by_supplier` | 25 | `af1e9b5d1d663a02…` | ✅ |
| `orders_by_customer` | 25 | `ecff10cfcfa9cc34…` | ✅ |
| `revenue_by_category` | 96 | `23ba39858bf74ace…` | ✅ |
| `revenue_by_supplier` | 25 | `41900ee05a8f994a…` | ✅ |
| `revenue_by_customer` | 25 | `639a286559282c97…` | ✅ |
| `order_line_sales_by_country` | 15 | `a86030b2ba5eede9…` | ✅ |
| `low_stock_products` | 100 | `a8f2f994d920e6d8…` | ✅ |
| `products_in_category` | 100 | `e207262bf51ed857…` | ✅ |
| `order_line_quantity_distribution` | 25 | `e239e5f47878c862…` | ✅ |
| `customer_order_details` | 100 | `9d6ef178ee445057…` | ✅ |

✅ No intra-run correctness errors.

## Power Consumption

- _No powermetrics samples available:_ missing: /tmp/pr1826-matrix-1127/reports-final2/experimental-projected-true/nornicdb.powermetrics.plist

## Memory Pressure

- _No vm_stat samples available:_ missing: /tmp/pr1826-matrix-1127/reports-final2/experimental-projected-true/nornicdb.vmstat.log

## Storage

- **Raw data files:** 110.4 MiB (115,773,440 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 8.0 KiB (8,192 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 145.7 MiB (152,793,088 bytes)
- Unclassified (other): 4.0 KiB (4,096 bytes)
- Full data directory `du`: 256.1 MiB (268,587,008 bytes)
- Classified sum: 256.1 MiB (268,587,008 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000002.sst` | 39.2 MiB |
| `000003.sst` | 38.0 MiB |
| `000004.sst` | 33.2 MiB |
| `000001.sst` | 4.0 KiB |
| `000005.sst` | 4.0 KiB |
| `000001.vlog` | 4.0 KiB |
| `000002.vlog` | 4.0 KiB |

</details>

## Queries

### `products_per_category`

```cypher
MATCH (c:Category)<-[:PART_OF]-(p:Product)
			RETURN c.categoryName AS categoryName, count(p) AS productCount
			ORDER BY productCount DESC
```

### `customer_category_distinct_orders`

```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)
			RETURN c.companyName AS companyName, cat.categoryName AS categoryName, count(DISTINCT o) AS orders
			ORDER BY orders DESC, companyName ASC, categoryName ASC
			LIMIT 10
```

### `optional_match_orders_count`

```cypher
MATCH (p:Product)
			OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
			RETURN p.productName AS productName, count(o) AS orderCount
			ORDER BY orderCount DESC, productName ASC
			LIMIT 100
```

### `revenue_by_product`

```cypher
MATCH (p:Product)<-[r:ORDERS]-(:Order)
			WITH p, sum(p.unitPrice * r.quantity) AS revenue
			RETURN p.productName AS productName, revenue
			ORDER BY revenue DESC, productName ASC
			LIMIT 10
```

### `products_by_supplier`

```cypher
MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)
			RETURN s.companyName AS supplier, count(p) AS products
			ORDER BY products DESC, supplier ASC
			LIMIT 25
```

### `orders_by_customer`

```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)
			RETURN c.companyName AS customer, count(o) AS orders
			ORDER BY orders DESC, customer ASC
			LIMIT 25
```

### `revenue_by_category`

```cypher
MATCH (c:Category)<-[:PART_OF]-(p:Product)<-[r:ORDERS]-(:Order)
			RETURN c.categoryName AS category, sum(p.unitPrice * r.quantity) AS revenue
			ORDER BY revenue DESC, category ASC
```

### `revenue_by_supplier`

```cypher
MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)<-[r:ORDERS]-(:Order)
			RETURN s.companyName AS supplier, sum(p.unitPrice * r.quantity) AS revenue
			ORDER BY revenue DESC, supplier ASC
			LIMIT 25
```

### `revenue_by_customer`

```cypher
MATCH (c:Customer)-[:PURCHASED]->(:Order)-[r:ORDERS]->(p:Product)
			RETURN c.companyName AS customer, sum(p.unitPrice * r.quantity) AS revenue
			ORDER BY revenue DESC, customer ASC
			LIMIT 25
```

### `order_line_sales_by_country`

```cypher
MATCH (o:Order)-[r:ORDERS]->(:Product)
			RETURN o.shipCountry AS country, count(r) AS orderLines, sum(r.quantity) AS units
			ORDER BY orderLines DESC, country ASC
```

### `low_stock_products`

```cypher
MATCH (p:Product)
			WHERE p.unitsInStock < 25
			RETURN p.productName AS product, p.unitsInStock AS unitsInStock, p.unitPrice AS unitPrice
			ORDER BY unitsInStock ASC, product ASC
			LIMIT 100
```

### `products_in_category`

```cypher
MATCH (c:Category {categoryID: 7})<-[:PART_OF]-(p:Product)
			RETURN p.productName AS product, p.unitPrice AS unitPrice, p.unitsInStock AS unitsInStock
			ORDER BY product ASC
			LIMIT 100
```

### `order_line_quantity_distribution`

```cypher
MATCH ()-[r:ORDERS]->()
			RETURN r.quantity AS quantity, count(r) AS lineCount
			ORDER BY quantity ASC
```

### `customer_order_details`

```cypher
MATCH (c:Customer {customerID: 42})-[:PURCHASED]->(o:Order)-[r:ORDERS]->(p:Product)
			RETURN o.orderID AS orderID, p.productName AS product, r.quantity AS quantity,
			       p.unitPrice * r.quantity AS extendedPrice
			ORDER BY orderID ASC, product ASC
			LIMIT 100
```
