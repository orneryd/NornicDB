# LadybugDB — Northwind Benchmark Report

**Run:** `2026-10-09T09:33:13.262583-07:00` → `2026-10-09T09:33:26.118019-07:00`
**Endpoint:** `ladybug://embedded//Users/tjsweet/src/NornicDB/bench-data/ladybug` (database `/Users/tjsweet/src/NornicDB/bench-data/ladybug`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **4,944.22 ms**
- Wipe duration: **0.00 ms**
- Index setup duration: **0.00 ms**
- Ingestion duration (row generation and writes): **0.00 ms**
- Ingestion nodes/sec: **0.00**
- Ingestion relationships/sec: **0.00**
- Seed batch size: **500 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 0.88 | 0.87 | 1.00 | 1.05 | 0.71 | 1.06 | 0.07 | 1,112.59 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 58.22 | 58.36 | 59.67 | 59.82 | 55.54 | 59.82 | 1.10 | 17.17 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 10.22 | 10.15 | 10.69 | 11.05 | 9.78 | 11.18 | 0.29 | 97.66 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 5.08 | 5.07 | 5.26 | 5.35 | 4.84 | 5.37 | 0.10 | 196.46 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 0.77 | 0.76 | 0.81 | 0.83 | 0.71 | 0.83 | 0.03 | 1,292.86 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 0.98 | 0.98 | 1.03 | 1.05 | 0.91 | 1.05 | 0.04 | 1,016.09 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 34.45 | 34.40 | 35.41 | 35.53 | 33.07 | 35.57 | 0.65 | 29.01 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 35.09 | 35.04 | 35.69 | 36.09 | 34.18 | 36.23 | 0.48 | 28.49 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 61.75 | 61.03 | 63.54 | 68.34 | 59.89 | 70.28 | 2.03 | 16.19 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 2.05 | 2.04 | 2.17 | 2.22 | 1.92 | 2.24 | 0.08 | 485.05 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 2.33 | 2.32 | 2.57 | 2.63 | 2.13 | 2.65 | 0.13 | 424.01 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 2.22 | 2.20 | 2.38 | 2.41 | 2.06 | 2.42 | 0.10 | 445.89 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 1.51 | 1.49 | 1.68 | 1.76 | 1.30 | 1.78 | 0.11 | 660.23 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 2.63 | 2.59 | 2.79 | 3.04 | 2.46 | 3.15 | 0.14 | 375.17 |

- **Overall mean latency:** 15.58 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 54.91 ops/sec
- **Query-latency-only aggregate throughput:** 64.17 ops/sec
- **Query-loop duration:** 7.649 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 13.037 s

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

- Samples collected: **12** (~1s each)
- Sampled duration: **12.23 s**
- Avg CPU power: **12,963.9 mW**
- Avg GPU power: **40.0 mW**
- Avg package power: **13,003.9 mW**
- Estimated energy (benchmark window): **159.00 J**

## Memory Pressure

- Samples collected: **15** (~1s each)
- Avg used (active + wired + compressor): **24.3 GiB**
- Peak used: **24.8 GiB**
- Avg free: **5.1 GiB**
- Min free: **4.3 GiB**
- Avg compressed (logical): **7.1 GiB**
- Peak compressed: **7.1 GiB**

## Storage

- **Raw data files:** 0 B (0 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 0 B (0 bytes)
- Metadata/bookkeeping: 0 B (0 bytes)
- Preallocated scratch (excluded): 0 B (0 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 25.1 MiB (26,279,936 bytes)
- Classified sum: 0 B (0 bytes, Δ vs du = -26,279,936 bytes)

> ⚠️ **Classifier/du mismatch:** -26,279,936 bytes. A file type may be uncategorised — inspect the data directory manually and extend NORNIC_RULES / NEO4J_RULES.

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

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
