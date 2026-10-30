# FalkorDB — Northwind Benchmark Report

**Run:** `2026-10-09T09:32:08.187902-07:00` → `2026-10-09T09:32:38.167901-07:00`
**Endpoint:** `falkor://localhost:17690` (database `falkor`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **9,320.88 ms**
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
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 7.77 | 7.67 | 8.51 | 8.88 | 7.30 | 8.95 | 0.37 | 127.90 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 167.69 | 167.29 | 170.53 | 176.14 | 164.10 | 178.29 | 2.52 | 5.96 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 73.29 | 72.64 | 77.24 | 80.20 | 70.77 | 80.86 | 2.13 | 13.63 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 72.60 | 72.44 | 74.65 | 75.65 | 70.67 | 76.02 | 1.26 | 13.77 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 4.94 | 4.91 | 5.15 | 5.54 | 4.72 | 5.70 | 0.18 | 201.32 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 5.93 | 5.95 | 6.07 | 6.09 | 5.76 | 6.10 | 0.11 | 168.03 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 53.82 | 53.82 | 54.60 | 54.71 | 52.78 | 54.73 | 0.52 | 18.54 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 56.08 | 55.94 | 57.48 | 58.24 | 54.95 | 58.50 | 0.83 | 17.82 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 53.10 | 52.50 | 57.27 | 63.29 | 51.53 | 64.47 | 2.63 | 18.82 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 41.13 | 40.89 | 42.57 | 43.95 | 40.09 | 44.37 | 0.84 | 24.30 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 2.22 | 2.22 | 2.31 | 2.38 | 2.07 | 2.40 | 0.06 | 425.63 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 0.68 | 0.67 | 0.78 | 0.78 | 0.58 | 0.78 | 0.07 | 1,289.04 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 40.96 | 40.90 | 41.65 | 41.98 | 40.04 | 42.10 | 0.47 | 24.40 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 0.76 | 0.75 | 0.88 | 0.93 | 0.62 | 0.95 | 0.09 | 1,115.40 |

- **Overall mean latency:** 41.50 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 20.59 ops/sec
- **Query-latency-only aggregate throughput:** 24.10 ops/sec
- **Query-loop duration:** 20.400 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 30.563 s

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

- Samples collected: **29** (~1s each)
- Sampled duration: **29.96 s**
- Avg CPU power: **6,387.9 mW**
- Avg GPU power: **0.9 mW**
- Avg package power: **6,388.8 mW**
- Estimated energy (benchmark window): **191.42 J**

## Memory Pressure

- Samples collected: **33** (~1s each)
- Avg used (active + wired + compressor): **22.5 GiB**
- Peak used: **22.7 GiB**
- Avg free: **8.6 GiB**
- Min free: **8.5 GiB**
- Avg compressed (logical): **7.2 GiB**
- Peak compressed: **7.2 GiB**

## Storage

- **Raw data files:** 14.1 MiB (14,770,176 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 0 B (0 bytes)
- Metadata/bookkeeping: 0 B (0 bytes)
- Preallocated scratch (excluded): 0 B (0 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 14.1 MiB (14,770,176 bytes)
- Classified sum: 14.1 MiB (14,770,176 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `dump.rdb` | 14.1 MiB |

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
