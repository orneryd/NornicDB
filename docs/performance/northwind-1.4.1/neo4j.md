# Neo4j — Northwind Benchmark Report

**Run:** `2026-10-09T09:31:27.408652-07:00` → `2026-10-09T09:31:54.389442-07:00`
**Endpoint:** `bolt://localhost:7687` (database `neo4j`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **5,390.93 ms**
- Wipe duration: **100.14 ms**
- Index setup duration: **492.42 ms**
- Ingestion duration (row generation and writes): **4,798.36 ms**
- Ingestion nodes/sec: **20,306.93**
- Ingestion relationships/sec: **65,032.61**
- Seed batch size: **500 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 6.33 | 6.25 | 7.35 | 8.65 | 5.62 | 9.10 | 0.68 | 157.49 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 172.48 | 171.01 | 182.50 | 192.73 | 165.32 | 196.69 | 7.12 | 5.80 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 53.05 | 52.90 | 54.88 | 55.75 | 51.66 | 56.09 | 0.99 | 18.84 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 67.65 | 67.10 | 70.47 | 71.28 | 65.36 | 71.49 | 1.51 | 14.78 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 6.87 | 6.52 | 8.75 | 9.82 | 5.87 | 9.99 | 1.03 | 145.33 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 6.74 | 6.66 | 6.89 | 8.39 | 6.46 | 8.99 | 0.44 | 148.13 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 63.18 | 62.36 | 68.46 | 69.24 | 60.74 | 69.48 | 2.37 | 15.82 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 60.92 | 60.68 | 62.69 | 63.42 | 59.30 | 63.68 | 0.96 | 16.41 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 69.97 | 69.60 | 72.59 | 73.82 | 68.21 | 74.29 | 1.48 | 14.29 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 48.79 | 48.51 | 50.78 | 51.67 | 48.05 | 51.69 | 0.85 | 20.49 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 7.57 | 7.47 | 7.79 | 8.97 | 7.32 | 9.45 | 0.37 | 131.51 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 0.95 | 0.84 | 1.07 | 2.58 | 0.79 | 3.19 | 0.43 | 1,015.91 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 22.58 | 22.05 | 25.98 | 26.36 | 21.55 | 26.40 | 1.29 | 44.26 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 0.95 | 0.83 | 1.22 | 2.85 | 0.67 | 3.50 | 0.51 | 1,018.17 |

- **Overall mean latency:** 42.00 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 19.57 ops/sec
- **Query-latency-only aggregate throughput:** 23.81 ops/sec
- **Query-loop duration:** 21.458 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 44.367 s

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

- Samples collected: **43** (~1s each)
- Sampled duration: **43.98 s**
- Avg CPU power: **5,859.9 mW**
- Avg GPU power: **1.2 mW**
- Avg package power: **5,861.1 mW**
- Estimated energy (benchmark window): **257.77 J**

## Memory Pressure

- Samples collected: **47** (~1s each)
- Avg used (active + wired + compressor): **23.3 GiB**
- Peak used: **23.5 GiB**
- Avg free: **7.2 GiB**
- Min free: **6.6 GiB**
- Avg compressed (logical): **7.2 GiB**
- Peak compressed: **7.2 GiB**

## Storage

- **Raw data files:** 50.7 MiB (53,207,040 bytes)
- Indexes/stats: 7.5 MiB (7,913,472 bytes)
- Write-ahead logs: 144.4 MiB (151,400,448 bytes)
- Metadata/bookkeeping: 1.1 MiB (1,191,936 bytes)
- Preallocated scratch (excluded): 4.0 KiB (4,096 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 203.8 MiB (213,716,992 bytes)
- Classified sum: 203.8 MiB (213,716,992 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `databases/neo4j/neostore.propertystore.db` | 18.1 MiB |
| `databases/neo4j/neostore.propertystore.db.strings` | 14.9 MiB |
| `databases/neo4j/neostore.relationshipstore.db` | 10.2 MiB |
| `databases/neo4j/neostore.propertystore.db.arrays` | 5.9 MiB |
| `databases/neo4j/neostore.nodestore.db` | 1.4 MiB |
| `databases/neo4j/neostore.relationshipgroupstore.degrees.db` | 48.0 KiB |
| `databases/system/neostore.relationshipgroupstore.degrees.db` | 40.0 KiB |
| `databases/neo4j/neostore` | 8.0 KiB |
| `databases/neo4j/neostore.labeltokenstore.db.names` | 8.0 KiB |
| `databases/neo4j/neostore.relationshiptypestore.db.names` | 8.0 KiB |

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
