# Neo4j — Northwind Benchmark Report

**Run:** `2026-09-28T00:38:20.030559-07:00` → `2026-09-28T00:38:53.314858-07:00`
**Endpoint:** `bolt://localhost:7687` (database `neo4j`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **6,588.26 ms**
- Wipe duration: **125.66 ms**
- Index setup duration: **547.96 ms**
- Ingestion duration (row generation and writes): **5,914.62 ms**
- Ingestion nodes/sec: **16,474.42**
- Ingestion relationships/sec: **52,759.07**
- Seed batch size: **500 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 7.99 | 7.79 | 9.19 | 10.10 | 7.18 | 10.45 | 0.68 | 124.68 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 207.03 | 199.20 | 252.24 | 277.04 | 194.14 | 278.10 | 20.20 | 4.83 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 67.21 | 66.75 | 70.01 | 72.07 | 66.11 | 72.57 | 1.43 | 14.87 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 85.34 | 85.47 | 87.19 | 87.38 | 83.41 | 87.45 | 1.23 | 11.72 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 8.42 | 8.11 | 8.99 | 10.92 | 7.83 | 11.67 | 0.72 | 118.65 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 9.09 | 8.97 | 9.43 | 11.21 | 8.79 | 11.89 | 0.55 | 109.93 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 79.65 | 76.72 | 95.89 | 96.95 | 72.43 | 97.36 | 8.10 | 12.55 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 73.28 | 73.00 | 76.73 | 77.33 | 71.42 | 77.57 | 1.61 | 13.64 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 81.38 | 81.16 | 83.88 | 85.13 | 79.53 | 85.54 | 1.37 | 12.29 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 66.52 | 66.02 | 68.53 | 69.52 | 65.56 | 69.69 | 1.04 | 15.03 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 9.87 | 9.71 | 10.53 | 11.94 | 9.58 | 12.39 | 0.53 | 100.96 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 1.28 | 1.18 | 1.34 | 3.04 | 1.09 | 3.74 | 0.47 | 757.92 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 30.11 | 29.77 | 31.61 | 31.68 | 29.35 | 31.71 | 0.73 | 33.20 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 1.14 | 1.03 | 1.30 | 3.14 | 0.86 | 3.88 | 0.53 | 841.78 |

- **Overall mean latency:** 52.02 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 15.83 ops/sec
- **Query-latency-only aggregate throughput:** 19.22 ops/sec
- **Query-loop duration:** 26.531 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 52.999 s

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

- Samples collected: **51** (~1s each)
- Sampled duration: **51.54 s**
- Avg CPU power: **6,391.0 mW**
- Avg GPU power: **9.7 mW**
- Avg package power: **6,400.7 mW**
- Estimated energy (benchmark window): **329.89 J**

## Memory Pressure

- Samples collected: **55** (~1s each)
- Avg used (active + wired + compressor): **19.6 GiB**
- Peak used: **19.9 GiB**
- Avg free: **565.8 MiB**
- Min free: **52.4 MiB**
- Avg compressed (logical): **20.6 GiB**
- Peak compressed: **20.6 GiB**

## Storage

- **Raw data files:** 50.7 MiB (53,207,040 bytes)
- Indexes/stats: 7.6 MiB (7,970,816 bytes)
- Write-ahead logs: 144.6 MiB (151,674,880 bytes)
- Metadata/bookkeeping: 1.1 MiB (1,191,936 bytes)
- Preallocated scratch (excluded): 4.0 KiB (4,096 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 204.1 MiB (214,048,768 bytes)
- Classified sum: 204.1 MiB (214,048,768 bytes, Δ vs du = +0 bytes)

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
