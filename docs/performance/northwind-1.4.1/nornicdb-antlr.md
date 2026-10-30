# NornicDB (ANTLR parser) — Northwind Benchmark Report

**Run:** `2026-10-09T09:30:52.442158-07:00` → `2026-10-09T09:31:17.72512-07:00`
**Endpoint:** `bolt://localhost:17687` (database `nornic`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **5,354.84 ms**
- Wipe duration: **22.65 ms**
- Index setup duration: **10.05 ms**
- Ingestion duration (row generation and writes): **5,322.12 ms**
- Ingestion nodes/sec: **18,308.49**
- Ingestion relationships/sec: **58,632.64**
- Seed batch size: **500 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 0.18 | 0.18 | 0.21 | 0.23 | 0.14 | 0.23 | 0.02 | 5,020.33 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 0.18 | 0.17 | 0.22 | 0.29 | 0.15 | 0.32 | 0.03 | 5,488.64 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 0.19 | 0.18 | 0.23 | 0.25 | 0.16 | 0.26 | 0.03 | 4,807.92 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 0.16 | 0.15 | 0.18 | 0.20 | 0.13 | 0.20 | 0.02 | 6,224.71 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 0.14 | 0.14 | 0.16 | 0.17 | 0.13 | 0.18 | 0.01 | 6,700.67 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 0.13 | 0.13 | 0.15 | 0.15 | 0.12 | 0.16 | 0.01 | 7,268.54 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 0.20 | 0.20 | 0.24 | 0.25 | 0.18 | 0.25 | 0.02 | 4,367.55 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 0.16 | 0.16 | 0.19 | 0.20 | 0.14 | 0.21 | 0.02 | 5,985.98 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 0.16 | 0.15 | 0.18 | 0.18 | 0.14 | 0.18 | 0.01 | 6,132.30 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 0.14 | 0.14 | 0.17 | 0.18 | 0.12 | 0.19 | 0.02 | 6,835.79 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 0.20 | 0.19 | 0.22 | 0.24 | 0.18 | 0.24 | 0.01 | 4,488.30 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 0.19 | 0.19 | 0.20 | 0.21 | 0.17 | 0.21 | 0.01 | 4,635.35 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 0.14 | 0.13 | 0.17 | 0.17 | 0.11 | 0.17 | 0.02 | 7,048.87 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 0.21 | 0.21 | 0.22 | 0.23 | 0.19 | 0.23 | 0.01 | 4,188.51 |

- **Overall mean latency:** 0.17 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 21.08 ops/sec
- **Query-latency-only aggregate throughput:** 5,908.17 ops/sec
- **Query-loop duration:** 19.923 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 27.372 s

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

- Samples collected: **26** (~1s each)
- Sampled duration: **26.48 s**
- Avg CPU power: **9,073.6 mW**
- Avg GPU power: **172.5 mW**
- Avg package power: **9,246.1 mW**
- Estimated energy (benchmark window): **244.88 J**

## Memory Pressure

- Samples collected: **30** (~1s each)
- Avg used (active + wired + compressor): **23.4 GiB**
- Peak used: **23.9 GiB**
- Avg free: **7.3 GiB**
- Min free: **6.8 GiB**
- Avg compressed (logical): **7.2 GiB**
- Peak compressed: **7.2 GiB**

## Storage

- **Raw data files:** 148.2 MiB (155,422,720 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 260.0 KiB (266,240 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 1.0 MiB (1,048,576 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 149.5 MiB (156,745,728 bytes)
- Classified sum: 149.5 MiB (156,745,728 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000002.sst` | 49.9 MiB |
| `000003.sst` | 49.4 MiB |
| `000004.sst` | 48.9 MiB |
| `000001.sst` | 4.0 KiB |
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
