# NornicDB — Northwind Benchmark Report

**Run:** `2026-11-27T23:16:24.46076-07:00` → `2026-11-27T23:35:25.624753-07:00`
**Endpoint:** `bolt://127.0.0.1:27687` (database `matrix`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **12,466.91 ms**
- Wipe duration: **3.12 ms**
- Index setup duration: **8.04 ms**
- Ingestion duration (row generation and writes): **12,455.72 ms**
- Ingestion nodes/sec: **7,822.91**
- Ingestion relationships/sec: **25,052.74**
- Seed batch size: **2,000 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 722.15 | 692.04 | 1,014.62 | 1,284.96 | 531.91 | 1,393.95 | 186.96 | 1.38 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 4,917.52 | 4,349.29 | 6,623.69 | 6,751.27 | 4,253.33 | 6,772.51 | 852.79 | 0.20 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 4,730.15 | 4,346.47 | 6,361.00 | 6,662.98 | 3,755.53 | 6,693.48 | 913.67 | 0.21 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 2,856.13 | 2,854.49 | 3,650.33 | 3,925.52 | 2,350.77 | 3,999.33 | 438.15 | 0.35 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 530.54 | 511.29 | 622.61 | 673.61 | 478.50 | 684.23 | 47.75 | 1.88 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 390.90 | 384.80 | 421.80 | 431.10 | 365.75 | 433.39 | 17.67 | 2.56 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 5,210.81 | 5,642.26 | 6,384.79 | 6,402.00 | 3,820.21 | 6,404.67 | 951.67 | 0.19 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 4,081.93 | 3,633.72 | 5,853.95 | 6,121.74 | 3,397.54 | 6,213.63 | 874.36 | 0.24 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 3,512.01 | 3,305.42 | 4,731.69 | 5,216.77 | 3,145.56 | 5,349.23 | 555.36 | 0.28 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 2,117.68 | 1,969.51 | 3,025.38 | 3,442.29 | 1,889.87 | 3,517.03 | 388.48 | 0.47 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 323.69 | 303.91 | 407.83 | 597.31 | 226.18 | 663.56 | 77.78 | 3.09 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 3.22 | 3.25 | 3.41 | 3.55 | 2.94 | 3.60 | 0.15 | 307.12 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 2,781.97 | 2,559.71 | 3,908.20 | 4,532.93 | 2,486.56 | 4,783.41 | 543.75 | 0.36 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 1.35 | 1.26 | 1.61 | 1.91 | 1.18 | 2.01 | 0.18 | 710.42 |

- **Overall mean latency:** 2,298.58 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 0.37 ops/sec
- **Query-latency-only aggregate throughput:** 0.44 ops/sec
- **Query-loop duration:** 1,128.408 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 1,141.351 s

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

- _No powermetrics samples available:_ missing: /tmp/pr1826-matrix-1127/reports-final2/standard-projected-false/nornicdb.powermetrics.plist

## Memory Pressure

- _No vm_stat samples available:_ missing: /tmp/pr1826-matrix-1127/reports-final2/standard-projected-false/nornicdb.vmstat.log

## Storage

- **Raw data files:** 158.9 MiB (166,612,992 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 8.0 KiB (8,192 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 136.0 MiB (142,630,912 bytes)
- Unclassified (other): 4.0 KiB (4,096 bytes)
- Full data directory `du`: 294.9 MiB (309,264,384 bytes)
- Classified sum: 294.9 MiB (309,264,384 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000004.sst` | 55.1 MiB |
| `000003.sst` | 52.1 MiB |
| `000002.sst` | 51.7 MiB |
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
