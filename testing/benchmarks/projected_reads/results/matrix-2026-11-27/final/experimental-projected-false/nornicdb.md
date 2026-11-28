# NornicDB — Northwind Benchmark Report

**Run:** `2026-11-27T22:15:06.129016-07:00` → `2026-11-27T22:33:41.811427-07:00`
**Endpoint:** `bolt://127.0.0.1:27687` (database `matrix`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **10,587.32 ms**
- Wipe duration: **3.28 ms**
- Index setup duration: **9.28 ms**
- Ingestion duration (row generation and writes): **10,574.73 ms**
- Ingestion nodes/sec: **9,214.42**
- Ingestion relationships/sec: **29,509.02**
- Seed batch size: **2,000 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 664.47 | 635.63 | 938.18 | 1,103.68 | 476.98 | 1,144.12 | 167.38 | 1.50 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 4,956.15 | 4,460.06 | 6,754.88 | 7,912.68 | 4,207.89 | 8,259.28 | 963.31 | 0.20 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 4,637.41 | 4,280.51 | 6,303.91 | 6,790.23 | 3,752.08 | 6,951.26 | 915.20 | 0.22 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 2,690.26 | 2,398.70 | 3,730.05 | 3,902.69 | 2,278.84 | 3,969.48 | 533.89 | 0.37 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 489.22 | 485.36 | 507.56 | 510.29 | 473.78 | 511.25 | 12.40 | 2.04 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 456.29 | 439.95 | 669.55 | 712.94 | 366.12 | 729.29 | 98.86 | 2.19 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 3,785.30 | 3,536.52 | 5,321.42 | 5,582.59 | 3,341.31 | 5,596.72 | 647.95 | 0.26 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 4,038.12 | 3,679.43 | 5,210.17 | 6,143.22 | 3,334.71 | 6,477.47 | 761.35 | 0.25 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 4,096.61 | 3,917.75 | 5,593.78 | 5,731.82 | 3,185.87 | 5,749.86 | 756.09 | 0.24 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 2,410.42 | 2,210.39 | 3,359.42 | 3,560.72 | 1,905.41 | 3,623.39 | 510.62 | 0.41 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 264.72 | 252.21 | 338.59 | 365.96 | 230.89 | 373.15 | 38.51 | 3.78 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 2.72 | 2.71 | 2.77 | 2.92 | 2.63 | 2.97 | 0.06 | 363.10 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 2,976.40 | 2,780.34 | 4,131.61 | 4,395.45 | 2,558.39 | 4,460.11 | 539.85 | 0.34 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 1.82 | 1.73 | 2.58 | 2.72 | 1.35 | 2.76 | 0.39 | 530.54 |

- **Overall mean latency:** 2,247.85 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 0.38 ops/sec
- **Query-latency-only aggregate throughput:** 0.44 ops/sec
- **Query-loop duration:** 1,105.071 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 1,115.701 s

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

- _No powermetrics samples available:_ missing: /tmp/pr1826-matrix-1127/reports-final2/experimental-projected-false/nornicdb.powermetrics.plist

## Memory Pressure

- _No vm_stat samples available:_ missing: /tmp/pr1826-matrix-1127/reports-final2/experimental-projected-false/nornicdb.vmstat.log

## Storage

- **Raw data files:** 101.4 MiB (106,344,448 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 8.0 KiB (8,192 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 136.0 MiB (142,630,912 bytes)
- Unclassified (other): 4.0 KiB (4,096 bytes)
- Full data directory `du`: 237.5 MiB (248,995,840 bytes)
- Classified sum: 237.5 MiB (248,995,840 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000003.sst` | 34.1 MiB |
| `000002.sst` | 34.0 MiB |
| `000004.sst` | 33.3 MiB |
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
