# NornicDB — Northwind Benchmark Report

**Run:** `2026-11-27T18:45:36.243609-07:00` → `2026-11-27T19:10:51.937399-07:00`
**Endpoint:** `bolt://127.0.0.1:27687` (database `matrix`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **12,316.47 ms**
- Wipe duration: **2.82 ms**
- Index setup duration: **8.05 ms**
- Ingestion duration (row generation and writes): **12,305.56 ms**
- Ingestion nodes/sec: **7,918.37**
- Ingestion relationships/sec: **25,358.46**
- Seed batch size: **2,000 rows**
- Seed parallelism: **4 sessions per phase**
- Query workloads: **14**
- Iterations per query: **30**
- Warmup iterations per query: **5**

## Query Latency

| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | Product counts grouped by category, with a full result sort. | 30 | 826.33 | 864.07 | 1,052.37 | 1,309.84 | 508.78 | 1,412.96 | 208.45 | 1.21 |
| `customer_category_distinct_orders` | Four-hop customer-to-category traversal with distinct-order aggregation. | 30 | 6,864.20 | 7,259.54 | 8,174.87 | 8,395.69 | 4,388.78 | 8,481.32 | 1,192.35 | 0.15 |
| `optional_match_orders_count` | Optional product-to-order traversal with zero-match preservation and top-100 sorting. | 30 | 5,882.82 | 6,382.54 | 7,147.77 | 7,183.01 | 3,822.04 | 7,184.25 | 1,128.01 | 0.17 |
| `revenue_by_product` | Relationship-property arithmetic and revenue aggregation grouped by product. | 30 | 3,692.36 | 3,696.95 | 4,319.56 | 4,708.79 | 2,771.24 | 4,844.03 | 405.51 | 0.27 |
| `products_by_supplier` | Supplier-to-product traversal with top-N aggregation and deterministic ties. | 30 | 871.05 | 917.79 | 1,023.56 | 1,045.35 | 615.10 | 1,054.18 | 139.99 | 1.15 |
| `orders_by_customer` | Customer-to-order traversal grouped into a top-25 order-count ranking. | 30 | 488.01 | 452.10 | 630.86 | 707.14 | 367.86 | 733.14 | 91.36 | 2.05 |
| `revenue_by_category` | Three-hop category revenue aggregation from order-line quantities and product prices. | 30 | 5,244.20 | 5,573.75 | 6,501.36 | 6,761.64 | 3,460.81 | 6,828.34 | 1,074.59 | 0.19 |
| `revenue_by_supplier` | Supplier-to-product-to-order traversal with revenue aggregation and top-25 sorting. | 30 | 5,692.15 | 5,935.17 | 6,662.16 | 6,872.64 | 3,482.61 | 6,957.91 | 872.55 | 0.18 |
| `revenue_by_customer` | Customer-order-product traversal with relationship-property revenue aggregation. | 30 | 5,504.25 | 5,572.77 | 6,081.16 | 6,182.68 | 4,498.50 | 6,206.73 | 457.90 | 0.18 |
| `order_line_sales_by_country` | Order-line scan grouped by shipping country with line-count and unit aggregation. | 30 | 3,027.75 | 2,975.31 | 3,398.42 | 3,538.57 | 2,373.19 | 3,594.34 | 250.19 | 0.33 |
| `low_stock_products` | Selective numeric property filter followed by a stable top-100 product sort. | 30 | 750.05 | 762.36 | 879.69 | 1,120.36 | 505.07 | 1,202.31 | 133.44 | 1.33 |
| `products_in_category` | Selective category lookup and adjacent product traversal with a top-100 result. | 30 | 3.89 | 3.65 | 5.34 | 6.01 | 2.93 | 6.24 | 0.84 | 253.26 |
| `order_line_quantity_distribution` | Full relationship-property scan grouped by line quantity. | 30 | 4,183.43 | 4,295.56 | 4,783.88 | 4,835.46 | 3,172.26 | 4,836.98 | 479.99 | 0.24 |
| `customer_order_details` | Selective customer lookup followed by order-line expansion and computed row projection. | 30 | 1.93 | 1.71 | 2.73 | 3.19 | 1.31 | 3.38 | 0.59 | 500.54 |

- **Overall mean latency:** 3,073.74 ms
- **Measured query operations:** 420
- **End-to-end query-loop throughput:** 0.28 ops/sec
- **Query-latency-only aggregate throughput:** 0.33 ops/sec
- **Query-loop duration:** 1,501.512 s
- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.
- **Full lifecycle wall-clock (sampled):** 1,515.698 s

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

_No powermetrics samples available on this Mac._

## Memory Pressure

_No vm_stat samples collected for this run._

## Storage

- **Raw data files:** 110.6 MiB (115,994,624 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 8.0 KiB (8,192 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 138.4 MiB (145,162,240 bytes)
- Unclassified (other): 4.0 KiB (4,096 bytes)
- Full data directory `du`: 249.1 MiB (261,177,344 bytes)
- Classified sum: 249.1 MiB (261,177,344 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000002.sst` | 39.1 MiB |
| `000003.sst` | 37.5 MiB |
| `000004.sst` | 33.9 MiB |
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
