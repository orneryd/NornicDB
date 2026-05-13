# Neo4j — Northwind Benchmark Report

**Run:** `2026-05-13T12:51:35.861062-07:00` → `2026-05-13T12:51:46.908517-07:00`
**Endpoint:** `bolt://localhost:7687` (database `neo4j`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **5,750.07 ms**
- Iterations per query: **10**

## Query Latency

| Query | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 9.34 | 8.88 | 12.82 | 13.01 | 7.36 | 13.06 | 1.96 | 106.74 |
| `customer_category_distinct_orders` | 226.67 | 228.87 | 259.13 | 261.76 | 184.64 | 262.41 | 26.98 | 4.41 |
| `optional_match_orders_count` | 72.58 | 72.00 | 84.98 | 85.57 | 63.23 | 85.71 | 8.41 | 13.77 |
| `revenue_by_product` | 85.96 | 84.17 | 93.62 | 95.71 | 80.46 | 96.23 | 4.70 | 11.63 |

- **Overall mean latency:** 98.64 ms
- **Overall throughput:** 7.76 ops/sec
- **Total benchmark wall-clock (sampled):** 31.630 s

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

✅ No intra-run correctness errors.

## Power Consumption

- Samples collected: **30** (~1s each)
- Sampled duration: **30.43 s**
- Avg CPU power: **8,608.1 mW**
- Avg GPU power: **95.4 mW**
- Avg package power: **8,703.5 mW**
- Estimated energy (benchmark window): **264.87 J**

## Memory Pressure

- Samples collected: **34** (~1s each)
- Avg used (active + wired + compressor): **32.5 GiB**
- Peak used: **33.4 GiB**
- Avg free: **12.0 GiB**
- Min free: **10.9 GiB**
- Avg compressed (logical): **17.1 GiB**
- Peak compressed: **17.1 GiB**

## Storage

- **Raw data files:** 50.7 MiB (53,207,040 bytes)
- Indexes/stats: 7.5 MiB (7,823,360 bytes)
- Write-ahead logs: 144.4 MiB (151,363,584 bytes)
- Metadata/bookkeeping: 1.1 MiB (1,191,936 bytes)
- Preallocated scratch (excluded): 4.0 KiB (4,096 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 203.7 MiB (213,590,016 bytes)
- Classified sum: 203.7 MiB (213,590,016 bytes, Δ vs du = +0 bytes)

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
