# NornicDB — Northwind Benchmark Report

**Run:** `2026-05-13T12:51:11.555582-07:00` → `2026-05-13T12:51:22.112128-07:00`
**Endpoint:** `bolt://localhost:17687` (database `nornic`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **6,795.99 ms**
- Iterations per query: **10**

## Query Latency

| Query | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.23 | 0.23 | 0.26 | 0.26 | 0.18 | 0.26 | 0.03 | 4,022.12 |
| `customer_category_distinct_orders` | 0.25 | 0.25 | 0.27 | 0.28 | 0.23 | 0.28 | 0.01 | 3,946.72 |
| `optional_match_orders_count` | 0.20 | 0.20 | 0.24 | 0.25 | 0.18 | 0.26 | 0.02 | 4,467.36 |
| `revenue_by_product` | 0.24 | 0.22 | 0.33 | 0.36 | 0.16 | 0.37 | 0.06 | 4,170.79 |

- **Overall mean latency:** 0.23 ms
- **Overall throughput:** 17.70 ops/sec
- **Total benchmark wall-clock (sampled):** 13.999 s

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

- Samples collected: **13** (~1s each)
- Sampled duration: **13.16 s**
- Avg CPU power: **8,923.5 mW**
- Avg GPU power: **39.9 mW**
- Avg package power: **8,963.4 mW**
- Estimated energy (benchmark window): **118.00 J**

## Memory Pressure

- Samples collected: **16** (~1s each)
- Avg used (active + wired + compressor): **31.8 GiB**
- Peak used: **32.5 GiB**
- Avg free: **13.1 GiB**
- Min free: **12.3 GiB**
- Avg compressed (logical): **17.1 GiB**
- Peak compressed: **17.1 GiB**

## Storage

- **Raw data files:** 133.1 MiB (139,575,296 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 260.0 KiB (266,240 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 1.0 MiB (1,048,576 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 134.4 MiB (140,898,304 bytes)
- Classified sum: 134.4 MiB (140,898,304 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000001.sst` | 55.1 MiB |
| `000002.sst` | 54.9 MiB |
| `000003.sst` | 23.1 MiB |
| `000001.vlog` | 4.0 KiB |

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
