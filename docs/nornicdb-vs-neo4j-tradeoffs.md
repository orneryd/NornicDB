# NornicDB vs Neo4j — Tradeoffs

Snapshot of where NornicDB lands against Neo4j on the Northwind 48k-product / 48k-order benchmark, after the recent storage and codec work (id dictionary, compact edge codec, MVCC head index, property-key dictionary). Numbers are from `benchmark_northwind_vs_neo4j.sh`, 10 iterations after 2 warmup, Bolt protocol via `neo4j-go-driver`.

## At a glance

| Dimension | NornicDB | Neo4j | Delta | Who wins |
|---|---:|---:|---:|---|
| Mean query latency | 0.23 ms | 98.64 ms | **432× faster** | NornicDB |
| Throughput (ops/s) | 17.70 | 7.76 | 2.28× higher | NornicDB |
| Benchmark wall-clock | 14.0 s | 31.6 s | 2.26× faster | NornicDB |
| Energy per workload | 118 J | 265 J | 55% less | NornicDB |
| GPU avg power | 40 mW | 95 mW | 58% less | NornicDB |
| CPU/package avg power | 8.96 W | 8.70 W | +3% | tie |
| Peak memory used | 32.5 GiB | 33.4 GiB | -3% | tie |
| Seed duration | 6.80 s | 5.75 s | +18% | Neo4j |
| Raw data files | 133 MiB | 51 MiB | 2.62× larger | Neo4j |
| Write-ahead log | 260 KiB | 144 MiB | 555× smaller | NornicDB |
| Total disk (`du`) | 134 MiB | 204 MiB | -34% | NornicDB |

## Tradeoffs by category

| Category | NornicDB strength | Neo4j strength | Why |
|---|---|---|---|
| **Read latency** | sub-millisecond on every query class — `customer_category_distinct_orders` runs ~900× faster than Neo4j (0.25 ms vs 227 ms) | — | Numeric-ID secondary indexes (8B per ref instead of 40-50B UUID strings), compact edge codec, in-memory dictionary lookups, MVCC head index pointing at current bodies without scan |
| **Write latency / seed** | — | 18% faster on the 168k-edge seed | Neo4j is in-place record-store mutation; NornicDB writes the body, the id-dict forward+reverse, the property-key forward+reverse, six secondary indexes, and an MVCC head per record. Acceptable cost for the read-side win |
| **Energy efficiency** | 55% less energy per equivalent workload, 58% lower GPU usage | — | Mostly a function of finishing 2.26× faster at equal package power. GPU delta is real — NornicDB doesn't push query work onto the GPU |
| **Body bytes on disk** | — | Record-store layout is denser per record | LSM amplifies (multiple SST levels, value log preallocation, MVCC archives). V2 property-key tokenization recovered ~20% of the body cost but the LSM gap is structural |
| **Total disk footprint** | 34% less when WAL is included | — | Neo4j's WAL is 144 MiB (transactional log durability); NornicDB's is 260 KiB (Badger handles durability through memtable + value log) |
| **Operational simplicity** | Single binary, embeds Badger directly, no JVM | Mature ops tooling, established backup/restore, broad cloud presence | Different points on the maturity curve — NornicDB is leaner; Neo4j has the runway behind it |
| **Multi-tenant isolation** | Per-namespace id space and property-key dict; one tenant's schema sprawl can't exhaust another's | Shared store, single global token table | NornicDB designed for multi-tenant from the namespace prefix up |
| **Schema flexibility** | No declared schema required; properties allocate dictionary IDs on first sight | Optional indexes / constraints surface explicitly | Neo4j's explicitness helps planner / DBA workflow; NornicDB stays out of your way |
| **Memory footprint at idle** | similar (~32 GiB peak system-wide during the bench, broadly tied) | similar | Neither engine is the memory bottleneck on a 100k-entity workload |
| **Maturity / ecosystem** | — | Cypher reference implementation, broad tooling, Bloom, APOC, GDS, hosted offerings | Hard tradeoff to argue against — Neo4j has been in production for 15+ years |

## Where the wins come from (architecturally)

| Lever | Effect |
|---|---|
| `idDictionary` | 40-50B string IDs → 8B numIDs in every secondary index. Indexes shrink by ~80%, fewer cache misses on traversal |
| Compact edge codec | Drops the per-record `gob`/`msgpack` field-name overhead (`StartNode`, `EndNode`, `Type`, etc.). Endpoints are 8B numIDs; scalar metadata is fixed-width binary |
| MVCC head index | Reads of "current state" hit a single pointer key instead of scanning version chains. Latency on point lookups is dominated by Badger's block cache, not the engine |
| Property-key dictionary (V2) | Property names tokenized to per-namespace varint IDs. ~20% body size reduction on shared schemas |
| Per-txn counter batching | Dictionary counter writes amortize to one Set per dirty namespace per commit, not one per allocation. Seed-heavy workloads pay 2 counter writes total instead of N |
| Badger value log | Large blob values (embeddings, descriptions) live in the value log out of the LSM key path. Block cache only holds the LSM keys + small inline values |

## Where Neo4j is structurally ahead

| Lever | Effect |
|---|---|
| Fixed-size record store | Each entity is N bytes wherever it lives. No LSM amplification, no value log, no MVCC archive. Smaller raw-data bucket |
| In-place mutation | Updates rewrite the same record in place. NornicDB's MVCC archives the prior body for snapshot isolation, paying disk for read consistency |
| Mature query planner | Multi-table cost-based optimization, hint syntax, dump/restore, online indexes |
| Tooling depth | Bloom for visualization, APOC for utility procedures, GDS for graph algorithms, neo4j-admin for ops |

## Scalability outlook

| Scale axis | Expected behavior |
|---|---|
| Linear corpus growth (more nodes/edges of same shape) | Latency advantage **holds or widens**. Numeric-ID indexes don't slow down with relationship count; Neo4j traversal does |
| Schema growth (more property names) | Dictionary stays tiny — ~30B per ever-used name. Even 10k distinct names per namespace is well under 1 MB of dict |
| Multi-tenant scale (more namespaces) | Per-namespace dict isolation prevents cross-tenant interference. Memory cost is O(distinct names × namespaces); negligible until tenant count is in the thousands |
| Hot-data working set vs total store | Badger's block cache is the dominant factor. Smaller bodies (V2) mean more records fit in cache; V1→V2 directly improves cache hit rate |
| WAL retention | NornicDB's 555× smaller WAL becomes the dominant cost saver over time on workloads with high write volume — Neo4j's transactional log grows unbounded between checkpoints |

## Honest cost line

- **One-way storage upgrade.** Moving an existing NornicDB store to V2 requires `--upgrade-storage` and a backup. No downgrade path.
- **First-open cost on V1 stores.** Eager rewrite is bounded by corpus size — seconds on this 48k seed, hours on a 1B-record store. Resumable and crash-safe, but not free.
- **Less mature than Neo4j on cypher edge cases.** Bolt protocol is implemented to the spec the benchmark exercises; less-common Cypher constructs may need feature parity work.

## Bottom line

NornicDB trades a 2.6× larger raw-data bucket for a **432× faster mean query latency**, **55% less energy per workload**, and a **555× smaller WAL**. Total disk footprint actually favors NornicDB once WAL is included. The latency and energy wins come from the index architecture (numeric IDs, compact codecs, MVCC head pointers); the body-size delta is structural to LSM vs record-store and is partially closed by the property-key dictionary.

For workloads dominated by reads, traversals, and aggregations on graph data — i.e. most graph-database use cases — the architectural wins compound. For workloads where storage cost per record matters more than query latency, the gap is real but narrowing, and the WAL difference may flip the balance over time.
