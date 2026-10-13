# Release Notes — v1.4.2

> **Scope:** All changes between tag `v1.4.1` (`67105489`) and `v1.4.2` — 340 commits on `main`. The full list is in [CHANGELOG.md](../CHANGELOG.md#142).

---

## Highlights

- **Cypher 25 query structure and GQL path patterns.** `NEXT`, `WHEN … THEN … ELSE`, `GROUP BY`, interpolated strings, map comprehensions, path selectors (`SHORTEST k`, `ALL TRAIL`, …), quantified path patterns, match modes and dynamic labels and types, checked against Neo4j 2026.09 and 5.26 (#907, #1028).
- **`VECTOR` and `UUID` values** with Neo4j's functions, ordering, type checks and Bolt / HTTP encodings, and `vector.similarity.*` scores that match Neo4j bit for bit.
- **Relationship search and graph retrieval.** Keyword and vector search over relationship properties, `db.retrieveGraph` / `db.rretrieveGraph` ranking nodes and relationships together, a `matches` column with the exact passage that matched, and a Cypher `where` filter checked before `limit` (#1060, #1065, #1133).
- **Change Data Capture** in Neo4j's event format (`txLogEnrichment`, `db.cdc.*`), with Bolt `tx_metadata` (#937).
- **Durable writes by default, plus an opt-in write-behind buffer** that sizes itself, writes to the WAL before acknowledging, and is about 3x faster per statement.
- **Bulk loads under constraints stay linear.** `UNIQUE`, `NODE KEY`, `TEMPORAL NO OVERLAP` and `REQUIRE { }` contracts no longer slow down as data grows (#1114, #1120, #1126).
- **Many correctness fixes** across Cypher semantics, subqueries, `CASE`, APOC functions, storage, restore and search.

---

## 1. Cypher 25 and GQL

### Query structure

```cypher
CYPHER 25
MATCH (o:Order) RETURN o.customer AS c, count(*) AS n
NEXT
WHEN n > 10 THEN RETURN c, 'frequent' AS kind
ELSE RETURN c, 'occasional' AS kind
```

- `NEXT` chains query parts; `WHEN … THEN … ELSE` picks a branch; `{ q }` groups UNIONs; `RETURN` / `WITH … GROUP BY` groups explicitly.
- `RETURN ALL` / `WITH ALL`, interpolated strings (`s"{p.name} is {p.age}"`), map comprehensions, `IS [NOT] LABELED`, dynamic label tests, `allReduce(...)` and `PROPERTY_EXISTS(n, key)`.
- Any clause may follow a `SHOW` command's `YIELD`.
- Cypher 25 statements follow Neo4j 2026.09's rules where they differ from 5.26 (parameters, `toString()`, string concatenation, `substring()` bounds, removed `SET x = entity` forms). Cypher 5 statements keep 5.26's rules.

### Path patterns

```cypher
MATCH p = SHORTEST 2 (a:Station {name: 'A'})((x)-[r:LINK]->(y) WHERE r.open){1,5}(b:Station {name: 'B'})
RETURN p
```

- Path selectors and modes: `ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k [GROUPS]`, `ANY [k]`, `ALL [WALK | TRAIL | ACYCLIC]`.
- Quantified and parenthesised path patterns, with group variables bound as lists and per-iteration `WHERE`.
- `MATCH REPEATABLE ELEMENTS` and `MATCH DIFFERENT RELATIONSHIPS`.
- Dynamic labels, types and keys: `$(expr)`, `$all(expr)`, `$any(expr)` in patterns, `SET` / `REMOVE n:$(expr)`, `SET n[key] = v`.

### Values, types and functions

- `VECTOR` and `UUID`: `vector(...)`, `vector_distance`, `vector_norm`, `uuid()` (v7) and friends, with `IS :: VECTOR(n)` / `IS :: UUID`.
- Property type constraints accept every Neo4j type, unions and synonyms, plus `LIST<T>`; every Neo4j spelling of a uniqueness or key constraint is accepted.
- New functions: `coll.*`, `string.*`, `cardinality()`, GQL aliases (`ceiling`, `ln`, `zoned_datetime`, `percentile_cont`, …), `format(value, pattern)`. Pattern parsing (`date('1986-11-18', 'yyyy-MM-dd')`) is Cypher 25 only.
- `db.schema.nodeTypeProperties()` / `relTypeProperties()`, kind-specific `SHOW … CONSTRAINTS`, and the Cypher 25 `SHOW TRANSACTIONS` columns.

---

## 2. Search and Retrieval

```cypher
CALL db.retrieveGraph({query: $q, where: 'node.current IS NULL OR node.current <> false'})
YIELD kind, node, relationship, score, matches
RETURN kind, coalesce(node, relationship) AS hit, score,
       [m IN matches | m.property] AS matchedProperties
```

- **Relationship search** (`NORNICDB_SEARCH_RELATIONSHIPS_ENABLED=true`, off by default): relationship properties get their own keyword and vector indexes, searched with `db.retrieveRelationships` or `relationships: true` on `POST /nornicdb/search`. Property lists are separate from the node lists (#1065, #1150, #1154).
- **`db.retrieveGraph` / `db.rretrieveGraph`** rank nodes and relationships in one list, with an optional rerank (#1139).
- **`matches`** on every hit: the best embedding chunks or query-term occurrences, with `[start, end)` code-point offsets into the property (#1060, #1152, #1157).
- **`where`** filters candidates with any Cypher predicate before `limit` (#1133, #1135).
- **Searches report partial results:** dropped query chunks, failed or flat reranks and `minRRFScore` cuts set `fallback_reason` (#907, #1019, #1092, #1141).
- `NORNICDB_SEARCH_BM25_LABELS` limits keyword indexing by label (#1067), and `NORNICDB_SEARCH_QUERY_MAX_CHUNKS` / `_CHUNK_OVERFLOW` control long queries.
- Embeddings no longer go missing after bulk writes, during the first HNSW build, or because numeric list properties set a wrong index dimension (#1116).

---

## 3. Durability, Write-Behind and CDC

- **Durable by default.** `NORNICDB_ASYNC_WRITES_ENABLED` now defaults to `false`; every Cypher write commits through the transaction, as in Neo4j. The old `AsyncEngine` is removed.
- **Opt-in write-behind buffer.** With `NORNICDB_ASYNC_WRITES_ENABLED=true`, auto-commit statements are acknowledged after validation and a WAL write, then flushed to Badger in the background. Acknowledged writes are visible to reads and constraint checks immediately, and survive a process kill (not a power loss). `CREATE` drops from 60 µs to 21 µs per statement. The flush interval and batch size adapt to measured latency; HTTP returns `202 Accepted` with `X-NornicDB-Consistency: eventual`.
- **Change Data Capture.** Enable per database with `txLogEnrichment` (`FULL` or `DIFF`), then read with `db.cdc.current()`, `db.cdc.earliest()` and `db.cdc.query(...)`. Events are written in the same commit as the data and carry key-constraint values. See [Change Data Capture](user-guides/change-data-capture.md) (#937).
- **Smaller WAL:** raw-byte payloads and zstd compression cut the Northwind seed's WAL from 743 MB to 62 MB.
- **Fixes:** `/admin/restore` takes the store offline safely (#1020); stale cache reads after concurrent writes (#1024); auto-compaction honours WAL retention; IDs lost after a rolled-back fixed-ID create; `TERMINATE TRANSACTIONS` stops commit-time validation (#1115).

---

## 4. Constraints and Bulk-Load Performance

| Workload | Before | After |
|---|---|---|
| 16,000 nodes under one `UNIQUE` constraint, one transaction | 10.3 s | 161 ms |
| 1,000-row `MERGE` batch, composite `NODE KEY` (after 8,000 nodes) | 5.2 s | ~0.13 s |
| 200-node `MERGE` under `TEMPORAL NO OVERLAP` | grew to 217 ms | ~41 ms |
| 8,000 temporal nodes in one transaction | 79 s | 98 ms |
| 8,000 buffered writes under write-behind | 8.5 s | 106 ms |
| 200-row relationship `MERGE` from a busy node | 2.87 s | 8.6 ms |
| Relationship constraint DDL with 100k edges in another database | 55 ms | 0.13 ms |

- A `UNIQUE` entry in a `REQUIRE { }` contract builds its lookup at creation; before, a 100k-node load never finished.
- Node `TEMPORAL NO OVERLAP` constraints take composite keys, and `LIST<T>` type constraints are supported.
- `DROP DATABASE` removes the schema, `DROP CONSTRAINT` drops contracts, and contract predicates treat an absent property as SQL `CHECK` does.
- Uniqueness and temporal checks look at the transaction's final state.

---

## 5. Cypher Correctness

Highlights from the compatibility work against Neo4j 5.26 and 2026.09:

- Pattern predicates and subquery expressions (`EXISTS`, `COUNT`, `COLLECT { }`) work anywhere a value is allowed, with correct scoping; all 109 subquery-scope differential statements match Neo4j (#1055).
- Arithmetic precedence and associativity (`10 - 3 - 2` gave 9, `2 ^ 3 ^ 2` gave 512).
- `CASE` with keyword-named variables, nested `CASE` and type parameters (#1136, #1144, #1145, #1162).
- Standalone `ORDER BY` / `SKIP` / `OFFSET` / `LIMIT` clauses; `OPTIONAL MATCH` row loss after `MATCH … WHERE … MATCH`.
- Unknown functions fail in every clause (#1166); function argument types are checked as in Neo4j.
- Trigonometric functions match Neo4j via a musl port.
- APOC: `apoc.coll.*`, `apoc.convert.*`, `apoc.algo.pageRank`, `apoc.cypher.runMany`, `apoc.custom.*` procedures (#1066, #1164).
- New extensions: `text.locate`, `uuid.v5`, `kp.suppress` / `kp.unsuppress`.

---

## 6. Other Performance Work

- Concurrent writes: ID lookups across 64 locks (69k → 148k nodes/s with 16 writers); commits no longer serialize on label counts.
- `UNWIND … CREATE` writes in one bulk operation; relationship batches reuse endpoint lookups.
- `{id: null}` patterns match nothing without a scan (900 ms → 3 ms for 500 lookups) (#1130).
- `MATCH ()-[r:T {id: $id}]->()` starts from the type index (0.95 s → 2.3 ms for 200 lookups).
- Long string parameters stay references in pipeline statements (#1094).

---

## Upgrade Notes

- **Writes are durable by default.** If you relied on the old async default for speed, set `NORNICDB_ASYNC_WRITES_ENABLED=true` to use the new write-behind buffer. `NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE` / `_EDGE_CACHE_SIZE` are deprecated in favour of `NORNICDB_ASYNC_MAX_OPS`.
- **Bolt connections are uncapped by default.** The server was always limited to 100; set `NORNICDB_BOLT_MAX_CONNECTIONS` to restore a cap. `server.bolt_*` YAML settings (TLS, timeouts, WebSocket) now take effect.
- **`vector.similarity.cosine` / `.euclidean` scores change** to Neo4j's `[0, 1]` scale, as do `db.index.vector.query*` scores. Re-tune any score thresholds.
- **Procedure renames.** Knowledge-policy and decay procedures move to `kp.*`; the `nornicdb.knowledgepolicy.*` and `nornicdb.decay.info` names still work but are deprecated.
- **Internal functions.** `__nornic_*` functions are no longer callable from user statements.
- **Temporal index rebuild.** Stores written by an earlier version rebuild their temporal history index once on first start, so `TEMPORAL NO OVERLAP` lookups stay correct across restarts.
- **ID repair.** Stores that rolled back fixed-ID creates under earlier versions may have nodes missing from traversal; run `nornicdb-admin database check-ids --repair` offline.
- **Stricter errors.** Unknown functions, wrong-typed arguments, `group_by` without `ranked_then_id`, and failed reranks now raise errors or report `fallback_reason` instead of returning quietly wrong results.
