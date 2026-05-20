---
name: nornicdb-cypher-queries
description: Pick fast, predictable Cypher query shapes in NornicDB — point lookups, batch retrieval, pagination, search, traversal, batched UNWIND/MERGE writes, cleanup, multi-tenant isolation. Use when writing or reviewing Cypher whose latency or throughput matters; maps user intent to the executor's hot-path query templates.
---

# NornicDB Cypher Query Shapes (Hot-Path Cookbook)

NornicDB has a set of Cypher patterns that the executor recognizes and routes through specialized hot paths. Writing your queries to match these shapes is the single biggest lever for predictable latency. Outside of these shapes you still get correct results, but you fall onto the generic per-row path.

This skill is a menu of proven shapes plus the rules that decide whether the executor accepts them. The full reference with explanatory commentary lives at [`docs/performance/hot-path-query-cookbook.md`](../performance/hot-path-query-cookbook.md).

## Generic schema used in examples

- **Node labels:** `EntityA`, `EntityB`, `Tenant`, `Event`
- **Relationship types:** `KEY_REL`, `BELONGS_TO`, `LINKS_TO`
- **Properties:** `primaryKey`, `alternateKey`, `tenantId`, `category`, `status`, `createdAt`, `updatedAt`, `sessionId`

Substitute your own labels/types/properties. The shapes do not depend on the names.

## Recommended baseline indexes & constraints

```cypher
CREATE CONSTRAINT entitya_primarykey_uniq IF NOT EXISTS FOR (n:EntityA) REQUIRE n.primaryKey IS UNIQUE;
CREATE INDEX entitya_alternatekey_idx IF NOT EXISTS FOR (n:EntityA) ON (n.alternateKey);
CREATE INDEX entitya_tenantid_idx IF NOT EXISTS FOR (n:EntityA) ON (n.tenantId);
CREATE INDEX entityb_category_status_createdat_idx IF NOT EXISTS FOR (n:EntityB) ON (n.category, n.status, n.createdAt);
CREATE INDEX entityb_tenantid_idx IF NOT EXISTS FOR (n:EntityB) ON (n.tenantId);
CREATE INDEX event_createdat_idx IF NOT EXISTS FOR (e:Event) ON (e.createdAt);
```

Composite index order matters — list the index columns in the same order the query uses for filtering and sorting.

---

## 1. Point lookup & existence

### Upsert, then read by stable key
```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
SET   n.payload = $payload, n.updatedAt = $now;

MATCH (n:EntityA {primaryKey: $primaryKey})
RETURN n
LIMIT 1;
```

### Lookup by either of two keys (OR → UNION)
```cypher
CALL {
  WITH $lookupKey AS k
  MATCH (n:EntityA {primaryKey:   k}) RETURN n
  UNION
  WITH $lookupKey AS k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
RETURN n
LIMIT 1;
```
Always prefer this over a single `WHERE n.primaryKey = $k OR n.alternateKey = $k` predicate — the executor cannot use indexes through cross-property `OR`.

### Existence check
```cypher
MATCH (n:EntityA {primaryKey: $primaryKey})
RETURN 1 AS found
LIMIT 1;
```

### Direct ID seek
```cypher
MATCH (n:EntityA)
WHERE elementId(n) = $id
RETURN n
LIMIT 1;
```

---

## 2. Batch retrieval

### Bulk lookup over many keys
```cypher
UNWIND $keys AS k
CALL {
  WITH k
  MATCH (n:EntityA {primaryKey:   k}) RETURN n
  UNION
  WITH k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
RETURN k AS lookupKey, collect(n) AS results;
```

### Chunked `IN`-list lookup
```cypher
MATCH (n:EntityA)
WHERE n.primaryKey IN $keys
RETURN n;
```
Cap chunk size at 100–1000 keys per request. Beyond that, split client-side.

### Static small-set lookup
```cypher
MATCH (n:EntityA)
WHERE n.primaryKey IN ['k1', 'k2', 'k3']
RETURN n;
```

---

## 3. Queue, feed, & pagination

### Newest-first list view
```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status   = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```
Aligns with `(category, status, createdAt)` composite index.

### Keyset (seek) pagination — preferred for deep pages
```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category    = $category
  AND b.status      = $status
  AND b.createdAt   < $cursorCreatedAt
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT $pageSize;
```

### Offset pagination — only for shallow pages
```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
SKIP $offset
LIMIT $limit;
```
Skip cost grows linearly. Avoid past page ~10 of any user-facing feed.

### Optional filter — split into two templates, don't conditionalize
```cypher
-- Without prefix
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b, a ORDER BY b.createdAt DESC LIMIT 30;

-- With prefix
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status   = $status
  AND a.scope    STARTS WITH $scopePrefix
RETURN b, a ORDER BY b.createdAt DESC LIMIT 30;
```
Two stable templates beat one query with `($scopePrefix = '' OR a.scope STARTS WITH $scopePrefix)` — the conditional form forces a scan.

---

## 4. Search

### Vector similarity
```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH (node:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
RETURN node, b, score
ORDER BY score DESC
LIMIT $topK;
```

### Vector candidate pinning by `elementId()`
```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
WHERE elementId(node) = $id
RETURN node, score
LIMIT 1;
```

### Vector + bounded chain depth
```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH p = (node)-[:LINKS_TO*1..6]->(:EntityB)
WITH node, score, max(length(p)) AS maxDepth
RETURN elementId(node) AS nodeId, maxDepth, score
ORDER BY score DESC
LIMIT $topK;
```
Keep the `*1..6` upper bound a literal in query text; parameterizing it disables the hot path.

### Vector + branching path-cap aggregation
```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH p = (node)-[:LINKS_TO|BELONGS_TO*1..6]->(x)
WHERE ALL(n IN nodes(p) WHERE size(labels(n)) > 0)
WITH node, score, p, length(p) AS d
ORDER BY d ASC
WITH node, score, collect(p)[0..$pathCap] AS paths
RETURN elementId(node) AS nodeId, score, size(paths) AS pathCount
LIMIT $topK;
```

### Vector + frontier reachability
```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH (node)-[:LINKS_TO*1..6]->(x)
WITH node, score, length(shortestPath((node)-[:LINKS_TO*1..6]->(x))) AS d
WITH node, score, min(d) AS nearest, count(*) AS reachable
RETURN elementId(node) AS nodeId, score, nearest, reachable
LIMIT $topK;
```

### Vector + constrained traversal
```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH p = (node)-[:LINKS_TO*1..6]->(x)
WHERE any(r IN relationships(p) WHERE r.weight  >= $minWeight)
  AND any(n IN nodes(p)         WHERE n.category IN $categories)
RETURN elementId(node) AS nodeId, score, max(length(p)) AS maxDepth
LIMIT $topK;
```

### Full-text then graph expand
```cypher
CALL db.index.fulltext.queryNodes('idx_entitya_text', $query)
YIELD node, score
MATCH (node)-[:KEY_REL]->(b:EntityB)
RETURN node, b, score
ORDER BY score DESC
LIMIT $topK;
```

### Hybrid (vector + BM25 + RRF)
For combined ranking with optional rerank, use `db.retrieve` / `db.rretrieve` from the `nornicdb-rag-procedures` skill — don't roll your own merge.

---

## 5. Aggregates & dashboards

```cypher
-- Grouped count by status
MATCH (:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
RETURN b.status AS status, count(*) AS total
ORDER BY total DESC;

-- Rolling window
MATCH (e:Event)
WHERE e.createdAt >= $windowStart
RETURN count(*) AS eventsInWindow;

-- Tenant-scoped aggregate
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE a.tenantId = $tenantId
RETURN b.status, count(*) AS total;

-- Null-aware predicate (don't wrap with coalesce on the filtered column)
MATCH (b:EntityB)
WHERE b.status = 'open'
  AND (b.isReviewed = false OR b.isReviewed IS NULL)
RETURN b
LIMIT 100;
```

---

## 6. Relationship & traversal

### Idempotent relationship upsert
```cypher
MATCH (a:EntityA {primaryKey: $fromKey})
MATCH (b:EntityB {primaryKey: $toKey})
MERGE (a)-[r:LINKS_TO]->(b)
SET   r.updatedAt = $now;
```

### Relationship attach by ID
```cypher
MATCH (a:EntityA) WHERE elementId(a) = $fromId
MATCH (b:EntityB) WHERE elementId(b) = $toId
CREATE (a)-[:LINKS_TO {createdAt: $now}]->(b);
```

### Bounded traversal (always cap depth and result count)
```cypher
MATCH p = (start:EntityA {primaryKey: $startKey})-[:LINKS_TO*1..3]->(n)
RETURN p
LIMIT 100;
```

### Indexed start-node pruning
```cypher
MATCH (a:EntityA {primaryKey: $primaryKey})-[:LINKS_TO]->(b:EntityB)
RETURN a, b
LIMIT 100;
```
Filter the start node first so candidate pruning happens before expansion.

---

## 7. Write hot paths

### Single-statement upsert (autocommit)
```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
ON CREATE SET n.createdAt = $now
SET   n.updatedAt = $now, n.payload = $payload
RETURN n;
```

### Read-after-write in one transaction
```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
SET   n.updatedAt = $now
WITH n
MATCH (n)-[:KEY_REL]->(b:EntityB)
RETURN n, b
LIMIT 1;
```

### Bulk ingest with `UNWIND` (`UnwindSimpleMergeBatch`)
```cypher
UNWIND $rows AS row
MERGE (n:EntityA {primaryKey: row.primaryKey})
SET   n += row.props, n.updatedAt = $now;
```
Bound the batch (typically 100–2000 rows). Beyond that, split client-side.

### Distinct ON CREATE / ON MATCH sets
```cypher
UNWIND $rows AS row
MERGE (n:EntityA {primaryKey: row.primaryKey})
ON CREATE SET n.payload  = row.payload,
              n.createdAt = row.now,
              n.updatedAt = row.now,
              n.sessionId = row.sessionId
ON MATCH  SET n.payload   = row.payload,
              n.updatedAt = row.now
RETURN count(n) AS prepared;
```
Use whenever immutable fields must only be written on create.

### Fixed-depth chain materialization (`UnwindFixedChainLinkBatch`)
```cypher
-- By key
UNWIND $rows AS row
MATCH (root:EntityA {primaryKey: row.primaryKey})
MATCH (n1:EntityB   {primaryKey: row.primaryKey + ':1'})
MATCH (n2:EntityB   {primaryKey: row.primaryKey + ':2'})
MATCH (n3:EntityB   {primaryKey: row.primaryKey + ':3'})
MERGE (root)-[:LINKS_TO]->(n1)
MERGE (n1)-[:LINKS_TO]->(n2)
MERGE (n2)-[:LINKS_TO]->(n3)
RETURN count(root) AS prepared;

-- By root elementId
UNWIND $rows AS row
MATCH (root:EntityA) WHERE elementId(root) = row.rootId
MATCH (n1:EntityB {primaryKey: row.rootKey + ':1'})
MATCH (n2:EntityB {primaryKey: row.rootKey + ':2'})
MATCH (n3:EntityB {primaryKey: row.rootKey + ':3'})
MERGE (root)-[:LINKS_TO]->(n1)
MERGE (n1)-[:LINKS_TO]->(n2)
MERGE (n2)-[:LINKS_TO]->(n3)
RETURN count(root) AS prepared;
```

### Generalized batched merge chain (`UnwindMergeChainBatch`)
```cypher
UNWIND $rows AS row
MERGE (key:EntityA  {primaryKey: row.primaryKey, tenantId: row.tenantId})
MERGE (state:EntityB {primaryKey: row.stateKey})
SET   state.status   = row.status,
      state.updatedAt = row.updatedAt,
      state.category  = row.category
MERGE (key)-[:KEY_REL]->(state)
MERGE (event:Event {primaryKey: row.eventKey})
ON CREATE SET event.createdAt = row.updatedAt,
              event.tenantId  = row.tenantId
MERGE (event)-[:LINKS_TO]->(state)
MERGE (event)-[:BELONGS_TO]->(key);
```

### Lookup-then-upsert with `+= row.props`
```cypher
UNWIND $rows AS row
MATCH (parent:EntityA {primaryKey: row.parentKey})
MERGE (child:EntityB  {primaryKey: row.childKey})
SET   child += row.props
MERGE (parent)-[:KEY_REL]->(child)
RETURN count(child) AS prepared;
```

### Multi-MATCH + CREATE bulk seed (`UnwindMultiMatchCreateBatch`)
```cypher
UNWIND $rows AS row
MATCH (c:Category  {categoryID: row.categoryID})
MATCH (s:Supplier  {supplierID: row.supplierID})
CREATE (p:Product  {productID: row.productID, productName: row.productName, unitPrice: row.unitPrice})
CREATE (p)-[:PART_OF]->(c)
CREATE (s)-[:SUPPLIES]->(p);
```
Constraints for this hot path:
- All `MATCH` property values are `row.<field>` references.
- All `CREATE` property values are `row.<field>` or scalar literals.
- No `RETURN`, `WITH`, `SET`, `MERGE`, `DELETE`, `REMOVE`, `FOREACH`, `WHERE`, nested `UNWIND`, or inline relationship patterns inside `MATCH`.
- 100–2000 rows per batch.

Edge-only variant (when targets were already seeded):
```cypher
UNWIND $lineRows AS row
MATCH (o:Order   {orderID: row.orderID})
MATCH (p:Product {productID: row.productID})
CREATE (o)-[:ORDERS {quantity: row.quantity, discount: row.discount}]->(p);
```

### Optional-lookup write with coalesced target (`UnwindMergeChainBatch` w/ OPTIONAL MATCH)
```cypher
UNWIND $rows AS row
MERGE (event:Event   {primaryKey: row.eventKey})
SET   event.status    = row.status,
      event.updatedAt = row.updatedAt
MERGE (owner:EntityA {primaryKey: row.ownerKey})
ON CREATE SET owner.createdAt = row.updatedAt,
              owner.tenantId  = row.tenantId
MERGE (owner)-[:LINKS_TO]->(event)
WITH event, row
OPTIONAL MATCH (direct:EntityB   {primaryKey: row.targetKey})
WITH event, row, direct
OPTIONAL MATCH (fallback:EntityB {alternateKey: row.fallbackTargetKey, tenantId: row.tenantId})
WITH event, coalesce(direct, fallback) AS target
WHERE target IS NOT NULL
MERGE (event)-[:KEY_REL]->(target);
```

### Staged compound `UNWIND` pipeline (`CompoundQueryFastPath`)
```cypher
UNWIND $rows AS row
MERGE (key:EntityA  {primaryKey: row.primaryKey, tenantId: row.tenantId})
MERGE (state:EntityB {primaryKey: row.stateKey})
SET   state.status   = row.status,
      state.updatedAt = row.updatedAt,
      state.category  = row.category
MERGE (event:Event  {primaryKey: row.eventKey})
ON CREATE SET event.createdAt = row.updatedAt,
              event.tenantId  = row.tenantId
WITH $rows AS rows
UNWIND rows AS row
MATCH (key:EntityA  {primaryKey: row.primaryKey, tenantId: row.tenantId})
MATCH (state:EntityB {primaryKey: row.stateKey})
MATCH (event:Event  {primaryKey: row.eventKey})
MERGE (key)-[:KEY_REL]->(state)
MERGE (event)-[:LINKS_TO]->(state)
MERGE (event)-[:BELONGS_TO]->(key);
```
The executor splits at `WITH $rows AS ... UNWIND ... AS row` boundaries and routes each stage through batch hot paths. Don't rewrite this as multiple round-trips.

---

## 8. Cleanup, TTL, archival

```cypher
-- Safe batch cleanup (repeat until zero rows remain)
MATCH (n:EntityB)
WHERE n.sessionId = $sessionId
WITH n LIMIT 1000
DETACH DELETE n;

-- TTL cutoff (run on a schedule)
MATCH (e:Event)
WHERE e.createdAt < $cutoff
WITH e LIMIT 5000
DETACH DELETE e;

-- Archive then delete
MATCH (n:EntityB)
WHERE n.createdAt < $cutoff
WITH n LIMIT 1000
CREATE (:ArchiveRecord {sourceId: elementId(n), payload: n, archivedAt: $now})
DETACH DELETE n;

-- Indexed delete by key list
MATCH (n:EntityA)
WHERE n.primaryKey IN $keys
WITH n LIMIT 500
DETACH DELETE n;
```
Always cap a deletion batch with a `WITH n LIMIT k` window, then loop client-side. One unbounded `DETACH DELETE` is the easiest way to lock the database.

---

## 9. Multi-tenant isolation

```cypher
-- Tenant-first point lookup
MATCH (n:EntityA)
WHERE n.tenantId = $tenantId AND n.primaryKey = $primaryKey
RETURN n
LIMIT 1;

-- Tenant-scoped feed
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE a.tenantId = $tenantId
  AND b.category = $category
  AND b.status   = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```
Keep the tenant predicate explicit in every hot-path template. Don't push it into a global filter or `WITH` later.

---

## 10. Optional-match heavy reads

```cypher
-- Light optional expansion
MATCH (a:EntityA {primaryKey: $primaryKey})
OPTIONAL MATCH (a)-[:KEY_REL]->(b:EntityB)
RETURN a, collect(b) AS related;

-- Optional expansion with key-list seed
MATCH (a:EntityA)
WHERE a.primaryKey IN $keys
OPTIONAL MATCH (a)-[:KEY_REL]->(b:EntityB {category: $category})
RETURN a.primaryKey AS lookupKey, collect(b) AS related;

-- Optional expansion by elementId
MATCH (a:EntityA)
WHERE elementId(a) = $id
OPTIONAL MATCH (a)-[:KEY_REL]->(b:EntityB)
RETURN a, collect(b) AS related;
```
If optional branches grow large or sparse, split into focused queries and compose application-side.

---

## 11. Diagnostics & template stability

```cypher
-- PROFILE to confirm index acceptance
PROFILE
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b
ORDER BY b.createdAt DESC
LIMIT 30;

-- Inspect indexes/constraints
SHOW INDEXES;
SHOW CONSTRAINTS;
```
Reuse the same query text shape and pass only parameter changes — plan caching is per-text. Trivial cosmetic changes (whitespace, alias renames) defeat caching.

## Anti-patterns to avoid

1. Cross-property `OR` in one predicate. Split with `UNION`.
2. Function-wrapped filter columns (`coalesce(n.x, …)`, `toLower(n.name)`). Pre-normalize on write.
3. Conditional optional predicates (`$x = '' OR ...`). Use two templates.
4. Broad `CONTAINS` substring filters on large datasets. Use full-text or vector search.
5. Unbounded `*` traversals. Always cap depth and result count.
6. Single-transaction deletes/ingests of unbounded size. Window with `LIMIT` and loop.
7. Deep `SKIP/LIMIT` pagination on user-facing feeds. Use keyset pagination.
8. Parameterizing the upper bound of `*1..k` traversals — the literal is part of the hot-path key.

## Operational checklist

- Separate cold-run from warm-run measurements.
- Track p50, p95, p99 under realistic concurrency.
- Reuse stable query templates across calls.
- Keep related operations in one transaction when practical.
- After a latency spike: confirm indexes are `ONLINE`, re-run the same shape 3–5 times to compare cold vs warm, then split optional branches into focused templates.

## Hot-path inventory (quick reference)

The executor recognizes these specialized routes by name. If your query maps to one, you're on the fast path.

| Hot path | Shape it matches |
|---|---|
| `OuterIndexTopK` | `MATCH (n:Label) WHERE ... RETURN ... ORDER BY n.prop LIMIT k` |
| `SimpleMatchLimitFastPath` | `MATCH (n) RETURN n LIMIT k` |
| `TraversalStartSeedTopK` | start-seeded traversal with `ORDER BY ... LIMIT` |
| `TraversalEndSeedTopK` | reverse traversal bounded by result count |
| `UnwindSimpleMergeBatch` | single-node `UNWIND ... MERGE` upsert |
| `UnwindMergeChainBatch` | `UNWIND` batch with N node MERGEs, M relationship MERGEs, MATCH/OPTIONAL MATCH lookups |
| `UnwindFixedChainLinkBatch` | `UNWIND`-driven fixed-depth chain materialization |
| `UnwindMultiMatchCreateBatch` | bulk seed: many MATCHes + CREATEs, no RETURN/WITH/SET/MERGE |
| `CompoundQueryFastPath` | staged compound `UNWIND` pipeline split at `WITH $rows AS ... UNWIND` boundaries |
| `CallTailTraversalFastPath` | `CALL { ... }` tail traversal/path-count shape |
| `MergeSchemaLookupUsed` | index-backed `MERGE` lookup |
| `FabricBatchedApplyRows` | Fabric cross-shard batched APPLY rows |

For the full hot-path catalog and rationale, see [`docs/performance/hot-path-query-cookbook.md`](../performance/hot-path-query-cookbook.md).

## Per-database search index master switches

Search-related Cypher (`db.index.vector.queryNodes`, `db.index.fulltext.queryNodes`, hybrid search via `/nornicdb/search`) can be disabled or lazy-warmed per database. Behaviour callers must handle:

- Vector disabled: `db.index.vector.queryNodes` returns **zero rows** with a WARN log. The query does not error. Cypher pipelines that gracefully handle empty vector results continue to succeed.
- Both disabled: `POST /nornicdb/search` returns 503 `search_disabled_for_database`, `retryable: false` — clients must NOT retry.
- `warming=lazy`: first inbound search blocks synchronously while the build runs (any entry point, not just HTTP). Concurrent first-readers wait on the same build. Subsequent queries are warm.

For reachability/liveness probing of a database, do NOT issue `CALL db.index.vector.queryNodes` or `POST /nornicdb/search` against a `lazy` or `*_enabled=false` database — use `GET /nornicdb/health` or `GET /admin/databases/{name}/config` instead.

See [docs/operations/configuration.md#per-database-search-index-control](../operations/configuration.md#per-database-search-index-control).

## See also
- [`nornicdb-vector-search`](vector-search.skill.md) — vector and full-text index DDL and queries.
- [`nornicdb-rag-procedures`](rag-procedures.skill.md) — `db.retrieve`, `db.rerank`, `db.infer` for end-to-end RAG.
- [`nornicdb-knowledge-policies`](knowledge-policies.skill.md) — decay/promotion DDL and how scoring composes with the filters above.
