# Hot Path Query Cookbook

A practical, domain-neutral guide for fast, predictable graph query latency in production systems.

Use this cookbook as a menu of proven query shapes. Pick the shape that matches your endpoint behavior, adapt labels/properties, and measure p50/p95/p99.

Hot-path additions since `v1.0.36` include bounded `UNWIND ... MERGE` batch upserts, fixed-depth chain materialization, vector-result `elementId()` pinning, and vector-seeded traversal aggregations for chain, branching, frontier, and constrained traversal shapes.

## Hot Path Eligibility Inventory

These are the query shapes currently recognized as specialized hot paths or optimized executor routes.

Traced executor hot paths:

- `OuterIndexTopK`: `MATCH (n:Label) WHERE ... RETURN ... ORDER BY n.prop LIMIT k`
- `OuterScanFallbackUsed`: same outer read shape when index pruning is unavailable or rejected
- `FabricBatchedApplyRows`: Fabric cross-shard batched APPLY row lookups
- `SimpleMatchLimitFastPath`: `MATCH (n) RETURN n LIMIT k`
- `CompoundQueryFastPath`: staged compound `UNWIND` mutation pipeline that splits repeated `WITH $rows AS ... UNWIND ... AS row` segments into hot-path-eligible batch stages
- `TraversalStartSeedTopK`: start-node-seeded traversal with `ORDER BY ... LIMIT`
- `TraversalEndSeedTopK`: end-node-seeded reverse traversal with bounded result count
- `UnwindSimpleMergeBatch`: single-node `UNWIND ... MERGE` upsert with optional `SET` and optional `RETURN count(...)`
- `UnwindMergeChainBatch`: generalized `UNWIND` batch mutation pipeline with N-ary node `MERGE`s, relationship `MERGE`s, `MATCH` and `OPTIONAL MATCH` lookups on row-bound expressions, non-aggregating `WITH` aliases, and `WHERE` filters such as `IS NOT NULL`
- `UnwindFixedChainLinkBatch`: `UNWIND`-driven fixed-depth chain materialization by key or root `elementId()`
- `UnwindMultiMatchCreateBatch`: `UNWIND $rows AS row` driving N independent `MATCH (v:Label {k: row.f})` lookups and M `CREATE` clauses (node + edge) with no `RETURN`/`WITH`/nested `UNWIND`. Parses the mutation once, then for each row resolves MATCHes via property index and writes created nodes/edges directly against storage. Bulk-seed shape used by fixtures and import pipelines.
- `CallTailTraversalFastPath`: `CALL { ... }` tail traversal/path-count shapes
- `MergeSchemaLookupUsed`: index-backed `MERGE` lookup on property or composite schema keys
- `MergeScanFallbackUsed`: label-scan `MERGE` lookup when no suitable schema path exists

Composite pipeline executor:

- `executePipeline` (pkg/cypher/pipeline_executor.go) walks a query as an ordered sequence of `MATCH`/`CREATE`/`WITH`/`UNWIND`/`RETURN` clauses, threading a binding context between them. It handles composite shapes that the single-clause handlers mis-parse — most notably `MATCH ... CREATE ... WITH ... UNWIND <list> AS x MATCH ... CREATE ...` where a nested `UNWIND` follows a `WITH` that carries a row forward. `$param` references are substituted from context before clause splitting. The pipeline refuses (falls back) on `MERGE`, `OPTIONAL MATCH`, `SET`, `REMOVE`, `DELETE`, `FOREACH`, and `CALL` clauses — those stay on their dedicated handlers.

Optimized executor patterns:

- `PatternMutualRelationship`: `MATCH (a)-[:T]->(b)-[:T]->(a) RETURN ...`
- `PatternIncomingCountAgg`: `MATCH (x)<-[:T]-(y) RETURN x.name, count(y)`
- `PatternOutgoingCountAgg`: `MATCH (x)-[:T]->(y) RETURN x.name, count(y)`
- `PatternEdgePropertyAgg`: grouped edge-property aggregates such as `avg(r.prop), count(r)`
- `PatternLargeResultSet`: large `MATCH ... LIMIT > 100` result sets routed to batched lookup execution

Not every fast query in this cookbook maps 1:1 to a single trace flag, but every traced or optimized hot path currently exposed by the executor is represented here.

## Generic Model Used In Examples

- Node labels: `EntityA`, `EntityB`, `Tenant`, `Event`
- Relationship types: `KEY_REL`, `BELONGS_TO`, `LINKS_TO`
- Properties: `primaryKey`, `alternateKey`, `tenantId`, `category`, `status`, `createdAt`, `updatedAt`, `sessionId`

Replace names with your schema names.

## Baseline Index And Constraint Patterns

```cypher
CREATE CONSTRAINT entitya_primarykey_uniq IF NOT EXISTS FOR (n:EntityA) REQUIRE n.primaryKey IS UNIQUE;
CREATE INDEX entitya_alternatekey_idx IF NOT EXISTS FOR (n:EntityA) ON (n.alternateKey);
CREATE INDEX entitya_tenantid_idx IF NOT EXISTS FOR (n:EntityA) ON (n.tenantId);
CREATE INDEX entityb_category_status_createdat_idx IF NOT EXISTS FOR (n:EntityB) ON (n.category, n.status, n.createdAt);
CREATE INDEX entityb_tenantid_idx IF NOT EXISTS FOR (n:EntityB) ON (n.tenantId);
CREATE INDEX event_createdat_idx IF NOT EXISTS FOR (e:Event) ON (e.createdAt);
```

Test-only cleanup support:

```cypher
CREATE INDEX entitya_sessionid_idx IF NOT EXISTS FOR (n:EntityA) ON (n.sessionId);
CREATE INDEX entityb_sessionid_idx IF NOT EXISTS FOR (n:EntityB) ON (n.sessionId);
```

## Area 1: Point Lookup And Existence

### 1.1 Upsert And Read By Stable Key

```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
SET n.payload = $payload, n.updatedAt = $now;

MATCH (n:EntityA {primaryKey: $primaryKey})
RETURN n
LIMIT 1;
```

### 1.2 Lookup By Either Of Two Keys

```cypher
CALL {
  WITH $lookupKey AS k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH $lookupKey AS k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
RETURN n
LIMIT 1;
```

### 1.3 Fast Existence Check

```cypher
MATCH (n:EntityA {primaryKey: $primaryKey})
RETURN 1 AS found
LIMIT 1;
```

Use when you only need yes/no, not full payload.

### 1.4 Direct ID Seek Shape

```cypher
MATCH (n:EntityA)
WHERE elementId(n) = $id
RETURN n
LIMIT 1;
```

Keep this as a dedicated template so planners can reliably choose direct ID seek.

## Area 2: Batch Retrieval

### 2.1 Bulk Lookup For Many Keys

```cypher
UNWIND $keys AS k
CALL {
  WITH k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
RETURN k AS lookupKey, collect(n) AS results;
```

### 2.2 Chunked IN-List Lookup

```cypher
MATCH (n:EntityA)
WHERE n.primaryKey IN $keys
RETURN n;
```

Use with bounded chunk size (for example 100-1000 keys per call).

### 2.2b Literal IN-List Lookup (Small, Fixed Sets)

```cypher
MATCH (n:EntityA)
WHERE n.primaryKey IN ['k1', 'k2', 'k3']
RETURN n;
```

Use for static, small key sets embedded directly in query text.

### 2.3 OR-To-UNION Dual-Key Lookup

```cypher
CALL {
  WITH $lookupKey AS k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH $lookupKey AS k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
WITH DISTINCT n
RETURN n;
```

Prefer this over a single `OR` predicate across different properties.

## Area 3: Queue, Feed, And Pagination

### 3.1 Queue/List View (Newest First)

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

### 3.2 Optional Filter As Separate Template

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
  AND a.scope STARTS WITH $scopePrefix
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

### 3.3 Keyset (Seek) Pagination

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
  AND b.createdAt < $cursorCreatedAt
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT $pageSize;
```

Preferred for deep pagination.

### 3.4 Offset Pagination (Small Pages Only)

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
SKIP $offset
LIMIT $limit;
```

Use only for shallow pages.

### 3.5 Top-N With Composite Index-Friendly Shape

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Keep the sort key and filtered fields aligned with a composite index when possible.

### 3.6 Optional Predicate Split (Two Templates)

Template A (no prefix filter):

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Template B (with prefix filter):

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
  AND b.status = $status
  AND a.scope STARTS WITH $scopePrefix
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Prefer two templates over `($scopePrefix = '' OR a.scope STARTS WITH $scopePrefix)`.

## Area 4: Search Patterns

### 4.1 Vector Similarity Search

```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH (node:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
RETURN node, b, score
ORDER BY score DESC
LIMIT $topK;
```

### 4.1b Vector Candidate Pinning By `elementId()`

```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
WHERE elementId(node) = $id
RETURN node, score
LIMIT 1;
```

Use when the caller already has a root ID and wants vector scoring plus exact candidate verification without scanning unrelated matches.

### 4.1c Vector Search Plus Bounded Chain Depth Aggregation

```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH p = (node)-[:LINKS_TO*1..6]->(:EntityB)
WITH node, score, max(length(p)) AS maxDepth
RETURN elementId(node) AS nodeId, maxDepth, score
ORDER BY score DESC
LIMIT $topK;
```

Use when you need the deepest reachable bounded chain per vector candidate. Keep the upper bound literal in query text.

### 4.1d Vector Search Plus Branching Path-Cap Aggregation

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

Use for Falkor-style branching expansions where you need bounded per-seed path counts. Cap collected paths explicitly.

### 4.1e Vector Search Plus Frontier Reachability Summary

```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH (node)-[:LINKS_TO*1..6]->(x)
WITH node, score, length(shortestPath((node)-[:LINKS_TO*1..6]->(x))) AS d
WITH node, score, min(d) AS nearest, count(*) AS reachable
RETURN elementId(node) AS nodeId, score, nearest, reachable
LIMIT $topK;
```

Use for BFS-like frontier reads where you need nearest-distance plus bounded reachable counts from each seeded node.

### 4.1f Vector Search Plus Constrained Traversal Depth

```cypher
CALL db.index.vector.queryNodes('idx_entitya_embedding', $topK, $vector)
YIELD node, score
MATCH p = (node)-[:LINKS_TO*1..6]->(x)
WHERE any(r IN relationships(p) WHERE r.weight >= $minWeight)
  AND any(n IN nodes(p) WHERE n.category IN $categories)
RETURN elementId(node) AS nodeId, score, max(length(p)) AS maxDepth
LIMIT $topK;
```

Use when traversal depth must respect relationship and node predicates. Keep predicate lists and thresholds parameterized; keep traversal bounds literal and tight.

Traversal matrix coverage for the current hot path:

- Chain depth aggregation: depth sweep `1..6`
- Branching path-cap aggregation: fanout sweep `1..3`, depth sweep `1..6`, bounded by `$pathCap`
- Frontier reachability summary: fanout sweep `1..3`, depth sweep `1..6`
- Constrained traversal depth: validated at fanout `2`, depth sweep `1..6`, with weight/category predicates

These dimensions mirror the deterministic e2e workload matrix used to verify Bolt and HTTP routing for Falkor-style traversal reads.

### 4.2 Full-Text Then Graph Expand

```cypher
CALL db.index.fulltext.queryNodes('idx_entitya_text', $query)
YIELD node, score
MATCH (node)-[:KEY_REL]->(b:EntityB)
RETURN node, b, score
ORDER BY score DESC
LIMIT $topK;
```

### 4.3 Hybrid Candidate Merge (Application Side)

Run vector and full-text candidate queries separately, merge and rerank in application code for stable control.

## Area 5: Aggregates And Dashboard Reads

### 5.1 Grouped Count By Status

```cypher
MATCH (:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category
RETURN b.status AS status, count(*) AS total
ORDER BY total DESC;
```

### 5.2 Rolling Window Counts

```cypher
MATCH (e:Event)
WHERE e.createdAt >= $windowStart
RETURN count(*) AS eventsInWindow;
```

### 5.3 Tenant-Scoped Aggregate

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE a.tenantId = $tenantId
RETURN b.status, count(*) AS total;
```

### 5.4 Null-Normalized Predicate Shape

```cypher
MATCH (b:EntityB)
WHERE b.status = 'open'
  AND (b.isReviewed = false OR b.isReviewed IS NULL)
RETURN b
LIMIT 100;
```

Prefer explicit null-aware predicates instead of `coalesce(...)` wrappers on filtered columns.

## Area 6: Relationship And Traversal

### 6.1 Idempotent Relationship Upsert

```cypher
MATCH (a:EntityA {primaryKey: $fromKey})
MATCH (b:EntityB {primaryKey: $toKey})
MERGE (a)-[r:LINKS_TO]->(b)
SET r.updatedAt = $now;
```

NornicDB maintains direct relationship-existence indexes for this shape. The
typed head key maps `(start node, end node, relationship type)` to one edge ID
for the common single-edge lookup, while the set key keeps
`(start node, end node, relationship type, edge ID)` entries for
`GetEdgesBetween` and valid multiple same-type edges. Reads fall back to the
legacy outgoing-edge scan and self-heal the new keys when opening mixed-version
stores, so correctness does not depend solely on the one-time backfill marker.

### 6.2 Relationship Attach By ID

```cypher
MATCH (a:EntityA) WHERE elementId(a) = $fromId
MATCH (b:EntityB) WHERE elementId(b) = $toId
CREATE (a)-[:LINKS_TO {createdAt: $now}]->(b);
```

Use stable ID lookups before relationship creation for predictable attach latency.

### 6.3 Bounded Traversal

```cypher
MATCH p = (start:EntityA {primaryKey: $startKey})-[:LINKS_TO*1..3]->(n)
RETURN p
LIMIT 100;
```

Always bound traversal depth and result count.

### 6.4 Traversal With Early Filters

```cypher
MATCH p = (start:EntityA {primaryKey: $startKey})-[:LINKS_TO*1..3]->(n:EntityB)
WHERE n.status = $status
RETURN p
LIMIT 100;
```

### 6.5 Relationship Match With Indexed Start-Node Pruning

```cypher
MATCH (a:EntityA {primaryKey: $primaryKey})-[:LINKS_TO]->(b:EntityB)
RETURN a, b
LIMIT 100;
```

Prefer exact/prefix-start predicates on the traversal start node so candidate pruning happens before expansion.

## Area 7: Write Hot Paths

### 7.1 Targeted Bulk Update

```cypher
CALL {
  WITH $lookupKey AS k
  MATCH (n:EntityA {primaryKey: k}) RETURN n
  UNION
  WITH $lookupKey AS k
  MATCH (n:EntityA {alternateKey: k}) RETURN n
}
MATCH (n)-[:KEY_REL]->(b:EntityB {category: $category})
WHERE b.status = 'pending'
SET b.status = 'queued', b.updatedAt = $now;
```

### 7.2 Read-After-Write In One Transaction

```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
SET n.updatedAt = $now
WITH n
MATCH (n)-[:KEY_REL]->(b:EntityB)
RETURN n, b
LIMIT 1;
```

Use when the API needs immediate confirmed view of updated data.

### 7.3 Bulk Ingestion With UNWIND

```cypher
UNWIND $rows AS row
MERGE (n:EntityA {primaryKey: row.primaryKey})
SET n += row.props, n.updatedAt = $now;
```

Use bounded batch sizes to avoid oversized transactions.

### 7.3b Batched Upsert With Distinct `ON CREATE` And `ON MATCH` Sets

```cypher
UNWIND $rows AS row
MERGE (n:EntityA {primaryKey: row.primaryKey})
ON CREATE SET n.payload = row.payload,
              n.createdAt = row.now,
              n.updatedAt = row.now,
              n.sessionId = row.sessionId
ON MATCH SET  n.payload = row.payload,
              n.updatedAt = row.now
RETURN count(n) AS prepared;
```

Use for idempotent bounded upsert batches where immutable fields must only be written on create.

### 7.3c Batched Fixed-Depth Chain Materialization By Key

```cypher
UNWIND $rows AS row
MATCH (root:EntityA {primaryKey: row.primaryKey})
MATCH (n1:EntityB {primaryKey: row.primaryKey + ':1'})
MATCH (n2:EntityB {primaryKey: row.primaryKey + ':2'})
MATCH (n3:EntityB {primaryKey: row.primaryKey + ':3'})
MERGE (root)-[:LINKS_TO]->(n1)
MERGE (n1)-[:LINKS_TO]->(n2)
MERGE (n2)-[:LINKS_TO]->(n3)
RETURN count(root) AS prepared;
```

Use when the application materializes bounded chains in one batch and the chain depth is fixed in query text. Re-running stays idempotent through `MERGE`.

### 7.3d Batched Fixed-Depth Chain Materialization By Root ID

```cypher
UNWIND $rows AS row
MATCH (root:EntityA)
WHERE elementId(root) = row.rootId
MATCH (n1:EntityB {primaryKey: row.rootKey + ':1'})
MATCH (n2:EntityB {primaryKey: row.rootKey + ':2'})
MATCH (n3:EntityB {primaryKey: row.rootKey + ':3'})
MERGE (root)-[:LINKS_TO]->(n1)
MERGE (n1)-[:LINKS_TO]->(n2)
MERGE (n2)-[:LINKS_TO]->(n3)
RETURN count(root) AS prepared;
```

Use this variant when the caller already has stable `elementId()` values for roots and only needs a lightweight key suffix to resolve chain members.

### 7.3e Generalized Batched Merge Chain

```cypher
UNWIND $rows AS row
MERGE (key:EntityA {primaryKey: row.primaryKey, tenantId: row.tenantId})
MERGE (state:EntityB {primaryKey: row.stateKey})
SET state.status = row.status,
    state.updatedAt = row.updatedAt,
    state.category = row.category
MERGE (key)-[:KEY_REL]->(state)
MERGE (event:Event {primaryKey: row.eventKey})
ON CREATE SET event.createdAt = row.updatedAt,
              event.tenantId = row.tenantId
MERGE (event)-[:LINKS_TO]->(state)
MERGE (event)-[:BELONGS_TO]->(key)
```

Use this for qVersions-style write families where one input row materializes multiple node upserts and multiple idempotent relationship upserts in a deterministic order.

### 7.3e.1 Lookup-Then-Upsert With Map-Merge Properties

```cypher
UNWIND $rows AS row
MATCH (parent:EntityA {primaryKey: row.parentKey})
MERGE (child:EntityB {primaryKey: row.childKey})
SET child += row.props
MERGE (parent)-[:KEY_REL]->(child)
RETURN count(child) AS prepared;
```

Use this when each row first resolves an existing parent, upserts a child by a
schema-backed key, merges a row-provided property map, and links the two
idempotently. This shape is optimized by `UnwindMergeChainBatch`; it avoids the
generic per-row fallback for `UNWIND ... MATCH ... MERGE ... SET += row.props
... MERGE relationship` ingestion pipelines.

When the batch creates the relationship target and immediately links an existing
parent to it, `UnwindMergeChainBatch` skips committed relationship existence
probes for that batch-created endpoint and uses a batch-local relationship cache
for duplicate rows. That keeps duplicate input idempotent while avoiding
repeated scans across an already-large source-node fanout for relationships that
could not have existed before the target node was created.

### 7.3e.2 Staged Compound `UNWIND` Version Pipeline

```cypher
UNWIND $rows AS row
MERGE (key:EntityA {primaryKey: row.primaryKey, tenantId: row.tenantId})
MERGE (state:EntityB {primaryKey: row.stateKey})
SET state.status = row.status,
    state.updatedAt = row.updatedAt,
    state.category = row.category
MERGE (event:Event {primaryKey: row.eventKey})
ON CREATE SET event.createdAt = row.updatedAt,
              event.tenantId = row.tenantId
WITH $rows AS rows
UNWIND rows AS row
MATCH (key:EntityA {primaryKey: row.primaryKey, tenantId: row.tenantId})
MATCH (state:EntityB {primaryKey: row.stateKey})
MATCH (event:Event {primaryKey: row.eventKey})
MERGE (key)-[:KEY_REL]->(state)
MERGE (event)-[:LINKS_TO]->(state)
MERGE (event)-[:BELONGS_TO]->(key)
```

Use this when the caller emits a single standard Cypher statement with multiple repeated `UNWIND $rows AS row` stages. The executor now splits the statement at `WITH $rows AS ... UNWIND ... AS row` boundaries and routes each stage through the batch mutation hot path instead of falling back to generic per-row execution.

### 7.3f Batched Optional Lookup Chain With Coalesced Target Resolution

```cypher
UNWIND $rows AS row
MERGE (event:Event {primaryKey: row.eventKey})
SET event.status = row.status,
    event.updatedAt = row.updatedAt
MERGE (owner:EntityA {primaryKey: row.ownerKey})
ON CREATE SET owner.createdAt = row.updatedAt,
              owner.tenantId = row.tenantId
MERGE (owner)-[:LINKS_TO]->(event)
WITH event, row
OPTIONAL MATCH (direct:EntityB {primaryKey: row.targetKey})
WITH event, row, direct
OPTIONAL MATCH (fallback:EntityB {alternateKey: row.fallbackTargetKey, tenantId: row.tenantId})
WITH event, coalesce(direct, fallback) AS target
WHERE target IS NOT NULL
MERGE (event)-[:KEY_REL]->(target)
```

Use this for qEvents-style write families where each row always upserts source entities but only conditionally attaches the terminal relationship after `OPTIONAL MATCH` and `coalesce(...)` target resolution.

### 7.3f.1 Staged Compound `UNWIND` Optional-Lookup Pipeline

```cypher
UNWIND $rows AS row
MERGE (event:Event {primaryKey: row.eventKey})
SET event.status = row.status,
    event.updatedAt = row.updatedAt
MERGE (owner:EntityA {primaryKey: row.ownerKey})
ON CREATE SET owner.createdAt = row.updatedAt,
              owner.tenantId = row.tenantId
WITH $rows AS rows
UNWIND rows AS row
MATCH (event:Event {primaryKey: row.eventKey})
MATCH (owner:EntityA {primaryKey: row.ownerKey})
MERGE (owner)-[:LINKS_TO]->(event)
WITH event, row
OPTIONAL MATCH (direct:EntityB {primaryKey: row.targetKey})
OPTIONAL MATCH (fallback:EntityB {alternateKey: row.fallbackTargetKey, tenantId: row.tenantId})
WITH event, coalesce(direct, fallback) AS target
WHERE target IS NOT NULL
MERGE (event)-[:KEY_REL]->(target)
```

Use this when a monolithic repeated-`UNWIND` statement first upserts source entities and then performs row-bound lookup/link work. Consecutive `OPTIONAL MATCH` clauses are supported as long as the stage remains non-aggregating and ends in idempotent `MERGE` relationship creation.

### 7.3g Single-Row Fallback Version Shape

```cypher
MERGE (key:EntityA {primaryKey: $primaryKey, tenantId: $tenantId})
MERGE (state:EntityB {primaryKey: $stateKey})
SET state.status = $status,
    state.updatedAt = $updatedAt,
    state.category = $category
MERGE (event:Event {primaryKey: $eventKey})
ON CREATE SET event.createdAt = $updatedAt,
              event.tenantId = $tenantId
WITH $primaryKey AS primaryKey, $tenantId AS tenantId, $stateKey AS stateKey, $eventKey AS eventKey
MATCH (key:EntityA {primaryKey: primaryKey, tenantId: tenantId})
MATCH (state:EntityB {primaryKey: stateKey})
MATCH (event:Event {primaryKey: eventKey})
MERGE (key)-[:KEY_REL]->(state)
MERGE (event)-[:LINKS_TO]->(state)
MERGE (event)-[:BELONGS_TO]->(key)
```

This is the exact single-row fallback form commonly used when a batched version pipeline times out or is retried one row at a time. It is semantically covered by deterministic E2E tests and should remain behaviorally equivalent to the batched version family, but it is not itself a batch hot path.

### 7.3h Bulk-Seed: Multi-MATCH Lookups Plus CREATE Writes (No RETURN)

```cypher
UNWIND $rows AS row
MATCH (c:Category {categoryID: row.categoryID})
MATCH (s:Supplier {supplierID: row.supplierID})
CREATE (p:Product {productID: row.productID, productName: row.productName, unitPrice: row.unitPrice})
CREATE (p)-[:PART_OF]->(c)
CREATE (s)-[:SUPPLIES]->(p);
```

Use this for bulk seeder / fixture / importer shapes that need to attach newly-created nodes to multiple already-existing entities found via property lookup. Matches the `UnwindMultiMatchCreateBatch` hot path: the mutation body is parsed once, then for each row the executor resolves every MATCH via property index (when available) and writes the CREATE nodes and edges directly against storage, bypassing the per-row Cypher re-parse used by the generic fallback. Requires all MATCH property values to reference `row.<field>`; all CREATE property values to be either `row.<field>` references or scalar literals; no `RETURN`, `WITH`, `SET`, `MERGE`, `DELETE`, `REMOVE`, `FOREACH`, `WHERE`, nested `UNWIND`, or inline relationship patterns inside `MATCH`.

A typical two-pass edge-only variant — used when the targets were already seeded in an earlier pass — also falls on this hot path:

```cypher
UNWIND $lineRows AS row
MATCH (o:Order {orderID: row.orderID})
MATCH (p:Product {productID: row.productID})
CREATE (o)-[:ORDERS {quantity: row.quantity, discount: row.discount}]->(p);
```

Match the row map keys exactly to the field names used in `row.<field>` references. Scale batch size to roughly 100–2000 rows per request for balanced throughput/latency.

### 7.4 Single-Statement Autocommit Shape

```cypher
MERGE (n:EntityA {primaryKey: $primaryKey})
ON CREATE SET n.createdAt = $now
SET n.updatedAt = $now, n.payload = $payload
RETURN n;
```

Favor single-statement request shapes on hot write/read paths.

## Area 8: Cleanup, TTL, And Archival

### 8.1 Safe Batch Cleanup For Test Data

```cypher
MATCH (n:EntityB)
WHERE n.sessionId = $sessionId
WITH n LIMIT 1000
DETACH DELETE n;
```

Repeat until zero rows remain.

### 8.2 TTL-Like Time-Bucket Cleanup

```cypher
MATCH (e:Event)
WHERE e.createdAt < $cutoff
WITH e LIMIT 5000
DETACH DELETE e;
```

Run on a schedule.

### 8.3 Archive Then Delete Pattern

```cypher
MATCH (n:EntityB)
WHERE n.createdAt < $cutoff
WITH n LIMIT 1000
CREATE (:ArchiveRecord {sourceId: elementId(n), payload: n, archivedAt: $now})
DETACH DELETE n;
```

Use when retention policy requires recoverable archives.

### 8.4 Streaming Batch Delete Loop

```cypher
MATCH (n:EntityB)
WHERE n.sessionId = $sessionId
WITH n LIMIT 500
DETACH DELETE n;
```

Repeat in application/job scheduler until zero rows are affected.

### 8.5 Indexed Delete Batches By Key Lists

```cypher
MATCH (n:EntityA)
WHERE n.primaryKey IN $keys
WITH n LIMIT 500
DETACH DELETE n;
```

Prefer indexed key predicates in batched delete loops to avoid broad label scans.

## Area 9: Multi-Tenant Query Isolation

### 9.1 Tenant-First Point Lookup

```cypher
MATCH (n:EntityA)
WHERE n.tenantId = $tenantId AND n.primaryKey = $primaryKey
RETURN n
LIMIT 1;
```

### 9.2 Tenant-Scoped List

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE a.tenantId = $tenantId
  AND b.category = $category
  AND b.status = $status
RETURN b, a
ORDER BY b.createdAt DESC
LIMIT 30;
```

Keep tenant predicates explicit in all hot-path templates.

## Area 10: Optional-Match Heavy Workloads

### 10.1 Optional Data Expansion (Light)

```cypher
MATCH (a:EntityA {primaryKey: $primaryKey})
OPTIONAL MATCH (a)-[:KEY_REL]->(b:EntityB)
RETURN a, collect(b) AS related;
```

### 10.1b Optional Expansion With Key-List Seed

```cypher
MATCH (a:EntityA)
WHERE a.primaryKey IN $keys
OPTIONAL MATCH (a)-[:KEY_REL]->(b:EntityB {category: $category})
RETURN a.primaryKey AS lookupKey, collect(b) AS related;
```

Seed OPTIONAL MATCH with indexed candidate sets first (`IN`, exact key, or ID) to keep expansion bounded.

### 10.1c Optional Expansion By Direct Node ID

```cypher
MATCH (a:EntityA)
WHERE elementId(a) = $id
OPTIONAL MATCH (a)-[:KEY_REL]->(b:EntityB)
RETURN a, collect(b) AS related;
```

Use when caller already has element IDs and needs deterministic low-latency expansion.

### 10.2 Split Heavy Optional Shapes

If optional branches become large and sparse, split into multiple focused queries and compose in the application layer.

## Area 11: Plan Reuse And Diagnostics

### 11.1 Stable Parameterized Template Reuse

```cypher
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b
ORDER BY b.createdAt DESC
LIMIT $limit;
```

Keep the query text shape stable and pass only parameter changes across calls.

### 11.2 PROFILE Index Acceptance/Rejection Checks

```cypher
PROFILE
MATCH (a:EntityA)-[:KEY_REL]->(b:EntityB)
WHERE b.category = $category AND b.status = $status
RETURN b
ORDER BY b.createdAt DESC
LIMIT 30;
```

Use profiling to confirm index-backed seeks/sorts and to identify rejection causes such as function wrapping or predicate shape.

## Common Anti-Patterns To Avoid

1. Cross-property `OR` in one predicate.
2. Function-wrapped filter columns in `WHERE` (`coalesce`, `toLower`, etc.).
3. Optional branch predicates inside one template (`$x = '' OR ...`).
4. Broad `CONTAINS` substring filters on large datasets.
5. Unbounded traversals.
6. Large single-transaction delete/ingest operations.
7. Deep `SKIP/LIMIT` pagination for user-facing feeds.

## Operational Checklist

1. Separate cold-run and warm-run measurements.
2. Track p50, p95, and p99 under realistic concurrency.
3. Reuse stable query templates.
4. Keep related operations in one transaction when practical.
5. Validate indexes and constraints regularly:

```cypher
SHOW INDEXES;
SHOW CONSTRAINTS;
```

If latency spikes:

1. Verify required indexes are `ONLINE`.
2. Re-run identical shape 3-5 times to compare first-hit vs steady state.
3. Split optional-filter and optional-match paths into separate templates.
