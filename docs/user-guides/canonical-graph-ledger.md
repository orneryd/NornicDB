# Canonical Graph Ledger Patterns

## Start with ownership of an epoch

An account owns an asset for a span of time. Put that span on the relationship:

```text
(Account) ──OWNS {starts_at, ends_at}──> (Asset)
```

Each `OWNS` relationship records one ownership epoch. Keep old relationships when
ownership changes. The graph then answers who owns the asset now and who owned it
before, without separate fact-key nodes or current pointers.

### Prevent conflicting ownership

```cypher
CREATE CONSTRAINT account_id IF NOT EXISTS
FOR (a:Account) REQUIRE a.id IS NODE KEY;

CREATE CONSTRAINT asset_id IF NOT EXISTS
FOR (a:Asset) REQUIRE a.id IS NODE KEY;

CREATE CONSTRAINT asset_ownership IF NOT EXISTS
FOR ()-[r:OWNS]-()
REQUIRE temporal.epoch(r.starts_at, r.ends_at)
GROUP BY r.assetId;
```

`temporal.epoch` reserves the half-open span `[starts_at, ends_at)`. A null end
means the epoch is still open. `GROUP BY r.assetId` makes ownership exclusive per
asset: overlapping epochs for the same asset are rejected, even for different
accounts. Different assets may be owned at the same time. Adjacent epochs may
share a boundary.

The constraint groups by relationship properties, not endpoints. Each write must
set `r.assetId` to the destination asset's ID. Grouping by account instead would
enforce a different rule: one asset per account at a time.

### Record the first owner

Run this after declaring the constraints, in an empty example database:

```cypher
CREATE (alice:Account {id:'alice'}), (bob:Account {id:'bob'}),
       (asset:Asset {id:'asset-42'})
CREATE (alice)-[:OWNS {
  assetId:asset.id,
  starts_at:datetime('2026-01-01T00:00:00Z'), ends_at:null
}]->(asset);
```

Alice owns `asset-42` from January 1 onward. Bob exists but has no ownership epoch.
The identifiers and property names are application choices, not reserved names.

### Transfer ownership

```cypher
MATCH (:Account {id:'alice'})-[old:OWNS]->(asset:Asset {id:'asset-42'}),
      (next:Account {id:'bob'})
WHERE old.ends_at IS NULL
WITH old, asset, next, datetime('2026-02-01T00:00:00Z') AS boundary
SET old.ends_at=boundary
CREATE (next)-[:OWNS {
  assetId:asset.id, starts_at:boundary, ends_at:null
}]->(asset);
```

Submit this as one statement: closing Alice's epoch and opening Bob's commit
together. Both use the same boundary, so there is no gap or overlap. Alice's
relationship remains in the graph. If the expected open ownership or next account
is missing, `MATCH` returns no rows and nothing changes; the application must
check that the transfer occurred. Handle concurrent transaction conflicts through
the normal retry contract.

This preserves ownership history, but closing an epoch changes its end property.
It is not an immutable event log. Use explicit change events when that is needed.

### Find the owner at a time

```cypher
MATCH (owner:Account)-[r:OWNS]->(:Asset {id:'asset-42'})
WHERE r.starts_at <= datetime('2026-01-15T00:00:00Z')
  AND (r.ends_at IS NULL OR datetime('2026-01-15T00:00:00Z') < r.ends_at)
RETURN owner.id AS owner;
```

After the transfer, this returns `alice`. February 1 returns `bob`. These queries
read the explicit epochs in the current graph; they do not require MVCC history.
Deleting an ownership relationship deletes that application history.

### Keep database history separate

```cypher
CALL db.temporal.asOf(
  $recordedAt,
  'MATCH (owner:Account)-[r:OWNS]->(:Asset {id:$id})
   RETURN owner.id AS owner, r.starts_at AS starts_at, r.ends_at AS ends_at',
  {id:'asset-42'}
) YIELD row
RETURN row;
```

`$recordedAt` asks what the database had recorded at that instant. It does not
select the owner by the epoch's bounds; add that predicate inside the query when
both questions matter. The constraint, the epoch-selection query and `asOf` are
independent.

`asOf` requires retained MVCC versions. It excludes currently deleted entities,
never crosses deletion/recreation boundaries, and has no historical embedding
vectors. Explicit ownership epochs last as long as you keep their relationships;
MVCC history follows its own retention policy. See
[Historical Reads & MVCC Retention](historical-reads-mvcc-retention.md).

### Adapt the pattern

Use `EMPLOYED_BY`, `ASSIGNED_TO` or another relationship type for your domain.
Choose the grouping tuple to express what must be exclusive. For example,
`GROUP BY r.tenantId, r.assetId` scopes exclusivity by tenant and asset. Every
component must match before two epochs compete. Extra constraints can require
fields and types; see [Managing Constraints](managing-constraints.md).

Relationship epoch validation currently scans the relevant relationship type;
an ordinary property index does not turn it into a neighboring-epoch lookup.
See [algorithmic costs and measured limits](../performance/temporal-read-and-constraint-costs.md).

## Alternative: a generic fact ledger

Use this model when one schema must represent many predicates, when facts need
independent links/provenance, or when explicit mutation events are required:

```text
Entity ──HAS_FACT──> FactKey ──HAS_VERSION──> FactVersion
                       └────CURRENT──────> FactVersion
MutationEvent ──AFFECTS───────────────────> FactVersion
```

It adds a stable identity for each `(subject, predicate)` slot and an explicit
current pointer. Those are useful capabilities, but they add constraints and
write coordination. They are not prerequisites for historical reads or exclusive
periods. The following sections describe this more configurable alternative;
its bootstrap script is not required for the recommended pattern.

### Overview

This guide shows how to implement a **canonical graph** in NornicDB with:

- **Declarative constraints** (UNIQUE, EXISTS, NODE KEY, type, temporal no‑overlap, cardinality, and endpoint policies)
- **Versioned facts** with validity windows
- **Append‑only mutation log** (graph events + WAL txlog)
- **Receipts** for auditability (tx_id, wal_seq_start/end, hash)
- **Vector search** for semantic retrieval over canonical facts

The epoch constraint spelling and historical-query signature shown here target v1.5.0. The ≤1.4.x tuple constraint and positional asOf forms remain accepted but deprecated.

---

### Where This Pattern Is Useful

The canonical graph ledger pattern is especially useful when you need both **graph intelligence** and **auditability**:

- **Financial systems**: track rate/risk/collateral fact versions with non-overlapping validity windows and reconstruct state "as of" a regulator-requested timestamp.
- **Compliance and RegTech**: model KYC/AML assertions as retained fact versions, keep actor/transaction provenance, and correlate changes with WAL-backed receipts.
- **Audit platforms**: correlate graph-level mutation events to WAL sequence ranges and receipt hashes for investigation and reconciliation workflows.
- **AI governance**: store model-produced assertions (`asserted_by=model:vX`) and human overrides as separate versions, then explain who changed what and when.
- **Data lineage systems**: preserve derivation chains and temporal validity so downstream reports can be replayed against historical truth states.

#### Graph-RAG / RAG Pipeline Simplification

Canonical graph ledger modeling also simplifies LLM retrieval pipelines:

- Keep **facts, relationships, vector embeddings, and provenance** in one database.
- Run **hybrid retrieval** (vector + keyword) and **graph traversal** without moving data across separate vector and graph stores.
- Filter explicit fact periods for "what applied last quarter?"; use **as-of reads** for "what had been recorded then?", subject to retention.
- Return **audit context** (tx_id, WAL range, receipt hash) with retrieved facts for high-trust inference paths.

This reduces ETL glue code and lowers the risk of retrieval/lineage drift between systems.

---

### Prerequisites

- **Persistent storage** (Badger) — schema and constraints persist across restarts.
- **Embeddings enabled** (optional) if you want vector search.

If you use vector search:

- Configure your embedding provider and dimensions.
- Trigger embed worker after bulk loads:
  - `POST /nornicdb/embed/trigger`
  - `GET /nornicdb/embed/stats` to verify dimensions.

---

### Step 1 — Configure WAL retention (optional, opt‑in)

By default, auto‑compaction (snapshots + truncation) is enabled. For **ledger‑grade** retention, enable sealed segment retention and/or disable auto‑compaction.

**YAML:**

```yaml
database:
  wal_auto_compaction_enabled: true
  wal_retention_max_segments: 24
  wal_retention_max_age: "168h" # 7 days
  wal_ledger_retention_defaults: false
```

**Env:**

```bash
export NORNICDB_WAL_AUTO_COMPACTION_ENABLED=true
export NORNICDB_WAL_RETENTION_MAX_SEGMENTS=24
export NORNICDB_WAL_RETENTION_MAX_AGE=168h
export NORNICDB_WAL_LEDGER_RETENTION_DEFAULTS=false
```

**Notes:**
- Retention is **opt‑in**. Review the current [segment retention algorithm and limits](../operations/wal-compaction.md#retained-segments); a count setting is not a minimum retained history or a tamper-proof ledger.
- Disabling auto‑compaction is recommended only when you have a durable WAL retention plan.

---

### Step 2 — Bootstrap the canonical schema

The [bootstrap script](../examples/canonical-bootstrap.cypher) is a starting point for the generic schema. It retains older temporal guidance; apply the epoch declarations below for native no-overlap enforcement. Use your configured administrator password:

```bash
cat docs/examples/canonical-bootstrap.cypher | cypher-shell -u admin -p "$NORNICDB_PASSWORD"
```

The generic schema can combine:
- Required fields (EXISTS) for nodes and relationships
- Uniqueness constraints (single and composite) for nodes and relationships
- NODE KEY and RELATIONSHIP KEY constraints
- Property type constraints for nodes and relationships
- **Temporal no‑overlap** constraints for nodes and relationships (NornicDB extension)
- **Domain/enum** constraints restricting properties to allowed value sets (NornicDB extension)
- **Cardinality** constraints limiting outgoing/incoming edge count per node (NornicDB extension)
- **Endpoint policy** constraints restricting allowed/disallowed label pairs for relationship types (NornicDB extension)
- Vector indexes
- Property indexes for lookup speed

#### Node constraints

```cypher
// Uniqueness
CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE

// Existence (required field)
CREATE CONSTRAINT entity_type_exists IF NOT EXISTS
FOR (e:Entity) REQUIRE e.entity_type IS NOT NULL

// Node key (composite uniqueness + existence)
CREATE CONSTRAINT factkey_nk IF NOT EXISTS
FOR (fk:FactKey) REQUIRE (fk.subject_entity_id, fk.predicate) IS NODE KEY

// Property type
CREATE CONSTRAINT fv_valid_from_type IF NOT EXISTS
FOR (fv:FactVersion) REQUIRE fv.valid_from IS :: DATETIME

// Temporal no‑overlap (NornicDB extension)
CREATE CONSTRAINT fv_temporal IF NOT EXISTS
FOR (fv:FactVersion) REQUIRE temporal.epoch(fv.valid_from, fv.valid_to) GROUP BY fv.fact_key

// Domain/enum (NornicDB extension)
CREATE CONSTRAINT entity_status_domain IF NOT EXISTS
FOR (e:Entity) REQUIRE e.status IN ['active', 'archived', 'deleted']
```

#### Relationship constraints

```cypher
// Uniqueness — each CURRENT edge carries a unique fact_key
CREATE CONSTRAINT current_fk_unique IF NOT EXISTS
FOR ()-[r:CURRENT]-() REQUIRE r.fact_key IS UNIQUE

// Existence — every HAS_VERSION edge must have a version number
CREATE CONSTRAINT has_version_exists IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE r.version IS NOT NULL

// Property type — version must be an integer
CREATE CONSTRAINT has_version_type IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE r.version IS :: INTEGER

// Relationship key (composite uniqueness + existence)
CREATE CONSTRAINT has_version_rk IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE (r.fact_key, r.version) IS RELATIONSHIP KEY

// Temporal no‑overlap on relationships (NornicDB extension)
// temporal.epoch names the time range; GROUP BY names the grouping key
CREATE CONSTRAINT has_version_temporal IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE temporal.epoch(r.valid_from, r.valid_to) GROUP BY r.fact_key

// Domain/enum on relationships (NornicDB extension)
CREATE CONSTRAINT affects_op_domain IF NOT EXISTS
FOR ()-[r:AFFECTS]-() REQUIRE r.op_type IN ['CREATE_FACT', 'UPDATE_FACT', 'CLOSE_FACT']

// Cardinality — each FactKey may have at most one CURRENT edge (NornicDB extension)
CREATE CONSTRAINT current_max_one IF NOT EXISTS
FOR ()-[r:CURRENT]->() REQUIRE MAX COUNT 1

// Endpoint policy — only FactKey nodes may have outgoing HAS_VERSION to FactVersion (NornicDB extension)
CREATE CONSTRAINT has_version_allowed IF NOT EXISTS
FOR (:FactKey)-[r:HAS_VERSION]->(:FactVersion) REQUIRE ALLOWED

// Endpoint policy — disallow MutationEvent directly linking to Entity (NornicDB extension)
CREATE CONSTRAINT no_direct_mutation_entity IF NOT EXISTS
FOR (:MutationEvent)-[r:AFFECTS]->(:Entity) REQUIRE DISALLOWED
```

Verify:

```cypher
CALL db.constraints();
SHOW INDEXES;
```

#### Block-style constraint contracts

NornicDB also supports grouping several related checks into a single contract with `REQUIRE { ... }`. The block can mix primitive constraints that compile into existing schema objects with boolean predicates that are checked at creation time and on every write. Use [Managing Constraints](managing-constraints.md) for the operational guide; this section keeps the canonical-graph examples in one place.

##### Node contract example

```cypher
CREATE CONSTRAINT person_contract
FOR (n:Person)
REQUIRE {
  n.id IS UNIQUE
  n.name IS NOT NULL
  n.age IS :: INTEGER
  n.status IN ['active', 'inactive']
  (n.tenant, n.externalId) IS NODE KEY
  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
}

SHOW CONSTRAINT CONTRACTS
```

Expected `SHOW CONSTRAINT CONTRACTS` row:

```text
name              | targetEntityType | targetLabelOrType | entryCount | compiledEntryCount | runtimeEntryCount | definition
person_contract   | NODE             | Person            | 6          | 5                  | 1                 | CREATE CONSTRAINT person_contract ...
```

If the current data already violates the contract, creation fails immediately:

```cypher
CREATE (:Person {id: 'p-1', name: 'Ada', age: 34, status: 'paused', tenant: 't1', externalId: 'e1'})

CREATE CONSTRAINT person_contract
FOR (n:Person)
REQUIRE {
  n.id IS UNIQUE
  n.name IS NOT NULL
  n.age IS :: INTEGER
  n.status IN ['active', 'inactive']
  (n.tenant, n.externalId) IS NODE KEY
}
```

Expected error:

```text
constraint contract person_contract violated: predicate `n.status IN ['active', 'inactive']` evaluated to false
```

##### Relationship contract example

```cypher
CREATE CONSTRAINT works_at_contract
FOR ()-[r:WORKS_AT]-()
REQUIRE {
  r.id IS UNIQUE
  r.startedAt IS NOT NULL
  r.role IS :: STRING
  (r.tenant, r.externalId) IS RELATIONSHIP KEY
  temporal.epoch(r.startedAt, r.endedAt) GROUP BY r.tenant, r.employeeId
  startNode(r) <> endNode(r)
  startNode(r).tenant = endNode(r).tenant
  r.status IN ['active', 'inactive']
  r.hoursPerWeek > 0
}

SHOW CONSTRAINT CONTRACTS
```

Expected `SHOW CONSTRAINT CONTRACTS` row:

```text
name               | targetEntityType | targetLabelOrType | entryCount | compiledEntryCount | runtimeEntryCount | definition
works_at_contract   | RELATIONSHIP     | WORKS_AT          | 9          | 5                  | 4                 | CREATE CONSTRAINT works_at_contract ...
```

The epoch entry reserves employment time per `(tenant, employeeId)`. Its inline
`GROUP BY` belongs only to that entry. A second relationship for the same employee
cannot overlap even if it has a different external ID. Adjacent epochs are
allowed; the remaining checks still apply independently.

Runtime enforcement rejects invalid writes as soon as they are attempted:

```cypher
MATCH (a:Person {id: 'p-1'}), (b:Person {id: 'p-2'})
CREATE (a)-[:WORKS_AT {id: 'w-1', startedAt: '2024-01-01', endedAt: null, role: 'Engineer', tenant: 't1', employeeId: 'p-1', externalId: 'rel-1', status: 'paused', hoursPerWeek: 40}]->(b)
```

Expected error:

```text
constraint contract works_at_contract violated: predicate `r.status IN ['active', 'inactive']` evaluated to false
```

---

### Step 3 — Write canonical entities and facts

Canonical model:
- `(:Entity)` — canonical identity
- `(:FactKey)` — slot per `(subject_entity_id, predicate)`
- `(:FactVersion)` — retained assertion whose end is closed when superseded

**Create entity + fact key:**

```cypher
CREATE (e:Entity {
  entity_id: 'product-123',
  entity_type: 'Product',
  display_name: 'Widget Pro',
  created_at: datetime()
})
MERGE (fk:FactKey {
  subject_entity_id: 'product-123',
  predicate: 'price'
})
MERGE (e)-[:HAS_FACT]->(fk);
```

**Create a fact version:**

```cypher
CREATE (fv:FactVersion {
  fact_key: 'product-123|price',
  value_json: '{"amount": 99.99, "currency": "USD"}',
  valid_from: datetime(),
  valid_to: null,
  asserted_at: datetime(),
  asserted_by: 'user:alice'
})
WITH fv
MATCH (fk:FactKey {subject_entity_id: 'product-123', predicate: 'price'})
MERGE (fk)-[:CURRENT {fact_key:fv.fact_key}]->(fv)
MERGE (fk)-[:HAS_VERSION {fact_key:fv.fact_key, version:1, valid_from:fv.valid_from, valid_to:fv.valid_to}]->(fv);
```

**Important:** The temporal no‑overlap constraint prevents overlapping validity windows for a given `fact_key`.

---

### Step 4 — Update facts with new versions

Close the previous version and create a new one in one statement. This two-version example uses version numbers 1 and 2; applications must allocate subsequent numbers transactionally:

```cypher
MATCH (fk:FactKey {subject_entity_id: 'product-123', predicate: 'price'})
MATCH (fk)-[current:CURRENT]->(fv_old:FactVersion)
MATCH (fk)-[previous:HAS_VERSION]->(fv_old)
WHERE fv_old.valid_to IS NULL
WITH fk, fv_old, current, previous, datetime() AS boundary
SET fv_old.valid_to = boundary, previous.valid_to = boundary
DELETE current

CREATE (fv_new:FactVersion {
  fact_key: 'product-123|price',
  value_json: '{"amount": 89.99, "currency": "USD"}',
  valid_from: boundary,
  valid_to: null,
  asserted_at: datetime(),
  asserted_by: 'user:alice'
})
WITH fv_new, fk
MERGE (fk)-[:CURRENT {fact_key:fv_new.fact_key}]->(fv_new)
MERGE (fk)-[:HAS_VERSION {fact_key:fv_new.fact_key, version:2, valid_from:fv_new.valid_from, valid_to:fv_new.valid_to}]->(fv_new);
```

---

### Step 5 — Mutation log (events + WAL)

#### Graph‑native events

```cypher
CREATE (me:MutationEvent {
  event_id: 'event-' + toString(timestamp()),
  tx_id: 'tx-' + toString(timestamp()),
  actor: 'user:alice',
  timestamp: datetime(),
  op_type: 'UPDATE_FACT_VERSION'
})
WITH me
MATCH (fv:FactVersion {fact_key: 'product-123|price'})
WHERE fv.valid_to IS NULL
MERGE (me)-[:AFFECTS {op_type:'UPDATE_FACT'}]->(fv);
```

#### WAL txlog queries (ledger view)

```cypher
// The 1,000 most recent entries of the current database
CALL db.txlog.entries() YIELD txId, db, kind, seq, timestamp, payload
RETURN seq, kind, txId, timestamp, payload
ORDER BY seq;

// Entries from sequence 1000 to 1200 (inclusive)
CALL db.txlog.entries(1000, 1200) YIELD seq, kind, txId
RETURN seq, kind, txId
ORDER BY seq;

// Entries of one transaction, at most 200 of them
CALL db.txlog.byTxId('tx-123', 200) YIELD txId, db, kind, seq, timestamp, payload
RETURN seq, kind, txId, timestamp, payload
ORDER BY seq;
```

- `db.txlog.entries(fromSeq = null, toSeq = null)` returns the current database's entries from `fromSeq` (inclusive, at least 1) to `toSeq` (inclusive; `null` or `0` is no upper bound). Without either, it returns the 1,000 most recent entries.
- `db.txlog.byTxId(txId, limit = null)` returns the current database's entries of one transaction; `limit` caps how many (`null`, `0` or less is no limit).
- Both yield `txId` (the entry's transaction ID, `""` when it has none), `db`, `kind` (the operation, such as `create_node` or `tx_commit`), `seq` (the WAL sequence), `timestamp` (RFC 3339, UTC) and `payload` (the entry's JSON data).
- Arguments may be literals, parameters or variables.

---

### Step 6 — Receipts (proof of mutation)

Receipts provide:
- `tx_id`
- `wal_seq_start`
- `wal_seq_end`
- `hash`

**HTTP:** Successful native transactional mutations return `TransactionResponse.receipt`, including write-behind acknowledgements. The receipt identifies the WAL transaction records; it does not by itself prove that a buffered write has been materialized into Badger. An actual buffered acknowledgement also produces HTTP `202 Accepted` with `X-NornicDB-Consistency: eventual`. Optimistic metadata is additional execution metadata and does not replace the receipt.
**MCP:** `store`, `link`, `task` responses include `receipt`.

You can use the receipt to fetch the associated WAL entries via `db.txlog.byTxId`.

---

### Step 7 — As‑of reads (temporal queries)

Read the fact graph as it was at a chosen instant. `query` is an ordinary read-only `MATCH` query, and `parameters` is an optional map bound to its `$parameters`:

```cypher
CALL db.temporal.asOf(
  datetime('2024-02-15T00:00:00Z'),
  'MATCH (fv:FactVersion) WHERE fv.fact_key = $factKey
   RETURN fv.fact_key AS fact_key, fv.value_json AS value_json,
          fv.valid_from AS valid_from, fv.valid_to AS valid_to',
  {factKey: 'product-123|price'}
) YIELD row
RETURN row;
```

Each yielded `row` is one row of the inner query resolved against the historical view: the properties, labels, and relationships of entity incarnations that still exist. Deleted nodes and relationships are not resurrected, an identifier that was deleted and later reused does not expose the earlier incarnation, and no temporal constraint is required to read history. If the requested instant is outside retained MVCC history, the call fails with an explicit error. Historical embedding vectors are not retained, so a historical read does not return them.

The older positional `db.temporal.asOf(label, keyProp, keyValue, validFromProp, validToProp, asOf)` form, and the tuple DDL `REQUIRE (fv.fact_key, fv.valid_from, fv.valid_to) IS TEMPORAL NO OVERLAP`, are deprecated starting in 1.5.0. Both remain accepted for compatibility, and no removal version is set.

See [Historical Reads & MVCC Retention](historical-reads-mvcc-retention.md) for the historical read contract, retention policy, and maintenance details.

---

### Step 8 — Vector search over canonical facts

Create a vector index (already in bootstrap):

```cypher
CALL db.index.vector.queryNodes('canonical_fact_idx', 10, 'price update for product x')
YIELD node, score
RETURN node, score;
```

---

### Operational Checklist

-  Run `canonical-bootstrap.cypher`
-  Ensure `FactVersion` validity windows don’t overlap
-  Record `MutationEvent` nodes for provenance
-  Use receipts for audit‑grade mutation proofs
-  Configure WAL retention only when you need ledger‑grade durability

---

### Related Guides

- [Cypher Queries](cypher-queries.md)
- [Transactions](transactions.md)
- [Vector Search](vector-search.md)
- [Hybrid Search](hybrid-search.md)
- [Data Import/Export](data-import-export.md)
