# Canonical Graph + Mutation Log Guide

**Build a canonical truth store with constraints, temporal validity, and an audit‑grade mutation log.**

Last Updated: January 21, 2026

---

## Overview

This guide shows how to implement a **canonical graph** in NornicDB with:

- **Declarative constraints** (UNIQUE, EXISTS, NODE KEY, type, temporal no‑overlap, cardinality, and endpoint policies)
- **Versioned facts** with validity windows
- **Append‑only mutation log** (graph events + WAL txlog)
- **Receipts** for auditability (tx_id, wal_seq_start/end, hash)
- **Vector search** for semantic retrieval over canonical facts

Everything here works **as‑is** with current NornicDB features.

---

## Where This Pattern Is Useful

The canonical graph ledger pattern is especially useful when you need both **graph intelligence** and **auditability**:

- **Financial systems**: track rate/risk/collateral fact versions with non-overlapping validity windows and reconstruct state "as of" a regulator-requested timestamp.
- **Compliance and RegTech**: model KYC/AML assertions as immutable fact versions, keep actor/tx provenance, and prove mutation history with WAL-backed receipts.
- **Audit platforms**: correlate graph-level mutation events to WAL sequence ranges and receipt hashes for investigation and reconciliation workflows.
- **AI governance**: store model-produced assertions (`asserted_by=model:vX`) and human overrides as separate versions, then explain who changed what and when.
- **Data lineage systems**: preserve derivation chains and temporal validity so downstream reports can be replayed against historical truth states.

### Graph-RAG / RAG Pipeline Simplification

Canonical graph ledger modeling also simplifies LLM retrieval pipelines:

- Keep **facts, relationships, vector embeddings, and provenance** in one database.
- Run **hybrid retrieval** (vector + keyword) and **graph traversal** without moving data across separate vector and graph stores.
- Apply **as-of temporal reads** to answer time-bounded prompts ("what was true last quarter?").
- Return **audit context** (tx_id, WAL range, receipt hash) with retrieved facts for high-trust inference paths.

This reduces ETL glue code and lowers the risk of retrieval/lineage drift between systems.

---

## Prerequisites

- **Persistent storage** (Badger) — schema and constraints persist across restarts.
- **Embeddings enabled** (optional) if you want vector search.

If you use vector search:

- Configure your embedding provider and dimensions.
- Trigger embed worker after bulk loads:
  - `POST /nornicdb/embed/trigger`
  - `GET /nornicdb/embed/stats` to verify dimensions.

---

## Step 1 — Configure WAL retention (optional, opt‑in)

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
- Retention is **opt‑in** and does not change defaults unless you set it.
- Disabling auto‑compaction is recommended only when you have a durable WAL retention plan.

---

## Step 2 — Bootstrap the canonical schema

Run the idempotent bootstrap script once per database:

```bash
cat docs/plans/canonical-bootstrap.cypher | cypher-shell -u admin -p password
```

This creates:
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

### Node constraints

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
FOR (fv:FactVersion) REQUIRE (fv.fact_key, fv.valid_from, fv.valid_to) IS TEMPORAL NO OVERLAP

// Domain/enum (NornicDB extension)
CREATE CONSTRAINT entity_status_domain IF NOT EXISTS
FOR (e:Entity) REQUIRE e.status IN ['active', 'archived', 'deleted']
```

### Relationship constraints

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
// The last two properties are always the time range; preceding properties form the grouping key
CREATE CONSTRAINT has_version_temporal IF NOT EXISTS
FOR ()-[r:HAS_VERSION]-() REQUIRE (r.fact_key, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP

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

### Block-style constraint contracts

NornicDB also supports grouping several related checks into a single contract with `REQUIRE { ... }`. The block can mix primitive constraints that compile into existing schema objects with boolean predicates that are checked at creation time and on every write. Use [Managing Constraints](managing-constraints.md) for the operational guide; this section keeps the canonical-graph examples in one place.

#### Node contract example

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

#### Relationship contract example

```cypher
CREATE CONSTRAINT works_at_contract
FOR ()-[r:WORKS_AT]-()
REQUIRE {
  r.id IS UNIQUE
  r.startedAt IS NOT NULL
  r.role IS :: STRING
  (r.tenant, r.externalId) IS RELATIONSHIP KEY
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
works_at_contract   | RELATIONSHIP     | WORKS_AT          | 8          | 4                  | 4                 | CREATE CONSTRAINT works_at_contract ...
```

Runtime enforcement rejects invalid writes as soon as they are attempted:

```cypher
MATCH (a:Person {id: 'p-1'}), (b:Person {id: 'p-2'})
CREATE (a)-[:WORKS_AT {id: 'w-1', startedAt: '2024-01-01', role: 'Engineer', tenant: 't1', externalId: 'rel-1', status: 'paused', hoursPerWeek: 40}]->(b)
```

Expected error:

```text
constraint contract works_at_contract violated: predicate `r.status IN ['active', 'inactive']` evaluated to false
```

---

## Step 3 — Write canonical entities and facts

Canonical model:
- `(:Entity)` — canonical identity
- `(:FactKey)` — slot per `(subject_entity_id, predicate)`
- `(:FactVersion)` — immutable, versioned assertion

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
MERGE (fk)-[:CURRENT]->(fv)
MERGE (fk)-[:HAS_VERSION]->(fv);
```

**Important:** The temporal no‑overlap constraint prevents overlapping validity windows for a given `fact_key`.

---

## Step 4 — Update facts with new versions

Close the previous version and create a new one:

```cypher
MATCH (fk:FactKey {subject_entity_id: 'product-123', predicate: 'price'})
MATCH (fv_old:FactVersion)-[:CURRENT]-(fk)
WHERE fv_old.valid_to IS NULL
SET fv_old.valid_to = datetime()
REMOVE fv_old:CURRENT

CREATE (fv_new:FactVersion {
  fact_key: 'product-123|price',
  value_json: '{"amount": 89.99, "currency": "USD"}',
  valid_from: datetime(),
  valid_to: null,
  asserted_at: datetime(),
  asserted_by: 'user:alice'
})
WITH fv_new, fk
MERGE (fk)-[:CURRENT]->(fv_new)
MERGE (fk)-[:HAS_VERSION]->(fv_new);
```

---

## Step 5 — Mutation log (events + WAL)

### Graph‑native events

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
MERGE (me)-[:AFFECTS]->(fv);
```

### WAL txlog queries (ledger view)

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

## Step 6 — Receipts (proof of mutation)

Receipts provide:
- `tx_id`
- `wal_seq_start`
- `wal_seq_end`
- `hash`

**HTTP:** `TransactionResponse.receipt` is returned for durable transactional mutations. Eventual async responses expose `optimistic` metadata instead and do not include a durable receipt until the write is flushed/committed through the durable path.  
**MCP:** `store`, `link`, `task` responses include `receipt`.

You can use the receipt to fetch the associated WAL entries via `db.txlog.byTxId`.

---

## Step 7 — As‑of reads (temporal queries)

Use the temporal helper procedure. Positional args: `label, keyProp, keyValue, validFromProp, validToProp, asOf` (plus two optional MVCC selectors `systemTime` and `systemSequence`).

```cypher
CALL db.temporal.asOf(
  'FactVersion',                        -- label
  'fact_key',                           -- keyProp
  'product-123|price',                  -- keyValue
  'valid_from',                         -- validFromProp
  'valid_to',                           -- validToProp
  datetime('2024-02-15T00:00:00Z')      -- asOf
) YIELD node
RETURN node;
```

See [Historical Reads & MVCC Retention](historical-reads-mvcc-retention.md) for the full set of as-of read patterns including the MVCC snapshot selectors.

---

## Step 8 — Vector search over canonical facts

Create a vector index (already in bootstrap):

```cypher
CALL db.index.vector.queryNodes('canonical_fact_idx', 10, 'price update for product x')
YIELD node, score
RETURN node, score;
```

---

## Operational Checklist

- ✅ Run `canonical-bootstrap.cypher`
- ✅ Ensure `FactVersion` validity windows don’t overlap
- ✅ Record `MutationEvent` nodes for provenance
- ✅ Use receipts for audit‑grade mutation proofs
- ✅ Configure WAL retention only when you need ledger‑grade durability

---

## Related Guides

- [Cypher Queries](cypher-queries.md)
- [Transactions](transactions.md)
- [Vector Search](vector-search.md)
- [Hybrid Search](hybrid-search.md)
- [Data Import/Export](data-import-export.md)
