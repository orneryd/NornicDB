---
name: nornicdb-neo4j-migration
description: Migrate from Neo4j 5 to NornicDB end-to-end over Bolt. Covers schema replication (constraints + indexes including fulltext and vector), node and edge replication using hot-path UNWIND/MERGE shapes from the cookbook, label preservation strategy, what does and does not transfer, the cutover playbook. Points to runnable Python/Go/Node migration scripts and ties every batch shape back to a named executor fast path.
---

# Migrating from Neo4j to NornicDB

NornicDB speaks Bolt and Cypher. Migration is a Bolt → Bolt replay using the same drivers Neo4j users already have (`neo4j`, `neo4j-driver`, `neo4j-go-driver/v5`). No protocol bridge, no ETL service.

This skill covers the migration plan and the exact query shapes used. Runnable scripts live in [`scripts/migration/neo4j/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration/neo4j).

## Phase order (do not reorder)

1. **Schema** — constraints, then indexes (vector and fulltext last). Idempotent via `IF NOT EXISTS`.
2. **Nodes** — `UNWIND $rows MERGE (n:_Migrated {_neo4j_id: row.id}) SET n += row.props` per label, keyset-paginated by `elementId`. Hits [`UnwindSimpleMergeBatch`](cypher-queries.skill.md#7-write-hot-paths).
3. **Edges** — `UNWIND $rows MATCH (a) MATCH (b) CREATE (a)-[:T]->(b)` per relationship type. Hits [`UnwindMultiMatchCreateBatch`](cypher-queries.skill.md#multi-match-create-bulk-seed-unwindmultimatchcreatebatch).

Reordering breaks the edge phase because it depends on `_Migrated` nodes being present.

## Schema replication

Source-side queries used by every script:

```cypher
SHOW CONSTRAINTS
SHOW INDEXES
```

Each row maps to a target DDL statement that NornicDB's parser accepts:

| Source row | Emitted DDL |
|---|---|
| Node UNIQUENESS | `CREATE CONSTRAINT <name> IF NOT EXISTS FOR (n:L) REQUIRE n.p IS UNIQUE` |
| Node NODE_KEY | `CREATE CONSTRAINT <name> IF NOT EXISTS FOR (n:L) REQUIRE (n.p1, n.p2) IS NODE KEY` |
| Node PROPERTY_EXISTENCE | `CREATE CONSTRAINT <name> IF NOT EXISTS FOR (n:L) REQUIRE n.p IS NOT NULL` |
| Relationship UNIQUENESS / EXISTENCE | `CREATE CONSTRAINT <name> IF NOT EXISTS FOR ()-[r:T]-() REQUIRE r.p IS UNIQUE / IS NOT NULL` |
| Range/B-tree node index | `CREATE INDEX <name> IF NOT EXISTS FOR (n:L) ON (…)` |
| Fulltext node index | `CREATE FULLTEXT INDEX <name> IF NOT EXISTS FOR (n:L) ON EACH [n.p1, n.p2]` |
| Vector node index | `CREATE VECTOR INDEX <name> IF NOT EXISTS FOR (n:L) ON (n.p) OPTIONS {indexConfig: {…dimensions, similarity_function…}}` |

Indexes that are owned by an existing constraint are skipped — applying the constraint DDL recreates the index. Anything we don't know how to map cleanly is logged and skipped.

## Node replication — hot-path shape

```cypher
UNWIND $rows AS row
MERGE (n:_Migrated {_neo4j_id: row.id})
SET n += row.props
SET n._neo4j_labels = row.labels
```

Why this shape:

- `MERGE` on a stable key (`_neo4j_id`) lands on `UnwindSimpleMergeBatch`. The batch hot path probes the `_neo4j_id` schema lookup once per batch, not per row.
- Idempotent: re-running the same batch refreshes properties without creating duplicates.
- Labels stored as data on `_neo4j_labels` because Cypher cannot parameterize labels. A follow-up promotion pass adds them as real labels.

Source-side keyset pagination (do not use `SKIP/LIMIT` — that is the cookbook's anti-pattern #7):

```cypher
MATCH (n:`<Label>`) WHERE elementId(n) > $cursor
RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props
ORDER BY elementId(n)
LIMIT $limit
```

## Edge replication — hot-path shape

```cypher
UNWIND $rows AS row
MATCH (a:_Migrated {_neo4j_id: row.startId})
MATCH (b:_Migrated {_neo4j_id: row.endId})
CREATE (a)-[:`T` {_neo4j_id: row.id}]->(b)
```

Why this shape:

- The two MATCHes followed by CREATE with no RETURN/WITH/SET/MERGE is the exact shape `UnwindMultiMatchCreateBatch` recognizes — the bulk-seed edge-only variant.
- Property values are restricted to `row.<field>` references, so the executor can short-circuit per-row Cypher re-parse.

Edge properties are applied in a follow-up pass only when the chunk has any:

```cypher
UNWIND $rows AS row
MATCH (a:_Migrated {_neo4j_id: row.startId})-[r:`T` {_neo4j_id: row.id}]->(b:_Migrated {_neo4j_id: row.endId})
SET r += row.props
```

This keeps the create-side strictly on the bulk-seed hot path and avoids paying SET cost when there's nothing to set.

## Label promotion (one Cypher pass per real label)

Cypher cannot parameterize labels. After the script finishes, promote each label with a per-label MATCH:

```cypher
MATCH (n:_Migrated)
WHERE 'Person' IN n._neo4j_labels
SET n:Person
REMOVE n:_Migrated
REMOVE n._neo4j_labels;
```

Run one statement per source label. The `_Migrated` label and the bookkeeping properties drop off as you go.

If you don't need the original Neo4j ID afterwards, also remove `_neo4j_id` from nodes and edges. If anything in your application still keys off Neo4j IDs, keep it for compatibility.

## Batch sizing

| Phase | Default | Tuning hint |
|---|---|---|
| Nodes | 500 rows / batch | Up to 2000 if rows are small. Keep below 5000 to avoid commit-time write timeout. |
| Edges | 500 rows / batch | Same range; edge property maps tend to be smaller than node maps so more headroom. |

The `UnwindMultiMatchCreateBatch` hot path scales well from 100 to 2000 rows. The 500 default is a balance between per-batch overhead and commit lifetime.

## What does NOT transfer

- **Multi-database routing** — NornicDB is single-instance per data dir. Migrate one database at a time, choose `--target-database` accordingly.
- **APOC procedures called from Neo4j-side bootstrap** — re-author against NornicDB's APOC subset (`docs/features/apoc-functions.md`).
- **Username/password and RBAC** — re-create on the NornicDB side. Neo4j's user store is not portable.
- **Neo4j fulltext analyzer settings** beyond `standard` — NornicDB uses BM25 with its own tokenizer; the source analyzer is treated as best-effort.
- **Vector index OPTIONS keys outside `vector.dimensions` / `vector.similarity_function`** — NornicDB auto-tunes HNSW; the rest of Neo4j's keys are ignored.
- **Triggers, listeners, change-data-capture hooks** — re-implement using NornicDB's WAL/txlog (`db.txlog.entries`) if needed.

## Cutover playbook

1. **Stand up an empty NornicDB instance.** No bootstrap; the migration writes the schema.
2. **Dry-run** with `--dry-run` and a single label / type to confirm DDL maps cleanly:
   ```bash
   python scripts/migration/neo4j/migrate.py … --labels User --types KNOWS --dry-run
   ```
3. **Schema-only pass** (`--skip-nodes --skip-edges`) once. Verify with `SHOW CONSTRAINTS` / `SHOW INDEXES` on NornicDB.
4. **Cold corpus pass** while Neo4j is still serving. Use `--labels` to chunk the migration if your dataset is large.
5. **Pause writes** on Neo4j (consumer-side queue drain or read-only mode).
6. **Top-up pass** to capture any deltas — node phase is idempotent (`MERGE`), edge phase needs `--skip-nodes --skip-edges` then a re-run on just the touched types if you split the migration.
7. **Promote labels** with the per-label promotion Cypher above.
8. **Cut read traffic over to NornicDB.**
9. **Decommission Neo4j** once you're satisfied; keep it read-only for the rollback window.

## Verification

```cypher
SHOW CONSTRAINTS;
SHOW INDEXES;
MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count DESC LIMIT 20;
MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC LIMIT 20;

-- Confirm no orphan _Migrated remains after promotion
MATCH (n:_Migrated) RETURN count(n);
```

A clean migration ends with `count(n:_Migrated) = 0` once you've run promotion for every label you care about.

## Running the scripts

All three scripts implement the exact same phases and accept the same flags:

```bash
# Python
python scripts/migration/neo4j/migrate.py \
  --source-url bolt://neo4j.prod:7687 --source-user neo4j --source-pass <pw> \
  --target-url bolt://nornicdb.local:7687 \
  --batch-size 500

# Go
go run ./scripts/migration/neo4j/migrate.go \
  --source-url bolt://neo4j.prod:7687 --source-user neo4j --source-pass <pw> \
  --target-url bolt://nornicdb.local:7687 \
  --batch-size 500

# Node
node scripts/migration/neo4j/migrate.mjs \
  --source-url bolt://neo4j.prod:7687 --source-user neo4j --source-pass <pw> \
  --target-url bolt://nornicdb.local:7687 \
  --batch-size 500
```

Common flags: `--labels A,B`, `--types T,U`, `--skip-schema | --skip-nodes | --skip-edges`, `--dry-run`.

## See also
- [`bolt-client.skill.md`](bolt-client.skill.md) — Bolt connection, retry classification, MERGE under concurrent writers.
- [`cypher-queries.skill.md`](cypher-queries.skill.md) — the hot-path catalog this migration's UNWIND/MERGE shapes target.
- [`scripts/migration/neo4j/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration/neo4j) — Python, Go, Node migration scripts.
- [`docs/neo4j-migration/cypher-compatibility.md`](../neo4j-migration/cypher-compatibility.md) — full Cypher feature parity matrix.
- [`docs/performance/hot-path-query-cookbook.md`](../performance/hot-path-query-cookbook.md) — the long-form cookbook the script's batch shapes pin to.
