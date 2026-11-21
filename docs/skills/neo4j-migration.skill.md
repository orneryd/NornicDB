---
name: nornicdb-neo4j-migration
description: Migrate from Neo4j 5 to NornicDB end-to-end over Bolt. Covers schema replication (constraints + indexes including fulltext and vector), node and edge replication using shared idempotent UNWIND/MERGE statements, label preservation strategy, what does and does not transfer, the cutover playbook. Points to runnable Python/Go/Node migration scripts and verifies applied target rows.
---

# Migrating from Neo4j to NornicDB

NornicDB speaks Bolt and Cypher. Migration is a Bolt → Bolt replay using the same drivers Neo4j users already have (`neo4j`, `neo4j-driver`, `neo4j-go-driver/v5`). No protocol bridge, no ETL service.

This skill covers the migration plan and the exact query shapes used. Runnable scripts live in [`scripts/migration/neo4j/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration/neo4j).

## Phase order (do not reorder)

1. **Schema** — constraints, then indexes (vector and fulltext last). Idempotent via `IF NOT EXISTS`.
2. **Nodes** — `UNWIND` and `MERGE` by source `elementId` under the visible `Neo4jMigrated` label, preserving source labels as data.
3. **Edges** — match visible endpoints, `MERGE` each relationship by source ID and apply properties in the same statement.

Reordering breaks the edge phase because both imported endpoints must exist. All three clients load the same [`queries.json`](https://github.com/orneryd/nornicdb/blob/main/scripts/migration/neo4j/queries.json) destination statements. Keep it beside Python/Node; Go embeds it when built.

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

## Node replication

```cypher
UNWIND $rows AS row
MERGE (n:Neo4jMigrated {_neo4j_id: row.id})
SET n += row.props
SET n._neo4j_id = row.id
SET n._neo4j_labels = row.labels
RETURN count(n) AS applied
```

`Neo4jMigrated` is a visible bridge label; underscore-prefixed labels denote internal records and cannot serve ordinary endpoint MATCH. Reserve `_neo4j_id`, `_neo4j_labels` and the bridge label for the migration. Repeating a batch refreshes existing nodes. Source labels remain data until promotion.

The scripts keyset-paginate each selected source label by `elementId`, avoiding `SKIP`:

```cypher
MATCH (n:`<Label>`) WHERE elementId(n) > $cursor
RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props
ORDER BY elementId(n)
LIMIT $limit
```

## Relationship replication

```cypher
UNWIND $rows AS row
MATCH (a:Neo4jMigrated {_neo4j_id: row.startId})
MATCH (b:Neo4jMigrated {_neo4j_id: row.endId})
MERGE (a)-[r:`T` {_neo4j_id: row.id}]->(b)
SET r += row.props
SET r._neo4j_id = row.id
RETURN count(r) AS applied
```

The source relationship ID distinguishes parallel relationships with the same type and endpoints. Reruns preserve each relationship and update its properties. Clients consume the returned `applied` count and fail if it differs from the supplied batch size. Missing endpoints therefore produce a visible batch failure, not false success. Earlier batches remain applied and may be rerun.

Reported totals are applied source rows, not newly created or unique entities. Multi-label nodes can appear in several label scans. Dry runs report source rows read without claiming target writes.

## Label promotion (one Cypher pass per real label)

After import, add every source label before removing the label-list bookkeeping:

```cypher
MATCH (n:Neo4jMigrated)
WHERE 'Person' IN n._neo4j_labels
SET n:Person
RETURN count(n) AS promoted;
```

Repeat for `Employee` and every other source label, including labels carried by nodes selected through a different `--labels` filter. The node's full list stays available throughout promotion. After all labels are promoted and verified, remove only that list:

```cypher
MATCH (n:Neo4jMigrated)
REMOVE n._neo4j_labels;
```

Keep `Neo4jMigrated` and `_neo4j_id` on the imported graph for repeatable migration after promotion. A rerun restores `_neo4j_labels`; promote any new source labels before cutover. Removing the bridge or identity metadata ends the rerun contract.

The scripts do not repair hidden `_Migrated` nodes left by an older failed migration. Use a fresh target for the corrected migration or plan separate recovery; rerunning does not reuse or remove those historical hidden records. Preserve the old target until recovery is verified.

## Batch sizing

| Phase | Default | Tuning hint |
|---|---|---|
| Nodes | 500 rows / batch | Up to 2000 if rows are small. Keep below 5000 to avoid commit-time write timeout. |
| Edges | 500 rows / batch | Same range; edge property maps tend to be smaller than node maps so more headroom. |

The default is 500 rows. Measure batch size against the source/target workload; these counted MERGE statements are not a claim of the former CREATE-only fast-path performance.

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
6. **Top-up pass** to refresh imported nodes and relationships. Both phases use stable source IDs and may be rerun; this is not a deletion/change-data-capture synchronizer.
7. **Promote labels** with the per-label promotion Cypher above.
8. **Cut read traffic over to NornicDB.**
9. **Decommission Neo4j** once you're satisfied; keep it read-only for the rollback window.

## Verification

```cypher
SHOW CONSTRAINTS;
SHOW INDEXES;
MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count DESC LIMIT 20;
MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC LIMIT 20;

-- Count imported nodes (bridge intentionally retained for reruns)
MATCH (n:Neo4jMigrated) RETURN count(n);
```

Compare source-labelled node counts, relationship counts, properties and endpoints with the selected source population. A retained bridge count is not a remaining-work count. Zero hidden-label matches or absent bookkeeping does not establish completion.

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
