# Neo4j → NornicDB Migration Scripts

Three runnable migrations to move a full Neo4j database into NornicDB. All three speak Bolt on both ends — NornicDB exposes the same protocol Neo4j uses, so existing drivers (`neo4j`, `neo4j-driver`, `neo4j-go-driver/v5`) work unchanged.

| Script | Stack |
|---|---|
| [`migrate.py`](migrate.py)  | Python 3.10+, `neo4j` |
| [`migrate.go`](migrate.go)  | Go 1.21+, `github.com/neo4j/neo4j-go-driver/v5` |
| [`migrate.mjs`](migrate.mjs) | Node 20+, `neo4j-driver` |

All three implement the **same three phases in the same order**:

1. **Schema** — `SHOW CONSTRAINTS` + `SHOW INDEXES` on the source, port to NornicDB DDL with `IF NOT EXISTS`. Constraints first, indexes second (vector and fulltext last).
2. **Nodes** — `UNWIND $rows AS row MERGE (n:_Migrated {_neo4j_id: row.id}) SET n += row.props` for every label, keyset-paginated by `elementId`. This is the [`UnwindSimpleMergeBatch`](../../../docs/performance/hot-path-query-cookbook.md#73-bulk-ingestion-with-unwind) hot-path shape.
3. **Edges** — `UNWIND $rows AS row MATCH (a) MATCH (b) CREATE (a)-[:T]->(b)` for every relationship type, also keyset-paginated. This is the [`UnwindMultiMatchCreateBatch`](../../../docs/performance/hot-path-query-cookbook.md#73h-bulk-seed-multi-match-lookups-plus-create-writes-no-return) edge-only variant. After the create, a follow-up MATCH ... SET applies edge properties only when at least one row carries any.

## How label and ID continuity is preserved

- **Every migrated node carries `_Migrated` plus its preserved `_neo4j_id` (Neo4j's `elementId`).** Edges bind to those IDs, so the source's graph topology is reproduced exactly.
- **Original labels are stored on `n._neo4j_labels`** as a list. Cypher cannot parameterize labels, so each script preserves them as data and leaves the final label-promotion step to the operator. A typical follow-up:
  ```cypher
  MATCH (n:_Migrated)
  WHERE 'Person' IN n._neo4j_labels
  SET n:Person
  REMOVE n:_Migrated
  REMOVE n._neo4j_labels
  REMOVE n._neo4j_id;
  ```
- The `_Migrated` label is the bridge that makes edge MATCHes deterministic. Once you've promoted real labels, you can `REMOVE n:_Migrated` in the same pass.

## Common flags

| Flag | Default | Effect |
|---|---|---|
| `--source-url`, `--source-user`, `--source-pass` | (required) | Source Neo4j connection |
| `--target-url`, `--target-user`, `--target-pass` | `bolt://localhost:7687`, `neo4j`, `password` | Target NornicDB connection |
| `--source-database`, `--target-database` | `neo4j` | Per side |
| `--batch-size N` | `500` | Rows per UNWIND batch |
| `--labels A,B,C` | all | Migrate only the named node labels |
| `--types T,U,V` | all | Migrate only the named relationship types |
| `--skip-schema` / `--skip-nodes` / `--skip-edges` | off | Skip a phase |
| `--dry-run` | off | Read everything, write nothing |

## Typical run

```bash
# Python
python scripts/migration/neo4j/migrate.py \
  --source-url bolt://neo4j.prod:7687 \
  --source-user neo4j --source-pass <pw> \
  --target-url bolt://nornicdb.local:7687 \
  --batch-size 500
```

Phase output:

```
[schema]
  → CREATE CONSTRAINT user_email_unique IF NOT EXISTS FOR (n:User) REQUIRE n.email IS UNIQUE
  → CREATE INDEX user_id_idx IF NOT EXISTS FOR (n:User) ON (n.id)
  → CREATE FULLTEXT INDEX docs_text IF NOT EXISTS FOR (n:Document) ON EACH [n.title, n.content]

[nodes]
  · :User: 500
  · :User: 1000
  · :Document: 500
  ...
  total nodes upserted: 38421

[edges]
  · :KNOWS: 500
  · :KNOWS: 1000
  · :MENTIONED_IN: 500
  ...
  total edges created: 92117

Done in 41.2s
```

## Constraint and index porting

| Source (Neo4j) | Target DDL emitted by these scripts |
|---|---|
| Node UNIQUENESS | `CREATE CONSTRAINT … FOR (n:L) REQUIRE n.p IS UNIQUE` |
| Node NODE_KEY | `CREATE CONSTRAINT … FOR (n:L) REQUIRE (n.p1, n.p2) IS NODE KEY` |
| Node PROPERTY_EXISTENCE | `CREATE CONSTRAINT … FOR (n:L) REQUIRE n.p IS NOT NULL` |
| Relationship UNIQUENESS / EXISTENCE | `CREATE CONSTRAINT … FOR ()-[r:T]-() REQUIRE r.p IS UNIQUE / IS NOT NULL` |
| Range/B-tree indexes | `CREATE INDEX … FOR (n:L) ON (…)` |
| Fulltext indexes | `CREATE FULLTEXT INDEX … FOR (n:L) ON EACH [n.p1, n.p2]` |
| Vector indexes | `CREATE VECTOR INDEX … OPTIONS {indexConfig: {…}}` (dimensions + similarity preserved) |

Constraints owned by automatically-generated indexes are emitted only as the constraint DDL — NornicDB recreates the underlying index automatically. Constraints we don't safely know how to port are skipped with a log line; check the output before assuming the schema replicated cleanly.

## What does NOT get migrated

- **Multi-database routing**: NornicDB instances are not 1:1 with Neo4j 5's multi-database model. Migrate one database at a time and choose `--target-database` accordingly.
- **APOC procedures and stored procedures** that ran in Neo4j as part of bootstrap. Re-author against NornicDB's APOC subset (see `docs/features/apoc-functions.md`).
- **Username/password and RBAC**: re-create on the NornicDB side with `Auth.Enabled=true` plus a YAML auth block. Neo4j's user store is not portable.
- **Vector index OPTIONS keys outside `vector.dimensions` / `vector.similarity_function`**: NornicDB auto-tunes HNSW; the rest of Neo4j's keys are ignored.

## Verification after migration

```bash
# Spot-check counts at the target
cypher-shell -a bolt://nornicdb.local:7687 -u neo4j -p password <<'EOF'
SHOW INDEXES;
SHOW CONSTRAINTS;
MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count DESC LIMIT 20;
MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC LIMIT 20;
EOF
```

After the run, the only `_Migrated` nodes that still have that label are ones you haven't promoted. Run the label-promotion follow-up Cypher above to remove the bridge label.

## See also
- [`docs/skills/neo4j-migration.skill.md`](../../../docs/skills/neo4j-migration.skill.md) — the consumer-facing migration skill.
- [`docs/skills/cypher-queries.skill.md`](../../../docs/skills/cypher-queries.skill.md) — hot-path catalog the script's UNWIND/MERGE shapes target.
- [`docs/skills/bolt-client.skill.md`](../../../docs/skills/bolt-client.skill.md) — Bolt connection defaults and retry classification.
- [`docs/neo4j-migration/cypher-compatibility.md`](../../../docs/neo4j-migration/cypher-compatibility.md) — full Cypher feature parity matrix.
