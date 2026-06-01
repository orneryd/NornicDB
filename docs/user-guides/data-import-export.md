# Data Import/Export Guide

**Neo4j compatible data migration and backup**

---

## Overview

NornicDB is fully compatible with Neo4j data formats, making migration seamless. This guide covers importing data from Neo4j and exporting data for backup or migration.

---

## Importing from Neo4j

### Using Neo4j Driver

```python
from neo4j import GraphDatabase

# Connect to NornicDB (same as Neo4j)
driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("admin", "admin")
)

# Run Cypher queries - identical syntax
with driver.session() as session:
    session.run("""
        CREATE (n:Person {name: 'Alice', age: 30})
        CREATE (m:Person {name: 'Bob', age: 25})
        CREATE (n)-[:KNOWS {since: 2020}]->(m)
    """)
```

### Bulk Import via Cypher

```cypher
-- Create multiple nodes
UNWIND $nodes AS nodeData
CREATE (n:Person)
SET n = nodeData

-- Create relationships
UNWIND $relationships AS relData
MATCH (a:Person {id: relData.from})
MATCH (b:Person {id: relData.to})
CREATE (a)-[:KNOWS {since: relData.since}]->(b)
```

### JSON Import

```bash
# Import nodes from JSON
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [{
      "statement": "UNWIND $nodes AS n CREATE (p:Person) SET p = n",
      "parameters": {
        "nodes": [
          {"name": "Alice", "age": 30},
          {"name": "Bob", "age": 25}
        ]
      }
    }]
  }'
```

---

## Exporting Data

### Export via Cypher

```cypher
-- Export all nodes as JSON
MATCH (n)
RETURN labels(n) AS labels, properties(n) AS properties

-- Export with relationships
MATCH (n)-[r]->(m)
RETURN
  labels(n) AS fromLabels,
  properties(n) AS fromProps,
  type(r) AS relType,
  properties(r) AS relProps,
  labels(m) AS toLabels,
  properties(m) AS toProps
```

### Export via HTTP API

```bash
# Export query results
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [{
      "statement": "MATCH (n:Person) RETURN n"
    }]
  }' | jq '.results[0].data'
```

---

## Neo4j Compatibility

### Supported Features

| Feature        | Status  | Notes               |
| -------------- | ------- | ------------------- |
| Cypher Queries | ✅ Full | All standard Cypher |
| Bolt Protocol  | ✅ Full | v4.4 compatible     |
| HTTP API       | ✅ Full | Neo4j REST API      |
| Transactions   | ✅ Full | ACID compliant      |
| Indexes        | ✅ Full | B-tree and vector   |
| Constraints    | ✅ Full | Unique, exists      |

### Driver Compatibility

All official Neo4j drivers work with NornicDB:

- **Python**: `neo4j` package
- **JavaScript**: `neo4j-driver`
- **Java**: Neo4j Java Driver
- **Go**: `neo4j-go-driver`
- **.NET**: Neo4j.Driver

---

## Migration Steps

### From Neo4j to NornicDB

1. **Export from Neo4j**:

Export Neo4j-compatible CSV files from Neo4j using the tooling you already use for offline import packages.

2. **Start NornicDB**:

   ```bash
   docker run -d -p 7474:7474 -p 7687:7687 nornicdb
   ```

3. **Import to NornicDB**:

   ```bash
   nornicdb-admin database import full mydb \
    --from-path=./neo4j-export \
    --data-dir=./data
   ```

If the folder includes `schema.cypher`, `nornicdb-admin` will apply it automatically unless you override `--schema`.

See the [admin tool guide](../operations/admin-tool.md) for CSV header examples, `--from-path`, and recovery notes.

### From NornicDB to Neo4j

Use the offline admin export tool to create a Neo4j-compatible package:

```bash
nornicdb-admin database export neo4j-csv mydb \
  --to-path=./neo4j-export \
  --data-dir=./data
```

This writes:

- `nodes.csv`
- `relationships.csv` when the database has relationships
- `schema.cypher` when the database has exportable constraints or indexes

That package is designed to be re-imported with `nornicdb-admin database import full --from-path=...` and to stay close to Neo4j's offline CSV conventions.

---

## Backup & Restore

### Creating Backups

```bash
# Trigger a snapshot via the authenticated admin HTTP endpoint
curl -X POST http://localhost:7474/admin/backup \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

For volume-level backups (raw `tar` of `/data`) see the [Backup & Restore operations guide](../operations/backup-restore.md).

---

## GDPR Compliance

### Data Export (Right to Access)

```bash
curl -X POST http://localhost:7474/gdpr/export \
  -H "Content-Type: application/json" \
  -d '{"userId": "user123"}'
```

### Data Deletion (Right to Erasure)

```bash
curl -X POST http://localhost:7474/gdpr/delete \
  -H "Content-Type: application/json" \
  -d '{"userId": "user123"}'
```

---

## Related Documentation

- **[Getting Started](../getting-started/README.md)** - Installation guide
- **[Cypher Queries](cypher-queries.md)** - Query language reference
- **[API Reference](../api-reference/README.md)** - Complete API docs
- **[nornicdb-admin](../operations/admin-tool.md)** - Offline bulk import, `--from-path`, and Neo4j-compatible export workflows

---

**Ready to migrate?** → **[Getting Started](../getting-started/README.md)**
