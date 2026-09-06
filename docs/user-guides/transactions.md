# Transactions

**Complete guide to ACID transactions in NornicDB**

Last Updated: November 2025

---

## Overview

NornicDB transactions provide full ACID guarantees for graph mutations:

- **Atomicity** — All operations in a transaction commit together or none do
- **Operation Buffering** — Changes are invisible to other readers until commit
- **Read-Your-Writes** — A transaction can read its own uncommitted changes
- **Rollback** — Discard all pending operations and restore the previous state
- **Conflict Detection** — Concurrent write-write races fail at commit instead of silently overwriting newer state

## Isolation

NornicDB provides **snapshot isolation** via MVCC:

- Each transaction captures a read snapshot at the moment it begins
- Snapshot-aware reads of node bodies, label scans, and directional relationships see that snapshot plus the transaction's own pending changes
- Uncommitted changes from other transactions are never visible
- If two transactions attempt to write to the same data, the second to commit receives a conflict error

Transaction snapshot lookups pin a separate read-only Badger transaction for
node bodies, label scans, and directional relationship traversal. Reserving an
MVCC sequence does not publish a write: a peer deletion committed after that
physical snapshot cannot make a relationship disappear between enumeration and
`DELETE`. Pending writes remain visible through the transaction's own overlay.

Relationship identity lookups used by `MERGE` still consult latest committed
state. A peer relationship discovered there but absent from the transaction
snapshot retains the existing transient conflict classification.

Write conflict checks also compare the head's physical publication revision,
including tombstones. This catches a peer that reserved its sequence before the
reader began but committed afterward. The conflict retains the existing
`changed after transaction start` error and Bolt transient classification, so
existing conflict telemetry and bounded client retries continue to apply.
The pinned reader is released on commit, rollback, or transaction expiry;
long-running transactions retain the underlying versions until release.

---

## Usage

### Auto-Commit (Single Statement)

The simplest way to run a query is the auto-commit endpoint. Each request is its own transaction:

```bash
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [{
      "statement": "CREATE (u:User {name: $name, email: $email}) RETURN u",
      "parameters": {"name": "Alice", "email": "alice@example.com"}
    }]
  }'
```

Multiple statements in a single request execute atomically — they all commit together or all roll back:

```bash
curl -X POST http://localhost:7474/db/nornic/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [
      {"statement": "CREATE (u:User {id: \"user-1\", name: \"Alice\"})"},
      {"statement": "CREATE (u:User {id: \"user-2\", name: \"Bob\"})"},
      {"statement": "MATCH (a:User {id: \"user-1\"}), (b:User {id: \"user-2\"}) CREATE (a)-[:FOLLOWS]->(b)"}
    ]
  }'
```

### Bolt Protocol

When connecting via the Bolt protocol (e.g. with the Neo4j driver), transactions follow standard Neo4j semantics:

```javascript
const neo4j = require('neo4j-driver');
const driver = neo4j.driver('bolt://localhost:7687');
const session = driver.session();

// Auto-commit
const result = await session.run(
  'CREATE (u:User {name: $name}) RETURN u',
  { name: 'Alice' }
);

// Explicit transaction
const tx = session.beginTransaction();
try {
  await tx.run('CREATE (u:User {id: "user-1", name: "Alice"})');
  await tx.run('CREATE (u:User {id: "user-2", name: "Bob"})');
  await tx.run('MATCH (a:User {id: "user-1"}), (b:User {id: "user-2"}) CREATE (a)-[:FOLLOWS]->(b)');
  await tx.commit();
} catch (error) {
  await tx.rollback();
  console.error('Transaction failed:', error);
}

await session.close();
await driver.close();
```

### Python (Neo4j Driver)

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")

# Transaction function (recommended — auto-retries on transient errors)
def create_friendship(tx, name_a, name_b):
    tx.run("MERGE (a:User {name: $name_a})", name_a=name_a)
    tx.run("MERGE (b:User {name: $name_b})", name_b=name_b)
    tx.run("""
        MATCH (a:User {name: $name_a}), (b:User {name: $name_b})
        CREATE (a)-[:FOLLOWS]->(b)
    """, name_a=name_a, name_b=name_b)

with driver.session() as session:
    session.execute_write(create_friendship, "Alice", "Bob")

driver.close()
```

---

## Error Handling

If a transaction encounters an error, all pending changes are automatically discarded:

| Scenario | Behavior |
|---|---|
| Write conflict (concurrent modification) | Transaction fails at commit; no changes applied |
| Constraint violation | Statement fails; transaction can be rolled back |
| Syntax error in Cypher | Statement rejected; transaction remains open for rollback |
| Connection lost | Transaction is automatically rolled back on the server |

---

## How It Works

Imagine you're rearranging furniture in your room:

1. **BEGIN** = "I'm going to rearrange my room"
2. **Operations** = Moving furniture around (but not committing yet)
3. **COMMIT** = "Yes! I like this arrangement, keep it!"
4. **ROLLBACK** = "Nope, put everything back where it was"

The transaction remembers where everything was before, so if you change your mind (ROLLBACK), everything goes back to the original spots!
