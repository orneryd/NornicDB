# Change Data Capture

NornicDB records every committed change to the nodes and relationships of a
database whose change capture is on, and serves them through Neo4j's
Change Data Capture procedures and event format. An application can follow
a database's changes from a stored cursor instead of polling for them.

## Turning it on

Change capture is a database option, `txLogEnrichment`, as in Neo4j:

```cypher
CREATE DATABASE library OPTIONS {txLogEnrichment: 'FULL'}
ALTER DATABASE library SET OPTION txLogEnrichment 'DIFF'
ALTER DATABASE library REMOVE OPTION txLogEnrichment
SHOW DATABASES YIELD name, options
```

- `OFF` (the default) captures nothing.
- `FULL` records each entity's whole state before and after the change.
- `DIFF` records only the properties and labels an update changed. A create
  records the whole state after it, and a delete the whole state before it,
  in both modes.

The option is kept with the database's metadata and survives restarts.
Seeding options Neo4j defines (`seedURI`, `existingData`, `storeFormat`, …)
are not supported by NornicDB and are rejected.

## Reading changes

```cypher
// Store this as your cursor:
CALL db.cdc.current() YIELD id

// Later: the changes committed after the cursor, in commit order.
CALL db.cdc.query($cursor) YIELD id, txId, seq, metadata, event
RETURN id, txId, seq, metadata, event
```

- `db.cdc.current()` is the identifier just after the last committed change.
- `db.cdc.query(from = "", selectors = [])` returns the changes after `from`
  (exclusive; `""` is `db.cdc.current()`). Store the `id` of the last change
  you processed and pass it as the next `from`: the query resumes after it,
  even in the middle of a transaction.
- `db.cdc.earliest()` is the identifier just before the oldest change still
  kept.
- A third argument, `limit` (a NornicDB extension), returns at most that many
  changes: read a large backlog in parts by passing the last `id` as the next
  `from`. Without it, every change since `from` is returned.

Each row is one change:

- `txId` and `seq`: the transaction's place in the database's commit order,
  and the change's place in its transaction. Changes of one transaction share
  a `txId`.
- `event` for a node: `eventType: 'n'`, `elementId`, `operation` (`'c'`,
  `'u'` or `'d'`), `labels`, `keys`, and `state` with `before` and `after`,
  each `null` or `{properties, labels}`.
- `event` for a relationship: `eventType: 'r'`, `elementId`, `type`,
  `operation`, `keys`, `start` and `end` (each `{elementId, keys, labels}`),
  and `state` with `before` and `after`, each `null` or `{properties}`.
- `metadata`: `captureMode`, `txStartTime`, `txCommitTime`, `txMetadata`,
  `databaseId`, `executingUser` and `authenticatedUser` (the user whose
  privileges the transaction ran with, and the signed-in user; they differ
  only when the signed-in user impersonated the other over Bolt, and both are
  empty when no one signed in and for changes recorded before NornicDB kept
  users), and the connection fields of Neo4j's format (`connectionType`, …),
  which NornicDB does not record yet and leaves empty.

A `DETACH DELETE` reports the relationships it removes before the node.

`db.cdc.current()` and `db.cdc.earliest()` are read procedures.
`db.cdc.query` returns every change, whatever the caller may read, so it
needs admin rights, like Neo4j's `EXECUTE BOOSTED PROCEDURE`.

## Selectors

`db.cdc.query`'s second argument filters the changes: a list of selectors,
any of which may match (OR), each matching only when all its fields do (AND):

```cypher
CALL db.cdc.query($cursor, [
  {select: 'n', labels: ['Document'], changesTo: ['title']},
  {select: 'r', type: 'CITES', operation: 'c'}
])
```

- `select`: `'e'` (any entity), `'n'` (nodes) or `'r'` (relationships).
- Every selector: `operation`, `changesTo` (all the listed properties
  changed), `txMetadata` (these metadata entries), `executingUser`,
  `authenticatedUser`.
- Nodes: `elementId`, `labels` (all of them, before or after the change),
  `key`.
- Relationships: `elementId`, `type`, `key`, and `start` / `end`, node
  selectors (`elementId`, `labels`) for the endpoints.
- `key` matches the entity's NODE KEY / RELATIONSHIP KEY values, as Neo4j's
  does, so a `DIFF` update that left the key unchanged is still matched.
  Without a matching key constraint it falls back to the entity's property
  values (after the change, or before a delete); Neo4j matches key
  constraints only.

## Exactness

- Changes are written in the same storage commit as the data, so a rolled
  back transaction leaves no changes, and no committed change is missing.
- Writes through Bolt and HTTP, auto-commit and explicit transactions, and
  through NornicDB's other APIs (MCP, the Qdrant-compatible API, imports) are
  all captured. With async writes on (the write-behind buffer), a capturing
  database's commits still go straight to storage, with their events; other
  databases keep the buffer.

## Retention

Changes of transactions committed more than `NORNICDB_CDC_RETENTION` ago
(YAML `database.cdc_retention`; a Go duration, `48h` by default, Neo4j's
default transaction log retention) are deleted, once a minute. The last
transaction's changes are always kept, so `db.cdc.current()` never moves
back.

## Errors

Identifiers are local to their database: one from another database, or from
a database dropped and created again under the same name, is invalid.

| Situation | Error |
| --- | --- |
| The database's capture is off | `Neo.DatabaseError.ChangeDataCapture.Disabled` |
| A malformed or foreign identifier, or one older than `db.cdc.earliest()` | `Neo.ClientError.ChangeDataCapture.InvalidIdentifier` |
| An identifier past `db.cdc.current()` | `Neo.TransientError.ChangeDataCapture.FutureIdentifier` |
| An invalid selector | `Neo.ClientError.Statement.ArgumentError` |
