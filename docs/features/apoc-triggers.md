# APOC triggers

NornicDB runs Neo4j 5 APOC triggers: a Cypher statement a database runs at
the commit of each of its transactions, with the transaction's changes as
parameters. The procedures, phases and parameters are those of APOC 5
(`apoc.trigger.*`).

## Enabling triggers

Triggers are off by default, as `apoc.trigger.enabled` is in APOC. Set

```
NORNICDB_APOC_TRIGGER_ENABLED=true
```

in the server's environment. While triggers are off, every trigger procedure
fails with
`Neo.ClientError.Procedure.ProcedureCallFailed` and no installed trigger
fires.

## Managing triggers

These run on the `system` database and name the database they manage. All of
them need the admin entitlement.

| Procedure | What it does |
| --- | --- |
| `apoc.trigger.install(databaseName, name, statement, selector, config = {})` | Installs a trigger, or replaces the one with that name. `selector` is `{phase: ...}`; `config.params` are extra parameters for the statement. Returns the trigger. |
| `apoc.trigger.drop(databaseName, name)` | Removes a trigger; returns it. |
| `apoc.trigger.dropAll(databaseName)` | Removes all the database's triggers; returns them. |
| `apoc.trigger.stop(databaseName, name)` / `start(...)` | Pauses / resumes a trigger. |
| `apoc.trigger.show(databaseName)` | Lists the database's triggers. |

On a user database, `apoc.trigger.list()` lists the session database's
triggers. The deprecated APOC 4 forms `apoc.trigger.add`, `remove`,
`removeAll`, `pause` and `resume` manage the session database's triggers.

Every procedure returns rows of `name, query, selector, params, installed,
paused`.

`install` replaces a trigger by name, so running the same `install` again
(for example from schema setup at every start) leaves one trigger with the
same definition. As in APOC, `install` always installs the trigger running:
re-installing a stopped trigger starts it again.

## Phases

| `selector.phase` | When the statement runs |
| --- | --- |
| `before` (default) | In the committing transaction, before its commit. The statement reads the transaction's final state and its writes are part of the same commit. If it fails, the transaction is rolled back. |
| `after` | After a successful commit, in a new transaction. |
| `afterAsync` | Like `after`, on a background worker. |
| `rollback` | After a transaction that changed something rolled back, in a new transaction. |

A `before` trigger fires for every way a write reaches the database:
auto-commit and explicit transactions over Bolt and HTTP, `apoc.cypher.*`
and `apoc.periodic.*`, `CALL { } IN TRANSACTIONS` (each batch), `USE` and
composite databases, write-behind commits, and writes that don't go through
Cypher (bulk import, the Qdrant-compatible API, the DB API).

When several triggers run in one phase, they run in name order. If any
`before` trigger fails, the others still run, then the commit fails with
`Neo.ClientError.Transaction.TransactionHookFailed`, naming each trigger that
failed and its error.

### Recursion

A `before` trigger runs once per commit. What it writes is part of the
transaction, but no `before` trigger is called again for it, so a trigger
that sets a property on the nodes it is given doesn't fire itself again.
Every trigger of a phase gets the same parameters, taken before the first one
ran.

`after` and `afterAsync` triggers run in a transaction tagged with the
metadata `{apoc.trigger: true}`. That transaction's commit runs the
database's `before` triggers once, but no `after` or `afterAsync` trigger, so
after triggers don't cascade.

## Parameters

| Parameter | Value |
| --- | --- |
| `$createdNodes`, `$deletedNodes` | List of nodes |
| `$createdRelationships`, `$deletedRelationships` | List of relationships |
| `$assignedLabels`, `$removedLabels` | Map: label → list of nodes |
| `$assignedNodeProperties` | Map: key → list of `{key, node, old, new}` |
| `$removedNodeProperties` | Map: key → list of `{key, node, old}` |
| `$assignedRelationshipProperties` | Map: key → list of `{key, relationship, old, new}` |
| `$removedRelationshipProperties` | Map: key → list of `{key, relationship, old}` |
| `$transactionId` | The commit's transaction ID in `after` / `afterAsync`; -1 otherwise |
| `$commitTime` | The commit time (ms) in `after` / `afterAsync`; -1 otherwise |
| `$metaData` | The transaction's metadata |
| `$trigger` | The trigger's name |

A created node's properties are all assigned (with `old` null) and its labels
assigned; a deleted node's are removed. A node created and deleted in the same
transaction is in no list. Setting a property to the value it has, or adding a
label the node has, is no assignment. `config.params` are added last and
replace a parameter of the same name.

### Example: a derived key, kept in the same transaction

```cypher
CALL apoc.trigger.install('nornic', 'name_key',
  'UNWIND $keys AS k
   UNWIND coalesce($assignedNodeProperties[k], []) AS e
   WITH collect(e.node) AS touched
   UNWIND touched + coalesce($assignedLabels[$label], []) AS n
   WITH DISTINCT n
   WHERE $label IN labels(n)
   SET n.name_key = toLower(n.name)',
  {phase: 'before'},
  {params: {keys: ['name'], label: 'Person'}})
```

Every `:Person` node whose `name` is created or changed, or that gains the
label `Person`, gets `name_key` in the same transaction, whatever wrote it.

## Storage and privileges

Triggers are kept in the system database, so they survive a restart, stopped
ones included. Dropping a database deletes its triggers.

A `before` trigger runs with the rights of the user whose transaction it
runs in: if the statement needs a right the user lacks, the user's
transaction fails. `after`, `afterAsync` and `rollback` triggers run in their
own transaction, as the database itself.

With no trigger installed (or all stopped) a database's commits do nothing
extra.

## Differences from APOC

- **Immediate:** installing, dropping, stopping or starting a trigger takes
  effect at once. APOC picks the change up at its next `apoc.trigger.refresh`
  (60 s by default).
- **Checked at install:** `install` and `add` refuse a statement that doesn't
  parse and a phase that doesn't exist. APOC accepts them, and the trigger
  then fails every later write of the database.
- **Dropped with the database:** APOC keeps a dropped database's triggers, so
  a database created later with the same name would run them.
- **Deleted entities:** a deleted node or relationship in the parameters
  carries its last properties and labels: `n.name` gives the old value. In
  Neo4j they can be read only with `apoc.any.properties`.
