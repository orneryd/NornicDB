# Stored Procedure Runtime and Transaction Script Extensions

This document describes the current procedure runtime in NornicDB, including:

- registry-backed `CALL` execution parity behavior
- user-defined procedure DDL (`CREATE/DROP PROCEDURE`)
- startup precompile/loading of persisted procedures
- Nornic transaction script extensions (`BEGIN TRANSACTION`, `BEGIN` shorthand, conditional rollback scripts)

## What Is Standard vs Extension

- **Neo4j-compatible surface:**
  - `CALL ...`
  - `SHOW PROCEDURES`
  - `CALL dbms.procedures()`
  - existing built-in/APOC-style procedure behavior and `YIELD` semantics
- **Nornic extensions:**
  - `CREATE [OR REPLACE] PROCEDURE ... MODE ... AS ...`
  - `DROP PROCEDURE ...`
  - transaction script blocks using `BEGIN TRANSACTION` or `BEGIN` with inline `COMMIT`/`ROLLBACK`
  - conditional transaction script pattern: `CASE WHEN ... THEN ROLLBACK ELSE RETURN ... COMMIT`

## Runtime Contract

- A single registry-backed runtime resolves both built-in and user-defined procedures.
- `SHOW PROCEDURES` and `CALL dbms.procedures()` are generated from the same runtime state.
- Procedure metadata includes:
  - canonical `name`
  - `signature`
  - `description`
  - `mode` (`READ`, `WRITE`, `SCHEMA`, `ADMIN`, `DBMS`)
  - `worksOnSystem`
  - argument cardinality (`minArgs`, `maxArgs`)

## Procedure DDL Syntax (Nornic Extension)

```cypher
CREATE OR REPLACE PROCEDURE nornic.touchUser($id, $ts)
MODE WRITE
AS
MATCH (u:User {id: $id})
SET u.last_seen = $ts
RETURN u
```

```cypher
DROP PROCEDURE nornic.touchUser
```

### DDL constraints

- `CREATE PROCEDURE` and `DROP PROCEDURE` are rejected inside an active explicit transaction.
- Procedure mode validation is enforced at create/compile time (for example, `MODE READ` cannot contain write operations).
- Procedure modes are authorization contracts for Bolt callers: `WRITE` requires the `write` entitlement, `SCHEMA` requires `schema`, and `ADMIN` or `DBMS` requires `admin`. `READ` requires `read`.
- Dynamic statements run by procedures inherit the invoking caller's effective entitlements. A query supplied through a parameter cannot bypass the caller's write, schema, or admin permissions.

## Startup Precompile and Persistence

- User-defined procedures are persisted in the database metadata graph under catalog label `_ProcedureCatalog`.
- Catalog payload is msgpack-encoded and stored as a base64 string property.
- On executor startup:
  - built-ins are registered
  - persisted user-defined procedures are loaded
  - each definition is compiled and registered before query execution

This provides fast runtime dispatch without first-call compilation overhead.

## Transaction Script Extensions (Nornic Extension)

### Explicit form

```cypher
BEGIN TRANSACTION
CALL nornic.touchUser('u-10', datetime())
YIELD u
RETURN u.id, u.last_seen
COMMIT
```

### Shorthand form (equivalent)

```cypher
BEGIN
CALL nornic.touchUser('u-10', datetime())
YIELD u
RETURN u.id, u.last_seen
COMMIT
```

### Conditional rollback script

```cypher
BEGIN TRANSACTION
CALL nornic.touchUser('u-10', datetime())
YIELD u
CASE
  WHEN u.age < 18 THEN ROLLBACK
  ELSE
    RETURN u.id, u.last_seen
COMMIT
```

Whitespace and casing variations are accepted for these script forms.

## Compatibility Behavior and Errors

- Unknown procedure:
  - `unknown procedure: <name> (try SHOW PROCEDURES for available procedures)`
- Argument arity mismatch:
  - minimum: `procedure <name> requires at least N arguments, got M`
  - maximum: `procedure <name> accepts at most N arguments, got M`
- Unknown YIELD column:
  - `unknown YIELD column: <column>`

## Built-in Procedure Surface

Built-ins are registered in `pkg/cypher/procedure_registry_builtin.go`, including:

- core schema/info (`db.labels`, `db.relationshipTypes`, `db.propertyKeys`, `db.info`, `db.ping`)
- fulltext/vector (`db.index.fulltext.*`, `db.index.vector.*`, `db.create.set*VectorProperty`)
- DBMS (`dbms.components`, `dbms.info`, `dbms.listConfig`, `dbms.clientConfig`, `dbms.listConnections`, `dbms.procedures`, `dbms.functions`)
- index/stats/ops (`db.await*`, `db.resampleIndex`, `db.stats.*`, `db.clearQueryCaches`, `tx.setMetaData`)
- NornicDB (`nornicdb.version`, `nornicdb.stats`, `nornicdb.decay.info`)

## Plugin-based User Procedures

Procedure plugins remain supported via the plugin system:

- plugin can expose `Procedures() map[string]...` in addition to function entries
- procedure handlers are registered in the same global runtime registry
- plugin procedures are visible in both `SHOW PROCEDURES` and `CALL dbms.procedures()`

Supported plugin handler shape:

- `func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error)`

## Tests Covering This Feature

- `pkg/cypher/procedure_ddl_test.go`
- `pkg/cypher/transaction_script_test.go`
- existing procedure compatibility tests under `pkg/cypher/*procedures*_test.go`

## Migration Guidance

- Existing `CALL` usage requires no query changes.
- Use procedure DDL for Nornic-native persisted procedures.
- For maximal compatibility with Neo4j clients, keep application logic based on `CALL` and avoid relying on Nornic-only DDL/transaction-script syntax in portable query sets.
