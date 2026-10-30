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

## APOC Custom Procedures (`apoc.custom`)

The APOC form of the same feature is portable to Neo4j with APOC. A statement
installed under a name is called as `custom.<name>`:

```cypher
CALL apoc.custom.installProcedure(
  'reach(start :: INTEGER, depth = 1 :: INTEGER) :: (n :: INTEGER)',
  'MATCH (s:Step {n: $start})-[:NEXT*1..10]->(t) WHERE t.n - s.n <= $depth RETURN t.n AS n',
  'neo4j', 'read', 'steps reachable from a start');

CALL custom.reach(1) YIELD n RETURN n;      // depth defaults to 1
CALL custom.reach($start, 3) YIELD n RETURN n;
```

- `apoc.custom.installProcedure(signature, statement, databaseName = 'neo4j', mode = 'read', description = '')`
  (APOC 5) and `apoc.custom.declareProcedure(signature, statement, mode = 'read', description = '')`
  (APOC 4) install it, replacing a procedure of the same name. The procedure
  is stored in the current database's procedure catalog, like `CREATE
  PROCEDURE`, and is available again after a restart; `databaseName` is
  accepted for compatibility.
- The signature declares the arguments (`name :: TYPE`, optionally with a
  default, `name = value :: TYPE`; optional arguments come last) and the
  output columns (`:: (column :: TYPE, …)`, or `:: VOID`). The statement reads
  the arguments as parameters (`$start`) and returns the output columns.
- `mode` is `read`, `write`, `schema` or `dbms`; a `read` procedure can't
  contain writes.
- `apoc.custom.dropProcedure(name, databaseName = 'neo4j')` removes it;
  `apoc.custom.list()` and `apoc.custom.show()` list the installed procedures
  (type, name, description, mode, statement, inputs, outputs, forceSingle).
- A custom procedure is one for the whole server: whoever calls it, on any
  database, runs its statement with their own permissions. Installing,
  replacing and dropping one need the `admin` entitlement, as `CREATE` /
  `DROP PROCEDURE` do and as APOC 5 marks `installProcedure` and
  `dropProcedure`. A call of a custom procedure is never answered from the
  result cache, so the call after a replacement runs the new statement.
- Custom functions (`apoc.custom.installFunction`) are not provided.

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
- NornicDB (`nornicdb.version`, `nornicdb.stats`, `kp.decayInfo`)

## Plugin-based User Procedures

Procedure plugins remain supported via the plugin system:

- plugin can expose `Procedures() map[string]...` in addition to function entries
- procedure handlers are registered in the same global runtime registry
- plugin procedures are visible in both `SHOW PROCEDURES` and `CALL dbms.procedures()`

Supported plugin handler shapes:

- `cypher.ProcedureHandler`, or its equivalent Go function type:
  `func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error)`
- The existing `func(context.Context, string, []interface{}) (*cypher.ExecuteResult, error)` ABI remains supported.

The canonical ABI receives the calling executor and statement context, rather
than a captured global storage backend. For example, a plugin's `Procedures()`
entry can declare a read procedure using:

```go
Handler: cypher.ProcedureHandler(func(ctx context.Context, exec *cypher.StorageExecutor, query string, args []interface{}) (*cypher.ExecuteResult, error) {
    return &cypher.ExecuteResult{
        Columns: []string{"value"},
        Rows: [][]interface{}{{args[0]}},
    }, nil
}),
Mode: "READ",
MinArgs: 1,
MaxArgs: 1,
```

Names preserve their registered case. Declare `WRITE`, `SCHEMA`, `ADMIN`, or
`DBMS` when appropriate; an unknown mode or unsupported/nil handler fails the
plugin load. Every procedure declaration is validated before any procedure from
that plugin is published. Arity, authorization, database selection, and `YIELD`
continue through the shared procedure registry.

`CALL apoc.cypher.doIt(statement, params) YIELD value` is a built-in `WRITE`
procedure. It runs parameterized data statements through the calling
transaction, and returns each inner row as a `value` map. For example:

```cypher
CALL apoc.cypher.doIt('CREATE (n:Person {name:$name}) RETURN n.name AS name',
                     {name:'Ada'})
YIELD value RETURN value.name
```

Rolling back the caller rolls back the inner write. The same statement policy
as the other dynamic procedures rejects administrative/internal statements.
This is the two-argument [APOC procedure contract](https://neo4j.com/docs/apoc/current/overview/apoc.cypher/apoc.cypher.doIt/);
it does not promote scalar functions such as `apoc.math.sigmoid` to procedures.

## Tests Covering This Feature

- `pkg/cypher/procedure_ddl_test.go`
- `pkg/cypher/transaction_script_test.go`
- existing procedure compatibility tests under `pkg/cypher/*procedures*_test.go`

## Migration Guidance

- Existing `CALL` usage requires no query changes.
- Use procedure DDL for Nornic-native persisted procedures.
- For maximal compatibility with Neo4j clients, keep application logic based on `CALL` and avoid relying on Nornic-only DDL/transaction-script syntax in portable query sets.
