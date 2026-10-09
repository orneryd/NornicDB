# Managing NornicDB Constraints

NornicDB supports both Neo4j-compatible primitive constraints and several NornicDB-specific schema extensions. This page is the canonical guide for creating, inspecting, and operating those constraints, including block-style constraint contracts defined with `REQUIRE { ... }`.

Use this guide when you need to:

- apply NornicDB-only constraint families such as domain, temporal, cardinality, or endpoint-policy rules
- group multiple related rules into one named schema contract
- understand what is enforced at constraint creation time versus write time
- inspect primitive constraints separately from block-contract metadata

## Choose the right constraint form

Use a primitive constraint when one rule is enough:

```cypher
CREATE CONSTRAINT person_email_unique
FOR (n:Person) REQUIRE n.email IS UNIQUE
```

Use a block-style contract when several rules belong together and should be managed as one named schema object:

```cypher
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

Primitive rules continue to show up in `SHOW CONSTRAINTS`. Contract metadata is listed separately with `SHOW CONSTRAINT CONTRACTS`.

## NornicDB-specific constraint families

In addition to standard uniqueness, existence, node key, relationship key, and property type constraints, NornicDB adds several schema features that are specific to this engine.

### Domain and enum constraints

Restrict a property to a fixed allowed set.

```cypher
CREATE CONSTRAINT person_status_domain
FOR (n:Person) REQUIRE n.status IN ['active', 'inactive', 'suspended']

CREATE CONSTRAINT works_at_role_domain
FOR ()-[r:WORKS_AT]-() REQUIRE r.role IN ['engineer', 'manager', 'director']
```

### Temporal no-overlap constraints

Prevent overlapping validity windows for the same logical key.

```cypher
CREATE CONSTRAINT fact_version_no_overlap
FOR (n:FactVersion)
REQUIRE (n.fact_key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP
```

### Cardinality constraints

Limit outgoing or incoming relationship count per node. Direction is encoded in the `FOR` clause.

```cypher
CREATE CONSTRAINT employee_primary_employer_max_one
FOR ()-[r:PRIMARY_EMPLOYER]->() REQUIRE MAX COUNT 1

CREATE CONSTRAINT company_ceo_max_one
FOR ()<-[r:CEO]-() REQUIRE MAX COUNT 1
```

### Endpoint policy constraints

Control which label pairs a relationship type may connect.

```cypher
CREATE CONSTRAINT works_at_allowed
FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED

CREATE CONSTRAINT no_direct_mutation_entity
FOR (:MutationEvent)-[r:AFFECTS]->(:Entity) REQUIRE DISALLOWED
```

`ALLOWED` rules form a whitelist for a relationship type once any allowed rule exists. `DISALLOWED` rules are a blacklist and take precedence.

## Managing block-style constraint contracts

Block-style contracts let you group multiple checks under a single `CREATE CONSTRAINT` statement using `REQUIRE { ... }`.

Each block entry must be either:

- a primitive entry that maps onto the existing schema engine
- a boolean predicate that must evaluate to true for the targeted node or relationship

### Node contract example

```cypher
CREATE CONSTRAINT person_contract
FOR (n:Person)
REQUIRE {
  n.id IS UNIQUE
  n.name IS NOT NULL
  n.age IS :: INTEGER
  n.status IS :: STRING
  n.status IN ['active', 'inactive']
  (n.tenant, n.externalId) IS NODE KEY
  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
  NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }
}
```

### Relationship contract example

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
```

### How contract entries are enforced

Inside a block, primitive entries compile into the existing primitive constraint system when the semantics already match. Examples include:

- `n.id IS UNIQUE`
- `n.name IS NOT NULL`
- `n.age IS :: INTEGER`
- `(n.tenant, n.externalId) IS NODE KEY`
- `(n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP`
- `r.id IS UNIQUE`
- `r.startedAt IS NOT NULL`
- `r.role IS :: STRING`
- `(r.tenant, r.externalId) IS RELATIONSHIP KEY`

Boolean predicates remain runtime contract entries. Examples include:

- `n.status IN ['active', 'inactive']`
- `COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1`
- `NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }`
- `startNode(r) <> endNode(r)`
- `startNode(r).tenant = endNode(r).tenant`
- `r.hoursPerWeek > 0`

Property predicates (`n.status IN [...]`, `r.hoursPerWeek > 0`) follow SQL `CHECK` semantics: a node or relationship that does not have the property satisfies them, the same way primitive domain and type constraints accept null. Add `n.status IS NOT NULL` to the block when the property is required.

Every boolean predicate is compiled when the contract is created. A predicate the engine cannot evaluate (for example `n.status IS NULL OR n.status IN [...]`) fails `CREATE CONSTRAINT` with `unsupported node predicate` / `unsupported relationship predicate`, even when the target label has no data yet, and nothing is stored.

This split matters operationally:

- compiled entries reuse the current primitive storage and enforcement path
- runtime entries are evaluated only for affected contracts on writes
- compiled entries are not enforced twice

## What happens when you create a contract

Contract creation is all-or-nothing.

When you run `CREATE CONSTRAINT ... REQUIRE { ... }`, NornicDB:

1. parses the block
2. compiles block entries that map to existing primitives
3. validates every entry against current data
4. stores contract metadata only if every entry is valid and the current graph already satisfies the whole contract

If any existing node or relationship violates any entry, creation fails and no partial contract is stored.

Example failure:

```cypher
CREATE (:Person {
  id: 'p-1',
  name: 'Ada',
  age: 34,
  status: 'paused',
  tenant: 't1',
  externalId: 'e1'
})

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

## What happens on writes

After a contract exists, NornicDB enforces it on the same write paths that can change validity:

- node create
- node property updates
- node label changes
- relationship create
- relationship property updates
- node or relationship deletes when the predicate depends on edge presence
- transaction commit for batched writes

Evaluation order is:

1. primitive constraints
2. residual boolean predicates from block contracts

NornicDB evaluates only affected contracts rather than rescanning the whole graph. In practice that means node-targeted contracts for matching labels and relationship-targeted contracts for matching types, plus endpoint-adjacent checks when a predicate depends on connected entities.

## Inspect constraints and contracts

Use `SHOW CONSTRAINTS` to inspect actual primitive constraints, including primitives compiled out of a block contract.

```cypher
SHOW CONSTRAINTS
```

Use `SHOW CONSTRAINT CONTRACTS` to inspect the contract object itself.

```cypher
SHOW CONSTRAINT CONTRACTS
```

The contract listing includes:

- `name`
- `targetEntityType`
- `targetLabelOrType`
- `entryCount`
- `compiledEntryCount`
- `runtimeEntryCount`
- `definition`

Example row:

```text
name             | targetEntityType | targetLabelOrType | entryCount | compiledEntryCount | runtimeEntryCount | definition
person_contract  | NODE             | Person            | 8          | 6                  | 2                 | CREATE CONSTRAINT person_contract ...
```

Use the two listings together:

- `SHOW CONSTRAINTS` answers which primitive schema rules are active
- `SHOW CONSTRAINT CONTRACTS` answers which higher-level contract definition produced them and how much of the contract remains runtime-only

## Operational guidance

### Commit schema migrations separately

Ordinary index and constraint CREATE/DROP commands participate in explicit
transactions. Their changes are visible to that transaction, become public on
commit, and are discarded on rollback. Do not mix schema changes with graph
entity writes in either order: the transaction fails with
`Neo.ClientError.Transaction.ForbiddenDueToTransactionType` and must be rolled
back. Read-only queries may accompany schema changes.

For example, using a Neo4j-compatible driver session:

```python
with session.begin_transaction() as tx:
  tx.run("CREATE CONSTRAINT account_id FOR (n:Account) REQUIRE n.id IS UNIQUE").consume()
  tx.commit()
```

Commit data migrations in a separate transaction. A schema transaction whose
database data changed since its snapshot may fail with a transient conflict;
retry the complete schema transaction rather than publishing an incomplete
index backfill. Autocommit DDL continues to use the database's schema persister.
Native decay/promotion declarations use their separate knowledge-policy
transaction contract.

Compatibility vector/fulltext schema creation/drop procedures follow the same
commit/rollback lifetime as ordinary DDL. Relationship fulltext creation uses
relationship types rather than node labels. Like Neo4j, the deprecated
`db.index.vector.createNodeIndex` reports a schema invocation rejected after a
data write as `Neo.ClientError.Procedure.ProcedureCallFailed`; performing a data
write after its schema change still fails with
`Neo.ClientError.Transaction.ForbiddenDueToTransactionType`.

### Prefer explicit names

Always name NornicDB-specific constraints and contracts. Named schema objects are easier to inspect, compare across environments, and drop or recreate during migrations.

### Use `IF NOT EXISTS` for idempotent rollout

```cypher
CREATE CONSTRAINT person_status_domain IF NOT EXISTS
FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']
```

Use this for deployment scripts and bootstrap flows when the intended definition is stable.

### Validate data before rollout

Because creation fails on existing violations, it is usually worth running a targeted audit query before applying a new contract.

```cypher
MATCH (n:Person)
WHERE n.status IS NOT NULL AND NOT n.status IN ['active', 'inactive']
RETURN n.id, n.status
```

This is especially important for runtime predicates such as cardinality-style checks embedded in a block.

### Drop a contract

```cypher
DROP CONSTRAINT person_contract IF EXISTS
```

Dropping a contract by name removes the contract metadata and every primitive constraint compiled from it (`person_contract__entry_NN`). `SHOW CONSTRAINTS` and `SHOW CONSTRAINT CONTRACTS` both stop listing it, and the name can be reused. Contracts have no `ALTER`; to change one, drop it and create the new definition.

### Treat contract edits as schema migrations

Changing a block definition changes one named schema contract, not just one primitive row. Plan updates the same way you would plan any schema migration:

- clean or backfill existing data first
- apply the new contract definition
- verify both `SHOW CONSTRAINTS` and `SHOW CONSTRAINT CONTRACTS`

### Keep primitives separate when grouping adds no value

Do not use a block contract just because it is available. A single uniqueness or existence rule is clearer as a normal primitive constraint. Use a contract when the grouped rules describe one business invariant.

## Unsupported forms and scope limits

The following are not supported in block contracts:

- nested relationship mini-constraints inside `REQUIRE { ... }`
- automatic migration of existing primitive constraints into block contracts
- synthetic contract rows inside `SHOW CONSTRAINTS`

`SHOW CONSTRAINTS` remains the primitive schema listing. `SHOW CONSTRAINT CONTRACTS` is the contract-specific introspection surface.

## Recommended rollout pattern

1. Create or clean the target data model.
2. Run audit queries for expected enum, endpoint, or cardinality violations.
3. Create primitive constraints or one named block contract.
4. Verify `SHOW CONSTRAINTS` for compiled primitives.
5. Verify `SHOW CONSTRAINT CONTRACTS` for the contract metadata.
6. Exercise one valid write and one invalid write before promoting the migration.

## Related guides

- For general Cypher syntax, see [Cypher Queries](cypher-queries.md).
- For property type and value examples, see [Property Data Types](property-data-types.md).
- For a domain-model walkthrough that includes constraint examples, see [Canonical Graph Ledger](canonical-graph-ledger.md).
- For feature parity and Neo4j comparison, see [Cypher Compatibility](../neo4j-migration/cypher-compatibility.md).