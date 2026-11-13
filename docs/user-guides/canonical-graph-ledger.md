# Canonical Graph Ledger Patterns

## Start with ownership of an epoch

An account owns an asset for a span of time. Put that span on the relationship:

```text
(Account) ──OWNS {starts_at, ends_at}──> (Asset)
```

Each `OWNS` relationship records one ownership epoch. Keep old relationships when
ownership changes. The graph then answers who owns the asset now and who owned it
before, without separate fact-key nodes or current pointers.

Run the [complete ownership example](../examples/temporal-ownership.cypher) in an
empty example database, or follow the steps below. This script models explicit
epochs; it does not require MVCC retention.

### Prevent conflicting ownership

<!-- temporal-example: account-key -->
```cypher
CREATE CONSTRAINT account_id IF NOT EXISTS
FOR (a:Account) REQUIRE a.id IS NODE KEY;
```

<!-- temporal-example: asset-key -->
```cypher
CREATE CONSTRAINT asset_id IF NOT EXISTS
FOR (a:Asset) REQUIRE a.id IS NODE KEY;
```

<!-- temporal-example: ownership-constraint -->
```cypher
CREATE CONSTRAINT asset_ownership IF NOT EXISTS
FOR ()-[r:OWNS]-()
REQUIRE temporal.epoch(r.starts_at, r.ends_at)
GROUP BY r.assetId;
```

`temporal.epoch` reserves the half-open span `[starts_at, ends_at)`. A null end
means unbounded future; a null start means unbounded past. At least one bound
must be present. Two finite bounds must increase. A malformed non-null bound is
rejected. An epoch starting in the future does not imply ownership today. `GROUP BY r.assetId` makes ownership exclusive per
asset: overlapping epochs for the same asset are rejected, even for different
accounts. Different assets may be owned at the same time. Adjacent epochs may
share a boundary.

The constraint groups by relationship properties, not endpoints. Each write must
set `r.assetId` to the destination asset's ID. Grouping by account instead would
enforce a different rule: one asset per account at a time.

### Record the first owner

Run this after declaring the constraints, in an empty example database:

<!-- temporal-example: ownership-first -->
```cypher
CREATE (alice:Account {id:'alice'}), (bob:Account {id:'bob'}),
       (asset:Asset {id:'asset-42'})
CREATE (alice)-[:OWNS {
  assetId:asset.id,
  starts_at:datetime('2026-01-01T00:00:00Z'), ends_at:null
}]->(asset);
```

Alice owns `asset-42` from January 1 onward. Bob exists but has no ownership epoch.
The identifiers and property names are application choices, not reserved names.

### Transfer ownership

<!-- temporal-example: ownership-transfer -->
```cypher
MATCH (:Account {id:'alice'})-[old:OWNS]->(asset:Asset {id:'asset-42'}),
      (next:Account {id:'bob'})
WHERE old.ends_at IS NULL
WITH old, asset, next, datetime('2026-02-01T00:00:00Z') AS boundary
SET old.ends_at=boundary
CREATE (next)-[:OWNS {
  assetId:asset.id, starts_at:boundary, ends_at:null
}]->(asset);
```

Submit this as one statement: closing Alice's epoch and opening Bob's commit
together. Both use the same boundary, so there is no gap or overlap. Alice's
relationship remains in the graph. If the expected open ownership or next account
is missing, `MATCH` returns no rows and nothing changes; the application must
check that the transfer occurred. Handle concurrent transaction conflicts through
the normal retry contract.

This preserves ownership history, but closing an epoch changes its end property.
It is not an immutable event log. Use explicit change events when that is needed.

### Find the owner at a time

<!-- temporal-example: ownership-at -->
```cypher
MATCH (owner:Account)-[r:OWNS]->(:Asset {id:'asset-42'})
WHERE (r.starts_at IS NULL OR r.starts_at <= datetime('2026-01-15T00:00:00Z'))
  AND (r.ends_at IS NULL OR datetime('2026-01-15T00:00:00Z') < r.ends_at)
RETURN owner.id AS owner;
```

After the transfer, this returns `alice`. February 1 returns `bob`. These queries
read the explicit epochs in the current graph; they do not require MVCC history.
Deleting an ownership relationship deletes that application history.

### Keep database history separate

<!-- temporal-example: ownership-recorded -->
```cypher
CALL db.temporal.asOf(
  $recordedAt,
  'MATCH (owner:Account)-[r:OWNS]->(:Asset {id:$id})
   RETURN owner.id AS owner, r.starts_at AS starts_at, r.ends_at AS ends_at',
  {id:'asset-42'}
) YIELD row
RETURN row;
```

`$recordedAt` asks what the database had recorded at that instant. It does not
select the owner by the epoch's bounds; add that predicate inside the query when
both questions matter. The constraint, the epoch-selection query and `asOf` are
independent.

`asOf` requires retained MVCC versions. It excludes currently deleted entities,
never crosses deletion/recreation boundaries, and has no historical embedding
vectors. Explicit ownership epochs last as long as you keep their relationships;
MVCC history follows its own retention policy. See
[Historical Reads & MVCC Retention](historical-reads-mvcc-retention.md).

### Record an owner whose start predates your records

<!-- temporal-example: ownership-open-past -->
```cypher
MATCH (alice:Account {id:'alice'})
CREATE (asset:Asset {id:'legacy-asset'})
CREATE (alice)-[:OWNS {
  assetId:asset.id, starts_at:null,
  ends_at:datetime('2026-02-01T00:00:00Z')
}]->(asset);
```

This reserves every instant before February 1 for Alice. It is an explicit claim
about the past, not merely an unknown start. It rejects competing earlier claims
for the asset. It does not claim that the database already contained this record
then; `asOf` remains limited to actual retained commits.

<!-- temporal-example: ownership-past-query -->
```cypher
MATCH (owner:Account)-[r:OWNS]->(:Asset {id:'legacy-asset'})
WHERE (r.starts_at IS NULL OR r.starts_at <= datetime('2020-01-01T00:00:00Z'))
  AND (r.ends_at IS NULL OR datetime('2020-01-01T00:00:00Z') < r.ends_at)
RETURN owner.id AS owner;
```

Returns `alice`. The start check must include `IS NULL`; a direct comparison with
null does not match. Bob can take over at exactly February 1 with a new open-ended
epoch. Both-null bounds are rejected: one relationship cannot reserve all time.
Overlapping claims are rejected even when both relationships have the same owner.

### Adapt the pattern

Use `EMPLOYED_BY`, `ASSIGNED_TO` or another relationship type for your domain.
Choose the grouping tuple to express what must be exclusive. For example,
`GROUP BY r.tenantId, r.assetId` scopes exclusivity by tenant and asset. Every
component must match before two epochs compete. To require datetime-typed properties rather than accepting the supported string
and numeric time inputs, add [type checks](managing-constraints.md#require-datetime-typed-epoch-bounds).

Relationship epoch validation currently scans the relevant relationship type;
an ordinary property index does not turn it into a neighboring-epoch lookup.
See [algorithmic costs and measured limits](../performance/temporal-read-and-constraint-costs.md).

## Alternative: a generic fact ledger

Use explicit fact nodes when many predicates share one schema, when facts need
independent links, or when separate mutation events are required:

```text
Entity ──HAS_FACT──> FactKey ──HAS_VERSION──> FactVersion
                       └────CURRENT──────> FactVersion
MutationEvent ──AFFECTS───────────────────> FactVersion
```

`FactKey` names the stable `(subject, predicate)` slot. `FactVersion` contains one
assertion. `CURRENT` is an optional shortcut; replacing it requires coordination
with the version update. This additional structure is not required by `asOf` or
by epoch constraints.

### Declare the slot and its exclusive epochs

<!-- temporal-example: fact-key -->
```cypher
CREATE CONSTRAINT fact_key IF NOT EXISTS
FOR (k:FactKey) REQUIRE (k.subject, k.predicate) IS NODE KEY;
```

<!-- temporal-example: fact-epochs -->
```cypher
CREATE CONSTRAINT fact_epochs IF NOT EXISTS
FOR (v:FactVersion)
REQUIRE temporal.epoch(v.starts_at, v.ends_at) GROUP BY v.subject, v.predicate;
```

### Record and replace an assertion

<!-- temporal-example: fact-first -->
```cypher
CREATE (e:Entity {id:'product-123'})
CREATE (e)-[:HAS_FACT]->(k:FactKey {subject:e.id, predicate:'price'})
CREATE (v:FactVersion {
  subject:e.id, predicate:'price', amount:99.99, currency:'USD',
  starts_at:datetime('2026-01-01T00:00:00Z'), ends_at:null, actor:'alice'
})
CREATE (k)-[:CURRENT]->(v)
CREATE (k)-[:HAS_VERSION]->(v);
```

<!-- temporal-example: fact-replace -->
```cypher
MATCH (k:FactKey {subject:'product-123', predicate:'price'})-[current:CURRENT]->(old:FactVersion)
WHERE old.ends_at IS NULL
WITH k, old, current, datetime('2026-02-01T00:00:00Z') AS boundary
SET old.ends_at=boundary
DELETE current
CREATE (next:FactVersion {
  subject:k.subject, predicate:k.predicate, amount:89.99, currency:'USD',
  starts_at:boundary, ends_at:null, actor:'bob'
})
CREATE (k)-[:CURRENT]->(next)
CREATE (k)-[:HAS_VERSION]->(next);
```

Run the replacement as one statement. It closes the previous epoch, removes the
old pointer and creates the next assertion with one new pointer. The previous
`HAS_VERSION` relationship remains. No row matched means no change.

<!-- temporal-example: fact-current -->
```cypher
MATCH (:FactKey {subject:'product-123', predicate:'price'})-[:CURRENT]->(v:FactVersion)
RETURN v.amount AS amount;
```

This returns `89.99`; following `HAS_VERSION` instead exposes both assertions.
The epoch key is only `(subject, predicate)`, so this example permits one price
at a time, regardless of currency. Add currency to the grouping tuple if each
currency needs an independent timeline.

### Add change records only when needed

<!-- temporal-example: fact-event -->
```cypher
MATCH (:FactKey {subject:'product-123', predicate:'price'})-[:CURRENT]->(v:FactVersion)
CREATE (:MutationEvent {id:'change-2', actor:'bob', action:'price-changed'})-[:AFFECTS]->(v);
```

In an application, write the event in the same transaction as the replacement.
The separate statement above demonstrates the link only. An event ID is an
application value, not a WAL transaction ID. Treat these events as append-only
by application policy; the model itself does not forbid edits or deletion.

Native transaction receipts can correlate a write with retained WAL records;
they do not make the graph tamper-proof. See [Transactions](transactions.md) and
[WAL lifecycle](../operations/wal-compaction.md) for durability and retention.
Vector search is optional and current-only; see [Vector Search](vector-search.md).
The [older bootstrap](../examples/canonical-bootstrap.cypher) demonstrates a
larger schema; it is not required for either pattern here.

For mixed primitive and boolean checks, including an epoch entry with its own
inline grouping, see [Constraint blocks](managing-constraints.md#epoch-entries-with-their-own-grouping).
