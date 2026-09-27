# Multi-Database Architecture Reference

This document describes the design and shipping implementation of NornicDB's multi-database surface. For day-to-day usage see [Multi-Database User Guide](../user-guides/multi-database.md).

The features listed in this document are **all implemented**:

- Database aliases (`CREATE/DROP/SHOW ALIAS`).
- Per-database resource limits (`ALTER DATABASE … SET LIMIT …`, `SHOW LIMITS`).
- Composite databases with both **local** and **remote** constituents.
- Schema merging across constituents (constraints + property/composite/fulltext/vector/range indexes).
- Many-read / one-write distributed transaction semantics.
- OIDC credential forwarding for remote constituents.

Plain (non-composite) cross-database queries are **not** supported by design — composite databases are the supported route to query across multiple databases. Within a composite database, every query that touches a constituent must use a `USE <composite>.<alias>` Fabric subquery; plain `MATCH` on the composite root is rejected.

---

## Storage Layer

### Key-prefix namespacing

The base storage engine is shared across logical databases. Every key is prefixed with the database name (e.g. `nornic:node-uuid`) and a thin `NamespacedEngine` wrapper translates between the namespaced storage keys and the user-visible IDs.

This means:

- One BadgerDB process, many logical databases.
- Complete isolation: no cross-database data leakage at the storage layer.
- A single MVCC keyspace per logical database (so per-database lifecycle works independently).

### Database catalog

Database metadata lives in the `system` database under nodes labelled `_Database`, `_DatabaseAlias`, and `_DatabaseLimits`. The catalog is loaded into the `DatabaseManager` on startup and persisted on every mutation, so all server processes see the same view (relevant for HA standby and Raft deployments).

---

## Database Aliases

`CREATE ALIAS <alias> FOR DATABASE <db>` creates an alternate name. Resolution happens at every routing entry point:

- Bolt `HELLO`/`RUN` `database` extra
- HTTP `/db/{name}/tx/commit`
- Cypher `:USE <name>` and `USE <name>` clauses
- GraphQL `database` field

Each entry point resolves the alias to the canonical database name through `DatabaseManager.ResolveAlias` before the request reaches the executor or storage layer. Alias chains are explicitly disallowed — an alias can only point to a real database.

### Reserved names

Aliases cannot mask `system`, `nornic`, or any other reserved name, and an alias cannot collide with an existing database name. Conflict is detected at the catalog write before the alias is persisted.

---

## Per-Database Resource Limits

Limits are stored as part of the database catalog (`pkg/multidb/limits.go`) and applied at runtime by enforcement middleware (`pkg/multidb/enforcement.go`).

The full enforced set:

| Limit | Type | Effect |
|---|---|---|
| `max_nodes` | int | Cap on the node count for the database. |
| `max_edges` | int | Cap on the edge count for the database. |
| `max_bytes` | int | Cap on the cumulative serialized byte size of nodes + edges; tracked incrementally for O(1) checks. |
| `max_query_time` | duration string | Per-query execution timeout. |
| `max_results` | int | Per-query result-row cap. |
| `max_concurrent_queries` | int | Concurrency cap for the database. |
| `max_connections` | int | Concurrent client connection cap. |
| `max_queries_per_second` | int | Token-bucket rate limit for reads + writes. |
| `max_writes_per_second` | int | Separate cap for write operations only. |

When a limit is hit, the operation fails with a clear error including current usage, the configured cap, and (for `max_bytes`) the size of the entity being created.

---

## Composite Databases

Composite databases are NornicDB's sharding/federation surface. A composite is a virtual top-level database that routes Cypher subqueries to constituent physical databases. Every composite operation is expressed through `USE <composite>.<alias>` Fabric-style subqueries.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  Client Query                                    │
│       USE nornic                                                 │
│       CALL { USE nornic.tr ... }                                 │
│       CALL { USE nornic.txt ... }                                │
└────────────┬────────────────────────────────────────────────────┘
             │
   ┌─────────▼─────────┐
   │ Composite Engine  │  pkg/storage/composite_engine.go
   │ (read facade)     │  Reads route to all constituents and merge
   └─────────┬─────────┘
             │ subquery USE <alias> dispatch
   ┌─────────┼──────────────────────┐
   │         │                      │
┌──▼───┐  ┌──▼───┐               ┌──▼───────────┐
│ tr   │  │ txt  │               │ rx (remote)  │
│local │  │local │               │  HTTP /db    │
└──────┘  └──────┘               └──────────────┘
```

### Local vs remote constituents

`CREATE COMPOSITE DATABASE` accepts either a local `ALIAS <alias> FOR DATABASE <db>` clause or a remote variant with `AT '<url>'`:

```cypher
CREATE COMPOSITE DATABASE nornic
  ALIAS tr  FOR DATABASE nornic_tr
  ALIAS txt FOR DATABASE nornic_txt
    AT "https://shard-b.example/nornic-db"
    OIDC CREDENTIAL FORWARDING
    TYPE remote
    ACCESS read_write
```

Implementation files:

- `pkg/cypher/composite_commands.go` — DDL parser and validation.
- `pkg/multidb/composite.go` — composite catalog and constituent registry.
- `pkg/storage/composite_engine.go` — read facade that fans out across constituents and merges + dedupes results.
- `pkg/cypher/executor_fabric.go` — Fabric subquery executor that routes `USE <composite>.<alias>` blocks.

For remote constituents the HTTP `/db/{db}/tx/commit` endpoint is used as the wire protocol. Auth is forwarded via `OIDC CREDENTIAL FORWARDING` (the caller's `Authorization` header is propagated), or via `USER/PASSWORD` in the constituent definition.

### Distributed transactions

Composite write transactions follow the **many-read / one-write rule per transaction**: a single explicit transaction can read from any number of constituents but may only write to one. Attempting writes on a second constituent within the same transaction returns `Neo.ClientError.Statement.AccessMode` ("Writing to more than one database per transaction is not allowed"), as in Neo4j. A standard database is its own transaction domain: in an explicit transaction, a statement that targets another database with `USE` or `:USE` fails with the same code, and the transaction commits nothing from it.

For local constituents, explicit transactions run as real per-constituent subtransactions with full commit/rollback durability. For remote constituents they bind to the remote server's `/db/{db}/tx` lifecycle (open, statement execution, commit/rollback) under the distributed-transaction coordinator.

### Composite-root semantics

Plain queries on the composite root are rejected by design:

- `MATCH (n) RETURN n` on the composite root ⇒ `Neo.ClientError.Statement.NotSupported`.
- Schema introspection (`SHOW INDEXES`, `SHOW CONSTRAINTS`) on the composite root ⇒ same error, with a remediation message asking the caller to target a constituent via `USE <composite>.<alias>`.

To list merged schema metadata across all constituents, the storage facade exposes `CompositeEngine.GetSchema()` which returns a deduplicated read-only view.

### What does not transfer

- **Cross-constituent relationships.** A relationship lives entirely within one constituent's storage. Joining across constituents is done in user code via `USE <alias>` subqueries plus a property-key join.
- **Cross-constituent multi-write transactions.** Two-phase commit across constituents is not implemented; the many-read/one-write rule prevents the multi-write case from being silently lossy.
- **Composite-level vector indexes.** Each constituent owns its own vector index. Cross-shard semantic search uses `CALL { USE <alias> ... }` fan-out and merges in the outer query.
- **Plain cross-database queries.** Use composite databases instead.

### Routing decisions

Routing across constituents is **expressed in user code** through the explicit `USE <composite>.<alias>` clauses. The Go API exposes `RoutingStrategy` types in `pkg/multidb/routing.go` (`LabelRouting`, `PropertyRouting`, `FullScanRouting`) that programmatic users can plug into a custom dispatcher; **there is no Cypher DDL** for declarative composite routing today (no `ALTER COMPOSITE DATABASE ... SET ROUTING ...` syntax). Sharding strategy lives in your application's `USE <alias>` choice.

For a worked example of label-, hash-, and region-based sharding patterns over composite databases, see [Infinigraph Topology](../user-guides/infinigraph-topology.md).

---

## Operational Notes

### Backup, restore, and resource isolation

Each constituent of a composite database is an ordinary NornicDB database for backup, restore, MVCC lifecycle, and resource-limit purposes. The composite layer adds no separate persistent state beyond the catalog entries (composite name, constituent alias list, optional remote constituent URLs, optional encrypted remote credentials).

### Encryption of remote constituent credentials

Remote constituents using `USER/PASSWORD` auth have the password encrypted before it lands in the database catalog. Key selection on the coordinator follows this order:

1. `NORNICDB_REMOTE_CREDENTIALS_KEY` (recommended dedicated key).
2. `NORNICDB_ENCRYPTION_PASSWORD` / `database.encryption_password` (fallback).
3. `NORNICDB_AUTH_JWT_SECRET` / `auth.jwt_secret` (compatibility fallback; warning logged).

OIDC credential forwarding does not require any of these; only `USER/PASSWORD` does.

### Search behavior on composite roots

The HTTP search endpoints reject composite roots:

- `POST /nornicdb/search` with `database=<composite>` → 400.
- `POST /nornicdb/search/rebuild` with `database=<composite>` → 400.
- `POST /nornicdb/similar` with `database=<composite>` → 400.

Target a constituent database directly (e.g. `database=analytics.tenant_a`).

### Stats provenance

`GET /db/{composite}` returns aggregated stats with provenance fields:

- `statsAggregation`: always `constituent_sum` for composite databases.
- `statsPartial`: `true` when one or more constituent stats could not be collected.
- `statsProvenance`: alias-sorted list of per-constituent stats.

This makes aggregate counts traceable to their constituent sources and stable across calls.

---

## See Also

- [Multi-Database User Guide](../user-guides/multi-database.md) — operator surface
- [Multi-Database Implementation Spec](multi-db-implementation-spec.md) — feature inventory
- [Composite DB Analysis](composite-db-analysis.md) — capability comparison
- [Infinigraph Topology](../user-guides/infinigraph-topology.md) — sharded composite design pattern
- [Clustering Roadmap](clustering-roadmap.md) — overview of distribution surfaces (replication + composite sharding)
