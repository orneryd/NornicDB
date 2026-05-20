# Multi-Database Support

NornicDB supports multiple isolated databases within a single storage backend, similar to Neo4j 4.x.

## Overview

Multi-database support enables:

- **Complete data isolation** between databases
- **Application-level multi-tenancy** - each database can be used as an isolation boundary
- **Neo4j 4.x compatibility** - works with existing Neo4j drivers and tools
- **Shared storage backend** - efficient resource usage

## Qdrant gRPC collections

NornicDB’s Qdrant-compatible gRPC API maps **collections to databases** (namespace isolation).

- Guide: `docs/user-guides/qdrant-grpc.md`
- Architecture: `docs/architecture/qdrant-collection-to-database-diagrams.md`

## Default Database

By default, NornicDB uses **`"nornic"`** as the default database name (Neo4j uses `"neo4j"`).

This is configurable:

**Config File:**

```yaml
database:
  default_database: "custom"
```

**Environment Variable:**

```bash
export NORNICDB_DEFAULT_DATABASE=custom
# Or Neo4j-compatible:
export NEO4J_dbms_default__database=custom
```

**Configuration Precedence:**

1. CLI arguments (highest priority)
2. Environment variables
3. Config file
4. Built-in defaults (`"nornic"`)

## Using Multiple Databases

### Creating Databases

```cypher
CREATE DATABASE tenant_a
CREATE DATABASE tenant_b
```

### Listing Databases

```cypher
SHOW DATABASES
```

### Dropping Databases

```cypher
DROP DATABASE tenant_a
```

### Switching Databases

**In Cypher Shell:**

```cypher
:USE tenant_a
```

**In Drivers:**

```python
# Python
driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    database="tenant_a"
)

# JavaScript
const driver = neo4j.driver(
    "bolt://localhost:7687",
    neo4j.auth.basic("admin", "password"),
    { database: "tenant_a" }
)
```

**HTTP API:**

```
POST /db/tenant_a/tx/commit
```

### Discovery Endpoint

The discovery endpoint (`GET /`) returns information about the server, including the default database name:

```bash
curl http://localhost:7474/
```

**Response:**

```json
{
  "bolt_direct": "bolt://localhost:7687",
  "bolt_routing": "neo4j://localhost:7687",
  "transaction": "http://localhost:7474/db/{databaseName}/tx",
  "neo4j_version": "5.0.0",
  "neo4j_edition": "community",
  "default_database": "nornic"
}
```

**Note:** The `default_database` field is a NornicDB extension that helps clients automatically determine which database to use by default. This is particularly useful for UI clients and automated tools that need to connect without hardcoding database names.

## Data Isolation

Each database is completely isolated:

```cypher
// In tenant_a
CREATE (n:Person {name: "Alice"})

// In tenant_b
CREATE (n:Person {name: "Bob"})

// tenant_a only sees Alice
// tenant_b only sees Bob
```

## System Database

The system database (`"system"`) is a special database used for NornicDB's internal metadata and user accounts. It is not accessible to users for regular queries but is used by the system for:

- **Database metadata** - Information about all databases (names, creation dates, etc.)
- **User accounts** - All authentication user accounts are stored here
- **System configuration** - Internal system settings

**User Storage:**

- All user accounts are stored as nodes with labels `["_User", "_System"]` in the system database
- Users are automatically loaded from the system database on server startup
- User accounts are included in database backups automatically
- Internal database IDs are never exposed in API responses for security

See **[User Storage in System Database](#system-database)** for details.

The system database is:

- Automatically created
- Not accessible to users for regular queries (internal use only)
- Cannot be dropped

## Automatic Migration

When you upgrade to NornicDB with multi-database support, **existing data is automatically migrated** to the default database namespace on first startup.

**What happens:**

- On first startup, NornicDB detects any data without namespace prefixes
- All unprefixed nodes and edges are automatically migrated to the default database (`"nornic"` by default)
- All indexes are automatically updated
- Migration status is saved - it only runs once
- Your existing data remains fully accessible through the default database

**No action required** - migration happens automatically and transparently.

**Example:**

```cypher
// Before upgrade: data stored as "node-123"
// After upgrade: automatically becomes "nornic:node-123"
// You access it the same way - no changes needed!
MATCH (n) RETURN n
```

## Backwards Compatibility

✅ **Fully backwards compatible:**

- Existing code without database parameter works with default database
- All existing data automatically migrated and accessible in default database
- No breaking changes to existing APIs
- No manual migration steps required

## Configuration Examples

### Default Configuration

```yaml
database:
  default_database: "nornic" # Default
```

### Custom Default Database

```yaml
database:
  default_database: "main"
```

### Environment Variable Override

```bash
export NORNICDB_DEFAULT_DATABASE=production
./nornicdb serve
```

## Database Aliases

Database aliases allow you to create alternate names for databases, making database management and migration easier.

### Creating Aliases

```cypher
CREATE ALIAS main FOR DATABASE tenant_primary_2024
CREATE ALIAS prod FOR DATABASE production_v2
CREATE ALIAS current FOR DATABASE v1.2.3
```

### Using Aliases

Aliases work exactly like database names - you can use them anywhere a database name is expected:

**In Cypher Shell:**

```cypher
:USE main
MATCH (n) RETURN n
```

**In Drivers:**

```python
# Python
driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    database="main"  # Uses alias
)
```

**HTTP API:**

```
POST /db/main/tx/commit
```

**Bolt Protocol:**
The `database` parameter in HELLO messages accepts aliases.

### Listing Aliases

```cypher
-- List all aliases
SHOW ALIASES

-- List aliases for a specific database
SHOW ALIASES FOR DATABASE tenant_primary_2024
```

### Dropping Aliases

```cypher
DROP ALIAS main
DROP ALIAS main IF EXISTS  -- No error if alias doesn't exist
```

### Alias Rules

- **Unique**: Each alias must be unique across all databases
- **No Conflicts**: Aliases cannot conflict with existing database names
- **Reserved Names**: Cannot create aliases for reserved names (`system`, `nornic`)
- **Direct Only**: Aliases point directly to database names (no alias chains)

### Use Cases

- **Database Renaming**: Create alias while migrating to new name
- **Environment Mapping**: `prod` → `production_v2`
- **Version Management**: `current` → `v1.2.3`
- **Simplified Access**: `main` → `tenant_primary_2024`

## Per-Database Resource Limits

Resource limits allow administrators to control resource usage per database, preventing any single database from consuming excessive resources.

### Setting Limits

Limits are configured using Cypher commands:

```cypher
-- Set storage limits
ALTER DATABASE tenant_a SET LIMIT max_nodes = 1000000
ALTER DATABASE tenant_a SET LIMIT max_edges = 5000000
ALTER DATABASE tenant_a SET LIMIT max_bytes = 10737418240  -- 10GB

-- Set query limits
ALTER DATABASE tenant_a SET LIMIT max_query_time = '60s'
ALTER DATABASE tenant_a SET LIMIT max_results = 10000
ALTER DATABASE tenant_a SET LIMIT max_concurrent_queries = 10

-- Set connection limits
ALTER DATABASE tenant_a SET LIMIT max_connections = 50

-- Set rate limits
ALTER DATABASE tenant_a SET LIMIT max_queries_per_second = 100
ALTER DATABASE tenant_a SET LIMIT max_writes_per_second = 50
```

### Viewing Limits

```cypher
-- Show all limits for a database
SHOW LIMITS FOR DATABASE tenant_a
```

### Limit Types

1. **Storage Limits**:
   - `max_nodes`: Maximum number of nodes (0 = unlimited)
   - `max_edges`: Maximum number of edges (0 = unlimited)
   - `max_bytes`: Maximum storage size in bytes (0 = unlimited)
     - **Enforced with exact size calculation**: The actual serialized size of each node/edge is calculated using gob encoding (matching the storage format)
     - **No estimation**: Storage size is tracked incrementally for accurate, O(1) limit checks
     - **Clear error messages**: When exceeded, operations fail with: "would exceed max_bytes limit (current: X bytes, limit: Y bytes, new entity: Z bytes)"

2. **Query Limits**:
   - `max_query_time`: Maximum query execution time (0 = unlimited)
   - `max_results`: Maximum number of results returned (0 = unlimited)
   - `max_concurrent_queries`: Maximum concurrent queries (0 = unlimited)

3. **Connection Limits**:
   - `max_connections`: Maximum concurrent connections (0 = unlimited)

4. **Rate Limits**:
   - `max_queries_per_second`: Maximum queries per second (0 = unlimited)
   - `max_writes_per_second`: Maximum writes per second (0 = unlimited)

### Default Limits

By default, all limits are **unlimited** (0). You must explicitly set limits for databases that need them.

### Limit Persistence

Limits are **fully persisted** to disk as part of database metadata:

- Limits are saved immediately when set
- Limits survive server restarts
- Limits are automatically loaded on startup
- Limits are stored in the system database alongside other metadata

### Limit Enforcement

All limits are enforced at runtime with clear, actionable error messages:

**MaxNodes/MaxEdges**: When the count limit is reached, create operations fail with:

```
storage limit exceeded: database 'tenant_a' has reached max_nodes limit (1000/1000)
```

**MaxBytes**: When the size limit would be exceeded, create operations fail with:

```
storage limit exceeded: database 'tenant_a' would exceed max_bytes limit
(current: 500 bytes, limit: 1024 bytes, new entity: 600 bytes)
```

**MaxBytes Implementation Details**:

- Uses **exact size calculation**, not estimation
- Calculates the actual serialized size of each node/edge using gob encoding
- Tracks storage size incrementally (initialized lazily on first access)
- Provides O(1) limit checks without recalculating from all entities
- Error messages include current size, limit, and the size of the entity being created

### Example: MaxBytes Enforcement

```cypher
-- Set a 1GB storage limit
ALTER DATABASE tenant_a SET LIMIT max_bytes = 1073741824

-- Create nodes normally
CREATE (n:User {name: "Alice", email: "alice@example.com"})

-- If a node would exceed the limit, creation fails with a clear error
CREATE (n:Document {content: "..."})  -- Fails if this would exceed 1GB
-- Error: storage limit exceeded: database 'tenant_a' would exceed max_bytes limit
--        (current: 1073741800 bytes, limit: 1073741824 bytes, new entity: 100 bytes)
```

### Use Cases

- **Fair Resource Allocation**: Ensure no tenant monopolizes resources
- **Cost Control**: Limit storage per tenant with exact byte-level precision
- **Performance Protection**: Prevent slow queries from affecting other databases
- **Compliance**: Enforce data retention limits
- **Storage Quotas**: Enforce exact storage quotas per database (e.g., 10GB per tenant)

## Composite Databases

Composite databases (similar to Neo4j Fabric) allow you to create a virtual database that spans multiple physical databases. Queries against a composite database transparently access data from all constituent databases, providing a unified view without explicit database references.

If you want to use composite databases as a Neo4j-style distributed graph or "infinigraph" topology, see `docs/user-guides/infinigraph-topology.md` for topology design guidance, remote-auth choices, and cross-constituent modeling patterns.

### Creating Composite Databases

```cypher
-- Create a composite database with multiple constituents
CREATE COMPOSITE DATABASE analytics
  ALIAS tenant_a FOR DATABASE tenant_a
  ALIAS tenant_b FOR DATABASE tenant_b
  ALIAS tenant_c FOR DATABASE tenant_c
```

### Querying Composite Databases

Composite databases require explicit graph targeting using `USE <composite>.<alias>` to direct
queries to specific constituents. Plain queries (MATCH, CREATE, MERGE, etc.) on the composite
root are rejected — this matches Neo4j Fabric semantics.

```cypher
-- Switch to composite database
:USE analytics

-- Target a specific constituent using USE
CALL {
  USE analytics.tenant_a
  MATCH (n:Person)
  RETURN n.name AS name
}
RETURN name

-- Write to a specific constituent
CALL {
  USE analytics.tenant_a
  CREATE (n:Person {name: "Alice"})
  RETURN n
}
RETURN n

-- Query across multiple constituents (fan-out)
CALL {
  USE analytics.tenant_a
  MATCH (n:Person) RETURN n.name AS name
}
CALL {
  USE analytics.tenant_b
  MATCH (n:Person) RETURN n.name AS name
}
RETURN name
```

**Important:** Plain queries without `USE` on a composite database will fail:

```cypher
-- This is REJECTED on composite databases:
:USE analytics
MATCH (n:Person) RETURN n
-- Error: Queries on composite databases require explicit graph targeting.
--        Use USE <composite>.<alias> to target a specific constituent
```

### Managing Composite Databases

```cypher
-- Show all composite databases
SHOW COMPOSITE DATABASES

-- Show constituents of a composite database
SHOW CONSTITUENTS FOR COMPOSITE DATABASE analytics

-- Drop a composite database
DROP COMPOSITE DATABASE analytics
```

**Note:** Dropping a composite database does not affect the constituent databases - they remain intact.

### Constituent Management

```cypher
-- Add a constituent to an existing composite database
ALTER COMPOSITE DATABASE analytics
  ADD ALIAS tenant_d FOR DATABASE tenant_d

-- Remove a constituent
ALTER COMPOSITE DATABASE analytics
  DROP ALIAS tenant_c
```

### Use Cases

- **Analytics Across Tenants**: Aggregate data from multiple tenant databases
- **Unified Reporting**: Generate reports across multiple databases as if they were one
- **Data Federation**: Query distributed data transparently
- **Multi-Region Queries**: Access data from databases in different regions

### Schema DDL on Composite Databases

Schema commands (CREATE/DROP INDEX, CREATE/DROP CONSTRAINT, SHOW INDEXES, SHOW CONSTRAINTS)
must target a specific constituent — they cannot run on the composite root. This matches
Neo4j Fabric semantics.

```cypher
-- REJECTED: schema DDL on composite root
:USE analytics
SHOW INDEXES
-- Error: SHOW INDEXES on composite databases requires a constituent target.
--        Use USE <composite>.<alias> SHOW INDEXES

CREATE INDEX idx_name FOR (p:Person) ON (p.name)
-- Error: Schema DDL on composite databases requires a constituent target.
--        Use USE <composite>.<alias> to target a specific constituent

DROP INDEX idx_name
-- Error: Schema DDL on composite databases requires a constituent target.

-- CORRECT: target a specific constituent
:USE analytics.tenant_a
CREATE INDEX idx_name FOR (p:Person) ON (p.name)
SHOW INDEXES
DROP INDEX idx_name
DROP INDEX idx_name IF EXISTS  -- silent no-op if index doesn't exist
```

#### HTTP Transaction API Example (Composite + USE)

Schema operations over HTTP follow the same rule: the statement must target a constituent.

```bash
# Rejected: composite-root SHOW INDEXES
curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"SHOW INDEXES"}]}' \
  "http://localhost:7474/db/analytics/tx/commit"

# Accepted: constituent-scoped schema DDL/introspection
curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"USE analytics.tenant_a CREATE INDEX idx_name FOR (p:Person) ON (p.name)"}]}' \
  "http://localhost:7474/db/analytics/tx/commit"

curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"USE analytics.tenant_a SHOW INDEXES"}]}' \
  "http://localhost:7474/db/analytics/tx/commit"
```

### Schema Merging (Read-Only View)

The `CompositeEngine.GetSchema()` method merges schemas from all constituents for
programmatic access. This merged view is read-only metadata:

- **Constraints**: All constraint types (UNIQUE, NODE_KEY, EXISTS) are merged from all constituents
- **Indexes**: All index types are merged:
  - Property indexes (single property)
  - Composite indexes (multiple properties)
  - Full-text indexes
  - Vector indexes
  - Range indexes
- **Deduplication**: If multiple constituents have indexes/constraints with the same name, only one is shown
- **Metadata Only**: The merged schema shows metadata only — actual indexed data remains in constituent databases

### How It Works

1. **Explicit Graph Targeting**: Queries use `USE <composite>.<alias>` to target specific constituents (Neo4j Fabric semantics)
2. **Fabric Planner Decomposition**: Queries with `USE` clauses are decomposed into fragments at USE-clause boundaries by the Fabric planner
3. **Many-Read/One-Write**: Reads can span any number of constituents; only one constituent may receive writes per transaction
4. **Schema Merging**: Constraints and indexes from all constituents are merged into a unified metadata view (read-only)

### Remote Constituents

Composite databases support remote constituents directly in Cypher. A remote constituent points at another NornicDB server and is queried as part of the same composite route.

For a full coordinator-plus-shards design guide, including proxy-ID joins and recommended topology patterns, see `docs/user-guides/infinigraph-topology.md`.

#### Remote Constituent Fields

- `alias`: local name used in the composite catalog.
- `database_name`: target database name on the remote server.
- `type`: must be `remote`.
- `access_mode`: `read`, `write`, or `read_write`.
- `uri`: remote service base URL (for example: `https://remote-host/nornic-db`).
- `secret_ref` (optional): logical secret reference for your deployment automation (metadata only; not runtime-resolved by query execution).
- `auth_mode`: `oidc_forwarding` (default) or `user_password`.
- `user`/`password`: explicit remote credentials when `auth_mode` is `user_password`.

#### Authentication and Identity Propagation

When a query is executed against a composite database with remote constituents:

1. NornicDB reads the caller `Authorization` header from the incoming request.
2. NornicDB forwards that header to remote constituent requests.
3. The remote server evaluates auth/RBAC for the same caller identity.

This preserves service-principal or user identity across Fabric-style fan-out.

#### Define Remote Constituents in Cypher

```cypher
CREATE COMPOSITE DATABASE nornic
  ALIAS tr FOR DATABASE nornic_tr
    AT "https://shard-a.example/nornic-db"
    OIDC CREDENTIAL FORWARDING
    TYPE remote
    ACCESS read_write
  ALIAS txt FOR DATABASE nornic_txt
    AT "https://shard-b.example/nornic-db"
    USER "svc-nornic"
    PASSWORD "svc-password"
    TYPE remote
    ACCESS read_write
```

```cypher
ALTER COMPOSITE DATABASE nornic
  ADD ALIAS rx FOR DATABASE nornic_rx
    AT "https://shard-c.example/nornic-db"
    SECRET REF "spn-nornic-c"
    OIDC CREDENTIAL FORWARDING
    TYPE remote
    ACCESS read
```

```cypher
SHOW CONSTITUENTS FOR COMPOSITE DATABASE nornic
```

Result columns include `alias`, `database`, `type`, `access_mode`, `uri`, `secret_ref`, `auth_mode`, `user`.

#### Remote Auth Behavior (Neo4j-Compatible)

- `AT '<url>' USER <user> PASSWORD '<password>'`:
  uses explicit Basic auth to the remote constituent.
- `AT '<url>' OIDC CREDENTIAL FORWARDING`:
  forwards the caller `Authorization` header to the remote constituent.
- `AT '<url>'` with no explicit auth clause:
  defaults to OIDC credential forwarding.
- `USER/PASSWORD` and `OIDC CREDENTIAL FORWARDING` cannot be combined in one constituent clause.

#### Remote Credential Encryption Key Selection

For remote constituents using `USER/PASSWORD`, the password is encrypted before metadata persistence.

Key selection order on the coordinator:

1. `NORNICDB_REMOTE_CREDENTIALS_KEY` (recommended, dedicated key)
2. `NORNICDB_ENCRYPTION_PASSWORD` / `database.encryption_password`
3. `NORNICDB_AUTH_JWT_SECRET` / `auth.jwt_secret`

Notes:

- `NORNICDB_REMOTE_CREDENTIALS_KEY` is a separate key and **overrides** fallback values.
- If fallback key sources are used, the server logs a warning at startup.
- JWT-secret fallback is supported for compatibility, but dedicated key separation is more secure.

#### End-to-End Example

Coordinator: `https://coordinator.example/nornic-db`  
Remote shard A: `https://shard-a.example/nornic-db`  
Remote shard B: `https://shard-b.example/nornic-db`

Create/query data directly on remote shards first:

```bash
curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"CREATE (n:Translation {id:\"tr-1\", textKey:\"WELCOME\"})"}]}' \
  "https://shard-a.example/nornic-db/db/nornic_tr/tx/commit"

curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"CREATE (n:TranslationText {translationId:\"tr-1\", locale:\"en-US\", value:\"Welcome\"})"}]}' \
  "https://shard-b.example/nornic-db/db/nornic_txt/tx/commit"
```

#### Verification Query (Composite Read)

```cypher
:USE nornic
CALL {
  USE nornic.tr
  MATCH (n) RETURN labels(n) AS labels
}
CALL {
  USE nornic.txt
  MATCH (n) RETURN labels(n) AS labels
}
RETURN labels, count(*) AS c
ORDER BY labels
```

Expected: rows from both remote constituents appear in one result stream.

#### Fabric-Style Subquery Routing (USE inside CALL)

You can route each subquery to a specific constituent alias:

```cypher
USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey
}
CALL {
  USE nornic.txt
  MATCH (tt:TranslationText)
  RETURN count(tt) AS textCount
}
RETURN translationId, textKey, textCount
```

Behavior:

- The outer query context runs on `nornic`.
- Each `CALL { USE nornic.<alias> ... }` block executes on that constituent.
- Results are merged in outer query order.
- The same semantics are used across Bolt, HTTP transaction API, and GraphQL execution paths.

#### Transaction Behavior for Composite/Remote

- Explicit transactions (`BEGIN`/`COMMIT`/`ROLLBACK`) are supported through the standard protocol transaction APIs.
- Distributed writes are constrained to one write shard per transaction (many-read/one-write model).
- Attempting writes on a second shard in the same transaction returns a deterministic transaction error.
- Identity/auth context is forwarded for remote constituent execution when OIDC credential forwarding is configured.
- For local constituents, explicit transactions use real per-constituent subtransactions with commit/rollback durability.
- For remote constituents, explicit transactions now use real remote transaction handles (`/db/{db}/tx` open, statement execution, commit/rollback) bound to the distributed transaction coordinator lifecycle.

#### Search Behavior on Composite Roots

Search endpoints do not execute on composite roots. This matches strict graph-target semantics.

- `POST /nornicdb/search` with `database=<composite>`: rejected
- `POST /nornicdb/search/rebuild` with `database=<composite>`: rejected
- `POST /nornicdb/similar` with `database=<composite>`: rejected

Use a constituent database instead (for example `analytics.tenant_a` as the database target).

#### Composite Database Stats Provenance

`GET /db/{composite}` returns deterministic constituent provenance fields for aggregated stats:

- `statsAggregation`: always `constituent_sum` for composite databases
- `statsPartial`: `true` when one or more constituent stats could not be collected
- `statsProvenance`: alias-sorted list of per-constituent stats (`alias`, `database`, `type`, `nodeCount`, `edgeCount`, `nodeStorageBytes`, `managedEmbeddingBytes`, search readiness/progress fields)

This ensures aggregate counts are traceable to constituent databases and stable across calls.

#### Composite Schema Operations Over HTTP (Neo4j-Compatible Targeting)

Schema operations on composites must target a constituent via `USE <composite>.<alias>`.

Create and inspect a unique constraint on a constituent:

```bash
curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"USE nornic.tr CREATE CONSTRAINT tr_id_unique FOR (n:Translation) REQUIRE n.id IS UNIQUE"}]}' \
  "http://localhost:7474/db/nornic/tx/commit"

curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"USE nornic.tr SHOW CONSTRAINTS"}]}' \
  "http://localhost:7474/db/nornic/tx/commit"
```

Drop the constraint:

```bash
curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"USE nornic.tr DROP CONSTRAINT tr_id_unique"}]}' \
  "http://localhost:7474/db/nornic/tx/commit"
```

Composite-root schema introspection/DDL is rejected:

```bash
curl -s -u admin:password \
  -H "Content-Type: application/json" \
  -d '{"statements":[{"statement":"SHOW CONSTRAINTS"}]}' \
  "http://localhost:7474/db/nornic/tx/commit"
```

Expected: Neo4j-style client semantic error instructing you to target a constituent graph.

Example: valid explicit transaction (one write shard, multi-shard reads)

```cypher
BEGIN
CALL {
  USE nornic.tr
  CREATE (t:Translation {id: "tr-tx-1", textKey: "ORDERS_WHERE"})
  RETURN count(t) AS c
}
RETURN c
COMMIT
```

Example: rejected explicit transaction (write on second shard in same tx)

```cypher
BEGIN
CALL {
  USE nornic.tr
  CREATE (t:Translation {id: "tr-tx-2"})
  RETURN count(t) AS c
}
RETURN c
CALL {
  USE nornic.txt
  CREATE (tt:TranslationText {translationId: "tr-tx-2", locale: "en-US", value: "Where are my orders?"})
  RETURN count(tt) AS c
}
RETURN c
```

Expected error:

`Neo.ClientError.Transaction.ForbiddenDueToTransactionType: Writing to more than one database per transaction is not allowed`

#### Troubleshooting

`Neo.ClientError.Database.General: failed to get remote storage for constituent ... remote engine factory is not configured`

- The coordinator was started without remote-engine factory wiring.

`Neo.ClientError.Database.General: ... dial failed ...`

- `uri` is unreachable, TLS/DNS/network issue, or remote server unavailable.

`Neo.ClientError.Security.Forbidden` from a remote operation

- Caller token was forwarded, but that principal lacks permissions on the remote shard database.

No `Authorization` header on incoming request

- Query can still execute if remote shard allows anonymous/basic access; otherwise auth fails remotely.

`remote user/password auth requires remote credential encryption key configuration`

- Set one of:
  - `NORNICDB_REMOTE_CREDENTIALS_KEY` (recommended)
  - `NORNICDB_ENCRYPTION_PASSWORD`
  - `NORNICDB_AUTH_JWT_SECRET`

### Current Constraints

- One transaction can write to only one constituent shard (many-read/one-write model).
- Routing rule customization is currently limited compared to full custom planner policies.
- Cross-shard relationship semantics depend on explicit query patterns and available IDs.

## Per-database search configuration

Each database can have its BM25 fulltext and vector ANN indexes independently enabled, disabled, eagerly built, or lazily warmed. Per-database overrides always win over the global default in **both directions** — an override of `true` turns on a globally-disabled index, an override of `false` turns off a globally-enabled one.

Four keys control this:

| Key                              | Type    | Default   |
| -------------------------------- | ------- | --------- |
| `NORNICDB_SEARCH_BM25_ENABLED`   | boolean | `true`    |
| `NORNICDB_SEARCH_BM25_WARMING`   | enum    | `startup` |
| `NORNICDB_SEARCH_VECTOR_ENABLED` | boolean | `true`    |
| `NORNICDB_SEARCH_VECTOR_WARMING` | enum    | `startup` |

Resolution order (highest → lowest precedence):

1. Per-database override stored via `PUT /admin/databases/{name}/config` (admin API edits are authoritative).
2. yaml `databases:` map seed (read once on first boot if no admin edit exists for that key).
3. Global default (env var, CLI flag, or yaml `memory:` block).

Example yaml configuration:

```yaml
memory:
  search_bm25_enabled:   true
  search_bm25_warming:   startup
  search_vector_enabled: true
  search_vector_warming: startup

databases:
  hot_app_db: {}                                       # default startup, both enabled

  analytics:
    NORNICDB_SEARCH_BM25_ENABLED:   "false"
    NORNICDB_SEARCH_VECTOR_WARMING: "lazy"

  audit_logs:
    NORNICDB_SEARCH_BM25_ENABLED:   "false"
    NORNICDB_SEARCH_VECTOR_ENABLED: "false"            # search disabled

  exports_only:
    NORNICDB_SEARCH_BM25_ENABLED:   "true"
    NORNICDB_SEARCH_VECTOR_ENABLED: "false"            # write embeddings; never load in-process

  multi_tenant_archive:
    NORNICDB_SEARCH_BM25_WARMING:   "lazy"
    NORNICDB_SEARCH_VECTOR_WARMING: "lazy"             # cold; absorb cost on first query
```

See [Per-Database Search Index Flags](../operations/configuration.md#per-database-search-index-control) for the full configuration reference.

## See Also

- [Configuration Guide](../operations/configuration.md) - Configuration options for multi-database
- [Multi-Database Architecture](../architecture/multi-db-architecture.md) - Design notes for the multi-database surface (composite, aliases, per-DB limits)
- [Hybrid Search](hybrid-search.md) — search response shapes including per-DB flag fields
