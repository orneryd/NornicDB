# Multi-Database Support

NornicDB supports multiple isolated databases within a single storage backend, similar to Neo4j 4.x.

## Overview

Multi-database support enables:
- **Complete data isolation** between databases
- **Application-level multi-tenancy** - each database can be used as an isolation boundary
- **Neo4j 4.x compatibility** - works with existing Neo4j drivers and tools
- **Shared storage backend** - efficient resource usage

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

See **[User Storage in System Database](../../plans/user-storage-in-system-db.md)** for details.

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
  default_database: "nornic"  # Default
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

### Creating Composite Databases

```cypher
-- Create a composite database with multiple constituents
CREATE COMPOSITE DATABASE analytics
  ALIAS tenant_a FOR DATABASE tenant_a
  ALIAS tenant_b FOR DATABASE tenant_b
  ALIAS tenant_c FOR DATABASE tenant_c
```

### Querying Composite Databases

Once created, you can query a composite database just like any other database:

```cypher
-- Switch to composite database
:USE analytics

-- Query across all constituents
MATCH (n:Person)
RETURN count(n)  -- Counts across all tenant databases

-- Create nodes (routed to appropriate constituent)
CREATE (n:Person {name: "Alice", database_id: "db_a"})

-- Match across all constituents
MATCH (n:Person)
WHERE n.database_id = "db_a"
RETURN n
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

### Schema Merging

Composite databases automatically merge schemas (constraints and indexes) from all constituent databases:

```cypher
-- Each constituent can have its own indexes and constraints
-- In tenant_a:
CREATE INDEX ON :Person(name)
CREATE CONSTRAINT unique_person_email ON (p:Person) ASSERT p.email IS UNIQUE

-- In tenant_b:
CREATE INDEX ON :Company(name)
CREATE INDEX ON :Company(country, city)

-- Querying the composite database shows all merged schemas
SHOW INDEXES  -- Shows indexes from all constituents
SHOW CONSTRAINTS  -- Shows constraints from all constituents
```

**Schema Merging Details:**
- **Constraints**: All constraint types (UNIQUE, NODE_KEY, EXISTS) are merged from all constituents
- **Indexes**: All index types are merged:
  - Property indexes (single property)
  - Composite indexes (multiple properties)
  - Full-text indexes
  - Vector indexes
  - Range indexes
- **Deduplication**: If multiple constituents have indexes/constraints with the same name, only one is shown in the merged schema
- **Metadata Only**: The merged schema shows metadata only - actual indexed data remains in constituent databases

### How It Works

1. **Query Routing**: Queries are automatically routed to relevant constituents based on labels and properties
2. **Result Merging**: Results from all constituents are merged transparently (duplicates removed by ID)
3. **Write Routing**: Write operations are routed to the appropriate constituent based on routing rules
4. **Schema Merging**: Constraints and indexes from all constituents are merged into a unified schema view

### Limitations

- **Local Constituents Only**: Currently only supports local databases (remote constituents planned for future)
- **No Distributed Transactions**: Writes to multiple constituents are not atomic
- **Routing Configuration**: Advanced routing rules are not yet user-configurable (uses default hash-based routing)

## Limitations (v1)


## See Also

- [Configuration Guide](../operations/configuration.md) - Configuration options for multi-database
- [Future Features Plan](../architecture/MULTI_DB_FUTURE_FEATURES.md) - Plans for cross-database queries
