# Multi-Database Future Features Plan

**Status:** 📋 Planning  
**Last Updated:** 2024-12-04

This document outlines the implementation plan for advanced multi-database features. **Note:** Database Aliases and Per-Database Resource Limits are now **✅ IMPLEMENTED** - see [Multi-Database User Guide](../user-guides/multi-database.md) for usage.

---

## Table of Contents

1. [Composite Databases](#composite-databases) - Future Enhancement
2. [Completed Features](#completed-features) - ✅ **IMPLEMENTED**

---

## Composite Databases

**Status:** ✅ **IMPLEMENTED** (v1.2)  
**Priority:** Medium  
**Estimated Effort:** 20-30 days

### Overview

Composite Databases (also known as Neo4j Fabric) enable creating a virtual database that spans multiple physical databases. Unlike cross-database queries which require explicit database references in each query, composite databases provide a unified view where queries appear to operate on a single database, while transparently accessing data from multiple constituent databases.

**This feature supersedes the need for explicit cross-database querying** by providing a cleaner, more intuitive abstraction layer.

**See [Multi-Database User Guide](../user-guides/multi-database.md#composite-databases) for usage instructions.**

### Key Concepts

1. **Composite Database**: A virtual database that doesn't store data itself
2. **Constituent Databases**: Physical databases that contain the actual data
3. **Database Aliases**: References from composite database to constituent databases
4. **Transparent Querying**: Queries against composite database automatically access all constituents

### Use Cases

- **Analytics Across Tenants**: Aggregate data from multiple tenant databases without explicit database references
- **Unified Reporting**: Generate reports across multiple databases as if they were one
- **Data Federation**: Query distributed data across multiple databases transparently
- **Multi-Region Queries**: Access data from databases in different regions/locations
- **Legacy Migration**: Gradually migrate data while maintaining unified access

### Design Approach

#### Composite Database Structure

```go
type CompositeDatabaseInfo struct {
    Name            string              // Composite database name
    Type            string              // "composite"
    Constituents    []ConstituentRef    // List of constituent databases
    CreatedAt       time.Time
    UpdatedAt       time.Time
}

type ConstituentRef struct {
    Alias           string  // Alias name within composite database
    DatabaseName    string  // Actual database name (or alias)
    Type            string  // "local" or "remote" (future)
    AccessMode      string  // "read", "write", "read_write"
}
```

#### Query Execution Flow

1. **Query Routing**: Identify which constituents are needed for the query
2. **Parallel Execution**: Execute query against relevant constituents in parallel
3. **Result Merging**: Combine results from all constituents
4. **Transparent Return**: Return unified results as if from single database

### Implementation Strategy

#### Phase 1: Composite Database Management

**Files:** `pkg/multidb/composite.go` (new), `pkg/multidb/manager.go`

1. **Composite Database Storage**:
   ```go
   // Store composite database metadata in system database
   type CompositeDatabaseInfo struct {
       Name         string
       Type         string  // "composite"
       Constituents []ConstituentRef
       CreatedAt    time.Time
       UpdatedAt    time.Time
   }
   
   // Add to DatabaseInfo
   type DatabaseInfo struct {
       Name          string
       Type          string  // "standard", "system", "composite"
       Constituents  []ConstituentRef  // Only for composite type
       // ... existing fields
   }
   ```

2. **Management Commands**:
   ```cypher
   -- Create composite database
   CREATE COMPOSITE DATABASE analytics
     ALIAS tenant_a FOR DATABASE tenant_a
     ALIAS tenant_b FOR DATABASE tenant_b
     ALIAS tenant_c FOR DATABASE tenant_c
   
   -- Add constituent to existing composite
   ALTER COMPOSITE DATABASE analytics
     ADD ALIAS tenant_d FOR DATABASE tenant_d
   
   -- Remove constituent
   ALTER COMPOSITE DATABASE analytics
     DROP ALIAS tenant_c
   
   -- Drop composite database
   DROP COMPOSITE DATABASE analytics
   
   -- Show composite databases
   SHOW COMPOSITE DATABASES
   SHOW CONSTITUENTS FOR COMPOSITE DATABASE analytics
   ```

#### Phase 2: Composite Database Executor

**File:** `pkg/cypher/composite_executor.go` (new)

1. **Composite Query Executor**:
   ```go
   type CompositeExecutor struct {
       dbManager     *multidb.DatabaseManager
       compositeInfo *CompositeDatabaseInfo
       executors     sync.Map  // map[string]*StorageExecutor per constituent
   }
   
   func (e *CompositeExecutor) Execute(
       ctx context.Context,
       query string,
       params map[string]interface{},
   ) (*ExecuteResult, error) {
       // 1. Analyze query to determine which constituents are needed
       constituents := e.analyzeQuery(query)
       
       // 2. Execute query against each constituent in parallel
       results := e.executeParallel(ctx, constituents, query, params)
       
       // 3. Merge results based on query type
       return e.mergeResults(results, query)
   }
   ```

2. **Query Analysis**:
   - **Label-based routing**: Route queries to constituents based on labels
   - **Property-based routing**: Route based on property values (e.g., database_id)
   - **Full scan**: Query all constituents if routing is ambiguous

3. **Result Merging Strategies**:
   - **UNION**: Combine all results (MATCH queries)
   - **AGGREGATION**: Sum/Count across constituents (COUNT, SUM, etc.)
   - **DISTINCT**: Remove duplicates across constituents
   - **ORDER BY**: Sort merged results
   - **LIMIT**: Apply limit after merging

#### Phase 3: Query Routing & Optimization

**File:** `pkg/cypher/composite_router.go` (new)

1. **Routing Strategies**:
   ```go
   type RoutingStrategy interface {
       RouteQuery(query *Query, constituents []ConstituentRef) []string
   }
   
   // Label-based routing: route to constituents that have nodes with specific labels
   type LabelRouting struct {
       labelMap map[string][]string  // label -> constituent aliases
   }
   
   // Property-based routing: route based on property values
   type PropertyRouting struct {
       property string
       valueMap map[interface{}]string  // property value -> constituent alias
   }
   
   // Full scan: query all constituents
   type FullScanRouting struct{}
   ```

2. **Routing Configuration**:
   ```cypher
   -- Configure label-based routing
   ALTER COMPOSITE DATABASE analytics
     SET ROUTING LABEL Person TO tenant_a, tenant_b
     SET ROUTING LABEL Order TO tenant_c
   
   -- Configure property-based routing
   ALTER COMPOSITE DATABASE analytics
     SET ROUTING PROPERTY database_id
       WHERE database_id = 'db_a' TO db_a
       WHERE database_id = 'db_b' TO db_b
   ```

#### Phase 4: Write Operations

1. **Write Routing**:
   - Route writes to appropriate constituent based on routing rules
   - Support explicit constituent selection: `CREATE (n:Person) IN tenant_a`
   - Default routing when ambiguous

2. **Transaction Handling**:
   - Single-constituent transactions: Normal ACID guarantees
   - Multi-constituent transactions: Two-phase commit (future enhancement)

#### Phase 5: Performance Optimization

1. **Parallel Execution**:
   ```go
   func (e *CompositeExecutor) executeParallel(
       ctx context.Context,
       constituents []string,
       query string,
       params map[string]interface{},
   ) map[string]*ExecuteResult {
       var wg sync.WaitGroup
       results := make(map[string]*ExecuteResult)
       errors := make(map[string]error)
       
       for _, alias := range constituents {
           wg.Add(1)
           go func(alias string) {
               defer wg.Done()
               executor := e.getExecutor(alias)
               result, err := executor.Execute(ctx, query, params)
               if err != nil {
                   errors[alias] = err
               } else {
                   results[alias] = result
               }
           }(alias)
       }
       
       wg.Wait()
       return results
   }
   ```

2. **Caching**:
   - Cache routing decisions
   - Cache query results per constituent
   - Invalidate on constituent updates

3. **Query Optimization**:
   - Push filters down to constituents
   - Use indexes from each constituent
   - Minimize data transfer

### Example Implementation

```go
// pkg/multidb/composite.go

type CompositeDatabaseManager struct {
    dbManager *DatabaseManager
    composites map[string]*CompositeDatabaseInfo
    mu sync.RWMutex
}

func (m *CompositeDatabaseManager) CreateCompositeDatabase(
    name string,
    constituents []ConstituentRef,
) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    
    // Validate name
    if m.dbManager.Exists(name) {
        return ErrDatabaseExists
    }
    
    // Validate all constituents exist
    for _, ref := range constituents {
        if !m.dbManager.Exists(ref.DatabaseName) {
            return fmt.Errorf("constituent database '%s' not found", ref.DatabaseName)
        }
    }
    
    // Create composite database info
    info := &DatabaseInfo{
        Name: name,
        Type: "composite",
        Constituents: constituents,
        CreatedAt: time.Now(),
        UpdatedAt: time.Now(),
        Status: "online",
    }
    
    // Store in system database
    return m.dbManager.persistDatabaseInfo(info)
}

func (m *CompositeDatabaseManager) GetCompositeStorage(
    name string,
) (storage.Engine, error) {
    m.mu.RLock()
    info, exists := m.composites[name]
    m.mu.RUnlock()
    
    if !exists {
        return nil, ErrDatabaseNotFound
    }
    
    // Return composite engine that routes to constituents
    return NewCompositeEngine(m.dbManager, info), nil
}
```

```go
// pkg/cypher/composite_executor.go

type CompositeExecutor struct {
    dbManager     *multidb.DatabaseManager
    compositeInfo *multidb.CompositeDatabaseInfo
    router        *CompositeRouter
    executors     sync.Map  // map[string]*StorageExecutor
}

func (e *CompositeExecutor) Execute(
    ctx context.Context,
    query string,
    params map[string]interface{},
) (*ExecuteResult, error) {
    // Parse query
    ast, err := e.parseQuery(query)
    if err != nil {
        return nil, err
    }
    
    // Determine which constituents to query
    constituents := e.router.RouteQuery(ast, e.compositeInfo.Constituents)
    
    if len(constituents) == 0 {
        return &ExecuteResult{Rows: []map[string]interface{}{}}, nil
    }
    
    // Execute in parallel
    results := e.executeParallel(ctx, constituents, query, params)
    
    // Merge results
    return e.mergeResults(results, ast)
}

func (e *CompositeExecutor) mergeResults(
    results map[string]*ExecuteResult,
    ast *QueryAST,
) (*ExecuteResult, error) {
    // Determine merge strategy based on query type
    if ast.HasAggregation {
        return e.mergeAggregation(results, ast)
    }
    
    // Union merge for regular queries
    var allRows []map[string]interface{}
    for _, result := range results {
        allRows = append(allRows, result.Rows...)
    }
    
    // Apply DISTINCT if needed
    if ast.HasDistinct {
        allRows = e.deduplicate(allRows)
    }
    
    // Apply ORDER BY
    if ast.OrderBy != nil {
        e.sortRows(allRows, ast.OrderBy)
    }
    
    // Apply LIMIT
    if ast.Limit > 0 {
        if int64(len(allRows)) > ast.Limit {
            allRows = allRows[:ast.Limit]
        }
    }
    
    return &ExecuteResult{Rows: allRows}, nil
}
```

### Testing Strategy

1. **Unit Tests**:
   - Composite database creation/deletion
   - Constituent management
   - Query routing logic
   - Result merging (union, aggregation, distinct)
   - Write routing

2. **Integration Tests**:
   - Queries against composite database
   - Multi-constituent queries
   - Write operations
   - Routing strategies
   - Performance with multiple constituents

3. **E2E Tests**:
   - Real-world analytics queries
   - Large-scale data across constituents
   - Concurrent access patterns
   - Failure scenarios (constituent offline)

### Security Considerations

- **Access Control**: Composite database inherits permissions from constituents
- **Audit Logging**: Log all composite database queries
- **Constituent Access**: Users must have access to all constituents
- **Write Restrictions**: Option to restrict composite databases to read-only
- **Rate Limiting**: Apply rate limits across all constituents

### Performance Considerations

- **Parallel Execution**: Critical for performance with multiple constituents
- **Result Size Limits**: Prevent memory exhaustion when merging large results
- **Query Timeout**: Set reasonable timeouts for composite queries
- **Routing Efficiency**: Minimize unnecessary constituent queries
- **Caching**: Cache routing decisions and results
- **Index Usage**: Leverage indexes from each constituent

### Limitations (v1)

- **Local Only**: Only local databases as constituents (remote support future)
- **No Cross-Constituent Relationships**: Cannot create relationships across constituents
- **Simple Merging**: Basic union/aggregation merging (advanced joins future)
- **No Distributed Transactions**: Multi-constituent writes are best-effort

### Future Enhancements

1. **Remote Constituents**: Support databases in other NornicDB instances
2. **Advanced Routing**: More sophisticated routing strategies
3. **Distributed Transactions**: Two-phase commit for multi-constituent writes
4. **Cross-Constituent Relationships**: Virtual relationships across constituents
5. **Query Optimization**: More advanced query planning and optimization

### Estimated Effort

- **Composite Database Management**: 3-4 days
- **Composite Executor**: 5-7 days
- **Query Routing**: 3-4 days
- **Result Merging**: 3-4 days
- **Write Operations**: 2-3 days
- **Performance Optimization**: 2-3 days
- **Testing**: 4-5 days
- **Total**: ~22-30 days

### Comparison with Cross-Database Queries

| Feature | Cross-Database Queries | Composite Databases |
|---------|------------------------|---------------------|
| **Syntax** | Explicit `FROM DATABASE` clauses | Transparent, single database view |
| **Complexity** | Verbose, requires database references | Clean, intuitive |
| **Use Case** | Ad-hoc cross-database queries | Regular analytics/reporting |
| **Performance** | Similar (both parallel execution) | Similar |
| **Security** | Explicit access per query | Configured once per composite |
| **Maintenance** | Query-level changes | Database-level configuration |

**Recommendation**: Implement Composite Databases instead of explicit cross-database queries. Composite databases provide a cleaner abstraction and better user experience for the primary use cases (analytics, reporting).

---

## Completed Features

### ✅ Database Aliases (v1.2)

**Status:** ✅ **IMPLEMENTED**  
**See:** [Multi-Database User Guide - Database Aliases](../user-guides/multi-database.md#database-aliases)

Database aliases allow creating alternate names for databases, enabling easier database management and migration scenarios.

**Features:**
- CREATE/DROP/SHOW ALIAS commands
- Alias resolution integrated into all routing points (Bolt, HTTP, Cypher)
- Alias persistence in database metadata
- Validation and conflict detection

### ✅ Per-Database Resource Limits (v1.2)

**Status:** ✅ **IMPLEMENTED**  
**See:** [Multi-Database User Guide - Resource Limits](../user-guides/multi-database.md#per-database-resource-limits)

Per-database resource limits enable administrators to set resource limits per database, preventing any single database from consuming excessive resources.

**Features:**
- Storage limits (max nodes, edges, bytes)
- Query limits (max query time, results, concurrent queries)
- Connection limits (max connections)
- Rate limits (max queries/writes per second)
- Limit enforcement at runtime
- Limit configuration and persistence

---

## Implementation Priority

### Completed ✅

1. **Database Aliases** - ✅ **IMPLEMENTED** (v1.2)
   - Fully functional with CREATE/DROP/SHOW ALIAS commands
   - Alias resolution integrated into all routing points
   - Persisted in database metadata

2. **Per-Database Resource Limits** - ✅ **IMPLEMENTED** (v1.2)
   - Limit configuration and storage implemented
   - Limits persisted in database metadata
   - Enforcement implemented with comprehensive unit tests

### Completed ✅

3. **Composite Databases** - ✅ **IMPLEMENTED** (v1.2)
   - Provides unified view across multiple databases
   - Supersedes need for explicit cross-database queries
   - Better user experience for analytics/reporting use cases
   - Foundation for future distributed database features
   - See [Multi-Database User Guide](../user-guides/multi-database.md#composite-databases) for usage

### Dependencies

- **Composite Databases**: Requires Database Aliases (✅ available) for constituent references

---

## Configuration Examples

### Composite Databases

```cypher
-- Create composite database for analytics
CREATE COMPOSITE DATABASE analytics
  ALIAS tenant_a FOR DATABASE tenant_a
  ALIAS tenant_b FOR DATABASE tenant_b
  ALIAS tenant_c FOR DATABASE tenant_c

-- Query composite database (transparent access to all constituents)
USE DATABASE analytics
MATCH (n:Person)
RETURN count(n)  -- Counts across all tenant databases

-- Configure routing
ALTER COMPOSITE DATABASE analytics
  SET ROUTING LABEL Person TO tenant_a, tenant_b
  SET ROUTING LABEL Order TO tenant_c

-- Add new constituent
ALTER COMPOSITE DATABASE analytics
  ADD ALIAS tenant_d FOR DATABASE tenant_d

-- Show composite database info
SHOW COMPOSITE DATABASES
SHOW CONSTITUENTS FOR COMPOSITE DATABASE analytics

-- Drop composite database
DROP COMPOSITE DATABASE analytics
```

### Database Aliases (Implemented)

```cypher
-- Create alias
CREATE ALIAS main FOR DATABASE tenant_primary_2024
CREATE ALIAS prod FOR DATABASE production_v2

-- Use alias
:USE main
MATCH (n) RETURN n

-- Drop alias
DROP ALIAS main

-- Show aliases
SHOW ALIASES
SHOW ALIASES FOR DATABASE tenant_primary_2024
```

### Resource Limits (Implemented)

```cypher
-- Set storage limits
ALTER DATABASE tenant_a SET LIMIT max_nodes = 1000000
ALTER DATABASE tenant_a SET LIMIT max_edges = 5000000
ALTER DATABASE tenant_a SET LIMIT max_storage_bytes = 10737418240  -- 10GB

-- Set query limits
ALTER DATABASE tenant_a SET LIMIT max_query_time = '60s'
ALTER DATABASE tenant_a SET LIMIT max_results = 10000
ALTER DATABASE tenant_a SET LIMIT max_concurrent_queries = 10

-- Set connection limits
ALTER DATABASE tenant_a SET LIMIT max_connections = 50

-- Set rate limits
ALTER DATABASE tenant_a SET LIMIT max_queries_per_second = 100
ALTER DATABASE tenant_a SET LIMIT max_writes_per_second = 50

-- Show limits
SHOW LIMITS FOR DATABASE tenant_a

-- Show usage
SHOW USAGE FOR DATABASE tenant_a
```

---

## See Also

- [Multi-Database User Guide](../user-guides/multi-database.md) - Current multi-database features
- [Multi-Database Implementation Spec](MULTI_DB_IMPLEMENTATION_SPEC.md) - Current implementation details
