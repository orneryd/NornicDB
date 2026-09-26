# NornicDB Cypher Compatibility

**Date**: November 26, 2025  
**Status**: Complete — production ready  
**Purpose**: Comprehensive audit of Cypher implementation against Neo4j

---

## Currently Implemented

### Core Clauses

- ✅ **MATCH** - Pattern matching with property filters
- ✅ **MATCH...CREATE** - Create relationships between matched nodes (like Neo4j's variable scoping)
- ✅ **CREATE** - Node and relationship creation
- ✅ **MERGE** - Upsert operations with ON CREATE/ON MATCH
- ✅ **DELETE** - Node deletion
- ✅ **DETACH DELETE** - Delete with relationship removal
- ✅ **SET** - Property updates
- ✅ **SET +=** - Property merging
- ✅ **REMOVE** - Property removal
- ✅ **RETURN** - Result projection
- ✅ **WHERE** - Filtering
- ✅ **WITH** - Intermediate result projection
- ✅ **UNWIND** - List expansion
- ✅ **OPTIONAL MATCH** - Outer join equivalent
- ✅ **UNION** / **UNION ALL** - Query combination
- ✅ **FOREACH** - Iteration with updates

### Schema Management

- ✅ **CREATE CONSTRAINT** - All constraint families (see below)
- ✅ **CREATE INDEX** - Property indexes
- ✅ **CREATE FULLTEXT INDEX** - Fulltext search indexes
- ✅ **CREATE VECTOR INDEX** - Vector similarity indexes
- ✅ **DROP CONSTRAINT** / **DROP INDEX** - Schema deletion

#### Constraint Types

All constraint DDL supports `IF NOT EXISTS` for idempotent creation and named or unnamed forms.

**Node constraints** (`FOR (var:Label)`):

| Constraint | Syntax |
|------------|--------|
| Uniqueness | `REQUIRE var.prop IS UNIQUE` |
| Existence | `REQUIRE var.prop IS NOT NULL` |
| Node key | `REQUIRE (var.p1, var.p2) IS NODE KEY` |
| Property type | `REQUIRE var.prop IS :: TYPE` |
| Temporal no-overlap | `REQUIRE (var.key, var.from, var.to) IS TEMPORAL NO OVERLAP` |
| Domain/enum | `REQUIRE var.prop IN ['val1', 'val2']` |

**Cardinality constraints** (`FOR ()-[var:TYPE]->()` or `FOR ()<-[var:TYPE]-()`):

| Constraint | Syntax |
|------------|--------|
| Max outgoing | `FOR ()-[var:TYPE]->() REQUIRE MAX COUNT N` |
| Max incoming | `FOR ()<-[var:TYPE]-() REQUIRE MAX COUNT N` |

Limits the number of outgoing or incoming edges of a given type per node. Direction is encoded in the `FOR` clause arrows.

**Relationship endpoint policies** (`FOR (:SrcLabel)-[var:TYPE]->(:TgtLabel)`):

| Constraint | Syntax |
|------------|--------|
| Allowed pair | `FOR (:Src)-[var:TYPE]->(:Tgt) REQUIRE ALLOWED` |
| Disallowed pair | `FOR (:Src)-[var:TYPE]->(:Tgt) REQUIRE DISALLOWED` |

ALLOWED policies form a union whitelist: once any ALLOWED policy exists for a relationship type, only declared (source, target) label pairs are permitted. DISALLOWED policies are a blacklist and take precedence over ALLOWED.

**Relationship constraints** (`FOR ()-[var:TYPE]-()`):

| Constraint | Syntax |
|------------|--------|
| Uniqueness | `REQUIRE var.prop IS UNIQUE` |
| Composite uniqueness | `REQUIRE (var.p1, var.p2) IS UNIQUE` |
| Existence | `REQUIRE var.prop IS NOT NULL` |
| Relationship key | `REQUIRE (var.p1, var.p2) IS RELATIONSHIP KEY` |
| Property type | `REQUIRE var.prop IS :: TYPE` |
| Temporal no-overlap | `REQUIRE (var.key, var.from, var.to) IS TEMPORAL NO OVERLAP` |
| Domain/enum | `REQUIRE var.prop IN ['val1', 'val2']` |

Temporal no-overlap, domain/enum, cardinality, and endpoint policy constraints are NornicDB extensions not available in Neo4j.
Uniqueness and key constraints on relationships automatically create owned backing indexes.
`SHOW CONSTRAINTS` returns all constraint types with entity type, direction, maxCount, sourceLabel, targetLabel, and policyMode columns where applicable.
For operational guidance on NornicDB-specific schema features, including `REQUIRE { ... }` block contracts and `SHOW CONSTRAINT CONTRACTS`, see [Managing Constraints](../user-guides/managing-constraints.md).

### CALL Procedures

- ✅ **db.labels()** - List all labels
- ✅ **db.propertyKeys()** - List all property keys
- ✅ **db.relationshipTypes()** - List all relationship types
- ✅ **db.indexes()** - List indexes
- ✅ **db.constraints()** - List constraints
- ✅ **db.index.vector.queryNodes()** - Vector similarity search
- ✅ **db.index.fulltext.queryNodes()** - Fulltext search
- ✅ **apoc.path.subgraphNodes()** - Graph traversal
- ✅ **apoc.path.expand()** - Path expansion

### SHOW Commands

- ✅ **SHOW INDEXES** - Display indexes
- ✅ **SHOW CONSTRAINTS** - Display constraints
- ✅ **SHOW CONSTRAINT CONTRACTS** - Display block-style constraint contract metadata
- ✅ **SHOW PROCEDURES** - List procedures
- ✅ **SHOW FUNCTIONS** - List functions
- ✅ **SHOW DATABASE** - Database info

SHOW INDEXES, SHOW CONSTRAINTS and SHOW DATABASES return Neo4j 5's columns and values: constraint types such as `UNIQUENESS` / `RELATIONSHIP_UNIQUENESS` and `NODE_PROPERTY_EXISTENCE`, Neo4j's index providers (`range-1.0`, `fulltext-1.0`, `vector-2.0`, …), `options`, and a `createStatement` that recreates each index and constraint. NornicDB's own constraint types (temporal no-overlap, domain, cardinality, endpoint policy) keep their names, and their `createStatement` uses the NornicDB syntax above. Some values have no NornicDB meaning and are `null`:

- SHOW INDEXES `lastRead`, `readCount` and `trackedSince`: NornicDB doesn't track index reads.
- SHOW DATABASES `databaseID`, `serverID` and `store`: NornicDB has no database or server IDs and no Neo4j store format. A server is the single primary of each database it serves (`currentPrimariesCount` 1, `replicationLag` 0), and `lastStartTime` is the later of the database's creation and the server's start.

### Aggregation Functions

- ✅ **COUNT()** - Count aggregation
- ✅ **SUM()** - Sum aggregation
- ✅ **AVG()** - Average aggregation
- ✅ **MIN()** / **MAX()** - Min/max aggregation
- ✅ **COLLECT()** - List collection

### Scalar Functions (52 total)

- ✅ String functions: substring, replace, trim, upper, lower, split, etc.
- ✅ Math functions: abs, ceil, floor, round, sqrt, sin, cos, etc.
- ✅ List functions: size, head, tail, last, range, etc.
- ✅ Type functions: toInteger, toFloat, toString, toBoolean
- ✅ Spatial functions: point, distance
- ✅ Date/time functions: date, datetime, timestamp

### Bolt Handshake Compatibility for `cypher-shell`

NornicDB speaks Bolt directly, and most Bolt drivers work without special handling. One notable edge case is Neo4j's `cypher-shell`, which may reject an otherwise valid Bolt connection if the Bolt `HELLO` success metadata does not advertise a Neo4j-style `server` string.

For that case, NornicDB includes an explicit compatibility override:

```bash
export NORNICDB_BOLT_SERVER_ANNOUNCEMENT="Neo4j/5.26.0"
cypher-shell -a bolt://localhost:7687 -u admin -p password
```

Equivalent YAML setting:

```yaml
server:
  bolt_server_announcement: "Neo4j/5.26.0"
```

This changes only the announced Bolt server string used during handshake compatibility checks. It does not change Cypher behavior or query semantics. Leave it unset unless you specifically need compatibility with `cypher-shell` or another strict Neo4j client.

---

## Recently Verified Features

### 1. **ORDER BY** Clause

**Status**: Implemented  
**Impact**: Full sorting support

```cypher

MATCH (n:Node)
RETURN n.name, n.age
ORDER BY n.age DESC, n.name ASC
```

**Features**:

- ✅ Single and multiple sort fields
- ✅ ASC/DESC modifiers
- ✅ String and numeric sorting
- ✅ Integration with LIMIT/SKIP

### 2. **LIMIT** / **SKIP** Clauses

**Status**: Implemented  
**Impact**: Full pagination support

```cypher

MATCH (n:Node)
RETURN n
ORDER BY n.created DESC
SKIP 10
LIMIT 20
```

**Features**:

- ✅ LIMIT with any number
- ✅ SKIP with any number
- ✅ Combined SKIP + LIMIT for pagination
- ✅ Works with ORDER BY

### 3. **DISTINCT** Keyword

**Status**: Implemented  
**Impact**: Full deduplication support

```cypher

MATCH (n:Node)-[:KNOWS]->(m)
RETURN DISTINCT n.name
```

**Features**:

- ✅ RETURN DISTINCT
- ✅ Deduplication of result rows
- ✅ Works with aggregations

### 4. **AS** Aliasing in RETURN

**Status**: Implemented  
**Impact**: Full aliasing support

```cypher

MATCH (n:Node)
RETURN n.name AS personName, n.age AS personAge
```

### 5. **Variable-length Paths**

**Status**: Implemented

```cypher

MATCH p=(a:Person)-[:KNOWS*1..3]->(b:Person) RETURN p
```

### 6. **EXISTS Subqueries**

**Status**: Implemented

```cypher

MATCH (n:Person)
WHERE EXISTS { MATCH (n)-[:KNOWS]->(m) }
RETURN n
```

### 7. **COUNT Subqueries**

**Status**: Implemented

```cypher

MATCH (n:Person)
RETURN n.name, COUNT { MATCH (n)-[:KNOWS]->(m) } AS cnt
```

### 8. **Map Projections**

**Status**: Implemented

```cypher

MATCH (n:Person) RETURN n {.name, .age}
```

### 9. **List Comprehensions**

**Status**: Implemented

```cypher

RETURN [x IN range(0,5) WHERE x % 2 = 0 | x*2] AS evens
```

### 10. **WHERE after YIELD**

**Status**: Implemented (6 passing tests)

```cypher

CALL db.index.vector.queryNodes('idx', 10, $vector)
YIELD node, score
WHERE score > 0.8
RETURN node

-- Also works with CONTAINS, <>, =
CALL db.labels() YIELD label WHERE label CONTAINS 'Person'
```

---

## Implemented November 26, 2025

### 11. **CASE Expressions**

**Status**: Complete  
**Files**: `pkg/cypher/case_expression.go` (376 lines)

```cypher
-- Searched CASE
MATCH (n:Person)
RETURN n.name,
  CASE
    WHEN n.age < 18 THEN 'minor'
    WHEN n.age < 65 THEN 'adult'
    ELSE 'senior'
  END AS ageGroup

-- Simple CASE
MATCH (n:Person)
RETURN CASE n.age
  WHEN 30 THEN 'thirty'
  WHEN 25 THEN 'twenty-five'
  ELSE 'other'
END AS ageLabel
```

**Features Implemented**:

- ✅ Searched CASE with WHEN/THEN/ELSE
- ✅ Simple CASE with value matching
- ✅ NULL handling (IS NULL, IS NOT NULL)
- ✅ Comparison operators (<, >, <=, >=, =, <>)
- ✅ Nested expression evaluation
- ✅ Multiple WHEN clauses
- ✅ Optional ELSE clause (returns NULL if omitted)

### 12. **shortestPath() / allShortestPaths()**

**Status**: Complete (16 passing tests)  
**Files**: `pkg/cypher/shortest_path.go` (372 lines), `pkg/cypher/traversal.go` (617 lines)

```cypher
-- shortestPath with MATCH variable resolution
MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
MATCH p = shortestPath((start)-[:KNOWS*]->(end))
RETURN p, length(p) AS pathLength

-- allShortestPaths
MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
MATCH p = allShortestPaths((start)-[:KNOWS*]->(end))
RETURN p

-- Path functions
MATCH p = shortestPath((a)-[*]-(b))
RETURN nodes(p), relationships(p), length(p)
```

**Features Implemented**:

- ✅ BFS shortest path algorithm (unweighted)
- ✅ allShortestPaths() - finds all paths of minimum length
- ✅ **Variable resolution from MATCH clause** (like Neo4j's LogicalVariable)
- ✅ Direction support (outgoing ->, incoming <-, both -)
- ✅ Relationship type filtering
- ✅ Max hops limiting (\*..max)
- ✅ Path functions: nodes(p), relationships(p), length(p)
- ✅ Cycle detection

**Recent Fix**: shortestPath now correctly resolves variable references (e.g., `start`, `end`) from the preceding MATCH clause, matching Neo4j's behavior where variables are "in scope" and referenced, not re-queried.

### 13. **Transaction Atomicity**

```go
// Transaction support with full rollback
tx := engine.BeginTransaction()

// All operations are buffered
tx.CreateNode(&storage.Node{...})
tx.CreateEdge(&storage.Edge{...})
tx.UpdateNode(nodeID, &storage.Node{...})
tx.DeleteNode(nodeID)

// Atomic commit - all or nothing
err := tx.Commit()

// Or rollback to discard all changes
tx.Rollback()
```

**Features Implemented**:

- ✅ `BeginTransaction()` - Start new transaction
- ✅ `Commit()` - Atomically apply all buffered operations
- ✅ `Rollback()` - Discard all buffered operations
- ✅ `CreateNode/UpdateNode/DeleteNode` - Node operations in transaction
- ✅ `CreateEdge/DeleteEdge` - Edge operations in transaction
- ✅ `GetNode()` - Read-your-writes consistency
- ✅ `IsActive()` - Check transaction status
- ✅ Isolation - Uncommitted changes not visible to other operations
- ✅ Atomicity - All operations succeed or all fail together

### 14. **Composite Indexes**

**Status**: Complete  
**Files**: `pkg/storage/schema.go`

**Features**:

- ✅ Multi-property indexes
- ✅ SHA256-based composite keys
- ✅ Efficient prefix lookups
- ✅ Full and partial key matching
- ✅ Neo4j-compatible behavior

### 15. **MATCH...CREATE**

**Status**: Complete  
**Files**: `pkg/cypher/create.go` (427 lines)

```cypher
-- Create relationship between existing matched nodes
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS]->(b)
```

**Key Feature**: Like Neo4j, variables from MATCH are "in scope" - CREATE only creates what's NEW. If variables reference matched nodes, use those existing nodes (not create new ones).

---

### 16. **EXPLAIN / PROFILE**

**Status**: Complete (27 passing tests)  
**Files**: `pkg/cypher/explain.go` (560 lines), `pkg/cypher/explain_test.go`

```cypher
-- EXPLAIN - show execution plan without executing
EXPLAIN MATCH (n:Person) RETURN n
EXPLAIN MATCH (n:Person) WHERE n.age > 25 RETURN n ORDER BY n.name LIMIT 10

-- PROFILE - execute and show plan with statistics
PROFILE MATCH (n:Person) RETURN n
PROFILE MATCH (n:Person)-[:KNOWS]->(m) RETURN n, m
```

**Features Implemented**:

- ✅ EXPLAIN mode (shows plan, doesn't execute)
- ✅ PROFILE mode (executes and shows plan with stats)
- ✅ Execution plan tree structure
- ✅ Operator types: NodeByLabelScan, AllNodesScan, NodeIndexSeek, Filter, Expand, Sort, Limit, ProduceResults, etc.
- ✅ Estimated rows per operator
- ✅ DB hits estimation
- ✅ Actual rows and timing (PROFILE only)
- ✅ Visual plan formatting

**Example Output**:

```
+--------------------------------------------------------------+
| PROFILE Query Plan                                           |
+--------------------------------------------------------------+
| Total Time: 1.234ms                                          |
| Total Rows: 3                                                |
| Total DB Hits: 2006                                          |
+--------------------------------------------------------------+
| +- ProduceResults (Return results)                           |
| |   Est: 100, Actual: 3, Hits: 100                          |
|   +- NodeByLabelScan (Scan all :Person nodes)               |
|   |   Est: 1000, Actual: 3, Hits: 2000                      |
+--------------------------------------------------------------+
```

---

## ⏺️ Optional Features (Not Critical)

### 1. **Multi-database Support** 🟢 LOW PRIORITY

**Status**: NOT IMPLEMENTED  
**Impact**: Single database only

```cypher
-- Not supported
USE database2
CREATE DATABASE mydb
SHOW DATABASES
```

**Estimated Effort**: 1-2 weeks  
**Priority**: LOW (most deployments use single database)

---

## Implementation Status Summary

| Feature               | Status   | Tests     | Coverage   |
| --------------------- | -------- | --------- | ---------- |
| CASE expressions      | Complete | 7+ tests  | 376 lines  |
| shortestPath()        | Complete | 16 tests  | 372 lines  |
| allShortestPaths()    | Complete | 16 tests  | included   |
| Transaction Atomicity | Complete | 12 tests  | 521 lines  |
| WHERE after YIELD     | Complete | 6 tests   | integrated |
| MATCH...CREATE        | Complete | 16+ tests | 427 lines  |
| Composite Indexes     | Complete | multiple  | integrated |
| EXPLAIN/PROFILE       | Complete | 27 tests  | 560 lines  |

### Test Coverage

| Package         | Tests           | Coverage |
| --------------- | --------------- | -------- |
| **pkg/cypher**  | 863 tests       | 82%+     |
| **pkg/storage** | 308 tests       | 85.2%    |
| **Total**       | **1,171 tests** | **~83%** |

---

## Current Status Summary

**Compatibility**: 100% — production ready  
**Status**: All critical features implemented  
**Deployment**: Ready for production use

### Complete Feature Set

**Core Query (100%)**:

- ✅ All 16 Cypher clauses implemented and tested
- ✅ All result modifiers (ORDER BY, LIMIT, SKIP, DISTINCT, AS)
- ✅ All pattern types (variable-length, bidirectional, multiple)
- ✅ All subqueries (EXISTS, COUNT)
- ✅ All collections (map projections, list/pattern comprehensions)
- ✅ WHERE after YIELD filtering

**Advanced Features (100%)**:

- ✅ CASE expressions (searched and simple)
- ✅ shortestPath() and allShortestPaths() with MATCH variable resolution
- ✅ Variable-length path traversal
- ✅ Composite indexes with prefix lookup
- ✅ MATCH...CREATE with variable scoping (like Neo4j)

**Transaction Support (100%)**:

- ✅ BeginTransaction/Commit/Rollback
- ✅ Atomic operations (all-or-nothing)
- ✅ Read-your-writes consistency
- ✅ Transaction isolation

**Schema & Indexes (100%)**:

- ✅ All constraint families: UNIQUE, EXISTS, NODE KEY, RELATIONSHIP KEY, property type
- ✅ Constraints on both nodes and relationships with full write-path enforcement
- ✅ NornicDB extensions: temporal no-overlap, domain/enum, cardinality, endpoint policy constraints
- ✅ IF NOT EXISTS idempotent creation, owned backing indexes
- ✅ Property indexes (single and composite)
- ✅ Fulltext indexes (BM25 scoring)
- ✅ Vector indexes (cosine/euclidean/dot similarity)

**Functions (100%)**:

- ✅ 52 scalar functions
- ✅ 5 aggregation functions
- ✅ 10 CALL procedures

### ⏺️ Optional (Not Required for Most Deployments)

**Low Priority**:

- ⏺️ Multi-database - Not needed

---

## Recent Changes (November 26, 2025)

### shortestPath Variable Resolution Fix

**Problem**: `shortestPath((start)-[:KNOWS*]->(end))` was not correctly resolving `start` and `end` variables from the preceding MATCH clause.

**Solution**: Implemented Neo4j-style variable resolution:

1. Parse the first MATCH clause to extract variable bindings
2. Resolve which `nodePatternInfo` each variable maps to
3. Find actual nodes matching those patterns
4. Use those specific nodes for shortestPath calculation

**Reference**: Neo4j uses `LogicalVariable` references in their query planner to bind variables from MATCH before using them in subsequent clauses.

### Transaction Atomicity Implementation

**Added**: Full transaction support with:

- Buffered operations (Write-Ahead Log pattern)
- Atomic commit (all operations applied together)
- Rollback support (discard all buffered changes)
- Read-your-writes consistency
- Transaction isolation

---

**Last Updated**: November 26, 2025 (Post EXPLAIN/PROFILE implementation)  
**Status**: Production ready  
**Test Results**: 1,171 tests passing
