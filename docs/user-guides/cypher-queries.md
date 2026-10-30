# Cypher Query Language Guide

**Complete guide to querying NornicDB with Cypher - the graph query language.**

## 📋 Table of Contents

- [Introduction](#introduction)
- [Basic Syntax](#basic-syntax)
- [Creating Data](#creating-data)
- [Reading Data](#reading-data)
- [Updating Data](#updating-data)
- [Inline Embedding on Mutations](#inline-embedding-on-mutations-with-embedding)
- [Deleting Data](#deleting-data)
- [Pattern Matching](#pattern-matching)
- [Functions](#functions)
- [Aggregations](#aggregations)
- [Advanced Queries](#advanced-queries)
- [Temporal Snapshot Queries](#temporal-snapshot-queries)

## Introduction

Cypher is a declarative graph query language that allows you to express what data you want, not how to get it. It uses ASCII-art syntax to represent graph patterns.

### Basic Concepts

- **Nodes:** `(n)` - Entities in your graph
- **Relationships:** `-[r]->` - Connections between nodes
- **Labels:** `(n:Person)` - Types/categories
- **Properties:** `{name: "Alice"}` - Key-value data

## Basic Syntax

### Node Syntax

```cypher
()                    // Anonymous node
(n)                   // Node with variable
(n:Person)            // Node with label
(n:Person:Employee)   // Node with multiple labels
(n {name: "Alice"})   // Node with properties
(n:Person {name: "Alice", age: 30})  // Combined
```

### Relationship Syntax

```cypher
-->                   // Outgoing relationship
<--                   // Incoming relationship
--                    // Undirected relationship
-[r]->                // Relationship with variable
-[r:KNOWS]->          // Relationship with type
-[r:KNOWS {since: 2020}]->  // With properties
-[r:KNOWS|FOLLOWS]->  // Multiple types
-[*1..3]->            // Variable length (1-3 hops)
```

## Creating Data

### CREATE - Create Nodes

```cypher
// Single node
CREATE (alice:Person {name: "Alice", age: 30})
RETURN alice

// Multiple nodes
CREATE
  (bob:Person {name: "Bob"}),
  (carol:Person {name: "Carol"}),
  (company:Company {name: "TechCorp"})
RETURN bob, carol, company
```

### CREATE - Create Relationships

```cypher
// Create nodes and relationship together
CREATE (alice:Person {name: "Alice"})-[r:KNOWS {since: 2020}]->(bob:Person {name: "Bob"})
RETURN alice, r, bob

// Connect existing nodes
MATCH
  (alice:Person {name: "Alice"}),
  (bob:Person {name: "Bob"})
CREATE (alice)-[r:KNOWS]->(bob)
RETURN r
```

### MERGE - Create if Not Exists

```cypher
// Create node if it doesn't exist
MERGE (alice:Person {name: "Alice"})
ON CREATE SET alice.created = timestamp()
ON MATCH SET alice.accessed = timestamp()
RETURN alice

// Create relationship if it doesn't exist
MATCH
  (alice:Person {name: "Alice"}),
  (bob:Person {name: "Bob"})
MERGE (alice)-[r:KNOWS]->(bob)
ON CREATE SET r.since = timestamp()
RETURN r
```

## Reading Data

### MATCH - Find Patterns

```cypher
// Find all nodes with label
MATCH (p:Person)
RETURN p

// Find nodes with properties
MATCH (p:Person {name: "Alice"})
RETURN p

// Find relationships
MATCH (p:Person)-[r:KNOWS]->(friend:Person)
RETURN p.name, friend.name

// Find paths
MATCH path = (alice:Person {name: "Alice"})-[:KNOWS*1..3]->(friend)
RETURN path
```

### WHERE - Filter Results

```cypher
// Simple conditions
MATCH (p:Person)
WHERE p.age > 30
RETURN p.name, p.age

// Multiple conditions
MATCH (p:Person)
WHERE p.age > 25 AND p.age < 40
RETURN p

// String matching
MATCH (p:Person)
WHERE p.name STARTS WITH "A"
RETURN p.name

// Regular expressions
MATCH (p:Person)
WHERE p.email =~ ".*@example\\.com"
RETURN p.name, p.email

// NULL checks
MATCH (p:Person)
WHERE p.email IS NOT NULL
RETURN p

// List membership
MATCH (p:Person)
WHERE p.name IN ["Alice", "Bob", "Carol"]
RETURN p
```

### RETURN - Specify Output

```cypher
// Return nodes
MATCH (p:Person)
RETURN p

// Return properties
MATCH (p:Person)
RETURN p.name, p.age

// Return with alias
MATCH (p:Person)
RETURN p.name AS personName, p.age AS personAge

// Return distinct
MATCH (p:Person)
RETURN DISTINCT p.city

// Return with expressions
MATCH (p:Person)
RETURN p.name, p.age, p.age + 10 AS ageIn10Years
```

### ORDER BY - Sort Results

```cypher
// Sort ascending
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age

// Sort descending
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC

// Multiple sort keys
MATCH (p:Person)
RETURN p.name, p.age, p.city
ORDER BY p.city, p.age DESC
```

### LIMIT & SKIP - Pagination

```cypher
// Limit results
MATCH (p:Person)
RETURN p
LIMIT 10

// Skip and limit (pagination)
MATCH (p:Person)
RETURN p
ORDER BY p.name
SKIP 20
LIMIT 10
```

## Temporal Snapshot Queries

For historical and temporal reads, use the temporal procedures rather than trying to express MVCC snapshot selection directly in plain `MATCH` syntax.

```cypher
CALL db.temporal.asOf(
  'Account',
  'accountId',
  'acct-42',
  'valid_from',
  'valid_to',
  datetime('2026-03-01T00:00:00Z'),
  datetime('2026-03-15T12:30:00Z')
) YIELD node
RETURN node
```

The final optional argument pair is the MVCC snapshot selector:

- `systemTime`
- `systemSequence`

See [Historical Reads & MVCC Retention](historical-reads-mvcc-retention.md) for pruning, retention policy, and maintenance details.

## Updating Data

### SET - Update Properties

```cypher
// Set single property
MATCH (alice:Person {name: "Alice"})
SET alice.age = 31
RETURN alice

// Set multiple properties
MATCH (alice:Person {name: "Alice"})
SET alice.age = 31, alice.city = "San Francisco"
RETURN alice

// Set from map
MATCH (alice:Person {name: "Alice"})
SET alice += {age: 31, city: "San Francisco"}
RETURN alice

// Add label
MATCH (alice:Person {name: "Alice"})
SET alice:Employee
RETURN labels(alice)

// Replace all properties
MATCH (alice:Person {name: "Alice"})
SET alice = {name: "Alice", age: 31}
RETURN alice
```

### REMOVE - Remove Properties/Labels

```cypher
// Remove property
MATCH (alice:Person {name: "Alice"})
REMOVE alice.email
RETURN alice

// Remove label
MATCH (alice:Person {name: "Alice"})
REMOVE alice:Employee
RETURN labels(alice)
```

## Inline Embedding on Mutations (`WITH EMBEDDING`)

`WITH EMBEDDING` is a NornicDB Cypher extension that runs embedding generation inline for node mutations and commits both the mutation and embedding state in the same statement transaction.

Use this when you need embedded content to be immediately available right after the write.

### Requirements

- Embeddings must be configured (`SetEmbedder(...)` in embedded usage, or server embedding enabled).
- Query must be a write mutation that touches nodes (`CREATE`, `MERGE`, `SET`, `UNWIND ... MERGE/SET`, etc.).

### Supported Pattern

Place `WITH EMBEDDING` immediately before `RETURN` (or at statement end if no return projection):

```cypher
... <mutation clauses> WITH EMBEDDING RETURN ...
```

### Examples

Single create:

```cypher
CREATE (d:Doc {id: 'd1', content: 'hello world'})
WITH EMBEDDING
RETURN count(d) AS created
```

Multi-create:

```cypher
CREATE
  (a:Doc {id: 'a1', content: 'alpha text'}),
  (b:Doc {id: 'b1', content: 'beta text'})
WITH EMBEDDING
RETURN count(*) AS created
```

Unwind pipeline:

```cypher
UNWIND $hops AS hop
MERGE (h:BenchmarkHop {hopId: hop.hopId})
SET h.benchmarkRun = hop.runID
WITH EMBEDDING
RETURN count(h) AS prepared
```

Set existing node properties and re-embed:

```cypher
MATCH (d:Doc {id: 'd1'})
SET d.content = 'updated text', d.version = 2
WITH EMBEDDING
RETURN d.id, d.version
```

### What gets written

The statement writes managed embedding fields in the same canonical format used by the async worker:

- `ChunkEmbeddings` populated with chunk vectors
- `EmbedMeta.chunk_count`
- `EmbedMeta.embedding_model`
- `EmbedMeta.embedding_dimensions`
- `EmbedMeta.has_embedding`
- `EmbedMeta.embedded_at`

### Behavior notes

- `WITH EMBEDDING` is node-focused; relationship-only writes do not produce node embeddings.
- If embedding generation fails, the statement fails and the transaction rolls back.
- This extension is useful for low-latency consistency; async queue embedding remains suitable for high-throughput ingestion.

## Deleting Data

### DELETE - Remove Nodes/Relationships

```cypher
// Delete relationship
MATCH (alice:Person)-[r:KNOWS]->(bob:Person)
DELETE r

// Delete node (must have no relationships)
MATCH (bob:Person {name: "Bob"})
DELETE bob

// Delete node and relationships
MATCH (bob:Person {name: "Bob"})
DETACH DELETE bob

// Delete all data (⚠️ Use with caution!)
MATCH (n)
DETACH DELETE n
```

## Pattern Matching

### Simple Patterns

```cypher
// Direct relationship
MATCH (alice:Person)-[:KNOWS]->(friend:Person)
RETURN alice.name, friend.name

// Bidirectional
MATCH (alice:Person)-[:KNOWS]-(friend:Person)
RETURN alice.name, friend.name

// Multiple relationships
MATCH (alice:Person)-[:KNOWS]->(friend)-[:WORKS_AT]->(company:Company)
RETURN alice.name, friend.name, company.name
```

### Variable Length Paths

```cypher
// Friends of friends (2 hops)
MATCH (alice:Person {name: "Alice"})-[:KNOWS*2]-(fof:Person)
RETURN DISTINCT fof.name

// Up to 3 hops
MATCH (alice:Person {name: "Alice"})-[:KNOWS*1..3]-(connected:Person)
RETURN DISTINCT connected.name

// Any length (⚠️ Can be slow!)
MATCH (alice:Person {name: "Alice"})-[:KNOWS*]-(connected:Person)
RETURN DISTINCT connected.name
```

### Shortest Path

```cypher
// Shortest path between two nodes
MATCH path = shortestPath(
  (alice:Person {name: "Alice"})-[:KNOWS*]-(dave:Person {name: "Dave"})
)
RETURN path, length(path)

// All shortest paths
MATCH path = allShortestPaths(
  (alice:Person {name: "Alice"})-[:KNOWS*]-(dave:Person {name: "Dave"})
)
RETURN path
```

### Optional Patterns

```cypher
// Optional match (like LEFT JOIN)
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name
```

## Functions

### String Functions

```cypher
MATCH (p:Person)
RETURN
  toLower(p.name) AS lowercase,
  toUpper(p.name) AS uppercase,
  substring(p.name, 0, 3) AS first3,
  replace(p.name, "Alice", "Alicia") AS replaced,
  split(p.email, "@") AS emailParts,
  trim(p.name) AS trimmed
```

### Math Functions

```cypher
RETURN
  abs(-5) AS absolute,
  ceil(3.2) AS ceiling,
  floor(3.8) AS floor,
  round(3.14159, 2) AS rounded,
  sqrt(16) AS squareRoot,
  rand() AS random
```

### List Functions

```cypher
RETURN
  size([1,2,3,4,5]) AS listSize,
  head([1,2,3]) AS first,
  tail([1,2,3]) AS rest,
  last([1,2,3]) AS lastElement,
  range(1, 10) AS numbers
```

### Temporal Functions

```cypher
RETURN
  timestamp() AS currentTimestamp,
  date() AS currentDate,
  datetime() AS currentDateTime,
  duration({days: 7}) AS oneWeek
```

See [Cypher Functions Reference](../api-reference/cypher-functions/README.md) for complete list.

## Aggregations

### COUNT

```cypher
// Count nodes
MATCH (p:Person)
RETURN count(p) AS totalPeople

// Count relationships
MATCH ()-[r:KNOWS]->()
RETURN count(r) AS totalFriendships

// Count distinct
MATCH (p:Person)
RETURN count(DISTINCT p.city) AS uniqueCities
```

### SUM, AVG, MIN, MAX

```cypher
MATCH (p:Person)
RETURN
  sum(p.age) AS totalAge,
  avg(p.age) AS averageAge,
  min(p.age) AS youngest,
  max(p.age) AS oldest
```

### COLLECT

```cypher
// Collect into list
MATCH (p:Person)
RETURN collect(p.name) AS allNames

// Collect distinct
MATCH (p:Person)
RETURN collect(DISTINCT p.city) AS cities
```

### GROUP BY (Implicit)

```cypher
// Group by city
MATCH (p:Person)
RETURN p.city, count(p) AS peopleInCity
ORDER BY peopleInCity DESC

// Group by multiple fields
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name, p.city, count(p) AS employees
ORDER BY employees DESC
```

## Advanced Queries

### WITH - Pipeline Queries

```cypher
// Filter aggregated results
MATCH (p:Person)
WITH p.city AS city, count(p) AS population
WHERE population > 10
RETURN city, population

// Multiple steps
MATCH (p:Person)-[:KNOWS]->(friend:Person)
WITH p, count(friend) AS friendCount
WHERE friendCount > 5
RETURN p.name, friendCount
ORDER BY friendCount DESC
```

### UNWIND - Expand Lists

```cypher
// Expand list into rows
UNWIND [1, 2, 3, 4, 5] AS number
RETURN number * 2 AS doubled

// Create multiple nodes from list
UNWIND ["Alice", "Bob", "Carol"] AS name
CREATE (p:Person {name: name})
RETURN p
```

### CASE - Conditional Logic

```cypher
MATCH (p:Person)
RETURN p.name,
  CASE
    WHEN p.age < 18 THEN "Minor"
    WHEN p.age < 65 THEN "Adult"
    ELSE "Senior"
  END AS ageGroup
```

### UNION - Combine Results

```cypher
// Union (removes duplicates)
MATCH (p:Person)
RETURN p.name AS name
UNION
MATCH (c:Company)
RETURN c.name AS name

// Union all (keeps duplicates)
MATCH (p:Person)
RETURN p.name AS name
UNION ALL
MATCH (c:Company)
RETURN c.name AS name
```

### Subqueries

```cypher
// Correlated subquery
MATCH (p:Person)
CALL {
  WITH p
  MATCH (p)-[:KNOWS]->(friend:Person)
  RETURN count(friend) AS friendCount
}
RETURN p.name, friendCount
```

## Best Practices

### 1. Use Parameters

```cypher
// Bad: Hardcoded values
MATCH (p:Person {name: "Alice"})
RETURN p

// Good: Use parameters
MATCH (p:Person {name: $name})
RETURN p
```

### 2. Create Indexes

```cypher
// Create index for better performance
CREATE INDEX person_name FOR (p:Person) ON (p.name)
CREATE INDEX person_email FOR (p:Person) ON (p.email)
```

### 3. Use EXPLAIN/PROFILE

```cypher
// Understand query plan
EXPLAIN MATCH (p:Person) WHERE p.age > 30 RETURN p

// Profile actual execution
PROFILE MATCH (p:Person)-[:KNOWS]->(friend) RETURN p, friend
```

### 4. Limit Results

```cypher
// Always limit for exploration
MATCH (p:Person)
RETURN p
LIMIT 100
```

### 5. Use DISTINCT Carefully

```cypher
// DISTINCT can be expensive
MATCH (p:Person)
RETURN DISTINCT p.city

// Better: Use aggregation
MATCH (p:Person)
WITH p.city AS city
RETURN city
```

## ⏭️ Next Steps

- **[Cypher Functions Reference](../api-reference/cypher-functions/README.md)** - Complete function list
- **[Graph Traversal](graph-traversal.md)** - Advanced pattern matching
- **[Complete Examples](complete-examples.md)** - Full applications
- **[Performance Guide](../performance/searching.md)** - Query optimization

---

**Need more examples?** → **[Complete Examples](complete-examples.md)**  
**Ready for advanced patterns?** → **[Graph Traversal](graph-traversal.md)**
