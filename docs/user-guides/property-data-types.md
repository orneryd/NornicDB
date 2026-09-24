# Node Property Data Types

**Complete reference for all supported property value types in NornicDB**

---

## Overview

NornicDB supports the Neo4j property value types for node and relationship properties: primitives, temporal values and lists of them. Properties are stored as `map[string]any` (key-value pairs). As in Neo4j, a map or a list that contains lists is not a property value: CREATE, MERGE and SET reject it with `Neo.ClientError.Statement.TypeError` (see [Maps are not property values](#maps-are-not-property-values)).

---

## Supported Data Types

### 1. Primitive Types

#### String (`string`)
Text values of any length.

**Example:**
```cypher
CREATE (n:Person {
  name: "Alice Johnson",
  email: "alice@example.com",
  bio: "Software engineer passionate about graph databases"
})
```

**Use Cases:**
- Names, titles, descriptions
- Email addresses, URLs
- Text content, comments
- Identifiers, codes

---

#### Integer (`int`, `int64`)
Whole numbers (positive, negative, or zero).

**Example:**
```cypher
CREATE (n:Person {
  age: 30,
  score: 100,
  year: 2024
})
```

**Supported Integer Types:**
- `int` (32-bit or 64-bit, platform-dependent)
- `int64` (64-bit signed integer)
- `int32` (32-bit signed integer)

**Note:** JSON deserialization may convert integers to `float64`. NornicDB accepts whole-number floats as integers for compatibility.

**Use Cases:**
- Ages, counts, quantities
- Years, timestamps
- Scores, ratings
- IDs, indices

---

#### Float (`float64`, `float32`)
Floating-point numbers (decimals).

**Example:**
```cypher
CREATE (n:Product {
  price: 29.99,
  rating: 4.5,
  weight: 1.25,
  temperature: -10.5
})
```

**Supported Float Types:**
- `float64` (64-bit floating point, default)
- `float32` (32-bit floating point)

**Use Cases:**
- Prices, measurements
- Ratings, percentages
- Coordinates, distances
- Scientific values

---

#### Boolean (`bool`)
True or false values.

**Example:**
```cypher
CREATE (n:User {
  verified: true,
  active: false,
  premium: true
})
```

**Use Cases:**
- Flags, toggles
- Status indicators
- Feature enablement
- Binary states

---

#### Null (`nil`)
Missing or undefined values.

**Example:**
```cypher
CREATE (n:Person {
  name: "Alice",
  middleName: null,  // Optional field
  email: "alice@example.com"
})
```

**Note:** `null` is valid for any property type. Properties with `null` values are stored but may be omitted in queries.

---

### 2. Temporal Types

#### Date/Time (`time.Time`)
Timestamps and dates (stored as Go `time.Time`).

**Example:**
```cypher
CREATE (n:Event {
  name: "Conference 2024",
  startDate: datetime("2024-06-15T09:00:00Z"),
  endDate: datetime("2024-06-17T17:00:00Z")
})
```

**Supported Formats:**
- ISO 8601: `"2024-06-15T09:00:00Z"`
- Unix timestamp: `1704067200`
- Date strings: `"2024-06-15"`

**Use Cases:**
- Event dates, deadlines
- Created/updated timestamps
- Birth dates, anniversaries
- Scheduling, calendars

**Note:** When stored via Go API, use `time.Time` directly. When stored via JSON/Cypher, dates are parsed from strings.

---

### 3. Collection Types

#### Arrays/Lists (`[]interface{}`, `[]string`, `[]int`, `[]int64`, `[]float64`)
Ordered sequences of values.

**Example:**
```cypher
CREATE (n:Person {
  name: "Alice",
  tags: ["developer", "graph-db", "go"],
  scores: [95, 87, 92],
  prices: [29.99, 49.99, 19.99],
  mixed: ["text", 42, true, 3.14]
})
```

**Supported Array Types:**
- `[]interface{}` - Mixed types (most flexible)
- `[]string` - String arrays
- `[]int` - Integer arrays
- `[]int64` - 64-bit integer arrays
- `[]float64` - Float arrays

**Use Cases:**
- Tags, categories
- Lists, sequences
- Coordinates, vectors
- Multiple values per property

**Querying Arrays:**
```cypher
// Check if value is in array
MATCH (n:Person)
WHERE "developer" IN n.tags
RETURN n

// Access array elements
MATCH (n:Person)
RETURN n.scores[0] AS firstScore

// Array length
MATCH (n:Person)
RETURN size(n.tags) AS tagCount
```

---

#### Maps are not property values
A map (`{street: "…", city: "…"}`), or a list that contains lists or maps, can't be stored as a property value. CREATE, MERGE and SET reject it with `Neo.ClientError.Statement.TypeError`, and the statement writes nothing, as in Neo4j:

```cypher
CREATE (n:Person {address: {city: "San Francisco"}})   // TypeError
CREATE (n:Person {matrix: [[1, 2], [3, 4]]})           // TypeError
MATCH (n:Person) SET n.address = {city: "San Francisco"} // TypeError
```

Maps are still ordinary values everywhere else: parameters, `WITH` / `RETURN` expressions, `UNWIND` rows, and map projections (`RETURN n {.name, .city}`).

**Storing structured data:**
```cypher
// Flatten into properties
CREATE (n:Person {name: "Alice", address_street: "123 Main St", address_city: "San Francisco"})

// Or model it as a related node
CREATE (p:Person {name: "Alice"})-[:LIVES_AT]->(:Address {street: "123 Main St", city: "San Francisco"})

// Or store a serialized string, and parse it in the application
CREATE (n:Person {name: "Alice", metadata: '{"created": "2024-01-15", "version": 1}'})
```

---

### 4. Special Types

#### Vector Embeddings (`[]float32`)
Vector embeddings for semantic search (stored in `NamedEmbeddings` or `ChunkEmbeddings`, not in properties).

**Note:** While vectors are `[]float32` internally, they are **not** stored as regular properties. Use `NamedEmbeddings` or `ChunkEmbeddings` struct fields instead.

**Example (via Cypher):**

When managed embeddings are enabled, NornicDB generates embeddings for new nodes automatically — you do not call any embedding mutator from Cypher. Embeddings are queried with the standard vector index procedure:

```cypher
// Query the configured node vector index by text or by a precomputed vector
CALL db.index.vector.queryNodes('embeddings', 10, 'document content text')
YIELD node, score
RETURN node.title, score
```

**Example (via Go API):**
```go
node := &storage.Node{
    ID:     storage.NodeID("doc-1"),
    Labels: []string{"Document"},
    Properties: map[string]any{
        "title": "My Document",
    },
    NamedEmbeddings: map[string][]float32{
        "content": []float32{0.1, 0.2, 0.3, ...}, // 768-dim vector
    },
}
```

See [Vector Embeddings](../features/vector-embeddings.md) for details.

---

## Type Constraints (Schema Validation)

NornicDB supports property type constraints for schema enforcement on both nodes and relationships.

**Supported Constraint Types:**
- `STRING` - String values only
- `INTEGER` - Integer values only
- `FLOAT` - Float values only
- `BOOLEAN` - Boolean values only
- `DATE` - Date values only
- `DATETIME` - DateTime values only

### Node property type constraints

```cypher
CREATE CONSTRAINT person_age_integer FOR (p:Person) REQUIRE p.age IS :: INTEGER

// This will fail:
CREATE (p:Person {age: "thirty"})  // Error: expected INTEGER, got string

// This will succeed:
CREATE (p:Person {age: 30})  // Valid
```

### Relationship property type constraints

Relationship constraints use the `FOR ()-[var:TYPE]-()` pattern:

```cypher
CREATE CONSTRAINT works_at_since_date
FOR ()-[r:WORKS_AT]-() REQUIRE r.since IS :: DATE

// This will fail:
MATCH (a:Person), (b:Company)
CREATE (a)-[:WORKS_AT {since: "not-a-date"}]->(b)  // Error: expected DATE

// This will succeed:
MATCH (a:Person), (b:Company)
CREATE (a)-[:WORKS_AT {since: date("2024-01-15")}]->(b)  // Valid
```

### Domain/enum constraints (NornicDB extension)

Domain constraints restrict a property to a fixed set of allowed values. They work on both nodes and relationships:

```cypher
// Node domain constraint
CREATE CONSTRAINT person_status_domain
FOR (n:Person) REQUIRE n.status IN ['active', 'inactive', 'suspended']

// Relationship domain constraint
CREATE CONSTRAINT works_at_role_domain
FOR ()-[r:WORKS_AT]-() REQUIRE r.role IN ['engineer', 'manager', 'director']

// This will fail:
CREATE (p:Person {status: "unknown"})  // Error: value not in allowed set

// This will succeed:
CREATE (p:Person {status: "active"})  // Valid
```

All constraint DDL supports `IF NOT EXISTS` for idempotent creation. NornicDB also provides cardinality constraints (`REQUIRE MAX COUNT N`) to limit edge count per node, endpoint policy constraints (`REQUIRE ALLOWED` / `REQUIRE DISALLOWED`) to restrict which label pairs may be connected by a relationship type, and block-style contract definitions with `REQUIRE { ... }` when you need several related checks under one named schema object.

Use [Managing Constraints](managing-constraints.md) as the canonical guide for block contracts, creation-time validation, runtime enforcement, and `SHOW CONSTRAINT CONTRACTS`. See [Canonical Graph Ledger](canonical-graph-ledger.md) for a domain walkthrough and [APOC Schema Functions](../features/apoc-functions.md) for programmatic schema management.

---

## Type Conversion & Coercion

### JSON Deserialization
When properties are loaded from JSON (HTTP API, Cypher via JSON), type conversions occur:

- **Integers**: May be deserialized as `float64` (JSON limitation). NornicDB accepts whole-number floats as integers.
- **Numbers**: `int`, `int64`, `float32`, `float64` are all supported.
- **Booleans**: `true`/`false` only (not `1`/`0` or `"true"`/`"false"`).

### Go API Type Handling
When using the Go API directly, types are preserved:

```go
// Types are preserved exactly
node := &storage.Node{
    Properties: map[string]any{
        "age":    int64(30),      // Preserved as int64
        "price":  float64(29.99), // Preserved as float64
        "active": true,            // Preserved as bool
    },
}
```

### Cypher Type Handling
Cypher queries handle type conversion automatically:

```cypher
// Cypher automatically converts literals
CREATE (n:Person {
  age: 30,        // Integer
  price: 29.99,   // Float
  active: true    // Boolean
})
```

---

## Examples by Use Case

### User Profile
```cypher
CREATE (u:User {
  // Strings
  username: "alice",
  email: "alice@example.com",
  bio: "Software engineer",
  
  // Numbers
  age: 30,
  score: 95.5,
  
  // Boolean
  verified: true,
  premium: false,
  
  // Arrays
  tags: ["developer", "graph-db"],
  skills: ["Go", "Cypher", "GraphQL"],
  
  // Objects
  profile: {
    avatar: "https://example.com/avatar.jpg",
    location: "San Francisco",
    timezone: "PST"
  },
  
  // Date
  createdAt: datetime("2024-01-15T10:00:00Z")
})
```

### Product Catalog
```cypher
CREATE (p:Product {
  // Strings
  name: "NornicDB Pro",
  description: "Enterprise graph database",
  sku: "NOR-001",
  
  // Numbers
  price: 99.99,
  stock: 100,
  rating: 4.8,
  
  // Boolean
  available: true,
  featured: false,
  
  // Arrays
  categories: ["database", "graph", "enterprise"],
  images: [
    "https://example.com/img1.jpg",
    "https://example.com/img2.jpg"
  ],
  
  // Objects
  metadata: {
    weight: 1.5,
    dimensions: {
      width: 10,
      height: 20,
      depth: 5
    },
    manufacturer: {
      name: "NornicDB Inc",
      country: "USA"
    }
  }
})
```

### Event/Calendar
```cypher
CREATE (e:Event {
  // Strings
  title: "Graph Database Conference",
  description: "Annual conference on graph databases",
  location: "San Francisco Convention Center",
  
  // Numbers
  capacity: 500,
  price: 299.99,
  
  // Boolean
  soldOut: false,
  virtual: false,
  
  // Arrays
  speakers: ["Alice", "Bob", "Carol"],
  tags: ["graph-db", "conference", "networking"],
  
  // Temporal
  start: datetime("2024-06-15T09:00:00Z"),
  end: datetime("2024-06-17T17:00:00Z"),
  timezone: "PST"
})-[:HELD_AT]->(:Venue {
  // Related data as its own node (maps are not property values)
  name: "SF Convention Center",
  street: "747 Howard St",
  city: "San Francisco",
  state: "CA",
  zip: "94103"
})
```

---

## Best Practices

### 1. Use Appropriate Types
- Use `int`/`int64` for whole numbers (ages, counts, IDs)
- Use `float64` for decimals (prices, measurements, ratings)
- Use `string` for text (names, descriptions, codes)
- Use `bool` for binary states (flags, toggles)

### 2. Avoid Type Mixing in Arrays
While `[]interface{}` supports mixed types, prefer homogeneous arrays for clarity:

```cypher
// ✅ Good: Homogeneous array
tags: ["tag1", "tag2", "tag3"]

// ⚠️ Acceptable but less clear: Mixed array
mixed: ["text", 42, true]
```

### 3. Model Related Data as Nodes or Flat Properties
Maps are not property values (a TypeError). Group related data in a related node, or in prefixed properties:

```cypher
// ✅ Related node
(:Person {name: "Alice"})-[:LIVES_AT]->(:Address {street: "123 Main St", city: "San Francisco", state: "CA"})

// ✅ Flat properties
address_street: "123 Main St",
address_city: "San Francisco",
address_state: "CA"

// ❌ TypeError: a map is not a property value
address: {street: "123 Main St", city: "San Francisco"}
```

### 4. Handle Null Values Explicitly
Use `null` for optional fields rather than empty strings or zeros:

```cypher
// ✅ Good: Explicit null for optional field
CREATE (n:Person {
  name: "Alice",
  middleName: null,  // Optional
  age: 30
})

// ❌ Less clear: Using empty string
CREATE (n:Person {
  name: "Alice",
  middleName: "",  // Ambiguous: empty or missing?
  age: 30
})
```

### 5. Use Type Constraints for Schema Enforcement
Enforce types at the schema level for data integrity:

```cypher
// Create constraints
CREATE CONSTRAINT person_age_integer FOR (p:Person) REQUIRE p.age IS :: INTEGER
CREATE CONSTRAINT person_email_string FOR (p:Person) REQUIRE p.email IS :: STRING
```

---

## Limitations & Notes

### Storage Limits
- **String length**: No hard limit (limited by available memory)
- **Array size**: No hard limit (limited by available memory)
- **Property count**: No hard limit per node

### Type Preservation
- **JSON API**: Types may be converted (integers → float64) during JSON round-trip
- **Go API**: Types are preserved exactly as provided
- **Cypher**: Types are inferred from literals and query context

### Vector Embeddings
- Vector embeddings are **not** stored as regular properties
- Use `NamedEmbeddings` or `ChunkEmbeddings` struct fields
- See [Vector Embeddings](../features/vector-embeddings.md) for details

### Neo4j Compatibility
- All Neo4j property types are supported
- As in Neo4j, maps and lists of lists are not property values (TypeError on CREATE, MERGE and SET)
- Type constraints follow Neo4j syntax

---

## Related Documentation

- [First Queries](../getting-started/first-queries.md) - Basic Cypher examples
- [Complete Examples](complete-examples.md) - Real-world use cases
- [Vector Embeddings](../features/vector-embeddings.md) - Vector search
- [Schema Constraints](../features/apoc-functions.md) - Type constraints
- [Cypher Queries](cypher-queries.md) - Advanced querying

---

## Summary

**Supported Types:**
- ✅ **Primitives**: `string`, `int`/`int64`, `float64`/`float32`, `bool`, `null`
- ✅ **Temporal**: `time.Time` (dates/timestamps)
- ✅ **Collections**: Arrays (`[]interface{}`, `[]string`, `[]int`, `[]float64`) of primitive or temporal values
- ✅ **Constraints**: Type constraints for schema enforcement

**Not Supported as Properties:**
- ❌ Maps and lists that contain lists or maps (TypeError on CREATE, MERGE and SET)
- ❌ Functions, closures
- ❌ Binary data (use base64 strings)
- ❌ Circular references (will cause serialization issues)

**Best Practices:**
- Use appropriate types for clarity
- Prefer homogeneous arrays
- Group related data in related nodes or prefixed properties
- Use `null` for optional fields
- Enforce types with constraints

---

**Questions?** See [Complete Examples](complete-examples.md) for real-world usage patterns.

