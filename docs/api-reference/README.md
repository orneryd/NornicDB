# API Reference

**Complete API documentation for NornicDB.**

## 📚 Documentation Sections

### Cypher Functions

- **[Function Index](cypher-functions/)** - Complete list of all 62 functions
- **[String Functions](cypher-functions/#string-functions-15-functions)** - Text manipulation
- **[Math Functions](cypher-functions/#mathematical-functions-7-functions)** - Calculations
- **[Aggregation Functions](cypher-functions/#aggregation-functions-2-functions)** - COUNT, SUM, AVG
- **[List Functions](cypher-functions/#list-functions-9-functions)** - Array operations
- **[Date/Time Functions](cypher-functions/#datetime-functions-4-functions)** - Date/time
- **[Node & Relationship Functions](cypher-functions/#node--relationship-functions-11-functions)** - Graph operations

### HTTP API

- **[REST Endpoints](http-api.md)** - HTTP API documentation
- **[Transaction API](http-api.md#transactions)** - ACID transactions
- **[Search Endpoints](http-api.md#search)** - Vector and hybrid search
- **[Admin Endpoints](http-api.md#admin)** - System management

### Protocols

- **[Bolt Protocol](bolt-protocol.md)** - Binary protocol specification
- **[Client Drivers](client-drivers.md)** - Compatible drivers

## 🚀 Quick Start

### Using Cypher Functions

```cypher
// String functions
RETURN toLower("HELLO") AS lowercase

// Math functions
RETURN sqrt(16) AS squareRoot

// Aggregations
MATCH (p:Person)
RETURN count(p) AS total, avg(p.age) AS averageAge
```

### Using HTTP API

```bash
# Execute Cypher query
curl -X POST http://localhost:7474/db/data/tx/commit \
  -H "Content-Type: application/json" \
  -d '{
    "statements": [{
      "statement": "MATCH (n:Person) RETURN n LIMIT 10"
    }]
  }'
```

### Using Bolt Protocol

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    result = session.run("MATCH (n:Person) RETURN n LIMIT 10")
    for record in result:
        print(record["n"])
```

## 📖 Function Categories

### String Functions (15 functions)

Transform and manipulate text data.

**Common:** `toLower()`, `toUpper()`, `trim()`, `substring()`, `replace()`

[See all string functions →](cypher-functions/#string-functions-15-functions)

### Mathematical Functions (7 functions)

Perform calculations and transformations.

**Common:** `abs()`, `round()`, `sqrt()`, `rand()`

[See all math functions →](cypher-functions/#mathematical-functions-7-functions)

### Aggregation Functions (6 functions)

Summarize data across multiple rows.

**Common:** `count()`, `sum()`, `avg()`, `min()`, `max()`, `collect()`

[See all aggregation functions →](cypher-functions/#aggregation-functions-2-functions)

### List Functions (8 functions)

Work with arrays and collections.

**Common:** `size()`, `head()`, `tail()`, `range()`

[See all list functions →](cypher-functions/#list-functions-9-functions)

### Temporal Functions (4 functions)

Handle dates, times, and durations.

**Common:** `timestamp()`, `date()`, `datetime()`, `duration()`

[See all date/time functions →](cypher-functions/#datetime-functions-4-functions)

### Node & Relationship Functions (11 functions)

Access graph structure and metadata.

**Common:** `id()`, `labels()`, `type()`, `properties()`, `nodes()`, `relationships()`

[See all node/relationship functions →](cypher-functions/#node--relationship-functions-11-functions)

## 🌐 HTTP API Endpoints

### Neo4j Compatible

```
GET  /                           - Discovery endpoint
GET  /db/{name}                  - Database info
POST /db/{name}/tx/commit       - Execute query (implicit transaction)
POST /db/{name}/tx              - Begin transaction
POST /db/{name}/tx/{id}         - Execute in transaction
POST /db/{name}/tx/{id}/commit  - Commit transaction
DELETE /db/{name}/tx/{id}       - Rollback transaction
```

### NornicDB Extensions

```
POST /auth/token                - Get JWT token
GET  /auth/me                   - Current user info
POST  /nornicdb/search           - Hybrid search
POST  /nornicdb/similar          - Vector similarity
GET  /admin/stats               - System statistics
GET  /admin/gpu                 - GPU status
POST /gdpr/export               - GDPR data export
POST /gdpr/delete               - GDPR erasure
```

[See complete HTTP API documentation →](http-api.md)

## 🔌 Client Drivers

### Official Neo4j Drivers

NornicDB is compatible with official Neo4j drivers:

- **Python:** `neo4j` package
- **JavaScript:** `neo4j-driver` package
- **Java:** Neo4j Java Driver
- **Go:** `neo4j-go-driver`
- **.NET:** Neo4j.Driver

[See driver compatibility →](client-drivers.md)

### Example Usage

**Python:**

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("admin", "admin")
)

with driver.session() as session:
    result = session.run(
        "MATCH (p:Person {name: $name}) RETURN p",
        name="Alice"
    )
    for record in result:
        print(record["p"])
```

**JavaScript:**

```javascript
const neo4j = require("neo4j-driver");

const driver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic("admin", "admin")
);

const session = driver.session();
const result = await session.run("MATCH (p:Person {name: $name}) RETURN p", {
  name: "Alice",
});

result.records.forEach((record) => {
  console.log(record.get("p"));
});
```

## 📚 Related Documentation

- **[User Guides](../user-guides/)** - How to use features
- **[Features](../features/)** - Feature documentation
- **[Getting Started](../getting-started/)** - Installation and setup

## 🔍 Search This Documentation

Looking for a specific function or endpoint?

- **[Cypher Functions Index](cypher-functions/)** - Searchable function list
- **[HTTP API Reference](http-api.md)** - All endpoints
- **[Bolt Protocol Spec](bolt-protocol.md)** - Protocol details

---

**Ready to start?** → **[Cypher Functions](cypher-functions/)**  
**Need examples?** → **[User Guides](../user-guides/)**
