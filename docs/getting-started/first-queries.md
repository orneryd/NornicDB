# Your First Queries

**Learn basic Cypher queries to create, read, update, and delete graph data.**

## 📋 Prerequisites

- NornicDB installed and running (see [Installation](installation.md))
- Connected to database (HTTP or Bolt)

## 🎯 What You'll Learn

1. Creating nodes and relationships
2. Querying graph data
3. Updating properties
4. Deleting data
5. Basic graph patterns

## 1️⃣ Creating Your First Node

Let's create a person node:

```cypher
CREATE (alice:Person {
  name: "Alice Johnson",
  age: 30,
  email: "alice@example.com"
})
RETURN alice
```

**What this does:**
- `CREATE` - Creates a new node
- `(alice:Person ...)` - Variable name `alice`, label `Person`
- `{name: ..., age: ...}` - Properties (see [Property Data Types](../user-guides/property-data-types.md) for all supported types)
- `RETURN alice` - Returns the created node

## 2️⃣ Creating Multiple Nodes

```cypher
CREATE 
  (bob:Person {name: "Bob Smith", age: 35}),
  (carol:Person {name: "Carol White", age: 28}),
  (company:Company {name: "TechCorp", founded: 2010})
RETURN bob, carol, company
```

## 3️⃣ Creating Relationships

Connect Alice to the company:

```cypher
MATCH 
  (alice:Person {name: "Alice Johnson"}),
  (company:Company {name: "TechCorp"})
CREATE (alice)-[r:WORKS_AT {since: 2020, role: "Engineer"}]->(company)
RETURN alice, r, company
```

**Relationship syntax:**
- `(alice)-[r:WORKS_AT {...}]->(company)` - Directed relationship
- `r:WORKS_AT` - Relationship type
- `{since: 2020, role: "Engineer"}` - Relationship properties

## 4️⃣ Querying Data

### Find all people

```cypher
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC
```

### Find relationships

```cypher
MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
RETURN p.name, r.role, c.name
```

### Filter with WHERE

```cypher
MATCH (p:Person)
WHERE p.age > 30
RETURN p.name, p.age
```

### Pattern matching

```cypher
// Find people who work at the same company
MATCH (p1:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(p2:Person)
WHERE p1.name < p2.name  // Avoid duplicates
RETURN p1.name, p2.name, c.name
```

## 5️⃣ Updating Data

### Update properties

```cypher
MATCH (alice:Person {name: "Alice Johnson"})
SET alice.age = 31, alice.city = "San Francisco"
RETURN alice
```

### Add labels

```cypher
MATCH (alice:Person {name: "Alice Johnson"})
SET alice:Employee:Manager
RETURN labels(alice)
```

### Remove properties

```cypher
MATCH (alice:Person {name: "Alice Johnson"})
REMOVE alice.email
RETURN alice
```

## 6️⃣ Deleting Data

### Delete a node (must delete relationships first)

```cypher
MATCH (bob:Person {name: "Bob Smith"})
DETACH DELETE bob
```

**Note:** `DETACH DELETE` removes the node and all its relationships.

### Delete specific relationships

```cypher
MATCH (alice:Person)-[r:WORKS_AT]->()
DELETE r
```

### Delete all data (⚠️ Use with caution!)

```cypher
MATCH (n)
DETACH DELETE n
```

## 7️⃣ Common Patterns

### Create if not exists (MERGE)

```cypher
MERGE (alice:Person {name: "Alice Johnson"})
ON CREATE SET alice.created = timestamp()
ON MATCH SET alice.accessed = timestamp()
RETURN alice
```

### Count nodes

```cypher
MATCH (p:Person)
RETURN count(p) as totalPeople
```

### Aggregate data

```cypher
MATCH (p:Person)
RETURN 
  count(p) as total,
  avg(p.age) as averageAge,
  min(p.age) as youngest,
  max(p.age) as oldest
```

### Collect into list

```cypher
MATCH (p:Person)
RETURN collect(p.name) as allNames
```

## 8️⃣ Practical Example: Social Network

Let's build a small social network:

```cypher
// Create people
CREATE 
  (alice:Person {name: "Alice", age: 30}),
  (bob:Person {name: "Bob", age: 35}),
  (carol:Person {name: "Carol", age: 28}),
  (dave:Person {name: "Dave", age: 32})

// Create friendships
CREATE
  (alice)-[:FRIENDS_WITH {since: 2020}]->(bob),
  (alice)-[:FRIENDS_WITH {since: 2019}]->(carol),
  (bob)-[:FRIENDS_WITH {since: 2021}]->(dave),
  (carol)-[:FRIENDS_WITH {since: 2020}]->(dave)

RETURN *
```

### Find friends of friends

```cypher
MATCH (alice:Person {name: "Alice"})-[:FRIENDS_WITH*2]-(fof:Person)
WHERE alice <> fof
RETURN DISTINCT fof.name as friendOfFriend
```

### Find shortest path

```cypher
MATCH path = shortestPath(
  (alice:Person {name: "Alice"})-[:FRIENDS_WITH*]-(dave:Person {name: "Dave"})
)
RETURN path
```

## 9️⃣ Working with Properties

### JSON-like properties

```cypher
CREATE (doc:Document {
  title: "README",
  metadata: {
    author: "Alice",
    version: "1.0",
    tags: ["documentation", "guide"]
  }
})
RETURN doc
```

### List properties

```cypher
CREATE (person:Person {
  name: "Alice",
  skills: ["Python", "Go", "JavaScript"],
  languages: ["English", "Spanish"]
})
RETURN person
```

### Access nested properties

```cypher
MATCH (doc:Document)
RETURN doc.metadata.author, doc.metadata.tags
```

## 🔟 Tips & Best Practices

### 1. Use parameters for dynamic values

```cypher
// Instead of:
MATCH (p:Person {name: "Alice"})

// Use parameters:
MATCH (p:Person {name: $name})
```

### 2. Create indexes for better performance

```cypher
CREATE INDEX person_name FOR (p:Person) ON (p.name)
```

### 3. Use EXPLAIN to understand query plans

```cypher
EXPLAIN MATCH (p:Person) WHERE p.age > 30 RETURN p
```

### 4. Use PROFILE for performance analysis

```cypher
PROFILE MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p, c
```

### 5. Limit results for large datasets

```cypher
MATCH (p:Person)
RETURN p
LIMIT 10
```

## 📚 Common Functions

### String functions

```cypher
MATCH (p:Person)
RETURN 
  toLower(p.name) as lowercase,
  toUpper(p.name) as uppercase,
  substring(p.name, 0, 3) as first3chars
```

### Math functions

```cypher
RETURN 
  abs(-5) as absolute,
  round(3.14159, 2) as rounded,
  sqrt(16) as squareRoot
```

### List functions

```cypher
RETURN 
  size([1,2,3,4,5]) as listSize,
  head([1,2,3]) as firstElement,
  tail([1,2,3]) as restOfList
```

## ⏭️ Next Steps

Now that you know the basics:

1. **[Vector Search Guide](../user-guides/vector-search.md)** - Semantic search
2. **[Complete Examples](../user-guides/complete-examples.md)** - Full applications
3. **[Cypher Functions Reference](../api-reference/cypher-functions/README.md)** - All functions
4. **[Advanced Topics](../advanced/README.md)** - K-Means clustering, embeddings, custom functions

## 🆘 Common Issues

### "Node not found"
- Make sure you created the node first
- Check spelling of labels and properties

### "Cannot delete node with relationships"
- Use `DETACH DELETE` instead of `DELETE`

### "Query too slow"
- Create indexes on frequently queried properties
- Use `EXPLAIN` to analyze query plan
- Limit result sets with `LIMIT`

---

**Need more examples?** → **[Complete Examples](../user-guides/complete-examples.md)**  
**Ready for advanced features?** → **[User Guides](../user-guides/README.md)**
