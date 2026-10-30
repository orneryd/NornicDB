# Cypher Compatibility Guide

**Purpose**: Ensure Neo4j Cypher compatibility in NornicDB  
**Audience**: AI coding agents  
**Goal**: 100% compatibility for supported features

---

## Compatibility Philosophy

### Core Principles

1. **Exact Semantics** - Match Neo4j behavior precisely
2. **Same Syntax** - Support Neo4j Cypher syntax
3. **Compatible Results** - Return data in Neo4j format
4. **Error Messages** - Use Neo4j-style error codes
5. **Parameter Syntax** - Support $param substitution

### Compatibility Levels

**Level 1: Full Compatibility** ✅
- MATCH, CREATE, MERGE, DELETE, SET, REMOVE
- WHERE, RETURN, WITH, ORDER BY, LIMIT, SKIP
- Basic functions (count, sum, avg, etc.)
- Parameter substitution
- Pattern matching

**Level 2: Partial Compatibility** ⚠️
- CALL procedures (built-ins only)
- OPTIONAL MATCH (basic cases)
- UNWIND (simple cases)
- Some APOC functions

**Level 3: Not Supported** ❌
- User-defined procedures
- Complex path expressions
- Some advanced APOC functions
- Multi-database queries
- Role-based access control

---

## Testing Cypher Compatibility

### Test Against Neo4j

**Always verify behavior matches Neo4j:**

```go
func TestCypherCompatibility_BasicMatch(t *testing.T) {
    // 1. Setup identical data in both databases
    nornicStore := storage.NewMemoryEngine()
    nornicExec := cypher.NewStorageExecutor(nornicStore)
    
    neo4jDriver := setupNeo4jDriver(t)
    defer neo4jDriver.Close()
    
    // Create same data
    setupData := []string{
        `CREATE (a:Person {name: 'Alice', age: 30})`,
        `CREATE (b:Person {name: 'Bob', age: 25})`,
        `CREATE (a)-[:KNOWS]->(b)`,
    }
    
    for _, query := range setupData {
        // Execute in NornicDB
        _, err := nornicExec.Execute(context.Background(), query, nil)
        require.NoError(t, err)
        
        // Execute in Neo4j
        session := neo4jDriver.NewSession(neo4j.SessionConfig{})
        _, err = session.Run(query, nil)
        require.NoError(t, err)
        session.Close()
    }
    
    // 2. Run same query in both
    query := `MATCH (p:Person) WHERE p.age > 25 RETURN p.name ORDER BY p.name`
    
    // NornicDB result
    nornicResult, err := nornicExec.Execute(context.Background(), query, nil)
    require.NoError(t, err)
    
    // Neo4j result
    session := neo4jDriver.NewSession(neo4j.SessionConfig{})
    neo4jResult, err := session.Run(query, nil)
    require.NoError(t, err)
    
    // 3. Compare results
    nornicNames := extractNames(nornicResult)
    neo4jNames := extractNamesFromNeo4j(neo4jResult)
    
    assert.Equal(t, neo4jNames, nornicNames, 
        "NornicDB and Neo4j should return identical results")
}
```

### Compatibility Test Template

```go
// TestCypherCompatibility_[Feature] verifies Neo4j compatibility for [feature].
//
// This test executes the same query in both NornicDB and Neo4j and verifies
// that the results are identical. This ensures we maintain Neo4j compatibility.
func TestCypherCompatibility_[Feature](t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping Neo4j compatibility test in short mode")
    }
    
    // Setup
    nornicExec := setupNornicDB(t)
    neo4jSession := setupNeo4j(t)
    defer neo4jSession.Close()
    
    // Create test data
    setupTestData(t, nornicExec, neo4jSession)
    
    // Test cases
    tests := []struct {
        name   string
        query  string
        params map[string]interface{}
    }{
        {
            name:  "basic case",
            query: "MATCH (n) RETURN n",
        },
        {
            name:  "with parameters",
            query: "MATCH (n {name: $name}) RETURN n",
            params: map[string]interface{}{"name": "Alice"},
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Execute in both databases
            nornicResult := executeNornic(t, nornicExec, tt.query, tt.params)
            neo4jResult := executeNeo4j(t, neo4jSession, tt.query, tt.params)
            
            // Compare results
            assertResultsEqual(t, neo4jResult, nornicResult)
        })
    }
}
```

---

## Cypher Feature Implementation

### 1. MATCH Clause

**Neo4j behavior:**
- Pattern matching with nodes and relationships
- Multiple patterns separated by commas
- Variable-length paths: `[*1..5]`
- Optional patterns: `OPTIONAL MATCH`

**Implementation checklist:**

```go
func (e *Executor) executeMatch(ctx context.Context, match *Match) ([][]interface{}, error) {
    // ✅ Parse pattern: (n:Label {prop: value})
    // ✅ Support multiple labels: (n:Label1:Label2)
    // ✅ Support property matching: {name: 'Alice'}
    // ✅ Support relationship patterns: -[r:TYPE]->
    // ✅ Support variable-length: -[*1..5]->
    // ✅ Support bidirectional: -[r]-
    // ✅ Handle OPTIONAL MATCH (left outer join)
    
    // Implementation...
}
```

**Test cases:**

```go
func TestMatch_Compatibility(t *testing.T) {
    tests := []struct {
        name  string
        query string
    }{
        {
            name:  "simple node match",
            query: "MATCH (n) RETURN n",
        },
        {
            name:  "match with label",
            query: "MATCH (n:Person) RETURN n",
        },
        {
            name:  "match with properties",
            query: "MATCH (n:Person {name: 'Alice'}) RETURN n",
        },
        {
            name:  "match relationship",
            query: "MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b",
        },
        {
            name:  "variable-length path",
            query: "MATCH (a)-[*1..3]->(b) RETURN a, b",
        },
        {
            name:  "optional match",
            query: "MATCH (a) OPTIONAL MATCH (a)-[r]->(b) RETURN a, r, b",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            verifyNeo4jCompatibility(t, tt.query)
        })
    }
}
```

### 2. WHERE Clause

**Neo4j behavior:**
- Boolean expressions: AND, OR, NOT
- Comparisons: =, <>, <, >, <=, >=
- String operations: STARTS WITH, ENDS WITH, CONTAINS
- Null checks: IS NULL, IS NOT NULL
- List operations: IN [list]
- Regular expressions: =~ 'pattern'

**Implementation checklist:**

```go
func (e *Executor) applyWhere(rows [][]interface{}, where *Where) [][]interface{} {
    // ✅ Boolean operators: AND, OR, NOT
    // ✅ Comparison operators: =, <>, <, >, <=, >=
    // ✅ String functions: STARTS WITH, ENDS WITH, CONTAINS
    // ✅ Null checks: IS NULL, IS NOT NULL
    // ✅ IN operator: value IN [1, 2, 3]
    // ✅ Regex: property =~ 'pattern'
    // ✅ Property existence: exists(n.property)
    
    // Implementation...
}
```

**Test cases:**

```go
func TestWhere_Compatibility(t *testing.T) {
    tests := []struct {
        name  string
        query string
    }{
        {
            name:  "equality",
            query: "MATCH (n) WHERE n.age = 30 RETURN n",
        },
        {
            name:  "comparison",
            query: "MATCH (n) WHERE n.age > 25 AND n.age < 40 RETURN n",
        },
        {
            name:  "string operations",
            query: "MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n",
        },
        {
            name:  "null check",
            query: "MATCH (n) WHERE n.email IS NOT NULL RETURN n",
        },
        {
            name:  "IN operator",
            query: "MATCH (n) WHERE n.age IN [25, 30, 35] RETURN n",
        },
        {
            name:  "regex",
            query: "MATCH (n) WHERE n.email =~ '.*@example\\.com' RETURN n",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            verifyNeo4jCompatibility(t, tt.query)
        })
    }
}
```

### 3. RETURN Clause

**Neo4j behavior:**
- Return nodes, relationships, properties
- Expressions: n.age + 10
- Aggregations: count(), sum(), avg()
- DISTINCT modifier
- Aliases: RETURN n.name AS name

**Implementation checklist:**

```go
func (e *Executor) executeReturn(rows [][]interface{}, ret *Return) (*Result, error) {
    // ✅ Return nodes and relationships
    // ✅ Return properties: n.name
    // ✅ Return expressions: n.age + 10
    // ✅ Aggregation functions: count(), sum(), avg(), min(), max()
    // ✅ DISTINCT modifier
    // ✅ Aliases: AS name
    // ✅ Return all: RETURN *
    
    // Implementation...
}
```

### 4. CREATE Clause

**Neo4j behavior:**
- Create nodes with labels and properties
- Create relationships
- Return created entities
- Atomic operation (all or nothing)

**Implementation checklist:**

```go
func (e *Executor) executeCreate(ctx context.Context, create *Create) (*Result, error) {
    // ✅ Create nodes: CREATE (n:Label {prop: value})
    // ✅ Create relationships: CREATE (a)-[r:TYPE]->(b)
    // ✅ Multiple labels: CREATE (n:Label1:Label2)
    // ✅ Return created: CREATE (n) RETURN n
    // ✅ Atomic operation (transaction)
    // ✅ Generate IDs if not provided
    
    // Implementation...
}
```

### 5. MERGE Clause

**Neo4j behavior:**
- Upsert operation (create if not exists)
- ON CREATE SET
- ON MATCH SET
- Match on pattern, not just ID

**Implementation checklist:**

```go
func (e *Executor) executeMerge(ctx context.Context, merge *Merge) (*Result, error) {
    // ✅ MERGE pattern: MERGE (n:Label {prop: value})
    // ✅ ON CREATE SET: Set properties on create
    // ✅ ON MATCH SET: Set properties on match
    // ✅ MERGE relationships: MERGE (a)-[r:TYPE]->(b)
    // ✅ Atomic operation
    // ✅ Return merged entity
    
    // Implementation...
}
```

**Test case:**

```go
func TestMerge_Compatibility(t *testing.T) {
    query := `
        MERGE (n:Person {email: 'alice@example.com'})
        ON CREATE SET n.created = timestamp(), n.count = 1
        ON MATCH SET n.count = n.count + 1
        RETURN n
    `
    
    // First execution: creates node
    result1 := executeNornic(t, exec, query, nil)
    assert.Equal(t, int64(1), result1.Rows[0][0].(*Node).Properties["count"])
    
    // Second execution: matches and increments
    result2 := executeNornic(t, exec, query, nil)
    assert.Equal(t, int64(2), result2.Rows[0][0].(*Node).Properties["count"])
    
    // Verify same behavior in Neo4j
    verifyNeo4jCompatibility(t, query)
}
```

### 6. DELETE Clause

**Neo4j behavior:**
- DELETE removes nodes and relationships
- DETACH DELETE removes node and all relationships
- Cannot delete node with relationships (unless DETACH)

**Implementation checklist:**

```go
func (e *Executor) executeDelete(ctx context.Context, del *Delete) error {
    // ✅ DELETE node (fails if has relationships)
    // ✅ DELETE relationship
    // ✅ DETACH DELETE (removes node and relationships)
    // ✅ Error if node has relationships (without DETACH)
    
    // Implementation...
}
```

### 7. Aggregation Functions

**Neo4j behavior:**
- count(), sum(), avg(), min(), max()
- collect() - collect values into list
- Aggregation with GROUP BY (implicit via WITH)

**Implementation checklist:**

```go
func (e *Executor) executeAggregation(rows [][]interface{}, agg *Aggregation) ([][]interface{}, error) {
    // ✅ count(*) - count all rows
    // ✅ count(n) - count non-null values
    // ✅ sum(n.value) - sum numeric values
    // ✅ avg(n.value) - average
    // ✅ min(n.value), max(n.value)
    // ✅ collect(n.value) - collect into list
    // ✅ GROUP BY (via WITH clause)
    
    // Implementation...
}
```

**Test case:**

```go
func TestAggregation_Compatibility(t *testing.T) {
    // Setup data
    setupData(t, exec, []string{
        `CREATE (a:Person {name: 'Alice', age: 30})`,
        `CREATE (b:Person {name: 'Bob', age: 25})`,
        `CREATE (c:Person {name: 'Charlie', age: 35})`,
    })
    
    tests := []struct {
        name  string
        query string
    }{
        {
            name:  "count all",
            query: "MATCH (n:Person) RETURN count(*)",
        },
        {
            name:  "sum",
            query: "MATCH (n:Person) RETURN sum(n.age)",
        },
        {
            name:  "average",
            query: "MATCH (n:Person) RETURN avg(n.age)",
        },
        {
            name:  "min and max",
            query: "MATCH (n:Person) RETURN min(n.age), max(n.age)",
        },
        {
            name:  "collect",
            query: "MATCH (n:Person) RETURN collect(n.name)",
        },
        {
            name:  "group by",
            query: "MATCH (n:Person) WITH n.age AS age, count(*) AS cnt RETURN age, cnt",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            verifyNeo4jCompatibility(t, tt.query)
        })
    }
}
```

---

## Parameter Substitution

**Neo4j syntax: `$paramName`**

```go
func TestParameters_Compatibility(t *testing.T) {
    tests := []struct {
        name   string
        query  string
        params map[string]interface{}
    }{
        {
            name:   "simple parameter",
            query:  "MATCH (n {name: $name}) RETURN n",
            params: map[string]interface{}{"name": "Alice"},
        },
        {
            name:   "multiple parameters",
            query:  "MATCH (n) WHERE n.age > $minAge AND n.age < $maxAge RETURN n",
            params: map[string]interface{}{"minAge": 25, "maxAge": 40},
        },
        {
            name:   "list parameter",
            query:  "MATCH (n) WHERE n.age IN $ages RETURN n",
            params: map[string]interface{}{"ages": []int{25, 30, 35}},
        },
        {
            name:   "map parameter",
            query:  "CREATE (n:Person $props) RETURN n",
            params: map[string]interface{}{
                "props": map[string]interface{}{
                    "name": "Alice",
                    "age":  30,
                },
            },
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            verifyNeo4jCompatibility(t, tt.query, tt.params)
        })
    }
}
```

---

## Error Handling

**Match Neo4j error codes and messages:**

```go
// Neo4j error codes
const (
    ErrSyntaxError          = "Neo.ClientError.Statement.SyntaxError"
    ErrSemanticError        = "Neo.ClientError.Statement.SemanticError"
    ErrTypeError            = "Neo.ClientError.Statement.TypeError"
    ErrConstraintViolation  = "Neo.ClientError.Schema.ConstraintValidationFailed"
    ErrEntityNotFound       = "Neo.ClientError.Statement.EntityNotFound"
)

type CypherError struct {
    Code    string
    Message string
}

func (e *CypherError) Error() string {
    return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Example usage
func (e *Executor) Execute(ctx context.Context, query string) (*Result, error) {
    ast, err := e.parser.Parse(query)
    if err != nil {
        return nil, &CypherError{
            Code:    ErrSyntaxError,
            Message: fmt.Sprintf("Invalid syntax: %v", err),
        }
    }
    
    // ... more execution
}
```

---

## Compatibility Testing Strategy

### 1. Unit Tests

Test individual Cypher features:

```go
func TestCypher_Match(t *testing.T) { /* ... */ }
func TestCypher_Where(t *testing.T) { /* ... */ }
func TestCypher_Return(t *testing.T) { /* ... */ }
```

### 2. Integration Tests

Test against real Neo4j:

```go
func TestNeo4jCompatibility(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping Neo4j integration test")
    }
    
    // Run same queries in both databases
    // Compare results
}
```

### 3. Regression Tests

Test bug fixes remain fixed:

```go
func TestBug_WhereIsNotNull(t *testing.T) {
    // Ensure bug doesn't reoccur
}
```

### 4. Documentation Examples

Test all examples from docs:

```go
func TestDocumentationExamples(t *testing.T) {
    // Test every example from documentation
}
```

---

## Compatibility Checklist

Before claiming Neo4j compatibility:

- [ ] Feature works identically to Neo4j
- [ ] Same syntax supported
- [ ] Same results returned
- [ ] Same error codes used
- [ ] Parameter substitution works
- [ ] Edge cases handled
- [ ] Integration test passes
- [ ] Documentation updated
- [ ] Examples tested

---

## Quick Reference

### Compatibility Test Template

```go
func TestCypherCompatibility_[Feature](t *testing.T) {
    // 1. Setup identical data
    setupData(t, nornicExec, neo4jSession)
    
    // 2. Execute same query
    query := "MATCH (n) RETURN n"
    nornicResult := executeNornic(t, nornicExec, query, nil)
    neo4jResult := executeNeo4j(t, neo4jSession, query, nil)
    
    // 3. Compare results
    assertResultsEqual(t, neo4jResult, nornicResult)
}
```

### Running Compatibility Tests

```bash
# Run all compatibility tests
go test ./pkg/cypher -run TestCypherCompatibility -v

# Run specific compatibility test
go test ./pkg/cypher -run TestCypherCompatibility_Match -v

# Skip integration tests (no Neo4j required)
go test ./pkg/cypher -short
```

---

**Remember**: Neo4j compatibility is critical for NornicDB's value proposition. Always verify behavior matches Neo4j exactly.
