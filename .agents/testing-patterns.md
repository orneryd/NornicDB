# Testing Patterns & Best Practices

**Purpose**: Comprehensive guide to testing in NornicDB  
**Audience**: AI coding agents  
**Coverage Target**: 90%+ for all code

---

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Test Structure](#test-structure)
3. [Table-Driven Tests](#table-driven-tests)
4. [Bug Regression Tests](#bug-regression-tests)
5. [Integration Tests](#integration-tests)
6. [Benchmark Tests](#benchmark-tests)
7. [Race Condition Tests](#race-condition-tests)
8. [Mock & Stub Patterns](#mock--stub-patterns)
9. [Coverage Requirements](#coverage-requirements)
10. [Common Patterns from Codebase](#common-patterns-from-codebase)

---

## Testing Philosophy

### Test Pyramid

```
        ┌──────────────┐
        │  E2E Tests   │  5% - Full system integration
        │   (Slow)     │
        ├──────────────┤
        │ Integration  │  15% - Multi-component
        │    Tests     │
        ├──────────────┤
        │  Unit Tests  │  80% - Single function/method
        │   (Fast)     │
        └──────────────┘
```

### Key Principles

1. **Fast Feedback** - Unit tests run in milliseconds
2. **Isolated** - Tests don't depend on each other
3. **Repeatable** - Same input = same output
4. **Self-Validating** - Pass/fail is automatic
5. **Timely** - Written before or with code (TDD)

---

## Test Structure

### Standard Test Function

```go
func TestFeatureName(t *testing.T) {
    // 1. ARRANGE - Setup test data and dependencies
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    // Create test data
    setupTestData(t, store)
    
    // 2. ACT - Execute the operation being tested
    result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n", nil)
    
    // 3. ASSERT - Verify expected outcomes
    require.NoError(t, err, "Query should execute without error")
    assert.Len(t, result.Rows, 3, "Should return 3 people")
    assert.Equal(t, "Alice", result.Rows[0][0].(string))
}
```

### Subtests for Variations

```go
func TestNodeValidation(t *testing.T) {
    t.Run("valid node", func(t *testing.T) {
        node := &Node{ID: "123", Labels: []string{"Person"}}
        err := ValidateNode(node)
        assert.NoError(t, err)
    })
    
    t.Run("empty ID", func(t *testing.T) {
        node := &Node{ID: "", Labels: []string{"Person"}}
        err := ValidateNode(node)
        assert.Error(t, err)
        assert.Contains(t, err.Error(), "empty ID")
    })
    
    t.Run("nil labels", func(t *testing.T) {
        node := &Node{ID: "123", Labels: nil}
        err := ValidateNode(node)
        assert.NoError(t, err, "Labels are optional")
    })
}
```

---

## Table-Driven Tests

**Use for testing multiple inputs/outputs:**

```go
func TestTypeConversion(t *testing.T) {
    tests := []struct {
        name     string
        input    interface{}
        expected int64
        wantErr  bool
    }{
        {
            name:     "integer",
            input:    42,
            expected: 42,
            wantErr:  false,
        },
        {
            name:     "float64",
            input:    42.0,
            expected: 42,
            wantErr:  false,
        },
        {
            name:     "string number",
            input:    "42",
            expected: 42,
            wantErr:  false,
        },
        {
            name:     "invalid string",
            input:    "not a number",
            expected: 0,
            wantErr:  true,
        },
        {
            name:     "nil",
            input:    nil,
            expected: 0,
            wantErr:  true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := toInt64(tt.input)
            
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
                assert.Equal(t, tt.expected, result)
            }
        })
    }
}
```

### Real Example from Codebase

```go
// pkg/cypher/functions_test.go
func TestStringFunctions(t *testing.T) {
    tests := []struct {
        name     string
        query    string
        expected interface{}
    }{
        {
            name:     "toLower",
            query:    "RETURN toLower('HELLO') as result",
            expected: "hello",
        },
        {
            name:     "toUpper",
            query:    "RETURN toUpper('hello') as result",
            expected: "HELLO",
        },
        {
            name:     "trim",
            query:    "RETURN trim('  hello  ') as result",
            expected: "hello",
        },
    }
    
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := exec.Execute(ctx, tt.query, nil)
            require.NoError(t, err)
            require.Len(t, result.Rows, 1)
            assert.Equal(t, tt.expected, result.Rows[0][0])
        })
    }
}
```

---

## Bug Regression Tests

**MANDATORY for all bugs** - Prevent regressions forever.

### Pattern from Codebase

```go
// pkg/cypher/aggregation_bugs_test.go

// ====================================================================================
// BUG #1: WHERE ... IS NOT NULL combined with WITH aggregation returns empty results
// ====================================================================================

func TestBug_WhereIsNotNullWithAggregation(t *testing.T) {
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    // Setup: Create test data that triggers the bug
    setupAggregationTestData(t, store)
    
    t.Run("WHERE IS NOT NULL before WITH aggregation", func(t *testing.T) {
        // This is the exact query that fails in production
        // Expected: returns 2 rows (.ts=2, .md=3)
        // Actual bug: returns 0 rows
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.extension IS NOT NULL
            WITH f.extension as ext, COUNT(f) as count
            RETURN ext, count
            ORDER BY count DESC
        `, nil)
        
        require.NoError(t, err, "Query should execute without error")
        require.GreaterOrEqual(t, len(result.Rows), 2, 
            "Should return at least 2 rows (one per extension)")
        
        // Verify exact counts
        extCounts := make(map[string]int64)
        for _, row := range result.Rows {
            if row[0] != nil {
                ext := row[0].(string)
                count := row[1].(int64)
                extCounts[ext] = count
            }
        }
        
        assert.Equal(t, int64(2), extCounts[".ts"], ".ts should have 2 files")
        assert.Equal(t, int64(3), extCounts[".md"], ".md should have 3 files")
    })
    
    // Add variations to catch similar bugs
    t.Run("WHERE IS NOT NULL with ORDER BY", func(t *testing.T) {
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.extension IS NOT NULL
            RETURN f.extension
            ORDER BY f.extension
        `, nil)
        require.NoError(t, err)
        assert.GreaterOrEqual(t, len(result.Rows), 5)
    })
    
    t.Run("WHERE IS NOT NULL with LIMIT", func(t *testing.T) {
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.extension IS NOT NULL
            RETURN f.extension
            LIMIT 3
        `, nil)
        require.NoError(t, err)
        assert.Len(t, result.Rows, 3)
    })
}

// Helper function to setup test data
func setupAggregationTestData(t *testing.T, store *storage.MemoryEngine) {
    ctx := context.Background()
    exec := NewStorageExecutor(store)
    
    queries := []string{
        `CREATE (f:File {id: 'file1', extension: '.ts'})`,
        `CREATE (f:File {id: 'file2', extension: '.ts'})`,
        `CREATE (f:File {id: 'file3', extension: '.md'})`,
        `CREATE (f:File {id: 'file4', extension: '.md'})`,
        `CREATE (f:File {id: 'file5', extension: '.md'})`,
        // Files without extension (to test IS NOT NULL filtering)
        `CREATE (f:File {id: 'file6', path: '/test/noext'})`,
        `CREATE (f:File {id: 'file7', path: '/test/noext2'})`,
    }
    
    for _, q := range queries {
        _, err := exec.Execute(ctx, q, nil)
        require.NoError(t, err)
    }
}
```

### Bug Test Template

```go
// ====================================================================================
// BUG #N: [Brief description of the bug]
// ====================================================================================
// Discovered: [Date]
// Reporter: [Who found it]
// Impact: [What breaks]
// Root Cause: [Why it happened]
// ====================================================================================

func TestBug_DescriptiveName(t *testing.T) {
    // Setup: Create minimal conditions that trigger bug
    store := storage.NewMemoryEngine()
    setupBugConditions(t, store)
    
    t.Run("exact reproduction", func(t *testing.T) {
        // Execute the exact operation that failed
        result, err := performBuggyOperation()
        
        // Assert expected behavior (this failed before fix)
        require.NoError(t, err)
        assert.Equal(t, expectedValue, result)
    })
    
    t.Run("variation 1", func(t *testing.T) {
        // Test related scenarios
    })
    
    t.Run("variation 2", func(t *testing.T) {
        // Test edge cases
    })
}
```

---

## Integration Tests

**Test multiple components working together:**

```go
// pkg/bolt/integration_test.go

func TestBoltServerIntegration(t *testing.T) {
    // Setup: Start real server with real storage
    store := storage.NewMemoryEngine()
    server := bolt.NewServer(store, ":7687")
    
    go server.Start()
    defer server.Stop()
    
    // Wait for server to be ready
    time.Sleep(100 * time.Millisecond)
    
    t.Run("connect and execute query", func(t *testing.T) {
        // Use real Neo4j driver
        driver, err := neo4j.NewDriver("bolt://localhost:7687", 
            neo4j.NoAuth())
        require.NoError(t, err)
        defer driver.Close()
        
        session := driver.NewSession(neo4j.SessionConfig{})
        defer session.Close()
        
        // Execute real Cypher query
        result, err := session.Run(
            "CREATE (n:Person {name: $name}) RETURN n",
            map[string]interface{}{"name": "Alice"},
        )
        require.NoError(t, err)
        
        // Verify result
        record, err := result.Single()
        require.NoError(t, err)
        
        node := record.Values[0].(neo4j.Node)
        assert.Equal(t, "Alice", node.Props["name"])
    })
    
    t.Run("multiple concurrent connections", func(t *testing.T) {
        // Test connection pooling
        var wg sync.WaitGroup
        for i := 0; i < 10; i++ {
            wg.Add(1)
            go func(id int) {
                defer wg.Done()
                
                driver, _ := neo4j.NewDriver("bolt://localhost:7687", 
                    neo4j.NoAuth())
                defer driver.Close()
                
                session := driver.NewSession(neo4j.SessionConfig{})
                defer session.Close()
                
                _, err := session.Run(
                    fmt.Sprintf("CREATE (n:Test {id: %d})", id),
                    nil,
                )
                assert.NoError(t, err)
            }(i)
        }
        wg.Wait()
        
        // Verify all nodes created
        session := driver.NewSession(neo4j.SessionConfig{})
        result, _ := session.Run("MATCH (n:Test) RETURN count(n)", nil)
        record, _ := result.Single()
        assert.Equal(t, int64(10), record.Values[0])
    })
}
```

---

## Benchmark Tests

**Measure performance and prevent regressions:**

```go
// pkg/cypher/executor_test.go

func BenchmarkQueryExecution(b *testing.B) {
    store := setupBenchmarkData()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    b.Run("simple MATCH", func(b *testing.B) {
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
            _, err := exec.Execute(ctx, 
                "MATCH (n:Person) RETURN n LIMIT 100", nil)
            if err != nil {
                b.Fatal(err)
            }
        }
    })
    
    b.Run("MATCH with WHERE", func(b *testing.B) {
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
            _, err := exec.Execute(ctx, 
                "MATCH (n:Person) WHERE n.age > 25 RETURN n", nil)
            if err != nil {
                b.Fatal(err)
            }
        }
    })
    
    b.Run("relationship traversal", func(b *testing.B) {
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
            _, err := exec.Execute(ctx, 
                "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b", nil)
            if err != nil {
                b.Fatal(err)
            }
        }
    })
}

// Setup benchmark data once
func setupBenchmarkData() *storage.MemoryEngine {
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    // Create 1000 nodes
    for i := 0; i < 1000; i++ {
        exec.Execute(ctx, fmt.Sprintf(
            "CREATE (n:Person {id: %d, age: %d})", i, 20+i%50), nil)
    }
    
    // Create 5000 relationships
    for i := 0; i < 5000; i++ {
        exec.Execute(ctx, fmt.Sprintf(
            "MATCH (a:Person {id: %d}), (b:Person {id: %d}) "+
            "CREATE (a)-[:KNOWS]->(b)", i%1000, (i+1)%1000), nil)
    }
    
    return store
}
```

### Benchmark Output Analysis

```bash
$ go test -bench=. -benchmem

BenchmarkQueryExecution/simple_MATCH-8           4252 ops/sec   2.3 MB/op
BenchmarkQueryExecution/MATCH_with_WHERE-8       3891 ops/sec   2.1 MB/op
BenchmarkQueryExecution/relationship_traversal-8 2769 ops/sec   3.4 MB/op
```

**What to look for:**
- **ops/sec**: Higher is better
- **MB/op**: Lower is better
- **Regressions**: >5% slower = investigate

---

## Race Condition Tests

**Detect concurrency bugs:**

```go
func TestConcurrentAccess(t *testing.T) {
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    // Create initial data
    exec.Execute(ctx, "CREATE (n:Counter {value: 0})", nil)
    
    // Concurrent writes
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            
            // Increment counter
            _, err := exec.Execute(ctx, `
                MATCH (n:Counter)
                SET n.value = n.value + 1
            `, nil)
            assert.NoError(t, err)
        }()
    }
    wg.Wait()
    
    // Verify final value
    result, err := exec.Execute(ctx, 
        "MATCH (n:Counter) RETURN n.value", nil)
    require.NoError(t, err)
    
    // Should be 100 if properly synchronized
    value := result.Rows[0][0].(int64)
    assert.Equal(t, int64(100), value, 
        "Race condition detected: concurrent updates lost")
}
```

**Run with race detector:**
```bash
go test ./... -race
```

---

## Mock & Stub Patterns

### Interface-Based Mocking

```go
// Define interface for dependency
type StorageEngine interface {
    GetNode(id NodeID) (*Node, error)
    CreateNode(node *Node) error
}

// Mock implementation for testing
type MockStorage struct {
    nodes map[NodeID]*Node
    calls []string  // Track method calls
}

func (m *MockStorage) GetNode(id NodeID) (*Node, error) {
    m.calls = append(m.calls, fmt.Sprintf("GetNode(%s)", id))
    
    node, ok := m.nodes[id]
    if !ok {
        return nil, ErrNotFound
    }
    return node, nil
}

func (m *MockStorage) CreateNode(node *Node) error {
    m.calls = append(m.calls, fmt.Sprintf("CreateNode(%s)", node.ID))
    
    if _, exists := m.nodes[node.ID]; exists {
        return ErrAlreadyExists
    }
    m.nodes[node.ID] = node
    return nil
}

// Use in tests
func TestWithMock(t *testing.T) {
    mock := &MockStorage{
        nodes: make(map[NodeID]*Node),
    }
    
    service := NewService(mock)
    
    // Test behavior
    err := service.DoSomething()
    assert.NoError(t, err)
    
    // Verify interactions
    assert.Contains(t, mock.calls, "GetNode(123)")
    assert.Len(t, mock.calls, 2, "Should call storage twice")
}
```

### Function Mocking

```go
// Use function types for easy mocking
type EmbeddingFunc func(text string) ([]float32, error)

type SearchEngine struct {
    embedder EmbeddingFunc
}

// Test with mock function
func TestSearchWithMockEmbeddings(t *testing.T) {
    mockEmbedder := func(text string) ([]float32, error) {
        // Return predictable embeddings for testing
        return []float32{0.1, 0.2, 0.3}, nil
    }
    
    engine := &SearchEngine{embedder: mockEmbedder}
    
    results, err := engine.Search("test query")
    assert.NoError(t, err)
    assert.Len(t, results, 3)
}
```

---

## Coverage Requirements

### Measuring Coverage

```bash
# Generate coverage report
go test ./... -coverprofile=coverage.out

# View summary
go tool cover -func=coverage.out

# View HTML report
go tool cover -html=coverage.out
```

### Coverage Targets

- **Core logic** (cypher, storage): **95%+**
- **Utilities** (cache, pool): **90%+**
- **Integration** (bolt, server): **85%+**
- **UI/CLI**: **70%+** (focus on business logic)

### What to Test

✅ **Always test:**
- Happy path (normal usage)
- Error conditions (invalid input, not found)
- Edge cases (empty, nil, boundary values)
- Concurrency (race conditions)
- All error returns
- All branches (if/else, switch cases)

❌ **Don't waste time:**
- Third-party library internals
- Generated code (unless custom logic)
- Trivial getters/setters (unless they have logic)
- Private functions (test through public API)

### Example Coverage Report

```
github.com/orneryd/nornicdb/pkg/cypher/executor.go:45:    Execute         95.2%
github.com/orneryd/nornicdb/pkg/cypher/parser.go:23:     Parse           92.8%
github.com/orneryd/nornicdb/pkg/storage/memory.go:67:    CreateNode      98.1%
github.com/orneryd/nornicdb/pkg/storage/badger.go:89:    GetNode         94.5%
---------------------------------------------------------------
TOTAL                                                           94.7%
```

---

## Common Patterns from Codebase

### Pattern 1: Setup Helper Functions

```go
// Reusable setup for multiple tests
func setupTestExecutor(t *testing.T) (*StorageExecutor, *storage.MemoryEngine) {
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    return exec, store
}

func setupTestData(t *testing.T, store *storage.MemoryEngine) {
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    queries := []string{
        `CREATE (a:Person {name: 'Alice', age: 30})`,
        `CREATE (b:Person {name: 'Bob', age: 25})`,
        `CREATE (a)-[:KNOWS]->(b)`,
    }
    
    for _, q := range queries {
        _, err := exec.Execute(ctx, q, nil)
        require.NoError(t, err)
    }
}
```

### Pattern 2: Assertion Helpers

```go
// Helper to assert query results
func assertQueryResult(t *testing.T, exec *Executor, query string, 
    expectedRows int, expectedCols int) {
    
    result, err := exec.Execute(context.Background(), query, nil)
    require.NoError(t, err, "Query should execute without error")
    assert.Len(t, result.Rows, expectedRows, 
        "Should return %d rows", expectedRows)
    
    if len(result.Rows) > 0 {
        assert.Len(t, result.Rows[0], expectedCols, 
            "Should return %d columns", expectedCols)
    }
}
```

### Pattern 3: Error Testing

```go
func TestErrorHandling(t *testing.T) {
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    t.Run("syntax error", func(t *testing.T) {
        _, err := exec.Execute(ctx, "INVALID QUERY", nil)
        assert.Error(t, err)
        assert.Contains(t, err.Error(), "syntax error")
    })
    
    t.Run("node not found", func(t *testing.T) {
        _, err := exec.Execute(ctx, 
            "MATCH (n:Person {id: 'nonexistent'}) RETURN n", nil)
        assert.NoError(t, err, "Query executes but returns empty")
        // Empty result is not an error in Cypher
    })
    
    t.Run("type mismatch", func(t *testing.T) {
        exec.Execute(ctx, "CREATE (n:Test {value: 'string'})", nil)
        
        _, err := exec.Execute(ctx, 
            "MATCH (n:Test) WHERE n.value > 10 RETURN n", nil)
        assert.Error(t, err)
        assert.Contains(t, err.Error(), "type mismatch")
    })
}
```

---

## Quick Reference

### Test Checklist

For every new feature or bug fix:

- [ ] Unit tests for core logic
- [ ] Table-driven tests for multiple inputs
- [ ] Error path tests
- [ ] Edge case tests (nil, empty, boundary)
- [ ] Integration tests (if multi-component)
- [ ] Benchmark tests (if performance-critical)
- [ ] Race condition tests (`go test -race`)
- [ ] Coverage ≥90% (`go tool cover`)
- [ ] All tests pass (`go test ./... -v`)

### Common Test Commands

```bash
# Run all tests
go test ./... -v

# Run specific package
go test ./pkg/cypher -v

# Run specific test
go test ./pkg/cypher -run TestBug_WhereIsNotNull -v

# Run with coverage
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out

# Run benchmarks
go test ./pkg/cypher -bench=. -benchmem

# Check for race conditions
go test ./... -race

# Run tests in parallel
go test ./... -parallel 8
```

---

**Remember**: Tests are documentation. Write them clearly so future developers (including AI agents) understand what the code does and why.
