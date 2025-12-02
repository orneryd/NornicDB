# NornicDB Development Guide for AI Agents

**Version:** 1.0.0  
**Last Updated:** 2024-12-01  
**Target Audience:** AI coding agents (Claude, GPT-4, etc.)

---

## Core Philosophy

NornicDB is an **enterprise-grade, high-performance graph database** that prioritizes:

1. **Neo4j Compatibility** - Drop-in replacement with zero code changes
2. **Performance** - 3-52x faster than Neo4j across benchmarks
3. **Correctness** - 90%+ test coverage, regression prevention mandatory
4. **Maintainability** - Clean architecture, separation of concerns, DRY principles
5. **Documentation** - Every public API fully documented with real-world examples

---

## 🎯 Golden Rules

### 1. **Prove Value Before Merging**

Every change must demonstrate **measurable improvements** without significant regressions:

- **Performance**: Benchmark before/after. Show ops/sec improvements.
- **Memory**: Profile memory usage. No >10% increases without justification.
- **Code Quality**: Maintain or improve test coverage (90% minimum).
- **Compatibility**: All Neo4j compatibility tests must pass.

**Example:**
```go
// ❌ BAD: No proof of improvement
func optimizeQuery() { /* new algorithm */ }

// ✅ GOOD: Benchmarked and documented
// BenchmarkQueryOptimization shows 2.3x speedup:
//   Before: 4,252 ops/sec
//   After:  9,780 ops/sec
//   Memory: -15% (reduced allocations)
func optimizeQuery() { /* new algorithm with proof */ }
```

### 2. **Test-Driven Bug Fixes**

**MANDATORY WORKFLOW** for all bugs:

1. **Write failing test** - Reproduce the exact bug condition
2. **Verify test fails** - Confirm it catches the bug
3. **Fix the bug** - Implement minimal fix
4. **Verify test passes** - Confirm fix works
5. **Add regression tests** - Prevent future occurrences

**Example from codebase:**
```go
// File: pkg/cypher/aggregation_bugs_test.go
// BUG #1: WHERE ... IS NOT NULL combined with WITH aggregation returns empty results

func TestBug_WhereIsNotNullWithAggregation(t *testing.T) {
    // 1. Setup test data that triggers bug
    setupAggregationTestData(t, store)
    
    // 2. Execute query that fails in production
    result, err := exec.Execute(ctx, `
        MATCH (f:File)
        WHERE f.extension IS NOT NULL
        WITH f.extension as ext, COUNT(f) as count
        RETURN ext, count
    `, nil)
    
    // 3. Assert expected behavior (this WILL fail before fix)
    require.NoError(t, err)
    assert.Equal(t, int64(2), extCounts[".ts"])
    assert.Equal(t, int64(3), extCounts[".md"])
}
```

See [.agents/testing-patterns.md](.agents/testing-patterns.md) for detailed testing guidelines.

### 3. **File Size Limits**

**Hard limit: 2500 lines per file**

When a file approaches 2500 lines:

1. **Identify separation of concerns** - What are the distinct responsibilities?
2. **Define clear contracts** - What interfaces connect the pieces?
3. **Extract cohesive modules** - Group related functionality
4. **Update imports** - Maintain clean dependency graph

**Example refactoring:**
```go
// Before: executor.go (3752 lines) ❌
// - Query parsing
// - Execution logic
// - Result formatting
// - Caching
// - Optimization

// After: Split into focused modules ✅
// executor.go (800 lines)      - Core execution orchestration
// parser.go (600 lines)         - Query parsing & AST
// optimizer.go (500 lines)      - Query optimization
// cache.go (400 lines)          - Result caching
// formatter.go (300 lines)      - Result formatting
```

See [.agents/refactoring-guidelines.md](.agents/refactoring-guidelines.md) for detailed strategies.

### 4. **Functional Go Patterns**

**Leverage function types for dependency injection:**

```go
// ✅ GOOD: Function interface enables runtime swapping
type EmbeddingFunc func(text string) ([]float32, error)

type SearchEngine struct {
    embedder EmbeddingFunc  // Inject different implementations
}

// Production: Use GPU-accelerated embeddings
engine := &SearchEngine{
    embedder: gpuEmbeddings.Generate,
}

// Testing: Use mock embeddings
testEngine := &SearchEngine{
    embedder: func(text string) ([]float32, error) {
        return []float32{0.1, 0.2, 0.3}, nil
    },
}

// Development: Use cached embeddings
devEngine := &SearchEngine{
    embedder: cachedEmbeddings.GetOrGenerate,
}
```

**Real example from codebase:**
```go
// pkg/storage/types.go - Storage interface for DI
type Engine interface {
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    // ... more methods
}

// Multiple implementations:
// - MemoryEngine (testing)
// - BadgerEngine (production)
// - MockEngine (unit tests)
```

See [.agents/functional-patterns.md](.agents/functional-patterns.md) for advanced patterns.

### 5. **Documentation Standards**

**Every public API must have:**

1. **Package-level documentation** - What does this package do?
2. **Type documentation** - What does this type represent?
3. **Function documentation** - What does this do? When to use it?
4. **1-3 Real-world examples** - Show actual usage
5. **ELI12 explanation** - Complex concepts explained simply

**Example from codebase:**
```go
// Package cypher provides Neo4j-compatible Cypher query execution for NornicDB.
//
// This package implements a Cypher query parser and executor that supports
// the core Neo4j Cypher query language features. It enables NornicDB to be
// compatible with existing Neo4j applications and tools.
//
// Supported Cypher Features:
//   - MATCH: Pattern matching with node and relationship patterns
//   - CREATE: Creating nodes and relationships
//   - MERGE: Upsert operations with ON CREATE/ON MATCH clauses
//   [... more features ...]
//
// Example Usage:
//
//	// Create executor with storage backend
//	storage := storage.NewMemoryEngine()
//	executor := cypher.NewStorageExecutor(storage)
//
//	// Execute Cypher queries
//	result, err := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice'})", nil)
//
// ELI12 (Explain Like I'm 12):
//
// Think of Cypher like asking questions about a social network:
//   1. MATCH: "Find all people named Alice"
//   2. CREATE: "Add a new person named Bob"
//   3. Relationships: "Find who Alice knows"
package cypher
```

See [.agents/documentation-standards.md](.agents/documentation-standards.md) for complete guide.

### 6. **Test Coverage Requirements**

**Minimum 90% coverage for all new code:**

```bash
# Run coverage report
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# View HTML coverage report
go tool cover -html=coverage.out
```

**Coverage targets by package type:**

- **Core logic** (cypher, storage): 95%+ coverage
- **Utilities** (cache, pool): 90%+ coverage
- **Integration** (bolt, server): 85%+ coverage
- **UI/CLI**: 70%+ coverage (focus on business logic)

**What to test:**

✅ **Always test:**
- Happy path (normal usage)
- Error conditions (invalid input, not found, etc.)
- Edge cases (empty, nil, boundary values)
- Concurrency (race conditions, deadlocks)
- Regression cases (all bugs get tests)

❌ **Don't waste time testing:**
- Third-party library internals
- Generated code (unless custom logic added)
- Trivial getters/setters (unless they have logic)

See [.agents/testing-patterns.md](.agents/testing-patterns.md) for comprehensive testing guide.

### 7. **DRY Principle (Don't Repeat Yourself)**

**Identify and extract common patterns:**

```go
// ❌ BAD: Repeated error handling
func GetUser(id string) (*User, error) {
    if id == "" {
        return nil, fmt.Errorf("invalid id: empty")
    }
    // ... logic
}

func GetPost(id string) (*Post, error) {
    if id == "" {
        return nil, fmt.Errorf("invalid id: empty")
    }
    // ... logic
}

// ✅ GOOD: Extract common validation
func validateID(id string) error {
    if id == "" {
        return fmt.Errorf("invalid id: empty")
    }
    return nil
}

func GetUser(id string) (*User, error) {
    if err := validateID(id); err != nil {
        return nil, err
    }
    // ... logic
}
```

**Real example from codebase:**
```go
// pkg/cypher/helpers.go - Shared helper functions
func toInt64(v interface{}) (int64, error) { /* ... */ }
func toString(v interface{}) (string, error) { /* ... */ }
func toBool(v interface{}) (bool, error) { /* ... */ }

// Used throughout cypher package instead of repeating type conversions
```

### 8. **Separation of Concerns**

**Clear boundaries between layers:**

```
┌─────────────────────────────────────┐
│   API Layer (bolt, http, mcp)       │  ← Protocol handling
├─────────────────────────────────────┤
│   Query Layer (cypher)               │  ← Query parsing & execution
├─────────────────────────────────────┤
│   Storage Layer (storage)            │  ← Data persistence
├─────────────────────────────────────┤
│   Infrastructure (cache, pool)       │  ← Cross-cutting concerns
└─────────────────────────────────────┘
```

**Each layer has clear responsibilities:**

- **API Layer**: Protocol translation, authentication, request/response formatting
- **Query Layer**: Cypher parsing, query planning, execution orchestration
- **Storage Layer**: CRUD operations, indexing, transactions
- **Infrastructure**: Caching, connection pooling, metrics

**Example contract:**
```go
// Storage layer exposes clean interface
type Engine interface {
    CreateNode(node *Node) error
    GetNode(id NodeID) (*Node, error)
    // ... more methods
}

// Query layer depends on interface, not implementation
type Executor struct {
    storage Engine  // Can swap implementations
}

// API layer depends on query layer, not storage
type BoltServer struct {
    executor *Executor  // Clean dependency chain
}
```

See [.agents/architecture-patterns.md](.agents/architecture-patterns.md) for detailed architecture.

### 9. **Follow Industry Standards**

**Use established patterns and conventions:**

- **Go conventions**: Follow [Effective Go](https://golang.org/doc/effective_go.html)
- **Neo4j compatibility**: Match Neo4j Cypher semantics exactly
- **Bolt protocol**: Implement Neo4j Bolt protocol specification
- **Graph algorithms**: Use standard graph theory algorithms
- **Testing**: Follow Go testing best practices (testify, table-driven tests)

**Example - Table-driven tests:**
```go
func TestNodeValidation(t *testing.T) {
    tests := []struct {
        name    string
        node    *Node
        wantErr bool
    }{
        {
            name: "valid node",
            node: &Node{ID: "123", Labels: []string{"Person"}},
            wantErr: false,
        },
        {
            name: "empty ID",
            node: &Node{ID: "", Labels: []string{"Person"}},
            wantErr: true,
        },
        {
            name: "nil labels",
            node: &Node{ID: "123", Labels: nil},
            wantErr: false,  // Labels are optional
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := ValidateNode(tt.node)
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}
```

### 10. **Open Standards & Broad Support**

**Prioritize compatibility without sacrificing performance:**

✅ **Support:**
- Neo4j Bolt protocol (industry standard)
- Cypher query language (open standard)
- JSON import/export (universal format)
- Standard graph algorithms (shortest path, PageRank, etc.)
- Multiple embedding providers (Ollama, OpenAI, local GGUF)

❌ **Avoid:**
- Vendor lock-in (proprietary formats)
- Single-provider dependencies (must support alternatives)
- Non-standard query syntax (unless significant benefit)

**Example - Multi-provider embeddings:**
```go
// Support multiple embedding providers
type EmbeddingProvider interface {
    Generate(text string) ([]float32, error)
}

// Implementations:
// - OllamaProvider (local)
// - OpenAIProvider (cloud)
// - LocalGGUFProvider (offline)
// - MockProvider (testing)
```

---

## 📁 Repository Structure

```
nornicdb/
├── cmd/
│   └── nornicdb/          # Main application entry point
├── pkg/
│   ├── bolt/              # Neo4j Bolt protocol server
│   ├── cypher/            # Cypher query parser & executor
│   ├── storage/           # Storage engine (memory, badger)
│   ├── embed/             # Embedding generation (GPU-accelerated)
│   ├── search/            # Vector search (HNSW index)
│   ├── inference/         # Auto-relationship inference
│   ├── cache/             # Query result caching
│   ├── pool/              # Connection pooling
│   ├── config/            # Configuration management
│   ├── audit/             # Audit logging
│   └── auth/              # Authentication
├── docs/                  # Documentation
│   ├── getting-started/   # Installation & quick start
│   ├── api-reference/     # API documentation
│   ├── user-guides/       # Usage examples
│   ├── architecture/      # System design
│   └── performance/       # Benchmarks
├── docker/                # Docker build files
├── scripts/               # Build & deployment scripts
├── ui/                    # Web UI (optional)
└── .agents/               # AI agent development guides
    ├── testing-patterns.md
    ├── refactoring-guidelines.md
    ├── functional-patterns.md
    ├── documentation-standards.md
    ├── architecture-patterns.md
    ├── performance-optimization.md
    ├── bug-fix-workflow.md
    └── cypher-compatibility.md
```

---

## 🚀 Development Workflow

### 1. **Before Starting Work**

```bash
# Update dependencies
go mod tidy

# Run full test suite
go test ./... -v

# Check test coverage
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# Run benchmarks (baseline)
go test ./pkg/cypher -bench=. -benchmem > before.txt
```

### 2. **During Development**

```bash
# Run tests for package you're working on
go test ./pkg/cypher -v

# Run specific test
go test ./pkg/cypher -run TestBug_WhereIsNotNull -v

# Watch mode (requires entr or similar)
find . -name "*.go" | entr -c go test ./pkg/cypher -v

# Check for race conditions
go test ./... -race
```

### 3. **Before Committing**

```bash
# Format code
go fmt ./...

# Run linter
golangci-lint run

# Run all tests
go test ./... -v

# Check coverage (must be ≥90%)
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | grep total

# Run benchmarks (compare with baseline)
go test ./pkg/cypher -bench=. -benchmem > after.txt
benchcmp before.txt after.txt

# Build binary
go build -o nornicdb ./cmd/nornicdb
```

### 4. **Commit Message Format**

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `perf`: Performance improvement
- `refactor`: Code refactoring
- `test`: Adding tests
- `docs`: Documentation changes
- `chore`: Maintenance tasks

**Example:**
```
fix(cypher): resolve WHERE IS NOT NULL with aggregation bug

The WHERE clause with IS NOT NULL was incorrectly filtering out all
results when combined with WITH aggregation. This was caused by the
filter being applied after the aggregation instead of before.

Changes:
- Apply WHERE filters before WITH aggregation
- Add regression test TestBug_WhereIsNotNullWithAggregation
- Update executor to preserve filter order

Fixes #123
Benchmark: No performance impact (same ops/sec)
Coverage: +2.3% (added 15 test cases)
```

---

## 🧪 Testing Philosophy

### Test Pyramid

```
        ┌─────────────┐
        │   E2E (5%)  │  ← Full system tests
        ├─────────────┤
        │ Integration │  ← Multi-component tests
        │    (15%)    │
        ├─────────────┤
        │    Unit     │  ← Single-function tests
        │    (80%)    │
        └─────────────┘
```

### Testing Checklist

For every new feature or bug fix:

- [ ] Unit tests for core logic (80% of tests)
- [ ] Integration tests for component interaction (15% of tests)
- [ ] End-to-end tests for critical paths (5% of tests)
- [ ] Benchmark tests for performance-critical code
- [ ] Race condition tests (`go test -race`)
- [ ] Error path tests (all error returns tested)
- [ ] Edge case tests (nil, empty, boundary values)
- [ ] Regression tests (all bugs get permanent tests)

### Example Test Structure

```go
func TestFeatureName(t *testing.T) {
    // Setup
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    t.Run("happy path", func(t *testing.T) {
        // Test normal usage
    })
    
    t.Run("error: invalid input", func(t *testing.T) {
        // Test error handling
    })
    
    t.Run("edge case: empty result", func(t *testing.T) {
        // Test edge cases
    })
    
    t.Run("concurrent access", func(t *testing.T) {
        // Test concurrency
    })
}
```

See [.agents/testing-patterns.md](.agents/testing-patterns.md) for comprehensive testing guide.

---

## 📊 Performance Optimization

### Benchmarking Requirements

**Every performance optimization must include:**

1. **Baseline benchmark** - Before optimization
2. **Optimized benchmark** - After optimization
3. **Comparison** - Show improvement percentage
4. **Memory profile** - Show memory impact
5. **Justification** - Why this optimization matters

**Example:**
```go
// BenchmarkQueryExecution measures query execution performance
func BenchmarkQueryExecution(b *testing.B) {
    store := setupBenchmarkData()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := exec.Execute(ctx, "MATCH (n:Person) RETURN n LIMIT 100", nil)
        if err != nil {
            b.Fatal(err)
        }
    }
}

// Results:
// Before: 4,252 ops/sec, 2.3 MB/op
// After:  9,780 ops/sec, 1.8 MB/op
// Improvement: 2.3x faster, 22% less memory
```

### Optimization Priorities

1. **Algorithmic improvements** - O(n²) → O(n log n)
2. **Memory allocations** - Reduce allocations in hot paths
3. **Concurrency** - Parallelize independent operations
4. **Caching** - Cache expensive computations
5. **I/O optimization** - Batch operations, reduce syscalls

See [.agents/performance-optimization.md](.agents/performance-optimization.md) for detailed strategies.

---

## 🐛 Bug Fix Workflow

**MANDATORY PROCESS** - No exceptions:

### Step 1: Reproduce the Bug

```go
// Create failing test that reproduces the exact bug
func TestBug_DescriptiveName(t *testing.T) {
    // Setup conditions that trigger bug
    store := storage.NewMemoryEngine()
    setupBugConditions(t, store)
    
    // Execute operation that fails
    result, err := performBuggyOperation()
    
    // Assert expected behavior (this WILL fail before fix)
    assert.NoError(t, err)
    assert.Equal(t, expectedValue, result)
}
```

### Step 2: Verify Test Fails

```bash
# Run test - it MUST fail
go test ./pkg/cypher -run TestBug_DescriptiveName -v

# Output should show failure:
# --- FAIL: TestBug_DescriptiveName (0.00s)
#     Expected: 2 rows
#     Got: 0 rows
```

### Step 3: Fix the Bug

```go
// Implement minimal fix
// Add comments explaining WHY the bug occurred
func fixedFunction() {
    // BUG FIX: Previously applied filter after aggregation,
    // causing all results to be filtered out. Now apply
    // filter before aggregation as per Cypher semantics.
    applyFilters()  // ← Moved before aggregation
    performAggregation()
}
```

### Step 4: Verify Test Passes

```bash
# Run test - it MUST pass now
go test ./pkg/cypher -run TestBug_DescriptiveName -v

# Output should show success:
# --- PASS: TestBug_DescriptiveName (0.00s)
```

### Step 5: Add Regression Tests

```go
// Add variations to prevent similar bugs
func TestBug_DescriptiveName_Variations(t *testing.T) {
    t.Run("with ORDER BY", func(t *testing.T) { /* ... */ })
    t.Run("with LIMIT", func(t *testing.T) { /* ... */ })
    t.Run("with multiple filters", func(t *testing.T) { /* ... */ })
}
```

See [.agents/bug-fix-workflow.md](.agents/bug-fix-workflow.md) for detailed workflow.

---

## 📚 Additional Resources

### Detailed Guides

- [Testing Patterns](.agents/testing-patterns.md) - Comprehensive testing guide
- [Refactoring Guidelines](.agents/refactoring-guidelines.md) - How to refactor large files
- [Functional Patterns](.agents/functional-patterns.md) - Advanced Go patterns
- [Documentation Standards](.agents/documentation-standards.md) - Documentation requirements
- [Architecture Patterns](.agents/architecture-patterns.md) - System design principles
- [Performance Optimization](.agents/performance-optimization.md) - Optimization strategies
- [Bug Fix Workflow](.agents/bug-fix-workflow.md) - Bug fixing process
- [Cypher Compatibility](.agents/cypher-compatibility.md) - Neo4j compatibility guide

### External Resources

- [Effective Go](https://golang.org/doc/effective_go.html) - Go best practices
- [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/current/) - Cypher reference
- [Neo4j Bolt Protocol](https://neo4j.com/docs/bolt/current/) - Protocol specification
- [Go Testing](https://golang.org/pkg/testing/) - Testing package documentation

---

## 🎓 Learning Path for New Contributors

### Week 1: Understanding the Codebase

1. Read this AGENTS.md thoroughly
2. Explore `pkg/storage/types.go` - Core data structures
3. Read `pkg/cypher/executor.go` - Query execution
4. Run test suite: `go test ./... -v`
5. Read existing tests to understand patterns

### Week 2: Making Your First Contribution

1. Find a "good first issue" or documentation improvement
2. Follow the development workflow
3. Write tests first (TDD)
4. Submit PR with benchmarks and coverage report

### Week 3: Advanced Topics

1. Study `pkg/cypher/` - Complex query execution
2. Understand `pkg/storage/badger_engine.go` - Persistence
3. Learn `pkg/embed/` - GPU-accelerated embeddings
4. Contribute a performance optimization

---

## ✅ Pre-Commit Checklist

Before submitting any code:

- [ ] All tests pass: `go test ./... -v`
- [ ] Coverage ≥90%: `go tool cover -func=coverage.out`
- [ ] No race conditions: `go test ./... -race`
- [ ] Code formatted: `go fmt ./...`
- [ ] Linter passes: `golangci-lint run`
- [ ] Benchmarks run: `go test -bench=. -benchmem`
- [ ] Documentation updated (if public API changed)
- [ ] Examples added (if new feature)
- [ ] CHANGELOG.md updated
- [ ] Commit message follows format

---

## 🤝 Code Review Standards

### What Reviewers Look For

1. **Correctness**: Does it work? Are there edge cases?
2. **Tests**: 90%+ coverage? All error paths tested?
3. **Performance**: Benchmarks provided? No regressions?
4. **Documentation**: Public APIs documented? Examples included?
5. **Style**: Follows Go conventions? DRY principle?
6. **Architecture**: Separation of concerns? Clean interfaces?

### Common Review Comments

- "Add test for error case"
- "Extract this repeated logic"
- "Document this public function"
- "Show benchmark results"
- "This file is >2500 lines, please refactor"
- "Add ELI12 explanation for this complex concept"

---

## 🎯 Success Metrics

### Code Quality Metrics

- **Test Coverage**: ≥90% for all packages
- **Benchmark Performance**: No regressions >5%
- **Memory Usage**: No increases >10% without justification
- **File Size**: No files >2500 lines
- **Documentation**: 100% of public APIs documented

### Compatibility Metrics

- **Neo4j Compatibility**: 100% of supported Cypher features work identically
- **Bolt Protocol**: Full protocol compliance
- **Import/Export**: Neo4j JSON format compatibility

### Performance Metrics

- **Query Execution**: 3-52x faster than Neo4j (maintain or improve)
- **Memory Footprint**: 100-500 MB vs 1-4 GB for Neo4j
- **Cold Start**: <1s vs 10-30s for Neo4j

---

## 📞 Getting Help

- **Documentation**: Check `docs/` directory first
- **Examples**: See `docs/user-guides/complete-examples.md`
- **Tests**: Read existing tests for patterns
- **Issues**: Search GitHub issues for similar problems
- **Architecture**: Read `.agents/architecture-patterns.md`

---

**Remember**: Quality over speed. Take time to write tests, document code, and prove improvements. NornicDB's reputation depends on reliability and performance.

**Happy coding! 🚀**
