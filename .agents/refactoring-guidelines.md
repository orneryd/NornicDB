# Refactoring Guidelines

**Purpose**: How to refactor large files and improve code structure  
**Audience**: AI coding agents  
**Hard Limit**: 2500 lines per file

---

## When to Refactor

### File Size Triggers

- **2000 lines**: Start planning refactoring
- **2500 lines**: MUST refactor before adding more code
- **3000+ lines**: Emergency - refactor immediately

### Code Smell Triggers

- Multiple unrelated responsibilities in one file
- Repeated code patterns (DRY violations)
- Difficult to test (too many dependencies)
- Hard to understand (complex logic)
- Frequent merge conflicts (hotspot file)

---

## Refactoring Process

### Step 1: Analyze Current Structure

**Identify distinct responsibilities:**

```bash
# Count lines per file
wc -l pkg/cypher/*.go | sort -n

# Find large files
find pkg -name "*.go" -exec wc -l {} \; | sort -n | tail -20

# Analyze file structure
grep "^func " pkg/cypher/executor.go | wc -l  # Count functions
grep "^type " pkg/cypher/executor.go | wc -l  # Count types
```

**Example analysis of `executor.go` (3752 lines):**

```
Functions by category:
- Query parsing: 15 functions (600 lines)
- Execution logic: 25 functions (1200 lines)
- Result formatting: 10 functions (400 lines)
- Caching: 8 functions (350 lines)
- Optimization: 12 functions (500 lines)
- Helpers: 20 functions (700 lines)

Conclusion: 6 distinct responsibilities → Split into 6 files
```

### Step 2: Define Module Boundaries

**Create clear separation of concerns:**

```
executor.go (3752 lines) →

├── executor.go (800 lines)         # Core orchestration
│   - Execute() entry point
│   - Query routing
│   - Error handling
│   - Transaction management
│
├── parser.go (600 lines)           # Query parsing
│   - Parse Cypher queries
│   - Build AST
│   - Syntax validation
│
├── optimizer.go (500 lines)        # Query optimization
│   - Query planning
│   - Index selection
│   - Join ordering
│
├── cache.go (400 lines)            # Result caching
│   - Cache management
│   - Cache invalidation
│   - Cache statistics
│
├── formatter.go (300 lines)        # Result formatting
│   - Format results for Neo4j
│   - Type conversions
│   - Error formatting
│
└── helpers.go (700 lines)          # Shared utilities
    - Type conversions
    - Validation
    - Common operations
```

### Step 3: Define Interfaces

**Create clean contracts between modules:**

```go
// parser.go
type Parser interface {
    Parse(query string) (*AST, error)
    Validate(ast *AST) error
}

// optimizer.go
type Optimizer interface {
    Optimize(ast *AST) (*ExecutionPlan, error)
    EstimateCost(plan *ExecutionPlan) int64
}

// cache.go
type Cache interface {
    Get(key string) (interface{}, bool)
    Set(key string, value interface{})
    Invalidate(pattern string)
}

// formatter.go
type Formatter interface {
    FormatResult(rows [][]interface{}) (*Result, error)
    FormatError(err error) *Neo4jError
}
```

### Step 4: Extract Modules

**Move code to new files systematically:**

```go
// 1. Create new file: parser.go
package cypher

import (
    // ... imports
)

// Move parser-related types
type AST struct {
    // ... fields
}

type ParseError struct {
    // ... fields
}

// Move parser-related functions
func Parse(query string) (*AST, error) {
    // ... implementation
}

func validateSyntax(ast *AST) error {
    // ... implementation
}

// 2. Update executor.go to use new module
package cypher

import (
    // ... imports
)

type Executor struct {
    parser    Parser      // ← Use interface
    optimizer Optimizer   // ← Use interface
    cache     Cache       // ← Use interface
    formatter Formatter   // ← Use interface
}

func (e *Executor) Execute(ctx context.Context, query string) (*Result, error) {
    // Parse query
    ast, err := e.parser.Parse(query)
    if err != nil {
        return nil, e.formatter.FormatError(err)
    }
    
    // Optimize
    plan, err := e.optimizer.Optimize(ast)
    if err != nil {
        return nil, err
    }
    
    // Check cache
    if cached, ok := e.cache.Get(query); ok {
        return cached.(*Result), nil
    }
    
    // Execute plan
    rows, err := e.executePlan(ctx, plan)
    if err != nil {
        return nil, err
    }
    
    // Format result
    result, err := e.formatter.FormatResult(rows)
    if err != nil {
        return nil, err
    }
    
    // Cache result
    e.cache.Set(query, result)
    
    return result, nil
}
```

### Step 5: Update Tests

**Ensure all tests still pass:**

```bash
# Run tests after each extraction
go test ./pkg/cypher -v

# Check coverage hasn't decreased
go test ./pkg/cypher -coverprofile=coverage.out
go tool cover -func=coverage.out

# Run race detector
go test ./pkg/cypher -race
```

**Add tests for new interfaces:**

```go
// parser_test.go
func TestParser(t *testing.T) {
    parser := NewParser()
    
    t.Run("valid query", func(t *testing.T) {
        ast, err := parser.Parse("MATCH (n) RETURN n")
        assert.NoError(t, err)
        assert.NotNil(t, ast)
    })
    
    t.Run("syntax error", func(t *testing.T) {
        _, err := parser.Parse("INVALID QUERY")
        assert.Error(t, err)
    })
}
```

### Step 6: Update Documentation

**Document new module structure:**

```go
// Package cypher provides Neo4j-compatible Cypher query execution.
//
// The package is organized into several modules:
//
// **Core Execution** (executor.go):
//   - Query routing and orchestration
//   - Transaction management
//   - Error handling
//
// **Query Parsing** (parser.go):
//   - Cypher query parsing
//   - AST construction
//   - Syntax validation
//
// **Query Optimization** (optimizer.go):
//   - Query planning
//   - Index selection
//   - Cost estimation
//
// **Result Caching** (cache.go):
//   - Query result caching
//   - Cache invalidation
//   - Statistics tracking
//
// **Result Formatting** (formatter.go):
//   - Neo4j-compatible result formatting
//   - Type conversions
//   - Error formatting
//
// Example Usage:
//
//	executor := cypher.NewExecutor(storage)
//	result, err := executor.Execute(ctx, "MATCH (n) RETURN n", nil)
package cypher
```

---

## Real-World Example

### Before: executor.go (3752 lines)

```go
// pkg/cypher/executor.go (3752 lines) ❌

package cypher

// Parsing types and functions (600 lines)
type AST struct { /* ... */ }
func Parse(query string) (*AST, error) { /* ... */ }
func validateSyntax(ast *AST) error { /* ... */ }
// ... 15 more parsing functions

// Execution types and functions (1200 lines)
type Executor struct { /* ... */ }
func (e *Executor) Execute(ctx context.Context, query string) (*Result, error) { /* ... */ }
func (e *Executor) executeMatch(ctx context.Context, match *Match) ([][]interface{}, error) { /* ... */ }
// ... 25 more execution functions

// Caching types and functions (350 lines)
type QueryCache struct { /* ... */ }
func (c *QueryCache) Get(key string) (interface{}, bool) { /* ... */ }
// ... 8 more caching functions

// Optimization types and functions (500 lines)
type QueryPlan struct { /* ... */ }
func optimizeQuery(ast *AST) (*QueryPlan, error) { /* ... */ }
// ... 12 more optimization functions

// Formatting types and functions (400 lines)
type ResultFormatter struct { /* ... */ }
func formatResult(rows [][]interface{}) (*Result, error) { /* ... */ }
// ... 10 more formatting functions

// Helper functions (700 lines)
func toInt64(v interface{}) (int64, error) { /* ... */ }
func toString(v interface{}) (string, error) { /* ... */ }
// ... 20 more helper functions
```

### After: Split into focused modules ✅

```go
// pkg/cypher/executor.go (800 lines) ✅
package cypher

// Executor orchestrates query execution using injected dependencies
type Executor struct {
    storage   storage.Engine
    parser    Parser
    optimizer Optimizer
    cache     Cache
    formatter Formatter
}

func NewExecutor(storage storage.Engine) *Executor {
    return &Executor{
        storage:   storage,
        parser:    NewParser(),
        optimizer: NewOptimizer(),
        cache:     NewQueryCache(),
        formatter: NewFormatter(),
    }
}

func (e *Executor) Execute(ctx context.Context, query string, params map[string]interface{}) (*Result, error) {
    // Parse
    ast, err := e.parser.Parse(query)
    if err != nil {
        return nil, err
    }
    
    // Optimize
    plan, err := e.optimizer.Optimize(ast)
    if err != nil {
        return nil, err
    }
    
    // Check cache
    cacheKey := e.cache.Key(query, params)
    if cached, ok := e.cache.Get(cacheKey); ok {
        return cached.(*Result), nil
    }
    
    // Execute
    rows, err := e.executePlan(ctx, plan, params)
    if err != nil {
        return nil, err
    }
    
    // Format
    result, err := e.formatter.FormatResult(rows)
    if err != nil {
        return nil, err
    }
    
    // Cache
    e.cache.Set(cacheKey, result)
    
    return result, nil
}

// ... core execution methods (25 functions, 700 lines)
```

```go
// pkg/cypher/parser.go (600 lines) ✅
package cypher

// Parser parses Cypher queries into AST
type Parser interface {
    Parse(query string) (*AST, error)
    Validate(ast *AST) error
}

type parser struct {
    // ... fields
}

func NewParser() Parser {
    return &parser{}
}

// Parse converts Cypher query string to AST
func (p *parser) Parse(query string) (*AST, error) {
    // ... implementation
}

// ... parsing functions (15 functions, 600 lines)
```

```go
// pkg/cypher/optimizer.go (500 lines) ✅
package cypher

// Optimizer optimizes query execution plans
type Optimizer interface {
    Optimize(ast *AST) (*ExecutionPlan, error)
    EstimateCost(plan *ExecutionPlan) int64
}

type optimizer struct {
    // ... fields
}

func NewOptimizer() Optimizer {
    return &optimizer{}
}

// ... optimization functions (12 functions, 500 lines)
```

```go
// pkg/cypher/cache.go (400 lines) ✅
package cypher

// Cache manages query result caching
type Cache interface {
    Get(key string) (interface{}, bool)
    Set(key string, value interface{})
    Invalidate(pattern string)
    Stats() CacheStats
}

type queryCache struct {
    // ... fields
}

func NewQueryCache() Cache {
    return &queryCache{
        data: make(map[string]interface{}),
    }
}

// ... caching functions (8 functions, 400 lines)
```

```go
// pkg/cypher/formatter.go (300 lines) ✅
package cypher

// Formatter formats query results for Neo4j compatibility
type Formatter interface {
    FormatResult(rows [][]interface{}) (*Result, error)
    FormatError(err error) *Neo4jError
}

type formatter struct {
    // ... fields
}

func NewFormatter() Formatter {
    return &formatter{}
}

// ... formatting functions (10 functions, 300 lines)
```

```go
// pkg/cypher/helpers.go (700 lines) ✅
package cypher

// Shared utility functions used across modules

// toInt64 safely converts interface{} to int64
func toInt64(v interface{}) (int64, error) {
    // ... implementation
}

// toString safely converts interface{} to string
func toString(v interface{}) (string, error) {
    // ... implementation
}

// ... helper functions (20 functions, 700 lines)
```

---

## Refactoring Patterns

### Pattern 1: Extract Interface

**Before:**
```go
type Executor struct {
    storage *storage.MemoryEngine  // ← Concrete type
}
```

**After:**
```go
type Executor struct {
    storage storage.Engine  // ← Interface
}
```

**Benefits:**
- Testability (can inject mocks)
- Flexibility (can swap implementations)
- Decoupling (no concrete dependencies)

### Pattern 2: Extract Helper Package

**Before:**
```go
// pkg/cypher/executor.go
func toInt64(v interface{}) (int64, error) { /* ... */ }
func toString(v interface{}) (string, error) { /* ... */ }
// ... used in multiple packages
```

**After:**
```go
// pkg/convert/convert.go
package convert

func ToInt64(v interface{}) (int64, error) { /* ... */ }
func ToString(v interface{}) (string, error) { /* ... */ }

// pkg/cypher/executor.go
import "github.com/orneryd/nornicdb/pkg/convert"

value, err := convert.ToInt64(input)
```

**Benefits:**
- Reusability across packages
- Single source of truth
- Easier to test

### Pattern 3: Extract Configuration

**Before:**
```go
type Executor struct {
    cacheSize      int
    cacheTimeout   time.Duration
    maxConcurrency int
    enableMetrics  bool
    // ... 20 more config fields
}
```

**After:**
```go
// pkg/cypher/config.go
type Config struct {
    Cache struct {
        Size    int
        Timeout time.Duration
    }
    Execution struct {
        MaxConcurrency int
        EnableMetrics  bool
    }
}

// pkg/cypher/executor.go
type Executor struct {
    config Config
}
```

**Benefits:**
- Grouped related settings
- Easier to validate
- Cleaner initialization

### Pattern 4: Extract Strategy Pattern

**Before:**
```go
func (e *Executor) execute(query string) (*Result, error) {
    if isSimpleQuery(query) {
        return e.executeSimple(query)
    } else if isComplexQuery(query) {
        return e.executeComplex(query)
    } else if isAggregationQuery(query) {
        return e.executeAggregation(query)
    }
    // ... many more conditions
}
```

**After:**
```go
// pkg/cypher/strategy.go
type ExecutionStrategy interface {
    CanHandle(query *AST) bool
    Execute(ctx context.Context, query *AST) (*Result, error)
}

type SimpleStrategy struct { /* ... */ }
type ComplexStrategy struct { /* ... */ }
type AggregationStrategy struct { /* ... */ }

// pkg/cypher/executor.go
type Executor struct {
    strategies []ExecutionStrategy
}

func (e *Executor) execute(ctx context.Context, ast *AST) (*Result, error) {
    for _, strategy := range e.strategies {
        if strategy.CanHandle(ast) {
            return strategy.Execute(ctx, ast)
        }
    }
    return nil, errors.New("no strategy found")
}
```

**Benefits:**
- Open/closed principle
- Easy to add new strategies
- Each strategy independently testable

---

## Refactoring Checklist

Before refactoring:
- [ ] Analyze file structure and responsibilities
- [ ] Identify clear module boundaries
- [ ] Define interfaces between modules
- [ ] Plan extraction order (least dependent first)
- [ ] Ensure full test coverage exists

During refactoring:
- [ ] Extract one module at a time
- [ ] Run tests after each extraction
- [ ] Maintain or improve test coverage
- [ ] Update documentation
- [ ] Keep commits small and focused

After refactoring:
- [ ] All tests pass
- [ ] Coverage ≥90% maintained
- [ ] No performance regression
- [ ] Documentation updated
- [ ] Code review completed

---

## Common Pitfalls

### Pitfall 1: Over-Abstraction

**Bad:**
```go
// Too many layers of abstraction
type QueryExecutorFactoryBuilder interface {
    Build() QueryExecutorFactory
}

type QueryExecutorFactory interface {
    Create() QueryExecutor
}

type QueryExecutor interface {
    Execute(query string) (*Result, error)
}
```

**Good:**
```go
// Simple and direct
type Executor interface {
    Execute(ctx context.Context, query string) (*Result, error)
}
```

### Pitfall 2: Circular Dependencies

**Bad:**
```go
// pkg/cypher/executor.go
import "github.com/orneryd/nornicdb/pkg/cypher/parser"

// pkg/cypher/parser/parser.go
import "github.com/orneryd/nornicdb/pkg/cypher"  // ← Circular!
```

**Good:**
```go
// pkg/cypher/executor.go
import "github.com/orneryd/nornicdb/pkg/cypher/parser"

// pkg/cypher/parser/parser.go
// No import of parent package
```

### Pitfall 3: Breaking Public API

**Bad:**
```go
// BEFORE (public API)
func Execute(query string) (*Result, error)

// AFTER (breaking change) ❌
func ExecuteQuery(ctx context.Context, q string) (*QueryResult, error)
```

**Good:**
```go
// BEFORE (public API)
func Execute(query string) (*Result, error)

// AFTER (backward compatible) ✅
func Execute(query string) (*Result, error) {
    return ExecuteContext(context.Background(), query)
}

func ExecuteContext(ctx context.Context, query string) (*Result, error) {
    // New implementation
}
```

---

## Quick Reference

### File Size Check

```bash
# Find files over 2000 lines
find pkg -name "*.go" -exec sh -c 'wc -l "$1" | awk "\$1 > 2000 {print}"' _ {} \;

# Count functions per file
for f in pkg/cypher/*.go; do 
    echo "$f: $(grep -c '^func ' $f) functions"
done
```

### Refactoring Commands

```bash
# Before refactoring: Save baseline
go test ./... -coverprofile=before.out
go test ./pkg/cypher -bench=. -benchmem > before-bench.txt

# After refactoring: Compare
go test ./... -coverprofile=after.out
go test ./pkg/cypher -bench=. -benchmem > after-bench.txt

# Compare coverage
go tool cover -func=before.out | tail -1
go tool cover -func=after.out | tail -1

# Compare benchmarks
benchcmp before-bench.txt after-bench.txt
```

---

**Remember**: Refactoring is about improving structure without changing behavior. Always have tests to verify behavior is preserved.
