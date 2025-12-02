# Bug Fix Workflow

**Purpose**: Mandatory process for fixing bugs in NornicDB  
**Audience**: AI coding agents  
**Goal**: Prevent regressions, ensure quality

---

## The Golden Rule

**Every bug MUST have a test that:**
1. Reproduces the exact bug condition
2. Fails before the fix
3. Passes after the fix
4. Prevents future regressions

**No exceptions.** This is non-negotiable.

---

## 5-Step Bug Fix Process

### Step 1: Reproduce the Bug

**Create a failing test that demonstrates the exact bug:**

```go
// ====================================================================================
// BUG #42: Query returns empty results when using IS NOT NULL with aggregation
// ====================================================================================
// Discovered: 2024-11-15
// Reporter: VSCode Extension user
// Impact: Stats queries return 0 rows instead of aggregated counts
// Root Cause: WHERE filter applied after WITH aggregation instead of before
// ====================================================================================

func TestBug_WhereIsNotNullWithAggregation(t *testing.T) {
    // Setup: Create minimal data that triggers the bug
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    // Create test data
    _, err := exec.Execute(ctx, `CREATE (f:File {id: '1', ext: '.ts'})`, nil)
    require.NoError(t, err)
    _, err = exec.Execute(ctx, `CREATE (f:File {id: '2', ext: '.ts'})`, nil)
    require.NoError(t, err)
    _, err = exec.Execute(ctx, `CREATE (f:File {id: '3', ext: '.md'})`, nil)
    require.NoError(t, err)
    
    t.Run("exact reproduction from production", func(t *testing.T) {
        // This is the EXACT query that fails in production
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.ext IS NOT NULL
            WITH f.ext as extension, COUNT(f) as count
            RETURN extension, count
            ORDER BY count DESC
        `, nil)
        
        // Assert expected behavior
        require.NoError(t, err, "Query should execute without error")
        require.Len(t, result.Rows, 2, 
            "Should return 2 rows (one per extension)")
        
        // Verify exact values
        extCounts := make(map[string]int64)
        for _, row := range result.Rows {
            ext := row[0].(string)
            count := row[1].(int64)
            extCounts[ext] = count
        }
        
        assert.Equal(t, int64(2), extCounts[".ts"], 
            ".ts should have 2 files")
        assert.Equal(t, int64(1), extCounts[".md"], 
            ".md should have 1 file")
    })
}
```

**Key elements:**
- **Header comment** - Bug number, description, impact, root cause
- **Minimal setup** - Only data needed to trigger bug
- **Exact reproduction** - Use the EXACT query/operation that fails
- **Clear assertions** - What should happen vs what actually happens

---

### Step 2: Verify Test Fails

**Run the test and confirm it fails:**

```bash
$ go test ./pkg/cypher -run TestBug_WhereIsNotNull -v

=== RUN   TestBug_WhereIsNotNullWithAggregation
=== RUN   TestBug_WhereIsNotNullWithAggregation/exact_reproduction_from_production
    aggregation_bugs_test.go:45: 
        Error Trace:    aggregation_bugs_test.go:45
        Error:          Should return 2 rows (one per extension)
        Expected:       2
        Actual:         0
--- FAIL: TestBug_WhereIsNotNullWithAggregation (0.01s)
    --- FAIL: TestBug_WhereIsNotNullWithAggregation/exact_reproduction_from_production (0.00s)
FAIL
FAIL    github.com/orneryd/nornicdb/pkg/cypher    0.123s
```

**If test passes before fix:**
- ❌ Test doesn't reproduce the bug
- ❌ Need to adjust test conditions
- ❌ May have misunderstood the bug

**If test fails as expected:**
- ✅ Bug is reproducible
- ✅ Test will catch regressions
- ✅ Ready to fix

---

### Step 3: Fix the Bug

**Implement the minimal fix:**

```go
// pkg/cypher/executor.go

func (e *Executor) executeWithClause(ctx context.Context, 
    withClause *WithClause, bindings map[string]interface{}) error {
    
    // BUG FIX: Apply WHERE filters BEFORE aggregation, not after
    // 
    // Previous behavior:
    //   1. Aggregate all rows
    //   2. Apply WHERE filter to aggregated results
    //   3. Return empty (filters don't match aggregated data)
    //
    // Correct behavior (Neo4j-compatible):
    //   1. Apply WHERE filter to raw rows
    //   2. Aggregate filtered rows
    //   3. Return aggregated results
    //
    // Root cause: Filter was in wrong position in execution pipeline
    
    // BEFORE (buggy):
    // rows := e.aggregate(allRows)
    // filtered := e.applyWhere(rows, whereClause)
    
    // AFTER (fixed):
    filtered := e.applyWhere(allRows, whereClause)  // ← Filter first
    rows := e.aggregate(filtered)                    // ← Then aggregate
    
    return rows
}
```

**Fix guidelines:**
- **Minimal change** - Fix only what's broken
- **Comment the fix** - Explain WHY the bug occurred
- **Show before/after** - Make the change obvious
- **Preserve behavior** - Don't break other functionality

---

### Step 4: Verify Test Passes

**Run the test again - it MUST pass now:**

```bash
$ go test ./pkg/cypher -run TestBug_WhereIsNotNull -v

=== RUN   TestBug_WhereIsNotNullWithAggregation
=== RUN   TestBug_WhereIsNotNullWithAggregation/exact_reproduction_from_production
--- PASS: TestBug_WhereIsNotNullWithAggregation (0.01s)
    --- PASS: TestBug_WhereIsNotNullWithAggregation/exact_reproduction_from_production (0.00s)
PASS
ok      github.com/orneryd/nornicdb/pkg/cypher    0.123s
```

**If test still fails:**
- ❌ Fix is incomplete or incorrect
- ❌ Need to debug further
- ❌ May need different approach

**If test passes:**
- ✅ Bug is fixed
- ✅ Test prevents regression
- ✅ Ready for additional tests

---

### Step 5: Add Regression Tests

**Add variations to catch similar bugs:**

```go
func TestBug_WhereIsNotNullWithAggregation(t *testing.T) {
    // ... original test from Step 1 ...
    
    // Add variations to prevent similar bugs
    
    t.Run("with ORDER BY", func(t *testing.T) {
        // Test that ORDER BY still works after fix
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.ext IS NOT NULL
            WITH f.ext as extension, COUNT(f) as count
            RETURN extension, count
            ORDER BY extension ASC
        `, nil)
        
        require.NoError(t, err)
        require.Len(t, result.Rows, 2)
        
        // Verify order
        assert.Equal(t, ".md", result.Rows[0][0])
        assert.Equal(t, ".ts", result.Rows[1][0])
    })
    
    t.Run("with LIMIT", func(t *testing.T) {
        // Test that LIMIT works after fix
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.ext IS NOT NULL
            WITH f.ext as extension, COUNT(f) as count
            RETURN extension, count
            LIMIT 1
        `, nil)
        
        require.NoError(t, err)
        assert.Len(t, result.Rows, 1)
    })
    
    t.Run("multiple WHERE conditions", func(t *testing.T) {
        // Test multiple filters
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.ext IS NOT NULL AND f.ext <> '.md'
            WITH f.ext as extension, COUNT(f) as count
            RETURN extension, count
        `, nil)
        
        require.NoError(t, err)
        require.Len(t, result.Rows, 1)
        assert.Equal(t, ".ts", result.Rows[0][0])
        assert.Equal(t, int64(2), result.Rows[0][1])
    })
    
    t.Run("nested aggregation", func(t *testing.T) {
        // Test more complex aggregation
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.ext IS NOT NULL
            WITH f.ext as extension, COUNT(f) as count
            WHERE count > 1
            RETURN extension, count
        `, nil)
        
        require.NoError(t, err)
        require.Len(t, result.Rows, 1)
        assert.Equal(t, ".ts", result.Rows[0][0])
    })
}
```

**Regression test goals:**
- **Prevent similar bugs** - Test related scenarios
- **Cover edge cases** - Test boundary conditions
- **Verify interactions** - Test with other features
- **Document behavior** - Show expected usage

---

## Real-World Example from Codebase

### Bug: Aggregation with IS NOT NULL Filter

**File:** `pkg/cypher/aggregation_bugs_test.go`

```go
// ====================================================================================
// BUG #1: WHERE ... IS NOT NULL combined with WITH aggregation returns empty results
// ====================================================================================
// Discovered: 2024-11-10
// Reporter: VSCode Code Intelligence stats queries
// Impact: File extension statistics return 0 rows instead of counts
// Root Cause: WHERE filter applied after WITH aggregation instead of before
// Neo4j Behavior: Filters applied before aggregation
// ====================================================================================

func TestBug_WhereIsNotNullWithAggregation(t *testing.T) {
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()

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

        // Verify we have both extensions
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

    t.Run("simple WHERE IS NOT NULL filter", func(t *testing.T) {
        // This simpler query should also work
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.extension IS NOT NULL
            RETURN f.extension as ext, count(*) as count
        `, nil)
        require.NoError(t, err)
        require.GreaterOrEqual(t, len(result.Rows), 1, "Should return at least 1 row")
    })

    t.Run("WHERE IS NOT NULL returns correct count", func(t *testing.T) {
        // Verify base filtering works
        result, err := exec.Execute(ctx, `
            MATCH (f:File)
            WHERE f.extension IS NOT NULL
            RETURN count(f) as count_with_ext
        `, nil)
        require.NoError(t, err)
        require.Len(t, result.Rows, 1)

        // We have 5 files with extension (file1-5), 2 without (file6-7)
        count := result.Rows[0][0].(int64)
        assert.Equal(t, int64(5), count, "Should count 5 files with extension")
    })
}

// setupAggregationTestData creates test data for aggregation tests
func setupAggregationTestData(t *testing.T, store *storage.MemoryEngine) {
    ctx := context.Background()
    exec := NewStorageExecutor(store)

    // Create File nodes with various extensions
    queries := []string{
        `CREATE (f:File {id: 'file1', path: '/test/file1.ts', extension: '.ts', name: 'file1.ts'})`,
        `CREATE (f:File {id: 'file2', path: '/test/file2.ts', extension: '.ts', name: 'file2.ts'})`,
        `CREATE (f:File {id: 'file3', path: '/test/file3.md', extension: '.md', name: 'file3.md'})`,
        `CREATE (f:File {id: 'file4', path: '/test/file4.md', extension: '.md', name: 'file4.md'})`,
        `CREATE (f:File {id: 'file5', path: '/test/file5.md', extension: '.md', name: 'file5.md'})`,
        // Files without extension (to test IS NOT NULL filtering)
        `CREATE (f:File {id: 'file6', path: '/test/noext', name: 'noext'})`,
        `CREATE (f:File {id: 'file7', path: '/test/noext2', name: 'noext2'})`,
    }

    for _, q := range queries {
        _, err := exec.Execute(ctx, q, nil)
        require.NoError(t, err)
    }
}
```

**What makes this a good bug test:**

✅ **Clear documentation** - Header explains everything  
✅ **Exact reproduction** - Uses real production query  
✅ **Minimal setup** - Only 7 test files needed  
✅ **Multiple variations** - Tests related scenarios  
✅ **Helper function** - Reusable setup  
✅ **Detailed assertions** - Verifies exact behavior  

---

## Bug Test Template

**Use this template for all bug fixes:**

```go
// ====================================================================================
// BUG #[NUMBER]: [One-line description]
// ====================================================================================
// Discovered: [Date]
// Reporter: [Who/where found]
// Impact: [What breaks, how severe]
// Root Cause: [Why it happened]
// Neo4j Behavior: [How Neo4j handles this]
// Related Issues: [GitHub issue numbers]
// ====================================================================================

func TestBug_[DescriptiveName](t *testing.T) {
    // Setup: Create minimal conditions that trigger bug
    store := storage.NewMemoryEngine()
    exec := NewStorageExecutor(store)
    ctx := context.Background()
    
    setupBugTestData(t, store)
    
    t.Run("exact reproduction from production", func(t *testing.T) {
        // Execute the EXACT operation that fails
        result, err := performBuggyOperation(ctx, exec)
        
        // Assert expected behavior (this WILL fail before fix)
        require.NoError(t, err, "Operation should succeed")
        assert.Equal(t, expectedValue, result, "Should return correct value")
    })
    
    t.Run("variation 1: [describe scenario]", func(t *testing.T) {
        // Test related scenario
    })
    
    t.Run("variation 2: [describe scenario]", func(t *testing.T) {
        // Test edge case
    })
    
    t.Run("variation 3: [describe scenario]", func(t *testing.T) {
        // Test interaction with other features
    })
}

// setupBugTestData creates minimal test data
func setupBugTestData(t *testing.T, store *storage.MemoryEngine) {
    // Create only data needed to trigger bug
}
```

---

## Common Bug Patterns

### Pattern 1: Order of Operations Bug

**Symptom:** Feature works alone but fails when combined  
**Cause:** Operations executed in wrong order  
**Fix:** Reorder execution pipeline

```go
// BEFORE (buggy)
func process() {
    aggregate()  // ← Wrong order
    filter()
}

// AFTER (fixed)
func process() {
    filter()     // ← Correct order
    aggregate()
}
```

### Pattern 2: Nil/Empty Handling Bug

**Symptom:** Crashes or wrong results with nil/empty input  
**Cause:** Missing nil checks or empty validation  
**Fix:** Add defensive checks

```go
// BEFORE (buggy)
func getValue(m map[string]interface{}, key string) interface{} {
    return m[key]  // ← Panics if m is nil
}

// AFTER (fixed)
func getValue(m map[string]interface{}, key string) interface{} {
    if m == nil {
        return nil
    }
    return m[key]
}
```

### Pattern 3: Type Conversion Bug

**Symptom:** Type assertion panics or wrong type returned  
**Cause:** Assuming type without checking  
**Fix:** Safe type conversion with checking

```go
// BEFORE (buggy)
func toInt(v interface{}) int64 {
    return v.(int64)  // ← Panics if not int64
}

// AFTER (fixed)
func toInt(v interface{}) (int64, error) {
    switch val := v.(type) {
    case int64:
        return val, nil
    case int:
        return int64(val), nil
    case float64:
        return int64(val), nil
    default:
        return 0, fmt.Errorf("cannot convert %T to int64", v)
    }
}
```

### Pattern 4: Concurrency Bug

**Symptom:** Intermittent failures, race conditions  
**Cause:** Shared state without synchronization  
**Fix:** Add proper locking or use channels

```go
// BEFORE (buggy)
type Cache struct {
    data map[string]interface{}  // ← Not thread-safe
}

func (c *Cache) Get(key string) interface{} {
    return c.data[key]  // ← Race condition
}

// AFTER (fixed)
type Cache struct {
    mu   sync.RWMutex
    data map[string]interface{}
}

func (c *Cache) Get(key string) interface{} {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.data[key]
}
```

---

## Verification Checklist

Before marking bug as fixed:

- [ ] Test reproduces bug (fails before fix)
- [ ] Test passes after fix
- [ ] Added regression tests (variations)
- [ ] All existing tests still pass
- [ ] No performance regression (run benchmarks)
- [ ] No race conditions (`go test -race`)
- [ ] Coverage maintained or improved
- [ ] Documentation updated (if behavior changed)
- [ ] CHANGELOG.md updated
- [ ] Commit message explains fix

---

## Quick Reference

### Bug Fix Commands

```bash
# 1. Create failing test
# Edit: pkg/cypher/aggregation_bugs_test.go

# 2. Verify test fails
go test ./pkg/cypher -run TestBug_YourBugName -v

# 3. Implement fix
# Edit: pkg/cypher/executor.go

# 4. Verify test passes
go test ./pkg/cypher -run TestBug_YourBugName -v

# 5. Run all tests
go test ./... -v

# 6. Check for race conditions
go test ./pkg/cypher -race

# 7. Verify coverage
go test ./pkg/cypher -coverprofile=coverage.out
go tool cover -func=coverage.out

# 8. Run benchmarks (ensure no regression)
go test ./pkg/cypher -bench=. -benchmem
```

### Commit Message Template

```
fix(scope): brief description of bug

Detailed explanation of:
- What was broken
- Why it was broken
- How it's fixed

Changes:
- List specific changes made
- Include test additions

Fixes #123
Benchmark: No performance impact
Coverage: +2.3% (added 5 test cases)
```

---

**Remember**: Every bug is an opportunity to improve test coverage and prevent future issues. Take time to write comprehensive tests.
