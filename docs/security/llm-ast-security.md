# LLM & AST Security Patterns

**Safe patterns for integrating Large Language Models with NornicDB's query system.**

## Overview

NornicDB uses a **stream parse-execute** architecture where queries are parsed and executed in a single pass, with a **lazy AST** built separately for LLM features. This document covers:

1. Why stream parse-execute is fast
2. Security considerations for this approach
3. Safe LLM integration patterns
4. Plugin security with AST

## Architecture: Stream Parse-Execute + Lazy AST

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     NornicDB Query Architecture                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Traditional DB (Full Parse → AST → Execute):                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Query → [Lexer] → [Parser] → AST → [Optimizer] → [Executor]    │   │
│  │                               ↑                                  │   │
│  │                    Full tree in memory                          │   │
│  │                    Multiple passes                              │   │
│  │                    ~10-50µs overhead                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  NornicDB (Stream Parse-Execute + Lazy AST):                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Query → [Stream Parser+Executor] ─────────────────→ Result     │   │
│  │            ↓ (async/lazy)                                        │   │
│  │          [AST Builder] → Cached AST (for LLM features)          │   │
│  │                                                                  │   │
│  │  • Single pass through query                                    │   │
│  │  • Execute as we parse                                          │   │
│  │  • No intermediate allocations for simple queries               │   │
│  │  • ~1-3µs for simple queries (10-50x faster)                   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Why Stream Parse-Execute is Fast

### Performance Benefits

| Aspect | Traditional AST | Stream Parse-Execute |
|--------|----------------|---------------------|
| Memory allocations | Full tree (~100+ nodes) | Minimal (on-demand) |
| Passes over query | 2-4 (lex, parse, optimize, execute) | 1 (combined) |
| Latency to first byte | After full parse | Immediate |
| Simple query overhead | ~10-50µs | ~1-3µs |
| Complex query overhead | ~50-200µs | ~10-50µs |

### Why This Works

```go
// Traditional: Parse everything, then execute
ast := parser.Parse(query)      // Allocate full AST
optimized := optimizer.Optimize(ast)  // Another pass
result := executor.Execute(optimized) // Finally execute

// Stream: Execute as we recognize tokens
// MATCH (n:Person) WHERE n.age > 21 RETURN n.name
//   ↓
// See MATCH → start pattern matching
// See (n:Person) → find nodes with label
// See WHERE → filter in-place
// See RETURN → project results
// No intermediate AST needed!
```

### Benchmarks

```
BenchmarkSimpleQuery/Traditional-16     50000    25000 ns/op   12000 B/op   150 allocs/op
BenchmarkSimpleQuery/StreamExecute-16  500000     2500 ns/op    1200 B/op    15 allocs/op
                                                   ↑ 10x faster    ↑ 10x less memory
```

## Security Considerations: Stream Parse-Execute

### ✅ Benefits

| Property | Explanation |
|----------|-------------|
| **No TOCTOU** | Check and use happen atomically - no race between validation and execution |
| **Smaller attack surface** | No intermediate AST to manipulate |
| **Consistent parsing** | Same code parses AND executes - no semantic drift |
| **Memory safety** | Less allocation = less chance of buffer issues |

### ⚠️ Considerations

| Concern | Risk | Mitigation |
|---------|------|------------|
| **Partial execution on error** | Side effects before error detected | Transaction rollback, implicit transactions |
| **No global semantic check** | Can't validate entire query before starting | Validate syntax first, use explicit transactions for critical ops |
| **Error recovery** | Harder to provide good error messages | Store context during parse for error reporting |
| **Optimization opportunities** | Can't reorder operations | Accept trade-off for latency; complex queries can use AST path |

### Partial Execution Risk

```cypher
// Risk: What if error occurs mid-query?
CREATE (a:Node) 
CREATE (b:Node)
CREATE (c:Invalid!)  // ← Syntax error here

// Without protection: a and b created, c fails
// With implicit transaction: all rolled back
```

**Our mitigation:**
```go
// Implicit transactions wrap non-explicit queries
func (e *Executor) Execute(ctx, query, params) {
    // For write operations without explicit transaction
    if isWriteQuery && !inExplicitTransaction {
        tx := e.storage.BeginTransaction()
        defer tx.Rollback()  // Rollback on any error
        
        result, err := e.executeWithTransaction(tx, query)
        if err != nil {
            return nil, err  // Transaction rolled back
        }
        
        tx.Commit()  // Only commit if fully successful
        return result, nil
    }
    // ...
}
```

## Safe LLM Integration Patterns

### Pattern 1: Read-Only AST Analysis (SAFE)

```go
// ✅ SAFE: LLM only reads AST, doesn't generate queries
func AnalyzeQueryComplexity(query string) (*Analysis, error) {
    info := analyzer.Analyze(query)
    ast := info.GetAST()
    
    // LLM analyzes structure
    complexity := llm.AnalyzeComplexity(ast)
    suggestions := llm.SuggestIndexes(ast)
    
    return &Analysis{
        Complexity: complexity,
        Suggestions: suggestions,
    }, nil
}
```

**Why safe:** LLM output is informational only, never executed.

### Pattern 2: Query Correction with Validation (SAFE with care)

```go
// ⚠️ REQUIRES VALIDATION: LLM generates corrected query
func CorrectQuery(originalQuery string, error error) (string, error) {
    info := analyzer.Analyze(originalQuery)
    ast := info.GetAST()
    
    // LLM suggests correction
    correctedQuery := llm.SuggestCorrection(ast, error)
    
    // ⚠️ CRITICAL: Validate the corrected query
    if err := validateQuerySafety(correctedQuery); err != nil {
        return "", fmt.Errorf("LLM generated unsafe query: %w", err)
    }
    
    // ⚠️ CRITICAL: User must approve before execution
    return correctedQuery, nil  // Return for user approval, don't auto-execute
}

func validateQuerySafety(query string) error {
    // 1. Parse with our parser (not LLM's interpretation)
    info := analyzer.Analyze(query)
    
    // 2. Check for dangerous patterns
    if info.HasDelete && !userHasDeletePermission {
        return errors.New("DELETE not permitted")
    }
    
    // 3. Validate all identifiers
    for _, label := range info.Labels {
        if !isValidIdentifier(label) {
            return fmt.Errorf("invalid label: %s", label)
        }
    }
    
    return nil
}
```

### Pattern 3: Query Generation from Natural Language (HIGH RISK)

```go
// ❌ DANGEROUS: Direct execution of LLM-generated queries
func DangerousNLToQuery(naturalLanguage string) (*Result, error) {
    query := llm.GenerateCypher(naturalLanguage)
    return executor.Execute(ctx, query, nil)  // ❌ NO VALIDATION!
}

// ✅ SAFE: Validated execution with constraints
func SafeNLToQuery(naturalLanguage string, constraints QueryConstraints) (*Result, error) {
    query := llm.GenerateCypher(naturalLanguage)
    
    // 1. Parse and analyze
    info := analyzer.Analyze(query)
    
    // 2. Enforce constraints
    if !constraints.AllowWrites && info.IsWriteQuery {
        return nil, errors.New("write operations not allowed")
    }
    
    if !constraints.AllowDelete && info.HasDelete {
        return nil, errors.New("delete operations not allowed")
    }
    
    // 3. Whitelist labels and relationships
    for _, label := range info.Labels {
        if !constraints.AllowedLabels.Contains(label) {
            return nil, fmt.Errorf("label %s not in whitelist", label)
        }
    }
    
    // 4. Use read-only transaction for safety
    if !info.IsWriteQuery {
        return executor.ExecuteReadOnly(ctx, query, nil)
    }
    
    // 5. Require explicit user approval for writes
    return nil, errors.New("write query requires user approval")
}
```

### Pattern 4: Plugin Query Execution (REQUIRES SANDBOXING)

```go
// Plugin-generated queries need strict sandboxing
type PluginQueryConstraints struct {
    MaxResults     int
    TimeoutMs      int
    AllowedLabels  []string
    AllowedTypes   []string
    ReadOnly       bool
    MaxDepth       int  // For path queries
}

func ExecutePluginQuery(plugin Plugin, query string, constraints PluginQueryConstraints) (*Result, error) {
    // 1. Validate plugin has permission for this query type
    info := analyzer.Analyze(query)
    
    if constraints.ReadOnly && info.IsWriteQuery {
        return nil, errors.New("plugin attempted write in read-only mode")
    }
    
    // 2. Check labels against plugin's allowed set
    for _, label := range info.Labels {
        if !contains(constraints.AllowedLabels, label) {
            return nil, fmt.Errorf("plugin not authorized for label: %s", label)
        }
    }
    
    // 3. Inject constraints into query
    constrainedQuery := injectConstraints(query, constraints)
    
    // 4. Execute with timeout
    ctx, cancel := context.WithTimeout(ctx, time.Duration(constraints.TimeoutMs)*time.Millisecond)
    defer cancel()
    
    return executor.Execute(ctx, constrainedQuery, nil)
}

func injectConstraints(query string, c PluginQueryConstraints) string {
    // Add LIMIT if not present
    if c.MaxResults > 0 && !strings.Contains(strings.ToUpper(query), "LIMIT") {
        query = query + fmt.Sprintf(" LIMIT %d", c.MaxResults)
    }
    return query
}
```

## AST Cache Security

### Cache Key Security

```go
// Cache keys include normalized query + parameter hash
type CacheKey struct {
    NormalizedQuery string
    ParamHash       uint64
}

// This prevents:
// 1. Cache confusion between different parameter values
// 2. Cache poisoning from similar queries
```

### Cache Isolation

```go
// Per-user cache isolation (if multi-tenant)
type UserScopedCache struct {
    userID string
    cache  *QueryCache
}

func (c *UserScopedCache) Get(query string, params map[string]any) (*QueryInfo, bool) {
    key := c.makeKey(c.userID, query, params)
    return c.cache.Get(key)
}
```

### Cache Invalidation Security

```go
// Write operations invalidate relevant caches
func (e *Executor) invalidateCachesAfterWrite(info *QueryInfo) {
    // Don't trust the query to tell us what it modified
    // Use actual affected labels from execution
    affectedLabels := e.getActualAffectedLabels()
    
    e.cache.InvalidateLabels(affectedLabels)
}
```

## Heimdall Plugin Security

### Plugin Query Constraints

```yaml
# Plugin manifest defines allowed operations
plugin:
  name: analytics-plugin
  permissions:
    queries:
      read_only: true
      allowed_labels: [Event, User, Session]
      allowed_relationships: [TRIGGERED, BELONGS_TO]
      max_results: 10000
      timeout_ms: 5000
    ast_access:
      can_read: true
      can_generate: false  # Cannot generate new queries
```

### Plugin AST Access

```go
// Plugins get read-only AST view
type PluginASTView struct {
    Clauses   []ASTClauseView  // Sanitized view
    IsReadOnly bool
    Labels    []string
}

func (ast *AST) ToPluginView() *PluginASTView {
    return &PluginASTView{
        Clauses:    sanitizeClauses(ast.Clauses),
        IsReadOnly: ast.IsReadOnly,
        Labels:     ast.Labels,
    }
}

// Plugins cannot:
// - Modify AST
// - Generate queries from AST
// - Access raw query text (potential injection source)
```

## Security Checklist

### For LLM Integration

- [ ] LLM output is NEVER directly executed
- [ ] All LLM-generated queries are re-parsed by our parser
- [ ] Write operations require explicit user approval
- [ ] Label/relationship whitelisting enforced
- [ ] Timeout and result limits applied
- [ ] Audit logging for all LLM-generated queries

### For Plugin Integration

- [ ] Plugin permissions declared in manifest
- [ ] Read-only mode enforced where declared
- [ ] Label/relationship access controlled
- [ ] Query timeout enforced
- [ ] Result count limited
- [ ] AST access is read-only view

### For AST Cache

- [ ] Cache keys include parameter hash
- [ ] Per-user isolation (if multi-tenant)
- [ ] Write operations invalidate affected caches
- [ ] Cache TTL prevents stale data

## Summary

| Component | Security Model |
|-----------|---------------|
| **Stream Parse-Execute** | Atomic parse+execute, no intermediate attack surface |
| **Lazy AST** | Observation only, never in execution path |
| **LLM Integration** | Re-parse all output, whitelist, require approval |
| **Plugin Queries** | Sandbox with permissions, timeouts, limits |
| **AST Cache** | Keyed by query+params, per-user isolation |

---

**See Also:**
- [Query Cache Security](query-cache-security.md) - Cache-specific security
- [HTTP Security](http-security.md) - Network-level protections
- [Plugin Development Guide](../development/plugin-guide.md) - Building secure plugins
