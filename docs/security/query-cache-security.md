# Query Analysis & Cache Security

**How NornicDB safely analyzes and caches Cypher queries.**

## Overview

NornicDB uses a `QueryAnalyzer` to extract metadata from Cypher queries for caching optimization. This document explains the security model, what protections exist, and edge cases.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Query Execution Flow                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────┐    ┌──────────────────┐    ┌──────────────────┐   │
│  │   Client    │───▶│  QueryAnalyzer   │───▶│   Cache Check    │   │
│  │   Query     │    │  (Metadata only) │    │  (Read-only?)    │   │
│  └─────────────┘    └──────────────────┘    └──────────────────┘   │
│                              │                        │              │
│                              ▼                        ▼              │
│                     ┌──────────────────┐    ┌──────────────────┐   │
│                     │  Query Executor  │◀───│  Cache Hit/Miss  │   │
│                     │  (Full parsing)  │    │                  │   │
│                     └──────────────────┘    └──────────────────┘   │
│                              │                                       │
│                              ▼                                       │
│                     ┌──────────────────┐                            │
│                     │  Storage Engine  │                            │
│                     │  (Data access)   │                            │
│                     └──────────────────┘                            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Security Model

### What QueryAnalyzer Does

The `QueryAnalyzer` performs **lightweight keyword detection** to determine:

- Is this query read-only? (cacheable)
- Is this query a write operation? (requires cache invalidation)
- What labels are affected? (for smart cache invalidation)

### What QueryAnalyzer Does NOT Do

- **NOT used for access control** - permissions are checked separately
- **NOT used for query validation** - the executor validates syntax
- **NOT a full parser** - just keyword detection for performance

## Threat Analysis

### ✅ Protected: Write Operations Hidden as Reads

**Threat:** An attacker crafts a query that deletes/modifies data but is marked as "read-only" by the analyzer.

**Protection:** This is **NOT POSSIBLE** because:

1. Cypher keywords must be whole words with boundaries
2. We search ALL occurrences of each keyword
3. There's no way to "hide" a keyword in valid Cypher syntax

**Example attacks that are blocked:**

```cypher
-- Hiding DELETE - NOT POSSIBLE
MATCH (n) WHERE n.name = 'safe' DELETE n
-- ✅ DELETE detected, marked as write

-- Splitting keywords - NOT POSSIBLE (invalid Cypher)
MATCH (n) DE/*comment*/LETE n
-- ✅ Invalid syntax, rejected by executor

-- Unicode tricks - NOT POSSIBLE
MATCH (n) ＤＥＬＥＴＥ n
-- ✅ Not ASCII "DELETE", not valid Cypher
```

### ⚡ Accepted: Read Operations Marked as Writes (False Positives)

**Scenario:** A read-only query is incorrectly marked as a write operation.

**Impact:** Performance only (not security)
- Query is not cached
- May trigger unnecessary cache invalidation
- Query still executes correctly

**When this happens:**

| Scenario | Example Query | What Happens |
|----------|---------------|--------------|
| Property named like keyword | `RETURN n.delete` | Marked as write |
| Keyword in string literal | `WHERE name = 'DELETE me'` | Marked as write |
| Keyword in comment | `RETURN n // TODO: delete` | Marked as write |

**Why we accept this:**

1. **Conservative is safer** - Better to not cache than to cache incorrectly
2. **Rare occurrence** - Few users name properties "delete" or "create"
3. **No security impact** - Query executes correctly, just not cached
4. **Simple implementation** - Complex parsing adds attack surface

### ✅ Protected: False Positives Cannot Leak Data

**Concern:** Could an attacker use a false positive to gain unauthorized data access?

**Answer:** No. The QueryAnalyzer is completely separate from access control.

```
┌─────────────────────────────────────────────────────────────┐
│                    Security Boundary                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   QueryAnalyzer              Access Control (RBAC)           │
│   ┌────────────────┐         ┌────────────────┐             │
│   │ Determines:    │         │ Determines:    │             │
│   │ • Caching      │         │ • Can user     │             │
│   │ • Invalidation │         │   read nodes?  │             │
│   │                │         │ • Can user     │             │
│   │ Does NOT       │         │   write nodes? │             │
│   │ affect:        │         │                │             │
│   │ • Permissions  │         │ ALWAYS         │             │
│   │ • Authorization│         │ ENFORCED       │             │
│   └────────────────┘         └────────────────┘             │
│          │                          │                       │
│          │ Performance              │ Security              │
│          ▼                          ▼                       │
│   ┌────────────────┐         ┌────────────────┐             │
│   │ Cache miss     │         │ Query allowed  │             │
│   │ (slower, but   │         │ or denied      │             │
│   │ still works)   │         │ (enforced)     │             │
│   └────────────────┘         └────────────────┘             │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Attack scenarios that DO NOT work:**

| Attack | Why It Fails |
|--------|--------------|
| "Read secret data by triggering write path" | Write path doesn't grant extra permissions |
| "Learn label exists via cache invalidation" | Query must succeed first (already requires access) |
| "Bypass RBAC by confusing the analyzer" | Analyzer has no role in authorization |
| "Force stale data via false positive" | False positive = no caching = always fresh data |

**Example:**

```cypher
-- Attacker tries to read :Secret nodes they shouldn't access
MATCH (n:Secret) RETURN n.delete
```

What happens:
1. QueryAnalyzer sees "delete" → marks as write (false positive)
2. **Access control checks if user can read :Secret** → DENIED
3. Query rejected - attacker gets nothing
4. Cache invalidation never happens (query failed)

The false positive only affects step 1 (caching decision). The security check in step 2 is completely independent and always enforced.

## Cache Security

### Result Caching

Only **read-only queries** are cached:

```go
// Cached (read-only):
MATCH (n:Person) RETURN n.name           // ✅ Cached
CALL db.labels()                          // ✅ Cached (db.* procedures)
SHOW INDEXES                              // ✅ Cached

// NOT cached (write operations):
CREATE (n:Person {name: 'Alice'})         // ❌ Not cached
MATCH (n) SET n.updated = timestamp()     // ❌ Not cached
CALL gds.graph.drop('myGraph')            // ❌ Not cached (non-db.* procedure)
```

### Cache Invalidation

Write operations invalidate relevant caches:

```go
// Smart invalidation by label:
CREATE (n:Person)                         // Invalidates :Person cache
MATCH (n:Company) DELETE n                // Invalidates :Company cache

// Full invalidation (no label detected):
MATCH (n) DELETE n                        // Invalidates ALL caches
```

### Cache Key Security

Cache keys include:
- Normalized query string
- Parameter values (hashed)

This prevents:
- Cache poisoning across different parameters
- Cache confusion between similar queries

## Procedure Call Security

### Read-Only Procedures (Cached)

Only `CALL db.*` procedures are marked as read-only:

```cypher
CALL db.labels()                    -- ✅ Read-only, cached
CALL db.propertyKeys()              -- ✅ Read-only, cached
CALL db.relationshipTypes()         -- ✅ Read-only, cached
```

### Write Procedures (Not Cached)

All other procedures are conservatively treated as writes:

```cypher
CALL gds.graph.project(...)         -- ❌ May be write, not cached
CALL gds.graph.drop(...)            -- ❌ Is a write, not cached
CALL apoc.create.node(...)          -- ❌ Is a write, not cached
CALL custom.myProcedure(...)        -- ❌ Unknown, not cached
```

## Implementation Details

### Keyword Detection

```go
// containsKeyword searches ALL occurrences, checking word boundaries
func containsKeyword(upper, keyword string) bool {
    // Iterates through string finding each occurrence
    // Checks character before: must not be alphanumeric or _
    // Checks character after: must not be alphanumeric or _
    // Returns true if ANY occurrence is a valid keyword
}
```

### Word Boundary Rules

A keyword match requires:
- **Before:** Start of string, or non-alphanumeric (except `_`)
- **After:** End of string, or non-alphanumeric (except `_`)

```
"DELETE n"        → DELETE matched (space before, space after)
"n.delete"        → delete matched (. is not alphanumeric) - FALSE POSITIVE
"ToDelete"        → DELETE not matched (o before)
"DELETED"         → DELETE not matched (D after)
```

## Query Types Reference

### Write Operations (Never Cached)

| Keyword | Detection | Example |
|---------|-----------|---------|
| `CREATE` | Word boundary | `CREATE (n:Node)` |
| `MERGE` | Word boundary | `MERGE (n:Node {id: 1})` |
| `DELETE` | Word boundary | `MATCH (n) DELETE n` |
| `DETACH DELETE` | Phrase | `MATCH (n) DETACH DELETE n` |
| `SET` | Word boundary | `MATCH (n) SET n.x = 1` |
| `REMOVE` | Word boundary | `MATCH (n) REMOVE n.label` |

### Read Operations (Cacheable)

| Pattern | Requirement | Example |
|---------|-------------|---------|
| `MATCH ... RETURN` | No write keywords | `MATCH (n) RETURN n` |
| `CALL db.*` | Starts with `CALL db.` | `CALL db.labels()` |
| `SHOW` | Starts with `SHOW` | `SHOW INDEXES` |

### Schema Operations (Special Handling)

| Operation | Cached | Invalidates |
|-----------|--------|-------------|
| `CREATE INDEX` | No | Schema cache |
| `DROP INDEX` | No | Schema cache |
| `CREATE CONSTRAINT` | No | Schema cache |
| `DROP CONSTRAINT` | No | Schema cache |

## Testing & Verification

### Security Tests

The following attack patterns are verified in tests:

```go
// All write operations detected:
"MATCH (n) DELETE n"                    // HasDelete = true ✅
"MATCH (n) WHERE type = 'safe' DELETE n" // HasDelete = true ✅
"CREATE (n:Node)"                        // HasCreate = true ✅
"MERGE (n:Node)"                         // HasMerge = true ✅
"MATCH (n) SET n.x = 1"                  // HasSet = true ✅
"MATCH (n) REMOVE n.label"               // HasRemove = true ✅

// False positives (safe, not cached):
"RETURN n.delete"                        // HasDelete = true (false positive)
"WHERE name = 'DELETE'"                  // HasDelete = true (false positive)
```

### Running Security Tests

```bash
# Run all cypher tests including security checks
cd nornicdb
go test ./pkg/cypher/... -v

# Run specific cache/security tests
go test ./pkg/cypher/... -run "Cache|Security" -v
```

## Best Practices

### For Query Authors

1. **Avoid keyword-named properties** - Don't name properties `delete`, `create`, `set`, etc.
2. **Use parameters for values** - `WHERE name = $name` instead of `WHERE name = 'DELETE'`
3. **Explicit transactions for writes** - Use `BEGIN/COMMIT` for critical write operations

### For Administrators

1. **Monitor cache hit rates** - Low hit rates may indicate false positives
2. **Review slow query logs** - Uncached queries appear more frequently
3. **Use production mode** - Ensures strict security settings

## Summary

| Aspect | Protection Level | Notes |
|--------|-----------------|-------|
| Write ops hidden as reads | ✅ **Fully Protected** | Not possible in valid Cypher |
| Read ops marked as writes | ⚡ **Accepted** | Performance impact only |
| False positive data leakage | ✅ **Protected** | Analyzer doesn't affect access control |
| Cache poisoning | ✅ **Protected** | Keys include parameters |
| Procedure writes | ✅ **Protected** | Only `db.*` cached |
| Access control bypass | ✅ **N/A** | Analyzer not used for auth |
| Cache timing side-channel | ✅ **Protected** | Query must succeed before cache action |

---

**See Also:**
- [HTTP Security](http-security.md) - Network-level protections
- [Compliance Guide](../compliance/) - Regulatory requirements
