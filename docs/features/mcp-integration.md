# MCP Tools Quick Reference
**For LLMs:** This is your cheat sheet for using NornicDB's memory system.

> **Note:** NornicDB focuses on storage, embeddings, and search. File indexing is handled by the application layer.

---

## 🎯 Quick Decision Tree

**Want to remember something?** → `store`  
**Know the ID, need to fetch?** → `recall`  
**Search by meaning/topic?** → `discover`  
**Connect two things?** → `link`  
**Work with tasks?** → `tasks` (create, manage and list)

---

## Core Tools (One-Liner Each)

| Tool | Use When | Example |
|------|----------|---------|
| `store` | Remembering any information | `store(content="Use Postgres", type="decision")` |
| `recall` | Getting something by ID or filters | `recall(id="node-123")` |
| `discover` | Finding by meaning, not keywords | `discover(query="auth implementation")` |
| `link` | Connecting two nodes (from/to must be node IDs from store or Cypher) | `link(from="node-abc", to="node-xyz", relation="depends_on")` |
| `tasks` | Task CRUD and listing | `tasks(title="Fix bug", priority="high")`, `tasks(status=["pending"], unblocked_only=true)` |

---

## 💡 Common Patterns

### Store & Link Pattern
```
1. id1 = store(content="We use PostgreSQL", type="decision")
2. id2 = store(content="Add connection pooling", type="task")
3. link(from=id2, to=id1, relation="implements")
```

### Search & Recall Pattern
```
1. results = discover(query="authentication bugs", limit=5)
2. For each result.id:
   - details = recall(id=result.id)  # Get full context
```

### Task Workflow Pattern
```
1. tasks(status=["pending"], unblocked_only=true)  # Find work
2. tasks(id="task-123", status="active")         # Start task
3. tasks(id="task-123", status="done")           # Complete task
```

### Code Search Pattern
```
# File indexing is done by the application layer; NornicDB stores and searches the result.
1. discover(query="database connection pool", type=["file", "file_chunk"])
2. recall(id="file-xyz")  # Get full file content
```

---

## 📋 Parameter Cheat Sheet

### store
```yaml
content: string ✅ REQUIRED
type: "memory" | "decision" | "concept" | "task" | "note" | "file" | "code"
title: string (auto-generated if omitted)
tags: ["tag1", "tag2"]
metadata: {key: "value"}
```

### recall
```yaml
id: "node-123" (fetch specific node)
OR
type: ["decision", "task"]
tags: ["urgent"]
since: "2024-11-01"
limit: 10
```

### discover
```yaml
query: "natural language search" ✅ REQUIRED
type: ["file", "decision", "task"] (filter)
limit: 10
min_similarity: 0.70 (0.0-1.0, lower=more results)
depth: 1 (1-3, higher=more related context)
```

### link
```yaml
from: "node-123" ✅ REQUIRED
to: "node-456" ✅ REQUIRED
relation: "depends_on" | "relates_to" | "implements" | "blocks" ✅ REQUIRED
strength: 1.0 (0.0-1.0)
metadata: {key: "value"}
```

### tasks
```yaml
# CREATE:
title: "Fix auth bug" ✅ REQUIRED
description: "Details..."
status: "pending" | "active" | "done" | "blocked"
priority: "low" | "medium" | "high" | "critical"
depends_on: ["task-123", "task-456"]
assign: "agent-worker-1"

# UPDATE:
id: "task-123" ✅ REQUIRED
status: "done" (or omit to toggle: pending→active→done)

# LIST (management args absent):
status: ["pending", "active"]
priority: ["high", "critical"]
assigned_to: "agent-worker-1"
unblocked_only: true (no blocking dependencies)
limit: 20
```

---

## 🔥 Most Common Mistakes

### ❌ Using recall for semantic search
```
❌ recall(query="authentication")  # Wrong! recall is for ID/filters
✅ discover(query="authentication") # Right! discover is for meaning
```

### ❌ Forgetting required parameters
```
❌ store(type="decision")          # Missing content!
✅ store(content="...", type="decision")
```

### ❌ Wrong relation names
```
❌ link(from=A, to=B, relation="connected")  # Not a valid relation
✅ link(from=A, to=B, relation="relates_to") # Valid
```

### ❌ Mixing management and listing arguments
```
❌ tasks(id="task-123", unblocked_only=true)  # id switches to update mode
✅ tasks(id="task-123")                      # update/toggle one task
✅ tasks(unblocked_only=true)                 # list unblocked tasks
```

---

## 📊 Response Fields You Should Use

### store response
```json
{
  "id": "node-abc123",           // ← Use this for linking!
  "title": "Generated Title",
  "embedded": true,
  "suggestions": [...]            // ← Similar nodes for auto-linking
}
```

### recall response
```json
{
  "nodes": [{...}],
  "count": 5,
  "related": [...]                // ← 1-hop neighbors for context
}
```

### discover response
```json
{
  "results": [{...}],
  "method": "vector",             // ← "vector" or "keyword"
  "total": 10,
  "suggestions": [...]            // ← Related searches
}
```

### task response
```json
{
  "task": {...},
  "blockers": [...],              // ← Tasks blocking this one
  "subtasks": [...],              // ← Child tasks
  "next_action": "..."            // ← Suggested next step
}
```

### tasks response
```json
{
  "tasks": [...],
  "stats": {                      // ← Aggregate statistics
    "total": 50,
    "by_status": {...},
    "by_priority": {...}
  },
  "dependency_graph": [...],      // ← Task dependencies
  "recommended": [...]            // ← Best tasks to work on
}
```

---

## Performance Tips

1. **Use IDs, not repeated queries**
   ```
   ❌ discover() → recall() → discover() → recall()  # Slow!
   ✅ discover() → get IDs → link(from=id1, to=id2) # Fast!
   ```

2. **Batch related operations**
   ```
   ❌ store() → link() → store() → link() (4 calls)
   ✅ id1=store() → id2=store() → link() (3 calls, parallel possible)
   ```

3. **Adjust similarity threshold**
   ```
   Too few results? → Lower min_similarity (0.65 instead of 0.75)
   Too many results? → Raise min_similarity (0.85 instead of 0.75)
   ```

4. **Use depth wisely**
   ```
   Quick search → depth=1 (direct matches only)
   Need context → depth=2 (includes 1-hop neighbors)
   Deep research → depth=3 (2-hop expansion, slower)
   ```

---

## 🎯 Task Status State Machine

```
pending → active → done
   ↓         ↓       ↑
   └──> blocked ────┘

Use tasks(id=X) with no status to auto-advance:
- pending → active
- active → done
- done → (no change)
- blocked → (no change, fix blockers first)
```

---

## 🔍 When to Use What Search

| Scenario | Tool | Why |
|----------|------|-----|
| "Get node abc-123" | `recall(id="abc-123")` | You know the ID |
| "Find all high priority tasks" | `recall(type=["task"], filters={priority:"high"})` | Exact property match |
| "What do we know about auth?" | `discover(query="authentication")` | Semantic meaning search |
| "Show pending tasks" | `tasks(status=["pending"])` | Specialized task query |

---

## 💬 Example Conversations

### Example 1: Storing a Decision
```
User: "We decided to use PostgreSQL instead of MongoDB for better ACID guarantees"

LLM: I'll store this decision.
→ store(
    content="We decided to use PostgreSQL instead of MongoDB for better ACID guarantees",
    type="decision",
    tags=["database", "architecture", "postgres"]
  )
← {id: "node-123", title: "Database Choice: PostgreSQL vs MongoDB"}

Done! Stored as decision node-123.
```

### Example 2: Finding Related Information
```
User: "What do we know about our database setup?"

LLM: Let me search our knowledge base.
→ discover(query="database setup and configuration", type=["decision", "code", "file"])
← {results: [
    {id: "node-123", title: "Database Choice: PostgreSQL..."},
    {id: "file-456", title: "database.go - connection pool"},
    ...
  ], method: "vector"}

Found 5 relevant items:
1. Decision: Database Choice: PostgreSQL vs MongoDB
2. File: database.go - connection pool configuration
...
```

### Example 3: Task Management
```
User: "Show me what I should work on next"

LLM: Looking for unblocked, high-priority tasks.
→ tasks(status=["pending"], priority=["high", "critical"], unblocked_only=true)
← {tasks: [...], recommended: [
    {id: "task-789", title: "Implement auth", priority: "high", blockers: []}
  ]}

I recommend starting with:
1. task-789: Implement auth (high priority, no blockers)
```

---

## 📚 Further Reading

- **Using MCP tools in Bifrost (agentic loop):** The same tools can be exposed to the Heimdall chat assistant so the LLM can call store/recall/link etc. in process. They are **off by default**. See [Enabling MCP tools in the agentic loop](../user-guides/heimdall-mcp-tools.md).
- **Rationale and tool set:** This document describes the current MCP tool set (store, recall, discover, link, tasks); implementation lives in `pkg/mcp/`.
- **Configuration:** To enable or restrict which MCP tools are available in the agentic loop, see [Configuration Guide](../operations/configuration.md) (Heimdall / MCP sections) and [heimdall-mcp-tools](../user-guides/heimdall-mcp-tools.md).

---
