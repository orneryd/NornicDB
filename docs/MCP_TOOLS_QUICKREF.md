# MCP Tools Quick Reference

**For LLMs:** This is your cheat sheet for using NornicDB's memory system.

---

## 🎯 Quick Decision Tree

**Want to remember something?** → `store`  
**Know the ID, need to fetch?** → `recall`  
**Search by meaning/topic?** → `discover`  
**Connect two things?** → `link`  
**Index code files?** → `index`  
**Remove indexed files?** → `unindex`  
**Work with tasks?** → `task` (single) or `tasks` (multiple)

---

## Core Tools (One-Liner Each)

| Tool | Use When | Example |
|------|----------|---------|
| `store` | Remembering any information | `store(content="Use Postgres", type="decision")` |
| `recall` | Getting something by ID or filters | `recall(id="node-123")` |
| `discover` | Finding by meaning, not keywords | `discover(query="auth implementation")` |
| `link` | Connecting related knowledge | `link(from="A", to="B", relation="depends_on")` |
| `index` | Making code searchable | `index(path="/workspace/src")` |
| `unindex` | Removing indexed code | `unindex(path="/workspace/old")` |
| `task` | Single task CRUD | `task(title="Fix bug", priority="high")` |
| `tasks` | Query/list multiple tasks | `tasks(status=["pending"], unblocked_only=true)` |

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
2. task(id="task-123", status="active")           # Start task
3. task(id="task-123", status="done")             # Complete task
```

### Code Search Pattern
```
1. index(path="/workspace/src", patterns=["*.go"])
2. discover(query="database connection pool", type=["file", "file_chunk"])
3. recall(id="file-xyz")  # Get full file content
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

### index
```yaml
path: "/workspace/src" ✅ REQUIRED
patterns: ["*.go", "*.md"] (default: all text files)
embeddings: true (generate vectors for search)
recursive: true (include subdirectories)
```

### unindex
```yaml
path: "/workspace/old" (folder to remove)
OR
watch_id: "watch-123" (use ID instead)
```

### task
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
```

### tasks
```yaml
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

### ❌ Using tasks for single task operations
```
❌ tasks(id="task-123")            # tasks is for multiple!
✅ task(id="task-123")             # task is for single
```

---

## 🎨 Relation Types Reference

Use these in the `link` tool:

| Relation | Meaning | Example |
|----------|---------|---------|
| `depends_on` | A requires B to complete | task depends_on decision |
| `relates_to` | A and B are related | concept relates_to concept |
| `implements` | A implements/executes B | task implements decision |
| `caused_by` | A was caused by B | bug caused_by code_change |
| `blocks` | A prevents B from progressing | bug blocks task |
| `contains` | A contains B as part | project contains task |
| `references` | A mentions/cites B | doc references code |
| `uses` | A uses/depends on B | code uses library |
| `evolved_from` | A is a newer version of B | decision_v2 evolved_from decision_v1 |
| `contradicts` | A contradicts/conflicts with B | decision_new contradicts decision_old |

---

## 🧠 Type System Reference

Use these in `store` and `discover`:

| Type | Purpose | Example Content |
|------|---------|-----------------|
| `memory` | General knowledge | "Team uses Slack for communication" |
| `decision` | Architectural decisions | "Chose PostgreSQL over MongoDB" |
| `concept` | Abstract concepts | "Microservices architecture explanation" |
| `task` | Work items | "Implement user authentication" |
| `note` | Temporary notes | "Remember to check logs" |
| `file` | File metadata | "src/auth.go - authentication module" |
| `code` | Code snippets | "Function for JWT validation" |
| `conversation` | Chat history | "Discussion about API design" |

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

## 🚀 Performance Tips

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

Use task(id=X) with no status to auto-advance:
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

- **Full Design Doc**: See `MCP_TOOLS_OPTIMIZATION.md` for rationale and architecture
- **Implementation**: See `pkg/mcp/` for Go implementation
- **Migration Guide**: See `MIGRATION_v0_to_v1.md` for upgrading from old tools

---

**Last Updated:** 2025-11-27  
**Version:** 1.0 (Design Phase)
