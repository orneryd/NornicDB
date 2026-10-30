# Using NornicDB with Cursor Chat Modes

> **Version:** 0.2.0  
> **Prerequisite:** NornicDB running locally or via Docker

This guide explains how to use NornicDB as a persistent memory system within Cursor IDE using custom chat modes (agent preambles).

---

## 🎯 What is nornicdb-v2 Chat Mode?

The `nornicdb-v2` chat mode transforms Cursor's AI assistant into a **memory-augmented coding agent** that:

- **Remembers** decisions, solutions, and context across sessions
- **Discovers** related knowledge semantically (by meaning, not keywords)
- **Links** concepts together into a knowledge graph
- **Tracks** tasks with dependencies and status

Instead of repeating context every conversation, the agent stores and retrieves from a persistent graph database.

---

## 📦 Setup

### 1. Start NornicDB

**Option A: Docker (Recommended)**

```bash
docker run -d \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  --name nornicdb \
  timothyswt/nornicdb-arm64-metal:v1.0.0
```

**Option B: Docker Compose**

```yaml
# docker-compose.yml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:v1.0.0
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - ./data/nornicdb:/data
    environment:
      - NORNICDB_DATA_DIR=/data
      - NORNICDB_HTTP_PORT=7474
      - NORNICDB_BOLT_PORT=7687
```

```bash
docker compose up -d
```

### 2. Configure Cursor MCP

Add NornicDB to your Cursor MCP configuration:

**File:** `~/.cursor/mcp.json`

```json
{
  "mcpServers": {
    "nornicdb": {
      "url": "http://localhost:7474/mcp",
      "type": "http",
      "description": "NornicDB MCP Server for persistent memory and task management"
    }
  }
}
```

### 3. Install the Chat Mode

Copy the `claudette-nornicdb-v2.md` file to your Cursor chat modes directory or create a new chat mode in Cursor settings.

**Location:** `docs/agents/claudette-nornicdb-v2.md`

### 4. Restart Cursor

After configuring MCP, restart Cursor to load the new server.

---

## 🛠️ Available MCP Tools

NornicDB provides **7 tools** via MCP:

| Tool       | Purpose                           | Required Params                   |
| ---------- | --------------------------------- | --------------------------------- |
| `store`    | Save knowledge/decisions/memories | `content`                         |
| `recall`   | Retrieve by ID or filters         | (none - all optional)             |
| `discover` | Semantic search by meaning        | `query`                           |
| `link`     | Connect related concepts          | `from`, `to`, `relation`          |
| `tasks`    | Create, update and query tasks    | `title` (create), `id` (update)   |

> **Note:** `index` and `unindex` tools are handled by the application layer, not NornicDB directly.

---

## 💬 Natural Conversation Patterns

### Storing Knowledge

**User:** "Remember that we're using PostgreSQL for this project"

**Agent:** "Storing that decision..."

```
→ store(content="Using PostgreSQL as primary database for ACID compliance and team familiarity", type="decision", tags=["database", "architecture"])
← node-abc123 created

"Stored as node-abc123. I'll reference this when making database-related decisions."
```

### Recalling Knowledge

**User:** "What did we decide about the database?"

**Agent:** "Checking memory..."

```
→ discover(query="database decision architecture")
← Found: node-abc123 "Using PostgreSQL..."

"We decided to use PostgreSQL (node-abc123) for ACID compliance and team familiarity."
```

### Multi-Hop Reasoning

**User:** "I'm getting authentication errors"

**Agent:** "Let me search for related issues..."

```
→ discover(query="authentication errors", depth=2)
← Found: node-456 "CORS credentials issue"
         → connected to node-789 "Session cookie config"
         → connected to node-012 "Redis session store"

"Found a chain of related issues:
1. CORS credentials (node-456)
2. Which connects to session cookies (node-789)
3. Which connects to Redis setup (node-012)

The root cause might be in Redis configuration. Let me check..."
```

### Task Management

**User:** "What should I work on next?"

**Agent:**

```
→ tasks(status=["pending"], priority=["high", "critical"], unblocked_only=true)
← Found 3 tasks, recommended: task-123 "Implement auth"

"I recommend starting with:
1. task-123: Implement authentication (high priority, no blockers)
2. task-456: API documentation (medium priority)

Shall I start on task-123?"
```

---

## 🔄 Memory-First Workflow

The nornicdb-v2 chat mode enforces a **memory-first** approach:

### Search Order (Mandatory)

1. **`discover()`** - Check memory FIRST for any question
2. **`recall()`** - Get specific nodes by ID or criteria
3. **Local files** - Only if memory has no results
4. **External search** - Last resort

### Session Initialization

When starting a new session:

```
1. Memory check: discover(query="current project context")
2. Task check: tasks(status=["pending", "active"])
3. Continue where you left off
```

### Continuous Learning

As you work, the agent automatically:

1. **Stores** decisions with reasoning
2. **Links** related concepts together
3. **Updates** task progress
4. **Builds** knowledge graph connections

---

## 📊 Example Session

```markdown
User: "I need to add user authentication to our Go API"

Agent: Discovering similar work in memory...
→ discover(query="user authentication Go API")
← Found: node-892 "JWT implementation pattern"
node-893 "Go middleware authentication"

"Found 2 related items:

- JWT implementation pattern (node-892)
- Go middleware auth (node-893)

Creating a task to track this work..."

→ tasks(title="Implement user authentication", description="Add JWT-based auth to Go API", priority="high")
← task-456 created

→ link(from="task-456", to="node-892", relation="implements")
← edge created

"Created task-456: Implement user authentication

- Linked to existing JWT pattern (node-892)
- Status: pending
- Priority: high

Based on node-892, I recommend using the middleware pattern with JWT tokens.
Shall I start implementation?"

User: "Yes, go ahead"

Agent: Starting work on task-456...
→ tasks(id="task-456", status="active")

[... implementation work ...]

→ store(content="Implemented JWT auth using RS256 with 24h token expiry. Middleware validates Bearer tokens and adds user context to request.", type="decision", tags=["auth", "jwt", "go"])
← node-897 created

→ link(from="node-897", to="task-456", relation="implements")
← edge created

→ tasks(id="task-456", status="done")

"Task completed! Stored the implementation decision as node-897 for future reference."
```

---

## 🎨 Relationship Types

When using `link()`, use these standard relations:

| Relation       | Use Case                    | Example                      |
| -------------- | --------------------------- | ---------------------------- |
| `depends_on`   | Task dependencies           | task-A depends_on task-B     |
| `relates_to`   | General association         | concept relates_to concept   |
| `implements`   | Code/task implements design | code implements decision     |
| `caused_by`    | Error root cause            | bug caused_by change         |
| `blocks`       | Blocking dependency         | issue blocks task            |
| `contains`     | Parent-child                | project contains task        |
| `references`   | Documentation links         | doc references code          |
| `uses`         | Library/pattern usage       | code uses library            |
| `evolved_from` | Version iteration           | v2 evolved_from v1           |
| `contradicts`  | Conflicting info            | new_decision contradicts old |

---

## Advanced Features

### Vector Search with Embeddings

NornicDB automatically generates embeddings for all stored content, enabling semantic search:

```
discover(query="how to handle concurrent database connections")
```

This finds relevant content even if the exact words don't match.

### Graph Depth Traversal

Use `depth` parameter to explore connected knowledge:

```
discover(query="authentication", depth=2)
```

- `depth=1`: Direct matches only (default)
- `depth=2`: Includes 1-hop neighbors
- `depth=3`: 2-hop expansion (slower)

### File Indexing

> **Note:** File indexing is handled by the application layer.

Search indexed code by meaning:

```
discover(query="database connection pooling", type=["file"])
```

### Task Dependencies

Create task chains with blocking dependencies:

```
tasks(title="Deploy to staging", depends_on=["task-123", "task-456"])
```

Query only unblocked tasks:

```
tasks(status=["pending"], unblocked_only=true)
```

---

## 🔧 Troubleshooting

### MCP Tools Not Available

1. Verify NornicDB is running: `curl http://localhost:7474/health`
2. Check mcp.json configuration
3. Restart Cursor
4. Check Cursor's MCP server status

### Slow Semantic Search

1. Lower `limit` parameter
2. Increase `min_similarity` threshold
3. Filter by `type` to narrow results

### Memory Not Persisting

1. Check Docker volume is mounted
2. Verify data directory permissions
3. Check NornicDB logs: `docker logs nornicdb`

---

## 📚 Further Reading

- **[MCP Tools Quick Reference](../features/mcp-integration.md)** - Tool cheat sheet
- **[Vector Search Guide](../user-guides/vector-search.md)** - Semantic search details
- **[Getting Started](../getting-started/README.md)** - NornicDB basics
- **[Agent Preamble](agent-preamble.md)** - Full chat mode config

---

**Last Updated:** 2025-11-28  
**Version:** 0.1.3
