# Getting Started with NornicDB

Get up and running with NornicDB in 5 minutes.

## Prerequisites

- Go 1.26 or later (see `go.mod`)
- Docker (optional, for containerized deployment)
- 2GB RAM minimum (4GB recommended)

## Installation

### Option 1: From Source

```bash
# Clone the repository
git clone https://github.com/orneryd/nornicdb.git
cd nornicdb

# Build the binary
go build -o nornicdb ./cmd/nornicdb

# Verify installation
./nornicdb --version

# See available commands
./nornicdb --help
```

**Available Commands:**

- `nornicdb version` - Print build version.
- `nornicdb serve` - Start the database server.
- `nornicdb shell` - Interactive Cypher query shell.
- `nornicdb init` - Initialize a new data directory.
- `nornicdb decay suppress` - Re-evaluate suppression status.
- `nornicdb decay stats` - Print decay statistics.

See **[CLI Commands Guide](../operations/cli-commands.md)** for complete documentation. For migration from Neo4j, use the runnable scripts in [`scripts/migration/neo4j/`](https://github.com/orneryd/nornicdb/tree/main/scripts/migration/neo4j) instead of a CLI subcommand.

### Option 2: Docker

```bash
# Pull the image (ARM64/Apple Silicon, with bundled BGE-M3 embedding model)
docker pull timothyswt/nornicdb-arm64-metal-bge:latest

# Or use the minimal "bring your own model" image
docker pull timothyswt/nornicdb-arm64-metal:latest

# Run the container
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest

# Verify it's running
curl http://localhost:7474/health
```

**Available variants:** see the [Docker Image Quick Reference](image-quick-reference.md) for the full image matrix (Apple Silicon Metal, NVIDIA CUDA, AMD/Intel CPU, Vulkan, headless, BYOM, BGE-bundled).

### Option 3: Go Package

```go
import "github.com/orneryd/nornicdb/pkg/nornicdb"

// Use in your Go application
db, err := nornicdb.Open("./data", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

## Quick Start

### 1. Create a Database and Store Data

```go
package main

import (
    "context"
    "log"

    "github.com/orneryd/nornicdb/pkg/nornicdb"
)

func main() {
    db, err := nornicdb.Open("./mydb", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Create a node via Cypher
    _, err = db.ExecuteCypher(ctx,
        `CREATE (n:KnowledgeFact {
            content: "Machine learning is a subset of AI",
            title: "ML Definition",
            tags: ["AI", "ML"]
        }) RETURN n`, nil)
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Stored knowledge fact")
}
```

### 2. Query Data

```go
result, err := db.ExecuteCypher(ctx,
    "MATCH (n:KnowledgeFact) RETURN count(n)", nil)
if err != nil {
    log.Fatal(err)
}

log.Printf("Total facts: %v\n", result.Rows[0][0])
```

### 3. Vector Search

```go
// Search with embeddings (4-arg signature: ctx, query, labels filter, limit)
results, err := db.Search(ctx, "artificial intelligence", nil, 10)
if err != nil {
    log.Fatal(err)
}

for _, result := range results {
    log.Printf("Found: %s (score: %.3f)\n",
        result.Title, result.Score)
}
```

Pass `nil` (or an empty `[]string`) for the labels argument to search across every label; pass `[]string{"Document", "Memory"}` to restrict the candidate set.

## Configuration

### Default Configuration

```go
config := nornicdb.DefaultConfig()
// Customization:
config.DecayEnabled = true
config.AutoLinksEnabled = true
config.BoltPort = 7687
config.HTTPPort = 7474

db, err := nornicdb.Open("./data", config)
```

### Production Configuration

```go
config := &nornicdb.Config{
    DataDir:                      "/var/lib/nornicdb",
    EmbeddingProvider:            "openai",
    EmbeddingAPIURL:              "https://api.openai.com/v1",
    EmbeddingModel:               "text-embedding-3-large",
    EmbeddingDimensions:          3072,
    DecayEnabled:                 true,
    DecayRecalculateInterval:     30 * time.Minute,
    DecayArchiveThreshold:        0.01,
    AutoLinksEnabled:             true,
    AutoLinksSimilarityThreshold: 0.85,
    AutoLinksCoAccessWindow:      60 * time.Second,
    AsyncWritesEnabled:           true,  // Enable write-behind caching
    AsyncFlushInterval:           50 * time.Millisecond, // Flush interval
    BoltPort:                     7687,
    HTTPPort:                     7474,
}

db, err := nornicdb.Open("./data", config)
```

### Write Consistency Options

NornicDB supports two write consistency modes:

| Mode                 | Config                      | Write Latency            | Durability                                | HTTP Status                              |
| -------------------- | --------------------------- | ------------------------ | ----------------------------------------- | ---------------------------------------- |
| **Strong**           | `AsyncWritesEnabled: false` | ~50-100ms                | Immediate                                 | `200 OK`                                 |
| **Eventual-capable** | `AsyncWritesEnabled: true`  | <1ms for eligible writes | Within flush interval for eligible writes | `202 Accepted` only on the eventual path |

**Strong Consistency** (default off, but recommended for critical data):

```go
config.AsyncWritesEnabled = false  // Writes block until persisted
```

**Eventual Consistency** (default on, faster writes):

```go
config.AsyncWritesEnabled = true           // Writes return immediately
config.AsyncFlushInterval = 50 * time.Millisecond  // Flush every 50ms
```

When `AsyncWritesEnabled` is true:

- Async-eligible auto-commit `CREATE` operations return immediately
- Data is flushed to disk every `AsyncFlushInterval`
- HTTP responses include header `X-NornicDB-Consistency: eventual` only when the eventual path was used
- Those eventual responses return `202 Accepted` with `optimistic` metadata
- Mutations that stay on the transactional path still return `200 OK` and include durable `receipt` metadata

**Trade-offs:**

- ✅ Much faster writes (~100x improvement)
- ✅ Better throughput for batch operations
- ⚠️ Data may be lost if crash before flush (use with WAL for durability)
- ⚠️ Reads may see slightly stale data (within flush interval)

## Enable Semantic Search (Embeddings)

Embedding generation is **disabled by default** in current releases. If you want semantic search without manually providing vectors, enable embeddings:

```bash
export NORNICDB_EMBEDDING_ENABLED=true
```

Or via CLI:

```bash
./nornicdb serve --embedding-enabled
```

Then you can verify embeddings are enabled via:

- logs at startup (`✅ Embeddings ready: ...`) and
- API: `GET /nornicdb/embed/stats` (requires auth)

## Optional: Qdrant gRPC Endpoint (Qdrant SDK Compatibility)

NornicDB can expose a **Qdrant-compatible gRPC endpoint** (default port `6334`) so you can use Qdrant SDKs against NornicDB.

Enable via env:

```bash
export NORNICDB_QDRANT_GRPC_ENABLED=true
export NORNICDB_QDRANT_GRPC_LISTEN_ADDR=":6334"  # optional
```

If you want Qdrant clients to upsert/update/delete vectors directly, also set:

```bash
export NORNICDB_EMBEDDING_ENABLED=false
```

See the [Qdrant gRPC user guide](../user-guides/qdrant-grpc.md) for full API coverage and SDK examples.

## Knowledge-Layer Scoring

NornicDB uses a declarative, profile-driven decay and promotion system instead of hardcoded memory tiers. You define **decay profile bundles** with `OPTIONS { ... }` and then attach them to node or edge patterns using `CREATE DECAY PROFILE ... FOR ... APPLY { ... }`:

```cypher
-- Define reusable decay behavior. `scoreFloor` clamps the score value
-- (a 0.0 floor here is a no-op until the curve hits 0). `visibilityThreshold`
-- is a separate suppression cutoff: when finalScore < 0.10 the entity is
-- hidden from queries. They are independent levers.
CREATE DECAY PROFILE memory_episode_retention OPTIONS {
  halfLifeSeconds: 604800,
  function: 'exponential',
  scoreFloor: 0.0,
  visibilityThreshold: 0.10
};

-- Bind that profile to a node label
CREATE DECAY PROFILE episode_retention_binding
FOR (n:MemoryEpisode)
APPLY {
  DECAY PROFILE 'memory_episode_retention'
};
```

Properties can have independent overrides or be pinned with `NO DECAY`:

```cypher
CREATE DECAY PROFILE knowledge_fact_retention OPTIONS {
  halfLifeSeconds: 31536000,
  function: 'exponential',
  visibilityThreshold: 0.02
};

CREATE DECAY PROFILE fact_retention_binding
FOR (n:KnowledgeFact)
APPLY {
  DECAY PROFILE 'knowledge_fact_retention'
  n.tenantId NO DECAY
  n.summary DECAY HALF LIFE 2592000
};
```

See [Knowledge-Layer Policies](../user-guides/knowledge-layer-policies.md) and the [Ebbinghaus-Roynard Bootstrap](../user-guides/ebbinghaus-roynard-bootstrap.md) for full reference.

## MCP Integration (For AI Agents)

NornicDB includes a native MCP (Model Context Protocol) server for AI agent integration.

### MCP Server Configuration

The MCP server is **enabled by default**. You can disable it if you don't need AI agent integration:

**CLI Flag:**

```bash
# Disable MCP server
./nornicdb serve --mcp-enabled=false
```

**Environment Variable:**

```bash
# Disable MCP server via environment
export NORNICDB_MCP_ENABLED=false
./nornicdb serve
```

**Go Config:**

```go
import "github.com/orneryd/nornicdb/pkg/server"

config := server.DefaultConfig()
config.MCPEnabled = false  // Disable MCP server
```

When MCP is disabled:

- The `/mcp` endpoint will not be registered
- All other HTTP API endpoints remain functional
- Memory is saved (no MCP overhead)
- Useful for pure database use without AI integration

### Configure Cursor IDE

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "nornicdb": {
      "url": "http://localhost:7474/mcp",
      "type": "http",
      "description": "NornicDB MCP Server"
    }
  }
}
```

### Available MCP Tools

| Tool       | Purpose                   |
| ---------- | ------------------------- |
| `store`    | Save knowledge/decisions  |
| `recall`   | Retrieve by ID or filters |
| `discover` | Semantic search           |
| `link`     | Connect concepts          |
| `index`    | Index files               |
| `unindex`  | Remove indexed files      |
| `task`     | Manage single task        |
| `tasks`    | Query multiple tasks      |

See **[Cursor Chat Mode Guide](../ai-agents/chat-modes.md)** for detailed usage.

## Next Steps

- **[Cursor Chat Mode Guide](../ai-agents/chat-modes.md)** - Use with Cursor IDE
- **[MCP Tools Quick Reference](../features/mcp-integration.md)** - Tool cheat sheet
- **[Vector Search Guide](../user-guides/vector-search.md)** - Learn semantic search
- **[Cypher Queries](../user-guides/cypher-queries.md)** - Master Neo4j queries
- **[API Reference](../api-reference/README.md)** - Complete API docs

## Search index defaults

Out of the box, NornicDB builds both BM25 fulltext and vector ANN indexes for every database at startup — the same behaviour as previous releases. If you're running multi-tenant deployments or memory-constrained environments, see **[Low Memory Mode → Deferring search-index load with `warming=lazy`](../operations/low-memory-mode.md#deferring-search-index-load-with-warminglazy)** for per-database controls that can defer or skip index builds.

## Troubleshooting

### Port Already in Use

```bash
# Change ports in configuration
config.BoltPort = 7688
config.HTTPPort = 7475
```

### Out of Memory

```go
// Reduce cache sizes
config := nornicdb.DefaultConfig()
// Adjust decay settings to archive more aggressively
config.DecayArchiveThreshold = 0.05
```

### Slow Queries

```go
// Enable GPU acceleration
// See GPU Acceleration guide
```

## Getting Help

- **[Documentation](../README.md)** - Full documentation
- **[GitHub Issues](https://github.com/orneryd/nornicdb/issues)** - Report bugs
- **[Issues](https://github.com/orneryd/NornicDB/issues)** - Ask questions
