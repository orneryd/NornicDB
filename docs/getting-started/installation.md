# Getting Started with NornicDB

Get up and running with NornicDB in 5 minutes.

## Prerequisites

- Go 1.21 or later
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

- `nornicdb serve` - Start the database server
- `nornicdb shell` - Interactive Cypher query shell
- `nornicdb decay` - Memory decay management (recalculate, archive, stats)
- `nornicdb init` - Initialize a new database
- `nornicdb import` - Import data from Neo4j export

See **[CLI Commands Guide](../operations/cli-commands.md)** for complete documentation.

### Option 2: Docker

```bash
# Pull the image (ARM64/Apple Silicon)
docker pull timothyswt/nornicdb-arm64-metal:v1.0.0

# Or use latest
docker pull timothyswt/nornicdb-arm64-metal:latest

# Run the container
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal:v1.0.0

# Verify it's running
curl http://localhost:7474/health
```

**Available Tags:**

- `timothyswt/nornicdb-arm64-metal:v1.0.0` - Current stable release
- `timothyswt/nornicdb-arm64-metal:latest` - Latest build

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
// Search with embeddings
results, err := db.Search(ctx, "artificial intelligence", 10)
if err != nil {
    log.Fatal(err)
}

for _, result := range results {
    log.Printf("Found: %s (score: %.3f)\n",
        result.Title, result.Score)
}
```

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

User guide: `docs/user-guides/qdrant-grpc.md`

## Knowledge-Layer Scoring

NornicDB uses a declarative, profile-driven decay and promotion system instead of hardcoded memory tiers. You define **decay profiles** and **retention bindings** using Cypher DDL:

```cypher
-- Define how fast a category of knowledge decays
CREATE DECAY PROFILE memory_episode_retention
  HALF LIFE 604800       -- 7 days in seconds
  DECAY FLOOR 0.0
  VISIBILITY THRESHOLD 0.10;

-- Bind the profile to a node label
CREATE RETENTION BINDING episode_retention
  FOR (n:MemoryEpisode)
  USING PROFILE memory_episode_retention;
```

Properties can have independent decay rates or be pinned with `NO DECAY`:

```cypher
CREATE RETENTION BINDING fact_retention
  FOR (n:KnowledgeFact)
  USING PROFILE knowledge_fact_retention
  PROPERTY n.tenantId NO DECAY
  PROPERTY n.summary HALF LIFE 2592000;
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
- **[API Reference](../api-reference/)** - Complete API docs

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
