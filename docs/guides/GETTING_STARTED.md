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
```

### Option 2: Docker

```bash
# Pull the image (ARM64/Apple Silicon)
docker pull timothyswt/nornicdb-arm64-metal:0.1.3

# Or use latest
docker pull timothyswt/nornicdb-arm64-metal:latest

# Run the container
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal:0.1.3

# Verify it's running
curl http://localhost:7474/health
```

**Available Tags:**
- `timothyswt/nornicdb-arm64-metal:0.1.3` - Current stable release
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

### 1. Create a Database

```go
package main

import (
    "context"
    "log"
    
    "github.com/orneryd/nornicdb/pkg/nornicdb"
)

func main() {
    // Open database
    db, err := nornicdb.Open("./mydb", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    ctx := context.Background()
    
    // Store a memory
    memory := &nornicdb.Memory{
        Content: "Machine learning is a subset of AI",
        Title:   "ML Definition",
        Tier:    nornicdb.TierSemantic,
        Tags:    []string{"AI", "ML"},
    }
    
    stored, err := db.Store(ctx, memory)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Stored memory: %s\n", stored.ID)
}
```

### 2. Query Data

```go
// Execute Cypher queries
result, err := db.ExecuteCypher(ctx, 
    "MATCH (n) RETURN count(n)", nil)
if err != nil {
    log.Fatal(err)
}

log.Printf("Total nodes: %v\n", result.Rows[0][0])
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

| Mode | Config | Write Latency | Durability | HTTP Status |
|------|--------|---------------|------------|-------------|
| **Strong** | `AsyncWritesEnabled: false` | ~50-100ms | Immediate | `200 OK` |
| **Eventual** | `AsyncWritesEnabled: true` | <1ms | Within flush interval | `202 Accepted` |

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
- Write operations (CREATE, DELETE, SET) return immediately
- Data is flushed to disk every `AsyncFlushInterval`
- HTTP responses include header `X-NornicDB-Consistency: eventual`
- Mutations return `202 Accepted` instead of `200 OK`

**Trade-offs:**
- ✅ Much faster writes (~100x improvement)
- ✅ Better throughput for batch operations
- ⚠️ Data may be lost if crash before flush (use with WAL for durability)
- ⚠️ Reads may see slightly stale data (within flush interval)

## Memory Tiers

NornicDB simulates human memory with three tiers:

| Tier | Half-Life | Use Case | Example |
|------|-----------|----------|---------|
| **Episodic** | 7 days | Short-term events | "I ran a test yesterday" |
| **Semantic** | 69 days | Facts and concepts | "Python is a programming language" |
| **Procedural** | 693 days | Skills and procedures | "How to deploy to production" |

```go
// Create episodic memory (short-term)
memory := &nornicdb.Memory{
    Content: "Fixed bug in authentication module",
    Tier:    nornicdb.TierEpisodic,
}

// Create semantic memory (long-term facts)
memory := &nornicdb.Memory{
    Content: "NornicDB supports Neo4j Cypher queries",
    Tier:    nornicdb.TierSemantic,
}

// Create procedural memory (skills)
memory := &nornicdb.Memory{
    Content: "Deploy using: docker-compose up -d",
    Tier:    nornicdb.TierProcedural,
}
```

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
    "mimir": {
      "url": "http://localhost:7474/mcp",
      "type": "http",
      "description": "NornicDB MCP Server"
    }
  }
}
```

### Available MCP Tools

| Tool | Purpose |
|------|---------|
| `store` | Save knowledge/decisions |
| `recall` | Retrieve by ID or filters |
| `discover` | Semantic search |
| `link` | Connect concepts |
| `index` | Index files |
| `unindex` | Remove indexed files |
| `task` | Manage single task |
| `tasks` | Query multiple tasks |

See **[Cursor Chat Mode Guide](cursor-chatmode.md)** for detailed usage.

## Next Steps

- **[Cursor Chat Mode Guide](cursor-chatmode.md)** - Use with Cursor IDE
- **[MCP Tools Quick Reference](../MCP_TOOLS_QUICKREF.md)** - Tool cheat sheet
- **[Vector Search Guide](vector-search.md)** - Learn semantic search
- **[Cypher Queries](cypher-queries.md)** - Master Neo4j queries
- **[API Reference](../api-reference.md)** - Complete API docs

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

- **[Documentation](../index.md)** - Full documentation
- **[GitHub Issues](https://github.com/orneryd/nornicdb/issues)** - Report bugs
- **[Discussions](https://github.com/orneryd/nornicdb/discussions)** - Ask questions
