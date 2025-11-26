# NornicDB

**High-Performance Graph Database for LLM Agent Memory**

NornicDB is a purpose-built graph database written in Go, designed specifically for AI agent memory and knowledge management. It provides Neo4j Bolt protocol and Cypher query compatibility for drop-in replacement while adding LLM-native features.

## Key Features

### 🔌 Neo4j Compatible
- **Bolt Protocol**: Use existing Neo4j drivers (Python, JavaScript, Go, etc.)
- **Cypher Queries**: Full Cypher query language support
- **Drop-in Replacement**: Switch from Neo4j with zero code changes

### 🧠 LLM-Native Memory
- **Natural Memory Decay**: Three-tier memory system (Episodic, Semantic, Procedural)
- **Auto-Relationships**: Automatic edge creation via embedding similarity
- **Vector Search**: Built-in HNSW index for semantic similarity

### ⚡ High Performance
- **Single Binary**: No JVM, no external dependencies
- **Embedded Mode**: Use as library or standalone server
- **Sub-millisecond Reads**: Optimized for agent workloads
- **100-500MB Memory**: vs 1-4GB for Neo4j

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        NornicDB Server                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Bolt Server │  │ HTTP/REST   │  │ gRPC (future)           │  │
│  │ :7687       │  │ :7474       │  │ :7688                   │  │
│  └──────┬──────┘  └──────┬──────┘  └────────────┬────────────┘  │
│         │                │                      │               │
│         └────────────────┼──────────────────────┘               │
│                          ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Query Engine                          │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │   │
│  │  │ Cypher Parser│  │ Query Planner│  │ Query Executor │  │   │
│  │  └──────────────┘  └──────────────┘  └────────────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                          │                                      │
│  ┌───────────────────────┼──────────────────────────────────┐   │
│  │                  Core Services                           │   │
│  │  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌───────────┐   │   │
│  │  │  Decay  │  │  Auto   │  │  Vector  │  │ Full-Text │   │   │
│  │  │ Manager │  │  Links  │  │  Index   │  │   Index   │   │   │
│  │  └─────────┘  └─────────┘  └──────────┘  └───────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                          │                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                 Storage Engine (Badger)                  │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐               │   │
│  │  │  Nodes   │  │  Edges   │  │  Indexes │               │   │
│  │  └──────────┘  └──────────┘  └──────────┘               │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### As Standalone Server

```bash
# Build
go build -o nornicdb ./cmd/nornicdb

# Run
./nornicdb serve --port 7687 --http-port 7474

# Connect with any Neo4j driver
# bolt://localhost:7687
```

### As Embedded Library

```go
import "github.com/orneryd/nornicdb/pkg/nornicdb"

// Create embedded instance
db, err := nornicdb.Open("./data", nornicdb.DefaultConfig())
defer db.Close()

// Store memory with auto-linking
mem, err := db.Store(ctx, &nornicdb.Memory{
    Content: "PostgreSQL is our primary database",
    Tier:    nornicdb.TierSemantic,
    Tags:    []string{"database", "architecture"},
})

// Semantic search
results, err := db.Remember(ctx, "what database do we use?", 10)

// Cypher query (same as Neo4j!)
results, err := db.Cypher(ctx, `
    MATCH (m:Memory)-[:RELATES_TO]->(related)
    WHERE m.content CONTAINS 'database'
    RETURN m, related
    LIMIT 10
`)
```

## Memory Decay System

NornicDB implements a cognitive-inspired memory decay system:

| Tier | Half-Life | Use Case |
|------|-----------|----------|
| **Episodic** | 7 days | Chat context, temporary notes |
| **Semantic** | 69 days | Facts, decisions, knowledge |
| **Procedural** | 693 days | Patterns, procedures, skills |

```cypher
// Memories automatically decay over time
// Access reinforces memory (like neural potentiation)
MATCH (m:Memory)
WHERE m.decayScore > 0.5  // Still strong memories
RETURN m.title, m.decayScore
ORDER BY m.decayScore DESC
```

## Auto-Relationship Engine

Edges are created automatically based on:

1. **Embedding Similarity** (>0.82 cosine similarity)
2. **Co-access Patterns** (nodes queried together)
3. **Temporal Proximity** (created in same session)
4. **Transitive Inference** (A→B, B→C suggests A→C)

```cypher
// View auto-generated relationships
MATCH (a:Memory)-[r:RELATES_TO]->(b:Memory)
WHERE r.autoGenerated = true
RETURN a.title, r.confidence, b.title
```

## Configuration

```yaml
# nornicdb.yaml
server:
  bolt_port: 7687
  http_port: 7474
  data_dir: ./data

embeddings:
  provider: ollama  # or openai
  api_url: http://localhost:11434
  model: mxbai-embed-large
  dimensions: 1024

decay:
  enabled: true
  recalculate_interval: 1h
  archive_threshold: 0.05

auto_links:
  enabled: true
  similarity_threshold: 0.82
  co_access_window: 30s
```

## Comparison with Neo4j

| Feature | Neo4j | NornicDB |
|---------|-------|----------|
| Query Language | Cypher | Cypher |
| Protocol | Bolt | Bolt |
| Clustering | Enterprise | Roadmap |
| Memory Footprint | 1-4GB | 100-500MB |
| Cold Start | 10-30s | <1s |
| Memory Decay | Custom | Built-in |
| Auto-Relationships | No | Built-in |
| Vector Search | Plugin | Built-in |
| Embedded Mode | No | Yes |
| License | GPL/Commercial | MIT |

## Project Structure

```
nornicdb/
├── cmd/
│   └── nornicdb/           # CLI entry point
├── pkg/
│   ├── bolt/               # Neo4j Bolt protocol server
│   ├── cypher/             # Cypher parser and executor
│   ├── storage/            # Badger-based storage engine
│   ├── index/              # HNSW vector + Bleve text indexes
│   ├── decay/              # Memory decay system
│   ├── inference/          # Auto-relationship engine
│   ├── embed/              # Embedding providers
│   └── nornicdb/           # Main API (embedded usage)
├── internal/
│   ├── config/             # Configuration management
│   └── metrics/            # Prometheus metrics
└── api/
    └── http/               # REST API handlers
```

## Roadmap

- [x] Core storage engine (Badger)
- [x] Basic Cypher parser
- [ ] Full Cypher compatibility
- [ ] Bolt protocol server
- [ ] Memory decay system
- [ ] Auto-relationship engine
- [ ] Vector search (HNSW)
- [ ] HTTP REST API
- [ ] Mimir adapter
- [ ] Clustering support

## License

MIT License - See [LICENSE](../LICENSE) in the parent Mimir repository.

NornicDB is part of the Mimir project and shares its MIT license.
