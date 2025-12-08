<p align="center">
  <img src="https://raw.githubusercontent.com/orneryd/NornicDB/refs/heads/main/docs/assets/logos/nornicdb-logo.svg" alt="NornicDB Logo" width="200"/>
</p>

<h1 align="center">NornicDB</h1>

<p align="center">
  <strong>The Graph Database That Learns</strong><br/>
  Neo4j-compatible • GPU-accelerated • Memory that evolves
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-1.0.0-success" alt="Version 1.0.0">
  <a href="https://github.com/orneryd/NornicDB"><img src="https://img.shields.io/badge/github-orneryd%2FNornicDB-blue?logo=github" alt="GitHub"></a>
  <a href="https://hub.docker.com/u/timothyswt"><img src="https://img.shields.io/badge/docker-ready-blue?logo=docker" alt="Docker"></a>
  <a href="https://neo4j.com/"><img src="https://img.shields.io/badge/neo4j-compatible-008CC1?logo=neo4j" alt="Neo4j Compatible"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%3E%3D1.21-00ADD8?logo=go" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#features">Features</a> •
  <a href="#docker-images">Docker</a> •
  <a href="#documentation">Docs</a>
</p>

---

## 🐳 Get Started in 30 Seconds

```bash
# Apple Silicon (M1/M2/M3) with bge-m3 embedding model + heimdall
docker pull timothyswt/nornicdb-arm64-metal-bge-heimdall:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge-heimdall


# Apple Silicon (M1/M2/M3) with bge-m3 embedding model
docker pull timothyswt/nornicdb-arm64-metal-bge:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge

# Apple Silicon (M1/M2/M3) BYOM
docker pull timothyswt/nornicdb-arm64-metal:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal

# Apple Silicon (M1/M2/M3) BYOM + no UI 
docker pull timothyswt/nornicdb-arm64-metal-headless:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-headless

# NVIDIA GPU (Windows/Linux) with bge-m3 embedding model + heimdall
docker pull timothyswt/nornicdb-amd64-cuda-bge-heimdall:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda-bge-heimdall

# NVIDIA GPU (Windows/Linux) with bge-m3 embedding model
docker pull timothyswt/nornicdb-amd64-cuda-bge:latest
docker run -d --gpus all -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda-bge

# NVIDIA GPU (Windows/Linux) BYOM
docker pull timothyswt/nornicdb-amd64-cuda:latest
docker run -d --gpus all -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cuda

# CPU Only (Windows/Linux) BYOM
docker pull timothyswt/nornicdb-amd64-cpu:latest
docker run -d -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cpu

# CPU Only (Windows/Linux) BYOM + no UI
docker pull timothyswt/nornicdb-amd64-headless:latest
docker run -d --gpus all -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data \
  timothyswt/nornicdb-amd64-cpu-headless
```

**Open [http://localhost:7474](http://localhost:7474)** — Admin UI with AI assistant ready to query your data.

---

## What is NornicDB?

NornicDB is a high-performance graph database designed for AI agents and knowledge systems. It speaks Neo4j's language (Bolt protocol + Cypher) so you can switch with zero code changes, while adding intelligent features that traditional databases lack.

NornicDB automatically discovers and manages relationships in your data, weaving connections that let meaning emerge from your knowledge graph.

## Quick Start

### Docker (Recommended)

```bash
# Ready to go - includes embedding model
docker run -d --name nornicdb \
  -p 7474:7474 -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-bge:latest  # Apple Silicon
  # timothyswt/nornicdb-amd64-cuda-bge:latest  # NVIDIA GPU
```

### From Source

```bash
git clone https://github.com/orneryd/NornicDB.git
cd NornicDB
go build -o nornicdb ./cmd/nornicdb
./nornicdb serve
```

### Connect

Use any Neo4j driver — Python, JavaScript, Go, Java, .NET:

```python
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687")
with driver.session() as session:
    session.run("CREATE (n:Memory {content: 'Hello NornicDB'})")
```

## Features

### 🔌 Neo4j Compatible

Drop-in replacement for Neo4j. Your existing code works unchanged.

- **Bolt Protocol** — Use official Neo4j drivers
- **Cypher Queries** — Full query language support
- **Schema Management** — Constraints, indexes, vector indexes

### 🧠 Intelligent Memory

Memory that behaves like human cognition.

| Memory Tier    | Half-Life | Use Case               |
| -------------- | --------- | ---------------------- |
| **Episodic**   | 7 days    | Chat context, sessions |
| **Semantic**   | 69 days   | Facts, decisions       |
| **Procedural** | 693 days  | Skills, patterns       |

```cypher
// Find memories that are still strong
MATCH (m:Memory) WHERE m.decayScore > 0.5
RETURN m.title ORDER BY m.decayScore DESC
```

### 🔗 Auto-Relationships

NornicDB weaves connections automatically:

- **Embedding Similarity** — Related concepts link together
- **Co-access Patterns** — Frequently queried pairs connect
- **Temporal Proximity** — Same-session nodes associate
- **Transitive Inference** — A→B + B→C suggests A→C

### ⚡ Performance

**LDBC Social Network Benchmark** (M3 Max, 64GB):

| Query Type                    | NornicDB      | Neo4j       | Speedup |
| ----------------------------- | ------------- | ----------- | ------- |
| **Message content lookup**    | 6,389 ops/sec | 518 ops/sec | **12x** |
| **Recent messages (friends)** | 2,769 ops/sec | 108 ops/sec | **25x** |
| **Avg friends per city**      | 4,713 ops/sec | 91 ops/sec  | **52x** |
| **Tag co-occurrence**         | 2,076 ops/sec | 65 ops/sec  | **32x** |

**Northwind Benchmark** (M3 Max vs Neo4j, same hardware):

| Operation        | NornicDB      | Neo4j         | Speedup  |
| ---------------- | ------------- | ------------- | -------- |
| **Index lookup** | 7,623 ops/sec | 2,143 ops/sec | **3.6x** |
| **Count nodes**  | 5,253 ops/sec | 798 ops/sec   | **6.6x** |
| **Write: node**  | 5,578 ops/sec | 1,690 ops/sec | **3.3x** |
| **Write: edge**  | 6,626 ops/sec | 1,611 ops/sec | **4.1x** |

**Cross-Platform (CUDA on Windows i9-9900KF + RTX 2080 Ti):**

| Operation                 | Throughput    |
| ------------------------- | ------------- |
| **Orders by customer**    | 4,252 ops/sec |
| **Products out of stock** | 4,174 ops/sec |
| **Find category**         | 4,071 ops/sec |

**Additional advantages:**

- **Memory footprint**: 100-500 MB vs 1-4 GB for Neo4j
- **Cold start**: <1s vs 10-30s for Neo4j

> See [full benchmark results](docs/performance/benchmarks-vs-neo4j.md) for detailed comparisons.

### 🎯 Vector Search

Native semantic search with GPU acceleration.

```cypher
// Find similar memories
CALL db.index.vector.queryNodes(
  'memory_embeddings',
  10,
  $queryVector
) YIELD node, score
RETURN node.content, score
```

### 🤖 Heimdall AI Assistant

Built-in AI that understands your database.

```bash
# Enable Heimdall
NORNICDB_HEIMDALL_ENABLED=true ./nornicdb serve
```

**Natural Language Queries:**
- "Get the database status"
- "Show me system metrics"
- "Run health check"

**Plugin System:**
- Create custom actions the AI can execute
- Lifecycle hooks (PrePrompt, PreExecute, PostExecute)
- Database event monitoring for autonomous actions
- Inline notifications with proper ordering

See [Heimdall AI Assistant Guide](docs/user-guides/heimdall-ai-assistant.md) and [Plugin Development](docs/user-guides/heimdall-plugins.md).

### 🧩 APOC Functions

60+ built-in functions for text, math, collections, and more. Plus a plugin system for custom extensions.

```cypher
// Text processing
RETURN apoc.text.camelCase('hello world')  // "helloWorld"
RETURN apoc.text.slugify('Hello World!')   // "hello-world"

// Machine learning
RETURN apoc.ml.sigmoid(0)                  // 0.5
RETURN apoc.ml.cosineSimilarity([1,0], [0,1])  // 0.0

// Collections
RETURN apoc.coll.sum([1, 2, 3, 4, 5])      // 15
```

Drop custom `.so` plugins into `/app/plugins/` for automatic loading. See the [APOC Plugin Guide](docs/features/plugin-system.md).

## Docker Images

All images available at [Docker Hub](https://hub.docker.com/u/timothyswt).

### ARM64 (Apple Silicon)

| Image | Size | Description |
|-------|------|-------------|
| `timothyswt/nornicdb-arm64-metal-bge-heimdall` | 1.1 GB | **Full** - Embeddings + AI Assistant |
| `timothyswt/nornicdb-arm64-metal-bge` | 586 MB | **Standard** - With BGE-M3 embeddings |
| `timothyswt/nornicdb-arm64-metal` | 148 MB | **Minimal** - Core database, BYOM |
| `timothyswt/nornicdb-arm64-metal-headless` | 148 MB | **Headless** - API only, no UI |

### AMD64 (Linux/Intel)

| Image | Size | Description |
|-------|------|-------------|
| `timothyswt/nornicdb-amd64-cuda-bge` | ~4.5 GB | **GPU + Embeddings** - CUDA + BGE-M3 |
| `timothyswt/nornicdb-amd64-cuda` | ~3 GB | **GPU** - CUDA acceleration, BYOM |
| `timothyswt/nornicdb-amd64-cuda-headless` | ~2.9 GB | **GPU Headless** - API only |
| `timothyswt/nornicdb-amd64-cpu` | ~500 MB | **CPU** - No GPU required |
| `timothyswt/nornicdb-amd64-cpu-headless` | ~500 MB | **CPU Headless** - API only |

**BYOM** = Bring Your Own Model (mount at `/app/models`)

```bash
# With your own model
docker run -d -p 7474:7474 -p 7687:7687 \
  -v /path/to/models:/app/models \
  timothyswt/nornicdb-arm64-metal:latest

# Headless mode (API only, no web UI)
docker run -d -p 7474:7474 -p 7687:7687 \
  -v nornicdb-data:/data \
  timothyswt/nornicdb-arm64-metal-headless:latest
```

### Headless Mode

For embedded deployments, microservices, or API-only use cases, NornicDB supports headless mode which disables the web UI for a smaller binary and reduced attack surface.

**Runtime flag:**

```bash
nornicdb serve --headless
```

**Environment variable:**

```bash
NORNICDB_HEADLESS=true nornicdb serve
```

**Build without UI (smaller binary):**

```bash
# Native build
make build-headless

# Docker build
docker build --build-arg HEADLESS=true -f docker/Dockerfile.arm64-metal .
```

## Configuration

```yaml
# nornicdb.yaml
server:
  bolt_port: 7687
  http_port: 7474
  data_dir: ./data

embeddings:
  provider: local # or ollama, openai
  model: bge-m3
  dimensions: 1024

decay:
  enabled: true
  recalculate_interval: 1h

auto_links:
  enabled: true
  similarity_threshold: 0.82
```

## Use Cases

- **AI Agent Memory** — Persistent, queryable memory for LLM agents
- **Knowledge Graphs** — Auto-organizing knowledge bases
- **RAG Systems** — Vector + graph retrieval in one database
- **Session Context** — Decaying conversation history
- **Research Tools** — Connect papers, notes, and insights

## Documentation

| Guide                                                                      | Description                    |
| -------------------------------------------------------------------------- | ------------------------------ |
| [Getting Started](docs/getting-started/README.md)                          | Installation & quick start     |
| [API Reference](docs/api-reference/README.md)                              | Cypher functions & procedures  |
| [User Guides](docs/user-guides/README.md)                                  | Complete examples & patterns   |
| [Performance](docs/performance/README.md)                                  | Benchmarks vs Neo4j            |
| [Neo4j Migration](docs/neo4j-migration/README.md)                          | Compatibility & feature parity |
| [Architecture](docs/architecture/README.md)                                | System design & internals      |
| [Docker Guide](docker/README.md)                                           | Build & deployment             |
| [Development](docs/development/README.md)                                  | Contributing & development     |

## Comparison

| Feature            | Neo4j    | NornicDB   |
| ------------------ | -------- | ---------- |
| Protocol           | Bolt ✓   | Bolt ✓     |
| Query Language     | Cypher ✓ | Cypher ✓   |
| Memory Decay       | Manual   | Automatic  |
| Auto-Relationships | No       | Built-in   |
| Vector Search      | Plugin   | Native     |
| GPU Acceleration   | No       | Metal/CUDA |
| Embedded Mode      | No       | Yes        |
| License            | GPL      | MIT        |

## Building

### Native Binary

```bash
# Basic build
make build

# Headless (no UI)
make build-headless

# With local LLM support
make build-localllm
```

### Docker Images

```bash
# Download models for Heimdall builds (automatic if missing)
make download-models        # BGE-M3 + Qwen2.5-0.5B (~750MB)
make check-models          # Verify models present

# ARM64 (Apple Silicon)
make build-arm64-metal                  # Base (BYOM)
make build-arm64-metal-bge              # With BGE embeddings
make build-arm64-metal-bge-heimdall     # With BGE + Heimdall AI
make build-arm64-metal-headless         # Headless (no UI)

# AMD64 CUDA (NVIDIA GPU)
make build-amd64-cuda                   # Base (BYOM)
make build-amd64-cuda-bge               # With BGE embeddings
make build-amd64-cuda-bge-heimdall      # With BGE + Heimdall AI
make build-amd64-cuda-headless          # Headless (no UI)

# AMD64 CPU-only
make build-amd64-cpu                    # Minimal
make build-amd64-cpu-headless           # Minimal headless

# Build all variants for your architecture
make build-all

# Deploy to registry
make deploy-all             # Build + push all variants
```

### Cross-Compilation

```bash
# Build for other platforms from macOS
make cross-linux-amd64     # Linux x86_64
make cross-linux-arm64     # Linux ARM64
make cross-rpi             # Raspberry Pi 4/5
make cross-windows         # Windows (CPU-only)
make cross-all             # All platforms
```

## Roadmap

- [x] Neo4j Bolt protocol
- [x] Cypher query engine (52 functions)
- [x] Memory decay system
- [x] GPU acceleration (Metal, CUDA)
- [x] Vector & full-text search
- [ ] Auto-relationship engine
- [ ] HNSW vector index
- [ ] Clustering support

## License

MIT License — Originally part of the [Mimir](https://github.com/orneryd/mimir) project, now maintained as a standalone repository.

See [NOTICES.md](NOTICES.md) for third-party license information, including bundled AI models (BGE-M3, Qwen2.5) and dependencies.

---

<p align="center">
  <em>Weaving your data's destiny</em>
</p>
