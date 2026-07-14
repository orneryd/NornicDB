# Architecture

**System design and internal architecture of NornicDB.**

## 📚 Documentation

- **[System Design](system-design.md)** - High-level architecture overview
- **[MVCC Lifecycle and Background Work](mvcc-lifecycle-background-work.md)** - Debounced mutation work, lifecycle scheduling, and query-protection behavior
- **[Embedding Search](embedding-search.md)** - Embedding storage model and search paths
- **[Knowledge-Policy Scoring Pipeline](knowledge-policy-scoring-pipeline.md)** - Pre-visibility scoring, decay, and promotion resolution
- **[Knowledge-Policy Visibility Layer and Deindex](knowledge-policy-visibility-layer.md)** - Reveal scopes, suppression enforcement, tombstones, and retention hand-off
- **[Graph-RAG: NornicDB vs Typical](graph-rag-nornicdb-comparison.md)** - In-memory vs distributed Graph-RAG and latency comparison
- **[Replication](replication.md)** - Clustering and replication internals
- **[Clustering Roadmap](clustering-roadmap.md)** - Future sharding and scaling plans
- **[Plugin System](plugin-system.md)** - Extensibility architecture
- **[Norns Mythology](norns-mythology.md)** - Project naming and philosophy

## 🏗️ Core Components

### Storage Layer

- Badger KV store for persistence
- In-memory engine for testing
- Property graph model
- ACID transactions

### Query Engine

- Cypher parser and planner
- Query optimizer
- Execution engine
- Result streaming

### Index System

- HNSW vector index
- B-tree property index
- Full-text BM25 index
- Automatic index selection

### Replication

- Hot Standby (2-node HA)
- Raft Consensus (3+ node strong consistency)
- Multi-Region (geographic distribution with async replication)
- WAL streaming and automatic failover
- Chaos-tested for extreme latency scenarios

### GPU Acceleration

- Multi-backend support (Metal, CUDA, OpenCL)
- Automatic CPU fallback
- Memory-optimized operations
- Batch processing

## 📖 Learn More

- **[System Design](system-design.md)** - Complete architecture
- **[MVCC Lifecycle and Background Work](mvcc-lifecycle-background-work.md)** - Maintenance behavior and background work control
- **[Replication](replication.md)** - Clustering internals
- **[Clustering Guide](../user-guides/clustering.md)** - User documentation
- **[Performance](../performance/README.md)** - Benchmarks and optimization
- **[Development](../development/README.md)** - Contributing guide

---

**Dive deeper** → **[System Design](system-design.md)**
