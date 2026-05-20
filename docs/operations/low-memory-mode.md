# Low Memory Mode

**Run NornicDB in resource-constrained environments like Docker containers, Raspberry Pi, or VMs with limited RAM.**

## Overview

NornicDB's Low Memory Mode reduces RAM usage by 50-70% compared to default settings, enabling deployment on systems with as little as 512MB of available memory. This is essential for:

- **Docker containers** with default 2GB memory limits
- **Raspberry Pi** and other ARM SBCs
- **Cloud VMs** with limited RAM (t2.micro, etc.)
- **Development environments** running multiple services
- **Edge deployments** where resources are scarce

## Quick Start

### Environment Variable

```bash
# Enable low memory mode via environment variable
export NORNICDB_LOW_MEMORY=true
nornicdb serve
```

### Docker

```bash
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NORNICDB_LOW_MEMORY=true \
  timothyswt/nornicdb-arm64-metal:latest
```

### Docker Compose

```yaml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    environment:
      - NORNICDB_LOW_MEMORY=true
      - GOGC=100  # Recommended with low memory mode
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - nornicdb-data:/data
```

### Command Line Flag

```bash
nornicdb serve --low-memory
```

## Memory Usage Comparison

| Mode | BadgerDB RAM | Typical Total | Use Case |
|------|--------------|---------------|----------|
| **High Performance** (default) | ~1GB | 1.5-2GB | Servers with 8GB+ RAM |
| **Default** | ~150MB | 300-500MB | General purpose |
| **Low Memory** | ~50MB | 150-300MB | Resource-constrained |

### Breakdown by Component

| Component | High Performance | Default | Low Memory |
|-----------|-----------------|---------|------------|
| BadgerDB MemTable | 128MB × 5 | 64MB × 3 | 8MB × 1 |
| Value Log | 256MB | 128MB | 32MB |
| Block Cache | 256MB | 128MB | 8MB |
| Index Cache | 128MB | 64MB | 4MB |
| **Total Storage Layer** | **~1GB** | **~150MB** | **~50MB** |

## Configuration Options

### Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `NORNICDB_LOW_MEMORY` | bool | `false` | Enable low memory mode |
| `NORNICDB_BADGER_HIGH_PERFORMANCE` | bool | `true` | High performance mode (mutually exclusive with low memory) |
| `GOGC` | int | `100` | Go garbage collector target percentage |

### CLI Flags

```bash
nornicdb serve --help

Flags:
  --low-memory             Use minimal RAM (for resource constrained environments)
  --badger-high-performance Enable BadgerDB high performance mode (default true)
  --gc-percent             GC aggressiveness (100=default, lower=more aggressive)
```

### Priority Order

1. `--low-memory` flag overrides everything
2. `NORNICDB_LOW_MEMORY=true` environment variable
3. Default: Auto-detect based on available system memory (planned)

## Performance Impact

Low memory mode trades some performance for reduced RAM usage:

| Metric | High Performance | Low Memory | Impact |
|--------|-----------------|------------|--------|
| Write throughput | 50,000 ops/sec | 20,000 ops/sec | -60% |
| Read throughput | 100,000 ops/sec | 80,000 ops/sec | -20% |
| Query latency (p50) | 1ms | 2ms | +100% |
| Query latency (p99) | 5ms | 15ms | +200% |
| Startup time | 2s | 1s | -50% |

> **Note:** These numbers are approximate and vary by workload. For most use cases, low memory mode provides acceptable performance.

## Docker Memory Limits

Docker Desktop defaults to 2GB memory. With NornicDB's high-performance mode + embedding model (~1.2GB), this can cause OOM kills.

### Symptoms of OOM

```bash
# Container repeatedly restarts
docker ps
CONTAINER ID   STATUS                         PORTS
abc123         Restarting (137) 5 seconds ago

# Exit code 137 = SIGKILL (OOM)
docker logs nornicdb
```

### Solution: Enable Low Memory Mode

```yaml
# docker-compose.yml
services:
  nornicdb:
    image: timothyswt/nornicdb-arm64-metal:latest
    environment:
      - NORNICDB_LOW_MEMORY=true
      - GOGC=100
    # Optional: Set explicit memory limit
    deploy:
      resources:
        limits:
          memory: 1G
```

### Alternative: Increase Docker Memory

```bash
# Docker Desktop: Settings > Resources > Memory > 4GB+

# Or use explicit limit
docker run --memory=4g nornicdb ...
```

## Embedding Model Considerations

The embedded BGE-M3 model requires ~1.2GB RAM. In low memory mode:

1. **Use external embedding service** (Ollama, OpenAI) instead of local model
2. **Or increase container memory** to 2GB+

```yaml
# Option 1: External embeddings (recommended for low memory)
services:
  nornicdb:
    environment:
      - NORNICDB_LOW_MEMORY=true
      - NORNICDB_EMBEDDING_PROVIDER=ollama
      - NORNICDB_EMBEDDING_API_URL=http://ollama:11434
  
  ollama:
    image: ollama/ollama:latest
    # Ollama manages its own memory
```

```yaml
# Option 2: Local embeddings with more memory
services:
  nornicdb:
    environment:
      - NORNICDB_EMBEDDING_PROVIDER=local
    deploy:
      resources:
        limits:
          memory: 2.5G  # 1.2GB model + 1GB BadgerDB + headroom
```

## WAL Compaction

Low memory mode works well with automatic WAL compaction to prevent disk bloat:

```yaml
services:
  nornicdb:
    environment:
      - NORNICDB_LOW_MEMORY=true
      - NORNICDB_WAL_ENABLED=true
    volumes:
      - nornicdb-data:/data
      # WAL will auto-compact every 5 minutes
      # Snapshots stored in /data/snapshots
```

### Manual WAL Cleanup

If your WAL has already grown large:

```bash
# Stop NornicDB
docker stop nornicdb

# Remove bloated WAL (data is safe in BadgerDB)
rm -f /path/to/data/wal/wal.log

# Restart with low memory mode
docker start nornicdb
```

## Raspberry Pi Deployment

For Raspberry Pi 4 (2GB+ RAM):

```bash
# Use ARMv8 image with low memory mode
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NORNICDB_LOW_MEMORY=true \
  -e NORNICDB_EMBEDDING_PROVIDER=ollama \
  -e GOGC=50 \
  -v nornicdb-data:/data \
  --restart unless-stopped \
  timothyswt/nornicdb-arm64:latest
```

For Raspberry Pi 3 (1GB RAM):

```bash
# Minimal configuration - disable local embeddings
docker run -d \
  --name nornicdb \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NORNICDB_LOW_MEMORY=true \
  -e NORNICDB_EMBEDDING_ENABLED=false \
  -e GOGC=30 \
  -v nornicdb-data:/data \
  --restart unless-stopped \
  timothyswt/nornicdb-arm64:latest
```

## Programmatic Configuration

### Go API

```go
import "github.com/orneryd/nornicdb/pkg/nornicdb"

config := nornicdb.DefaultConfig()
config.LowMemoryMode = true  // Enable low memory mode

db, err := nornicdb.Open("/data/nornicdb", config)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### Storage Engine Options

```go
import "github.com/orneryd/nornicdb/pkg/storage"

// Create low-memory BadgerDB engine
opts := storage.BadgerOptions{
    DataDir:   "/data/nornicdb",
    LowMemory: true,
}
engine, err := storage.NewBadgerEngineWithOptions(opts)
```

## Monitoring Memory Usage

### Container Stats

```bash
# Real-time memory usage
docker stats nornicdb

# One-time check
docker stats nornicdb --no-stream
```

### NornicDB Metrics

```bash
# Memory metrics endpoint
curl http://localhost:7474/metrics | grep memory

# Key metrics:
# - go_memstats_alloc_bytes: Current allocations
# - go_memstats_sys_bytes: Total memory from OS
# - badger_lsm_size_bytes: LSM tree size
# - badger_vlog_size_bytes: Value log size
```

### Health Check

```bash
# Check if NornicDB is healthy
curl http://localhost:7474/health

# Response includes memory info
{
  "status": "healthy",
  "memory": {
    "allocated": "45MB",
    "system": "120MB",
    "gc_pause_ns": 1234567
  }
}
```

## Troubleshooting

### Still Running Out of Memory

1. **Check actual usage:**
   ```bash
   docker stats nornicdb --no-stream
   ```

2. **Lower GC target:**
   ```bash
   # More aggressive garbage collection
   docker run -e GOGC=50 ...
   ```

3. **Disable features:**
   ```bash
   # Disable local embeddings
   docker run -e NORNICDB_EMBEDDING_ENABLED=false ...
   ```

4. **Use swap (last resort):**
   ```bash
   # Add swap to container (not recommended for production)
   docker run --memory=1g --memory-swap=2g ...
   ```

### Performance Too Slow

Low memory mode prioritizes RAM over speed. If performance is unacceptable:

1. **Increase available memory** and disable low memory mode
2. **Use SSD storage** to compensate for reduced caching
3. **Optimize queries** to reduce memory pressure
4. **Consider horizontal scaling** with multiple instances

### WAL Growing Despite Low Memory Mode

WAL growth is managed separately from memory mode. Ensure auto-compaction is enabled:

```yaml
environment:
  - NORNICDB_WAL_ENABLED=true
  # WAL will auto-compact every 5 minutes
```

## Deferring search-index load with `warming=lazy`

`NORNICDB_LOW_MEMORY` controls Badger cache sizes and embedder loading. The orthogonal lever for **search index load** is the per-database warming setting:

- `NORNICDB_SEARCH_BM25_WARMING=lazy` — defer BM25 build until first inbound search query for that database.
- `NORNICDB_SEARCH_VECTOR_WARMING=lazy` — defer vector index load (HNSW + IVF + brute-force substrate) until first inbound search query.
- `NORNICDB_SEARCH_BM25_ENABLED=false` / `NORNICDB_SEARCH_VECTOR_ENABLED=false` — strongest available memory-pressure lever; the indexes are never built and node embeddings are never iterated into RAM.

These can be set globally (env / CLI / yaml) **and** overridden per-database via `PUT /admin/databases/{name}/config`. Per-DB overrides win over global defaults in both directions.

### Memory savings from `warming=lazy`

Boot RSS is reduced by approximately:

- **Vector**: `vectors_per_db × dim × 4 bytes` per lazy database (e.g. 100K vectors × 1024 dim × 4 bytes = ~400MB per DB never loaded at boot).
- **BM25**: typically smaller — depends on corpus size and tokenization; 10-50MB per medium DB.
- **HNSW graph**: roughly `M × 4 × vectors` bytes (M=16 default → ~64 bytes/vector connection state) on top of the vector data.

### First-query latency tradeoff

The first inbound search query against a lazy database **blocks synchronously while the build runs**. For a multi-million-vector database that's seconds; for a tens-of-thousands DB that's milliseconds. Concurrent first-readers all wait on the same build (only one trigger fires). Once warm, subsequent queries pay no extra cost — the index behaves identically to a `startup`-warmed index until the process restarts.

### Recommended yaml shape for multi-tenant idle DBs

```yaml
# Boot only the hot database eagerly; everything else warms on demand.
memory:
  search_bm25_warming:   startup
  search_vector_warming: lazy

databases:
  active_tenant_a: {}                                     # default startup
  active_tenant_b: {}                                     # default startup
  archive_2024:
    NORNICDB_SEARCH_BM25_WARMING:   "lazy"
    NORNICDB_SEARCH_VECTOR_WARMING: "lazy"
  audit_logs:
    NORNICDB_SEARCH_BM25_ENABLED:   "false"
    NORNICDB_SEARCH_VECTOR_ENABLED: "false"
```

### Caveat: health checks

Health/liveness probes must NOT target `/nornicdb/search` for any `warming=lazy` or `*_enabled=false` database. A probe against a lazy DB triggers the synchronous build on every probe; against a disabled DB it streams `503 search_disabled_for_database` responses that look like real failures in monitoring. Use `/nornicdb/health` (DB-agnostic) or `/admin/databases/{name}/config` (lookup-only) for liveness signals.

## Best Practices

### Do ✅

- Use `NORNICDB_LOW_MEMORY=true` for containers with < 4GB RAM
- Set `GOGC=100` (or lower) alongside low memory mode
- Use external embedding services (Ollama) instead of local models
- Monitor memory usage with `docker stats`
- Enable WAL auto-compaction

### Don't ❌

- Don't enable both `LOW_MEMORY` and `HIGH_PERFORMANCE`
- Don't load large embedding models (>500MB) in low memory mode
- Don't disable WAL compaction (causes disk bloat)
- Don't set memory limits below 512MB

## Related Documentation

- **[WAL Compaction](wal-compaction.md)** - Automatic disk space management
- **[Docker Deployment](../getting-started/docker-deployment.md)** - Complete Docker guide
- **[Raspberry Pi](../packaging/raspberry-pi.md)** - Edge deployment guide
- **[Performance Benchmarks](../performance/benchmarks-vs-neo4j.md)** - Performance comparison

---

**Memory issues?** → **[Troubleshooting Guide](troubleshooting.md)**  
**Need more performance?** → Consider upgrading to a system with 8GB+ RAM

