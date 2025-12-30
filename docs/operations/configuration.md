# Configuration Guide

This guide covers all configuration options for NornicDB, including the new async write settings and search similarity configuration.

## Configuration File

NornicDB uses a YAML configuration file (typically `nornicdb.yaml`) that can be specified via:

```bash
./nornicdb serve --config /path/to/config.yaml
```

Or via environment variables (see Environment Variables section).

## Core Configuration

### Database Settings

```yaml
# Database storage and basic settings
database:
  path: /data/nornicdb.db
  default_database: "nornic"  # Default database name (like Neo4j's "neo4j")
  max_connections: 100
  connection_timeout: 30s
```

**Multi-Database Support:**
- Default database name: `"nornic"` (configurable)
- System database: `"system"` (for metadata, not user-accessible)
- Multiple databases can be created via `CREATE DATABASE` command
- Each database is completely isolated (multi-tenancy)
- **Database Aliases**: Create alternate names for databases (`CREATE ALIAS`, `DROP ALIAS`, `SHOW ALIASES`)
- **Resource Limits**: Set per-database resource limits (`ALTER DATABASE SET LIMIT`, `SHOW LIMITS`)
- **Automatic migration**: Existing data is automatically migrated to the default database on first startup after upgrading
- Configuration precedence: CLI args > Env vars > Config file > Defaults

**Environment Variables:**
- `NORNICDB_DEFAULT_DATABASE` - Set default database name
- `NEO4J_dbms_default__database` - Neo4j-compatible env var (backwards compat)

### Server Settings

```yaml
server:
  bolt_enabled: true
  bolt_port: 7687
  bolt_address: "0.0.0.0"
  bolt_tls_enabled: false
  
  http_enabled: true
  http_port: 7474
  http_address: "0.0.0.0"
  http_tls_enabled: false
```

## Async Write Settings ⭐ New

The async write engine provides write-behind caching for improved throughput. Writes return immediately after updating the cache and are flushed to disk asynchronously.

### Configuration

```yaml
# === Async Write Settings ===
# These control the async write-behind cache for better throughput
async_writes:
  enabled: true                    # Enable async writes (default: true)
  flush_interval: 50ms            # How often to flush pending writes
  max_node_cache_size: 50000      # Max nodes to buffer before forcing flush
  max_edge_cache_size: 100000     # Max edges to buffer before forcing flush
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NORNICDB_ASYNC_WRITES_ENABLED` | `true` | Enable/disable async writes |
| `NORNICDB_ASYNC_FLUSH_INTERVAL` | `50ms` | Control flush frequency |
| `NORNICDB_ASYNC_MAX_NODE_CACHE_SIZE` | `50000` | Limit memory usage for node cache |
| `NORNICDB_ASYNC_MAX_EDGE_CACHE_SIZE` | `100000` | Limit memory usage for edge cache |

### Performance Tuning

**For High Throughput (bulk operations):**
```yaml
async_writes:
  enabled: true
  flush_interval: 200ms           # Larger = better throughput
  max_node_cache_size: 100000     # Increase for bulk inserts
  max_edge_cache_size: 200000
```

**For Strong Consistency:**
```yaml
async_writes:
  enabled: true
  flush_interval: 10ms            # Smaller = more consistent
  max_node_cache_size: 1000       # Smaller = less memory risk
  max_edge_cache_size: 2000
```

**For Maximum Durability:**
```yaml
async_writes:
  enabled: false                   # Disable async writes
```

### Memory Management

The cache size limits prevent unbounded memory growth during bulk operations:

- **Set to 0** for unlimited cache size (not recommended for production)
- **Monitor memory usage** during bulk operations
- **Adjust based on available RAM** and operation patterns

## Vector Search Configuration

### Embedding Settings

```yaml
embeddings:
  provider: local                  # or ollama, openai
  model: bge-m3
  dimensions: 1024
```

### Search Similarity ⭐ New

Configure minimum similarity thresholds for vector search:

```yaml
search:
  min_similarity: 0.5             # Default threshold (0.0-1.0)
```

### Programmatic Configuration

You can also configure similarity settings programmatically:

```go
// Set default minimum similarity
searchService.SetDefaultMinSimilarity(0.7)

// Get current default
current := searchService.GetDefaultMinSimilarity()

// Per-search override
results, err := searchService.Search(ctx, &SearchOptions{
    Query: "machine learning",
    MinSimilarity: &[]float64{0.8}[0], // Override for this search only
})
```

### Apple Intelligence Compatibility

For Apple Intelligence integration, use lower similarity thresholds:

```yaml
search:
  min_similarity: 0.3             # Lower threshold for AI assistants
```

## Memory Decay Configuration

```yaml
decay:
  enabled: true
  recalculate_interval: 1h
  decay_rate: 0.1                 # How quickly memories fade
```

## Auto-Link Configuration

```yaml
auto_links:
  enabled: true
  similarity_threshold: 0.82      # Threshold for automatic relationships
```

## Encryption Configuration

```yaml
encryption:
  enabled: false
  password: "your-secure-password"  # Use environment variable in production
```

## Environment Variables

All configuration options can be set via environment variables using the pattern `NORNICDB_<SECTION>_<KEY>`:

```bash
# Server configuration
export NORNICDB_SERVER_BOLT_PORT=7687
export NORNICDB_SERVER_HTTP_PORT=7474

# Async writes
export NORNICDB_ASYNC_WRITES_ENABLED=true
export NORNICDB_ASYNC_FLUSH_INTERVAL=50ms

# Search
export NORNICDB_SEARCH_MIN_SIMILARITY=0.5

# Embeddings
export NORNICDB_EMBEDDINGS_PROVIDER=local
export NORNICDB_EMBEDDINGS_MODEL=bge-m3
```

## Qdrant gRPC Endpoint (Qdrant SDK Compatibility)

NornicDB can expose a **Qdrant-compatible gRPC endpoint** so existing Qdrant SDKs can connect without modification.

User guide: `docs/user-guides/qdrant-grpc.md`

### Configuration (YAML)

```yaml
features:
  qdrant_grpc_enabled: true
  qdrant_grpc_listen_addr: ":6334"
  qdrant_grpc_max_vector_dim: 4096
  qdrant_grpc_max_batch_points: 1000
  qdrant_grpc_max_top_k: 1000
```

### Environment variables

| Variable | Default | Description |
|---|---:|---|
| `NORNICDB_QDRANT_GRPC_ENABLED` | `false` | Enable the Qdrant-compatible gRPC server |
| `NORNICDB_QDRANT_GRPC_LISTEN_ADDR` | `:6334` | gRPC listen address |
| `NORNICDB_QDRANT_GRPC_MAX_VECTOR_DIM` | `4096` | Maximum vector dimension |
| `NORNICDB_QDRANT_GRPC_MAX_BATCH_POINTS` | `1000` | Max points per upsert |
| `NORNICDB_QDRANT_GRPC_MAX_TOP_K` | `1000` | Max search results per query |

### Embedding ownership

- If `NORNICDB_EMBEDDING_ENABLED=true`, NornicDB owns embeddings; Qdrant vector mutation RPCs may be rejected to avoid conflicting sources of truth.
- If you want Qdrant clients to upsert/update/delete vectors directly, set `NORNICDB_EMBEDDING_ENABLED=false`.

## Configuration Validation

NornicDB validates configuration on startup and will:

1. **Reject invalid values** (e.g., negative cache sizes)
2. **Apply sensible defaults** for missing settings
3. **Log warnings** for potentially problematic combinations
4. **Fail fast** on critical configuration errors

## Performance Impact

### Async Write Settings

| Setting | Impact | Recommendation |
|---------|--------|----------------|
| `enabled: true` | 3-10x write throughput improvement | Enable for most workloads |
| `flush_interval: 50ms` | Balance of consistency vs throughput | Default works well |
| `cache_size: 50000` | Memory usage vs bulk performance | Adjust based on RAM |

### Search Similarity

| Threshold | Use Case | Impact |
|-----------|----------|--------|
| `0.7-1.0` | High precision | Fewer, more relevant results |
| `0.5-0.7` | Balanced | Good for most applications |
| `0.3-0.5` | High recall | More results, good for AI assistants |

## Troubleshooting

### Common Issues

**High memory usage:**
- Reduce `max_node_cache_size` and `max_edge_cache_size`
- Monitor during bulk operations
- Consider disabling async writes for memory-constrained environments

**Stale data reads:**
- Reduce `flush_interval` for more frequent writes
- Disable async writes if strong consistency is required
- Monitor WAL size and compaction

**Poor search results:**
- Adjust `min_similarity` based on your embedding model
- Consider model-specific thresholds (e.g., lower for Apple Intelligence)
- Test with your specific embedding provider

### Monitoring

Monitor these metrics to optimize configuration:

```bash
# Async write performance
curl http://localhost:7474/metrics | grep async

# Cache hit rates
curl http://localhost:7474/metrics | grep cache

# Search performance
curl http://localhost:7474/metrics | grep search
```

## Example Configurations

### Development Environment

```yaml
async_writes:
  enabled: true
  flush_interval: 10ms            # Fast feedback
  max_node_cache_size: 1000
  max_edge_cache_size: 2000

search:
  min_similarity: 0.3             # More results for testing
```

### Production High-Throughput

```yaml
async_writes:
  enabled: true
  flush_interval: 100ms           # Better throughput
  max_node_cache_size: 100000
  max_edge_cache_size: 200000

search:
  min_similarity: 0.7             # Higher precision
```

### Memory-Constrained

```yaml
async_writes:
  enabled: false                   # Disable to save memory
  # or small cache sizes:
  # max_node_cache_size: 500
  # max_edge_cache_size: 1000

search:
  min_similarity: 0.8             # Reduce result processing
```
