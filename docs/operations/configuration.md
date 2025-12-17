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
  max_connections: 100
  connection_timeout: 30s
```

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
