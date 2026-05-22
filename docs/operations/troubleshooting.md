# Troubleshooting

**Common issues and solutions.**

## Quick Diagnostics

```bash
# Check if server is running
curl http://localhost:7474/health

# Check logs
docker logs nornicdb
# or
journalctl -u nornicdb -n 100

# Check resources
docker stats nornicdb
# or
htop
```

## Connection Issues

### Cannot Connect to Server

**Symptoms:**
- Connection refused
- Timeout

**Solutions:**

1. **Check if server is running:**
   ```bash
   curl http://localhost:7474/health
   ```

2. **Check bind address:**
   ```bash
   # Docker requires 0.0.0.0
   NORNICDB_ADDRESS=0.0.0.0
   ```

3. **Check ports:**
   ```bash
   netstat -tlnp | grep 7474
   ```

4. **Check firewall:**
   ```bash
   sudo ufw status
   sudo firewall-cmd --list-ports
   ```

### Connection Reset

**Symptoms:**
- Intermittent disconnects
- "Connection reset by peer"

**Solutions:**

1. **Check rate limiting:**
   ```bash
   # View rate limit hits
   curl http://localhost:7474/metrics | grep rate_limit
   ```

2. **Increase limits:**
   ```yaml
   rate_limiting:
     per_minute: 200
     per_hour: 6000
   ```

## Authentication Issues

### 401 Unauthorized

**Symptoms:**
- All requests return 401

**Solutions:**

1. **Check token:**
   ```bash
   # Get new token
   curl -X POST http://localhost:7474/auth/token \
     -d "grant_type=password&username=admin&password=admin"
   ```

2. **Check auth is disabled (dev only):**
   ```bash
   NORNICDB_AUTH=none
   ```

3. **Check JWT secret:**
   ```bash
   # Must be at least 32 characters
   NORNICDB_AUTH_JWT_SECRET="your-32-character-secret-key-here"
   ```

### 403 Forbidden

**Symptoms:**
- Authenticated but access denied

**Solutions:**

1. **Check user role:**
   ```bash
   # User needs appropriate permissions (admin token required)
   curl -X PUT http://localhost:7474/auth/users/alice \
     -H "Authorization: Bearer $ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"roles": ["editor"]}'
   ```

2. **Check endpoint permissions:**
   - `/status` requires `read` permission
   - `/metrics` requires `read` permission
   - Admin endpoints require `admin` permission

## Performance Issues

### Slow Queries

**Symptoms:**
- High latency
- Timeouts

**Solutions:**

1. **Enable query caching:**
   ```bash
   nornicdb serve --query-cache-size=5000
   ```

2. **Check query complexity:**
   ```cypher
   // Bad: Unbounded traversal
   MATCH (n)-[*]->(m) RETURN n, m
   
   // Good: Limited depth
   MATCH (n)-[*1..3]->(m) RETURN n, m LIMIT 100
   ```

3. **Add indexes:**
   ```cypher
   CREATE INDEX FOR (n:Person) ON (n.email)
   ```

4. **Enable parallel execution:**
   ```bash
   nornicdb serve --parallel=true
   ```

### Maximum query/content size (MCP + embeddings)

If you are using the MCP tools (`store`, `discover`, etc.):

- **HTTP request size**: MCP limits request bodies via `MaxRequestSize` (default **10MB**).
  - Applies to `/mcp`, `/mcp/tools/call`, and `/mcp/initialize`.
- **Embedding input limits**: the effective max “query size” for vector search is bounded by the embedding model context.
  - For **local GGUF embeddings**, long queries are **chunked** into embedding-safe pieces and searched across all chunks (results are fused), so vector search can still work with paragraph-sized queries.
- **Stored content**: large content is chunked for embeddings (default **8192 tokens per chunk** with 50-token overlap), but very large payloads will increase background embedding work.

### High Memory Usage

**Symptoms:**
- OOM errors (exit code 137 in Docker)
- Container restarts repeatedly
- Slow performance

**Solutions:**

1. **Enable Low Memory Mode (recommended):**
   ```bash
   # Reduces BadgerDB RAM from ~1GB to ~50MB
   nornicdb serve --low-memory
   
   # Or via environment variable
   NORNICDB_LOW_MEMORY=true nornicdb serve
   ```
   
   See **[Low Memory Mode Guide](low-memory-mode.md)** for details.

2. **Increase GC frequency:**
   ```bash
   nornicdb serve --gc-percent=50
   ```

3. **Enable object pooling:**
   ```bash
   nornicdb serve --pool-enabled=true
   ```

4. **Docker: Increase memory limit:**
   ```yaml
   deploy:
     resources:
       limits:
         memory: 4G
   ```

5. **Docker: Check for WAL bloat:**
   ```bash
   # Large WAL files can cause OOM on startup
   du -sh /path/to/data/wal/
   
   # If >1GB, consider trimming it.
   #
   # NOTE: Deleting the WAL can lose the *very latest* mutations if the process
   # crashed after WAL append but before the change was applied to the main store.
   # Prefer auto-recovery (snapshot + WAL) when possible.
   rm /path/to/data/wal/wal.log
   ```

## Data Integrity / Recovery

### Server won't start until data is deleted (corruption after crash)

**Typical trigger:**
- Hard kills (OOM / `docker kill -9`), GPU/CGO segfaults, host power loss, or storage hiccups.

**What to do:**

1. **Do not delete your data directory.** Instead, back it up first.

2. **Use snapshot + WAL recovery (Neo4j-style “tx log recovery”)**

NornicDB maintains:
- **Snapshots** in `<dataDir>/snapshots/` (e.g., `snapshot-YYYYMMDD-HHMMSS.json`)
- **WAL** in `<dataDir>/wal/wal.log`

**Auto-recovery (recommended)**

Auto-recovery is **enabled by default**. You can:
- **Disable**: `NORNICDB_AUTO_RECOVER_ON_CORRUPTION=false`
- **Force an attempt** (even if the open error message doesn’t match the corruption heuristics): `NORNICDB_AUTO_RECOVER_ON_CORRUPTION=true`

```bash
NORNICDB_AUTO_RECOVER_ON_CORRUPTION=true
```

On startup, NornicDB will:
- Repair the WAL tail if the previous process crashed mid-write
- Load the latest snapshot (if present)
- Replay WAL entries after that snapshot (or replay WAL-only if no snapshots exist yet)
- **Rename** your original directory to `<dataDir>.corrupted-<timestamp>` (for forensics)
- Rebuild a fresh store and restore recovered nodes/edges

Startup errors that report `property key id ... not in dictionary` are treated
as corruption signals. That error means a persisted node or edge references a
tokenized property-key dictionary entry that is missing from the store, so the
safe recovery path is snapshot + WAL replay into a fresh store. Large recovery
restores are split into smaller Badger transactions automatically when the
backend rejects the full restore batch as too large.

3. **If recovery can’t run**

- Ensure the container has write access to the volume
- Ensure at least one recovery artifact exists:
  - snapshots in `<dataDir>/snapshots/`, and/or
  - WAL in `<dataDir>/wal/` (`wal.log` or sealed `segments/seg-*.wal`)
- Avoid running the DB data directory on unstable/union filesystem mounts (prefer a dedicated disk path)

### High CPU Usage

**Symptoms:**
- CPU at 100%
- Slow responses

**Solutions:**

1. **Limit parallel workers:**
   ```bash
   nornicdb serve --parallel-workers=2
   ```

2. **Check for expensive queries:**
   ```bash
   # Enable query logging
   nornicdb serve --log-queries
   ```

3. **Check embedding queue:**
   ```bash
   curl http://localhost:7474/status | jq .embeddings
   ```

## Data Issues

### Data Not Persisting

**Symptoms:**
- Data lost on restart

**Solutions:**

1. **Check volume mount:**
   ```bash
   docker inspect nornicdb | grep Mounts
   ```

2. **Check data directory:**
   ```bash
   ls -la /data
   ```

3. **Check disk space:**
   ```bash
   df -h
   ```

### Corrupted Data

**Symptoms:**
- Read errors
- Inconsistent results

**Solutions:**

1. **Restart with auto-recovery:**
   NornicDB sets `NORNICDB_AUTO_RECOVER_ON_CORRUPTION=true` by default; on an unclean shutdown the server replays WAL/snapshots and renames the corrupted directory to `<dataDir>.corrupted-<timestamp>`. See [Feature Flags](../features/feature-flags.md#auto-recover-on-corruption) for details.

2. **Restore from backup:**
   Stop the server, replace the data directory with a backup, and start the server. Or, for embedded deployments, call `db.Restore(ctx, path)` from your application code.

3. **Rebuild search indexes:**
   ```bash
   curl -X POST http://localhost:7474/nornicdb/search/rebuild \
     -H "Authorization: Bearer $TOKEN"
   ```

## Embedding Issues

### Embeddings Not Generating

**Symptoms:**
- Nodes without embeddings
- Search not working

**Solutions:**

1. **Check embedding service:**
   ```bash
   curl http://localhost:11434/api/embed \
     -d '{"model":"mxbai-embed-large","input":"test"}'
   ```

2. **Check configuration:**
   ```bash
   NORNICDB_EMBEDDING_API_URL=http://ollama:11434
   NORNICDB_EMBEDDING_MODEL=mxbai-embed-large
   NORNICDB_EMBEDDING_PROVIDER=ollama
   ```

3. **Check pending queue:**
   ```bash
   curl http://localhost:7474/status | jq .embeddings.pending
   ```

4. **Trigger regeneration:**
   ```bash
   curl -X POST http://localhost:7474/nornicdb/embed/trigger?regenerate=true
   ```

## Docker Issues

### Container Won't Start

**Solutions:**

1. **Check logs:**
   ```bash
   docker logs nornicdb
   ```

2. **Check image:**
   ```bash
   docker pull timothyswt/nornicdb-arm64-metal:latest
   ```

3. **Check resources:**
   ```bash
   docker system df
   ```

### Permission Denied

**Solutions:**

1. **Fix volume permissions:**
   ```bash
   docker run --rm -v nornicdb-data:/data busybox chown -R 1000:1000 /data
   ```

2. **Run as root (not recommended):**
   ```yaml
   security_opt:
     - no-new-privileges:false
   ```

## Getting Help

### Collect Diagnostics

```bash
# System info
uname -a
docker version
go version

# NornicDB info
curl http://localhost:7474/status

# Logs
docker logs nornicdb > nornicdb.log 2>&1
```

### Log Locations

| Deployment | Location |
|------------|----------|
| Docker | `docker logs nornicdb` |
| Systemd | `journalctl -u nornicdb` |
| Binary | `./nornicdb.log` |

## See Also

- **[Monitoring](monitoring.md)** - Health monitoring
- **[Deployment](deployment.md)** - Deployment guide
- **[Scaling](scaling.md)** - Performance tuning
