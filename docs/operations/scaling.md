# Scaling

**Scale NornicDB for high availability and performance.**

## Scaling Options

| Strategy      | Use Case             | Complexity |
| ------------- | -------------------- | ---------- |
| Vertical      | Quick wins           | Low        |
| Read Replicas | Read-heavy workloads | Medium     |
| Sharding      | Large datasets       | High       |

## Vertical Scaling

### Increase Resources

```yaml
# Docker Compose
services:
  nornicdb:
    deploy:
      resources:
        limits:
          memory: 8G
          cpus: "4"
```

### Memory Optimization

```bash
nornicdb serve \
  --memory-limit=4GB \
  --gc-percent=50 \
  --pool-enabled=true
```

### Query Optimization

```bash
nornicdb serve \
  --query-cache-size=5000 \
  --query-cache-ttl=600000 \
  --parallel=true \
  --parallel-workers=4
```

## Read Replicas

### Hot Standby Architecture

```
┌─────────────┐     ┌─────────────┐
│   Primary   │────▶│   Standby   │
│   (Write)   │     │   (Read)    │
└─────────────┘     └─────────────┘
        │                  │
        ▼                  ▼
   Write Requests    Read Requests
```

### Configuration

Hot standby is configured via environment variables (the replication subsystem reads `NORNICDB_CLUSTER_*`). On the primary:

```bash
export NORNICDB_CLUSTER_MODE=ha_standby
export NORNICDB_CLUSTER_HA_ROLE=primary
export NORNICDB_CLUSTER_BIND_ADDR=0.0.0.0:7000
```

On the standby:

```bash
export NORNICDB_CLUSTER_MODE=ha_standby
export NORNICDB_CLUSTER_HA_ROLE=standby
export NORNICDB_CLUSTER_HA_PEER_ADDR=primary.nornicdb.local:7000
```

See [Clustering Guide](../user-guides/clustering.md) for the full set of cluster environment variables, and [Environment Variables Reference](environment-variables.md) for the canonical inventory (search for `NORNICDB_CLUSTER_HA_*`).

### Load Balancing

```nginx
# nginx.conf
upstream nornicdb_read {
    server replica-1:7474;
    server replica-2:7474;
}

upstream nornicdb_write {
    server primary:7474;
}

server {
    location /db/nornic/tx/commit {
        # Route writes to primary
        proxy_pass http://nornicdb_write;
    }

    location /nornicdb/search {
        # Route reads to replicas
        proxy_pass http://nornicdb_read;
    }
}
```

## High Availability

### Raft Consensus

For automatic failover, configure via environment variables. On the bootstrap node:

```bash
export NORNICDB_CLUSTER_MODE=raft
export NORNICDB_CLUSTER_NODE_ID=node-1
export NORNICDB_CLUSTER_BIND_ADDR=0.0.0.0:7000
export NORNICDB_CLUSTER_RAFT_BOOTSTRAP=true
export NORNICDB_CLUSTER_RAFT_PEERS="node-2:node-2.nornicdb.local:7000,node-3:node-3.nornicdb.local:7000"
```

On peer nodes set the same `NORNICDB_CLUSTER_RAFT_PEERS` list (rotating their own entry out) and `NORNICDB_CLUSTER_RAFT_BOOTSTRAP=false`. See [Clustering Guide → Raft](../user-guides/clustering.md) for full setup.

### Kubernetes StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nornicdb
spec:
  serviceName: nornicdb
  replicas: 3
  selector:
    matchLabels:
      app: nornicdb
  template:
    metadata:
      labels:
        app: nornicdb
    spec:
      containers:
        - name: nornicdb
          image: timothyswt/nornicdb-arm64-metal:latest
          env:
            - name: NORNICDB_CLUSTER_MODE
              value: "raft"
            - name: NORNICDB_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          ports:
            - containerPort: 7474
            - containerPort: 7687
            - containerPort: 7000 # Raft port
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

## Caching

### Query Plan Cache

```bash
nornicdb serve --query-cache-size=5000 --query-cache-ttl=600000
```

The per-database query result cache is sized in entries (`--query-cache-size`, default `1000`; `0` disables) with a TTL in milliseconds (`--query-cache-ttl`, default `300000`).

### Embedding Cache

```bash
nornicdb serve --embedding-cache=10000
```

In-process LRU embedding cache for query auto-embedding. Default `10000` entries (~40MB at 1024-dim). Set `0` to disable. See [Feature Flags → Embedding Cache](../features/feature-flags.md#embedding-cache).

### Search Result Cache

Search results are cached automatically by the hybrid search service (LRU 1000 entries, 5-minute TTL, invalidated on writes). No configuration knob is exposed today; see [Hybrid Search → Caching](../user-guides/hybrid-search.md#caching).

NornicDB has no external cache integration (no Redis adapter ships). All caching is in-process.

## Performance Tuning

### Parallel Query Execution

```bash
nornicdb serve \
  --parallel=true \
  --parallel-workers=0 \  # Auto-detect CPUs
  --parallel-batch-size=1000
```

### Object Pooling

```bash
# Reduce memory allocations
nornicdb serve --pool-enabled=true
```

## Monitoring at Scale

### Key Metrics

- Request rate per node
- Replication lag
- Query latency percentiles
- Memory usage per node
- Disk I/O

### Prometheus Alerts

```yaml
groups:
  - name: nornicdb-scaling
    rules:
      - alert: HighLoad
        expr: nornicdb_http_requests_total > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High request rate - consider scaling"

      - alert: ReplicationLag
        expr: nornicdb_replication_lag_entries > 1000
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Replication lag (WAL entries behind primary) detected"
```

## Capacity Planning

### Sizing Guidelines

| Nodes | Edges | RAM  | Storage |
| ----- | ----- | ---- | ------- |
| 1M    | 5M    | 4GB  | 10GB    |
| 10M   | 50M   | 16GB | 100GB   |
| 100M  | 500M  | 64GB | 1TB     |

### Growth Projections

```bash
# Monitor growth
curl http://localhost:7474/metrics | grep nornicdb_nodes_total
curl http://localhost:7474/metrics | grep nornicdb_storage_bytes
```

## See Also

- **[Deployment](deployment.md)** - Deployment guide
- **[Monitoring](monitoring.md)** - Performance monitoring
- **[Clustering](../user-guides/clustering.md)** - HA clustering guide
- **[Cluster Security](cluster-security.md)** - Authentication for clusters
- **[Clustering Roadmap](../architecture/clustering-roadmap.md)** - Sharding via composite databases
