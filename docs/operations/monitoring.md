# Monitoring

**Monitor NornicDB health, performance, and security.**

NornicDB's observability surface has two operator-facing layers:

- Prometheus-compatible metrics exposed from the telemetry listener
- OpenTelemetry traces exported through the configured OTLP pipeline

The Prometheus registry is the canonical metrics contract. OTEL metric export reads the same underlying registry via the bridge defined in the observability ADR, so the stable names you should alert and dashboard on are the `nornicdb_*` series.

## Endpoints

| Endpoint                       | Auth Required | Description                                   |
| ------------------------------ | ------------- | --------------------------------------------- |
| `:7474/health`                 | No            | Legacy basic health check                     |
| `:7474/status`                 | Yes           | Detailed status                               |
| `:7474/metrics`                | Yes           | Legacy authenticated metrics endpoint         |
| `:9090/metrics`                | No            | Preferred telemetry listener metrics endpoint |
| `:9090/livez`                  | No            | Process liveness                              |
| `:9090/readyz`                 | No            | Readiness and warm-up phase                   |
| `:9090/version`                | No            | Plain-text build version                      |
| `127.0.0.1:9091/debug/pprof/*` | No            | Opt-in profiling listener                     |

The `:9090` telemetry listener is the preferred scrape target. The `:7474/metrics` data-plane endpoint remains available for migration compatibility, but the observability ADR treats the telemetry listener as the stable monitoring surface.

## OTEL And Prometheus Model

- Prometheus names are always `nornicdb_*`.
- Subsystems are bounded and closed, including `http`, `bolt`, `cypher`, `storage`, `mvcc`, `embed`, `search`, `replication`, `cache`, `auth`, and `knowledge_policy`.
- When OTLP push is enabled, the OTEL pipeline exports the same measurements rather than a second, separately-maintained metric tree.
- Tenant-scoped labels can be disabled with `observability.metrics.tenant_labels_enabled` when database labels would be too sensitive or too high-cardinality for a deployment.

For the architecture-level contract, see [Observability](../architecture/observability.md). For the generated catalog, see [metrics-reference.md](metrics-reference.md).

## Health Check

### Basic Health

```bash
curl http://localhost:7474/health
```

Response:

```json
{
  "status": "healthy"
}
```

### Telemetry Liveness And Readiness

```bash
curl http://localhost:9090/livez
curl http://localhost:9090/readyz
```

During warm-up, `readyz` returns progress JSON rather than just a bare `200 OK` with no body.

### Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /livez
    port: 9090
  initialDelaySeconds: 30
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /readyz
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Status Endpoint

### Detailed Status

```bash
curl http://localhost:7474/status \
  -H "Authorization: Bearer $TOKEN"
```

Response:

```json
{
  "status": "healthy",
  "server": {
    "version": "0.1.4",
    "uptime": "24h15m30s",
    "started_at": "2024-12-01T00:00:00Z"
  },
  "database": {
    "nodes": 150000,
    "edges": 450000,
    "data_size": "2.5GB"
  },
  "embeddings": {
    "enabled": true,
    "provider": "ollama",
    "model": "mxbai-embed-large",
    "pending": 0
  }
}
```

## Prometheus Metrics

### Enable Metrics

The preferred scrape endpoint is the telemetry listener on port `9090`.

```bash
curl http://localhost:9090/metrics
```

If you are still scraping the legacy data-plane endpoint during migration:

```bash
curl http://localhost:7474/metrics \
  -H "Authorization: Bearer $TOKEN"
```

### Telemetry Configuration

```yaml
observability:
  metrics:
    enabled: true
    port: 9090
    tenant_labels_enabled: true
  tracing:
    enabled: true
    endpoint: http://otel-collector:4318
    protocol: http/protobuf
  pprof:
    enabled: false
    listen: 127.0.0.1:9091
```

Environment overrides commonly used in operations:

- `NORNICDB_METRICS_ENABLED`
- `NORNICDB_TELEMETRY_PORT`
- `NORNICDB_PPROF_ENABLED`
- `NORNICDB_PPROF_LISTEN`
- standard OTEL exporter variables such as `OTEL_EXPORTER_OTLP_ENDPOINT`

### Example Metrics

```prometheus
# HTTP edge
nornicdb_http_requests_total{method="GET",path_template="/health",status_class="2xx"} 1234
nornicdb_http_request_duration_seconds_bucket{method="POST",path_template="/db/{database}/tx/commit",le="0.05"} 42

# Storage
nornicdb_nodes_total 150000
nornicdb_storage_edges_total 450000
nornicdb_storage_bytes 2684354560

# Cypher
nornicdb_queries_total{op_type="read"} 5678
nornicdb_cypher_active_transactions 3

# Embeddings
nornicdb_embed_processed_total{provider="local",result="success",mode="metal"} 10000
nornicdb_queue_depth 0

# Knowledge policy
nornicdb_knowledge_policy_scored_total{entity_kind="node",result="visible"} 4200
nornicdb_knowledge_policy_suppressions_total{entity_kind="node",reason="below_threshold"} 17
```

### Knowledge Policy Metrics

The knowledge-policy subsystem adds a dedicated `nornicdb_knowledge_policy_*` family. These metrics are the primary OTEL/Prometheus signals for decay, suppression, on-access mutation work, and deindex churn.

| Metric                                                    | Type      | Use                                                         |
| --------------------------------------------------------- | --------- | ----------------------------------------------------------- |
| `nornicdb_knowledge_policy_scored_total`                  | Counter   | Total visible, suppressed, and no-decay scoring evaluations |
| `nornicdb_knowledge_policy_decay_score`                   | Histogram | Score distribution, sampled 1 in 32                         |
| `nornicdb_knowledge_policy_suppressions_total`            | Counter   | Why suppressions happen                                     |
| `nornicdb_knowledge_policy_access_flush_batch_rows`       | Histogram | Batch pressure in the access flusher                        |
| `nornicdb_knowledge_policy_access_flush_duration_seconds` | Histogram | Cost of a flush cycle                                       |
| `nornicdb_knowledge_policy_access_flush_buffer_fullness`  | Gauge     | Backpressure tripwire for the flush buffer                  |
| `nornicdb_knowledge_policy_on_access_mutations_total`     | Counter   | Promotion-policy mutation workload                          |
| `nornicdb_knowledge_policy_deindex_enqueued_total`        | Counter   | Secondary-index cleanup workload after suppression          |
| `nornicdb_knowledge_policy_read_filter_dropped_total`     | Counter   | Read-path drops caused by visibility filtering              |
| `nornicdb_knowledge_policy_reconcile_total`               | Counter   | Schema or startup-driven reconcile activity                 |

Practical interpretation:

- rising `scored_total{result="suppressed"}` means the working set is shrinking
- a flat `decay_score` histogram usually means the half-life or score anchor is mis-tuned
- sustained `access_flush_buffer_fullness > 0.9` means the flusher is falling behind
- rising `deindex_enqueued_total` should correlate with intentionally tighter suppression behavior

For the full per-instrument explanation, enum values, dashboard slices, and runbook, see [../observability/knowledge-policy-metrics.md](../observability/knowledge-policy-metrics.md).

### Knowledge Policy Spans

The knowledge-policy subsystem also emits OTEL tracing spans around its expensive lifecycle work:

- `nornicdb.knowledge_policy.flush`
- `nornicdb.knowledge_policy.reconcile`

Those spans are the right place to investigate long-running flush cycles, schema-change churn, or suppression-recheck work that is not obvious from counters alone.

### Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: "nornicdb"
    static_configs:
      - targets: ["localhost:9090"]
    metrics_path: "/metrics"
```

In Kubernetes, prefer a `ServiceMonitor` or `PodMonitor` bound to the telemetry port rather than scraping the data plane.

## Grafana Dashboard

### Example Dashboard JSON

```json
{
  "title": "NornicDB",
  "panels": [
    {
      "title": "Request Rate",
      "type": "graph",
      "targets": [
        {
          "expr": "rate(nornicdb_http_requests_total[5m])"
        }
      ]
    },
    {
      "title": "Knowledge Policy Suppressions",
      "type": "graph",
      "targets": [
        {
          "expr": "sum by (reason) (rate(nornicdb_knowledge_policy_suppressions_total[5m]))"
        }
      ]
    },
    {
      "title": "Knowledge Policy Flush Duration p99",
      "type": "graph",
      "targets": [
        {
          "expr": "histogram_quantile(0.99, sum by (le) (rate(nornicdb_knowledge_policy_access_flush_duration_seconds_bucket[5m])))"
        }
      ]
    }
  ]
}
```

## Alerting

### Prometheus Alerts

```yaml
# alerts.yml
groups:
  - name: nornicdb
    rules:
      - alert: NornicDBDown
        expr: up{job="nornicdb"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "NornicDB is down"

      - alert: HighErrorRate
        expr: rate(nornicdb_http_requests_total{status_class="5xx"}[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate detected"

      - alert: KnowledgePolicyFlushBackpressure
        expr: nornicdb_knowledge_policy_access_flush_buffer_fullness > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Knowledge-policy access flusher is near saturation"

      - alert: KnowledgePolicySuppressionsSpike
        expr: rate(nornicdb_knowledge_policy_suppressions_total[5m]) > 5
        for: 10m
        labels:
          severity: info
        annotations:
          summary: "Knowledge-policy suppressions are rising"
```

## Logging

### Log Levels

```bash
export NORNICDB_LOG_LEVEL=info  # debug, info, warn, error
```

### Log Format

```json
{
  "time": "2024-12-01T10:30:00.123Z",
  "level": "info",
  "msg": "Query executed",
  "query_type": "cypher",
  "duration_ms": 23,
  "rows": 100
}
```

### Log Aggregation

```yaml
logging:
  driver: "fluentd"
  options:
    fluentd-address: "localhost:24224"
    tag: "nornicdb"
```

## Performance Monitoring

### pprof For Goroutines And Locking

The pprof listener is separate from the telemetry listener and is disabled by default.

Enable it only when you are actively diagnosing runtime behavior:

```bash
export NORNICDB_PPROF_ENABLED=true
export NORNICDB_PPROF_LISTEN=127.0.0.1:9091
```

When pprof is enabled, NornicDB also enables:

- mutex profiling with `runtime.SetMutexProfileFraction(1)`
- block profiling with `runtime.SetBlockProfileRate(1000000)`

That makes these endpoints useful immediately:

- `/debug/pprof/goroutine`
- `/debug/pprof/mutex`
- `/debug/pprof/block`
- `/debug/pprof/heap`
- `/debug/pprof/profile`

Typical commands:

```bash
# CPU profile
go tool pprof http://127.0.0.1:9091/debug/pprof/profile?seconds=30

# Goroutine dump
go tool pprof http://127.0.0.1:9091/debug/pprof/goroutine

# Mutex contention
go tool pprof http://127.0.0.1:9091/debug/pprof/mutex

# Blocking profile
go tool pprof http://127.0.0.1:9091/debug/pprof/block
```

Use pprof together with the OTEL/Prometheus metrics:

- if `nornicdb_knowledge_policy_access_flush_duration_seconds` rises, check `/debug/pprof/mutex` and `/debug/pprof/block`
- if `nornicdb_knowledge_policy_access_flush_buffer_fullness` stays high, inspect goroutine growth and blocked stacks
- if the system is spending time in repeated reconcile or suppression work, correlate `reconcile_total` and suppression counters with goroutine and mutex profiles

### Query Performance

```bash
nornicdb serve --log-queries
```

### Search Timing Diagnostics

Enable detailed search timing logs when tuning search latency:

```bash
export NORNICDB_SEARCH_LOG_TIMINGS=true
export NORNICDB_SEARCH_DIAG_TIMINGS=true
```

You will see two complementary log lines per search:

- `⏱️ Search timing:` stage-level search-service timings (`vector_ms`, `bm25_ms`, `fusion_ms`, candidate counts, fallback)
- `🔎 Search timing db=...:` request-path timings (`embed_total`, `search_total`, `embed_calls`, chunk info)

Field reference (Apple M3 Max, 64GB RAM, Feb 2026):

- **Embedding-query path (best collected):**
  - Sequential varied queries: p50 11.28ms, p95 25.84ms
  - Concurrent (8 workers): p50 76.36ms, p95 87.41ms
  - Typical diagnostic pattern: `embed_total` dominates request time.
- **Fulltext-only path (best collected):**
  - Sequential varied queries: p50 0.57ms, p95 2.77ms
  - Diagnostic pattern: `embed_calls=0`, `embed_total=0s`, handler internal timing in tens of microseconds.

### Slow Query Log

```json
{
  "level": "warn",
  "msg": "Slow query",
  "query": "MATCH (n)-[r*1..5]->(m) RETURN n, r, m",
  "duration_ms": 1500,
  "threshold_ms": 1000
}
```

## Security Monitoring

### Failed Login Alerts

Failed logins are logged and can trigger alerts:

```json
{
  "level": "warn",
  "msg": "Login failed",
  "username": "admin",
  "ip": "192.168.1.100",
  "reason": "invalid_password"
}
```

### Audit Log Monitoring

```bash
tail -f /var/log/nornicdb/audit.log | jq 'select(.type == "LOGIN_FAILED")'
```

## Health Check Script

```bash
#!/bin/bash

HEALTH=$(curl -s http://localhost:7474/health)
STATUS=$(echo "$HEALTH" | jq -r '.status')

if [ "$STATUS" != "healthy" ]; then
  echo "NornicDB unhealthy: $HEALTH"
  exit 1
fi

echo "NornicDB healthy"
exit 0
```

## See Also

- **[Deployment](deployment.md)** - Deployment guide
- **[Troubleshooting](troubleshooting.md)** - Common issues
- **[Metrics Reference](metrics-reference.md)** - Auto-generated metric catalog
- **[Knowledge Policy Metrics](../observability/knowledge-policy-metrics.md)** - Detailed knowledge-policy metric reference
- **[Observability](../architecture/observability.md)** - Telemetry architecture and contract
- **[pprof Quick Guide](../performance/pprof-quick-guide.md)** - Interactive profiling workflow
- **[Audit Logging](../compliance/audit-logging.md)** - Security monitoring
