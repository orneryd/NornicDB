# ADR-0001: Best-in-Class Observability for NornicDB

| Field         | Value                                                                 |
|---------------|-----------------------------------------------------------------------|
| Status        | **Accepted**                                                          |
| Date          | 2026-04-29                                                            |
| Sign-off date | 2026-04-30                                                            |
| Authors       | Allen Sanabria                                                        |
| Supersedes    | (none — establishes the ADR convention for this repo)                 |
| Related       | `docs/operations/monitoring.md`, `docs/architecture/replication.md`, `pkg/audit/audit.go` |
| Target branch | `otel`                                                                |

> **About this ADR.** This document is the first formal Architecture Decision
> Record in NornicDB. ADRs in this repo live under
> `docs/architecture/adr/NNNN-<slug>.md`, are numbered monotonically, and use
> the `Context → Decision → Consequences` form (MADR‑lite). The ADR is the
> *contract*; implementation lives behind it and may evolve, but the named
> guarantees here may not be silently broken.

---

## 1. Context

NornicDB is a Neo4j-compatible, Cypher/Bolt graph database with a hybrid
vector + BM25 search stack, built-in embedding generation, MVCC lifecycle
management, multi-database tenancy, and three replication modes
(`ha_standby`, `raft`, `multi_region`). It is delivered as a single Go binary,
shipped as a container, and is increasingly run on Kubernetes by customers.

A complete audit of the current observability surface was performed against
the `otel` branch:

| Surface            | What exists today                                                                                              | Gap                                                                                                                                  |
|--------------------|----------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| HTTP `/health`     | Returns `{"status":"healthy"}` unconditionally (`pkg/server/server_public.go:44`).                             | Liveness only. No readiness signal that distinguishes "process up" from "search index warm".                                         |
| HTTP `/status`     | Auth‑gated; rich JSON with uptime, request counts, node/edge counts, embedding queue, search‑warming.          | Hand‑built JSON, not machine‑contract; no schema.                                                                                    |
| HTTP `/metrics`    | Auth‑gated; **hand-rolled Prometheus text** with ~10 gauges/counters (`pkg/server/server_public.go:198-268`).  | No histograms, no labels (no `database`, `method`, `op`, `result`), no exemplars, **`PermRead` auth breaks Prometheus operator scraping**. |
| HTTP middleware    | `metricsMiddleware` increments in-flight and total request counters (`pkg/server/server_middleware.go:344`).   | No latency histogram, no per-route/per-status labels.                                                                                |
| Logging            | `log.Printf`/`fmt.Println` (~165 call sites in `pkg/server`, `pkg/cypher`, `pkg/storage`).                     | `LoggingConfig{Level,Format,Output}` exists in `pkg/config/config.go:585` but `Format=json` is *unhonored* — there is no `slog`, `zerolog`, or `zap` anywhere. |
| Bolt protocol      | `pkg/bolt/server.go` has 16 `log.Printf` sites; no metrics; no spans.                                          | Bolt is fully opaque to operators today.                                                                                             |
| Tracing            | `go.opentelemetry.io/*` packages are present in `go.sum` only as `// indirect`.                                | No SDK initialization, no exporter, no spans, no propagation across replication.                                                     |
| Replication        | Internal counters in `pkg/replication/replicator.go` (term, commit index) but not exposed.                     | No leader/lag/quorum signal at `/metrics`.                                                                                           |
| Audit              | `pkg/audit/audit.go` writes immutable JSON for GDPR/HIPAA/SOC2.                                                | **Already correct**; this ADR explicitly does *not* fold compliance audit into operational telemetry.                                |
| K8s integration    | Probes documented, no `ServiceMonitor`/`PodMonitor` ships with the chart.                                      | Customers must hand-roll, and the `PermRead` gate currently makes that fail closed.                                                  |

### 1.1 Forces

The decision must reconcile competing forces:

1. **Customer-facing.** NornicDB is a *product*. Metric names are a public API
   that downstream Grafana dashboards, Alertmanager rules, Datadog monitors,
   and SRE runbooks will hard-code. Renaming a metric is a breaking change.
   Treat the metric catalog like SQL — versioned, deprecated with overlap,
   never removed without a major release.
2. **Self-hosted on K8s.** The dominant deployment is `kube-prometheus-stack`
   + `ServiceMonitor`. Anything that breaks the no-auth scrape contract on
   port `9090` breaks every prospect's first day.
3. **Cloud-neutral.** Customers who do not run Prometheus (Datadog, New Relic,
   Honeycomb, Grafana Cloud) need an OTLP push path with the same data.
4. **Defense in depth.** Metric scraping must not become a tenant naming
   oracle (label cardinality leak) or an unauthenticated profile path.
5. **AGENTS.md golden rules.** No >10% memory regression, 90% coverage,
   files under 2,500 lines, Neo4j wire compatibility preserved.
6. **Two protocols.** HTTP/REST and Bolt have disjoint code paths and must
   *both* be instrumented to the same standard.
7. **Replication-aware.** Tracing and metrics must survive the leader→follower
   wire, otherwise distributed-system bugs become unobservable.

### 1.2 Out of scope

* The compliance audit log (`pkg/audit`) is *not* changed by this ADR. It
  remains a distinct, append-only, signed stream.
* Continuous profiling (Pyroscope/Parca) is **deferred** to a follow-up ADR;
  pprof endpoints are addressed only as a gated debug surface.

---

## 2. Decision

NornicDB will adopt a three-pillar observability stack — **metrics, traces,
structured logs** — wired through a new package `pkg/observability` and
exposed on a dedicated network port suitable for a Kubernetes
`ServiceMonitor`.

### 2.1 Architecture overview

```
                     ┌──────────────────────── pkg/observability ────────────────────────┐
                     │                                                                    │
  HTTP edge ───►  otelhttp ──► slog (JSON) ──► prom registry  ──► /metrics on :9090       │
  Bolt edge ───►  bolt span ──► slog (JSON) ──► prom registry                             │
  Cypher    ───►  exec span ──► slog (JSON) ──► prom histograms (planner/exec/rows)       │
  Storage   ───►  badger span ─► slog (JSON) ──► prom histograms (read/write/compact)     │
  Replication ─►  raft span  ──► slog (JSON) ──► prom gauges (lag, term, role)            │
                     │                                                                    │
                     │      ┌───────────────────────────────────────────────────┐         │
                     │      │ OTel SDK (single tracer + meter provider)         │         │
                     │      │   ├── OTLP/gRPC exporter (traces + metrics push)  │         │
                     │      │   ├── Prom exporter (`/metrics` text)             │         │
                     │      │   └── stdout exporter (DEV only)                  │         │
                     │      └───────────────────────────────────────────────────┘         │
                     └────────────────────────────────────────────────────────────────────┘

                Service ports:
                  :7474  HTTP API (auth required, customer data plane)
                  :7687  Bolt    (auth required, customer data plane)
                  :9090  /metrics, /livez, /readyz   (cluster-internal, no auth)
                  :9091  /debug/pprof                (opt-in, NORNICDB_PPROF_ENABLED=true)
```

Three signals, **one** instrumentation surface (`pkg/observability`), **two**
egress paths (Prometheus pull + OTLP push) so customers pick whichever fits
their stack without us writing the code twice.

### 2.2 Metrics — `client_golang` is canonical, OTel is an additional egress

We will instrument metrics with **`prometheus/client_golang`** as the
canonical API, *not* the OTel meter API. Reasoning:

* `/metrics` is a published customer contract. `client_golang` produces
  Prometheus exposition format with native counter naming (`_total`),
  histogram bucket layout, and `# HELP`/`# TYPE` semantics that downstream
  dashboards depend on. The OTel→Prom bridge has documented quirks (unit
  suffix translation, dotted-attribute renaming) that would silently rename
  metrics for customers who are already scraping us today.
* Where customers want OTLP push (Datadog/New Relic/Honeycomb), an OTel
  metric reader will be wired *in parallel* against the same registry via
  the `otelprom` bridge. Internal call sites instrument once; both pipelines
  read the same numbers.

**Naming and labeling rules** (binding, customer-facing):

* Namespace prefix is always `nornicdb_`.
* Subsystem names map to internal packages: `http_`, `bolt_`, `cypher_`,
  `storage_`, `mvcc_`, `embed_`, `search_`, `replication_`, `cache_`,
  `apoc_`, `plugin_`, `process_`.
* Counters end in `_total`. Histograms end in the unit they measure
  (`_seconds`, `_bytes`, `_rows`).
* Allowed labels:
  - `database` (tenant database name; bounded by `dbManager.ListDatabases()`)
  - `method`, `path_template`, `status_class` (`2xx|4xx|5xx`) for HTTP
  - `op` for Bolt message type (`RUN`, `PULL`, `BEGIN`, `COMMIT`, `ROLLBACK`)
  - `result` (`success|error|conflict|timeout`)
  - `cache` (`plan|node|edge|query|embedding`)
  - `mode` for search (`vector|bm25|hybrid`)
  - `provider`, `model` for embeddings (only from configured allow-list)
  - `role` (`leader|follower|standby`) for replication
  - `peer` (cluster node ID, bounded by config) for replication links
* **Forbidden labels** (cardinality bombs / PII): full HTTP path, raw
  Cypher query, user ID, email, IP address, node ID/UUID, embedding text,
  span/trace IDs.
* Histograms use NornicDB's standard latency buckets:
  `[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]` seconds.
  Storage/byte histograms use the standard `prometheus.ExponentialBuckets(1024, 4, 10)`.

**Two-path egress.** `prometheus/client_golang` is canonical for the `:9090/metrics` text exposition; an OTel meter reader runs in parallel against the same numbers via the `otelprom` bridge for OTLP push customers. The bridge is configured with `WithoutUnits()` to suppress its auto-suffix translation (`_milliseconds_total`, `_bytes`), and bridge-emitted metrics live under the `nornicdb_otel_*` namespace to prevent silent collision with hand-instrumented `nornicdb_*` series. Resource attributes do not become labels through the bridge — they appear only as the `target_info` metric, which the bundled `ServiceMonitor` drops via `metricRelabelings`.

**Tenant label kill switch.** A `metrics.tenant_labels_enabled` config flag (default `true` on K8s detect via `KUBERNETES_SERVICE_HOST` env + ServiceAccount token; `false` otherwise) controls whether the `database` label appears on **all tenant-scoped subsystems** — `nornicdb_http_*`, `nornicdb_storage_*`, `nornicdb_cypher_*`, `nornicdb_search_*`, and `nornicdb_mvcc_*` series. Operators can override with `metrics.tenant_labels_enabled=false` even on K8s. The resolved value is logged at startup.

#### 2.2.1 Reconciled amendments

The following research-surfaced amendments were folded into the §2.2 prose above:

- **A1 — OTel→Prom bridge configuration:** the `otelprom` exporter is configured with `WithoutUnits()` to suppress the auto-suffix translation (`_milliseconds_total`, `_bytes`); bridge-emitted metrics live under the `nornicdb_otel_*` namespace to prevent silent collision with hand-instrumented `nornicdb_*` series; the `ServiceMonitor` includes `metricRelabelings` to drop the per-pod `target_info` series.

### 2.3 The NornicDB-centric metric catalog (v1)

This catalog is the customer contract. Modeled on `pg_stat_*` (Postgres) and
`db.*` (Neo4j JMX beans) but renamed to NornicDB semantics. Each line is a
metric NornicDB v1 *guarantees* to expose; additions are non-breaking,
removals require a major version.

#### HTTP edge

```
nornicdb_http_requests_total{method,path_template,status_class,database}      counter
nornicdb_http_request_duration_seconds{method,path_template,database}         histogram
nornicdb_http_in_flight_requests                                              gauge
nornicdb_http_request_body_bytes{method,path_template}                        histogram
nornicdb_http_response_body_bytes{method,path_template}                       histogram
```

#### Bolt protocol

```
nornicdb_bolt_connections_active                                              gauge
nornicdb_bolt_connections_total{result}                                       counter
nornicdb_bolt_session_duration_seconds                                        histogram
nornicdb_bolt_messages_total{op,result}                                       counter
nornicdb_bolt_message_duration_seconds{op}                                    histogram
nornicdb_bolt_packstream_decode_errors_total{reason}                          counter
```

#### Cypher (the "pg_stat_statements" of NornicDB)

```
nornicdb_cypher_queries_total{database,op_type,result}                        counter
nornicdb_cypher_query_duration_seconds{database,op_type}                      histogram
nornicdb_cypher_planner_duration_seconds{database}                            histogram
nornicdb_cypher_planner_cache_hits_total{database}                            counter
nornicdb_cypher_planner_cache_misses_total{database}                          counter
nornicdb_cypher_planner_cache_size                                            gauge
nornicdb_cypher_rows_returned{database,op_type}                               histogram
nornicdb_cypher_active_transactions{database}                                 gauge
nornicdb_cypher_transaction_conflicts_total{database,kind}                    counter
nornicdb_cypher_slow_queries_total                                            counter
nornicdb_cypher_slow_query_threshold_seconds                                  gauge
```

`op_type` is one of `read|write|schema|admin|fabric` — derived in the planner,
**not** taken from the query text. This bounds cardinality.

#### Storage / Badger

```
nornicdb_storage_nodes_total{database}                                        gauge
nornicdb_storage_edges_total{database}                                        gauge
nornicdb_storage_bytes{database,kind=nodes|edges|index|wal|search}            gauge
nornicdb_storage_op_duration_seconds{database,op=get|put|delete|scan}         histogram
nornicdb_storage_compactions_total{level,result}                              counter
nornicdb_storage_compaction_duration_seconds{level}                           histogram
nornicdb_storage_wal_lag_bytes{database}                                      gauge
nornicdb_storage_index_rebuild_total{database,index,result}                   counter
```

#### MVCC lifecycle

The existing `MVCCLifecycleEngine` already exposes a pressure band; this
becomes a first-class metric (mirroring `mvcc.bytes_pinned_by_oldest_reader`
in `pkg/server/server_helpers.go:367`):

```
nornicdb_mvcc_pressure_band{database,band=normal|warn|high|critical}          gauge
nornicdb_mvcc_pinned_bytes{database}                                          gauge
nornicdb_mvcc_oldest_reader_age_seconds{database}                             gauge
nornicdb_mvcc_active_readers{database}                                        gauge
```

#### Embeddings

```
nornicdb_embed_queue_depth                                                    gauge
nornicdb_embed_processed_total{provider,model,result}                         counter
nornicdb_embed_duration_seconds{provider,model}                               histogram
nornicdb_embed_cache_hits_total                                               counter
nornicdb_embed_cache_misses_total                                             counter
nornicdb_embed_worker_running                                                 gauge
```

#### Search (vector/BM25/hybrid)

```
nornicdb_search_requests_total{database,mode,result}                          counter
nornicdb_search_duration_seconds{database,mode,stage=embed|index|fuse}        histogram
nornicdb_search_candidates{database,mode}                                     histogram
nornicdb_search_index_size_bytes{database,kind=hnsw|bm25}                     gauge
```

#### Replication

```
nornicdb_replication_role{role}                                               gauge       (1 for current role, 0 otherwise; per-replica disambiguation via Prometheus scrape-target `instance` label)
nornicdb_replication_term                                                     gauge
nornicdb_replication_commit_index                                             gauge
nornicdb_replication_apply_index                                              gauge
nornicdb_replication_lag_bytes{peer}                                          gauge
nornicdb_replication_lag_entries{peer}                                        gauge
nornicdb_replication_apply_duration_seconds                                   histogram
nornicdb_replication_rtt_seconds{peer}                                        histogram
nornicdb_replication_leader_changes_total                                     counter
nornicdb_replication_last_contact_seconds{peer}                              gauge
```

#### Cache and runtime

```
nornicdb_cache_hits_total{cache}                                              counter
nornicdb_cache_misses_total{cache}                                            counter
nornicdb_cache_size_bytes{cache}                                              gauge
nornicdb_cache_evictions_total{cache,reason}                                  counter

nornicdb_process_uptime_seconds                                               gauge
nornicdb_build_info{version,commit,go_version,backend}                        gauge       (always 1)
```

*Cache hit ratio is exposed as a Prometheus recording rule (`nornicdb_cache_hit_ratio{cache}`) shipped in Phase 10, computed from `cache_hits_total` and `cache_misses_total`. It is not a raw metric.*

#### Auth

```
nornicdb_auth_attempts_total{result,protocol}                                 counter
```

`result` is bounded (`success|failure|error`); `protocol` is bounded (`http|bolt`).

The Go runtime collector (`collectors.NewGoCollector`) and process collector
(`collectors.NewProcessCollector`) are registered alongside the above and
provide `go_*`/`process_*` for free.

#### 2.3.0 Reconciled amendments (Phase 0)

Three research-surfaced gap closures were folded into the §2.3 catalog above:

- **A2 — GAP-1 `nornicdb_replication_last_contact_seconds{peer}`:** added to the Replication subsystem (10th metric family). Standard SRE alert for quiescence vs. stall.
- **A3 — GAP-6 `nornicdb_auth_attempts_total{result,protocol}`:** added as a new Auth subsystem. Failed-auth rate is a standard security alert.
- **A4 — GAP-2 `nornicdb_cache_hit_ratio`:** ships as a recording-rule template (Phase 10) over the existing `cache_hits_total` / `cache_misses_total` counters, not as a raw metric — keeps the catalog primitives bounded.

### 2.3.1 Phase 5 amendment — Legacy translation layer & tenant flag (2026-05-04)

This subsection records the customer-API contract additions delivered by Phase 5
(Legacy Translation Layer & Tenant Flag, plans 05-01 through 05-05). Future
changes to any of the four contract surfaces below require a follow-up ADR
amendment + reviewer sign-off; the subsection IS the contract.

**Sunset header value (D-03):** The legacy `:7474/metrics` endpoint emits
`Sunset: Fri, 31 Dec 2027 23:59:59 GMT` (RFC 7231 IMF-fixdate format) on
every response. This date is the customer-API contract for migration off
the legacy 12-metric surface; changing it requires a follow-up ADR
amendment + reviewer sign-off. The literal lives in
`pkg/observability/legacy_translation.go` as `const LegacySunset` and is
locked by `pkg/observability/legacy_translation_test.go::TestRenderLegacy_HeadersConsts`.

**Deprecation header value (D-03a / R-01):** The endpoint emits
`Deprecation: true` per the original draft of the Deprecation HTTP header.
RFC 9745 (March 2025) supersedes that draft and now mandates a
Structured-Field Date format (`Deprecation: @<unix-timestamp>`); M1
deliberately ships `true` per the locked REQ MET-20 / ROADMAP SC#2 / D-03a
contract. An RFC-9745 cutover is captured as a deferred item in
`.planning/phases/05-legacy-translation-layer-tenant-flag/deferred-items.md`;
the non-conformance with RFC 9745 §2.1 is intentional and customer-API-stable
for M1.

**Autodetect contract (D-02):** The `metrics.tenant_labels_enabled` flag
defaults via multi-signal AND logic: `KUBERNETES_SERVICE_HOST` env var
non-empty AND `/var/run/secrets/kubernetes.io/serviceaccount/token`
exists AND has non-zero size. Operator can override via explicit YAML
`metrics.tenant_labels_enabled: true|false` (R-02 sentinel `*bool` field
`MetricsConfig.TenantLabelsExplicit` preserves operator intent through to
startup-resolution time so `explicit_false` beats `autodetect_true` on K8s).
Resolution happens once at startup, between `observability.New` and
the first bag constructor (Phase 4 D-02c init-order chokepoint). The resolved
value + source reason is logged once via the injected slog logger
(LOG-09 compliant — never `slog.Default`):

`level=INFO msg="resolved tenant labels enabled" enabled=<bool> reason=<enum> service_host_present=<bool> token_file_present=<bool>`

The `reason` enum is closed-set with exactly six values:
`explicit_yaml`, `k8s_detected`, `not_k8s_service_host_absent`,
`not_k8s_token_file_absent`, `not_k8s_token_file_empty`,
`not_k8s_token_stat_error`. Operators grep this finite vocabulary for
forensic resolution outcomes. The token file is checked via `os.Stat` only —
JWT bytes never enter process memory (T-05-11 mitigation).

**Translation layer (D-01..D-01f):** Customer scrapers on `:7474/metrics`
continue to receive 12 legacy metric names (`nornicdb_uptime_seconds`,
`nornicdb_requests_total`, `nornicdb_errors_total`,
`nornicdb_active_requests`, `nornicdb_nodes_total`, `nornicdb_edges_total`,
`nornicdb_embeddings_processed`, `nornicdb_embeddings_failed`,
`nornicdb_embedding_worker_running`, `nornicdb_slow_queries_total`,
`nornicdb_slow_query_threshold_ms`, `nornicdb_info`) fed from the unified
Phase 4 registry via the `pkg/observability.RenderLegacy(reg, now) []byte`
pure function. The 12-row mapping table is locked by
`pkg/observability/legacy_snapshot.golden` (1660 bytes, 36 emit lines); CI
byte-equality verifies that future Phase-4 catalog changes do not silently
break the legacy contract. Regeneration is gated behind `REGEN=1` env var
(`go test -run TestRenderLegacy_RegenerateGolden`); CI never sets REGEN.
The `:7474/metrics` auth gate (`s.withAuth(s.handleMetrics, auth.PermRead)`)
is unchanged — dropping auth would itself require its own deprecation cycle
per CLAUDE.md "Public API contract" (D-04).

### 2.4 Tracing — OpenTelemetry SDK with OTLP

* Tracer provider: `go.opentelemetry.io/otel/sdk/trace` with a `BatchSpanProcessor` configured `MaxQueueSize=8192`, `MaxExportBatchSize=1024`, `BatchTimeout=2s` (production-correct for bursty DB workloads). The BSP exposes self-instrumentation: `nornicdb_otel_bsp_queue_depth` (gauge) and `nornicdb_otel_bsp_dropped_spans_total` (counter).
* Exporter: OTLP/gRPC (`go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc`)
  with HTTP fallback. Endpoint, headers, and TLS controlled by the standard
  `OTEL_EXPORTER_OTLP_*` environment variables — *do not* invent
  `NORNICDB_OTLP_*` aliases; defer to OTel conventions so customers can use
  any collector they already run.
* OTLP endpoint URL must be HTTPS in production mode; plaintext `http://` is rejected unless the standard `OTEL_EXPORTER_OTLP_INSECURE=true` is set explicitly (per-signal variants `OTEL_EXPORTER_OTLP_TRACES_INSECURE` / `OTEL_EXPORTER_OTLP_METRICS_INSECURE` are also honored). YAML symmetry: `tracing.endpoint` field complements the standard `OTEL_EXPORTER_OTLP_*` env vars.
* Sampler (v1 default): `TraceIDRatioBased(NORNICDB_TRACE_SAMPLE_RATIO)` standalone, default `0.01` (1%). The default does **not** honor parent context — storage-layer trace-volume control is preserved against upstream 100% samplers. Two opt-in modes are available: `NORNICDB_TRACE_PARENT_MODE=capped` (`parent_capped` — honors upstream `sampled=true` up to `NORNICDB_TRACE_PARENT_MAX_QPS`, default 100/s, then falls back to ratio-based) and `NORNICDB_TRACE_PARENT_MODE=strict` (`parent_strict` — full upstream honor; emits a `WARN` startup log about unbounded volume risk). Tail sampling remains the OTel collector's job, not ours.
* Resource attributes: `service.name=nornicdb`, `service.version=<buildinfo>`, `service.instance.id` (resolution chain: `cfg.NodeID` → `POD_NAME` env → `os.Hostname()` → `"standalone"`; resolved value logged at startup), `nornicdb.cluster.mode`, `nornicdb.replication.role`.
* Propagators: W3C `traceparent`/`tracestate` + W3C `baggage` (defaults).

**Spans NornicDB will produce by default:**

| Span name                          | Where created                       | Notable attributes                                                                            |
|------------------------------------|-------------------------------------|-----------------------------------------------------------------------------------------------|
| `nornicdb.http.<route>`            | `otelhttp` middleware on `mux`      | `http.method`, `http.route`, `http.status_code`, `nornicdb.database`                          |
| `nornicdb.bolt.session`            | `pkg/bolt/server.go` per-conn       | `nornicdb.bolt.user`, `nornicdb.bolt.client_version`                                          |
| `nornicdb.bolt.message`            | per Bolt message dispatch           | `nornicdb.bolt.op`, `nornicdb.bolt.fields`                                                    |
| `nornicdb.cypher.execute`          | `StorageExecutor.Execute`           | `nornicdb.cypher.op_type`, `nornicdb.cypher.plan_hash`, `nornicdb.cypher.rows`                |
| `nornicdb.cypher.plan`             | planner entry                       | `nornicdb.cypher.plan_cache_hit`                                                              |
| `nornicdb.cypher.exec.<op>`        | per logical operator (NodeByLabel…) | `nornicdb.exec.estimated_rows`, `nornicdb.exec.actual_rows`                                   |
| `nornicdb.storage.<op>`            | Badger adapter                      | `nornicdb.storage.kind`, `nornicdb.storage.bytes`                                             |
| `nornicdb.embed.batch`             | embed queue worker                  | `nornicdb.embed.provider`, `nornicdb.embed.model`, `nornicdb.embed.batch_size`                |
| `nornicdb.search.<mode>`           | search service                      | `nornicdb.search.candidates`, `nornicdb.search.recall`                                        |
| `nornicdb.replication.append`      | leader replication path             | `nornicdb.replication.peer`, `nornicdb.replication.entries`, `nornicdb.replication.bytes`     |
| `nornicdb.replication.apply`       | follower apply loop                 | `nornicdb.replication.term`, `nornicdb.replication.index`                                     |

**Bolt + replication context propagation.** The existing replication codec (`pkg/replication/codec.go`) gains a `codec_version` field FIRST (a separate PR ahead of any tracing work) so that follower frame parsing is safe in both rolling-upgrade directions; the optional `traceparent` field is a SECOND, separate PR that depends on the codec-version PR having merged. A new Bolt extra `nornicdb.traceparent` is accepted on `BEGIN`/`RUN` so that client-generated spans connect to server spans. Both are backwards-compatible: missing context simply starts a new trace, never an error. Per-driver Bolt conformance (KD-08) is documented in Phase 8 (DOC-07); v1 minimum is `neo4j-go-driver/v5`.

**Exemplars.** Histograms attach the active span's trace ID as a Prometheus exemplar via `client_golang`'s `ExemplarObserver` API; emission is unconditional (an `IsValid()` guard is applied before observe). End-to-end correlation requires customer-side configuration: Prometheus `--enable-feature=exemplar-storage`, OpenMetrics negotiation (`Accept: application/openmetrics-text`), and a Tempo (or equivalent) datasource. The CI integration test against `kube-prometheus-stack` + Tempo (Phase 10) is the proof that the full chain works; the ADR's "one-click trace correlation" claim is gated on that test, not on the emission itself.

**Sample-rate mismatch detector.** When peer replicas report different sample ratios (e.g., during a rolling config rollout), `nornicdb_otel_sample_rate_mismatch_total` increments so operators can detect the mismatch without reading SDK source. AsyncEngine flush goroutine spans use `WithLinks` (carrying `trace.SpanContext` from the originating request) rather than parent-child, so embedding-queue handoffs do not appear as orphaned roots.

#### 2.4.1 Reconciled amendments

Four research-surfaced amendments to the tracing pillar:

- **A5 — `service.instance.id` resolution:** fallback chain `cfg.NodeID` → `POD_NAME` env → `os.Hostname()` → `"standalone"`. Resolved value is logged at startup so operators can verify which leg fired.
- **A6 — BSP tuning + self-instrumentation:** `BatchSpanProcessor` is configured with `MaxQueueSize=8192`, `MaxExportBatchSize=1024`, `BatchTimeout=2s` (production-correct for bursty DB workloads). Two self-instrumentation metrics expose pipeline health: `nornicdb_otel_bsp_queue_depth` (gauge) and `nornicdb_otel_bsp_dropped_spans_total` (counter).
- **A7 — Replication codec versioning prerequisite (TRC-21):** the optional `traceparent` field on `AppendEntries` is preceded by a `codec_version` field. These are separate, ordered PRs; the codec-version PR ships first to make rolling-upgrade frame parsing safe in both directions.
- **A8 — `parent_capped` sampler (TRC-06):** a third opt-in sampler mode (`NORNICDB_TRACE_PARENT_MODE=capped`) that honors upstream `sampled=true` up to `NORNICDB_TRACE_PARENT_MAX_QPS` (default 100/s) and falls back to ratio-based otherwise. The v1 default sampler (`TraceIDRatioBased(0.01)` standalone) is unchanged.

### 2.5 Logs — `log/slog` with mandatory fields

* Migrate the codebase from `log.Printf`/`fmt.Println` to `log/slog`. The
  global logger is constructed in `cmd/nornicdb/main.go` from
  `LoggingConfig` and stored in `pkg/observability.Logger()`.
* Public interface: stdlib `log/slog` (`slog.Logger`, `slog.Handler`). All
  call sites code against the slog interface so the underlying handler is
  swappable.
* Handler implementation: `slog.NewJSONHandler` when `Logging.Format=json`
  (the default for containers); `slog.NewTextHandler` otherwise. Phase 2
  benchmarks the stdlib handler against NornicDB's hot paths; if it doesn't
  meet the per-record allocation budget, a custom slog-compatible
  `Handler` is fair game (we're not married to the stdlib implementation,
  only to the interface contract). The decision is recorded in the Phase 2
  SUMMARY.md with benchmark numbers.
* **Mandatory fields on every record**:
  - `time` (RFC3339Nano, automatic)
  - `level` (`DEBUG|INFO|WARN|ERROR`)
  - `msg`
  - `service` = `"nornicdb"`
  - `version` = `buildinfo.Version`
  - `node_id` (cluster node ID; `"standalone"` if unclustered)
  - `trace_id`, `span_id` when a span is active in `ctx`
* **Conventional groups** (used via `slog.Group`):
  - `http`: `method`, `path`, `status`, `duration_ms`, `request_id`
  - `cypher`: `op_type`, `plan_hash`, `rows`, `duration_ms`, `database`
  - `storage`: `op`, `kind`, `bytes`, `duration_ms`
  - `replication`: `role`, `term`, `peer`, `lag_bytes`
* PII redaction. The handler runs through a small `Redactor` middleware
  that strips known sensitive keys (`password`, `token`, `authorization`,
  `secret`, `api_key`) regardless of nesting. Same allow-list as
  `pkg/audit` to keep the rules in one place.
* The slow-query log emits `WARN` with the truncated 500-char query, the
  `plan_hash`, and `cypher.duration_ms`. The full query text is **never**
  attached as a metric label or span attribute — it goes to the log only.
* `pkg/audit/audit.go` is **not** migrated to `slog`. Audit must remain a
  distinct, signed, append-only stream with its own retention policy.

### 2.6 Kubernetes service monitor — dedicated unauthenticated port

A new listener on port `9090` (configurable via `NORNICDB_TELEMETRY_PORT`, value is the port number with no colon prefix; bind address is `:` so the listener accepts on all interfaces and the NetworkPolicy below restricts ingress) serves:

| Path           | Auth      | Purpose                                                              |
|----------------|-----------|----------------------------------------------------------------------|
| `/metrics`     | **None**  | Prometheus pull endpoint. Returns `client_golang` registry.          |
| `/livez`       | None      | Process liveness — returns 200 once the OS process is past `main.go` startup. |
| `/readyz`      | None      | Readiness — returns 200 throughout the lifecycle: during warm-up the body is a progress JSON object (`{"phase": "warming", "storage_open": true, "search_warming": "...%"}`), and after warm-up completes the body is a steady-state `{"phase": "ready"}`. The contract is "200-with-body, parse the body for phase". The `startupProbe` (high `failureThreshold`, default 60 ≈ 10 min) enforces the time bound; `readinessProbe` polls `/readyz` and the kubelet treats 200 as routable. Operators who prefer 503-during-warm-up semantics can configure that via `metrics.readyz_503_until_ready=true` (default `false`). |
| `/version`     | None      | Plain-text `buildinfo.DisplayVersion()` (already public in `/health` parent). |

The data-plane `:7474` keeps the **legacy** `/metrics`, `/health`, `/status`
endpoints intact in M1 and emits deprecation headers
(`Deprecation: true`, `Sunset: <date>`) to give existing scrapers time to
migrate. **M1 ships only the deprecation markers — no removal logic.** The
actual removal (data-plane `/metrics` going away, `/health` becoming a thin
alias for `/livez`) is scheduled for v1.1.0 in a separate follow-up PR
coordinated with [issue #117](https://github.com/orneryd/NornicDB/issues/117);
that work is explicitly out of scope for ADR-0001.

The opt-in `:9091` pprof listener binds **`127.0.0.1` by default** (not `:9091` interpreted as `0.0.0.0:9091`); binding all interfaces would make pprof a credential-exfiltration surface. Operators who need remote pprof access must explicitly configure a non-loopback listen address and a corresponding NetworkPolicy.

**Why a separate port and not just "remove auth from `/metrics`":**

* Metric labels include `database` (tenant name). Exposing
  `nornicdb_storage_nodes_total{database="acme-prod"}` to anyone who can
  reach `:7474` from the public internet is a tenant-naming oracle. The
  defense is *network-scoped*, not bearer-scoped — exactly how Postgres'
  `postgres_exporter` and CockroachDB's `_status/vars` do it.
* Prometheus Operator's `ServiceMonitor` does not natively send bearer
  tokens that rotate with NornicDB's auth; making it work requires either
  long-lived static tokens (a security regression) or a sidecar
  (operational regression). A dedicated port closes both problems.

A reference Helm `ServiceMonitor` will ship at
`docs/operations/kubernetes-servicemonitor.md` as a Phase 9 deliverable
(K8S-01); the inlined snippet below is the contract that doc will
encode and ship in the bundled Helm chart:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: nornicdb
  labels:
    release: kube-prometheus-stack
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: nornicdb
  endpoints:
    - port: telemetry          # 9090, named in the Service
      path: /metrics
      interval: 30s
      scrapeTimeout: 10s
      honorLabels: false
      relabelings:
        - sourceLabels: [__meta_kubernetes_pod_label_app_kubernetes_io_instance]
          targetLabel: cluster
```

A NetworkPolicy template in the same doc is **default-on** in the bundled Helm chart, restricting `:9090` ingress to the monitoring namespace; operators opt out via `--set networkPolicy.enabled=false`. A `PodMonitor` template ships alongside `ServiceMonitor`, toggled via `--set podMonitor.enabled=true`. The chart's `startupProbe` (high `failureThreshold`, default 60 ≈ 10 min) is distinct from `readinessProbe`; `/readyz`'s 200-with-progress-JSON contract (defined in the table above) lets the kubelet treat the pod as routable as soon as core surfaces are up while still surfacing warm-up state to operators. Versioned alert rule files (`nornicdb-alerts-v1.yaml`) ship with an upgrade-diff doc; the cache hit-ratio recording rule is part of the same alerts bundle.

#### 2.6.1 Reconciled amendments

One research-surfaced amendment to the K8s pillar:

- **A9 — pprof bind address (OBS-06):** `:9091` binds `127.0.0.1` by default. Binding all interfaces would make pprof a credential exfiltration surface; the Helm chart template honors this default.

### 2.7 Configuration surface

New `Observability` block in `pkg/config/config.go`, all overridable via
environment so containers stay declarative:

```yaml
observability:
  metrics:
    enabled: true                       # NORNICDB_METRICS_ENABLED
    port: 9090                          # NORNICDB_TELEMETRY_PORT (number, no colon)
    # The legacy `:7474/metrics` data-plane endpoint stays enabled in M1 — no
    # config flag. It carries Deprecation/Sunset headers and is removed
    # post-M1 in a follow-up PR (target v1.1.0, see §3.3 risk row + #117).
  tracing:
    enabled: false                      # NORNICDB_TRACING_ENABLED
    sample_ratio: 0.01                  # NORNICDB_TRACE_SAMPLE_RATIO
    # OTel-standard env vars are honored: OTEL_EXPORTER_OTLP_ENDPOINT, etc.
  logging:
    level: "INFO"                       # NORNICDB_LOG_LEVEL
    format: "json"                      # NORNICDB_LOG_FORMAT (default flips to json)
    output: "stdout"                    # NORNICDB_LOG_OUTPUT
    redact_extra: []                    # NORNICDB_LOG_REDACT_EXTRA (csv)
  pprof:
    enabled: false                      # NORNICDB_PPROF_ENABLED
    listen: "127.0.0.1:9091"            # loopback by default; override only with explicit NetworkPolicy
```

### 2.8 Implementation map

A new package `pkg/observability` is the single place call sites import.
This keeps the Prometheus and OTel SDK surface out of business logic, makes
testing straightforward (interface seam), and lets us swap exporters
without a tree-wide diff.

```
pkg/observability/
  observability.go       // Init(ctx, cfg) -> Provider; Shutdown(ctx)
  metrics.go             // package-level registry + helpers (must call Init first)
  tracing.go             // tracer, propagators, span helpers
  logging.go             // slog handler, redactor, ctx-aware logger
  middleware_http.go     // wraps mux: otelhttp + per-route histogram
  middleware_bolt.go     // hooks into pkg/bolt server lifecycle
  middleware_grpc.go     // wraps grpc.Server with otelgrpc Unary + Stream interceptors;
                         //   produces standard gRPC server/client metrics
                         //   (rpc.server.duration, rpc.server.requests, etc.) and spans
                         //   for replication peer-to-peer traffic and any future gRPC surfaces
  exemplar.go            // histogram exemplar wiring (trace_id from ctx)
  testing.go             // in-memory exporters for unit tests
```

Wiring sequence in `cmd/nornicdb/main.go runServe`:

1. Load config; build `LoggingConfig` slogger first so subsequent steps log
   structured.
2. `observability.Init(ctx, cfg.Observability, buildinfo)` — registers
   collectors, starts OTel SDK, starts the `:9090` listener.
3. Pass the resulting `Provider` into `server.New`, `bolt.New`, and the
   replication transport so each subsystem can grab its `Meter`/`Tracer`.
4. On shutdown, drain in this order: HTTP server → Bolt server → background
   workers → `observability.Shutdown(ctx)`. The OTel batch span processor
   needs ~5s to flush; the existing graceful-shutdown context allows this.

Per AGENTS.md golden rules:

* Every new metric/span/log call site comes with a benchmark that proves
  ≤2% throughput regression on `make bench-cypher` and ≤5% on
  `make bench-bolt`. Hot paths (Cypher execute, storage get/put) use
  `metric.WithAttributes` only when the result is non-trivial; in the
  fast path we use pre-bound counters.
* The `pkg/observability` package targets ≥90% test coverage with the
  in-memory OTel exporter + `prometheus/testutil`.
* No file in `pkg/observability` exceeds 800 lines.
* Bolt wire format and HTTP responses are not changed except for the
  optional `traceparent` extra; Neo4j drivers that don't send it continue
  to work unchanged.

#### 2.8.1 Reconciled amendments

Two research-surfaced amendments to the implementation map:

- **A10a — Shutdown context independence:** observability shutdown uses a separate `context.WithTimeout(context.Background(), 30s)`, not the cancelled parent context (using the parent would cause an immediate return without flush). The mandatory shutdown order is HTTP → Bolt → background workers → observability flush, with the `:9090` listener closing **last** so the kubelet can scrape during graceful drain.
- **A10b — Test isolation pattern:** every test in `pkg/observability` constructs an isolated `*prometheus.Registry`, `InMemoryExporter`, and `*slog.Logger` — never `prometheus.DefaultRegisterer`, never `slog.SetDefault`. Each `*Vec` has a `TestMetricCardinality_<name>` test asserting `CollectAndCount(reg, name) <= ceiling` under 1k tenant UUIDs and adversarial label values. The three named bucket constants (`LatencyBucketsSeconds`, `SizeBucketsBytes`, `RowCountBuckets`) plus the long-tail `EmbeddingLatencyBucketsSeconds` are the single source of truth; registration helpers enforce bucket type at definition site.
- **A10c — Lifecycle supervisor primitive:** the canonical process supervisor for all current and future NornicDB binaries (`cmd/nornicdb/`, planned `cmd/metrics-doc-gen/`, future CLI utilities) is a `Run(ctx, components...) error` driver in `pkg/lifecycle/` built on `golang.org/x/sync/errgroup` with `signal.NotifyContext(SIGINT, SIGTERM)`. Reverse-order drain runs on a fresh `context.WithTimeout(context.Background(), 30s)` per A10a. The dependency `golang.org/x/sync/errgroup` is named here per CLAUDE.md "direct deps require ADR amendment"; it was already present in `go.sum` as an indirect transitive — Phase 1 promotes it to direct require.

### 2.9 Rollout plan

| Phase | Scope                                                                                              | Gate                                                                |
|-------|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| 1     | Land `pkg/observability`, `slog` migration in HTTP edge + Cypher executor, `:9090/metrics` parity. | Existing Prometheus scrapes keep working against legacy `:7474`.    |
| 2     | Add tracing (OTel SDK, otelhttp, executor + storage spans, OTLP exporter).                         | `make bench-cypher` regression ≤2%; `OTEL_EXPORTER_OTLP_ENDPOINT` opt-in. |
| 3     | Bolt instrumentation + replication propagation + exemplar wiring.                                  | New Bolt benchmarks added; raft soak test stays green for 24h.      |
| 4     | Ship Helm `ServiceMonitor`, `NetworkPolicy`, Grafana dashboard JSON, alert rules.                  | Tested against `kube-prometheus-stack` in CI.                       |
| 5     | Deprecate data-plane `:7474/metrics`. Major version bump.                                          | At least one minor release of overlap with Sunset header.           |

Each phase is its own PR with its own benchmark proof, per the AGENTS.md
"prove value before merging" rule.

---

## 3. Consequences

### 3.1 Positive

* **Customer-grade telemetry.** Operators get the same depth of insight
  that `pg_stat_statements` and Neo4j JMX provide, but in modern formats
  (Prom + OTLP) and with NornicDB-specific signals (MVCC pressure,
  embedding queue depth, hybrid-search stage timings).
* **One-click trace correlation.** Exemplars on histograms make latency
  outliers actionable in seconds.
* **Zero-config K8s integration.** `helm install` produces a working
  `ServiceMonitor` and dashboard against any `kube-prometheus-stack` cluster.
* **Cloud-neutral.** OTLP push covers the non-Prometheus customers without
  duplicating instrumentation.
* **Defense in depth.** Tenant names never leak through unauthenticated
  surfaces; the metrics port is network-scoped.
* **Foundations for SLOs.** Every metric has a `result` label, enabling
  customer-side SLO recording rules (`error budget = sum(...{result="error"}) / sum(...)`).

### 3.2 Negative / costs

* **Real migration work.** ~165 `log.Printf` sites become `slog` calls.
  Each change is mechanical but reviewer-heavy.
* **One new port to firewall.** Operators upgrading from older NornicDB
  must open `:9090` cluster-internally (documented in the upgrade notes).
* **Memory floor rises.** OTel SDK + Prometheus registry add ~10–20 MB to
  the resident set. Acceptable per AGENTS.md ≤10% rule for typical 1 GB
  deployments; we will document the cost for low-memory mode and provide
  `observability.metrics.enabled=false` for embedded use.
* **Public API surface.** Metric and span names become a versioned
  contract. Renames now require a deprecation window.
* **Cardinality discipline forever.** The label allow-list is the wall
  between us and a Prometheus blow-up. Code review must enforce it; a
  golangci-lint custom analyzer is a follow-up.

### 3.3 Risks and mitigations

| Risk                                                                               | Mitigation                                                                                                  |
|------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| Hot-path overhead from span creation in Cypher executor.                           | Sample at the trace level, not the span level; pre-bind counters; benchmark gate ≤2%.                        |
| OTLP endpoint misconfiguration crashes the server on startup.                      | Init returns a `noop` provider on exporter error and logs `WARN`; never fatal.                              |
| Customer scrapers depending on the legacy `:7474/metrics` break on removal.        | M1 only ships deprecation headers (`Deprecation: true`, `Sunset: <date>`) + metric-name parity on `:9090`. Removal is scheduled for v1.1.0 in a separate follow-up PR alongside [#117](https://github.com/orneryd/NornicDB/issues/117), giving customers at least one minor-release overlap window to migrate. |
| Trace context across Bolt accidentally breaks driver compatibility.                | Use a *new* extra (`nornicdb.traceparent`); ignore if absent. Conformance test added against neo4j-go-driver.|
| Label cardinality regression sneaks in via a future PR.                            | Build a custom `golangci-lint` analyzer that flags free-form strings reaching `prometheus.With*` outside an allow-list. |

### 3.4 Accepted challenges (KD-04..KD-09)

The following challenges to ADR-0001's original draft were raised during
review, accepted, and folded back into the body of the ADR. This table is
the audit trail GOV-01 reviewers verify against: every row points to the
pillar where the resolution now lives.

| KD    | Challenge                                                                                  | Resolution                                                                                                                                              | Where it lives now                                                                                 |
|-------|--------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| KD-04 | Adopt a router dep (chi/gorilla) for `path_template` extraction in HTTP middleware?        | Reject — Go 1.22+ stdlib `http.ServeMux` exposes the matched template via `r.Pattern`; no new dep added.                                                  | §2.2 (allowed labels — `path_template` derived from `r.Pattern`)                                  |
| KD-05 | Default sampler should be `ParentBased` to honor upstream sampling decisions               | Reject — storage layer must control its own trace volume; default is `TraceIDRatioBased(0.01)` standalone. `ParentBased`-style behavior is opt-in only.   | §2.4 (Sampler — default + opt-in `parent_capped` / `parent_strict` modes)                         |
| KD-06 | NetworkPolicy alone is sufficient defense for tenant-name leak through `:9090` labels      | Reject — non-K8s deployments lack NetworkPolicy. Add `metrics.tenant_labels_enabled` flag with K8s auto-detect (default `true` on K8s, `false` otherwise). | §2.2 (label kill-switch) and §2.6 (K8s detect signals + override)                                 |
| KD-07 | Cut over `:7474/metrics` to the new schema in one release                                  | Reject — published metric names are a customer contract. Phase 1 ships a translation layer that emits legacy names from the new registry; one-cycle deprecation overlap with `Sunset` header. | §2.6 (legacy data-plane endpoint + Deprecation/Sunset headers)                                    |
| KD-08 | Test the `nornicdb.traceparent` Bolt extra against the full driver matrix in Phase 1       | Defer — full matrix decision is a Phase 3 entry input; minimum required for v1 is `neo4j-go-driver/v5` round-trip. Per-driver conformance doc ships in Phase 8. | §2.4 (Bolt + replication context propagation — per-driver conformance doc DOC-07)                 |
| KD-09 | Claim "one-click trace correlation" as an unqualified ADR guarantee                        | Reject — exemplars are emitted unconditionally, but end-to-end correlation requires customer-side Prom `--enable-feature=exemplar-storage` + Tempo datasource. CI integration test against `kube-prometheus-stack` + Tempo is the proof. | §2.4 (Exemplars wording) and §3.3 (risk row reframed)                                             |

### 3.5 What this enables next

* **Continuous profiling** (Pyroscope/Parca) — the second observability ADR
  builds on the same `pkg/observability` provider.
* **Query insights UI** — the `cypher.plan_hash` label lets us surface a
  "top slow plans" panel in the bundled UI without re-running EXPLAIN.
* **SLO templates** — recording-rule and burn-rate alert templates ship in
  `docs/operations/`.
* **Tenant-aware billing/usage** — `nornicdb_cypher_queries_total{database,…}`
  is the same primitive a future managed-cloud control plane would meter on.

---

## 4. References

* `pkg/server/server_public.go` (existing handlers under change)
* `pkg/server/server_middleware.go` (`metricsMiddleware` to be replaced)
* `pkg/config/config.go:585` (`LoggingConfig` to be extended)
* `pkg/audit/audit.go` (compliance audit; explicitly *not* changed)
* `pkg/replication/codec.go` (gains optional `traceparent` field)
* `pkg/bolt/server.go` (gains lifecycle hooks for `pkg/observability/middleware_bolt.go`)
* `docs/operations/monitoring.md` (will be updated to reference the `:9090` model after Phase 1)
* OpenTelemetry semantic conventions for HTTP and database client spans
  (https://opentelemetry.io/docs/specs/semconv/database/)
* Prometheus instrumentation best practices
  (https://prometheus.io/docs/practices/instrumentation/)

### 4.1 Phase exit references

**M1 ships as a single PR.** All 13 phases of Milestone 1 land on the
`otel` branch and merge together via PR [#126](https://github.com/orneryd/NornicDB/pull/126);
there is no per-phase PR. This means the GOV-03 audit trail uses
**commit ranges on `otel`** rather than PR links. Each row below is
filled in by the verifier of the corresponding phase when the phase's
commits land on `otel` and the verifier passes — not at PR merge.

The `Commit range` column is the inclusive SHA range that delivered the
phase (e.g., `<first-sha>..<last-sha>`); the `Verified` column is the
date the verifier signed off; the `Verifier` column is the reviewer
handle who verified (per the §5 reviewer roster). Phase 0's row is
filled in below (§5 sign-off → §4.1 row 0) to establish the format.

| Phase    | Title                                              | Commit range | Verified | Verifier |
|----------|----------------------------------------------------|--------------|----------|----------|
| Phase 0  | ADR Governance & Sign-off                          | e97ed2b..8e384cf | 2026-04-30 | orneryd |
| Phase 1  | Observability Foundation Skeleton                  | fa272fc..3ad44d9 | 2026-04-30 | asanabria |
| Phase 2  | Structured Logging Migration                       | 4201a24..021a983 | 2026-05-02 | asanabria |
| Phase 3  | Metrics Infrastructure & Discipline                | 1ba8f01..a62d30a | 2026-05-02 | asanabria |
| Phase 4  | Subsystem Metric Catalog                           | 523c23d..9a0e574 | 2026-05-03 | asanabria |
| Phase 5  | Legacy Translation Layer & Tenant Flag             | 65f8771..973bbd7 | 2026-05-04 | asanabria |
| Phase 6  | Tracing SDK & Core Spans                           |              |          |          |
| Phase 7  | Replication Codec Versioning                       |              |          |          |
| Phase 8  | Bolt, Replication & Async Tracing + PII Defense    |              |          |          |
| Phase 9  | Kubernetes Helm Chart Integration                  |              |          |          |
| Phase 10 | Dashboards, Alerts & CI Integration Test           |              |          |          |
| Phase 11 | Metrics Reference Doc Generator                    |              |          |          |
| Phase 12 | Performance Gates & Hardening                      |              |          |          |

---

## 5. Decision sign-off

Sign-off coordination: PR [#126](https://github.com/orneryd/NornicDB/pull/126)
is the M1 PR — it carries all 13 phases of work and stays open until
M1 is complete. The four-role sign-off below is for **ADR-0001 itself**
(the design contract), not for the M1 release as a whole. Reviewer
name, approval date, and PR #126's URL are filled in here as approvals
land. Once all four cells are populated, the metadata `Status` field at
the top of this file is flipped from `**Proposed**` to `**Accepted**`,
and §4.1 row "Phase 0" is filled in with the **commit range on `otel`**
that delivered the ADR work — making §4.1's first row the format
template that subsequent phases append to. The §5 sign-off recorded
here is durable text in the ADR; it survives any GitHub-side review
state changes (e.g., `dismiss_stale_reviews_on_push` after subsequent
phase commits land).

| Role             | Reviewer               | Date | PR |
|------------------|------------------------|------|----|
| Architecture     | orneryd       | 2026-04-30   | https://github.com/orneryd/NornicDB/pull/126 |
| SRE / Ops        | linuxdynasty  | 2026-04-30   | https://github.com/orneryd/NornicDB/pull/126 |
| Security         | linuxdynasty  | 2026-04-30   | https://github.com/orneryd/NornicDB/pull/126 |
| Public API owner | orneryd       | 2026-04-30   | https://github.com/orneryd/NornicDB/pull/126 |
