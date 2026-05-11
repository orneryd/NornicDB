# Requirements: NornicDB Milestone 1 — Best-in-Class Observability

**Defined:** 2026-04-29
**Core Value:** Operators can debug, monitor, and SLO-instrument NornicDB to the same depth as `pg_stat_statements` and Neo4j JMX MBeans, exposed via Prometheus pull *and* OTLP push, with zero-config Kubernetes integration, without leaking tenant identity through unauthenticated surfaces.
**Source:** ADR-0001 (`docs/architecture/adr/0001-observability.md`) + 6 originally accepted challenges + 10 research-surfaced amendments + parent_capped sampler.

---

## v1 Requirements

### ADR Governance (GOV)

- [x] **GOV-01** ✅ (2026-04-30): ADR-0001 revision incorporates all 6 originally-accepted challenges (KD-04 through KD-09 plus stdlib mux confirmation) and 10 research-surfaced amendments (parent_capped sampler, OTel bridge prose, GAP-1, GAP-6, GAP-2 recording rule, codec versioning prerequisite, BSP tuning, service.instance.id resolution, pprof bind, shutdown context, test isolation pattern) before any implementation phase begins
- [x] **GOV-02** ✅ (2026-04-30): All four ADR-0001 reviewer roles (Architecture, SRE/Ops, Security, Public API owner) record sign-off in the Status field before Phase 1 entry
- [x] **GOV-03** ✅ (2026-04-30): ADR-0001 §4.1 references are updated with the commit range that delivered each phase, verified by the phase verifier at phase exit (M1 ships as a single PR, so per-phase audit trail uses commit ranges on the M1 branch rather than PR links). Phase 0 row 0 is the first reference: `e97ed2b..8e384cf | 2026-04-30 | orneryd`. Phases 1–12 will append rows in this format.

### Observability Foundation (OBS)

- [x] **OBS-01**: A new `pkg/observability` package exists as the single instrumentation seam — a leaf in the import graph, never importing `pkg/cypher`, `pkg/storage`, `pkg/bolt`, or `pkg/server`
- [x] **OBS-02** ✅ (2026-04-30, Plan 01-02 commit 36f3a2c): Operator can configure observability via an `Observability` block in `nornicdb.yaml` and `NORNICDB_*` environment variables (metrics, tracing, logging, pprof sub-blocks)
- [x] **OBS-03** ✅ (2026-04-30, Plan 01-02 commit 7689318): Server starts with init order: logger → resource attributes → meter/tracer providers → registries → middleware → listeners
- [x] **OBS-04** ✅ (2026-04-30, Plan 01-02 commit 7689318): Operator can disable metrics entirely via `observability.metrics.enabled=false`
- [x] **OBS-05**: A new unauthenticated telemetry listener `:9090` (configurable via `NORNICDB_TELEMETRY_LISTEN`) serves `/metrics`, `/livez`, `/readyz`, `/version`
- [x] **OBS-06**: Optional pprof listener `:9091` is gated by `NORNICDB_PPROF_ENABLED=true` and binds to `127.0.0.1` by default
- [x] **OBS-07**: All four listeners (`:7474`, `:7687`, `:9090`, `:9091`) are supervised by a single `errgroup` derived from `signal.NotifyContext(SIGINT, SIGTERM)`
- [x] **OBS-08**: Shutdown drains in mandatory order: HTTP → Bolt → background workers → observability flush, with `:9090` listener closing last so the kubelet can scrape during graceful drain
- [x] **OBS-09**: Observability shutdown uses a separate `context.WithTimeout(context.Background(), 30s)` independent of the cancelled parent context (✅ Plan 01-01, commit 441fe27 — `pkg/lifecycle.Run` enforces fresh-context shutdown; `TestRun_FreshShutdownContext` is the keystone gate)
- [x] **OBS-10** ✅ (2026-04-30, Plan 01-02 commit dd35642): `service.instance.id` resolves via fallback chain: `cfg.NodeID` → `POD_NAME` env → `os.Hostname()` → `"standalone"`; resolved value is logged at startup
- [x] **OBS-11** ✅ (2026-04-30, Plan 01-02 commit 7689318): OTLP exporter init failure returns a noop provider with a `WARN` log line — never fatal at startup
- [x] **OBS-12** ✅ (2026-04-30, Plan 01-02 commit dd35642): `Observability` config block has YAML symmetry for OTLP endpoint (a `tracing.endpoint` field) in addition to honoring `OTEL_EXPORTER_OTLP_*` env vars

### Metrics Catalog and Infrastructure (MET)

**Naming and discipline**

- [x] **MET-01** ✅ (2026-05-02, Phase 3): All NornicDB-emitted metrics use the `nornicdb_` namespace prefix; subsystem prefixes match the package (`http_`, `bolt_`, `cypher_`, `storage_`, `mvcc_`, `embed_`, `search_`, `replication_`, `auth_`, `cache_`, `process_`)
- [x] **MET-02** ✅ (2026-05-02, Phase 3): Counters end in `_total`; histograms end in the unit they measure (`_seconds`, `_bytes`, `_rows`)
- [x] **MET-03** ✅ (2026-05-02, Phase 3): Three named bucket constants (`LatencyBucketsSeconds`, `SizeBucketsBytes`, `RowCountBuckets`) are the single source of truth; registration helpers enforce bucket type at definition site
- [x] **MET-04** ✅ (2026-05-02, Phase 3): Forbidden labels (full HTTP path, raw Cypher, user/IP/UUID, embedding text, span/trace IDs) are blocked at registration via a runtime allow-list check (Layer 1 panic + Layer 2 `make lint-cardinality`)
- [x] **MET-05** ✅ (2026-05-02, Phase 3): A long-tail latency bucket constant (`EmbeddingLatencyBucketsSeconds`) is used for embedding/LLM histograms

**Subsystem catalogs** (each REQ delivers the full set of metric families for that subsystem)

- [x] **MET-06** ✅ (2026-05-03, Phase 4): HTTP edge exposes 5 metric families: `requests_total{method,path_template,status_class,database}`, `request_duration_seconds`, `in_flight_requests`, `request_body_bytes`, `response_body_bytes`
- [x] **MET-07** ✅ (2026-05-03, Phase 4): Bolt protocol exposes 6 metric families: `connections_active`, `connections_total{result}`, `session_duration_seconds`, `messages_total{op,result}`, `message_duration_seconds{op}`, `packstream_decode_errors_total{reason}`
- [x] **MET-08** ✅ (2026-05-03, Phase 4): Cypher exposes 11 metric families per ADR-0001 §2.3 (queries_total, query_duration_seconds, planner_duration_seconds, planner_cache_hits/misses_total, planner_cache_size, rows_returned, active_transactions, transaction_conflicts_total, slow_queries_total, slow_query_threshold_seconds)
- [x] **MET-09** ✅ (2026-05-03, Phase 4): Cypher `op_type` label values come from the planner output, never from query text (read|write|schema|admin|fabric); `admin` queries that bypass the planner are classified at dispatch
- [x] **MET-10** ✅ (2026-05-03, Phase 4): Storage exposes 8 metric families: `nodes_total`, `edges_total`, `bytes{kind=nodes|edges|index|wal|search}`, `op_duration_seconds{op=get|put|delete|scan}`, `compactions_total{level,result}`, `compaction_duration_seconds{level}`, `wal_lag_bytes`, `index_rebuild_total{database,index,result}` (index label values are an enum, not free-form)
- [x] **MET-11** ✅ (2026-05-03, Phase 4): MVCC exposes 4 metric families: `pressure_band{database,band=normal|warn|high|critical}`, `pinned_bytes`, `oldest_reader_age_seconds`, `active_readers`
- [x] **MET-12** ✅ (2026-05-03, Phase 4): Embeddings expose 6 metric families: `queue_depth`, `processed_total{provider,model,result,mode}` (where `mode` is `gpu|cpu|cuda|metal|vulkan`), `duration_seconds` (long-tail buckets), `cache_hits_total`, `cache_misses_total`, `worker_running`; plus an FFI panic counter
- [x] **MET-13** ✅ (2026-05-03, Phase 4): Search exposes 4 metric families: `requests_total{database,mode,result}`, `duration_seconds{database,mode,stage=embed|index|fuse}`, `candidates`, `index_size_bytes{kind=hnsw|bm25}`
- [x] **MET-14** ✅ (2026-05-03, Phase 4): Replication exposes 10 metric families: `role`, `term`, `commit_index`, `apply_index`, `lag_bytes{peer}`, `lag_entries{peer}`, `apply_duration_seconds`, `rtt_seconds{peer}`, `leader_changes_total`, **`last_contact_seconds{peer}`** (GAP-1)
- [x] **MET-15** ✅ (2026-05-03, Phase 4): A new Auth subsystem exposes **`auth_attempts_total{result,protocol}`** (GAP-6)
- [x] **MET-16** ✅ (2026-05-03, Phase 4): Cache and runtime expose 6 metric families: `cache_hits_total{cache}`, `cache_misses_total{cache}`, `cache_size_bytes{cache}`, `cache_evictions_total{cache,reason}`, `process_uptime_seconds`, `build_info{version,commit,go_version,backend}`
- [x] **MET-17** ✅ (2026-05-03, Phase 4): Go runtime collector (`collectors.NewGoCollector`) and process collector (`collectors.NewProcessCollector`) are registered alongside NornicDB metrics

**Customer-facing infrastructure**

- [x] **MET-18** ✅ (2026-05-04, Phase 5): Operator can scrape `:9090/metrics` with no authentication and receive Prometheus exposition format
- [x] **MET-19** ✅ (2026-05-04, Phase 5): Customer scraper on legacy `:7474/metrics` continues to receive the original 12 metric names with old labels via a translation layer for one full release cycle
- [x] **MET-20** ✅ (2026-05-04, Phase 5): Legacy `:7474/metrics` responses include `Deprecation: true` and `Sunset: <date>` headers
- [x] **MET-21** ✅ (2026-05-04, Phase 5): When `metrics.tenant_labels_enabled=false`, the registry strips the `database` label from `nornicdb_storage_*`, `nornicdb_cypher_*`, `nornicdb_search_*`, `nornicdb_mvcc_*` series
- [x] **MET-22** ✅ (2026-05-04, Phase 5): `metrics.tenant_labels_enabled` defaults to `true` on K8s detection (multi-signal: `KUBERNETES_SERVICE_HOST` env + ServiceAccount token presence), `false` otherwise; resolved value logged at startup
- [x] **MET-23** ✅ (2026-05-02, Phase 3): OTel meter API instrumentation is bridged to the same numbers via the `otelprom` exporter as additional egress, configured with `WithoutUnits()` and a `nornicdb_otel_*` namespace to prevent collision
- [x] **MET-24** ✅ (2026-05-02, Phase 3): Histogram exemplars are emitted unconditionally via `client_golang` `ExemplarObserver`; `/metrics` negotiates `Accept: application/openmetrics-text`; `IsValid()` && `IsSampled()` guard applied before observe (single chokepoint in `pkg/observability/exemplar.go`)
- [x] **MET-25** ✅ (2026-05-02, Phase 3): Hot-path metric handles are pre-bound at construction (`Bind(lvs...)` returns `BoundObserver`); no `WithLabelValues` calls inside request loops; `BenchmarkObserve_Hot/hot_no_span` = 0 allocs/op
- [x] **MET-26** ✅ (2026-05-03, Phase 4): Slow-query threshold metric is exposed in seconds (`nornicdb_cypher_slow_query_threshold_seconds`), not milliseconds (legacy unit removed at deprecation cutover)

### Tracing (TRC)

- [ ] **TRC-01**: OTel SDK initializes with `BatchSpanProcessor` configured `MaxQueueSize=8192`, `MaxExportBatchSize=1024`, `BatchTimeout=2s`
- [ ] **TRC-02**: BSP exposes self-instrumentation: `nornicdb_otel_bsp_queue_depth` gauge and `nornicdb_otel_bsp_dropped_spans_total` counter
- [ ] **TRC-03**: OTLP/gRPC is the primary exporter; OTLP/HTTP available as fallback
- [ ] **TRC-04**: stdout exporter is available for DEV builds and disabled in production builds
- [ ] **TRC-05**: Default sampler is `TraceIDRatioBased(NORNICDB_TRACE_SAMPLE_RATIO)` standalone, with default ratio `0.01` — does NOT honor parent context by default
- [ ] **TRC-06**: Operator can opt into `parent_capped` sampler mode via `NORNICDB_TRACE_PARENT_MODE=capped` with `NORNICDB_TRACE_PARENT_MAX_QPS` (default 100/s); honors upstream `sampled=true` up to the cap, falls back to ratio-based otherwise
- [ ] **TRC-07**: Operator can opt into `parent_strict` sampler mode (full upstream honor) via `NORNICDB_TRACE_PARENT_MODE=strict`, with explicit `WARN` logged at startup about unbounded volume risk
- [ ] **TRC-08**: OTLP endpoint config honors standard `OTEL_EXPORTER_OTLP_*` env vars exclusively (no `NORNICDB_OTLP_*` aliases)
- [ ] **TRC-09**: OTLP endpoint URL must be HTTPS in production mode; plaintext `http://` is rejected unless `NORNICDB_OTLP_INSECURE=true` is set explicitly
- [ ] **TRC-10**: Resource attributes set on every span: `service.name=nornicdb`, `service.version`, `service.instance.id`, `nornicdb.cluster.mode`, `nornicdb.replication.role`
- [ ] **TRC-11**: W3C `traceparent`/`tracestate`/`baggage` propagators are configured as the default
- [ ] **TRC-12**: HTTP edge produces `nornicdb.http.<route>` spans via `otelhttp` middleware reading `r.Pattern` (Go 1.22+ stdlib)
- [ ] **TRC-13**: Bolt session produces `nornicdb.bolt.session` spans (one per connection); per-message dispatch produces `nornicdb.bolt.message` child spans; PULL chunks do not span
- [ ] **TRC-14**: Bolt accepts an optional `nornicdb.traceparent` extra on `BEGIN`/`RUN`; missing extra starts a new trace, never errors
- [ ] **TRC-15**: Cypher executor produces `nornicdb.cypher.execute` and `nornicdb.cypher.plan` spans with `plan_hash` attribute
- [ ] **TRC-16**: Per-logical-operator spans `nornicdb.cypher.exec.<op>` produced by the executor with `estimated_rows` and `actual_rows`
- [ ] **TRC-17**: Storage adapter produces `nornicdb.storage.<op>` spans with `kind` and `bytes` attributes
- [ ] **TRC-18**: Embedding queue worker produces `nornicdb.embed.batch` spans; queue handoff uses `WithLinks` not parent-child (job struct carries `trace.SpanContext`)
- [ ] **TRC-19**: Search service produces `nornicdb.search.<mode>` spans with `candidates` and `recall` attributes
- [ ] **TRC-20**: Replication append/apply paths produce `nornicdb.replication.append` (leader) and `nornicdb.replication.apply` (follower) spans with peer/term/index attributes
- [ ] **TRC-21**: Replication codec `AppendEntries` gains a `codec_version` field BEFORE the optional `traceparent` field is added (separate ordered PRs; codec version PR ships first)
- [ ] **TRC-22**: Follower with `codec_version` knowledge accepts both versioned and pre-version frames during rolling upgrade (rolling-upgrade test in CI)
- [ ] **TRC-23**: AsyncEngine flush goroutine creates a span linked to the originating request span via `WithLinks`
- [ ] **TRC-24**: A sample-rate-mismatch detector emits `nornicdb_otel_sample_rate_mismatch_total` counter when peer replicas report different sample ratios

### Structured Logging (LOG)

- [x] **LOG-01**: All `log.Printf`/`fmt.Println` sites in `pkg/server`, `pkg/cypher`, `pkg/storage`, `pkg/bolt` are migrated to `log/slog` (target: ~165 sites identified in the codebase audit; actual: 192 raw matches → 0 post-phase)
- [x] **LOG-02**: Default handler is `slog.NewJSONHandler` when `Logging.Format=json` (production-required default for containers)
- [x] **LOG-03**: Text handler is dev-only and explicitly insecure for untrusted input; CRLF stripping applied to attribute values
- [x] **LOG-04**: Every log record contains mandatory fields: `time` (RFC3339Nano), `level`, `msg`, `service=nornicdb`, `version`, `node_id`, plus `trace_id`/`span_id` when ctx has an active span
- [x] **LOG-05**: Conventional groups (`http`, `cypher`, `storage`, `replication`) are used via `slog.Group` for protocol-specific attributes
- [x] **LOG-06**: PII redactor (sharing the `pkg/audit` allow-list: `password`, `token`, `authorization`, `secret`, `api_key`) strips matching keys regardless of nesting; allow-list is configurable via `NORNICDB_LOG_REDACT_EXTRA`
- [x] **LOG-07**: Slow-query log emits `WARN` with truncated 500-char query, `plan_hash`, and `cypher.duration_ms`; full query text never appears as metric label or span attribute
- [x] **LOG-08**: Slow-query log applies Cypher AST literal redaction (`<REDACTED>` for string/number literals) before truncation
- [x] **LOG-09**: `slog.Default()` is forbidden outside `pkg/observability`; `make lint-slog` Makefile target rejects new uses (CI-falsifiable; POSIX-portable grep; wired into `make test`)
- [x] **LOG-10**: `pkg/audit/audit.go` is NOT migrated to `slog` and retains its existing immutable, signed, append-only stream and retention policy

### Kubernetes Integration (K8S)

- [ ] **K8S-01**: Helm chart includes a `ServiceMonitor` template (`docs/operations/kubernetes-servicemonitor.md`) targeting port `telemetry` (9090)
- [ ] **K8S-02**: Helm chart includes a `PodMonitor` template alongside `ServiceMonitor`, toggled via `--set podMonitor.enabled=true`
- [ ] **K8S-03**: `ServiceMonitor` includes `metricRelabelings` to drop `target_info` series emitted by the OTel→Prom bridge
- [ ] **K8S-04**: Helm chart `NetworkPolicy` is **default-on**; restricts `:9090` ingress to monitoring namespace; disabled via `--set networkPolicy.enabled=false`
- [ ] **K8S-05**: `/livez` returns 200 once the OS process is past `main.go` startup
- [ ] **K8S-06**: `/readyz` returns 200 only when storage is open AND search warming is complete; during warm-up returns 200 with progress JSON, not 503
- [ ] **K8S-07**: Helm chart includes a `startupProbe` with high `failureThreshold` (default 60, ~10 min) for storage warm-up, distinct from `readinessProbe`
- [ ] **K8S-08**: Helm chart includes a `preStop` lifecycle hook that sleeps 10s before SIGTERM to drain `kube-proxy` endpoints
- [ ] **K8S-09**: Reference Grafana dashboard JSON ships at `docs/operations/dashboards/`
- [ ] **K8S-10**: Versioned alert rule files (`nornicdb-alerts-v1.yaml`) ship at `docs/operations/alerts/` with an upgrade diff doc
- [ ] **K8S-11**: A cache hit-ratio recording rule template (`nornicdb_cache_hit_ratio{cache}`) ships at `docs/operations/alerts/` (GAP-2; not a raw metric)
- [ ] **K8S-12**: CI integration test against `kube-prometheus-stack` + Tempo confirms exemplar correlation, NetworkPolicy enforcement, ServiceMonitor scrape, `/readyz` warm-up behavior, and OTel→Prom bridge parity

### Documentation Deliverables (DOC)

- [ ] **DOC-01**: Metrics reference doc at `docs/operations/metrics-reference.md` is generated by a `cmd/metrics-doc-gen/` binary that walks `registry.Gather()` to produce the table
- [ ] **DOC-02**: Each metric in the reference doc has columns: Name, Type, Labels, Help, Purpose, "How you'd use it" (example operator workflow), Example PromQL
- [ ] **DOC-03**: Per-subsystem narrative ("when to alert," "common pitfalls," "what good looks like") is hand-written between `<!-- METRICS-TABLE-START/END -->` markers
- [ ] **DOC-04**: CI test asserts bidirectional consistency: every emitted metric appears in the doc; no doc entries exist for unregistered metrics
- [ ] **DOC-05**: `docs/operations/monitoring.md` is updated to reference the `:9090` model after Phase 1 ships
- [ ] **DOC-06**: Upgrade notes (in release notes) cover opening `:9090` cluster-internally for non-K8s deployments
- [ ] **DOC-07**: Per-driver Bolt conformance doc enumerates the driver matrix tested with the `nornicdb.traceparent` extra (input scope set at Phase 3 entry)

### Security and PII Defense (SEC)

- [ ] **SEC-01**: A span attribute redactor strips known sensitive keys before export, sharing the redaction list with `pkg/audit`
- [ ] **SEC-02**: W3C baggage forwarded across NornicDB applies an explicit allow-list (default: empty); unknown baggage keys are dropped at the HTTP edge
- [ ] **SEC-03**: Bolt HELLO authentication tokens never appear on spans, metrics, or logs (verified via SEC-04)
- [ ] **SEC-04**: A PII test corpus runs in CI from Phase 1: synthetic queries containing email-shaped, token-shaped, and credit-card-shaped strings are driven through every instrumented path; the test asserts zero occurrences in OTLP exporter output AND slog output
- [ ] **SEC-05**: `pkg/audit/audit.go` retention, signing, and serialization are unchanged; M1 verifies no audit-bypass path is introduced

### Performance Gates (PERF)

- [ ] **PERF-01**: `make bench-cypher` shows ≤2% throughput regression vs the pre-M1 baseline, measured with tracing both ON and OFF
- [ ] **PERF-02**: `make bench-bolt` shows ≤5% throughput regression vs the pre-M1 baseline
- [ ] **PERF-03**: Resident memory floor increases ≤25 MB above the pre-OTel baseline (verified by `TestObservability_MemoryFloor` in CI)
- [ ] **PERF-04**: Hot-path observation calls allocate ≤2 heap objects per invocation (verified via `testing.AllocsPerRun`)
- [x] **PERF-05**: `pkg/observability` line coverage ≥90% via `go test -cover` (Phase 1: 92.1%; Phase 2: 91.2%)
- [x] **PERF-06**: No file in `pkg/observability` exceeds 800 lines (Phase 1 max: 228 LOC `health.go`; Phase 2 max: 512 LOC `logging.go` — headroom 288)

### Test Discipline (TEST)

- [x] **TEST-01** ✅ (2026-05-02, Phase 3 confirms Phase 1 foundation): Every test in `pkg/observability` uses an isolated `*prometheus.Registry`, `InMemoryExporter`, and `*slog.Logger` — no global state (no `prometheus.DefaultRegisterer`, no `slog.SetDefault`)
- [x] **TEST-02** ✅ (2026-05-02, Phase 3): Each `*Vec` has a cardinality ceiling test: `(*TestEnv).AssertCardinalityCeiling(t, name, ceiling, drive)` shipped in `pkg/observability/testenv.go`; drives 1k tenant UUIDs across 8 goroutines + adversarial label values via `errgroup.SetLimit(8)`
- [x] **TEST-03** ✅ (2026-05-04, Phase 5): A golden-file test in `pkg/observability/legacy_translation_test.go` fails CI on any diff between new-registry-emitted-via-translation-layer and the locked legacy `:7474/metrics` snapshot
- [ ] **TEST-04**: A `kube-prometheus-stack` integration test in CI verifies exemplar emission, NetworkPolicy enforcement, ServiceMonitor scrape, `/readyz` warm-up behavior, and OTel→Prom bridge parity
- [ ] **TEST-05**: Bolt driver round-trip conformance test runs against the matrix defined for KD-08 (minimum: `neo4j-go-driver/v5`)
- [ ] **TEST-06**: Replication codec rolling-upgrade test exercises pre-version follower → version-aware leader and the reverse without parse failures
- [ ] **TEST-07**: PII test corpus (SEC-04) runs in CI on every change to `pkg/observability` or any instrumented site

---

## v2 Requirements

Acknowledged but deferred from M1. Tracked here so they aren't re-discovered later.

### Observability extensions (v2)

- **GAP-3**: Outbound connection-pool saturation metrics (per OTel semconv `db.client.connection.*` for the embedding HTTP client and KMS clients). Trigger: customer reports opaque embedding stalls.
- **GAP-4**: `pg_stat_statements`-style top-N plan stats endpoint — authenticated HTTP endpoint emitting per-`plan_hash` aggregate stats. Implementation: in-memory ring of fingerprints + endpoint.
- **GAP-5**: Search recall-estimate gauge — opt-in ground-truth probes producing `nornicdb_search_recall_estimate{database,mode}`.
- **GAP-7**: Per-database slow-query threshold — Neo4j `db.logs.query.threshold` analog. Defer until a customer reports needing it for a mixed OLTP/OLAP database.
- **PROFILE-01**: Continuous profiling integration (Pyroscope/Parca) — second observability ADR builds on the same `pkg/observability` provider.
- **LINT-01**: Custom `golangci-lint` cardinality analyzer — automates the label allow-list enforcement that M1 enforces via code review.

---

## Out of Scope

| Feature | Reason |
|---------|--------|
| Migrate `pkg/audit/audit.go` to `slog` | Compliance audit must remain a distinct, signed, append-only stream with its own retention policy. Folding it into operational telemetry collapses two threat models into one. |
| Continuous profiling (Pyroscope/Parca) as default | Deferred to a follow-up ADR. `pprof` is the only profiling surface in M1, opt-in only. |
| Tail sampling inside the database | Collector's job, not the database's. NornicDB emits at the configured ratio; downstream collectors trim further if needed. |
| Bearer-token auth on `:7474/metrics` | The deprecation path retains today's auth gate; the new unauth path is `:9090`. Adding bearer auth to `:9090` defeats `ServiceMonitor` integration. |
| Router migration (`chi`, `gorilla/mux`) | Stdlib `http.ServeMux` (Go 1.22+) supports pattern routing and exposes the matched template via `r.Pattern`. Adding a router dep buys ergonomics M1 doesn't need. (KD-04) |
| `NORNICDB_OTLP_*` env aliases | Defer to OTel-standard `OTEL_EXPORTER_OTLP_*` so customers reuse existing collector configurations. |
| All non-observability NornicDB feature work | Storage redesign, query language extensions, UI work, replication mode improvements. M1 is observability-only. Out-of-band feature work continues on other branches and is not coordinated through this `.planning/`. |
| Custom `golangci-lint` cardinality analyzer | Acknowledged in ADR §3.2 as desirable; deferred to a follow-up. M1 enforces cardinality through code review + the registration helper allow-list (MET-04). |
| Public API breakage of legacy metrics | Removing `nornicdb_uptime_seconds`, `nornicdb_requests_total`, etc. without the deprecation overlap is forbidden. Phase 5 deprecates; no earlier phase removes. |
| Folding compliance audit log into telemetry | Same separation rationale as the slog migration exclusion. |
| `parent_strict` sampler as default | Bursts of distributed traces from a customer's 100% sampler would defeat the storage-layer trace-volume control. Available as opt-in via `NORNICDB_TRACE_PARENT_MODE=strict` with WARN log. |

---

## Traceability

Each requirement maps to exactly one phase in `.planning/ROADMAP.md`. Generated by `gsd-roadmapper` on 2026-04-29.

| Requirement | Phase | Status |
|-------------|-------|--------|
| GOV-01 | Phase 0 | ✅ Done (2026-04-30; ADR §3.4 + §§2.X.1) |
| GOV-02 | Phase 0 | ✅ Done (2026-04-30; ADR §5 four-role sign-off recorded) |
| GOV-03 | Phase 0 | ✅ Done (2026-04-30; ADR §4.1 row 0 = `e97ed2b..8e384cf | 2026-04-30 | orneryd`) |
| OBS-01 | Phase 1 | Complete |
| OBS-02 | Phase 1 | ✅ Complete (Plan 01-02, 2026-04-30) |
| OBS-03 | Phase 1 | ✅ Complete (Plan 01-02, 2026-04-30) |
| OBS-04 | Phase 1 | ✅ Complete (Plan 01-02, 2026-04-30) |
| OBS-05 | Phase 1 | Complete |
| OBS-06 | Phase 1 | Complete |
| OBS-07 | Phase 1 | Complete |
| OBS-08 | Phase 1 | Complete |
| OBS-09 | Phase 1 | ✅ Complete (Plan 01-01, 2026-04-30) |
| OBS-10 | Phase 1 | ✅ Complete (Plan 01-02, 2026-04-30) |
| OBS-11 | Phase 1 | ✅ Complete (Plan 01-02, 2026-04-30) |
| OBS-12 | Phase 1 | ✅ Complete (Plan 01-02, 2026-04-30) |
| LOG-01 | Phase 2 | Complete |
| LOG-02 | Phase 2 | Complete |
| LOG-03 | Phase 2 | Complete |
| LOG-04 | Phase 2 | Complete |
| LOG-05 | Phase 2 | Complete |
| LOG-06 | Phase 2 | Complete |
| LOG-07 | Phase 2 | Complete |
| LOG-08 | Phase 2 | Complete |
| LOG-09 | Phase 2 | Complete |
| LOG-10 | Phase 2 | Complete |
| MET-01 | Phase 3 | Complete |
| MET-02 | Phase 3 | Complete |
| MET-03 | Phase 3 | Complete |
| MET-04 | Phase 3 | Complete |
| MET-05 | Phase 3 | Complete |
| MET-23 | Phase 3 | Complete |
| MET-24 | Phase 3 | Complete |
| MET-25 | Phase 3 | Complete |
| MET-06 | Phase 4 | Complete |
| MET-07 | Phase 4 | Complete |
| MET-08 | Phase 4 | Complete |
| MET-09 | Phase 4 | Complete |
| MET-10 | Phase 4 | Complete |
| MET-11 | Phase 4 | Complete |
| MET-12 | Phase 4 | Complete |
| MET-13 | Phase 4 | Complete |
| MET-14 | Phase 4 | Complete |
| MET-15 | Phase 4 | Complete |
| MET-16 | Phase 4 | Complete |
| MET-17 | Phase 4 | Complete |
| MET-26 | Phase 4 | Complete |
| MET-18 | Phase 5 | Complete |
| MET-19 | Phase 5 | Complete |
| MET-20 | Phase 5 | Complete |
| MET-21 | Phase 5 | Complete |
| MET-22 | Phase 5 | Complete |
| TRC-01 | Phase 6 | Pending |
| TRC-02 | Phase 6 | Pending |
| TRC-03 | Phase 6 | Pending |
| TRC-04 | Phase 6 | Pending |
| TRC-05 | Phase 6 | Pending |
| TRC-06 | Phase 6 | Pending |
| TRC-07 | Phase 6 | Pending |
| TRC-08 | Phase 6 | Pending |
| TRC-09 | Phase 6 | Pending |
| TRC-10 | Phase 6 | Pending |
| TRC-11 | Phase 6 | Pending |
| TRC-12 | Phase 6 | Pending |
| TRC-15 | Phase 6 | Pending |
| TRC-16 | Phase 6 | Pending |
| TRC-17 | Phase 6 | Pending |
| TRC-23 | Phase 6 | Pending |
| TRC-24 | Phase 6 | Pending |
| TRC-21 | Phase 7 | Pending |
| TRC-22 | Phase 7 | Pending |
| TRC-13 | Phase 8 | Pending |
| TRC-14 | Phase 8 | Pending |
| TRC-18 | Phase 8 | Pending |
| TRC-19 | Phase 8 | Pending |
| TRC-20 | Phase 8 | Pending |
| SEC-01 | Phase 8 | Pending |
| SEC-02 | Phase 8 | Pending |
| SEC-03 | Phase 8 | Pending |
| SEC-04 | Phase 8 | Pending |
| SEC-05 | Phase 8 | Pending |
| DOC-07 | Phase 8 | Pending |
| K8S-01 | Phase 9 | Pending |
| K8S-02 | Phase 9 | Pending |
| K8S-03 | Phase 9 | Pending |
| K8S-04 | Phase 9 | Pending |
| K8S-05 | Phase 9 | Pending |
| K8S-06 | Phase 9 | Pending |
| K8S-07 | Phase 9 | Pending |
| K8S-08 | Phase 9 | Pending |
| K8S-09 | Phase 10 | Pending |
| K8S-10 | Phase 10 | Pending |
| K8S-11 | Phase 10 | Pending |
| K8S-12 | Phase 10 | Pending |
| TEST-04 | Phase 10 | Pending |
| DOC-01 | Phase 11 | Pending |
| DOC-02 | Phase 11 | Pending |
| DOC-03 | Phase 11 | Pending |
| DOC-04 | Phase 11 | Pending |
| DOC-05 | Phase 11 | Pending |
| DOC-06 | Phase 11 | Pending |
| PERF-01 | Phase 12 | Pending |
| PERF-02 | Phase 12 | Pending |
| PERF-03 | Phase 12 | Pending |
| PERF-04 | Phase 12 | Pending |
| PERF-05 | Phase 1 | Complete |
| PERF-06 | Phase 1 | Complete |
| TEST-01 | Phase 3 | Complete |
| TEST-02 | Phase 3 | Complete |
| TEST-03 | Phase 5 | Complete |
| TEST-05 | Phase 8 | Pending |
| TEST-06 | Phase 7 | Pending |
| TEST-07 | Phase 8 | Pending |

**Coverage:**
- v1 requirements: 112 total (header previously stated 110 — corrected after enumeration: 3 GOV + 12 OBS + 26 MET + 24 TRC + 10 LOG + 12 K8S + 7 DOC + 5 SEC + 6 PERF + 7 TEST = 112)
  - GOV: 3
  - OBS: 12
  - MET: 26
  - TRC: 24
  - LOG: 10
  - K8S: 12
  - DOC: 7
  - SEC: 5
  - PERF: 6
  - TEST: 7
- Mapped to phases: 112 / Unmapped: 0 ✓
- Duplicate mappings: 0 ✓

---
*Requirements defined: 2026-04-29*
*Last updated: 2026-04-29 — Traceability section populated by gsd-roadmapper (13 phases).*
