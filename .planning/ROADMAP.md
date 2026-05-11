# ROADMAP: NornicDB Milestone 1 — Best-in-Class Observability

**Defined:** 2026-04-29
**Milestone:** M1 (ADR-0001 observability)
**Granularity:** fine (config: `granularity: fine` → 8-12 phases target; 13 chosen for natural fault lines)
**Source:** `.planning/PROJECT.md`, `.planning/REQUIREMENTS.md`, `.planning/research/SUMMARY.md`, `docs/architecture/adr/0001-observability.md`

## Core Value

Operators can debug, monitor, and SLO-instrument NornicDB to the same depth as `pg_stat_statements` and Neo4j JMX MBeans, exposed via Prometheus pull *and* OTLP push, with zero-config Kubernetes integration, without leaking tenant identity through unauthenticated surfaces.

## Universal Verifier Criteria (KD-12)

These hard gates are checked at every phase exit (do not repeat in per-phase success criteria):

- `make bench-cypher` Δ ≤ 2% (with tracing both ON and OFF)
- `make bench-bolt` Δ ≤ 5%
- `pkg/observability` ≥ 90% coverage (`go test -cover`)
- No file in `pkg/observability` exceeds 800 lines
- Neo4j wire compatibility preserved (driver matrix conformance)
- Resident memory floor ≤ 25 MB above pre-OTel baseline

## Phases

- [ ] **Phase 0: ADR Governance & Sign-off** — Revise ADR-0001 to fold all amendments and obtain four-role sign-off before any implementation
- [ ] **Phase 1: Observability Foundation Skeleton** — `pkg/observability` package, listeners, lifecycle supervision, shutdown, config block
- [x] **Phase 2: Structured Logging Migration** — slog handler init, 192-call-site migration, PII redaction, slow-query log (✅ 2026-05-02, commits 4201a24..021a983; ADR §4.1 row 2: `4201a24..021a983 | 2026-05-02 | asanabria`)
- [x] **Phase 3: Metrics Infrastructure & Discipline** — Naming, buckets, registration helpers, OTel→Prom bridge, exemplars, test isolation (✅ 2026-05-02, commits 1ba8f01..<commit-6a-SHA>; ADR §4.1 row 3: `1ba8f01..<commit-6a-SHA> | 2026-05-02 | asanabria`)
- [x] **Phase 4: Subsystem Metric Catalog** — 60+ metric families across HTTP, Bolt, Cypher, Storage, MVCC, Embed, Search, Replication, Auth, Cache+Runtime (✅ 2026-05-03, commits 523c23d..9a0e574; ADR §4.1 row 4: `523c23d..9a0e574 | 2026-05-03 | asanabria`)
- [x] **Phase 5: Legacy Translation Layer & Tenant Flag** — `:7474/metrics` translation, `tenant_labels_enabled` flag with K8s detection, golden-file test (✅ 2026-05-04, commits 65f8771..973bbd7; ADR §4.1 row 5: `65f8771..973bbd7 | 2026-05-04 | asanabria`)
- [ ] **Phase 6: Tracing SDK & Core Spans** — OTel SDK init, samplers (incl. `parent_capped`), HTTP/Cypher/Storage spans, BSP self-instrumentation
- [ ] **Phase 7: Replication Codec Versioning** — `codec_version` field prerequisite + rolling-upgrade test (BEFORE optional `traceparent`)
- [ ] **Phase 8: Bolt, Replication & Async Tracing + PII Defense** — Bolt session/message spans, traceparent propagation, AsyncEngine links, span redactor, baggage allow-list
- [ ] **Phase 9: Kubernetes Helm Chart Integration** — ServiceMonitor, PodMonitor, NetworkPolicy default-on, probes, preStop hook
- [ ] **Phase 10: Dashboards, Alerts & CI Integration Test** — Grafana dashboard, versioned alert rules, cache hit-ratio recording rule, kube-prometheus-stack CI test
- [ ] **Phase 11: Metrics Reference Doc Generator** — `cmd/metrics-doc-gen/`, hybrid-generated reference doc, bidirectional CI drift test, monitoring/upgrade docs
- [ ] **Phase 12: Performance Gates & Hardening** — `make bench-cypher`/`bench-bolt` regression verification, `TestObservability_MemoryFloor`, `AllocsPerRun` ≤ 2

## Phase Details

### Phase 0: ADR Governance & Sign-off
**Goal**: ADR-0001 is the contract; all challenges and amendments are folded in and four-role sign-off is recorded before any implementation work begins.
**Depends on**: Nothing (entry phase)
**Entry inputs**: None (this is the entry phase for M1)
**Requirements**: GOV-01, GOV-02, GOV-03
**Success Criteria** (what must be TRUE):
  1. ADR-0001 Status field reads `Accepted` (not `Proposed`) with date and signoff names
  2. Reviewer reads ADR-0001 and finds all 6 originally-accepted challenges (KD-04..KD-09) and all 10 research-surfaced amendments (parent_capped, OTel bridge prose, GAP-1, GAP-6, GAP-2 recording rule, codec versioning prerequisite, BSP tuning, service.instance.id resolution, pprof bind, shutdown ctx, test isolation pattern) explicitly addressed in §§2.2–2.8
  3. Architecture, SRE/Ops, Security, and Public-API-owner sign-offs are recorded in the ADR Status field
  4. ADR §4.1 has a "Phase exit references" section with `Commit range / Verified / Verifier` columns ready to receive per-phase commit ranges (M1 ships as a single PR; rows are filled at phase exit when commits land on `otel` and the verifier passes)
**Plans**: 2 plans
  - [ ] 00-01-PLAN.md — Fold KD-04..KD-09 challenges into §3.4, fold 10 research amendments inline into §§2.2–2.8 + per-pillar Reconciled subsections, pre-seed §4.1 with 13 phase rows, enrich §5 to four-column header
  - [ ] 00-02-PLAN.md — Open the M1 PR (#126), request four-role review for ADR sign-off (orneryd covers Arch + Public-API; linuxdynasty self-attests SRE + Security in §5), fill §5 reviewer cells + flip Status to Accepted + populate §4.1 row 0 with the Phase 0 commit range. PR stays open for Phase 1+ work; final merge is at M1 completion (post-Phase 12)

### Phase 1: Observability Foundation Skeleton
**Goal**: A `pkg/observability` leaf package exists with config, init order, listener supervision, and shutdown semantics — exposing `:9090` for telemetry and an opt-in `:9091` pprof — verifiable in isolation before any subsystem instrumentation lands.
**Depends on**: Phase 0
**Entry inputs**:
  - `tenant_label_mode=hash` decision: is hash-based pseudonymization needed in M1, or is `tenant_labels_enabled=false` sufficient? (Default: `enabled=false` is sufficient unless explicit decision otherwise.)
**Requirements**: OBS-01, OBS-02, OBS-03, OBS-04, OBS-05, OBS-06, OBS-07, OBS-08, OBS-09, OBS-10, OBS-11, OBS-12, PERF-05, PERF-06
**Success Criteria** (what must be TRUE):
  1. Operator can run NornicDB with default config and `curl http://127.0.0.1:9090/livez` returns 200, `/readyz` returns 200 once storage opens, `/version` returns build info, `/metrics` serves Prometheus exposition format
  2. Operator can set `NORNICDB_PPROF_ENABLED=true` and `curl http://127.0.0.1:9091/debug/pprof/` returns the index — and the same listener does NOT bind on `0.0.0.0` by default
  3. Operator can send SIGTERM and observe in logs that shutdown drains in order HTTP → Bolt → workers → observability flush, with `:9090` listener closing last (verifiable from log timestamps)
  4. Developer running `go test -cover ./pkg/observability/...` sees ≥ 90% line coverage, and no file in `pkg/observability` exceeds 800 lines
  5. Operator with no OTLP collector running sees a `WARN` log line about exporter init failure but the process stays up and `/livez` returns 200 (noop provider)
**Plans**: 5 plans
  - [x] 01-01-PLAN.md — pkg/lifecycle (Component, supervisor, shutdown, testenv) + ADR §2.8 errgroup amendment **(✅ 2026-04-30, commits fa272fc..3751c79)**
  - [x] 01-02-PLAN.md — pkg/observability core (config, resource, provider, registry); pkg/config integration **(✅ 2026-04-30, commits c842f82..36f3a2c — 90.4% coverage, OBS-02/03/04/10/11/12 GREEN)**
  - [x] 01-03-PLAN.md — pkg/observability listeners + health + pprof + testenv (OBS-01 leaf-package boundary)
  - [x] 01-04-PLAN.md — cmd/nornicdb integration: lifecycle.Run + adapters + db.HealthCheck wrapper
  - [ ] 01-05-PLAN.md — quality gates (PERF-05 coverage, PERF-06 file size, race stability, bench evidence)

### Phase 2: Structured Logging Migration
**Goal**: All ~165 `log.Printf`/`fmt.Println` sites in `pkg/server`, `pkg/cypher`, `pkg/storage`, `pkg/bolt` emit `log/slog` JSON records carrying `trace_id`/`span_id` correlation, mandatory fields, and PII redaction — without touching `pkg/audit`.
**Depends on**: Phase 1 (slog handler must live in `pkg/observability`)
**Entry inputs**: None
**Requirements**: LOG-01, LOG-02, LOG-03, LOG-04, LOG-05, LOG-06, LOG-07, LOG-08, LOG-09, LOG-10
**Success Criteria** (what must be TRUE):
  1. Operator running with `Logging.Format=json` (the container default) sees every log line as a single JSON object with `time` (RFC3339Nano), `level`, `msg`, `service=nornicdb`, `version`, `node_id` — and `trace_id`/`span_id` when the originating ctx has an active span
  2. A grep across `pkg/server`, `pkg/cypher`, `pkg/storage`, `pkg/bolt` finds zero remaining `log.Printf`/`fmt.Println` call sites; `pkg/audit/audit.go` is intentionally untouched and continues writing its signed stream
  3. Operator submits a query with `password=hunter2` in the literals; the slow-query log entry shows `<REDACTED>` for that literal and the truncated query is at most 500 chars; `password` keys nested inside structured attributes are stripped
  4. Lint/pre-commit hook rejects any new `slog.Default()` use outside `pkg/observability`
**Plans**: 6 plans
  - [x] 02-01-PLAN.md — pkg/observability custom slog handler stack (logger.go, logging.go, redaction.go, recovering.go, mandatory_fields.go) + bench harness + TestEnv record capture
  - [x] 02-02-PLAN.md — migrate pkg/server log call sites to slog (87 sites; D-08 two-phase init in cmd/nornicdb/main.go)
  - [x] 02-03-PLAN.md — migrate pkg/cypher (22 sites) + slow-query log + cypher.RedactLiterals + cypher.PlanHash + D-04d SlowQueryThreshold collapse
  - [x] 02-04-PLAN.md — migrate pkg/storage (48 sites) + D-06 AsyncEngine flushLog + D-07 walLog + structured [COUNT BUG]
  - [x] 02-05-PLAN.md — migrate pkg/bolt (18 sites) + [BOLT] bracket-to-component + HELLO credentials auto-redacted via D-03a
  - [x] 02-06-PLAN.md — make lint-slog (LOG-09 falsifiability) + phase-exit SUMMARY + ADR-0001 §4.1 row 2 audit-trail entry **(✅ 2026-05-02, commits ea66e2b / 021a983 / be5ab8a)**

### Phase 3: Metrics Infrastructure & Discipline
**Goal**: A registration helper layer in `pkg/observability` enforces naming discipline, bucket types, and the forbidden-label allow-list — and the OTel→Prom bridge is wired with `WithoutUnits()` and the `nornicdb_otel_*` namespace — before any subsystem starts emitting metrics.
**Depends on**: Phase 1
**Entry inputs**: None
**Requirements**: MET-01, MET-02, MET-03, MET-04, MET-05, MET-23, MET-24, MET-25, TEST-01, TEST-02
**Success Criteria** (what must be TRUE):
  1. Developer attempts to register a histogram with arbitrary buckets and the registration helper returns an error pointing to one of the three named bucket constants (`LatencyBucketsSeconds`, `SizeBucketsBytes`, `RowCountBuckets`); `EmbeddingLatencyBucketsSeconds` is available for embedding/LLM histograms
  2. Developer attempts to register a metric with a forbidden label (full path, raw Cypher, user/IP/UUID, embedding text, `trace_id`/`span_id`) and the helper rejects it at registration time with a clear error
  3. Every test in `pkg/observability` constructs an isolated `*prometheus.Registry`, `InMemoryExporter`, and `*slog.Logger`; running `go test -race ./pkg/observability/... -count=10` is stable (no global-state leakage)
  4. Each `*Vec` has a `TestMetricCardinality_<name>` test asserting `CollectAndCount(reg, name) <= ceiling` under 1k tenant UUIDs and adversarial label values
  5. Operator scraping `:9090/metrics` with `Accept: application/openmetrics-text` sees histogram exemplars with `trace_id` attached for at least one bucket, and the OTel-bridge metrics appear under `nornicdb_otel_*` (no auto-suffix collision with hand-instrumented `nornicdb_*`)
**Plans**: 6 plans
  - [x] 03-01-PLAN.md — Wave 0 RED scaffolding (6 _test.go files, compile-fail by design) (✅ 2026-05-02, commit 1ba8f01)
  - [x] 03-02-PLAN.md — bucket constants + typed constructors + label allow-list (MET-01..05) (✅ 2026-05-02, commits 9c548fe..e8beba7)
  - [x] 03-03-PLAN.md — exemplar wrapper types + Bind() pre-bound observers + bench (MET-24, MET-25) (✅ 2026-05-02, commit 5ad7ad0)
  - [x] 03-04-PLAN.md — /metrics OpenMetrics negotiation verify + AssertCardinalityCeiling helper (MET-24, TEST-01, TEST-02) (✅ 2026-05-02, commit e0a1298)
  - [x] 03-05-PLAN.md — Makefile lint-cardinality + falsifiability proof (MET-04 belt-and-suspenders) (✅ 2026-05-02, commit 5d6a243)
  - [x] 03-06-PLAN.md — Phase 3 SUMMARY + ADR §4.1 row 3 audit-trail entry (✅ 2026-05-02)

### Phase 4: Subsystem Metric Catalog
**Goal**: All ~60 metric families across nine subsystems (HTTP, Bolt, Cypher, Storage/Badger, MVCC, Embeddings, Search, Replication, Auth, Cache+Runtime) plus Go runtime/process collectors emit at `:9090/metrics` with hot-path handles pre-bound — including the GAP-1 (`replication_last_contact_seconds`) and GAP-6 (`auth_attempts_total`) additions.
**Depends on**: Phase 3
**Entry inputs**: None
**Requirements**: MET-06, MET-07, MET-08, MET-09, MET-10, MET-11, MET-12, MET-13, MET-14, MET-15, MET-16, MET-17, MET-26
**Success Criteria** (what must be TRUE):
  1. Operator scraping `:9090/metrics` finds every metric family enumerated in ADR-0001 §2.3 — HTTP (5), Bolt (6), Cypher (11), Storage (8), MVCC (4), Embeddings (6 + FFI panic counter), Search (4), Replication (10 incl. `last_contact_seconds`), Auth (1, GAP-6), Cache+Runtime (6) — plus `go_*` and `process_*` from the stdlib collectors
  2. Operator queries `nornicdb_cypher_queries_total{op_type=...}` and sees only the bounded enum values (`read|write|schema|admin|fabric`) — never raw query text classification
  3. Operator queries `nornicdb_storage_index_rebuild_total{index=...}` and sees only enum index names — no free-form values; `nornicdb_storage_bytes{kind=...}` similarly bounded
  4. Operator queries `nornicdb_cypher_slow_query_threshold_seconds` and gets the value in seconds; the legacy ms variant is not yet removed (Phase 13 cutover)
  5. Hot-path benchmark (`go test -bench Hot -benchmem ./pkg/observability/...`) shows ≤ 2 heap allocs per observation call (verified via `testing.AllocsPerRun`)
**Plans**:
  - [x] 04-01-PLAN.md — Wave-0 RED scaffolding (10 catalog stubs) + Cache+Runtime GREEN + build-tag matrix (✅ 2026-05-03, commits 523c23d..d8bc55c)
  - [x] 04-02-PLAN.md — HTTP + Bolt subsystem catalogs (MET-06, MET-07); instrumentedMux chokepoint (D-03) (✅ 2026-05-03)
  - [x] 04-03-PLAN.md — Cypher subsystem catalog (MET-08, MET-09 op_type closed enum, MET-26 slow_query_threshold GaugeFunc) (✅ 2026-05-03)
  - [x] 04-04-PLAN.md — Storage + MVCC catalogs (MET-10, MET-11; D-07 30s sweeper; D-13c index_rebuild enum; D-14 pressure_band) (✅ 2026-05-03)
  - [x] 04-05-PLAN.md — Embeddings + Search catalogs (MET-12, MET-13; D-06 Backend()+build-tag matrix; D-09 FFI panic counter) (✅ 2026-05-03)
  - [x] 04-06-PLAN.md — Replication + Auth catalogs (MET-14 incl. GAP-1 last_contact_seconds; MET-15 GAP-6 auth_attempts_total; D-05a/b) (✅ 2026-05-03)
  - [x] 04-07-PLAN.md — Phase 4 SUMMARY + ADR §4.1 row 4 + cumulative bench/coverage/lint-cardinality/audit-untouched/race-stability evidence (✅ 2026-05-03)

### Phase 5: Legacy Translation Layer & Tenant Flag
**Goal**: Existing customer scrapers on `:7474/metrics` continue to receive the original 12 metric names with their old labels — fed from the new `pkg/observability` registry via a translation layer — while a tenant-label kill switch with K8s auto-detect prevents tenant enumeration on `:9090`.
**Depends on**: Phase 4 (catalog must be complete to translate from)
**Entry inputs**: None
**Requirements**: MET-18, MET-19, MET-20, MET-21, MET-22, TEST-03
**Success Criteria** (what must be TRUE):
  1. Customer running an unmodified pre-M1 Prometheus scrape config against `:7474/metrics` continues to find `nornicdb_uptime_seconds`, `nornicdb_requests_total`, etc. with the same labels they had before — fed from the new registry, no second source of truth
  2. Every response from `:7474/metrics` carries `Deprecation: true` and `Sunset: <date>` HTTP headers
  3. Operator on a non-K8s host sees `tenant_labels_enabled=false` resolved at startup and confirms `database` label is absent on `nornicdb_storage_*`, `nornicdb_cypher_*`, `nornicdb_search_*`, `nornicdb_mvcc_*` series
  4. Operator on K8s (with `KUBERNETES_SERVICE_HOST` env + ServiceAccount token) sees `tenant_labels_enabled=true` resolved; can override via `metrics.tenant_labels_enabled=false`
  5. Developer running `go test ./pkg/observability/legacy_translation_test.go` against the locked snapshot fails CI on any diff between new-registry-emitted-via-translation-layer and the legacy snapshot (golden-file test)
**Plans**: 5 plans
  - [x] 05-01-PLAN.md — Wave-0 RED scaffolding: legacy_translation.go + k8s_detect.go skeletons + failing tests (locks public-API surface for Plans 05-02/05-03) (✅ 2026-05-04, commits 65f8771..f18d7ff)
  - [x] 05-02-PLAN.md — Translation engine: 12-row mapping table + reduction helpers + RenderLegacy emit + legacy_snapshot.golden contract lock (TEST-03) (✅ 2026-05-04, commits da0d1ca..58d8721)
  - [x] 05-03-PLAN.md — K8s autodetect: AND-signal Detect + ResolveTenantLabels + R-02 sentinel *bool + cmd/nornicdb startup-resolution hook + slog log line (MET-22) (✅ 2026-05-04, commits 863376f..e7ca461)
  - [x] 05-04-PLAN.md — Server adapter rewrite: handleMetrics 70 LOC → 7-line adapter + Deprecation/Sunset headers + integration tests (MET-19/20) (✅ 2026-05-04, commits 1ca2e61..849f948)
  - [x] 05-05-PLAN.md — Phase exit: 05-SUMMARY + ADR §2.3.1 amendment + ADR §4.1 row 5 + cumulative evidence (bench/coverage/file-size/race/audit-untouched/neo4j) + STATE/ROADMAP/REQUIREMENTS sync (✅ 2026-05-04, commits cf6791a..973bbd7)

### Phase 6: Tracing SDK & Core Spans
**Goal**: OTel tracing SDK initializes with production-correct BSP tuning and three sampler modes (default `TraceIDRatioBased(0.01)`, opt-in `parent_capped`, opt-in `parent_strict`); HTTP edge, Cypher executor, and Storage adapter produce the spans enumerated in ADR-0001 §2.4 — but Bolt and replication remain untraced (Phases 7–8).
**Depends on**: Phase 1 (resource attributes, init order, BSP self-instrumentation)
**Entry inputs**:
  - `parent_capped` sampler accept/reject (already accepted via TRC-06; confirm the default `NORNICDB_TRACE_PARENT_MAX_QPS=100/s` cap)
**Requirements**: TRC-01, TRC-02, TRC-03, TRC-04, TRC-05, TRC-06, TRC-07, TRC-08, TRC-09, TRC-10, TRC-11, TRC-12, TRC-15, TRC-16, TRC-17, TRC-23, TRC-24
**Success Criteria** (what must be TRUE):
  1. Operator runs NornicDB pointed at any OTLP/gRPC collector and observes `nornicdb.http.<route>` (template-resolved via `r.Pattern`), `nornicdb.cypher.execute`, `nornicdb.cypher.plan` (with `plan_hash` attribute), `nornicdb.cypher.exec.<op>` (with `estimated_rows`/`actual_rows`), and `nornicdb.storage.<op>` (with `kind`/`bytes`) spans flowing in
  2. Operator scraping `:9090/metrics` finds `nornicdb_otel_bsp_queue_depth` and `nornicdb_otel_bsp_dropped_spans_total` and can detect overflow without reading SDK source
  3. Operator with no `parent_*` mode set sees ~1% trace volume regardless of upstream sampling decision; setting `NORNICDB_TRACE_PARENT_MODE=capped` honors upstream `sampled=true` up to the QPS cap; setting `parent_strict` produces a startup `WARN` line
  4. Operator setting `OTEL_EXPORTER_OTLP_ENDPOINT=http://...` (plaintext) without `NORNICDB_OTLP_INSECURE=true` sees the SDK reject it with a clear error in production mode
  5. Operator running with two replicas configured at different sample ratios sees `nornicdb_otel_sample_rate_mismatch_total` increment; AsyncEngine flush spans appear linked (via `WithLinks`) to their originating request span, not as orphaned roots
**Plans**: TBD

### Phase 7: Replication Codec Versioning
**Goal**: Add a `codec_version` field to `AppendEntries` and ensure rolling upgrades work both directions BEFORE any optional `traceparent` field is added — codec-version PR ships first, traceparent PR ships second (TRC-21 hard ordering).
**Depends on**: Phase 0 (ADR amendment 5)
**Entry inputs**: None
**Requirements**: TRC-21, TRC-22, TEST-06
**Success Criteria** (what must be TRUE):
  1. A follower running the new binary accepts both pre-version `AppendEntries` frames (no `codec_version` field) AND versioned frames during a rolling upgrade — verified by a CI rolling-upgrade test
  2. A leader running the new binary continues sending pre-version frames if it detects any peer at the older codec version (mixed-cluster compat)
  3. CI test `TestReplicationCodec_RollingUpgrade` exercises pre-version-follower → version-aware-leader and the reverse without parse failures or panics
  4. The `codec_version` PR has merged before any PR that adds an optional `traceparent` field to `AppendEntries`
**Plans**: TBD

### Phase 8: Bolt, Replication & Async Tracing + PII Defense
**Goal**: Bolt session/message tracing, optional `nornicdb.traceparent` Bolt extra, replication append/apply spans with optional `traceparent` propagation, AsyncEngine span links, span attribute redactor, baggage allow-list, and PII test corpus — closing the cross-protocol trace context loop.
**Depends on**: Phase 6 (tracing SDK), Phase 7 (codec versioning prerequisite)
**Entry inputs**:
  - KD-08 Bolt driver matrix scope: minimum `neo4j-go-driver/v5`; broader matrix (Python, JS, Java, .NET) decided before Phase 8 starts
  - Bolt session span lifetime decision (default per ARCHITECTURE.md: session is parent, message is child; explicit ADR confirmation needed)
**Requirements**: TRC-13, TRC-14, TRC-18, TRC-19, TRC-20, SEC-01, SEC-02, SEC-03, SEC-04, SEC-05, TEST-05, TEST-07, DOC-07
**Success Criteria** (what must be TRUE):
  1. Operator with a Bolt client that sends `nornicdb.traceparent` in BEGIN/RUN sees the resulting Cypher span linked to the upstream trace; a client that omits the extra works identically and a new trace is started — verified via `neo4j-go-driver/v5` round-trip test (TEST-05)
  2. Operator inspects an OTLP export and finds zero occurrences of synthetic email-shaped, token-shaped, credit-card-shaped strings driven through every instrumented path — same assertion against slog output (PII test corpus, TEST-07, runs on every PR)
  3. Operator inspects spans for a Bolt HELLO and sees no authentication token attribute; the span attribute redactor strips known sensitive keys (sharing the `pkg/audit` allow-list)
  4. Operator on an HTTP edge sends arbitrary `baggage` headers; the receiving NornicDB span carries only baggage keys in the explicit allow-list (default: empty) — unknown keys are dropped
  5. Operator inspects replication spans and sees `nornicdb.replication.append` (leader) and `nornicdb.replication.apply` (follower) with peer/term/index attributes; embedding worker spans use `WithLinks` carrying `trace.SpanContext` from the queue handoff, not parent-child
  6. `pkg/audit/audit.go` retention/signing/serialization is unchanged (SEC-05); the per-driver Bolt conformance doc enumerates the agreed driver matrix
**Plans**: TBD

### Phase 9: Kubernetes Helm Chart Integration
**Goal**: Helm chart ships `ServiceMonitor`, `PodMonitor` (toggle), default-on `NetworkPolicy`, distinct `startupProbe`/`readinessProbe`/`livenessProbe`, and a `preStop` hook — so a customer running `helm install` on `kube-prometheus-stack` gets working scrape and graceful drain on first install.
**Depends on**: Phase 4 (metric catalog must be frozen — chart references metric names)
**Entry inputs**:
  - `/readyz` warm-up budget: is `startupProbe.failureThreshold=60` (~10 min) adequate for a production-sized customer index rebuild, or must it be configurable?
**Requirements**: K8S-01, K8S-02, K8S-03, K8S-04, K8S-05, K8S-06, K8S-07, K8S-08
**Success Criteria** (what must be TRUE):
  1. Operator runs `helm install` with default values on a cluster running `kube-prometheus-stack` and the Prometheus operator scrapes `:9090/metrics` successfully via the bundled `ServiceMonitor` (port `telemetry`)
  2. Operator with `--set podMonitor.enabled=true` gets a `PodMonitor` instead/alongside; the rendered `metricRelabelings` drop `target_info` series from the OTel bridge
  3. Operator with the default chart finds a `NetworkPolicy` blocking `:9090` ingress from outside the monitoring namespace; `--set networkPolicy.enabled=false` opts out
  4. Operator delivers SIGTERM to a pod and observes the `preStop` hook sleeps 10s before SIGTERM propagates — kube-proxy endpoint drain has time to complete
  5. Operator scaling up a fresh pod sees `/livez=200` immediately, `/readyz=200` with progress JSON during warm-up (not 503), `/readyz=200` with no progress JSON after warm-up; `startupProbe` tolerates the long warm-up via high `failureThreshold`
**Plans**: TBD
**UI hint**: yes

### Phase 10: Dashboards, Alerts & CI Integration Test
**Goal**: Reference Grafana dashboard JSON, versioned alert rule files with upgrade-diff doc, the `nornicdb_cache_hit_ratio` recording rule, and a CI integration test against `kube-prometheus-stack` + Tempo proving the full stack works end-to-end.
**Depends on**: Phase 9 (chart must exist), Phase 6 (tracing for exemplar correlation)
**Entry inputs**: None
**Requirements**: K8S-09, K8S-10, K8S-11, K8S-12, TEST-04
**Success Criteria** (what must be TRUE):
  1. Operator imports the dashboard JSON from `docs/operations/dashboards/` into a clean Grafana and immediately sees populated panels for HTTP/Bolt/Cypher/Storage/MVCC/Replication subsystems against a running NornicDB
  2. Operator applies `nornicdb-alerts-v1.yaml` from `docs/operations/alerts/` to Prometheus and the alert rules evaluate without syntax errors; the upgrade-diff doc explains v0→v1 changes
  3. Operator applies the recording rule template and `nornicdb_cache_hit_ratio{cache=...}` becomes queryable in Prometheus
  4. CI job `TestKubePromStack_Integration` (TEST-04 / K8S-12) provisions kube-prometheus-stack + Tempo, deploys NornicDB via the chart, and asserts: exemplar emission visible end-to-end, NetworkPolicy blocks unauthorized ingress, `ServiceMonitor` scrape succeeds, `/readyz` warm-up returns progress JSON, OTel→Prom bridge parity holds (no metric divergence)
**Plans**: TBD
**UI hint**: yes

### Phase 11: Metrics Reference Doc Generator
**Goal**: A standalone `cmd/metrics-doc-gen/` binary walks `registry.Gather()` to produce the canonical metrics table; hand-written narrative slots between markers; CI bidirectional drift test catches doc/code skew; `monitoring.md` and upgrade notes reflect the `:9090` model.
**Depends on**: Phase 4 (metric catalog must be complete to walk)
**Entry inputs**: None
**Requirements**: DOC-01, DOC-02, DOC-03, DOC-04, DOC-05, DOC-06
**Success Criteria** (what must be TRUE):
  1. Running `go run ./cmd/metrics-doc-gen` regenerates `docs/operations/metrics-reference.md` between `<!-- METRICS-TABLE-START -->` and `<!-- METRICS-TABLE-END -->` markers; per-subsystem narrative outside the markers is preserved
  2. Each row in the regenerated table has columns: Name, Type, Labels, Help, Purpose, "How you'd use it", Example PromQL
  3. CI drift test fails when (a) a registered metric is missing from the doc, or (b) a doc row references an unregistered metric — bidirectional consistency
  4. `docs/operations/monitoring.md` references the `:9090` model and links to the new reference doc; release-note upgrade notes cover the `:9090` cluster-internal firewalling story for non-K8s deployments
**Plans**: TBD

### Phase 12: Performance Gates & Hardening
**Goal**: Measured proof — across `make bench-cypher`/`make bench-bolt` with tracing both ON and OFF, `TestObservability_MemoryFloor`, and `AllocsPerRun` — that observability instrumentation has not regressed core throughput beyond the documented gates.
**Depends on**: Phases 1-11 (the entire instrumentation surface must exist to bench against)
**Entry inputs**: None
**Requirements**: PERF-01, PERF-02, PERF-03, PERF-04
**Success Criteria** (what must be TRUE):
  1. `make bench-cypher` with tracing OFF shows ≤ 2% throughput regression vs the locked pre-M1 baseline; `make bench-cypher` with tracing ON (sample ratio 0.01) shows ≤ 2% regression vs same baseline (the harder gate)
  2. `make bench-bolt` shows ≤ 5% throughput regression vs the locked pre-M1 baseline
  3. `TestObservability_MemoryFloor` in CI asserts resident memory ≤ 25 MB above the pre-OTel baseline at steady-state idle
  4. `testing.AllocsPerRun` for hot-path observation calls (HTTP middleware tick, storage op tick, Cypher op tick) reports ≤ 2 heap allocations per call — verified across the registered hot paths
**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 0. ADR Governance & Sign-off | 0/0 | Not started | - |
| 1. Observability Foundation Skeleton | 0/0 | Not started | - |
| 2. Structured Logging Migration | 0/0 | Not started | - |
| 3. Metrics Infrastructure & Discipline | 6/6 | Done | 2026-05-02 |
| 4. Subsystem Metric Catalog | 0/0 | Not started | - |
| 5. Legacy Translation Layer & Tenant Flag | 5/5 | Done | 2026-05-04 |
| 6. Tracing SDK & Core Spans | 0/0 | Not started | - |
| 7. Replication Codec Versioning | 0/0 | Not started | - |
| 8. Bolt, Replication & Async Tracing + PII Defense | 0/0 | Not started | - |
| 9. Kubernetes Helm Chart Integration | 0/0 | Not started | - |
| 10. Dashboards, Alerts & CI Integration Test | 0/0 | Not started | - |
| 11. Metrics Reference Doc Generator | 0/0 | Not started | - |
| 12. Performance Gates & Hardening | 0/0 | Not started | - |

## Coverage

- v1 requirements total: **112** (3 GOV + 12 OBS + 26 MET + 24 TRC + 10 LOG + 12 K8S + 7 DOC + 5 SEC + 6 PERF + 7 TEST). Note: REQUIREMENTS.md preamble said 110 (off by 2 — actual count is 112).
- Mapped to phases: **112 / 112** ✓
- Unmapped: 0 ✓
- Duplicate mappings: 0 ✓

## Hard Ordering Dependencies (cross-phase)

**Note on PR strategy:** M1 ships as a **single PR** (#126) on the `otel` branch. There are no per-phase PRs. The "ordering" rules below apply to **commit ordering on `otel`**, not PR-merge ordering.

1. **Phase 0 → Phase 1**: ADR sign-off gates implementation start. Sign-off is recorded in the ADR §5 table after orneryd's `Approve` review on PR #126; subsequent phase commits can begin landing on `otel` once §5 is filled in (orneryd's review may be auto-dismissed by GitHub when new commits land — that's expected; the §5 text is the durable audit trail).
2. **Phase 7 → Phase 8 (replication `traceparent`)**: the `codec_version` commits land on `otel` BEFORE any commit that adds an optional `traceparent` field to `AppendEntries` (TRC-21).
3. **Phase 4 → Phase 9**: Helm chart commits hard-code metric names; can't land on `otel` until catalog is frozen.
4. **slog handler in Phase 1 → Phase 6 tracing**: `trace_id`/`span_id` need somewhere to be logged; slog handler init commits land ahead of tracing wiring.
5. **Future deprecation cutover (post-M1)**: Removes `:7474/metrics` translation layer at major version; requires at least one minor release of overlap with `Sunset` header from Phase 5. Out of M1 scope and a separate PR alongside #117 (per `Out of Scope` table in PROJECT.md / REQUIREMENTS.md).

## Phase exit criteria (under single-PR strategy)

Each phase exits when **both** conditions are true:

1. The phase's commits have landed on `otel` and pass the universal performance/coverage gates (KD-12) plus phase-specific success criteria.
2. ADR-0001 §4.1's row for that phase has been updated with the commit range, verification date, and verifier handle.

There is no per-phase merge event; the M1 PR (#126) merges to `main` only after Phase 12 exits and all 13 §4.1 rows are populated.

---
*Roadmap created: 2026-04-29 by gsd-roadmapper for M1.*
